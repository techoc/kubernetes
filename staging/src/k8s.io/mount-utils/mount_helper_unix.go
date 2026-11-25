//go:build !windows

/*
Copyright 2019 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package mount

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"golang.org/x/sys/unix"
	"k8s.io/klog/v2"
	utilio "k8s.io/utils/io"
)

const (
	// expectedAtLeastNumFieldsPerMountInfo 是 /proc/<pid>/mountinfo 每行允许的最少字段数。
	// 可选字段数量不固定，只能校验下限，剩下按 "-" 分段解析。
	expectedAtLeastNumFieldsPerMountInfo = 10
	// maxListTries 限制为获得一致的 mountinfo 内容而重试的次数。
	// 老内核读 mountinfo 可能拿到半行，连续多次读到相同内容才算一致。
	maxListTries = 10
)

// IsCorruptedMnt 判断错误是否表示挂载点已损坏，损坏时返回 true。
// 这些 errno 表示挂载已失效（NFS 断连、句柄过期等），此时路径存在但不可用。
func IsCorruptedMnt(err error) bool {
	if err == nil {
		return false
	}
	// os 包会层层包装错误，先剥到最底层的 errno 再判断。
	var underlyingError error
	switch pe := err.(type) {
	case nil:
		return false
	case *os.PathError:
		underlyingError = pe.Err
	case *os.LinkError:
		underlyingError = pe.Err
	case *os.SyscallError:
		underlyingError = pe.Err
	case syscall.Errno:
		underlyingError = err
	}

	// 用 errors.Is 而非相等比较，兼容被包装过的 errno。
	return errors.Is(underlyingError, syscall.ENOTCONN) ||
		errors.Is(underlyingError, syscall.ESTALE) ||
		errors.Is(underlyingError, syscall.EIO) ||
		errors.Is(underlyingError, syscall.EACCES) ||
		errors.Is(underlyingError, syscall.EHOSTDOWN) ||
		errors.Is(underlyingError, syscall.EWOULDBLOCK) ||
		errors.Is(underlyingError, syscall.ENODEV)
}

// MountInfo 表示 /proc/<pid>/mountinfo 中的一行。
// ID/ParentID 用于重建挂载树，major:minor 与 Root 配合才能跨挂载点识别同一份数据。
type MountInfo struct {
	// ID 是挂载的唯一编号，卸载后可能被复用。
	// 因此它只在单次快照内唯一。
	ID int
	// ParentID 是父挂载的编号；当前挂载命名空间中挂载树的根指向自己。
	// 遍历挂载树时要防环。
	ParentID int
	// Major 是设备号中标识设备类别的部分，解析自该文件系统上文件的 st_dev。
	Major int
	// Minor 是设备号中标识具体设备实例的部分，解析自该文件系统上文件的 st_dev。
	// 设备号相同的多个挂载点要靠 Root 区分，不能只看 major:minor。
	Minor int
	// Root 是文件系统中构成该挂载根目录的路径。
	Root string
	// Source 是内核提供的挂载源信息，可能是设备路径，也可能是 tmpfs、NFS 等文件系统特定名称。
	Source string
	// MountPoint 是挂载点路径。
	// /proc 里记录的是解析过软链接的路径，直接比较可省一次 EvalSymlinks。
	MountPoint string
	// OptionalFields 保存零个或多个 "tag[:value]" 形式的可选字段。
	// 数量不固定，原样保留交给调用方解释，解析阶段不猜测语义。
	OptionalFields []string
	// FsType 是 "type[.subtype]" 形式的文件系统类型。
	// 可能带子类型后缀（如 fuse.sshfs），做类型匹配时要留意。
	FsType string
	// MountOptions 是仅适用于当前挂载点的选项。
	MountOptions []string
	// SuperOptions 是适用于超级块的选项。
	// 同一设备的多个挂载点共享这些选项，fsck 相关参数也在这里。
	SuperOptions []string
}

// ParseMountInfo 解析 /proc/xxx/mountinfo。
// 把 "-" 之前的可选字段与之后的三段固定字段分开处理，格式扩展不会污染固定部分。
func ParseMountInfo(filename string) ([]MountInfo, error) {
	content, err := readMountInfo(filename)
	if err != nil {
		return []MountInfo{}, err
	}
	contentStr := string(content)
	infos := []MountInfo{}

	for _, line := range strings.Split(contentStr, "\n") {
		if line == "" {
			// 按换行拆分后，末尾会多出一个空字符串。
			continue
		}
		// 字段位置是内核 ABI，改解析逻辑前先对照 man proc 的权威格式说明。
		fields := strings.Fields(line)
		if len(fields) < expectedAtLeastNumFieldsPerMountInfo {
			return nil, fmt.Errorf("wrong number of fields in (expected at least %d, got %d): %s", expectedAtLeastNumFieldsPerMountInfo, len(fields), line)
		}
		id, err := strconv.Atoi(fields[0])
		if err != nil {
			return nil, err
		}
		parentID, err := strconv.Atoi(fields[1])
		if err != nil {
			return nil, err
		}
		mm := strings.Split(fields[2], ":")
		if len(mm) != 2 {
			return nil, fmt.Errorf("parsing '%s' failed: unexpected minor:major pair %s", line, mm)
		}
		major, err := strconv.Atoi(mm[0])
		if err != nil {
			return nil, fmt.Errorf("parsing '%s' failed: unable to parse major device id, err:%v", mm[0], err)
		}
		minor, err := strconv.Atoi(mm[1])
		if err != nil {
			return nil, fmt.Errorf("parsing '%s' failed: unable to parse minor device id, err:%v", mm[1], err)
		}

		info := MountInfo{
			ID:           id,
			ParentID:     parentID,
			Major:        major,
			Minor:        minor,
			Root:         fields[3],
			MountPoint:   fields[4],
			MountOptions: splitMountOptions(fields[5]),
		}
		// 可选字段以 "-" 收尾，找不到就说明快照被截断或格式异常。
		i := 6
		for ; i < len(fields) && fields[i] != "-"; i++ {
			info.OptionalFields = append(info.OptionalFields, fields[i])
		}
		// "-" 之后固定是 fstype/source/super options，少一个就说明内容不完整。
		i++
		if len(fields)-i < 3 {
			return nil, fmt.Errorf("expect 3 fields in %s, got %d", line, len(fields)-i)
		}
		info.FsType = fields[i]
		info.Source = fields[i+1]
		info.SuperOptions = splitMountOptions(fields[i+2])
		infos = append(infos, info)
	}
	return infos, nil
}

// splitMountOptions 将逗号分隔的挂载选项解析为数组。
// 双引号内的逗号不作为分隔符，因为 SELinux 上下文等选项可能包含带逗号的引号串。
func splitMountOptions(s string) []string {
	inQuotes := false
	list := strings.FieldsFunc(s, func(r rune) bool {
		if r == '"' {
			inQuotes = !inQuotes
		}
		// 引号状态边扫描边翻转，不支持转义引号；内核的输出格式也用不到。
		return r == ',' && !inQuotes
	})
	return list
}

// isMountPointMatch 判断 mp 中的路径是否与 dir 相同。
// 内核会给失效 NFS 挂载点路径加上 deleted 后缀，去掉后才能匹配调用方给的原路径。
func isMountPointMatch(mp MountPoint, dir string) bool {
	return strings.TrimSuffix(mp.Path, "\\040(deleted)") == dir
}

// PathExists 判断指定路径是否存在；过期但未消失的挂载点返回 true 并附带损坏错误。
// TODO: 改用 pkg/util/file/FileExists。
// 多看一次 access 是为了识别“stat 失败但路径仍在”的过期挂载，通用 FileExists 不做这件事。
func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	} else if errors.Is(err, fs.ErrNotExist) {
		err = syscall.Access(path, syscall.F_OK)
		if err == nil {
			// access 认为文件存在而 stat 认为不存在，曾在 CIFS 服务端删除路径时出现。
			// POSIX 称之为过期文件句柄；这里伪造该错误，将路径视为存在但已损坏。
			// 返回 ESTALE 而不是普通 true，调用方据此走挂载清理流程。
			klog.Warningf("Potential stale file handle detected: %s", path)
			return true, syscall.ESTALE
		}
		return false, nil
	} else if IsCorruptedMnt(err) {
		return true, err
	}
	return false, err
}

// 以下变量仅供 kernelHasMountinfoBug 使用。
// 用 sync.Once 而非每次读内核版本：版本在进程生命周期内不会变。
var (
	hasMountinfoBug       bool
	checkMountinfoBugOnce sync.Once
)

// kernelHasMountinfoBug 通过内核版本判断可能导致 mountinfo 读取不完整的缺陷是否已修复。
// 该缺陷由内核提交 9f6c61f96f2d97 修复，自 Linux 5.8 起生效；除此之外只能依赖 uname 返回的版本。
// 5.8 之前的内核可能在并发修改下给出半截 mountinfo，只能靠重复读取兜底。
// 版本解析不出来时保守认定有 bug，宁可多读几次。
func kernelHasMountinfoBug() bool {
	checkMountinfoBugOnce.Do(func() {
		// uname 失败或版本格式异常时按有 bug 处理，默认偏保守。
		hasMountinfoBug = true

		uname := unix.Utsname{}
		err := unix.Uname(&uname)
		if err != nil {
			return
		}

		end := bytes.IndexByte(uname.Release[:], 0)
		v := bytes.SplitN(uname.Release[:end], []byte{'.'}, 3)
		if len(v) != 3 {
			return
		}
		major, _ := strconv.Atoi(string(v[0]))
		minor, _ := strconv.Atoi(string(v[1]))

		// 修复从 5.8 进入，边界上的 5.8 本身算已修复。
		if major > 5 || (major == 5 && minor >= 8) {
			hasMountinfoBug = false
		}
	})

	return hasMountinfoBug
}

// readMountInfo 读取 mountinfo，并针对旧内核的并发截断缺陷做一致性重试。
// 老内核用 ConsistentRead 多次比对；新内核直接读一次，省掉重复 open/read 的开销。
func readMountInfo(path string) ([]byte, error) {
	if kernelHasMountinfoBug() {
		return utilio.ConsistentRead(path, maxListTries)
	}

	return os.ReadFile(path)
}
