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
	// At least number of fields per line in /proc/<pid>/mountinfo.
	// 可选字段数量不固定，只能校验下限，剩下按 "-" 分段解析。
	expectedAtLeastNumFieldsPerMountInfo = 10
	// How many times to retry for a consistent read of /proc/mounts.
	// 老内核读 mountinfo 可能拿到半行，连续多次读到相同内容才算一致。
	maxListTries = 10
)

// IsCorruptedMnt return true if err is about corrupted mount point
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

// MountInfo represents a single line in /proc/<pid>/mountinfo.
// ID/ParentID 用于重建挂载树，major:minor 与 Root 配合才能跨挂载点识别同一份数据。
type MountInfo struct {
	// Unique ID for the mount (maybe reused after umount).
	// umount 之后 ID 会被复用，因此它只在单次快照内唯一。
	ID int
	// The ID of the parent mount (or of self for the root of this mount namespace's mount tree).
	// 根挂载的 ParentID 指向自己，遍历挂载树时要防环。
	ParentID int
	// Major indicates one half of the device ID which identifies the device class
	// (parsed from `st_dev` for files on this filesystem).
	Major int
	// Minor indicates one half of the device ID which identifies a specific
	// instance of device (parsed from `st_dev` for files on this filesystem).
	// 设备号相同的多个挂载点要靠 Root 区分，不能只看 major:minor。
	Minor int
	// The pathname of the directory in the filesystem which forms the root of this mount.
	Root string
	// Mount source, filesystem-specific information. e.g. device, tmpfs name.
	// 这是内核给的字符串，不一定是块设备路径（tmpfs/nfs 只是名字）。
	Source string
	// Mount point, the pathname of the mount point.
	// /proc 里记录的是解析过软链接的路径，直接比较可省一次 EvalSymlinks。
	MountPoint string
	// Optional fieds, zero or more fields of the form "tag[:value]".
	// 数量不固定，原样保留交给调用方解释，解析阶段不猜测语义。
	OptionalFields []string
	// The filesystem type in the form "type[.subtype]".
	// 可能带子类型后缀（如 fuse.sshfs），做类型匹配时要留意。
	FsType string
	// Per-mount options.
	MountOptions []string
	// Per-superblock options.
	// 同一设备的多个挂载点共享这些选项，fsck 相关参数也在这里。
	SuperOptions []string
}

// ParseMountInfo parses /proc/xxx/mountinfo.
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
			// the last split() item is empty string following the last \n
			continue
		}
		// 字段位置是内核 ABI，改解析逻辑前先对照 man page。
		// See `man proc` for authoritative description of format of the file.
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
		// All fields until "-" are "optional fields".
		i := 6
		for ; i < len(fields) && fields[i] != "-"; i++ {
			info.OptionalFields = append(info.OptionalFields, fields[i])
		}
		// "-" 之后固定是 fstype/source/super options，少一个就说明内容不完整。
		// Parse the rest 3 fields.
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

// splitMountOptions parses comma-separated list of mount options into an array.
// It respects double quotes - commas in them are not considered as the option separator.
// SELinux context 等选项里含有带逗号的引号串，裸按逗号切会把一个选项拆成两个。
func splitMountOptions(s string) []string {
	inQuotes := false
	list := strings.FieldsFunc(s, func(r rune) bool {
		if r == '"' {
			inQuotes = !inQuotes
		}
		// 引号状态边扫描边翻转，不支持转义引号；内核的输出格式也用不到。
		// Report a new field only when outside of double quotes.
		return r == ',' && !inQuotes
	})
	return list
}

// isMountPointMatch returns true if the path in mp is the same as dir.
// Handles case where mountpoint dir has been renamed due to stale NFS mount.
// 内核会给失效挂载点路径加上 deleted 后缀，不去掉就匹配不上调用方给的原路径。
func isMountPointMatch(mp MountPoint, dir string) bool {
	return strings.TrimSuffix(mp.Path, "\\040(deleted)") == dir
}

// PathExists returns true if the specified path exists.
// TODO: clean this up to use pkg/util/file/FileExists
// 多看一次 access 是为了识别"stat 失败但路径仍在"的过期挂载，通用 FileExists 不做这件事。
func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	} else if errors.Is(err, fs.ErrNotExist) {
		err = syscall.Access(path, syscall.F_OK)
		if err == nil {
			// The access syscall says the file exists, the stat syscall says it
			// doesn't. This was observed on CIFS when the path was removed at
			// the server somehow. POSIX calls this a stale file handle, let's fake
			// that error and treat the path as existing but corrupted.
			// 返回 STALE 而不是普通 true，调用方据此走挂载清理流程。
			klog.Warningf("Potential stale file handle detected: %s", path)
			return true, syscall.ESTALE
		}
		return false, nil
	} else if IsCorruptedMnt(err) {
		return true, err
	}
	return false, err
}

// These variables are used solely by kernelHasMountinfoBug.
// 用 sync.Once 而非每次读内核版本：版本在进程生命周期内不会变。
var (
	hasMountinfoBug       bool
	checkMountinfoBugOnce sync.Once
)

// kernelHasMountinfoBug checks if the kernel bug that can lead to incomplete
// mountinfo being read is fixed. It does so by checking the kernel version.
//
// The bug was fixed by the kernel commit 9f6c61f96f2d97 (since Linux 5.8).
// Alas, there is no better way to check if the bug is fixed other than to
// rely on the kernel version returned by uname.
// 5.8 之前的内核可能在并发修改下给出半截 mountinfo，只能靠重复读取兜底。
// 版本解析不出来时保守认定有 bug，宁可多读几次。
func kernelHasMountinfoBug() bool {
	checkMountinfoBugOnce.Do(func() {
		// uname 失败或版本格式异常时按有 bug 处理，默认偏保守。
		// Assume old kernel.
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

// 老内核上 mountinfo 可能被并发写截断，用 ConsistentRead 多次比对；
// 新内核直接读一次，省掉重复 open/read 的开销。
func readMountInfo(path string) ([]byte, error) {
	if kernelHasMountinfoBug() {
		return utilio.ConsistentRead(path, maxListTries)
	}

	return os.ReadFile(path)
}
