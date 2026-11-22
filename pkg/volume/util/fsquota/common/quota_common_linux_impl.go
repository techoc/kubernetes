//go:build linux

/*
Copyright 2018 The Kubernetes Authors.

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

package common

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"k8s.io/klog/v2"
)

var quotaCmd string
var quotaCmdInitialized bool
var quotaCmdLock sync.RWMutex

// 若将来出现采用非 XFS 项目配额语义的文件系统，需要修改此处。
// 上层无需感知内部细节。
type linuxFilesystemType struct {
	name      string
	typeMagic int64 // 文件系统魔数，见 statfs(2)
	maxQuota  int64
	// 是否接受 "quota" 命令输出为空。
	allowEmptyOutput bool
}

const (
	bitsPerWord = 32 << (^uint(0) >> 63) // 32 或 64
)

var (
	linuxSupportedFilesystems = []linuxFilesystemType{
		{
			name:      "XFS",
			typeMagic: 0x58465342,
			maxQuota:  1<<(bitsPerWord-1) - 1,
			// 未设置配额时 XFS 不输出任何内容。
			allowEmptyOutput: true,
		}, {
			name:      "ext4fs",
			typeMagic: 0xef53,
			maxQuota:  (1<<(bitsPerWord-1) - 1) & (1<<58 - 1),
			// 即使未设置配额，ext4 也总会输出内容。
			allowEmptyOutput: false,
		},
	}
)

// VolumeProvider 向通用代码提供配额施加器。
type VolumeProvider struct {
}

var quotaCmds = []string{
	"/sbin/xfs_quota",
	"/usr/sbin/xfs_quota",
	"/bin/xfs_quota",
}

var quotaParseRegexp = regexp.MustCompilePOSIX("^[^ \t]*[ \t]*([0-9]+)")

var lsattrCmd = "/usr/bin/lsattr"
var lsattrParseRegexp = regexp.MustCompilePOSIX("^ *([0-9]+) [^ ]+ (.*)$")

// GetQuotaApplier 判断该块设备是否支持可施加到目录上的配额；
// 支持时返回对应的配额施加器。
func (*VolumeProvider) GetQuotaApplier(mountpoint string, backingDev string) LinuxVolumeQuotaApplier {
	for _, fsType := range linuxSupportedFilesystems {
		if isFilesystemOfType(mountpoint, backingDev, fsType.typeMagic) {
			return linuxVolumeQuotaApplier{mountpoint: mountpoint,
				maxQuota:         fsType.maxQuota,
				allowEmptyOutput: fsType.allowEmptyOutput,
			}
		}
	}
	return nil
}

// linuxVolumeQuotaApplier 在 Linux 文件系统上通过 xfs_quota 施加和查询项目配额。
type linuxVolumeQuotaApplier struct {
	mountpoint       string
	maxQuota         int64
	allowEmptyOutput bool
}

// getXFSQuotaCmd 查找并返回可用的 xfs_quota 可执行文件，结果带缓存。
func getXFSQuotaCmd() (string, error) {
	quotaCmdLock.Lock()
	defer quotaCmdLock.Unlock()
	if quotaCmdInitialized {
		return quotaCmd, nil
	}
	for _, program := range quotaCmds {
		fileinfo, err := os.Stat(program)
		if err == nil && ((fileinfo.Mode().Perm() & (1 << 6)) != 0) {
			klog.V(3).Infof("Found xfs_quota program %s", program)
			quotaCmd = program
			quotaCmdInitialized = true
			return quotaCmd, nil
		}
	}
	quotaCmdInitialized = true
	return "", fmt.Errorf("no xfs_quota program found")
}

// doRunXFSQuotaCommand 使用指定的 mounts 文件执行 xfs_quota 命令。
func doRunXFSQuotaCommand(mountpoint string, mountsFile, command string) (string, error) {
	quotaCmd, err := getXFSQuotaCmd()
	if err != nil {
		return "", err
	}
	// 直接使用数字形式的项目 ID，无需扫描 /etc/projects 或 /etc/projid。
	klog.V(4).Infof("runXFSQuotaCommand %s -t %s -P/dev/null -D/dev/null -x -f %s -c %s", quotaCmd, mountsFile, mountpoint, command)
	cmd := exec.Command(quotaCmd, "-t", mountsFile, "-P/dev/null", "-D/dev/null", "-x", "-f", mountpoint, "-c", command)

	data, err := cmd.Output()
	if err != nil {
		return "", err
	}
	klog.V(4).Infof("runXFSQuotaCommand output %q", string(data))
	return string(data), nil
}

// 把关心的挂载点提取到临时 mounts 文件，避免 xfs_quota 扫描文件系统上的所有挂载；
// 例如存在卡住的 NFS 挂载时，全量扫描可能挂起。
// 相关问题的实例参见 https://bugzilla.redhat.com/show_bug.cgi?id=237120 。
func runXFSQuotaCommand(mountpoint string, command string) (string, error) {
	tmpMounts, err := os.CreateTemp("", "mounts")
	if err != nil {
		return "", fmt.Errorf("cannot create temporary mount file: %v", err)
	}
	tmpMountsFileName := tmpMounts.Name()
	defer tmpMounts.Close()
	defer os.Remove(tmpMountsFileName)

	mounts, err := os.Open(MountsFile)
	if err != nil {
		return "", fmt.Errorf("cannot open mounts file %s: %v", MountsFile, err)
	}
	defer mounts.Close()

	scanner := bufio.NewScanner(mounts)
	for scanner.Scan() {
		match := MountParseRegexp.FindStringSubmatch(scanner.Text())
		if match != nil {
			mount := match[2]
			if mount == mountpoint {
				if _, err := tmpMounts.WriteString(fmt.Sprintf("%s\n", scanner.Text())); err != nil {
					return "", fmt.Errorf("cannot write temporary mounts file: %v", err)
				}
				if err := tmpMounts.Sync(); err != nil {
					return "", fmt.Errorf("cannot sync temporary mounts file: %v", err)
				}
				return doRunXFSQuotaCommand(mountpoint, tmpMountsFileName, command)
			}
		}
	}
	return "", fmt.Errorf("cannot run xfs_quota: cannot find mount point %s in %s", mountpoint, MountsFile)
}

// SupportsQuotas 判断文件系统是否支持指定类型的配额。
func SupportsQuotas(mountpoint string, qType QuotaType) (bool, error) {
	data, err := runXFSQuotaCommand(mountpoint, "state -p")
	if err != nil {
		return false, err
	}
	if qType == FSQuotaEnforcing {
		return strings.Contains(data, "Enforcement: ON"), nil
	}
	return strings.Contains(data, "Accounting: ON"), nil
}

// isFilesystemOfType 判断挂载点是否为指定魔数对应的文件系统，且支持配额记账。
func isFilesystemOfType(mountpoint string, backingDev string, typeMagic int64) bool {
	var buf syscall.Statfs_t
	err := syscall.Statfs(mountpoint, &buf)
	if err != nil {
		klog.Warningf("Unable to statfs %s: %v", mountpoint, err)
		return false
	}
	if int64(buf.Type) != typeMagic {
		return false
	}
	if answer, _ := SupportsQuotas(mountpoint, FSQuotaAccounting); answer {
		return true
	}
	return false
}

// GetQuotaOnDir 获取与指定目录关联的配额 ID（若有）。
// 若无法执行系统调用，只能回答“未知”，由上层决定如何处理。
func (v linuxVolumeQuotaApplier) GetQuotaOnDir(path string) (QuotaID, error) {
	cmd := exec.Command(lsattrCmd, "-pd", path)
	data, err := cmd.Output()
	if err != nil {
		return BadQuotaID, fmt.Errorf("cannot run lsattr: %v", err)
	}
	match := lsattrParseRegexp.FindStringSubmatch(string(data))
	if match == nil {
		return BadQuotaID, fmt.Errorf("unable to parse lsattr -pd %s output %s", path, string(data))
	}
	if match[2] != path {
		return BadQuotaID, fmt.Errorf("mismatch between supplied and returned path (%s != %s)", path, match[2])
	}
	projid, err := strconv.ParseInt(match[1], 10, 32)
	if err != nil {
		return BadQuotaID, fmt.Errorf("unable to parse project ID from %s (%v)", match[1], err)
	}
	return QuotaID(projid), nil
}

// SetQuotaOnDir 对指定挂载点下的目录施加配额。
func (v linuxVolumeQuotaApplier) SetQuotaOnDir(path string, id QuotaID, bytes int64) error {
	if bytes < 0 || bytes > v.maxQuota {
		bytes = v.maxQuota
	}
	_, err := runXFSQuotaCommand(v.mountpoint, fmt.Sprintf("limit -p bhard=%v bsoft=%v %v", bytes, bytes, id))
	if err != nil {
		return err
	}

	_, err = runXFSQuotaCommand(v.mountpoint, fmt.Sprintf("project -s -p %s %v", path, id))
	return err
}

// getQuantity 查询指定配额 ID 的用量，并按 multiplier 换算单位。
func getQuantity(mountpoint string, id QuotaID, xfsQuotaArg string, multiplier int64, allowEmptyOutput bool) (int64, error) {
	data, err := runXFSQuotaCommand(mountpoint, fmt.Sprintf("quota -p -N -n -v %s %v", xfsQuotaArg, id))
	if err != nil {
		return 0, fmt.Errorf("unable to run xfs_quota: %v", err)
	}
	if data == "" && allowEmptyOutput {
		return 0, nil
	}
	match := quotaParseRegexp.FindStringSubmatch(data)
	if match == nil {
		return 0, fmt.Errorf("unable to parse quota output '%s'", data)
	}
	size, err := strconv.ParseInt(match[1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("unable to parse data size '%s' from '%s': %v", match[1], data, err)
	}
	klog.V(4).Infof("getQuantity %s %d %s %d => %d %v", mountpoint, id, xfsQuotaArg, multiplier, size, err)
	return size * multiplier, nil
}

// GetConsumption 通过配额返回已用字节数（若可用）。
func (v linuxVolumeQuotaApplier) GetConsumption(_ string, id QuotaID) (int64, error) {
	return getQuantity(v.mountpoint, id, "-b", 1024, v.allowEmptyOutput)
}

// GetInodes 通过配额返回已用 inode 数（若可用）。
func (v linuxVolumeQuotaApplier) GetInodes(_ string, id QuotaID) (int64, error) {
	return getQuantity(v.mountpoint, id, "-i", 1, v.allowEmptyOutput)
}

// QuotaIDIsInUse 判断指定配额 ID 在文件系统上是否仍被使用。
func (v linuxVolumeQuotaApplier) QuotaIDIsInUse(id QuotaID) (bool, error) {
	bytes, err := v.GetConsumption(v.mountpoint, id)
	if err != nil {
		return false, err
	}
	if bytes > 0 {
		return true, nil
	}
	inodes, err := v.GetInodes(v.mountpoint, id)
	return inodes > 0, err
}
