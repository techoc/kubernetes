//go:build windows

/*
Copyright 2017 The Kubernetes Authors.

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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"k8s.io/klog/v2"
	"k8s.io/utils/keymutex"
)

const (
	// 用于匹配底层错误文本，判断远程挂载是否因凭据问题失效。
	accessDenied string = "access is denied"
)

// Mounter 是 Windows 平台的默认 mount.Interface 实现。
// Windows 上挂载靠符号链接与 SMB 全局映射实现，不依赖 Linux 的挂载命名空间。
type Mounter struct {
	mounterPath string
}

// New 返回当前系统的 mount.Interface，并允许覆盖默认 mounter 行为。
// Windows 实现不使用 mounterPath，仍接收该参数以保持与各平台一致的签名。
func New(mounterPath string) Interface {
	return &Mounter{
		mounterPath: mounterPath,
	}
}

// getSMBMountMutex 按源路径加锁，避免并发对同一 SMB 共享重复建立映射。
var getSMBMountMutex = keymutex.NewHashed(0)

// Mount 使用给定选项将 source 挂载到 target，支持 CIFS（SMB）和磁盘绑定挂载。
// 敏感选项走单独入口，普通挂载直接委托给它。
func (mounter *Mounter) Mount(source string, target string, fstype string, options []string) error {
	return mounter.MountSensitive(source, target, fstype, options, nil /* sensitiveOptions */)
}

// MountSensitiveWithoutSystemd 与 MountSensitive 相同，但不使用 systemd 挂载。
// Windows 没有 systemd 挂载路径，降级转发并保留敏感选项即可。
func (mounter *Mounter) MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	return mounter.MountSensitive(source, target, fstype, options, sensitiveOptions /* sensitiveOptions */)
}

// MountSensitiveWithoutSystemdWithMountFlags 在 MountSensitiveWithoutSystemd 基础上附加挂载标志。
// Windows 没有 systemd 挂载路径，也不使用这些标志，降级转发并保留敏感选项。
func (mounter *Mounter) MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error {
	return mounter.MountSensitive(source, target, fstype, options, sensitiveOptions /* sensitiveOptions */)
}

// MountSensitive 与 Mount 相同，但 sensitiveOptions 单独传递且绝不写入日志。
// 调用方应将口令等敏感材料放入 sensitiveOptions；日志仅使用脱敏选项，口令经环境变量传给 PowerShell。
func (mounter *Mounter) MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	target = NormalizeWindowsPath(target)
	sanitizedOptionsForLogging := sanitizedOptionsForLogging(options, sensitiveOptions)

	if source == "tmpfs" {
		klog.V(3).Infof("mounting source (%q), target (%q), with options (%q)", source, target, sanitizedOptionsForLogging)
		return os.MkdirAll(target, 0o755)
	}

	parentDir := filepath.Dir(target)
	if err := os.MkdirAll(parentDir, 0o755); err != nil {
		return err
	}

	klog.V(4).Infof("mount options(%q) source:%q, target:%q, fstype:%q, begin to mount",
		sanitizedOptionsForLogging, source, target, fstype)
	bindSource := source

	if bind, _, _, _ := MakeBindOptsSensitive(options, sensitiveOptions); bind {
		bindSource = NormalizeWindowsPath(source)
	} else {
		allOptions := []string{}
		allOptions = append(allOptions, options...)
		allOptions = append(allOptions, sensitiveOptions...)
		if len(allOptions) < 2 {
			return fmt.Errorf("mount options(%q) should have at least 2 options, current number:%d, source:%q, target:%q",
				sanitizedOptionsForLogging, len(allOptions), source, target)
		}

		// 目前仅支持 CIFS 挂载。
		// 非 CIFS 类型没有对应的映射命令，提前失败比静默建错映射更清楚。
		if strings.ToLower(fstype) != "cifs" {
			return fmt.Errorf("only cifs mount is supported now, fstype: %q, mounting source (%q), target (%q), with options (%q)", fstype, source, target, sanitizedOptionsForLogging)
		}

		// 同一共享的映射检查与重建必须串行，避免并发互相覆盖。
		getSMBMountMutex.LockKey(source)
		defer getSMBMountMutex.UnlockKey(source)

		username := allOptions[0]
		password := allOptions[1]
		if output, err := newSMBMapping(username, password, source); err != nil {
			klog.Warningf("SMB Mapping(%s) returned with error(%v), output(%s)", source, err, string(output))
			if isSMBMappingExist(source) {
				valid, err := isValidPath(source)
				if !valid {
					if err == nil || isAccessDeniedError(err) {
						klog.V(2).Infof("SMB Mapping(%s) already exists while it's not valid, return error: %v, now begin to remove and remount", source, err)
						if output, err = removeSMBMapping(source); err != nil {
							return fmt.Errorf("Remove-SmbGlobalMapping failed: %v, output: %q", err, output)
						}
						if output, err := newSMBMapping(username, password, source); err != nil {
							return fmt.Errorf("New-SmbGlobalMapping(%s) failed: %v, output: %q", source, err, output)
						}
					}
				} else {
					klog.V(2).Infof("SMB Mapping(%s) already exists and is still valid, skip error(%v)", source, err)
				}
			} else {
				return fmt.Errorf("New-SmbGlobalMapping(%s) failed: %v, output: %q", source, err, output)
			}
		}
	}

	// Go 的 EvalSymlinks 曾无法解析不带结尾反斜杠的 UNC 共享根路径。
	// 例如 \\SERVER\share 解析失败，而 \\SERVER\share\ 可以解析。
	// Windows 上的 containerD 会调用 EvalSymlinks，因此创建软链接前需补齐反斜杠。
	// 该问题由 https://github.com/golang/go/pull/42096 修复，并随 Go 1.16 发布。
	mklinkSource := bindSource
	if !strings.HasSuffix(mklinkSource, "\\") {
		mklinkSource = mklinkSource + "\\"
	}

	err := os.Symlink(mklinkSource, target)
	if err != nil {
		klog.Errorf("symlink failed: %v, source(%q) target(%q)", err, mklinkSource, target)
		return err
	}
	klog.V(2).Infof("symlink source(%q) on target(%q) successfully", mklinkSource, target)

	return nil
}

// newSMBMapping 使用用户名、口令和远程路径建立 SMB 映射，返回命令输出与错误。
// 用户名与口令放入环境变量而非命令行参数，防止被进程列表或日志泄露。
func newSMBMapping(username, password, remotepath string) (string, error) {
	if username == "" || password == "" || remotepath == "" {
		return "", fmt.Errorf("invalid parameter(username: %s, password: %s, remotepath: %s)", username, sensitiveOptionsRemoved, remotepath)
	}

	// 通过 PowerShell 环境变量传递用户输入，避免命令行注入。
	// https://docs.microsoft.com/en-us/powershell/module/microsoft.powershell.core/about/about_environment_variables?view=powershell-5.1
	// 环境变量传值也避免凭据出现在命令行中，降低泄露风险。
	cmdLine := `$PWord = ConvertTo-SecureString -String $Env:smbpassword -AsPlainText -Force` +
		`;$Credential = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $Env:smbuser, $PWord` +
		`;New-SmbGlobalMapping -RemotePath $Env:smbremotepath -Credential $Credential -RequirePrivacy $true`
	cmd := exec.Command("powershell", "/c", cmdLine)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("smbuser=%s", username),
		fmt.Sprintf("smbpassword=%s", password),
		fmt.Sprintf("smbremotepath=%s", remotepath))

	output, err := cmd.CombinedOutput()
	return string(output), err
}

// isSMBMappingExist 检查远程路径是否已经建立 SMB 映射，避免重复建立导致冲突。
func isSMBMappingExist(remotepath string) bool {
	cmd := exec.Command("powershell", "/c", `Get-SmbGlobalMapping -RemotePath $Env:smbremotepath`)
	cmd.Env = append(os.Environ(), fmt.Sprintf("smbremotepath=%s", remotepath))
	_, err := cmd.CombinedOutput()
	return err == nil
}

// isValidPath 检查远程路径是否有效，有效时返回 true 和 nil。
// 已有映射出错时用它区分凭据失效与路径本身不可达。
func isValidPath(remotepath string) (bool, error) {
	cmd := exec.Command("powershell", "/c", `Test-Path $Env:remotepath`)
	cmd.Env = append(os.Environ(), fmt.Sprintf("remotepath=%s", remotepath))
	output, err := cmd.CombinedOutput()
	if err != nil {
		return false, fmt.Errorf("returned output: %s, error: %v", string(output), err)
	}

	return strings.HasPrefix(strings.ToLower(string(output)), "true"), nil
}

// 用小写文本匹配判断是否为拒绝访问错误，兼容 Windows 错误文案的大小写差异。
func isAccessDeniedError(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), accessDenied)
}

// removeSMBMapping 删除指定远程路径的 SMB 映射，返回命令输出与错误。
// 凭据变更时先清掉旧映射，再按新凭据重建。
func removeSMBMapping(remotepath string) (string, error) {
	cmd := exec.Command("powershell", "/c", `Remove-SmbGlobalMapping -RemotePath $Env:smbremotepath -Force`)
	cmd.Env = append(os.Environ(), fmt.Sprintf("smbremotepath=%s", remotepath))
	output, err := cmd.CombinedOutput()
	return string(output), err
}

// Unmount 卸载目标。
// Windows 上的挂载是符号链接，卸载即删除该链接。
func (mounter *Mounter) Unmount(target string) error {
	klog.V(4).Infof("Unmount target (%q)", target)
	target = NormalizeWindowsPath(target)

	if err := os.Remove(target); err != nil {
		klog.Errorf("removing directory %s failed: %v", target, err)
		return err
	}
	return nil
}

// List 返回已挂载文件系统列表。
// Windows 下缺少统一的挂载表查询接口，故当前始终返回空列表。
func (mounter *Mounter) List() ([]MountPoint, error) {
	return []MountPoint{}, nil
}

// IsLikelyNotMountPoint 判断目录是否可能不是挂载点。
// 通过符号链接位判断；特殊文件也视为挂载点，以免误卸载。
func (mounter *Mounter) IsLikelyNotMountPoint(file string) (bool, error) {
	stat, err := os.Lstat(file)
	if err != nil {
		return true, err
	}

	if stat.Mode()&os.ModeSymlink != 0 {
		return false, err
	}
	// Go 1.23 行为变更：https://github.com/golang/go/issues/63703#issuecomment-2535941458
	// 适配 Go 1.23 对 Windows 特殊文件模式位的变更，避免误判为普通目录。
	if stat.Mode()&os.ModeIrregular != 0 {
		return false, err
	}
	return true, nil
}

// CanSafelySkipMountPointCheck 表示是否可以安全跳过挂载点检查。
// Windows 无法保证跳过检查安全，故始终返回 false，要求调用方进行探测。
func (mounter *Mounter) CanSafelySkipMountPointCheck() bool {
	return false
}

// IsMountPoint 判断目录是否为挂载点。
// 复用 IsLikelyNotMountPoint 的反向判定，保持两个入口结果一致。
func (mounter *Mounter) IsMountPoint(file string) (bool, error) {
	isNotMnt, err := mounter.IsLikelyNotMountPoint(file)
	if err != nil {
		return false, err
	}
	return !isNotMnt, nil
}

// GetMountRefs 返回给定路径的挂载引用。
// Windows 无全局挂载表可查，有效路径只返回自身；不存在或损坏的路径返回空列表。
func (mounter *Mounter) GetMountRefs(pathname string) ([]string, error) {
	windowsPath := NormalizeWindowsPath(pathname)
	pathExists, pathErr := PathExists(windowsPath)
	if !pathExists {
		return []string{}, nil
	} else if IsCorruptedMnt(pathErr) {
		klog.Warningf("GetMountRefs found corrupted mount at %s, treating as unmounted path", windowsPath)
		return []string{}, nil
	} else if pathErr != nil {
		return nil, fmt.Errorf("error checking path %s: %v", windowsPath, pathErr)
	}
	return []string{pathname}, nil
}

// SafeFormatAndMount.formatAndMountSensitive 先初始化并格式化裸盘，再挂载到目标路径。
// 磁盘号会传给 PowerShell，必须先校验格式；敏感选项继续单独传递。
func (mounter *SafeFormatAndMount) formatAndMountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string, formatOptions []string) error {
	// 尝试格式化并挂载磁盘。
	klog.V(4).Infof("Attempting to formatAndMount disk: %s %s %s", fstype, source, target)

	if err := ValidateDiskNumber(source); err != nil {
		klog.Errorf("diskMount: formatAndMount failed, err: %v", err)
		return err
	}

	if len(fstype) == 0 {
		// 未指定文件系统时使用 NTFS，避免格式化命令缺少参数。
		fstype = "NTFS"
	}

	if len(formatOptions) > 0 {
		return fmt.Errorf("diskMount: formatOptions are not supported on Windows")
	}

	cmdString := "Get-Disk -Number $env:source | Where partitionstyle -eq 'raw' | Initialize-Disk -PartitionStyle GPT -PassThru" +
		" | New-Partition -UseMaximumSize | Format-Volume -FileSystem $env:fstype -Confirm:$false"
	cmd := mounter.Exec.Command("powershell", "/c", cmdString)
	env := append(os.Environ(),
		fmt.Sprintf("source=%s", source),
		fmt.Sprintf("fstype=%s", fstype),
	)
	cmd.SetEnv(env)
	klog.V(8).Infof("Executing command: %q", cmdString)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("diskMount: format disk failed, error: %v, output: %q", err, string(output))
	}
	klog.V(4).Infof("diskMount: Disk successfully formatted, disk: %q, fstype: %q", source, fstype)

	volumeIds, err := ListVolumesOnDisk(source)
	if err != nil {
		return err
	}
	driverPath := volumeIds[0]
	return mounter.MountSensitive(driverPath, target, fstype, options, sensitiveOptions)
}

// ListVolumesOnDisk 返回指定磁盘上的卷 ID 列表，供上层选定实际挂载路径。
func ListVolumesOnDisk(diskID string) (volumeIDs []string, err error) {
	// 同一磁盘有多个卷时，Get-Volume 的返回顺序可能变化。
	// 按 UniqueId 排序保证多次调用顺序稳定，避免选中错误的卷。
	cmd := exec.Command("powershell", "/c", "(Get-Disk -DeviceId $env:diskID | Get-Partition | Get-Volume | Sort-Object -Property UniqueId).UniqueId")
	cmd.Env = append(os.Environ(), fmt.Sprintf("diskID=%s", diskID))
	klog.V(8).Infof("Executing command: %q", cmd.String())
	output, err := cmd.CombinedOutput()
	klog.V(4).Infof("ListVolumesOnDisk id from %s: %s", diskID, string(output))
	if err != nil {
		return []string{}, fmt.Errorf("error list volumes on disk. cmd: %s, output: %s, error: %v", cmd, string(output), err)
	}

	volumeIds := strings.Split(strings.TrimSpace(string(output)), "\r\n")
	return volumeIds, nil
}
