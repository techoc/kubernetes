//go:build linux

/*
Copyright 2014 The Kubernetes Authors.

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
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/moby/sys/mountinfo"
	"golang.org/x/sys/unix"

	"k8s.io/klog/v2"
	utilexec "k8s.io/utils/exec"
)

const (
	// Number of fields per line in /proc/mounts as per the fstab man page.
	// 列数不符通常意味着读到了半行，宁可报错也不给出半截挂载表。
	expectedNumFieldsPerLine = 6
	// Location of the mount file to use
	// /proc/mounts 是 List() 的数据源；/etc/mtab 可能是过期符号链接，不能替代。
	procMountsPath = "/proc/mounts"
	// Location of the mountinfo file
	// mountinfo 带 root 与 major:minor，定位 bind mount 引用必须用这份文件。
	procMountInfoPath = "/proc/self/mountinfo"
	// 'fsck' found errors and corrected them
	// 1 表示错误已修好，属于可以继续挂载的退出码。
	fsckErrorsCorrected = 1
	// 'fsck' found errors but exited without correcting them
	// 4 表示仍有问题没修掉，此时必须中断挂载而不是带病上线。
	fsckErrorsUncorrected = 4
	// Error thrown by exec cmd.Run() when process spawned by cmd.Start() completes before cmd.Wait() is called (see - k/k issue #103753)
	// Start/Wait 的竞态会把成功命令也报成这个错，需要结合 ProcessState 再判一次。
	errNoChildProcesses = "wait: no child processes"
	// Error returned by some `umount` implementations when the specified path is not a mount point
	// 各实现文案不一，这里只认最常见的这一种，其余仍按真实失败处理。
	errNotMounted = "not mounted"
)

var (
	// Error statx support since Linux 4.11, https://man7.org/linux/man-pages/man2/statx.2.html
	// 单独造一个哨兵错误，调用方用 errors.Is 判断后即可回退到 stat。
	errStatxNotSupport = errors.New("the statx syscall is not supported. At least Linux kernel 4.11 is needed")
)

// Mounter provides the default implementation of mount.Interface
// for the linux platform.  This implementation assumes that the
// kubelet is running in the host's root mount namespace.
// systemd 与 umount 行为只在构造时探测一次并缓存，避免每次挂载都 fork 探测命令。
// withSystemd 用指针而非 bool，是为了区分"尚未探测"与"探测结果为无"。
// trySystemd 由构造方式固定，显式禁用时连探测都省掉。
type Mounter struct {
	mounterPath                string
	withSystemd                *bool
	trySystemd                 bool
	withSafeNotMountedBehavior bool
}

var _ MounterForceUnmounter = &Mounter{}

// New returns a mount.Interface for the current system.
// It provides options to override the default mounter behavior.
// mounterPath allows using an alternative to `/bin/mount` for mounting.
// 默认尝试 systemd scope，让 fuse 守护进程独立于 kubelet 的 cgroup 存活。
func New(mounterPath string) Interface {
	return &Mounter{
		mounterPath:                mounterPath,
		trySystemd:                 true,
		withSafeNotMountedBehavior: detectSafeNotMountedBehavior(),
	}
}

// NewWithoutSystemd returns a Linux specific mount.Interface for the current
// system. It provides options to override the default mounter behavior.
// mounterPath allows using an alternative to `/bin/mount` for mounting. Any
// detection for systemd functionality is disabled with this Mounter.
// 某些挂载不需要 fuse 守护进程，也就没必要引入 systemd-run 的不确定性。
func NewWithoutSystemd(mounterPath string) Interface {
	return &Mounter{
		mounterPath:                mounterPath,
		trySystemd:                 false,
		withSafeNotMountedBehavior: detectSafeNotMountedBehavior(),
	}
}

// hasSystemd validates that the withSystemd bool is set, if it is not,
// detectSystemd will be called once for this Mounter instance.
// 结果写回接收者供后续挂载复用；显式禁用时同步缓存，避免下次又去探测。
func (mounter *Mounter) hasSystemd() bool {
	if !mounter.trySystemd {
		mounter.withSystemd = &mounter.trySystemd
	}

	if mounter.withSystemd == nil {
		withSystemd := detectSystemd()
		mounter.withSystemd = &withSystemd
	}

	return *mounter.withSystemd
}

// Map unix.Statfs mount flags ro, nodev, noexec, nosuid, noatime, relatime,
// nodiratime to mount option flag strings.
// statfs 返回的是内核标志位，mount 命令只认字符串选项；不转换就会丢掉源端的加固属性。
func getBindMountOptions(path string, statfs func(path string, buf *unix.Statfs_t) (err error)) ([]string, error) {
	var s unix.Statfs_t
	var mountOpts []string
	if err := statfs(path, &s); err != nil {
		return nil, &os.PathError{Op: "statfs", Path: path, Err: err}
	}
	flagMapping := map[int]string{
		unix.MS_RDONLY:     "ro",
		unix.MS_NODEV:      "nodev",
		unix.MS_NOEXEC:     "noexec",
		unix.MS_NOSUID:     "nosuid",
		unix.MS_NOATIME:    "noatime",
		unix.MS_RELATIME:   "relatime",
		unix.MS_NODIRATIME: "nodiratime",
	}
	// 按位与而非相等判断：Flags 通常还带有其他与本映射无关的标志位。
	for k, v := range flagMapping {
		if int(s.Flags)&k == k {
			mountOpts = append(mountOpts, v)
		}
	}
	return mountOpts, nil
}

// Performs a bind mount with the specified options, and then remounts
// the mount point with the same `nodev`, `nosuid`, `noexec`, `nosuid`, `noatime`,
// `relatime`, `nodiratime` options as the original mount point.
// bind 本身不继承这些安全属性，必须在第二次 remount 时补回来。
// 敏感选项只进入首次挂载与敏感 remount，不会出现在普通 remount 的日志里。
func (mounter *Mounter) bindMountSensitive(mounterPath string, mountCmd string, source string, target string, fstype string, bindOpts []string, bindRemountOpts []string, bindRemountOptsSensitive []string, mountFlags []string, systemdMountRequired bool) error {
	err := mounter.doMount(mounterPath, mountCmd, source, target, fstype, bindOpts, bindRemountOptsSensitive, mountFlags, systemdMountRequired)
	if err != nil {
		return err
	}
	// Check if the source has ro, nodev, noexec, nosuid, noatime, relatime,
	// nodiratime flag...
	fixMountOpts, err := getBindMountOptions(source, unix.Statfs)
	if err != nil {
		return &os.PathError{Op: "statfs", Path: source, Err: err}
	}
	// ... and retry the mount with flags found above.
	bindRemountOpts = append(bindRemountOpts, fixMountOpts...)
	return mounter.doMount(mounterPath, mountCmd, source, target, fstype, bindRemountOpts, bindRemountOptsSensitive, mountFlags, systemdMountRequired)
}

// Mount mounts source to target as fstype with given options. 'source' and 'fstype' must
// be an empty string in case it's not required, e.g. for remount, or for auto filesystem
// type, where kernel handles fstype for you. The mount 'options' is a list of options,
// currently come from mount(8), e.g. "ro", "remount", "bind", etc. If no more option is
// required, call Mount with an empty string list or nil.
// 统一收敛到 MountSensitive，让脱敏逻辑只存在一份实现。
func (mounter *Mounter) Mount(source string, target string, fstype string, options []string) error {
	return mounter.MountSensitive(source, target, fstype, options, nil)
}

// MountSensitive is the same as Mount() but this method allows
// sensitiveOptions to be passed in a separate parameter from the normal
// mount options and ensures the sensitiveOptions are never logged. This
// method should be used by callers that pass sensitive material (like
// passwords) as mount options.
// GCI 镜像里 nfs/cifs 等用户态文件系统需要容器化 mount，其余类型直接用宿主二进制。
func (mounter *Mounter) MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	// Path to mounter binary if containerized mounter is needed. Otherwise, it is set to empty.
	// All Linux distros are expected to be shipped with a mount utility that a support bind mounts.
	mounterPath := ""
	bind, bindOpts, bindRemountOpts, bindRemountOptsSensitive := MakeBindOptsSensitive(options, sensitiveOptions)
	if bind {
		return mounter.bindMountSensitive(mounterPath, defaultMountCommand, source, target, fstype, bindOpts, bindRemountOpts, bindRemountOptsSensitive, nil /* mountFlags */, mounter.trySystemd)
	}
	// 只有这几类需要切换二进制；其余文件系统走默认路径，省掉一次无谓的替换。
	// The list of filesystems that require containerized mounter on GCI image cluster
	fsTypesNeedMounter := map[string]struct{}{
		"nfs":       {},
		"glusterfs": {},
		"ceph":      {},
		"cifs":      {},
	}
	if _, ok := fsTypesNeedMounter[fstype]; ok {
		mounterPath = mounter.mounterPath
	}
	return mounter.doMount(mounterPath, defaultMountCommand, source, target, fstype, options, sensitiveOptions, nil /* mountFlags */, mounter.trySystemd)
}

// MountSensitiveWithoutSystemd is the same as MountSensitive() but disable using systemd mount.
func (mounter *Mounter) MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	return mounter.MountSensitiveWithoutSystemdWithMountFlags(source, target, fstype, options, sensitiveOptions, nil /* mountFlags */)
}

// MountSensitiveWithoutSystemdWithMountFlags is the same as MountSensitiveWithoutSystemd with additional mount flags.
// mountFlags 走命令行位置参数，不属于 -o 选项；与敏感选项分离便于日志脱敏。
func (mounter *Mounter) MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error {
	mounterPath := ""
	bind, bindOpts, bindRemountOpts, bindRemountOptsSensitive := MakeBindOptsSensitive(options, sensitiveOptions)
	if bind {
		return mounter.bindMountSensitive(mounterPath, defaultMountCommand, source, target, fstype, bindOpts, bindRemountOpts, bindRemountOptsSensitive, mountFlags, false)
	}
	// The list of filesystems that require containerized mounter on GCI image cluster
	fsTypesNeedMounter := map[string]struct{}{
		"nfs":       {},
		"glusterfs": {},
		"ceph":      {},
		"cifs":      {},
	}
	if _, ok := fsTypesNeedMounter[fstype]; ok {
		mounterPath = mounter.mounterPath
	}
	return mounter.doMount(mounterPath, defaultMountCommand, source, target, fstype, options, sensitiveOptions, mountFlags, false)
}

// doMount runs the mount command. mounterPath is the path to mounter binary if containerized mounter is used.
// sensitiveOptions is an extension of options except they will not be logged (because they may contain sensitive material)
// systemdMountRequired is an extension of option to decide whether uses systemd mount.
// 同时持有原始参数与脱敏日志串，记录日志时无需再拼出密码。
func (mounter *Mounter) doMount(mounterPath string, mountCmd string, source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string, systemdMountRequired bool) error {
	mountArgs, mountArgsLogStr := MakeMountArgsSensitiveWithMountFlags(source, target, fstype, options, sensitiveOptions, mountFlags)
	if len(mounterPath) > 0 {
		mountArgs = append([]string{mountCmd}, mountArgs...)
		mountArgsLogStr = mountCmd + " " + mountArgsLogStr
		mountCmd = mounterPath
	}

	if systemdMountRequired && mounter.hasSystemd() {
		// Try to run mount via systemd-run --scope. This will escape the
		// service where kubelet runs and any fuse daemons will be started in a
		// specific scope. kubelet service than can be restarted without killing
		// these fuse daemons.
		//
		// Complete command line (when mounterPath is not used):
		// systemd-run --description=... --scope -- mount -t <type> <what> <where>
		//
		// Expected flow:
		// * systemd-run creates a transient scope (=~ cgroup) and executes its
		//   argument (/bin/mount) there.
		// * mount does its job, forks a fuse daemon if necessary and finishes.
		//   (systemd-run --scope finishes at this point, returning mount's exit
		//   code and stdout/stderr - thats one of --scope benefits).
		// * systemd keeps the fuse daemon running in the scope (i.e. in its own
		//   cgroup) until the fuse daemon dies (another --scope benefit).
		//   Kubelet service can be restarted and the fuse daemon survives.
		// * When the fuse daemon dies (e.g. during unmount) systemd removes the
		//   scope automatically.
		//
		// systemd-mount is not used because it's too new for older distros
		// (CentOS 7, Debian Jessie).
		// 只有调用方要求且确实探测到 systemd 时才改造命令行；否则保持原样。
		mountCmd, mountArgs, mountArgsLogStr = AddSystemdScopeSensitive("systemd-run", target, mountCmd, mountArgs, mountArgsLogStr)
		// } else {
		// No systemd-run on the host (or we failed to check it), assume kubelet
		// does not run as a systemd service.
		// No code here, mountCmd and mountArgs are already populated.
	}

	// Logging with sensitive mount options removed.
	klog.V(4).Infof("Mounting cmd (%s) with arguments (%s)", mountCmd, mountArgsLogStr)
	command := exec.Command(mountCmd, mountArgs...)
	output, err := command.CombinedOutput()
	if err != nil {
		if err.Error() == errNoChildProcesses {
			// CombinedOutput 把 Start/Wait 的竞态包装成字符串错误，只能靠 ProcessState 还原真实退出码。
			if command.ProcessState.Success() {
				// We don't consider errNoChildProcesses an error if the process itself succeeded (see - k/k issue #103753).
				return nil
			}
			// Rewrite err with the actual exit error of the process.
			err = &exec.ExitError{ProcessState: command.ProcessState}
		}
		klog.Errorf("Mount failed: %v\nMounting command: %s\nMounting arguments: %s\nOutput: %s\n", err, mountCmd, mountArgsLogStr, string(output))
		return fmt.Errorf("mount failed: %v\nMounting command: %s\nMounting arguments: %s\nOutput: %s",
			err, mountCmd, mountArgsLogStr, string(output))
	}
	return err
}

// detectSystemd returns true if OS runs with systemd as init. When not sure
// (permission errors, ...), it returns false.
// There may be different ways how to detect systemd, this one makes sure that
// systemd-runs (needed by Mount()) works.
// 探测的是"能否用 systemd-run --scope"，而非 systemd 是否安装：容器镜像常装了它但 PID 1 不是。
func detectSystemd() bool {
	if _, err := exec.LookPath("systemd-run"); err != nil {
		klog.V(2).Infof("Detected OS without systemd")
		return false
	}
	// 用 /bin/true 当探针比读 /proc/1/comm 更贴近实际用途：能在 scope 里跑起来才算可用。
	// Try to run systemd-run --scope /bin/true, that should be enough
	// to make sure that systemd is really running and not just installed,
	// which happens when running in a container with a systemd-based image
	// but with different pid 1.
	cmd := exec.Command("systemd-run", "--description=Kubernetes systemd probe", "--scope", "true")
	output, err := cmd.CombinedOutput()
	if err != nil {
		klog.V(2).Infof("Cannot run systemd-run, assuming non-systemd OS")
		klog.V(4).Infof("systemd-run output: %s, failed with: %v", string(output), err)
		return false
	}
	klog.V(2).Infof("Detected OS with systemd")
	return true
}

// detectSafeNotMountedBehavior returns true if the umount implementation replies "not mounted"
// when the specified path is not mounted. When not sure (permission errors, ...), it returns false.
// When possible, we will trust umount's message and avoid doing our own mount point checks.
// More info: https://github.com/util-linux/util-linux/blob/v2.2/mount/umount.c#L179
// 信任 umount 的应答能省掉每次卸载前的挂载点检查，但得先实测当前实现是否可靠。
func detectSafeNotMountedBehavior() bool {
	return detectSafeNotMountedBehaviorWithExec(utilexec.New())
}

// detectSafeNotMountedBehaviorWithExec is for testing with FakeExec.
// 把 exec 抽成参数，测试里就能用假命令模拟不同 umount 的输出。
func detectSafeNotMountedBehaviorWithExec(exec utilexec.Interface) bool {
	// 用确定未挂载的临时目录做探针，不碰真实挂载点。
	// create a temp dir and try to umount it
	path, err := os.MkdirTemp("", "kubelet-detect-safe-umount")
	if err != nil {
		klog.V(4).Infof("Cannot create temp dir to detect safe 'not mounted' behavior: %v", err)
		return false
	}
	defer os.RemoveAll(path)
	cmd := exec.Command("umount", path)
	output, err := cmd.CombinedOutput()
	if err != nil {
		if strings.Contains(string(output), errNotMounted) {
			klog.V(4).Infof("Detected umount with safe 'not mounted' behavior")
			return true
		}
		klog.V(4).Infof("'umount %s' failed with: %v, output: %s", path, err, string(output))
	}
	klog.V(4).Infof("Detected umount with unsafe 'not mounted' behavior")
	return false
}

// MakeMountArgs makes the arguments to the mount(8) command.
// options MUST not contain sensitive material (like passwords).
// 公开入口不收敏感参数，需要时调用方应走 Sensitive 版本。
func MakeMountArgs(source, target, fstype string, options []string) (mountArgs []string) {
	mountArgs, _ = MakeMountArgsSensitive(source, target, fstype, options, nil /* sensitiveOptions */)
	return mountArgs
}

// MakeMountArgsSensitive makes the arguments to the mount(8) command.
// sensitiveOptions is an extension of options except they will not be logged (because they may contain sensitive material)
// 两个返回值一一对应：前者执行命令，后者只用于日志。
func MakeMountArgsSensitive(source, target, fstype string, options []string, sensitiveOptions []string) (mountArgs []string, mountArgsLogStr string) {
	return MakeMountArgsSensitiveWithMountFlags(source, target, fstype, options, sensitiveOptions, nil /* mountFlags */)
}

// MakeMountArgsSensitiveWithMountFlags makes the arguments to the mount(8) command.
// sensitiveOptions is an extension of options except they will not be logged (because they may contain sensitive material)
// mountFlags are additional mount flags that are not related with the fstype
// and mount options
// mountFlags 放在命令最前，与 -t/-o 无关；日志串始终用脱敏后的选项拼装。
func MakeMountArgsSensitiveWithMountFlags(source, target, fstype string, options []string, sensitiveOptions []string, mountFlags []string) (mountArgs []string, mountArgsLogStr string) {
	// Build mount command as follows:
	//   mount [$mountFlags] [-t $fstype] [-o $options] [$source] $target
	mountArgs = []string{}
	mountArgsLogStr = ""

	mountArgs = append(mountArgs, mountFlags...)
	mountArgsLogStr += strings.Join(mountFlags, " ")

	if len(fstype) > 0 {
		mountArgs = append(mountArgs, "-t", fstype)
		mountArgsLogStr += " -t " + fstype
	}
	if len(options) > 0 || len(sensitiveOptions) > 0 {
		combinedOptions := []string{}
		combinedOptions = append(combinedOptions, options...)
		combinedOptions = append(combinedOptions, sensitiveOptions...)
		mountArgs = append(mountArgs, "-o", strings.Join(combinedOptions, ","))
		// 真实选项用于执行命令，日志里敏感项只留占位符。
		// exclude sensitiveOptions from log string
		mountArgsLogStr += " -o " + sanitizedOptionsForLogging(options, sensitiveOptions)
	}
	if len(source) > 0 {
		mountArgs = append(mountArgs, source)
		mountArgsLogStr += " " + source
	}
	mountArgs = append(mountArgs, target)
	mountArgsLogStr += " " + target

	return mountArgs, mountArgsLogStr
}

// AddSystemdScope adds "system-run --scope" to given command line
// If args contains sensitive material, use AddSystemdScopeSensitive to construct
// a safe to log string.
// description 带上挂载点，便于在 systemctl 里定位是哪个卷起了 scope。
func AddSystemdScope(systemdRunPath, mountName, command string, args []string) (string, []string) {
	descriptionArg := fmt.Sprintf("--description=Kubernetes transient mount for %s", mountName)
	systemdRunArgs := []string{descriptionArg, "--scope", "--", command}
	return systemdRunPath, append(systemdRunArgs, args...)
}

// AddSystemdScopeSensitive adds "system-run --scope" to given command line
// It also accepts takes a sanitized string containing mount arguments, mountArgsLogStr,
// and returns the string appended to the systemd command for logging.
// 与 AddSystemdScope 只差日志串：敏感信息由调用方脱敏后传进来。
func AddSystemdScopeSensitive(systemdRunPath, mountName, command string, args []string, mountArgsLogStr string) (string, []string, string) {
	descriptionArg := fmt.Sprintf("--description=Kubernetes transient mount for %s", mountName)
	systemdRunArgs := []string{descriptionArg, "--scope", "--", command}
	return systemdRunPath, append(systemdRunArgs, args...), strings.Join(systemdRunArgs, " ") + " " + mountArgsLogStr
}

// Unmount unmounts the target.
// If the mounter has safe "not mounted" behavior, no error will be returned when the target is not a mount point.
// 卸载要幂等：重复卸载同一路径不该让 kubelet 陷入错误循环。
func (mounter *Mounter) Unmount(target string) error {
	klog.V(4).Infof("Unmounting %s", target)
	command := exec.Command("umount", target)
	output, err := command.CombinedOutput()
	if err != nil {
		return checkUmountError(target, command, output, err, mounter.withSafeNotMountedBehavior)
	}
	return nil
}

// UnmountWithForce unmounts given target but will retry unmounting with force option
// after given timeout.
// 先给普通 umount 一个宽限期，超时才上 -f，避免对繁忙设备动辄强制卸载。
func (mounter *Mounter) UnmountWithForce(target string, umountTimeout time.Duration) error {
	err := tryUnmount(target, mounter.withSafeNotMountedBehavior, umountTimeout)
	if err != nil {
		// 只有超时才降级；其他错误照原样上报，不掩盖真实原因。
		if err == context.DeadlineExceeded {
			klog.V(2).Infof("Timed out waiting for unmount of %s, trying with -f", target)
			err = forceUmount(target, mounter.withSafeNotMountedBehavior)
		}
		return err
	}
	return nil
}

// List returns a list of all mounted filesystems.
// 直接解析 /proc/mounts，不调用外部命令，少一层路径依赖。
func (*Mounter) List() ([]MountPoint, error) {
	return ListProcMounts(procMountsPath)
}

// statx 能直接给出挂载根属性，比比较 st_dev 更准；ENOSYS 是内核过老的唯一正常信号。
// AT_STATX_DONT_SYNC 避免为了取属性去和 NFS 服务器同步。
func statx(file string) (unix.Statx_t, error) {
	var stat unix.Statx_t
	if err := unix.Statx(unix.AT_FDCWD, file, unix.AT_STATX_DONT_SYNC, 0, &stat); err != nil {
		if err == unix.ENOSYS {
			return stat, errStatxNotSupport
		}

		return stat, err
	}

	return stat, nil
}

// 老内核回退路径：靠目录与其父目录是否同设备来猜。bind mount 认不出来，所以只是 likely。
func (mounter *Mounter) isLikelyNotMountPointStat(file string) (bool, error) {
	stat, err := os.Stat(file)
	if err != nil {
		return true, err
	}
	rootStat, err := os.Stat(filepath.Dir(strings.TrimSuffix(file, "/")))
	if err != nil {
		return true, err
	}
	// 设备号不同即判为挂载点；同设备也可能是 bind mount，这里接受这个漏判。
	// If the directory has a different device as parent, then it is a mountpoint.
	if stat.Sys().(*syscall.Stat_t).Dev != rootStat.Sys().(*syscall.Stat_t).Dev {
		return false, nil
	}

	return true, nil
}

// 优先用 statx 的 MOUNT_ROOT 属性位；内核不提供该属性时才退回比较设备号。
func (mounter *Mounter) isLikelyNotMountPointStatx(file string) (bool, error) {
	var stat, rootStat unix.Statx_t
	var err error

	if stat, err = statx(file); err != nil {
		return true, err
	}

	// mask 为 0 表示内核没有这个属性，不能把"未置位"读成"不是挂载点"。
	if stat.Attributes_mask != 0 {
		if stat.Attributes_mask&unix.STATX_ATTR_MOUNT_ROOT != 0 {
			if stat.Attributes&unix.STATX_ATTR_MOUNT_ROOT != 0 {
				// file is a mountpoint
				return false, nil
			} else {
				// 属性位可用且未置位，结论已确定，不必再取父目录。
				// no need to check rootStat if unix.STATX_ATTR_MOUNT_ROOT supported
				return true, nil
			}
		}
	}

	root := filepath.Dir(strings.TrimSuffix(file, "/"))
	if rootStat, err = statx(root); err != nil {
		return true, err
	}

	return (stat.Dev_major == rootStat.Dev_major && stat.Dev_minor == rootStat.Dev_minor), nil
}

// IsLikelyNotMountPoint determines if a directory is not a mountpoint.
// It is fast but not necessarily ALWAYS correct. If the path is in fact
// a bind mount from one part of a mount to another it will not be detected.
// It also can not distinguish between mountpoints and symbolic links.
// mkdir /tmp/a /tmp/b; mount --bind /tmp/a /tmp/b; IsLikelyNotMountPoint("/tmp/b")
// will return true. When in fact /tmp/b is a mount point. If this situation
// is of interest to you, don't use this function...
// 先走 statx，老内核返回 ENOSYS 才回退，避免每次都做两组系统调用。
func (mounter *Mounter) IsLikelyNotMountPoint(file string) (bool, error) {
	notMountPoint, err := mounter.isLikelyNotMountPointStatx(file)
	if errors.Is(err, errStatxNotSupport) {
		// fall back to isLikelyNotMountPointStat
		return mounter.isLikelyNotMountPointStat(file)
	}

	return notMountPoint, err
}

// CanSafelySkipMountPointCheck relies on the detected behavior of umount when given a target that is not a mount point.
// "安全"指 umount 会明确报 not mounted，调用方因此可以省掉自己的预判。
func (mounter *Mounter) CanSafelySkipMountPointCheck() bool {
	return mounter.withSafeNotMountedBehavior
}

// GetMountRefs finds all mount references to pathname, returns a
// list of paths. Path could be a mountpoint or a normal
// directory (for bind mount).
// 先解析软链接再查 mountinfo：内核在 /proc 里记录的是解析后的路径。
func (mounter *Mounter) GetMountRefs(pathname string) ([]string, error) {
	pathExists, pathErr := PathExists(pathname)
	if !pathExists {
		return []string{}, nil
	} else if IsCorruptedMnt(pathErr) {
		// 失效挂载当作已卸载，别让损坏的 NFS 卡住整个卸载流程。
		klog.Warningf("GetMountRefs found corrupted mount at %s, treating as unmounted path", pathname)
		return []string{}, nil
	} else if pathErr != nil {
		return nil, fmt.Errorf("error checking path %s: %v", pathname, pathErr)
	}
	// 路径不存在时上面已返回空引用，这里失败就是真的异常。
	realpath, err := filepath.EvalSymlinks(pathname)
	if err != nil {
		return nil, err
	}
	return SearchMountPoints(realpath, procMountInfoPath)
}

// checkAndRepairFileSystem checks and repairs filesystems using command fsck.
// 只修可修复的问题；修不掉的必须中断挂载，带病上线更麻烦。
func (mounter *SafeFormatAndMount) checkAndRepairFilesystem(source string) error {
	klog.V(4).Infof("Checking for issues with fsck on disk: %s", source)
	args := []string{"-a", source}
	out, err := mounter.Exec.Command("fsck", args...).CombinedOutput()
	if err != nil {
		ee, isExitError := err.(utilexec.ExitError)
		switch {
		case err == utilexec.ErrExecutableNotFound:
			// 没装 fsck 只是能力缺失，继续挂载好过直接失败。
			klog.Warningf("'fsck' not found on system; continuing mount without running 'fsck'.")
		case isExitError && ee.ExitStatus() == fsckErrorsCorrected:
			// 已修复也要记录，方便事后追踪磁盘质量。
			klog.Infof("Device %s has errors which were corrected by fsck.", source)
		case isExitError && ee.ExitStatus() == fsckErrorsUncorrected:
			// 修不了就别挂：继续只会把坏块暴露给业务。
			return NewMountError(HasFilesystemErrors, "'fsck' found errors on device %s but could not correct them: %s", source, string(out))
		case isExitError && ee.ExitStatus() > fsckErrorsUncorrected:
			klog.Infof("`fsck` error %s", string(out))
		default:
			// fsck 自身异常时保守放行，别让探测手段故障阻塞挂载。
			klog.Warningf("fsck on device %s failed with error %v, output: %v", source, err, string(out))
		}
	}
	return nil
}

// formatAndMount uses unix utils to format and mount the given disk
// 只读卷遇到未格式化磁盘必须报错：格式化会改写数据，与 read-only 语义冲突。
// 类型不符只告警，让 mount 命令自己给出更准确的失败原因。
func (mounter *SafeFormatAndMount) formatAndMountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string, formatOptions []string) error {
	readOnly := false
	for _, option := range options {
		if option == "ro" {
			readOnly = true
			break
		}
	}
	if !readOnly {
		// ro 也可能藏在敏感选项里，漏判会导致只读卷被格式化。
		// Check sensitiveOptions for ro
		for _, option := range sensitiveOptions {
			if option == "ro" {
				readOnly = true
				break
			}
		}
	}

	// mount(8) 需要一组 -o 参数，用 defaults 占位避免拼出空选项。
	options = append(options, "defaults")
	mountErrorValue := UnknownMountError

	// Check if the disk is already formatted
	existingFormat, err := mounter.GetDiskFormat(source)
	if err != nil {
		return NewMountError(GetDiskFormatFailed, "failed to get disk format of disk %s: %v", source, err)
	}

	// 未指定类型时沿用历史默认 ext4。
	// Use 'ext4' as the default
	if len(fstype) == 0 {
		fstype = "ext4"
	}

	// 只有确认是裸盘才 mkfs；已有数据的盘绝不能顺手格式化。
	if existingFormat == "" {
		// Do not attempt to format the disk if mounting as readonly, return an error to reflect this.
		if readOnly {
			return NewMountError(UnformattedReadOnly, "cannot mount unformatted disk %s as we are manipulating it in read-only mode", source)
		}

		// Disk is unformatted so format it.
		args := []string{source}
		if fstype == "ext4" || fstype == "ext3" {
			args = []string{
				"-F",  // Force flag
				"-m0", // Zero blocks reserved for super-user
				source,
			}
		} else if fstype == "xfs" {
			args = []string{
				"-f", // force flag
				source,
			}
		}
		args = append(formatOptions, args...)

		klog.Infof("Disk %q appears to be unformatted, attempting to format as type: %q with options: %v", source, fstype, args)

		output, err := mounter.format(fstype, args)
		if err != nil {
			// Do not log sensitiveOptions only options
			sensitiveOptionsLog := sanitizedOptionsForLogging(options, sensitiveOptions)
			detailedErr := fmt.Sprintf("format of disk %q failed: type:(%q) target:(%q) options:(%q) errcode:(%v) output:(%v) ", source, fstype, target, sensitiveOptionsLog, err, string(output))
			klog.Error(detailedErr)
			return NewMountError(FormatFailed, "%s", detailedErr)
		}

		klog.Infof("Disk successfully formatted (mkfs): %s - %s %s", fstype, source, target)
	} else {
		// 类型不符只记录告警，最终的 mount 失败会带上内核侧的错误细节。
		if fstype != existingFormat {
			// Verify that the disk is formatted with filesystem type we are expecting
			mountErrorValue = FilesystemMismatch
			klog.Warningf("Configured to mount disk %s as %s but current format is %s, things might break", source, fstype, existingFormat)
		}

		// fsck 会写盘，只读卷跳过。
		if !readOnly {
			// Run check tools on the disk to fix repairable issues, only do this for formatted volumes requested as rw.
			err := mounter.checkAndRepairFilesystem(source)
			if err != nil {
				return err
			}
		}
	}

	// Mount the disk
	klog.V(4).Infof("Attempting to mount disk %s in %s format at %s", source, fstype, target)
	// 用先前记录的 mismatch 分类上报，便于上层区分类型不符与格式化失败。
	if err := mounter.MountSensitive(source, target, fstype, options, sensitiveOptions); err != nil {
		return NewMountError(mountErrorValue, "%s", err.Error())
	}

	return nil
}

// 用信号量限制同时跑的 mkfs 数量：批量挂载时几十个 mkfs 一起跑会把节点 CPU/IO 打满。
// 超时后看守 goroutine 归还槽位，避免卡死的 mkfs 永久占着并发额度。
func (mounter *SafeFormatAndMount) format(fstype string, args []string) ([]byte, error) {
	// 没配置上限时直接执行，不引入多余的 goroutine。
	if mounter.formatSem != nil {
		done := make(chan struct{})
		defer close(done)

		mounter.formatSem <- struct{}{}

		// 正常完成时 done 先关闭；只有超时才走到归还槽位的分支。
		go func() {
			defer func() { <-mounter.formatSem }()

			timeout := time.NewTimer(mounter.formatTimeout)
			defer timeout.Stop()

			select {
			case <-done:
			case <-timeout.C:
			}
		}()
	}

	// 走 Exec 接口而非直接 exec.Command，测试才能注入假命令。
	return mounter.Exec.Command("mkfs."+fstype, args...).CombinedOutput()
}

// 一次 blkid 同时取 TYPE 与 PTTYPE：有分区表说明盘上已有布局，不能当裸盘格式化。
// 退出码 2 是 blkid 的"没识别到"，不等于探测命令失败。
func getDiskFormat(exec utilexec.Interface, disk string) (string, error) {
	args := []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", disk}
	klog.V(4).Infof("Attempting to determine if disk %q is formatted using blkid with args: (%v)", disk, args)
	dataOut, err := exec.Command("blkid", args...).CombinedOutput()
	output := string(dataOut)
	klog.V(4).Infof("Output: %q", output)

	if err != nil {
		if exit, ok := err.(utilexec.ExitError); ok {
			if exit.ExitStatus() == 2 {
				// Disk device is unformatted.
				// For `blkid`, if the specified token (TYPE/PTTYPE, etc) was
				// not found, or no (specified) devices could be identified, an
				// exit code of 2 is returned.
				return "", nil
			}
		}
		klog.Errorf("Could not determine if disk %q is formatted (%v)", disk, err)
		return "", err
	}

	var fstype, pttype string

	lines := strings.Split(output, "\n")
	for _, l := range lines {
		if len(l) <= 0 {
			// Ignore empty line.
			continue
		}
		cs := strings.Split(l, "=")
		if len(cs) != 2 {
			return "", fmt.Errorf("blkid returns invalid output: %s", output)
		}
		// TYPE is filesystem type, and PTTYPE is partition table type, according
		// to https://www.kernel.org/pub/linux/utils/util-linux/v2.21/libblkid-docs/.
		if cs[0] == "TYPE" {
			fstype = cs[1]
		} else if cs[0] == "PTTYPE" {
			pttype = cs[1]
		}
	}

	// 有分区表时返回特殊非空字符串，阻止后续 mkfs 覆盖已有分区布局。
	if len(pttype) > 0 {
		klog.V(4).Infof("Disk %s detected partition table type: %s", disk, pttype)
		// Returns a special non-empty string as filesystem type, then kubelet
		// will not format it.
		return "unknown data, probably partitions", nil
	}

	return fstype, nil
}

// GetDiskFormat uses 'blkid' to see if the given disk is unformatted
// 抽成包级函数后，测试可以直接注入 FakeExec 而不必构造完整 Mounter。
func (mounter *SafeFormatAndMount) GetDiskFormat(disk string) (string, error) {
	return getDiskFormat(mounter.Exec, disk)
}

// ListProcMounts returns a list of all mounted filesystems.
// 路径参数化是为了测试能喂固定快照，生产代码只传 /proc/mounts。
func ListProcMounts(mountFilePath string) ([]MountPoint, error) {
	content, err := readMountInfo(mountFilePath)
	if err != nil {
		return nil, err
	}
	return parseProcMounts(content)
}

// 纯解析函数：只认传入的快照，不依赖解析瞬间的真实 /proc 状态。
func parseProcMounts(content []byte) ([]MountPoint, error) {
	out := []MountPoint{}
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		if line == "" {
			// 末尾换行产生的空行直接跳过。
			// the last split() item is empty string following the last \n
			continue
		}
		fields := strings.Fields(line)
		// 选项里可能带密码，错误只报列数，不回显整行。
		if len(fields) != expectedNumFieldsPerLine {
			// Do not log line in case it contains sensitive Mount options
			return nil, fmt.Errorf("wrong number of fields (expected %d, got %d)", expectedNumFieldsPerLine, len(fields))
		}

		mp := MountPoint{
			Device: fields[0],
			Path:   fields[1],
			Type:   fields[2],
			Opts:   strings.Split(fields[3], ","),
		}

		freq, err := strconv.Atoi(fields[4])
		if err != nil {
			return nil, err
		}
		mp.Freq = freq

		pass, err := strconv.Atoi(fields[5])
		if err != nil {
			return nil, err
		}
		mp.Pass = pass

		out = append(out, mp)
	}
	return out, nil
}

// SearchMountPoints finds all mount references to the source, returns a list of
// mountpoints.
// The source can be a mount point or a normal directory (bind mount). We
// didn't support device because there is no use case by now.
// Some filesystems may share a source name, e.g. tmpfs. And for bind mounting,
// it's possible to mount a non-root path of a filesystem, so we need to use
// root path and major:minor to represent mount source uniquely.
// 只按路径比对会把同一 tmpfs 的不同子挂载混为一谈，root 加 major:minor 才唯一。
func SearchMountPoints(hostSource, mountInfoPath string) ([]string, error) {
	mis, err := ParseMountInfo(mountInfoPath)
	if err != nil {
		return nil, err
	}

	mountID := 0
	rootPath := ""
	major := -1
	minor := -1

	// Finding the underlying root path and major:minor if possible.
	// We need search in backward order because it's possible for later mounts
	// 后挂载的条目会盖住先挂载的，倒序才能定位当前生效的那一层。
	// to overlap earlier mounts.
	for i := len(mis) - 1; i >= 0; i-- {
		if hostSource == mis[i].MountPoint || PathWithinBase(hostSource, mis[i].MountPoint) {
			// If it's a mount point or path under a mount point.
			mountID = mis[i].ID
			rootPath = filepath.Join(mis[i].Root, strings.TrimPrefix(hostSource, mis[i].MountPoint))
			major = mis[i].Major
			minor = mis[i].Minor
			break
		}
	}

	// 三者缺一就无法唯一标识源设备，宁可报错也不返回可能错误的引用。
	if rootPath == "" || major == -1 || minor == -1 {
		return nil, fmt.Errorf("failed to get root path and major:minor for %s", hostSource)
	}

	var refs []string
	for i := range mis {
		// 调用方要的是指向源的引用，源挂载本身不算。
		if mis[i].ID == mountID {
			// Ignore mount entry for mount source itself.
			continue
		}
		if mis[i].Root == rootPath && mis[i].Major == major && mis[i].Minor == minor {
			refs = append(refs, mis[i].MountPoint)
		}
	}

	return refs, nil
}

// IsMountPoint determines if a file is a mountpoint.
// It first detects bind & any other mountpoints using
// MountedFast function. If the MountedFast function returns
// sure as true and err as nil, then a mountpoint is detected
// successfully. When an error is returned by MountedFast, the
// following is true:
// 1. All errors are returned with IsMountPoint as false
// except os.IsPermission.
// 2. When os.IsPermission is returned by MountedFast, List()
// is called to confirm if the given file is a mountpoint are not.
//
// os.ErrNotExist should always be returned if a file does not exist
// as callers have in past relied on this error and not fallback.
//
// When MountedFast returns sure as false and err as nil (eg: in
// case of bindmounts on kernel version 5.10- ); mounter.List()
// endpoint is called to enumerate all the mountpoints and check if
// MountedFast 对 bind mount 等情形给不出确定答案；不确定的一律回退到 /proc/mounts 全表扫描。
// os.ErrNotExist 必须原样返回，历史调用方依赖它判断要不要创建目录。
// it is mountpoint match or not.
func (mounter *Mounter) IsMountPoint(file string) (bool, error) {
	isMnt, sure, isMntErr := mountinfo.MountedFast(file)
	if sure && isMntErr == nil {
		return isMnt, nil
	}
	if isMntErr != nil {
		// 路径不存在就不做回退，保持与历史行为一致。
		if errors.Is(isMntErr, fs.ErrNotExist) {
			return false, fs.ErrNotExist
		}
		// We were not allowed to do the simple stat() check, e.g. on NFS with
		// root_squash. Fall back to /proc/mounts check below when
		// fs.ErrPermission is returned.
		// 权限之外的失败说明探测本身不可用，回退到列表也无济于事。
		if !errors.Is(isMntErr, fs.ErrPermission) {
			return false, isMntErr
		}
	}
	// /proc/mounts 里是内核解析后的路径，不解析就会漏配。
	// Resolve any symlinks in file, kernel would do the same and use the resolved path in /proc/mounts.
	resolvedFile, err := filepath.EvalSymlinks(file)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, fs.ErrNotExist
		}
		return false, err
	}

	// 全表扫描慢，但能覆盖 bind mount 和被 rename 过的挂载点，是最后的兜底。
	// check all mountpoints since MountedFast is not sure.
	// is not reliable for some mountpoint types.
	mountPoints, mountPointsErr := mounter.List()
	if mountPointsErr != nil {
		return false, mountPointsErr
	}
	for _, mp := range mountPoints {
		if isMountPointMatch(mp, resolvedFile) {
			return true, nil
		}
	}
	return false, nil
}

// tryUnmount calls plain "umount" and waits for unmountTimeout for it to finish.
// 超时得自己返回 DeadlineExceeded：CombinedOutput 不会把 ctx 超时透传出来。
func tryUnmount(target string, withSafeNotMountedBehavior bool, unmountTimeout time.Duration) error {
	klog.V(4).Infof("Unmounting %s", target)
	ctx, cancel := context.WithTimeout(context.Background(), unmountTimeout)
	defer cancel()

	command := exec.CommandContext(ctx, "umount", target)
	output, err := command.CombinedOutput()

	// CombinedOutput() does not return DeadlineExceeded, make sure it's
	// propagated on timeout.
	if ctx.Err() != nil {
		return ctx.Err()
	}

	if err != nil {
		return checkUmountError(target, command, output, err, withSafeNotMountedBehavior)
	}
	return nil
}

// 强制卸载只在普通卸载超时后使用，避免对仍被占用的卷造成数据风险。
func forceUmount(target string, withSafeNotMountedBehavior bool) error {
	command := exec.Command("umount", "-f", target)
	output, err := command.CombinedOutput()
	if err != nil {
		return checkUmountError(target, command, output, err, withSafeNotMountedBehavior)
	}
	return nil
}

// checkUmountError checks a result of umount command and determine a return value.
// 把"目标本就没挂载"从错误里摘出来，让重复卸载保持幂等。
func checkUmountError(target string, command *exec.Cmd, output []byte, err error, withSafeNotMountedBehavior bool) error {
	// 与挂载路径同样的竞态，先看 ProcessState 再决定是否转成真实退出错误。
	if err.Error() == errNoChildProcesses {
		if command.ProcessState.Success() {
			// We don't consider errNoChildProcesses an error if the process itself succeeded (see - k/k issue #103753).
			return nil
		}
		// Rewrite err with the actual exit error of the process.
		err = &exec.ExitError{ProcessState: command.ProcessState}
	}
	// 只有实测确认 umount 会报 not mounted，才敢忽略这条错误。
	if withSafeNotMountedBehavior && strings.Contains(string(output), errNotMounted) {
		klog.V(4).Infof("ignoring 'not mounted' error for %s", target)
		return nil
	}
	return fmt.Errorf("unmount failed: %v\nUnmounting arguments: %s\nOutput: %s", err, target, string(output))
}
