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
	// 按照 fstab 手册，/proc/mounts 每行应包含的字段数。
	// 列数不符通常意味着读到了半行，宁可报错也不给出半截挂载表。
	expectedNumFieldsPerLine = 6
	// 所用挂载文件的位置；/proc/mounts 是 List() 的数据源，/etc/mtab 可能是过期符号链接，不能替代。
	procMountsPath = "/proc/mounts"
	// mountinfo 文件的位置；其中带 root 与 major:minor，定位 bind mount 引用必须用这份文件。
	procMountInfoPath = "/proc/self/mountinfo"
	// fsck 发现错误并已修复的退出码；1 表示可以继续挂载。
	fsckErrorsCorrected = 1
	// fsck 发现错误但未修复的退出码；4 表示必须中断挂载，不能带病上线。
	fsckErrorsUncorrected = 4
	// cmd.Start() 启动的进程先于 cmd.Wait() 完成时，exec cmd.Run() 返回的错误（参见 k/k issue #103753）。
	// Start/Wait 的竞态会把成功命令也报成这个错，需要结合 ProcessState 再判一次。
	errNoChildProcesses = "wait: no child processes"
	// 某些 umount 实现发现指定路径不是挂载点时返回的错误文本。
	// 各实现文案不一，这里只认最常见的这一种，其余仍按真实失败处理。
	errNotMounted = "not mounted"
)

var (
	// statx 支持错误：至少需要 Linux 4.11，https://man7.org/linux/man-pages/man2/statx.2.html。
	// 单独造一个哨兵错误，调用方用 errors.Is 判断后即可回退到 stat。
	errStatxNotSupport = errors.New("the statx syscall is not supported. At least Linux kernel 4.11 is needed")
)

// Mounter 是 mount.Interface 在 Linux 平台上的默认实现，假设 kubelet 运行在宿主机的根挂载命名空间中。
// systemd 与 umount 行为只在构造时探测一次并缓存，避免每次挂载都 fork 探测命令。
// withSystemd 用指针而非 bool，是为了区分“尚未探测”与“探测结果为无”。
// trySystemd 由构造方式固定，显式禁用时连探测都省掉。
type Mounter struct {
	mounterPath                string
	withSystemd                *bool
	trySystemd                 bool
	withSafeNotMountedBehavior bool
}

var _ MounterForceUnmounter = &Mounter{}

// New 返回当前系统使用的 mount.Interface，可覆盖默认挂载器行为。
// mounterPath 允许使用 /bin/mount 的替代程序执行挂载。
// 默认尝试 systemd scope，让 fuse 守护进程独立于 kubelet 的 cgroup 存活。
func New(mounterPath string) Interface {
	return &Mounter{
		mounterPath:                mounterPath,
		trySystemd:                 true,
		withSafeNotMountedBehavior: detectSafeNotMountedBehavior(),
	}
}

// NewWithoutSystemd 返回当前系统使用的 Linux 专用 mount.Interface，可覆盖默认挂载器行为。
// mounterPath 允许使用 /bin/mount 的替代程序执行挂载；此 Mounter 禁用所有 systemd 功能探测。
// 某些挂载不需要 fuse 守护进程，也就没必要引入 systemd-run 的不确定性。
func NewWithoutSystemd(mounterPath string) Interface {
	return &Mounter{
		mounterPath:                mounterPath,
		trySystemd:                 false,
		withSafeNotMountedBehavior: detectSafeNotMountedBehavior(),
	}
}

// hasSystemd 检查 withSystemd 是否已设置；未设置时，对当前 Mounter 实例调用一次 detectSystemd。
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

// getBindMountOptions 将 unix.Statfs 中的 ro、nodev、noexec、nosuid、noatime、relatime、nodiratime 标志映射为挂载选项字符串。
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

// bindMountSensitive 按指定选项执行绑定挂载，再以与原挂载点相同的 nodev、nosuid、noexec、noatime、relatime、nodiratime 选项重新挂载。
// bind 本身不继承这些安全属性，必须在第二次 remount 时补回来。
// 敏感选项只进入首次挂载与敏感 remount，不会出现在普通 remount 的日志里。
func (mounter *Mounter) bindMountSensitive(mounterPath string, mountCmd string, source string, target string, fstype string, bindOpts []string, bindRemountOpts []string, bindRemountOptsSensitive []string, mountFlags []string, systemdMountRequired bool) error {
	err := mounter.doMount(mounterPath, mountCmd, source, target, fstype, bindOpts, bindRemountOptsSensitive, mountFlags, systemdMountRequired)
	if err != nil {
		return err
	}
	// 检查源是否带有 ro、nodev、noexec、nosuid、noatime、relatime、nodiratime 标志。
	fixMountOpts, err := getBindMountOptions(source, unix.Statfs)
	if err != nil {
		return &os.PathError{Op: "statfs", Path: source, Err: err}
	}
	// 然后带上上述标志重新挂载。
	bindRemountOpts = append(bindRemountOpts, fixMountOpts...)
	return mounter.doMount(mounterPath, mountCmd, source, target, fstype, bindRemountOpts, bindRemountOptsSensitive, mountFlags, systemdMountRequired)
}

// Mount 按给定选项将 source 挂载到 target，文件系统类型为 fstype。
// 不需要 source 或 fstype 时必须传空字符串，例如重新挂载或由内核自动识别文件系统类型时。
// options 是来自 mount(8) 的选项列表，例如 "ro"、"remount"、"bind"；无需选项时传空切片或 nil。
// 统一收敛到 MountSensitive，让脱敏逻辑只存在一份实现。
func (mounter *Mounter) Mount(source string, target string, fstype string, options []string) error {
	return mounter.MountSensitive(source, target, fstype, options, nil)
}

// MountSensitive 与 Mount 相同，但通过单独参数接收敏感选项，并确保这些选项不会写入日志。
// 当挂载选项包含密码等敏感信息时，调用方应使用本方法。
// GCI 镜像里 nfs/cifs 等用户态文件系统需要容器化 mount，其余类型直接用宿主二进制。
func (mounter *Mounter) MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	// 需要容器化挂载器时保存其二进制路径，否则为空；所有 Linux 发行版都应自带支持绑定挂载的 mount 工具。
	mounterPath := ""
	bind, bindOpts, bindRemountOpts, bindRemountOptsSensitive := MakeBindOptsSensitive(options, sensitiveOptions)
	if bind {
		return mounter.bindMountSensitive(mounterPath, defaultMountCommand, source, target, fstype, bindOpts, bindRemountOpts, bindRemountOptsSensitive, nil /* mountFlags */, mounter.trySystemd)
	}
	// 只有这几类需要切换二进制；其余文件系统走默认路径，省掉一次无谓的替换。
	// GCI 镜像集群中需要容器化挂载器的文件系统类型。
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

// MountSensitiveWithoutSystemd 与 MountSensitive 相同，但不使用 systemd 挂载。
func (mounter *Mounter) MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	return mounter.MountSensitiveWithoutSystemdWithMountFlags(source, target, fstype, options, sensitiveOptions, nil /* mountFlags */)
}

// MountSensitiveWithoutSystemdWithMountFlags 与 MountSensitiveWithoutSystemd 相同，并额外接收挂载标志。
// mountFlags 走命令行位置参数，不属于 -o 选项；与敏感选项分离便于日志脱敏。
func (mounter *Mounter) MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error {
	mounterPath := ""
	bind, bindOpts, bindRemountOpts, bindRemountOptsSensitive := MakeBindOptsSensitive(options, sensitiveOptions)
	if bind {
		return mounter.bindMountSensitive(mounterPath, defaultMountCommand, source, target, fstype, bindOpts, bindRemountOpts, bindRemountOptsSensitive, mountFlags, false)
	}
	// GCI 镜像集群中需要容器化挂载器的文件系统类型。
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

// doMount 执行 mount 命令；使用容器化挂载器时，mounterPath 为其二进制路径。
// sensitiveOptions 是 options 的扩展，但不会写入日志，因为可能包含敏感信息。
// systemdMountRequired 决定是否使用 systemd 挂载。
// 同时持有原始参数与脱敏日志串，记录日志时无需再拼出密码。
func (mounter *Mounter) doMount(mounterPath string, mountCmd string, source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string, systemdMountRequired bool) error {
	mountArgs, mountArgsLogStr := MakeMountArgsSensitiveWithMountFlags(source, target, fstype, options, sensitiveOptions, mountFlags)
	if len(mounterPath) > 0 {
		mountArgs = append([]string{mountCmd}, mountArgs...)
		mountArgsLogStr = mountCmd + " " + mountArgsLogStr
		mountCmd = mounterPath
	}

	if systemdMountRequired && mounter.hasSystemd() {
		// 尝试通过 systemd-run --scope 运行 mount，以脱离 kubelet 所在服务；FUSE 守护进程会在独立 scope 中启动，kubelet 服务重启不会杀死它们。
		//
		// 完整命令行（未使用 mounterPath 时）：
		// systemd-run --description=... --scope -- mount -t <type> <what> <where>
		//
		// 预期流程：
		// * systemd-run 创建临时 scope（约等于 cgroup），并在其中执行参数指定的命令（/bin/mount）。
		// * mount 完成挂载，必要时 fork FUSE 守护进程后结束；systemd-run --scope 此时返回 mount 的退出码及标准输出、标准错误，这是 --scope 的优势之一。
		// * 在 FUSE 守护进程退出前，systemd 会让它继续在 scope（即独立 cgroup）中运行，这是 --scope 的另一优势；kubelet 服务重启后守护进程仍可存活。
		// * FUSE 守护进程退出时（例如卸载过程中），systemd 自动删除 scope。
		//
		// 不使用 systemd-mount，因为它对旧发行版（CentOS 7、Debian Jessie）而言太新。
		// 只有调用方要求且确实探测到 systemd 时才改造命令行；否则保持原样。
		mountCmd, mountArgs, mountArgsLogStr = AddSystemdScopeSensitive("systemd-run", target, mountCmd, mountArgs, mountArgsLogStr)
		// } else {
		// 宿主没有 systemd-run（或探测失败）时，假定 kubelet 不是作为 systemd 服务运行的。
		// 无需额外处理，mountCmd 和 mountArgs 已填好。
	}

	// 记录日志前移除敏感挂载选项。
	klog.V(4).Infof("Mounting cmd (%s) with arguments (%s)", mountCmd, mountArgsLogStr)
	command := exec.Command(mountCmd, mountArgs...)
	output, err := command.CombinedOutput()
	if err != nil {
		if err.Error() == errNoChildProcesses {
			// CombinedOutput 把 Start/Wait 的竞态包装成字符串错误，只能靠 ProcessState 还原真实退出码。
			if command.ProcessState.Success() {
				// 进程本身成功时，不把 errNoChildProcesses 视为错误（参见 k/k issue #103753）。
				return nil
			}
			// 用进程的实际退出错误替换原错误。
			err = &exec.ExitError{ProcessState: command.ProcessState}
		}
		klog.Errorf("Mount failed: %v\nMounting command: %s\nMounting arguments: %s\nOutput: %s\n", err, mountCmd, mountArgsLogStr, string(output))
		return fmt.Errorf("mount failed: %v\nMounting command: %s\nMounting arguments: %s\nOutput: %s",
			err, mountCmd, mountArgsLogStr, string(output))
	}
	return err
}

// detectSystemd 判断操作系统是否以 systemd 作为 init；遇到权限错误等不确定情况时返回 false。
// 探测方式不止一种，本方法确保 Mount 所需的 systemd-run 可用。
// 探测的是“能否用 systemd-run --scope”，而非 systemd 是否安装：容器镜像常装了它但 PID 1 不是。
func detectSystemd() bool {
	if _, err := exec.LookPath("systemd-run"); err != nil {
		klog.V(2).Infof("Detected OS without systemd")
		return false
	}
	// 用 /bin/true 当探针比读 /proc/1/comm 更贴近实际用途：能在 scope 里跑起来才算可用。
	// 尝试运行 systemd-run --scope /bin/true，足以确认 systemd 确实运行，而不只是安装了程序；基于 systemd 的容器镜像可能安装了它但 PID 1 不同。
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

// detectSafeNotMountedBehavior 判断指定路径未挂载时，umount 实现是否会回复 "not mounted"；遇到权限错误等不确定情况时返回 false。
// 可行时将信任 umount 的应答，避免自行检查挂载点。
// 更多信息：https://github.com/util-linux/util-linux/blob/v2.2/mount/umount.c#L179
// 信任 umount 的应答能省掉每次卸载前的挂载点检查，但得先实测当前实现是否可靠。
func detectSafeNotMountedBehavior() bool {
	return detectSafeNotMountedBehaviorWithExec(utilexec.New())
}

// detectSafeNotMountedBehaviorWithExec 供测试通过 FakeExec 注入命令执行器。
// 把 exec 抽成参数，测试里就能用假命令模拟不同 umount 的输出。
func detectSafeNotMountedBehaviorWithExec(exec utilexec.Interface) bool {
	// 用确定未挂载的临时目录做探针，不碰真实挂载点；创建临时目录后尝试卸载它。
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

// MakeMountArgs 构造 mount(8) 命令的参数。
// options 不得包含密码等敏感信息；需要接收敏感参数时，调用方应使用 Sensitive 版本。
func MakeMountArgs(source, target, fstype string, options []string) (mountArgs []string) {
	mountArgs, _ = MakeMountArgsSensitive(source, target, fstype, options, nil /* sensitiveOptions */)
	return mountArgs
}

// MakeMountArgsSensitive 构造 mount(8) 命令的参数。
// sensitiveOptions 是 options 的扩展，但不会写入日志，因为可能包含敏感信息。
// 两个返回值一一对应：前者执行命令，后者只用于日志。
func MakeMountArgsSensitive(source, target, fstype string, options []string, sensitiveOptions []string) (mountArgs []string, mountArgsLogStr string) {
	return MakeMountArgsSensitiveWithMountFlags(source, target, fstype, options, sensitiveOptions, nil /* mountFlags */)
}

// MakeMountArgsSensitiveWithMountFlags 构造 mount(8) 命令的参数。
// sensitiveOptions 是 options 的扩展，但不会写入日志，因为可能包含敏感信息。
// mountFlags 是与文件系统类型及挂载选项无关的额外挂载标志。
// mountFlags 放在命令最前，与 -t/-o 无关；日志串始终用脱敏后的选项拼装。
func MakeMountArgsSensitiveWithMountFlags(source, target, fstype string, options []string, sensitiveOptions []string, mountFlags []string) (mountArgs []string, mountArgsLogStr string) {
	// 按如下格式构建 mount 命令：
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
		// 真实选项用于执行命令，日志里敏感项只留占位符；日志串不包含 sensitiveOptions。
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

// AddSystemdScope 为给定命令行添加 "systemd-run --scope"。
// args 包含敏感信息时，应使用 AddSystemdScopeSensitive 构造可安全记录的日志串。
// description 带上挂载点，便于在 systemctl 里定位是哪个卷起了 scope。
func AddSystemdScope(systemdRunPath, mountName, command string, args []string) (string, []string) {
	descriptionArg := fmt.Sprintf("--description=Kubernetes transient mount for %s", mountName)
	systemdRunArgs := []string{descriptionArg, "--scope", "--", command}
	return systemdRunPath, append(systemdRunArgs, args...)
}

// AddSystemdScopeSensitive 为给定命令行添加 "systemd-run --scope"，并接收已脱敏的挂载参数日志串 mountArgsLogStr，返回追加到 systemd 命令后用于记录的字符串。
// 与 AddSystemdScope 只差日志串：敏感信息由调用方脱敏后传进来。
func AddSystemdScopeSensitive(systemdRunPath, mountName, command string, args []string, mountArgsLogStr string) (string, []string, string) {
	descriptionArg := fmt.Sprintf("--description=Kubernetes transient mount for %s", mountName)
	systemdRunArgs := []string{descriptionArg, "--scope", "--", command}
	return systemdRunPath, append(systemdRunArgs, args...), strings.Join(systemdRunArgs, " ") + " " + mountArgsLogStr
}

// Unmount 卸载目标；挂载器具备安全的 "not mounted" 行为时，目标不是挂载点也不会返回错误。
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

// UnmountWithForce 卸载给定目标，并在指定超时后带强制选项重试。
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

// List 返回所有已挂载文件系统的列表。
// 直接解析 /proc/mounts，不调用外部命令，少一层路径依赖。
func (*Mounter) List() ([]MountPoint, error) {
	return ListProcMounts(procMountsPath)
}

// statx 能直接给出挂载根属性，比比较 st_dev 更准；AT_STATX_DONT_SYNC 避免为了取属性去和 NFS 服务器同步。
// ENOSYS 表示内核不支持 statx，是回退到旧方法时的正常信号。
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
				// file 是挂载点。
				return false, nil
			} else {
				// 属性位可用且未置位，结论已确定，不必再取父目录；内核支持 unix.STATX_ATTR_MOUNT_ROOT 时无需检查 rootStat。
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

// IsLikelyNotMountPoint 快速判断目录是否不是挂载点，但结果不一定始终正确。
// 它无法检测从一个挂载区域绑定到另一个区域的挂载，也不能区分挂载点与符号链接。
// mkdir /tmp/a /tmp/b; mount --bind /tmp/a /tmp/b; IsLikelyNotMountPoint("/tmp/b")
// 会返回 true，但 /tmp/b 实际是挂载点；需要识别这种情况时不要使用本方法。
// 先走 statx，老内核返回 ENOSYS 才回退，避免每次都做两组系统调用。
func (mounter *Mounter) IsLikelyNotMountPoint(file string) (bool, error) {
	notMountPoint, err := mounter.isLikelyNotMountPointStatx(file)
	if errors.Is(err, errStatxNotSupport) {
		// 回退到 isLikelyNotMountPointStat。
		return mounter.isLikelyNotMountPointStat(file)
	}

	return notMountPoint, err
}

// CanSafelySkipMountPointCheck 返回对非挂载点目标执行 umount 时探测到的行为是否允许跳过挂载点检查。
// “安全”指 umount 会明确报 not mounted，调用方因此可以省掉自己的预判。
func (mounter *Mounter) CanSafelySkipMountPointCheck() bool {
	return mounter.withSafeNotMountedBehavior
}

// GetMountRefs 查找 pathname 的所有挂载引用并返回路径列表；路径可以是挂载点，也可以是用于绑定挂载的普通目录。
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

// checkAndRepairFilesystem 使用 fsck 命令检查并修复文件系统。
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

// formatAndMountSensitive 使用 Unix 工具按需格式化并挂载给定磁盘。
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
		// ro 也可能藏在敏感选项里，漏判会导致只读卷被格式化；因此也要检查 sensitiveOptions。
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

	// 检查磁盘是否已经格式化。
	existingFormat, err := mounter.GetDiskFormat(source)
	if err != nil {
		return NewMountError(GetDiskFormatFailed, "failed to get disk format of disk %s: %v", source, err)
	}

	// 未指定类型时沿用历史默认 ext4。
	if len(fstype) == 0 {
		fstype = "ext4"
	}

	// 只有确认是裸盘才 mkfs；已有数据的盘绝不能顺手格式化。
	if existingFormat == "" {
		// 以只读方式挂载时不尝试格式化磁盘，而是返回错误明确说明这一限制。
		if readOnly {
			return NewMountError(UnformattedReadOnly, "cannot mount unformatted disk %s as we are manipulating it in read-only mode", source)
		}

		// 磁盘未格式化，因此执行格式化。
		args := []string{source}
		if fstype == "ext4" || fstype == "ext3" {
			args = []string{
				"-F",  // 强制标志。
				"-m0", // 不为超级用户保留数据块。
				source,
			}
		} else if fstype == "xfs" {
			args = []string{
				"-f", // 强制标志。
				source,
			}
		}
		args = append(formatOptions, args...)

		klog.Infof("Disk %q appears to be unformatted, attempting to format as type: %q with options: %v", source, fstype, args)

		output, err := mounter.format(fstype, args)
		if err != nil {
			// 日志只记录普通 options，不记录 sensitiveOptions。
			sensitiveOptionsLog := sanitizedOptionsForLogging(options, sensitiveOptions)
			detailedErr := fmt.Sprintf("format of disk %q failed: type:(%q) target:(%q) options:(%q) errcode:(%v) output:(%v) ", source, fstype, target, sensitiveOptionsLog, err, string(output))
			klog.Error(detailedErr)
			return NewMountError(FormatFailed, "%s", detailedErr)
		}

		klog.Infof("Disk successfully formatted (mkfs): %s - %s %s", fstype, source, target)
	} else {
		// 类型不符只记录告警，最终的 mount 失败会带上内核侧的错误细节。
		if fstype != existingFormat {
			// 检查磁盘的文件系统类型是否符合预期。
			mountErrorValue = FilesystemMismatch
			klog.Warningf("Configured to mount disk %s as %s but current format is %s, things might break", source, fstype, existingFormat)
		}

		// fsck 会写盘，只读卷跳过。
		if !readOnly {
			// 仅对请求以读写方式挂载的已格式化卷运行检查工具，修复可处理的问题。
			err := mounter.checkAndRepairFilesystem(source)
			if err != nil {
				return err
			}
		}
	}

	// 挂载磁盘。
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

// getDiskFormat 通过一次 blkid 同时获取 TYPE 与 PTTYPE：有分区表说明盘上已有布局，不能当裸盘格式化。
// 退出码 2 表示 blkid 未识别到目标，不等于探测命令失败。
func getDiskFormat(exec utilexec.Interface, disk string) (string, error) {
	args := []string{"-p", "-s", "TYPE", "-s", "PTTYPE", "-o", "export", disk}
	klog.V(4).Infof("Attempting to determine if disk %q is formatted using blkid with args: (%v)", disk, args)
	dataOut, err := exec.Command("blkid", args...).CombinedOutput()
	output := string(dataOut)
	klog.V(4).Infof("Output: %q", output)

	if err != nil {
		if exit, ok := err.(utilexec.ExitError); ok {
			if exit.ExitStatus() == 2 {
				// 磁盘设备未格式化；blkid 在指定标记（TYPE/PTTYPE 等）不存在或无法识别任何指定设备时返回退出码 2。
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
			// 忽略空行。
			continue
		}
		cs := strings.Split(l, "=")
		if len(cs) != 2 {
			return "", fmt.Errorf("blkid returns invalid output: %s", output)
		}
		// 根据 https://www.kernel.org/pub/linux/utils/util-linux/v2.21/libblkid-docs/，TYPE 表示文件系统类型，PTTYPE 表示分区表类型。
		if cs[0] == "TYPE" {
			fstype = cs[1]
		} else if cs[0] == "PTTYPE" {
			pttype = cs[1]
		}
	}

	// 有分区表时返回特殊非空字符串，阻止后续 mkfs 覆盖已有分区布局。
	if len(pttype) > 0 {
		klog.V(4).Infof("Disk %s detected partition table type: %s", disk, pttype)
		// 返回特殊的非空字符串作为文件系统类型，使 kubelet 不会格式化该磁盘。
		return "unknown data, probably partitions", nil
	}

	return fstype, nil
}

// GetDiskFormat 使用 blkid 判断给定磁盘是否未格式化。
// 抽成包级函数后，测试可以直接注入 FakeExec 而不必构造完整 Mounter。
func (mounter *SafeFormatAndMount) GetDiskFormat(disk string) (string, error) {
	return getDiskFormat(mounter.Exec, disk)
}

// ListProcMounts 返回指定挂载文件中所有已挂载文件系统的列表。
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
			// 末尾换行经 split 后产生空字符串，直接跳过。
			continue
		}
		fields := strings.Fields(line)
		// 选项里可能带密码，错误只报列数，不回显整行，避免日志泄露敏感挂载选项。
		if len(fields) != expectedNumFieldsPerLine {
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

// SearchMountPoints 查找源的所有挂载引用并返回挂载点列表。
// 源可以是挂载点，也可以是用于绑定挂载的普通目录；目前不支持设备，因为没有使用场景。
// 某些文件系统可能共用源名称（例如 tmpfs），绑定挂载也可能挂载文件系统的非根路径，因此必须用根路径和 major:minor 唯一标识挂载源。
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

	// 尽可能查找底层根路径和 major:minor；后挂载的条目会盖住先挂载的，因此倒序搜索才能定位当前生效的那一层。
	for i := len(mis) - 1; i >= 0; i-- {
		if hostSource == mis[i].MountPoint || PathWithinBase(hostSource, mis[i].MountPoint) {
			// 处理挂载点本身或其下的路径。
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
		// 调用方要的是指向源的引用，源挂载本身不算，因此跳过对应条目。
		if mis[i].ID == mountID {
			continue
		}
		if mis[i].Root == rootPath && mis[i].Major == major && mis[i].Minor == minor {
			refs = append(refs, mis[i].MountPoint)
		}
	}

	return refs, nil
}

// IsMountPoint 判断文件是否为挂载点。
// 首先使用 MountedFast 检测绑定挂载及其他挂载点；MountedFast 返回 sure=true 且 err=nil 时直接采用结果。
// MountedFast 返回错误时：
// 1. 除 os.IsPermission 外，IsMountPoint 均返回 false 和该错误。
// 2. MountedFast 返回 os.IsPermission 时，调用 List() 确认文件是否为挂载点。
//
// 文件不存在时必须始终返回 os.ErrNotExist；历史调用方依赖该错误且不会执行回退，也依赖它判断是否需要创建目录。
//
// MountedFast 返回 sure=false 且 err=nil 时（例如内核 5.10- 上的绑定挂载），调用 mounter.List() 枚举所有挂载点并检查是否匹配。
// MountedFast 对绑定挂载等情形给不出确定答案；不确定的一律回退到 /proc/mounts 全表扫描。
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
		// 简单 stat() 检查未获允许，例如 NFS 启用 root_squash；收到 fs.ErrPermission 时回退到下面的 /proc/mounts 检查。
		// 权限之外的失败说明探测本身不可用，回退到列表也无济于事。
		if !errors.Is(isMntErr, fs.ErrPermission) {
			return false, isMntErr
		}
	}
	// /proc/mounts 里是内核解析后的路径，不解析就会漏配；内核同样会解析符号链接并使用解析后的路径。
	resolvedFile, err := filepath.EvalSymlinks(file)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return false, fs.ErrNotExist
		}
		return false, err
	}

	// 全表扫描慢，但能覆盖 bind mount 和被 rename 过的挂载点，是 MountedFast 对某些类型不可靠时的最后兜底。
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

// tryUnmount 调用普通 umount，并等待 unmountTimeout 指定的时间直到完成。
// 超时得自己返回 DeadlineExceeded：CombinedOutput 不会把 ctx 超时透传出来。
func tryUnmount(target string, withSafeNotMountedBehavior bool, unmountTimeout time.Duration) error {
	klog.V(4).Infof("Unmounting %s", target)
	ctx, cancel := context.WithTimeout(context.Background(), unmountTimeout)
	defer cancel()

	command := exec.CommandContext(ctx, "umount", target)
	output, err := command.CombinedOutput()

	// CombinedOutput() 不会返回 DeadlineExceeded，超时时必须显式传播该错误。
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

// checkUmountError 检查 umount 命令的结果并确定返回值。
// 把“目标本就没挂载”从错误里摘出来，让重复卸载保持幂等。
func checkUmountError(target string, command *exec.Cmd, output []byte, err error, withSafeNotMountedBehavior bool) error {
	// 与挂载路径同样的竞态，先看 ProcessState 再决定是否转成真实退出错误。
	if err.Error() == errNoChildProcesses {
		if command.ProcessState.Success() {
			// 进程本身成功时，不把 errNoChildProcesses 视为错误（参见 k/k issue #103753）。
			return nil
		}
		// 用进程的实际退出错误替换原错误。
		err = &exec.ExitError{ProcessState: command.ProcessState}
	}
	// 只有实测确认 umount 会报 not mounted，才敢忽略这条错误。
	if withSafeNotMountedBehavior && strings.Contains(string(output), errNotMounted) {
		klog.V(4).Infof("ignoring 'not mounted' error for %s", target)
		return nil
	}
	return fmt.Errorf("unmount failed: %v\nUnmounting arguments: %s\nOutput: %s", err, target, string(output))
}
