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

// TODO(thockin)：整个包目前高度偏向 Linux。一旦支持其他平台，还需进一步抽象。

package mount

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	utilexec "k8s.io/utils/exec"
)

const (
	// 未指定挂载器路径时使用的默认挂载命令。
	defaultMountCommand = "mount"
	// 日志中敏感挂载选项被移除后使用的标记。
	sensitiveOptionsRemoved = "<masked>"
)

// Interface 定义在系统上执行挂载操作所需的方法集合。
type Interface interface {
	// Mount 按给定的文件系统类型和选项将 source 挂载到 target。
	// options 不得包含密码等敏感信息。
	Mount(source string, target string, fstype string, options []string) error
	// MountSensitive 与 Mount 相同，但通过单独参数接收敏感选项，并确保这些选项不会写入日志。
	// 当挂载选项包含密码等敏感信息时，调用方应使用本方法。
	MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error
	// MountSensitiveWithoutSystemd 与 MountSensitive 相同，但不使用 systemd 挂载。
	MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error
	// MountSensitiveWithoutSystemdWithMountFlags 与 MountSensitiveWithoutSystemd 相同，并额外接收挂载标志。
	MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error
	// Unmount 卸载给定的 target。
	Unmount(target string) error
	// List 返回所有已挂载文件系统的列表，数量可能很大。
	// 在某些平台上，直接从操作系统读取挂载信息无法保证一致性，分块读取之间可能发生变化；本方法保证返回一致的结果。
	List() ([]MountPoint, error)
	// IsLikelyNotMountPoint 使用启发式判断目录是否不是挂载点。
	// 目录不存在时应返回 ErrNotExist。
	// 本方法不能正确识别所有挂载点类型，尤其是 Linux 绑定挂载和符号链接。
	// 对调用方不关心这些类型的情况，相比调用 List 并扫描结果，这是更快的替代方案。
	IsLikelyNotMountPoint(file string) (bool, error)
	// CanSafelySkipMountPointCheck 表示对非挂载点目标执行操作时，挂载器是否会返回错误。
	// 返回 true 时不会再返回这类错误。
	CanSafelySkipMountPointCheck() bool
	// IsMountPoint 判断目录是否为挂载点，目录不存在时应返回 ErrNotExist。
	// 它的开销高于 IsLikelyNotMountPoint，并能检测 Linux 绑定挂载。
	// 它可能调用 List 枚举所有挂载点；列表可能很大，随后使用 isMountPointMatch 判断目录是否为挂载点。
	IsMountPoint(file string) (bool, error)
	// GetMountRefs 查找 pathname 的所有挂载引用并返回路径切片。
	// pathname 可以是挂载点路径，也可以是普通目录（用于绑定挂载）；Linux 返回的切片不包含 pathname 本身。
	// 例如，/dev/sdc 同时挂载在 /path/a 和 /path/b 时：
	// GetMountRefs("/path/a") 返回 ["/path/b"]
	// GetMountRefs("/path/b") 返回 ["/path/a"]
	// Windows 无法查询所有挂载点；只要 pathname 是有效挂载，就会返回它。
	// Linux 下需据此统计同一设备的引用数，卸载前必须确认没有其它挂载点。
	GetMountRefs(pathname string) ([]string, error)
}

// 编译期检查，确保 Mounter 的所有实现都满足挂载接口。
var _ Interface = &Mounter{}

// MounterForceUnmounter 在默认卸载失败后可用强制选项重试，适用于挂载点卡住的场景。
type MounterForceUnmounter interface {
	Interface
	// UnmountWithForce 卸载给定目标，并在指定超时后带强制选项重试。
	UnmountWithForce(target string, umountTimeout time.Duration) error
}

// MountPoint 表示 /proc/mounts 或 /etc/fstab 中的一行。
// 各平台解析挂载表时统一填充该结构，调用方无需关心来源差异。
type MountPoint struct {
	Device string
	Path   string
	Type   string
	Opts   []string // Opts 可能包含密码等敏感挂载选项，必须按敏感信息处理，例如不得写入日志。
	Freq   int
	Pass   int
}

// MountErrorType 让调用方按错误类别决定重试或报错，而不必解析错误字符串。
type MountErrorType string

const (
	FilesystemMismatch  MountErrorType = "FilesystemMismatch"
	HasFilesystemErrors MountErrorType = "HasFilesystemErrors"
	UnformattedReadOnly MountErrorType = "UnformattedReadOnly"
	FormatFailed        MountErrorType = "FormatFailed"
	GetDiskFormatFailed MountErrorType = "GetDiskFormatFailed"
	UnknownMountError   MountErrorType = "UnknownMountError"
)

// MountError 携带分类与消息，便于上层区分可恢复错误和致命错误。
//
// Type 决定上层处理策略，例如格式不匹配需要重新格式化；
// Message 展示给调用方，构造时必须先脱敏。
type MountError struct {
	Type    MountErrorType
	Message string
}

// String 与 Error 保持一致，避免在不同日志路径上出现两种描述。
func (mountError MountError) String() string {
	return mountError.Message
}

// Error 返回挂载错误消息，实现 error 接口。
func (mountError MountError) Error() string {
	return mountError.Message
}

// NewMountError 集中构造带分类的错误，避免各处手写结构体时漏掉类型。
func NewMountError(mountErrorValue MountErrorType, format string, args ...interface{}) error {
	mountError := MountError{
		Type:    mountErrorValue,
		Message: fmt.Sprintf(format, args...),
	}
	return mountError
}

// SafeFormatAndMount 探测设备是否已格式化，即检查是否存在文件系统。
// 已有文件系统时直接挂载，否则先格式化设备再挂载。
// 它在普通挂载器之上叠加格式化保护，字段保持连续以便对齐。
//
// formatSem 作为并发令牌，限制同时执行的格式化数量；
// formatTimeout 到期即释放令牌，不中断已开始的格式化。
type SafeFormatAndMount struct {
	Interface
	Exec utilexec.Interface

	formatSem     chan any
	formatTimeout time.Duration
}

// NewSafeFormatAndMount 用可选配置构造格式化挂载器，未指定选项时保持旧的无限并发行为。
func NewSafeFormatAndMount(mounter Interface, exec utilexec.Interface, opts ...Option) *SafeFormatAndMount {
	res := &SafeFormatAndMount{
		Interface: mounter,
		Exec:      exec,
	}
	for _, opt := range opts {
		opt(res)
	}
	return res
}

// Option 只用于构造阶段，创建后再修改并发参数会导致令牌与超时不一致。
type Option func(*SafeFormatAndMount)

// WithMaxConcurrentFormat 设置挂载器并发执行格式化操作的最大数量。
// timeout 控制格式化操作释放并发令牌前的最长持续时间；令牌释放后，其它并发格式化操作可以获取它，原操作仍可继续完成。
// n < 1 时不限制并发。
// 超时只回收令牌，格式化本身继续跑完，以免中断正在写入的设备。
func WithMaxConcurrentFormat(n int, timeout time.Duration) Option {
	return func(mounter *SafeFormatAndMount) {
		if n > 0 {
			mounter.formatSem = make(chan any, n)
			mounter.formatTimeout = timeout
		}
	}
}

// FormatAndMount 按需格式化给定磁盘，然后挂载。
// 磁盘未格式化且不是只读挂载时，先格式化再挂载；磁盘已格式化或以只读方式挂载时，不执行格式化。
// options 不得包含密码等敏感信息。
func (mounter *SafeFormatAndMount) FormatAndMount(source string, target string, fstype string, options []string) error {
	return mounter.FormatAndMountSensitive(source, target, fstype, options, nil /* sensitiveOptions */)
}

// FormatAndMountSensitive 与 FormatAndMount 相同，但通过单独参数接收敏感选项，并确保这些选项不会写入日志。
// 当挂载选项包含密码等敏感信息时，调用方应使用本方法。
func (mounter *SafeFormatAndMount) FormatAndMountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	return mounter.FormatAndMountSensitiveWithFormatOptions(source, target, fstype, options, sensitiveOptions, nil /* formatOptions */)
}

// FormatAndMountSensitiveWithFormatOptions 的行为与 FormatAndMountSensitive 完全相同，但允许在格式化磁盘时传入额外选项。
// 这些选项不会经过任何校验，绝不能直接来自不可信的用户输入，否则存在注入风险。
func (mounter *SafeFormatAndMount) FormatAndMountSensitiveWithFormatOptions(source string, target string, fstype string, options []string, sensitiveOptions []string, formatOptions []string) error {
	return mounter.formatAndMountSensitive(source, target, fstype, options, sensitiveOptions, formatOptions)
}

// getMountRefsByDev 查找 mountPath 对应设备的所有引用并返回路径列表。
// mountPath 必须是解析所有符号链接后的路径；调用方需先解析符号链接，避免同一设备的不同路径被当成两个挂载点。
func getMountRefsByDev(mounter Interface, mountPath string) ([]string, error) {
	mps, err := mounter.List()
	if err != nil {
		return nil, err
	}

	// 查找挂载在 mountPath 上的设备。
	diskDev := ""
	for i := range mps {
		if mountPath == mps[i].Path {
			diskDev = mps[i].Device
			break
		}
	}

	// 查找该设备的所有引用。
	var refs []string
	for i := range mps {
		if mps[i].Device == diskDev || mps[i].Device == mountPath {
			if mps[i].Path != mountPath {
				refs = append(refs, mps[i].Path)
			}
		}
	}
	return refs, nil
}

// IsNotMountPoint 判断目录是否不是挂载点，依赖 IsMountPoint，开销高于 IsLikelyNotMountPoint。
// 目录不存在时应返回 ErrNotExist；发生错误时返回 true（按不是挂载点处理），调用方行为与 IsLikelyNotMountPoint 类似。
//
// Deprecated: 仅为兼容旧版库保留，调用方应优先使用 mounter.IsMountPoint。
func IsNotMountPoint(mounter Interface, file string) (bool, error) {
	isMnt, err := mounter.IsMountPoint(file)
	if err != nil {
		return true, err
	}
	return !isMnt, nil
}

// GetDeviceNameFromMount 从 /proc/mounts 查找给定挂载点对应的设备，返回设备名、引用计数和错误。
// 引用数用于卸载前判断是否还有其它挂载点使用该设备。
func GetDeviceNameFromMount(mounter Interface, mountPath string) (string, int, error) {
	mps, err := mounter.List()
	if err != nil {
		return "", 0, err
	}

	// 查找设备名。
	// FIXME：同一挂载路径对应多个设备时，只返回第一个。
	device := ""
	// mountPath 是符号链接时，需要获取其目标路径。
	slTarget, err := filepath.EvalSymlinks(mountPath)
	if err != nil {
		slTarget = mountPath
	}
	for i := range mps {
		if mps[i].Path == slTarget {
			device = mps[i].Device
			break
		}
	}

	// 查找该设备的所有引用。
	refCount := 0
	for i := range mps {
		if mps[i].Device == device {
			refCount++
		}
	}
	return device, refCount, nil
}

// MakeBindOpts 判断是否请求绑定挂载，并生成绑定挂载所需的重新挂载选项。
// 绑定挂载不会遵循原始挂载选项，因此需要在 remount 时显式带上。结果列表为：
//
//	options - 'bind' + 'remount'（不重复）
func MakeBindOpts(options []string) (bool, []string, []string) {
	bind, bindOpts, bindRemountOpts, _ := MakeBindOptsSensitive(options, nil /* sensitiveOptions */)
	return bind, bindOpts, bindRemountOpts
}

// MakeBindOptsSensitive 与 MakeBindOpts 相同，但通过单独参数接收敏感选项，并确保这些选项不会写入日志。
// 当挂载选项包含密码等敏感信息时，调用方应使用本方法。
func MakeBindOptsSensitive(options []string, sensitiveOptions []string) (bool, []string, []string, []string) {
	// 子路径绑定挂载已打开文件描述符，因此必须包含 "bind" 选项；否则以只读方式重新挂载时，目标会因繁忙而报错。
	// 因此，所有只读绑定挂载都不会再把底层卷挂载改为只读。
	bindRemountOpts := []string{"bind", "remount"}
	bindRemountSensitiveOpts := []string{}
	bind := false
	bindOpts := []string{"bind"}

	// _netdev 是用户态挂载选项，创建绑定挂载时不会自动添加，因此必须显式继承。
	if checkForNetDev(options, sensitiveOptions) {
		bindOpts = append(bindOpts, "_netdev")
	}

	for _, option := range options {
		switch option {
		case "bind":
			bind = true
		case "remount":
		default:
			bindRemountOpts = append(bindRemountOpts, option)
		}
	}

	for _, sensitiveOption := range sensitiveOptions {
		switch sensitiveOption {
		case "bind":
			bind = true
		case "remount":
		default:
			bindRemountSensitiveOpts = append(bindRemountSensitiveOpts, sensitiveOption)
		}
	}

	return bind, bindOpts, bindRemountOpts, bindRemountSensitiveOpts
}

// checkForNetDev 在普通和敏感选项里都要找，因为调用方可能把选项放在任一侧。
func checkForNetDev(options []string, sensitiveOptions []string) bool {
	for _, option := range options {
		if option == "_netdev" {
			return true
		}
	}
	for _, sensitiveOption := range sensitiveOptions {
		if sensitiveOption == "_netdev" {
			return true
		}
	}
	return false
}

// PathWithinBase 判断给定路径是否位于给定基础目录内。
// 仅做路径判断，不解析真实路径；调用方不能据此认定符号链接逃逸是安全的。
func PathWithinBase(fullPath, basePath string) bool {
	rel, err := filepath.Rel(basePath, fullPath)
	if err != nil {
		return false
	}
	if StartsWithBackstep(rel) {
		// 必须拒绝通过 .. 段逃出基础路径的情况。
		return false
	}
	return true
}

// StartsWithBackstep 判断给定路径是否以 .. 段开头。
// 统一用 / 判断，兼容 Windows 的反斜杠相对路径。
func StartsWithBackstep(rel string) bool {
	// 统一转换为 / 后检查 ../ 前缀。
	return rel == ".." || strings.HasPrefix(filepath.ToSlash(rel), "../")
}

// sanitizedOptionsForLogging 返回以逗号分隔的选项字符串，其中包含 options 和 sensitiveOptions。
// sensitiveOptions 的每一项都会替换为 sensitiveOptionsRemoved，例如 o1,o2,<masked>,<masked>。
// 敏感项一律只输出掩码，日志里不能泄露密码等凭据。
func sanitizedOptionsForLogging(options []string, sensitiveOptions []string) string {
	separator := ""
	if len(options) > 0 && len(sensitiveOptions) > 0 {
		separator = ","
	}

	sensitiveOptionsStart := ""
	sensitiveOptionsEnd := ""
	if len(sensitiveOptions) > 0 {
		sensitiveOptionsStart = strings.Repeat(sensitiveOptionsRemoved+",", len(sensitiveOptions)-1)
		sensitiveOptionsEnd = sensitiveOptionsRemoved
	}

	return strings.Join(options, ",") +
		separator +
		sensitiveOptionsStart +
		sensitiveOptionsEnd
}
