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

// TODO(thockin): This whole pkg is pretty linux-centric.  As soon as we have
// an alternate platform, we will need to abstract further.

package mount

import (
	"fmt"
	"path/filepath"
	"strings"
	"time"

	utilexec "k8s.io/utils/exec"
)

const (
	// Default mount command if mounter path is not specified.
	defaultMountCommand = "mount"
	// Log message where sensitive mount options were removed
	sensitiveOptionsRemoved = "<masked>"
)

// Interface defines the set of methods to allow for mount operations on a system.
type Interface interface {
	// Mount mounts source to target as fstype with given options.
	// options MUST not contain sensitive material (like passwords).
	Mount(source string, target string, fstype string, options []string) error
	// MountSensitive is the same as Mount() but this method allows
	// sensitiveOptions to be passed in a separate parameter from the normal
	// mount options and ensures the sensitiveOptions are never logged. This
	// method should be used by callers that pass sensitive material (like
	// passwords) as mount options.
	MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error
	// MountSensitiveWithoutSystemd is the same as MountSensitive() but this method disable using systemd mount.
	MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error
	// MountSensitiveWithoutSystemdWithMountFlags is the same as MountSensitiveWithoutSystemd() with additional mount flags
	MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error
	// Unmount unmounts given target.
	Unmount(target string) error
	// List returns a list of all mounted filesystems.  This can be large.
	// On some platforms, reading mounts directly from the OS is not guaranteed
	// consistent (i.e. it could change between chunked reads). This is guaranteed
	// to be consistent.
	List() ([]MountPoint, error)
	// IsLikelyNotMountPoint uses heuristics to determine if a directory
	// is not a mountpoint.
	// It should return ErrNotExist when the directory does not exist.
	// IsLikelyNotMountPoint does NOT properly detect all mountpoint types
	// most notably linux bind mounts and symbolic link. For callers that do not
	// care about such situations, this is a faster alternative to calling List()
	// and scanning that output.
	IsLikelyNotMountPoint(file string) (bool, error)
	// CanSafelySkipMountPointCheck indicates whether this mounter returns errors on
	// operations for targets that are not mount points. If this returns true, no such
	// errors will be returned.
	CanSafelySkipMountPointCheck() bool
	// IsMountPoint determines if a directory is a mountpoint.
	// It should return ErrNotExist when the directory does not exist.
	// IsMountPoint is more expensive than IsLikelyNotMountPoint.
	// IsMountPoint detects bind mounts in linux.
	// IsMountPoint may enumerate all the mountpoints using List() and
	// the list of mountpoints may be large, then it uses
	// isMountPointMatch to evaluate whether the directory is a mountpoint.
	IsMountPoint(file string) (bool, error)
	// GetMountRefs finds all mount references to pathname, returning a slice of
	// paths. Pathname can be a mountpoint path or a normal	directory
	// (for bind mount). On Linux, pathname is excluded from the slice.
	// For example, if /dev/sdc was mounted at /path/a and /path/b,
	// GetMountRefs("/path/a") would return ["/path/b"]
	// GetMountRefs("/path/b") would return ["/path/a"]
	// On Windows there is no way to query all mount points; as long as pathname is
	// a valid mount, it will be returned.
	// Linux 下需据此统计同一设备的引用数，卸载前必须确认没有其它挂载点。
	GetMountRefs(pathname string) ([]string, error)
}

// Compile-time check to ensure all Mounter implementations satisfy
// the mount interface.
var _ Interface = &Mounter{}

// MounterForceUnmounter 在默认卸载失败后可用强制选项重试，适用于挂载点卡住的场景。
type MounterForceUnmounter interface {
	Interface
	// UnmountWithForce unmounts given target but will retry unmounting with force option
	// after given timeout.
	UnmountWithForce(target string, umountTimeout time.Duration) error
}

// MountPoint represents a single line in /proc/mounts or /etc/fstab.
// 各平台解析挂载表时统一填充该结构，调用方无需关心来源差异。
type MountPoint struct {
	Device string
	Path   string
	Type   string
	Opts   []string // Opts may contain sensitive mount options (like passwords) and MUST be treated as such (e.g. not logged).
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

// SafeFormatAndMount probes a device to see if it is formatted.
// Namely it checks to see if a file system is present. If so it
// mounts it otherwise the device is formatted first then mounted.
// SafeFormatAndMount 在普通挂载器之上叠加格式化保护，字段保持连续以便对齐。
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

// WithMaxConcurrentFormat sets the maximum number of concurrent format
// operations executed by the mounter. The timeout controls the maximum
// duration of a format operation before its concurrency token is released.
// Once a token is released, it can be acquired by another concurrent format
// operation. The original operation is allowed to complete.
// If n < 1, concurrency is set to unlimited.
// 超时只回收令牌，格式化本身继续跑完，以免中断正在写入的设备。
func WithMaxConcurrentFormat(n int, timeout time.Duration) Option {
	return func(mounter *SafeFormatAndMount) {
		if n > 0 {
			mounter.formatSem = make(chan any, n)
			mounter.formatTimeout = timeout
		}
	}
}

// FormatAndMount formats the given disk, if needed, and mounts it.
// That is if the disk is not formatted and it is not being mounted as
// read-only it will format it first then mount it. Otherwise, if the
// disk is already formatted or it is being mounted as read-only, it
// will be mounted without formatting.
// options MUST not contain sensitive material (like passwords).
func (mounter *SafeFormatAndMount) FormatAndMount(source string, target string, fstype string, options []string) error {
	return mounter.FormatAndMountSensitive(source, target, fstype, options, nil /* sensitiveOptions */)
}

// FormatAndMountSensitive is the same as FormatAndMount but this method allows
// sensitiveOptions to be passed in a separate parameter from the normal mount
// options and ensures the sensitiveOptions are never logged. This method should
// be used by callers that pass sensitive material (like passwords) as mount
// options.
func (mounter *SafeFormatAndMount) FormatAndMountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	return mounter.FormatAndMountSensitiveWithFormatOptions(source, target, fstype, options, sensitiveOptions, nil /* formatOptions */)
}

// FormatAndMountSensitiveWithFormatOptions behaves exactly the same as
// FormatAndMountSensitive, but allows for options to be passed when the disk
// is formatted. These options are NOT validated in any way and should never
// come directly from untrusted user input as that would be an injection risk.
func (mounter *SafeFormatAndMount) FormatAndMountSensitiveWithFormatOptions(source string, target string, fstype string, options []string, sensitiveOptions []string, formatOptions []string) error {
	return mounter.formatAndMountSensitive(source, target, fstype, options, sensitiveOptions, formatOptions)
}

// getMountRefsByDev finds all references to the device provided
// by mountPath; returns a list of paths.
// Note that mountPath should be path after the evaluation of any symblolic links.
// 调用方先解析符号链接，避免同一设备的不同路径被当成两个挂载点。
func getMountRefsByDev(mounter Interface, mountPath string) ([]string, error) {
	mps, err := mounter.List()
	if err != nil {
		return nil, err
	}

	// Finding the device mounted to mountPath.
	diskDev := ""
	for i := range mps {
		if mountPath == mps[i].Path {
			diskDev = mps[i].Device
			break
		}
	}

	// Find all references to the device.
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

// IsNotMountPoint determines if a directory is a mountpoint.
// It should return ErrNotExist when the directory does not exist.
// IsNotMountPoint is more expensive than IsLikelyNotMountPoint
// and depends on IsMountPoint.
//
// If an error occurs, it returns true (assuming it is not a mountpoint)
// when ErrNotExist is returned for callers similar to IsLikelyNotMountPoint.
//
// Deprecated: This function is kept to keep changes backward compatible with
// previous library version. Callers should prefer mounter.IsMountPoint.
// 已弃用：仅为兼容旧调用方保留，新代码直接用 IsMountPoint。
func IsNotMountPoint(mounter Interface, file string) (bool, error) {
	isMnt, err := mounter.IsMountPoint(file)
	if err != nil {
		return true, err
	}
	return !isMnt, nil
}

// GetDeviceNameFromMount given a mnt point, find the device from /proc/mounts
// returns the device name, reference count, and error code.
// 引用数用于卸载前判断是否还有其它挂载点使用该设备。
func GetDeviceNameFromMount(mounter Interface, mountPath string) (string, int, error) {
	mps, err := mounter.List()
	if err != nil {
		return "", 0, err
	}

	// Find the device name.
	// FIXME if multiple devices mounted on the same mount path, only the first one is returned.
	device := ""
	// If mountPath is symlink, need get its target path.
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

	// Find all references to the device.
	refCount := 0
	for i := range mps {
		if mps[i].Device == device {
			refCount++
		}
	}
	return device, refCount, nil
}

// MakeBindOpts detects whether a bind mount is being requested and makes the remount options to
// use in case of bind mount, due to the fact that bind mount doesn't respect mount options.
// The list equals:
//
//	options - 'bind' + 'remount' (no duplicate)
//
// bind 挂载不继承原始挂载选项，因此需要在 remount 时显式带上。
func MakeBindOpts(options []string) (bool, []string, []string) {
	bind, bindOpts, bindRemountOpts, _ := MakeBindOptsSensitive(options, nil /* sensitiveOptions */)
	return bind, bindOpts, bindRemountOpts
}

// MakeBindOptsSensitive is the same as MakeBindOpts but this method allows
// sensitiveOptions to be passed in a separate parameter from the normal mount
// options and ensures the sensitiveOptions are never logged. This method should
// be used by callers that pass sensitive material (like passwords) as mount
// options.
func MakeBindOptsSensitive(options []string, sensitiveOptions []string) (bool, []string, []string, []string) {
	// Because we have an FD opened on the subpath bind mount, the "bind" option
	// needs to be included, otherwise the mount target will error as busy if you
	// remount as readonly.
	//
	// As a consequence, all read only bind mounts will no longer change the underlying
	// volume mount to be read only.
	bindRemountOpts := []string{"bind", "remount"}
	bindRemountSensitiveOpts := []string{}
	bind := false
	bindOpts := []string{"bind"}

	// _netdev is a userspace mount option and does not automatically get added when
	// bind mount is created and hence we must carry it over.
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

// PathWithinBase checks if give path is within given base directory.
// 仅做路径判断，不解析真实路径，避免把 symlink 逃逸误判为安全。
func PathWithinBase(fullPath, basePath string) bool {
	rel, err := filepath.Rel(basePath, fullPath)
	if err != nil {
		return false
	}
	if StartsWithBackstep(rel) {
		// Needed to escape the base path.
		return false
	}
	return true
}

// StartsWithBackstep checks if the given path starts with a backstep segment.
// StartsWithBackstep 统一用 / 判断，兼容 Windows 的反斜杠相对路径。
func StartsWithBackstep(rel string) bool {
	// normalize to / and check for ../
	return rel == ".." || strings.HasPrefix(filepath.ToSlash(rel), "../")
}

// sanitizedOptionsForLogging will return a comma separated string containing
// options and sensitiveOptions. Each entry in sensitiveOptions will be
// replaced with the string sensitiveOptionsRemoved
// e.g. o1,o2,<masked>,<masked>
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
