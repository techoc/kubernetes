/*
Copyright 2015 The Kubernetes Authors.

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
	"os"
	"path/filepath"
	"sync"

	"k8s.io/klog/v2"
)

// FakeMounter implements mount.Interface for tests.
// 本类型用内存中的挂载表替代真实系统调用，让测试无需 root 权限即可复现挂载状态。
type FakeMounter struct {
	MountPoints []MountPoint
	log         []FakeAction
	// Error to return for a path when calling IsLikelyNotMountPoint
	// 注入检查错误，用于模拟挂载点失效等真实文件系统才有的异常路径。
	MountCheckErrors map[string]error
	// Some tests run things in parallel, make sure the mounter does not produce
	// any golang's DATA RACE warnings.
	// 并发测试共享同一个 mounter，故所有可变状态都需由此锁保护。
	mutex sync.Mutex
	// 卸载前回调，供测试在真正移除挂载点前插入断言或模拟失败。
	UnmountFunc UnmountFunc
	// 为需要跳过挂载点探测的测试留出开关，避免依赖真实文件系统状态。
	skipMountPointCheck bool
}

// UnmountFunc is a function callback to be executed during the Unmount() call.
// 允许测试在卸载时注入副作用，例如验证调用顺序或返回自定义错误。
type UnmountFunc func(path string) error

var _ Interface = &FakeMounter{}

const (
	// FakeActionMount is the string for specifying mount as FakeAction.Action
	// 用字符串而非枚举，便于测试直接断言日志内容。
	FakeActionMount = "mount"
	// FakeActionUnmount is the string for specifying unmount as FakeAction.Action
	// 同上，保持日志表示简单可读。
	FakeActionUnmount = "unmount"
)

// FakeAction objects are logged every time a fake mount or unmount is called.
// 记录操作内容，测试据此核对调用方是否按预期发起挂载/卸载。
type FakeAction struct {
	Action string // "mount" or "unmount"
	Target string // applies to both mount and unmount actions
	Source string // applies only to "mount" actions
	FSType string // applies only to "mount" actions
}

// NewFakeMounter returns a FakeMounter struct that implements Interface and is
// suitable for testing purposes.
// 传入初始挂载表，让测试从预置的挂载状态出发。
func NewFakeMounter(mps []MountPoint) *FakeMounter {
	return &FakeMounter{
		MountPoints: mps,
	}
}

// 链式设置跳过挂载点检查的开关，方便测试一行内装配 mounter。
func (f *FakeMounter) WithSkipMountPointCheck() *FakeMounter {
	f.skipMountPointCheck = true
	return f
}

// ResetLog clears all the log entries in FakeMounter
// 让多个断言阶段互不干扰，清空前同样加锁以匹配并发写入。
func (f *FakeMounter) ResetLog() {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	f.log = []FakeAction{}
}

// GetLog returns the slice of FakeActions taken by the mounter
// 返回内部切片引用，调用方只读即可，避免拷贝开销。
func (f *FakeMounter) GetLog() []FakeAction {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.log
}

// Mount records the mount event and updates the in-memory mount points for FakeMounter
// 敏感选项走单独入口，普通挂载只需转发即可。
func (f *FakeMounter) Mount(source string, target string, fstype string, options []string) error {
	return f.MountSensitive(source, target, fstype, options, nil /* sensitiveOptions */)
}

// Mount records the mount event and updates the in-memory mount points for FakeMounter
// sensitiveOptions to be passed in a separate parameter from the normal
// mount options and ensures the sensitiveOptions are never logged. This
// method should be used by callers that pass sensitive material (like
// passwords) as mount options.
// 敏感选项只进入内存挂载表，不写入操作日志，避免测试日志泄露口令。
func (f *FakeMounter) MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	opts := []string{}

	for _, option := range options {
		// find 'bind' option
		if option == "bind" {
			// This is a bind-mount. In order to mimic linux behaviour, we must
			// use the original device of the bind-mount as the real source.
			// E.g. when mounted /dev/sda like this:
			//      $ mount /dev/sda /mnt/test
			//      $ mount -o bind /mnt/test /mnt/bound
			// then /proc/mount contains:
			// /dev/sda /mnt/test
			// /dev/sda /mnt/bound
			// (and not /mnt/test /mnt/bound)
			// I.e. we must use /dev/sda as source instead of /mnt/test in the
			// bind mount.
			for _, mnt := range f.MountPoints {
				if source == mnt.Path {
					source = mnt.Device
					break
				}
			}
		}
		// reuse MountPoint.Opts field to mark mount as readonly
		opts = append(opts, option)
	}

	// If target is a symlink, get its absolute path
	absTarget, err := filepath.EvalSymlinks(target)
	if err != nil {
		absTarget = target
	}
	f.MountPoints = append(f.MountPoints, MountPoint{Device: source, Path: absTarget, Type: fstype, Opts: append(opts, sensitiveOptions...)})
	klog.V(5).Infof("Fake mounter: mounted %s to %s", source, absTarget)
	f.log = append(f.log, FakeAction{Action: FakeActionMount, Target: absTarget, Source: source, FSType: fstype})
	return nil
}

// 假实现没有 systemd 概念，忽略该差异直接走通用挂载路径。
func (f *FakeMounter) MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	return f.MountSensitive(source, target, fstype, options, nil /* sensitiveOptions */)
}

// 同理，挂载标志在这里不影响内存状态，转交通用实现即可。
func (f *FakeMounter) MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error {
	return f.MountSensitive(source, target, fstype, options, nil /* sensitiveOptions */)
}

// Unmount records the unmount event and updates the in-memory mount points for FakeMounter
// 同步清理对应的检查错误，避免残留错误污染后续断言。
func (f *FakeMounter) Unmount(target string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	// If target is a symlink, get its absolute path
	absTarget, err := filepath.EvalSymlinks(target)
	if err != nil {
		absTarget = target
	}

	newMountpoints := []MountPoint{}
	for _, mp := range f.MountPoints {
		if mp.Path == absTarget {
			if f.UnmountFunc != nil {
				err := f.UnmountFunc(absTarget)
				if err != nil {
					return err
				}
			}
			klog.V(5).Infof("Fake mounter: unmounted %s from %s", mp.Device, absTarget)
			// Don't copy it to newMountpoints
			continue
		}
		newMountpoints = append(newMountpoints, MountPoint{Device: mp.Device, Path: mp.Path, Type: mp.Type})
	}
	f.MountPoints = newMountpoints
	f.log = append(f.log, FakeAction{Action: FakeActionUnmount, Target: absTarget})
	delete(f.MountCheckErrors, target)
	return nil
}

// List returns all the in-memory mountpoints for FakeMounter
// 假实现永不失败，直接暴露当前快照供测试核对。
func (f *FakeMounter) List() ([]MountPoint, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.MountPoints, nil
}

// IsLikelyNotMountPoint determines whether a path is a mountpoint by checking
// if the absolute path to file is in the in-memory mountpoints
// 先比对注入错误，再查内存表，以便测试覆盖探测失败的分支。
func (f *FakeMounter) IsLikelyNotMountPoint(file string) (bool, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	err := f.MountCheckErrors[file]
	if err != nil {
		return false, err
	}

	_, err = os.Stat(file)
	if err != nil {
		return true, err
	}

	// If file is a symlink, get its absolute path
	absFile, err := filepath.EvalSymlinks(file)
	if err != nil {
		absFile = file
	}

	for _, mp := range f.MountPoints {
		if mp.Path == absFile {
			klog.V(5).Infof("isLikelyNotMountPoint for %s: mounted %s, false", file, mp.Path)
			return false, nil
		}
	}
	klog.V(5).Infof("isLikelyNotMountPoint for %s: true", file)
	return true, nil
}

// 暴露开关，供调用方决定是否可省略挂载点探测。
func (f *FakeMounter) CanSafelySkipMountPointCheck() bool {
	return f.skipMountPointCheck
}

// 复用反向判定，避免两份逻辑在假实现里产生分歧。
func (f *FakeMounter) IsMountPoint(file string) (bool, error) {
	notMnt, err := f.IsLikelyNotMountPoint(file)
	if err != nil {
		return false, err
	}
	return !notMnt, nil
}

// GetMountRefs finds all mount references to the path, returns a
// list of paths.
// 按设备号比对内存挂载表，模拟同一块盘的多处引用。
func (f *FakeMounter) GetMountRefs(pathname string) ([]string, error) {
	realpath, err := filepath.EvalSymlinks(pathname)
	if err != nil {
		// Ignore error in FakeMounter, because we actually didn't create files.
		realpath = pathname
	}
	return getMountRefsByDev(f, realpath)
}
