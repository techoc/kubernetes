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

// FakeMounter 在测试中实现 mount.Interface。
// 本类型用内存中的挂载表替代真实系统调用，让测试无需 root 权限即可复现挂载状态。
type FakeMounter struct {
	// MountPoints 保存当前可见的挂载点，测试可直接预置或核对。
	MountPoints []MountPoint
	log         []FakeAction
	// MountCheckErrors 注入指定路径的检查错误，用于模拟挂载点失效等真实文件系统才有的异常路径。
	MountCheckErrors map[string]error
	// mutex 保护所有可变状态，避免并发测试共享 mounter 时触发数据竞争。
	mutex sync.Mutex
	// UnmountFunc 是卸载前回调，供测试在真正移除挂载点前插入断言或模拟失败。
	UnmountFunc UnmountFunc
	// skipMountPointCheck 为需要跳过挂载点探测的测试留出开关，避免依赖真实文件系统状态。
	skipMountPointCheck bool
}

// UnmountFunc 是 Unmount 执行期间调用的回调，用于注入副作用、验证调用顺序或返回自定义错误。
type UnmountFunc func(path string) error

var _ Interface = &FakeMounter{}

const (
	// FakeActionMount 表示挂载操作，供测试直接断言 FakeAction.Action。
	FakeActionMount = "mount"
	// FakeActionUnmount 表示卸载操作，供测试直接断言 FakeAction.Action。
	FakeActionUnmount = "unmount"
)

// FakeAction 记录每次假挂载或假卸载，测试据此核对调用方是否按预期发起操作。
type FakeAction struct {
	Action string // 值为 "mount" 或 "unmount"
	Target string // 挂载与卸载操作均适用的目标路径
	Source string // 仅适用于挂载操作的源路径
	FSType string // 仅适用于挂载操作的文件系统类型
}

// NewFakeMounter 返回实现 Interface 的 FakeMounter，并从传入的初始挂载表开始测试。
func NewFakeMounter(mps []MountPoint) *FakeMounter {
	return &FakeMounter{
		MountPoints: mps,
	}
}

// WithSkipMountPointCheck 启用跳过挂载点检查并返回自身，便于测试链式装配 mounter。
func (f *FakeMounter) WithSkipMountPointCheck() *FakeMounter {
	f.skipMountPointCheck = true
	return f
}

// ResetLog 清空 FakeMounter 的全部操作日志；清空前加锁以匹配并发写入。
func (f *FakeMounter) ResetLog() {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	f.log = []FakeAction{}
}

// GetLog 返回 mounter 记录的操作日志切片；调用方应只读，避免与内部状态发生数据竞争。
func (f *FakeMounter) GetLog() []FakeAction {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.log
}

// Mount 记录挂载事件并更新 FakeMounter 的内存挂载点；敏感选项走单独入口，普通挂载只需转发。
func (f *FakeMounter) Mount(source string, target string, fstype string, options []string) error {
	return f.MountSensitive(source, target, fstype, options, nil /* sensitiveOptions */)
}

// MountSensitive 记录挂载事件并更新内存挂载点。
// sensitiveOptions 与普通选项分开传递，只写入内存挂载表而不进入操作日志，避免测试日志泄露口令。
func (f *FakeMounter) MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	opts := []string{}

	for _, option := range options {
		// 查找 bind 选项。
		if option == "bind" {
			// 为模拟 Linux 行为，绑定挂载必须使用原挂载点的设备作为源。
			// 例如依次执行：
			//      $ mount /dev/sda /mnt/test
			//      $ mount -o bind /mnt/test /mnt/bound
			// /proc/mount 中会记录：
			// /dev/sda /mnt/test
			// /dev/sda /mnt/bound
			// 而不是 /mnt/test /mnt/bound，因此这里用 /dev/sda 替代 /mnt/test。
			for _, mnt := range f.MountPoints {
				if source == mnt.Path {
					source = mnt.Device
					break
				}
			}
		}
		// 复用 MountPoint.Opts 字段，使包含 ro 的挂载在内存表中呈现为只读。
		opts = append(opts, option)
	}

	// 目标为软链接时解析其实际路径；解析失败则保留原路径。
	absTarget, err := filepath.EvalSymlinks(target)
	if err != nil {
		absTarget = target
	}
	f.MountPoints = append(f.MountPoints, MountPoint{Device: source, Path: absTarget, Type: fstype, Opts: append(opts, sensitiveOptions...)})
	klog.V(5).Infof("Fake mounter: mounted %s to %s", source, absTarget)
	f.log = append(f.log, FakeAction{Action: FakeActionMount, Target: absTarget, Source: source, FSType: fstype})
	return nil
}

// MountSensitiveWithoutSystemd 忽略 systemd 差异并转交通用挂载路径。
// 假实现当前也不保留 sensitiveOptions，与既有行为保持一致。
func (f *FakeMounter) MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	return f.MountSensitive(source, target, fstype, options, nil /* sensitiveOptions */)
}

// MountSensitiveWithoutSystemdWithMountFlags 忽略 systemd 与挂载标志差异，转交通用实现。
// 假实现当前也不保留 sensitiveOptions。
func (f *FakeMounter) MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error {
	return f.MountSensitive(source, target, fstype, options, nil /* sensitiveOptions */)
}

// Unmount 记录卸载事件并更新 FakeMounter 的内存挂载点。
// 同步清理指定的检查错误，避免残留错误污染后续断言。
func (f *FakeMounter) Unmount(target string) error {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	// 目标为软链接时解析其实际路径；解析失败则保留原路径。
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
			// 不复制到新挂载点列表；其余挂载点仅保留设备、路径和类型，不保留选项。
			continue
		}
		newMountpoints = append(newMountpoints, MountPoint{Device: mp.Device, Path: mp.Path, Type: mp.Type})
	}
	f.MountPoints = newMountpoints
	f.log = append(f.log, FakeAction{Action: FakeActionUnmount, Target: absTarget})
	delete(f.MountCheckErrors, target)
	return nil
}

// List 返回 FakeMounter 当前的全部内存挂载点。
// 假实现永不失败，直接暴露当前快照供测试核对。
func (f *FakeMounter) List() ([]MountPoint, error) {
	f.mutex.Lock()
	defer f.mutex.Unlock()

	return f.MountPoints, nil
}

// IsLikelyNotMountPoint 通过判断文件解析后的路径是否在内存挂载表中来确定它是否可能不是挂载点。
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

	// 文件为软链接时解析其实际路径；解析失败则保留原路径。
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

// CanSafelySkipMountPointCheck 返回测试开关，供调用方决定是否可省略挂载点探测。
func (f *FakeMounter) CanSafelySkipMountPointCheck() bool {
	return f.skipMountPointCheck
}

// IsMountPoint 复用 IsLikelyNotMountPoint 的反向判定，避免假实现中的两份逻辑产生分歧。
func (f *FakeMounter) IsMountPoint(file string) (bool, error) {
	notMnt, err := f.IsLikelyNotMountPoint(file)
	if err != nil {
		return false, err
	}
	return !notMnt, nil
}

// GetMountRefs 查找指定路径对应设备的所有其它挂载引用并返回路径列表。
// 按设备名比对内存挂载表；软链接解析失败时保留原路径，因为假实现并未创建实际文件。
func (f *FakeMounter) GetMountRefs(pathname string) ([]string, error) {
	realpath, err := filepath.EvalSymlinks(pathname)
	if err != nil {
		// 假实现没有创建实际文件，因此解析失败时保留原路径。
		realpath = pathname
	}
	return getMountRefsByDev(f, realpath)
}
