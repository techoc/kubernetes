//go:build !linux && !windows

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

// 本文件为不支持的平台提供桩实现：接口必须完整、操作一律报错，
// 这样上层无需用构建标签分支处理平台差异。

package mount

import (
	"errors"
)

// Mounter 在非 Linux、非 Windows 平台实现 mount.Interface。
// 保留 mounterPath 只是为了与其他平台的构造签名一致，实际不会被使用。
type Mounter struct {
	mounterPath string
}

// 统一用一个错误值，调用方可用 errors.Is 判断"平台不支持"并给出明确提示。
var errUnsupported = errors.New("util/mount on this platform is not supported")

// New 返回当前系统的 mount.Interface，并允许覆盖默认 mounter 行为。
// 仍然接收并保存 mounterPath，保证各平台 New 签名一致，调用方无需条件编译。
func New(mounterPath string) Interface {
	return &Mounter{
		mounterPath: mounterPath,
	}
}

// Mount 在不支持的平台上始终返回错误。
// 明确报错好过返回 nil，避免上层误以为卷已经挂上。
func (mounter *Mounter) Mount(source string, target string, fstype string, options []string) error {
	return errUnsupported
}

// MountSensitive 在不支持的平台上始终返回错误。
// 与 Mount 保持同样语义，避免调用方在两个入口间得到不同结果。
func (mounter *Mounter) MountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	return errUnsupported
}

// MountSensitiveWithoutSystemd 在不支持的平台上始终返回错误。
// systemd 只是挂载方式之一，不支持平台上连探测都不做，直接拒绝。
func (mounter *Mounter) MountSensitiveWithoutSystemd(source string, target string, fstype string, options []string, sensitiveOptions []string) error {
	return errUnsupported
}

// MountSensitiveWithoutSystemdWithMountFlags 在不支持的平台上始终返回错误。
// 参数再多也不执行；保留完整签名是为了满足接口实现要求。
func (mounter *Mounter) MountSensitiveWithoutSystemdWithMountFlags(source string, target string, fstype string, options []string, sensitiveOptions []string, mountFlags []string) error {
	return errUnsupported
}

// Unmount 在不支持的平台上始终返回错误。
// 假装卸载成功会让上层跳过必要的清理，所以这里也明确失败。
func (mounter *Mounter) Unmount(target string) error {
	return errUnsupported
}

// List 在不支持的平台上始终返回错误。
// 返回空切片加错误，避免调用方忽略错误后拿到看似正常的挂载表。
func (mounter *Mounter) List() ([]MountPoint, error) {
	return []MountPoint{}, errUnsupported
}

// IsLikelyNotMountPoint 在不支持的平台上始终返回错误。
// 返回 true 加错误，调用方必须先检查错误，不能把 true 当成确定结论。
func (mounter *Mounter) IsLikelyNotMountPoint(file string) (bool, error) {
	return true, errUnsupported
}

// CanSafelySkipMountPointCheck 在不支持的平台上始终返回 false。
// 没有平台能力可供信任，保守地要求调用方自己做检查。
func (mounter *Mounter) CanSafelySkipMountPointCheck() bool {
	return false
}

// IsMountPoint 判断目录是否为挂载点。
// 在不支持的平台上始终返回错误，因为无法读取必要的系统状态。
func (mounter *Mounter) IsMountPoint(file string) (bool, error) {
	return false, errUnsupported
}

// GetMountRefs 在不支持的平台上始终返回错误。
// 返回 nil 而非空切片，强调这是查询失败，不是“没有引用”。
func (mounter *Mounter) GetMountRefs(pathname string) ([]string, error) {
	return nil, errUnsupported
}

// 本平台没有格式化能力，退化为直接调底层 Mount；后者会返回 errUnsupported。
// 敏感选项不参与：没有实际挂载动作，也就没有需要脱敏的日志。
func (mounter *SafeFormatAndMount) formatAndMountSensitive(source string, target string, fstype string, options []string, sensitiveOptions []string, formatOptions []string) error {
	return mounter.Interface.Mount(source, target, fstype, options)
}

// 无法探测磁盘格式，true 只是占位；真正有意义的是同时返回的错误。
func (mounter *SafeFormatAndMount) diskLooksUnformatted(disk string) (bool, error) {
	return true, errUnsupported
}

// SafeFormatAndMount.IsMountPoint 判断目录是否为挂载点。
// 与 Mounter 的同名方法保持一致，在不支持的平台上始终返回错误。
func (mounter *SafeFormatAndMount) IsMountPoint(file string) (bool, error) {
	return false, errUnsupported
}
