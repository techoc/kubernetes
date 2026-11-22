/*
Copyright 2025 The Kubernetes Authors.

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

// Package compatibility 管理组件的有效版本信息：二进制版本（BinaryVersion）、
// 用户请求模拟的版本（EmulationVersion）以及最低兼容版本（MinCompatibilityVersion）。
// 这些值统一由 --emulated-version / --min-compatibility-version 等 flag 驱动，
// 用于在同一个二进制上模拟多个 Kubernetes 版本的行为。

package compatibility

import (
	"fmt"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/util/version"
	apimachineryversion "k8s.io/apimachinery/pkg/version"
	baseversion "k8s.io/component-base/version"
)

// EffectiveVersion 保存组件的全部版本信息。
type EffectiveVersion interface {
	// BinaryVersion 是组件二进制的版本，绑定到具体的发布版本。
	BinaryVersion() *version.Version
	// EmulationVersion 是组件模拟其能力（API、feature 等）所对应的版本。
	// 当其与 BinaryVersion 不同时，组件按该版本而不是二进制版本来行事。
	EmulationVersion() *version.Version
	// MinCompatibilityVersion 是组件兼容的最低版本（涉及存储版本、校验规则等）。
	MinCompatibilityVersion() *version.Version
	// EqualTo 判断两个 EffectiveVersion 的三个版本是否全部相等。
	EqualTo(other EffectiveVersion) bool
	// String 返回便于日志与报错阅读的版本摘要。
	String() string
	// Validate 校验各版本是否落在允许范围内。
	Validate() []error
	// AllowedEmulationVersionRange 返回模拟版本的允许区间，仅用于文档与帮助信息。
	AllowedEmulationVersionRange() string
	// AllowedMinCompatibilityVersionRange 返回最低兼容版本的允许区间，仅用于文档与帮助信息。
	AllowedMinCompatibilityVersionRange() string

	// Info 返回组件的版本信息结构，供 --version 等输出使用。
	Info() *apimachineryversion.Info
}

// MutableEffectiveVersion 在 EffectiveVersion 之上增加设置方法，
// 供 flag 解析阶段写入模拟版本与最低兼容版本。
type MutableEffectiveVersion interface {
	EffectiveVersion
	SetEmulationVersion(emulationVersion *version.Version)
	SetMinCompatibilityVersion(minCompatibilityVersion *version.Version)
}

type effectiveVersion struct {
	// 为 true 时 BinaryVersion() 总是返回当前二进制版本。
	useDefaultBuildBinaryVersion atomic.Bool
	// 保存最近一次 Set 的二进制版本。
	binaryVersion atomic.Pointer[version.Version]
	// 用户设置的 emulationVersion 只应包含主、次版本；
	// 测试中它可能等于二进制版本或被直接设置，从而带 "alpha" 预发布标记，
	// 以便在清理过期 API 期间继续提供服务。
	emulationVersion atomic.Pointer[version.Version]
	// minCompatibilityVersion 只应包含主、次版本。
	minCompatibilityVersion atomic.Pointer[version.Version]
	// emulationVersionFloor 是允许的最小模拟版本；nil 表示不设下限。
	emulationVersionFloor *version.Version
	// minCompatibilityVersionFloor 是允许的最小最低兼容版本；nil 表示不设下限。
	minCompatibilityVersionFloor *version.Version
}

// BinaryVersion 返回当前生效的二进制版本；动态版本模式下总是重新读取构建信息。
func (m *effectiveVersion) BinaryVersion() *version.Version {
	if m.useDefaultBuildBinaryVersion.Load() {
		return defaultBuildBinaryVersion()
	}
	return m.binaryVersion.Load()
}

// EmulationVersion 返回用户请求模拟的版本。
func (m *effectiveVersion) EmulationVersion() *version.Version {
	return m.emulationVersion.Load()
}

// MinCompatibilityVersion 返回组件兼容的最低版本。
func (m *effectiveVersion) MinCompatibilityVersion() *version.Version {
	return m.minCompatibilityVersion.Load()
}

// EqualTo 比较三个版本是否全部相等；任一字段为 nil 时可能触发 panic，
// 因此调用方应确保对象已正确初始化。
func (m *effectiveVersion) EqualTo(other EffectiveVersion) bool {
	return m.BinaryVersion().EqualTo(other.BinaryVersion()) && m.EmulationVersion().EqualTo(other.EmulationVersion()) && m.MinCompatibilityVersion().EqualTo(other.MinCompatibilityVersion())
}

// String 输出 "{BinaryVersion: ..., EmulationVersion: ..., MinCompatibilityVersion: ...}"，
// 便于在日志中一眼识别版本关系。
func (m *effectiveVersion) String() string {
	if m == nil {
		return "<nil>"
	}
	return fmt.Sprintf("{BinaryVersion: %s, EmulationVersion: %s, MinCompatibilityVersion: %s}",
		m.BinaryVersion().String(), m.EmulationVersion().String(), m.MinCompatibilityVersion().String())
}

// majorMinor 去掉版本中的 patch 与预发布信息，只保留主、次版本。
// 模拟版本与兼容版本只按主次版本比较，patch 级别差异不参与兼容判断。
func majorMinor(ver *version.Version) *version.Version {
	if ver == nil {
		return ver
	}
	return version.MajorMinor(ver.Major(), ver.Minor())
}

// SetEmulationVersion 设置模拟版本，并把最低兼容版本默认收敛为“模拟版本减一个次版本”。
// 若该默认值低于允许下限，则截断到下限，避免产生非法配置。
func (m *effectiveVersion) SetEmulationVersion(emulationVersion *version.Version) {
	m.emulationVersion.Store(majorMinor(emulationVersion))
	// 默认 minCompatibilityVersion 为 emulationVersion - 1（若可行）。
	minCompatibilityVersion := majorMinor(emulationVersion.SubtractMinor(1))
	if minCompatibilityVersion.LessThan(m.minCompatibilityVersionFloor) {
		minCompatibilityVersion = m.minCompatibilityVersionFloor
	}
	m.minCompatibilityVersion.Store(minCompatibilityVersion)
}

// SetMinCompatibilityVersion 设置最低兼容版本；必须先调用 SetEmulationVersion，
// 否则随后的 SetEmulationVersion 会按默认规则覆盖这里的值。
func (m *effectiveVersion) SetMinCompatibilityVersion(minCompatibilityVersion *version.Version) {
	m.minCompatibilityVersion.Store(majorMinor(minCompatibilityVersion))
}

// AllowedEmulationVersionRange 返回 "[下限..二进制版本](default:当前值)" 形式的可读区间。
func (m *effectiveVersion) AllowedEmulationVersionRange() string {
	binaryVersion := m.BinaryVersion()
	if binaryVersion == nil {
		return ""
	}

	// 区间展示时不考虑 patch 版本。
	binaryVersion = version.MajorMinor(binaryVersion.Major(), binaryVersion.Minor())

	floor := m.emulationVersionFloor
	if floor == nil {
		floor = version.MajorMinor(0, 0)
	}

	return fmt.Sprintf("%s..%s(default:%s)", floor.String(), binaryVersion.String(), m.EmulationVersion().String())
}

// AllowedMinCompatibilityVersionRange 返回最低兼容版本的允许区间。
func (m *effectiveVersion) AllowedMinCompatibilityVersionRange() string {
	binaryVersion := m.BinaryVersion()
	if binaryVersion == nil {
		return ""
	}

	// 区间展示时不考虑 patch 版本。
	binaryVersion = version.MajorMinor(binaryVersion.Major(), binaryVersion.Minor())

	floor := m.minCompatibilityVersionFloor
	if floor == nil {
		floor = version.MajorMinor(0, 0)
	}

	return fmt.Sprintf("%s..%s(default:%s)", floor.String(), binaryVersion.String(), m.MinCompatibilityVersion().String())
}

// Validate 校验版本关系：emulationVersion 必须在 [floor, binaryVersion] 内，
// minCompatibilityVersion 必须在 [min floor, emulationVersion] 内。
func (m *effectiveVersion) Validate() []error {
	var errs []error
	// 只校验主、次版本，忽略 patch。
	binaryVersion := m.BinaryVersion().WithPatch(0)
	emulationVersion := m.emulationVersion.Load()
	minCompatibilityVersion := m.minCompatibilityVersion.Load()
	// emulationVersion 只能落在 floor 与 binaryVersion 之间。
	if emulationVersion.GreaterThan(binaryVersion) || emulationVersion.LessThan(m.emulationVersionFloor) {
		errs = append(errs, fmt.Errorf("emulation version %s is not between [%s, %s]", emulationVersion.String(), m.emulationVersionFloor.String(), binaryVersion.String()))
	}
	// minCompatibilityVersion 只能落在 floor 与 emulationVersion 之间。
	if minCompatibilityVersion.GreaterThan(emulationVersion) || minCompatibilityVersion.LessThan(m.minCompatibilityVersionFloor) {
		errs = append(errs, fmt.Errorf("minCompatibilityVersion version %s is not between [%s, %s]", minCompatibilityVersion.String(), m.minCompatibilityVersionFloor.String(), emulationVersion.String()))
	}
	return errs
}

// Info 返回组件的版本信息；二进制版本为 nil 时返回 nil。
// 信息以构建时的版本为基底，再按有效版本覆盖主次版本字段。
func (m *effectiveVersion) Info() *apimachineryversion.Info {
	binVer := m.BinaryVersion()
	if binVer == nil {
		return nil
	}

	info := baseversion.Get()
	info.Major = version.Itoa(binVer.Major())
	info.Minor = version.Itoa(binVer.Minor())
	// 保留构建系统提供的完整 GitVersion（含预发布/构建元数据）；
	// 只有缺失时才退回到二进制版本字符串。
	if info.GitVersion == "" {
		info.GitVersion = binVer.String()
	}

	if ev := m.EmulationVersion(); ev != nil {
		info.EmulationMajor = version.Itoa(ev.Major())
		info.EmulationMinor = version.Itoa(ev.Minor())
	}

	if mcv := m.MinCompatibilityVersion(); mcv != nil {
		info.MinCompatibilityMajor = version.Itoa(mcv.Major())
		info.MinCompatibilityMinor = version.Itoa(mcv.Minor())
	}

	return &info
}

// NewEffectiveVersion 由 binaryVersion 创建 MutableEffectiveVersion。
// 若 useDefaultBuildBinaryVersion 为 true，BinaryVersion() 总是返回当前二进制版本；
// 该模式只适用于二进制版本动态变化的场景，否则应使用 false 或 NewEffectiveVersionFromString。
// 默认的模拟版本等于二进制版本，默认最低兼容版本为二进制版本减一个次版本。
func NewEffectiveVersion(binaryVersion *version.Version, useDefaultBuildBinaryVersion bool, emulationVersionFloor, minCompatibilityVersionFloor *version.Version) MutableEffectiveVersion {
	effective := &effectiveVersion{
		emulationVersionFloor:        emulationVersionFloor,
		minCompatibilityVersionFloor: minCompatibilityVersionFloor,
	}
	compatVersion := binaryVersion.SubtractMinor(1)
	effective.binaryVersion.Store(binaryVersion)
	effective.useDefaultBuildBinaryVersion.Store(useDefaultBuildBinaryVersion)
	effective.SetEmulationVersion(binaryVersion)
	effective.SetMinCompatibilityVersion(compatVersion)
	return effective
}

// NewEffectiveVersionFromString 由版本字符串创建 MutableEffectiveVersion。
// 空字符串返回未初始化的对象（BinaryVersion 为 nil）；floor 参数为空时下限取 0.0。
func NewEffectiveVersionFromString(binaryVer, emulationVerFloor, minCompatibilityVerFloor string) MutableEffectiveVersion {
	if binaryVer == "" {
		return &effectiveVersion{}
	}
	binaryVersion := version.MustParse(binaryVer)
	emulationVersionFloor := version.MajorMinor(0, 0)
	if emulationVerFloor != "" {
		emulationVersionFloor = version.MustParse(emulationVerFloor)
	}
	minCompatibilityVersionFloor := version.MajorMinor(0, 0)
	if minCompatibilityVerFloor != "" {
		minCompatibilityVersionFloor = version.MustParse(minCompatibilityVerFloor)
	}
	return NewEffectiveVersion(binaryVersion, false, emulationVersionFloor, minCompatibilityVersionFloor)
}

// defaultBuildBinaryVersion 从构建信息中解析当前二进制版本，解析失败即 panic：
// 版本字符串由构建流程注入，缺失或非法属于构建错误。
func defaultBuildBinaryVersion() *version.Version {
	verInfo := baseversion.Get()
	return version.MustParse(verInfo.String())
}
