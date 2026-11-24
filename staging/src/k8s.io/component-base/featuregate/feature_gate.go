/*
Copyright 2016 The Kubernetes Authors.

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

// Package featuregate 实现 Kubernetes 的 feature gate 机制：按特性名开启或关闭实验性行为。
//
// Gate 支持版本化定义（VersionedSpecs），结合 emulationVersion 与 minCompatibilityVersion
// 选择当前生效的规格，并可通过 --feature-gates 或 SetFromMap 覆盖默认值。
// 未注册的特性名会被拒绝，避免拼写错误静默生效。
package featuregate

import (
	"context"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/spf13/pflag"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/naming"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/version"
	featuremetrics "k8s.io/component-base/metrics/prometheus/feature"
	baseversion "k8s.io/component-base/version"
	"k8s.io/klog/v2"
)

// Feature 是特性开关的名称，同时也是 --feature-gates 中的键。
type Feature string

const (
	// flagName 是命令行上承载 gate 集合的 flag 名。
	flagName = "feature-gates"

	// allAlphaGate 是 Alpha 特性的全局开关。单个特性的键值
	// 可以覆盖 allAlphaGate 设置的默认值。例如：
	//   AllAlpha=false,NewFeature=true  最终 NewFeature=true
	//   AllAlpha=true,NewFeature=false  最终 NewFeature=false
	allAlphaGate Feature = "AllAlpha"

	// allBetaGate 是 Beta 特性的全局开关。单个特性的键值
	// 可以覆盖 allBetaGate 设置的默认值。例如：
	//   AllBeta=false,NewFeature=true  最终 NewFeature=true
	//   AllBeta=true,NewFeature=false 	最终 NewFeature=false
	allBetaGate Feature = "AllBeta"
)

var (
	// 通用特性定义。
	// AllAlpha/AllBeta 是虚拟 gate，本身不承载行为，只在解析时批量设置对应成熟度的特性。
	defaultFeatures = map[Feature]VersionedSpecs{
		allAlphaGate: {{Default: false, PreRelease: Alpha, Version: version.MajorMinor(0, 0)}},
		allBetaGate:  {{Default: false, PreRelease: Beta, Version: version.MajorMinor(0, 0)}},
	}

	// 少数特性的特殊处理逻辑。
	// 只对未被显式设置的特性生效，因此显式 key 总是优先于 AllAlpha/AllBeta。
	specialFeatures = map[Feature]func(known map[Feature]VersionedSpecs, enabled map[Feature]bool, val bool, emuVer, minCompatVer *version.Version){
		allAlphaGate: setUnsetAlphaGates,
		allBetaGate:  setUnsetBetaGates,
	}
)

// FeatureSpec 描述一个特性在一个版本区间内的默认值、成熟度和兼容性要求。
// 多个 FeatureSpec 按 Version 升序排列构成 VersionedSpecs。
type FeatureSpec struct {
	// Default 表示该特性的默认启用状态。
	Default bool
	// LockToDefault 表示该特性被锁定为默认值，不能修改。
	LockToDefault bool
	// PreRelease 表示该特性当前的成熟度级别。
	PreRelease prerelease
	// Version 表示此 FeatureSpec 开始生效的最早版本。
	// 如果一个特性定义了多个 FeatureSpec，则选择版本最高且
	// 不高于组件当前有效版本的那一个。
	Version *version.Version
	// MinCompatibilityVersion 表示此特性规格所兼容的最低版本。
	// 组件的最低兼容版本必须大于或等于该版本，此规格才会生效。
	// 这样，存在版本偏差兼容性影响的特性可以先以 Beta 阶段引入，
	// 但只有最低兼容版本达到要求后，才会默认启用。
	// 如果未指定，则继承前一个特性阶段的值。
	// 如果指定该字段，则前面必须有一个 Version 相同、Default 不同且未设置 MinCompatibilityVersion 的 FeatureSpec。
	// 换言之，MinCompatibilityVersion 仅用于在同一版本下指定不同的默认值。
	//
	// Version 与 MinCompatibilityVersion 的区别：
	//  - Version 根据服务器的模拟版本决定特性的成熟度及可用性。
	//  - MinCompatibilityVersion 则进一步检查版本兼容性，
	// 确保该服务器与其预期通信的控制平面中的所有服务器兼容。
	//
	// 示例：
	// 	FeatureA: []FeatureSpec{
	// 		{Version: version.MustParse("1.35"), Default: false, PreRelease: Beta},
	// 		{Version: version.MustParse("1.35"), Default: true, PreRelease: Beta, MinCompatibilityVersion: version.MustParse("1.35")},
	// 	}
	// 对于运行在 1.35 版本的服务器：
	//  - 使用默认的 --min-compatibility-version=1.34 时，FeatureA 处于 Beta 阶段且默认关闭。
	//  - 使用 --min-compatibility-version=1.35 时，FeatureA 默认开启。
	MinCompatibilityVersion *version.Version
}

// HelpString 生成特性在 --help 中的说明。
// GA/Deprecated/PreAlpha 不展示：它们要么无需配置，要么在当前版本不可用。
func (spec *FeatureSpec) HelpString(featureName Feature, includeCompatVer bool) string {
	if spec.PreRelease == GA || spec.PreRelease == Deprecated || spec.PreRelease == PreAlpha {
		return ""
	}
	s := fmt.Sprintf("%s=true|false (%s - default=%t)", featureName, spec.PreRelease, spec.Default)
	if includeCompatVer && spec.MinCompatibilityVersion != nil {
		return s + fmt.Sprintf(" if --min-compatibility-version>=%s", spec.MinCompatibilityVersion)
	}
	return s
}

// VersionedSpecs 是一个特性按版本升序排列的规格列表。
type VersionedSpecs []FeatureSpec

// Len/Less/Swap 实现 sort.Interface，供版本化规格排序使用。
func (g VersionedSpecs) Len() int { return len(g) }
func (g VersionedSpecs) Less(i, j int) bool {
	return g[i].Version.LessThan(g[j].Version)
}
func (g VersionedSpecs) Swap(i, j int) { g[i], g[j] = g[j], g[i] }

// PromotionVersionMapping 记录各成熟度对应的推广版本，供文档和工具使用。
type PromotionVersionMapping map[prerelease]string

// prerelease 是特性的成熟度级别；作为独立类型便于在比较和排序中区分。
type prerelease string

const (
	// PreAlpha 表示在当前模拟版本下尚不可用的特性，Enabled 查询返回 false。
	PreAlpha = prerelease("PRE-ALPHA")
	// PreRelease 可选的成熟度取值。
	Alpha = prerelease("ALPHA")
	Beta  = prerelease("BETA")
	GA    = prerelease("")

	// Deprecated
	// 终态：特性已移除，仅为兼容旧配置保留名称并给出告警。
	Deprecated = prerelease("DEPRECATED")
)

// FeatureGate 用于查询指定特性是否已启用。
// 只读视图，组件用它查询开关状态，而不参与注册和配置。
type FeatureGate interface {
	// Enabled 在指定特性已启用时返回 true。
	// 未注册的特性会 panic；这是刻意的，以便尽早暴露拼写错误。
	Enabled(key Feature) bool
	// KnownFeatures 返回描述当前 FeatureGate 已知特性的字符串切片。
	KnownFeatures() []string
	// Dependencies 返回已知特性依赖关系的副本。
	Dependencies() map[Feature][]Feature
	// DeepCopy 返回 FeatureGate 对象的深拷贝，修改副本中的开关不会影响原对象。
	// 这适用于在正式提交配置变更前，先验证配置是否能够适配
	// 潜在的 Feature Gate 变更。
	DeepCopy() MutableVersionedFeatureGate
	// Validate 检查当前模拟版本下的特性开关配置是否合法。
	Validate() []error
}

// MutableFeatureGate 从字符串中解析并保存已知特性的开关配置，
// 字符串格式示例：feature1=true,feature2=false,...。
// 提供注册、修改和 flag 绑定能力；调用方通常只持有 FeatureGate。
type MutableFeatureGate interface {
	FeatureGate

	// AddFlag 向指定 FlagSet 注册用于设置全局特性开关的命令行参数。
	// 注册后 gate 集合被冻结，不能再 Add 新特性。
	AddFlag(fs *pflag.FlagSet)
	// Close 将 closed 设为 true，阻止后续调用 Add 注册新特性。
	Close()
	// Set 解析并保存已知特性的开关配置，
	// 字符串格式示例：feature1=true,feature2=false,...。
	Set(value string) error
	// SetFromMap 从 map[string]bool 中保存已知特性的开关配置；遇到错误时返回错误。
	SetFromMap(m map[string]bool) error
	// Add 向 featureGate 注册特性。
	Add(features map[Feature]FeatureSpec) error
	// GetAll 返回已知特性名称到特性规格映射的副本。
	GetAll() map[Feature]FeatureSpec
	// AddMetrics 添加特性启用状态指标。
	AddMetrics()
	// OverrideDefault 为指定特性已注册的默认值设置局部覆盖，
	// 如果该特性尚未注册（例如尚未调用 Add）、默认值已锁定，或者
	// FeatureGate 已经注册到 FlagSet，则会返回非 nil

	//
	// 当多个组件共同使用某个特性时，其中一个组件可以在运行时覆盖该特性的
	// 默认值，从而先于或晚于其他组件采用新的默认配置。
	// 例如，可以只在少数组件中将新特性的默认值覆盖为 true，
	// 以限制影响范围，而不必同时修改所有使用该特性的组件的默认值。

	OverrideDefault(name Feature, override bool) error
}

// MutableFeatureGateWithLogger 为 [MutableFeatureGate] 中的部分方法提供替代实现，

type MutableFeatureGateWithLogger interface {
	SetWithLogger(logger klog.Logger, value string) error
	SetFromMapWithLogger(logger klog.Logger, m map[string]bool) error
	OverrideDefaultWithLogger(logger klog.Logger, name Feature, override bool) error
}

// MutableVersionedFeatureGate 从字符串中解析并保存已知特性的开关配置，
// 字符串格式示例：feature1=true,feature2=false,...。
// 并根据 FeatureGate 的模拟版本决定特性配置。
// 版本化 gate 同时维护 emulationVersion 与 minCompatibilityVersion，用于版本模拟和跨组件兼容性。
type MutableVersionedFeatureGate interface {
	MutableFeatureGate
	// EmulationVersion 返回 FeatureGate 当前模拟的版本。
	// 设置后，FeatureGate 将依据模拟版本下的特性可用性和预发布阶段，
	// 而不是依据二进制文件自身的版本，决定特性启用或禁用状态。
	EmulationVersion() *version.Version
	// MinCompatibilityVersion 返回当前服务器必须保持兼容的所有控制平面组件中的最低版本。

	// 该版本用于控制存在跨组件兼容性要求的特性，例如集群升级或回滚期间。
	// 如果未显式设置，默认值为 EmulationVersion() 的前一个次版本。
	// 如果 FeatureSpec.MinCompatibilityVersion 高于该版本，对应特性阶段将被禁用。
	MinCompatibilityVersion() *version.Version
	// SetEmulationVersion 覆盖 FeatureGate 的 emulationVersion，
	// 并将 minCompatibilityVersion 设置为模拟版本之前一个次版本。
	// 如果未设置，emulationVersion 将与二进制文件版本相同。
	// 设置后，特性的默认值和可用性将按照模拟版本对应的行为确定。
	SetEmulationVersion(emulationVersion *version.Version) error
	// SetEmulationVersionAndMinCompatibilityVersion 同时覆盖 FeatureGate 的模拟版本和最低兼容版本。
	SetEmulationVersionAndMinCompatibilityVersion(emulationVersion *version.Version, minCompatibilityVersion *version.Version) error
	// GetAllVersioned 返回已知特性名称到版本化特性规格映射的副本。
	GetAllVersioned() map[Feature]VersionedSpecs
	// AddVersioned 向 featureGate 注册版本化特性规格。
	AddVersioned(features map[Feature]VersionedSpecs) error
	// AddDependencies 声明特性之间的依赖关系；必须在所有被引用的特性
	// （依赖方和被依赖方）注册完成后调用，并且不允许出现循环依赖。
	AddDependencies(features map[Feature][]Feature) error
	// OverrideDefaultAtVersion 根据指定版本所处的预发布生命周期，
	// 为指定特性已注册的默认值设置局部覆盖。
	// 如果该特性尚未注册（例如尚未调用 Add）、
	// 默认值已锁定，或者 FeatureGate 已经注册到 FlagSet，则会返回非 nil

	//
	// 当多个组件共同使用某个特性时，其中一个组件可以在运行时覆盖该特性的
	// 默认值，从而先于或晚于其他组件采用新的默认配置。
	// 例如，可以只在少数组件中将新特性的默认值覆盖为 true，
	// 以限制影响范围，而不必同时修改所有使用该特性的组件的默认值。

	OverrideDefaultAtVersion(name Feature, override bool, ver *version.Version) error
	// ExplicitlySet 在特性值由用户显式设置，而不是
	// 根据默认值或特殊特性推导得出时返回 true。
	ExplicitlySet(name Feature) bool
	// ResetFeatureValueToDefault 将特性值重置为默认值。
	ResetFeatureValueToDefault(name Feature) error
	// DeepCopyAndReset 复制 FeatureGate 对象中所有已注册特性、已知特性定义及覆盖配置，
	// 并重置新 FeatureGate 中所有特性的启用状态。
	// 适用于创建新的 FeatureGate 实例，同时避免继承基础实例中的启用配置。
	DeepCopyAndReset() MutableVersionedFeatureGate
}

// MutableVersionedFeatureGateWithLogger 为 [MutableVersionedFeatureGate] 中的部分方法提供替代实现，

type MutableVersionedFeatureGateWithLogger interface {
	SetEmulationVersionWithLogger(logger klog.Logger, emulationVersion *version.Version) error
	SetEmulationVersionAndMinCompatibilityVersionWithLogger(logger klog.Logger, emulationVersion *version.Version, minCompatibilityVersion *version.Version) error
	OverrideDefaultAtVersionWithLogger(logger klog.Logger, name Feature, override bool, ver *version.Version) error
}

// featureGate 实现 FeatureGate 接口，同时实现 pflag.Value 以支持命令行参数解析。
type featureGate struct {
	featureGateName string

	// special 映射 AllAlpha/AllBeta 等需要批量展开的虚拟 gate 到其处理函数。
	special map[Feature]func(map[Feature]VersionedSpecs, map[Feature]bool, bool, *version.Version, *version.Version)

	// lock 用于保护下方字段的写操作。
	// 读路径依赖 atomic.Value，因此锁只保护写入（注册、重新应用）。
	lock sync.Mutex
	// known 保存已注册特性及其版本化规格。
	known atomic.Value
	// enabled 保存 map[Feature]bool，记录解析后的特性启用状态。
	enabled      atomic.Value
	dependencies atomic.Pointer[map[Feature][]Feature]
	// enabledRaw 保存解析得到的原始 map[string]bool。
	// 它保留 AllAlpha 等特殊特性的原始值，
	// 而 enabled 保存展开后的各个特性的实际值。
	// 保留原始输入是 SetEmulationVersion 后重新展开的基础。
	enabledRaw atomic.Value
	// 调用 AddFlag 后 closed 会被设为 true，从而阻止后续调用 Add。
	closed bool
	// queriedFeatures 保存所有通过 Enabled 接口查询过的特性。
	// 调用 SetEmulationVersion 时会重置该集合。
	// 用于在切换模拟版本时告警“已查询过的特性值发生了变化”。
	queriedFeatures         atomic.Value
	emulationVersion        atomic.Pointer[version.Version]
	minCompatibilityVersion atomic.Pointer[version.Version]
}

var _ MutableVersionedFeatureGateWithLogger = &featureGate{}
var _ MutableFeatureGateWithLogger = &featureGate{}

// setUnsetAlphaGates 把尚未显式设置的 alpha 特性设为 val。
// 已存在 enabled 中的项不覆盖，从而保证单特性设置优先于 AllAlpha。
func setUnsetAlphaGates(known map[Feature]VersionedSpecs, enabled map[Feature]bool, val bool, emuVer, minCompatVer *version.Version) {
	for k, v := range known {
		if k == "AllAlpha" || k == "AllBeta" {
			continue
		}
		featureSpec := featureSpecAtEmulationAndMinCompatVersion(v, emuVer, minCompatVer)
		if featureSpec.PreRelease == Alpha {
			if _, found := enabled[k]; !found {
				enabled[k] = val
			}
		}
	}
}

// setUnsetBetaGates 与 setUnsetAlphaGates 相同，作用于 beta 特性。
func setUnsetBetaGates(known map[Feature]VersionedSpecs, enabled map[Feature]bool, val bool, emuVer, minCompatVer *version.Version) {
	for k, v := range known {
		if k == "AllAlpha" || k == "AllBeta" {
			continue
		}
		featureSpec := featureSpecAtEmulationAndMinCompatVersion(v, emuVer, minCompatVer)
		if featureSpec.PreRelease == Beta {
			if _, found := enabled[k]; !found {
				enabled[k] = val
			}
		}
	}
}

// Set、String 和 Type 实现 pflag.Value 接口所需的方法。
var _ pflag.Value = &featureGate{}

// internalPackages 列出生成 featureGate 名称时需要忽略的包。这些包位于常见调用链中，
// 将它们作为名称的一部分没有实际帮助。
var internalPackages = []string{"k8s.io/component-base/featuregate/feature_gate.go"}

// NewVersionedFeatureGate 创建一个 FeatureGate，并将模拟版本设置为传入的版本。
// 等价于调用 NewVersionedFeatureGateWithMinCompatibility(emulationVersion, emulationVersion-1)。
// 后续可以调用 SetEmulationVersion 将模拟版本修改为所需值。
func NewVersionedFeatureGate(emulationVersion *version.Version) *featureGate {
	return NewVersionedFeatureGateWithMinCompatibility(emulationVersion, emulationVersion.SubtractMinor(1))
}

// NewVersionedFeatureGateWithMinCompatibility 用给定的模拟版本和最低兼容版本构造 gate。
// 最低兼容版本默认取模拟版本前一个小版本，符合 Kubernetes 的控制面版本偏移约定。
func NewVersionedFeatureGateWithMinCompatibility(emulationVersion, minCompatibilityVersion *version.Version) *featureGate {
	// 从默认的 AllAlpha/AllBeta 定义开始，调用方再通过 AddVersioned 注册自己的特性。
	known := map[Feature]VersionedSpecs{}
	for k, v := range defaultFeatures {
		known[k] = v
	}

	f := &featureGate{
		// 从调用栈推导名称，便于在日志和错误里区分不同的 gate 实例。
		featureGateName: naming.GetNameFromCallsite(internalPackages...),
		special:         specialFeatures,
	}
	f.known.Store(known)
	f.dependencies.Store(new(map[Feature][]Feature))
	f.enabled.Store(map[Feature]bool{})
	f.enabledRaw.Store(map[string]bool{})
	f.emulationVersion.Store(emulationVersion)
	f.minCompatibilityVersion.Store(minCompatibilityVersion)
	f.queriedFeatures.Store(sets.Set[Feature]{})
	return f
}

// NewFeatureGate 使用当前二进制文件版本创建 FeatureGate。
// 以 DefaultKubeBinaryVersion 作为模拟版本，适合普通组件使用。
func NewFeatureGate() *featureGate {
	binaryVersison := version.MustParse(baseversion.DefaultKubeBinaryVersion)
	return NewVersionedFeatureGate(binaryVersison)
}

// Set 将格式为 "key1=value1,key2=value2,..." 的字符串解析为
// 已知键对应的 map[string]bool；遇到错误时返回错误。
//
//logcheck:context // 请改用 SetWithLogger。
func (f *featureGate) Set(value string) error {
	return f.SetWithLogger(klog.Background(), value)
}

// Set 将格式为 "key1=value1,key2=value2,..." 的字符串解析为
// 已知键对应的 map[string]bool；遇到错误时返回错误。
func (f *featureGate) SetWithLogger(logger klog.Logger, value string) error {
	// WithCallStackHelper 让 klog 输出的调用栈省略本包装层，告警定位到真实调用方。
	helper, logger := logger.WithCallStackHelper()
	helper()

	m := make(map[string]bool)
	for _, s := range strings.Split(value, ",") {
		// 容忍尾随逗号产生的空项。
		if len(s) == 0 {
			continue
		}
		arr := strings.SplitN(s, "=", 2)
		k := strings.TrimSpace(arr[0])
		if len(arr) != 2 {
			return fmt.Errorf("missing bool value for %s", k)
		}
		v := strings.TrimSpace(arr[1])
		boolValue, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("invalid value of %s=%s, err: %v", k, v, err)
		}
		m[k] = boolValue
	}
	return f.SetFromMapWithLogger(logger, m)
}

// Validate 检查当前模拟版本下的特性开关配置是否合法。
// 在真正应用前复用同一套解析/依赖校验逻辑，且不修改任何状态。
func (f *featureGate) Validate() []error {
	f.lock.Lock()
	defer f.lock.Unlock()
	m, ok := f.enabledRaw.Load().(map[string]bool)
	if !ok {
		return []error{fmt.Errorf("cannot cast enabledRaw to map[string]bool")}
	}
	enabled := map[Feature]bool{}
	// 此处禁用日志：当前只做校验，并未真正应用配置，输出日志容易造成误解。
	return f.unsafeSetFromMap(klog.Logger{}, enabled, m, f.EmulationVersion(), f.MinCompatibilityVersion())
}

// unsafeSetFromMap 将 map[string]bool 中已知特性的开关配置写入 enabled 映射。
// 调用方需持有 f.lock 或确保没有并发写；函数名中的 unsafe 即指这一前提。
func (f *featureGate) unsafeSetFromMap(logger klog.Logger, enabled map[Feature]bool, m map[string]bool, emulationVersion, minCompatibilityVersion *version.Version) []error {
	helper, logger := logger.WithCallStackHelper()
	helper()

	var errs []error
	// 复制现有状态。
	known := map[Feature]VersionedSpecs{}
	for k, v := range f.known.Load().(map[Feature]VersionedSpecs) {
		known[k] = v
	}

	for k, v := range m {
		key := Feature(k)
		versionedSpecs, ok := known[key]
		if !ok {
			// 遇到未知特性时立即返回。
			errs = append(errs, fmt.Errorf("unrecognized feature gate: %s", k))
			return errs
		}
		featureSpec := featureSpecAtEmulationAndMinCompatVersion(versionedSpecs, emulationVersion, minCompatibilityVersion)
		if featureSpec.LockToDefault && featureSpec.Default != v {
			errs = append(errs, fmt.Errorf("cannot set feature gate %v to %v, feature is locked to %v", k, v, featureSpec.Default))
			continue
		}
		// 处理 AllAlpha 等特殊特性开关。
		// 特殊 gate 只展开其它特性，自身也记录在 enabled 中以便 String() 回显。
		if fn, found := f.special[key]; found {
			fn(known, enabled, v, emulationVersion, minCompatibilityVersion)
			enabled[key] = v
			continue
		}
		// PreAlpha 特性在模拟版本下尚不存在，显式设置视为错误而非静默忽略。
		if featureSpec.PreRelease == PreAlpha {
			errs = append(errs, fmt.Errorf("cannot set feature gate %v to %v, feature is PreAlpha at emulated version %s, min compatibility version %s", k, v, emulationVersion, minCompatibilityVersion))
			continue
		}
		enabled[key] = v

		if featureSpec.PreRelease == Deprecated {
			logger.Info("Warning: setting deprecated feature gate. It will be removed in a future release.", "featureGate", k, "value", v)
		} else if featureSpec.PreRelease == GA {
			logger.Info("Warning: setting GA feature gate. It will be removed in a future release.", "featureGate", k, "value", v)
		}
	}

	if len(errs) > 0 {
		return errs
	}

	// 如果特性开关配置成功应用，则继续根据依赖关系校验启用状态。
	// 只有在本次输入全部合法后才检查依赖，避免对错误输入连带报出依赖错误。
	dependencies := *f.dependencies.Load()
	for feature, deps := range dependencies {
		if !featureEnabled(feature, enabled, known, emulationVersion, minCompatibilityVersion) {
			continue
		}

		var disabledDeps []Feature
		for _, dep := range deps {
			if !featureEnabled(dep, enabled, known, emulationVersion, minCompatibilityVersion) {
				disabledDeps = append(disabledDeps, dep)
			}
		}
		if len(disabledDeps) > 0 {
			errs = append(errs, fmt.Errorf("%s is enabled, but depends on features that are disabled: %v", feature, disabledDeps))
		}
	}

	return errs
}

// SetFromMap 从 map[string]bool 中保存已知特性的开关配置；遇到错误时返回错误。
//
//logcheck:context // 请改用 SetFromMapWithLogger。
func (f *featureGate) SetFromMap(m map[string]bool) error {
	return f.SetFromMapWithLogger(klog.Background(), m)
}

// SetFromMapWithLogger 使用指定 logger，从 map[string]bool 中保存已知特性的开关配置；遇到错误时返回错误。
func (f *featureGate) SetFromMapWithLogger(logger klog.Logger, m map[string]bool) error {
	helper, logger := logger.WithCallStackHelper()
	helper()

	f.lock.Lock()
	defer f.lock.Unlock()

	// 复制现有状态。
	// 先复制再修改，保证失败时旧状态保持不变（原子值不可原地修改）。
	enabled := map[Feature]bool{}
	for k, v := range f.enabled.Load().(map[Feature]bool) {
		enabled[k] = v
	}
	enabledRaw := map[string]bool{}
	for k, v := range f.enabledRaw.Load().(map[string]bool) {
		enabledRaw[k] = v
	}

	// 首先更新 enabledRaw。
	// 调用 SetFromMap 时，emulationVersion 可能尚未最终确定，因此还无法确定 enabled 的最终状态。
	// 但仍然需要先保存用户传入的开关配置。
	for k, v := range m {
		enabledRaw[k] = v
	}
	f.enabledRaw.Store(enabledRaw)

	errs := f.unsafeSetFromMap(logger, enabled, enabledRaw, f.EmulationVersion(), f.MinCompatibilityVersion())
	if len(errs) == 0 {
		// 提交变更。
		// 仅在所有输入合法时提交，避免部分应用造成状态不一致。
		f.enabled.Store(enabled)
		logger.V(1).Info("Updated", "featureGates", enabled)
	}
	return utilerrors.NewAggregate(errs)
}

// String 返回所有已启用特性开关组成的字符串，格式为 "key1=value1,key2=value2,..."。
// 排序后输出，便于测试比较和 flag 回显稳定。
func (f *featureGate) String() string {
	pairs := []string{}
	for k, v := range f.enabled.Load().(map[Feature]bool) {
		pairs = append(pairs, fmt.Sprintf("%s=%t", k, v))
	}
	sort.Strings(pairs)
	return strings.Join(pairs, ",")
}

func (f *featureGate) Type() string {
	return "mapStringBool"
}

// Add 向 featureGate 注册特性。
func (f *featureGate) Add(features map[Feature]FeatureSpec) error {
	vs := map[Feature]VersionedSpecs{}
	for name, spec := range features {
		// Add 会将 FeatureSpec 的版本统一设为 0.0，使该特性不受模拟版本限制，可以正常启用或禁用。
		spec.Version = version.MajorMinor(0, 0)
		vs[name] = VersionedSpecs{spec}
	}
	return f.AddVersioned(vs)
}

// AddVersioned 向 featureGate 注册版本化特性规格。
// 注册时做完整的一致性校验（版本单调、兼容性版本、成熟度不回退等），
// 以免错误的特性生命周期定义流入发布版本。
func (f *featureGate) AddVersioned(features map[Feature]VersionedSpecs) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.closed {
		return fmt.Errorf("cannot add a feature gate after adding it to the flag set")
	}
	// 复制现有状态。
	known := f.GetAllVersioned()

	for name, specs := range features {
		var completedSpecs VersionedSpecs
		// 校验新特性规格的格式和定义是否合法。
		var lastSpec FeatureSpec
		var wasBeta, wasGA, wasDeprecated, wasLockedToDefault bool
		for i, spec := range specs {
			if spec.Version == nil {
				return fmt.Errorf("feature %q did not provide a version", name)
			}
			if len(spec.Version.Components()) != 2 {
				return fmt.Errorf("feature %q specified patch version: %s", name, spec.Version)
			}
			if spec.MinCompatibilityVersion != nil {
				if i == 0 || !spec.Version.EqualTo(lastSpec.Version) {
					return fmt.Errorf("feature %q specified MinCompatibilityVersion: %s without a preceding entry at the same Version without MinCompatibilityVersion set", name, spec.MinCompatibilityVersion)
				}
				if len(spec.MinCompatibilityVersion.Components()) != 2 {
					return fmt.Errorf("feature %q specified patch MinCompatibilityVersion: %s", name, spec.MinCompatibilityVersion)
				}
				if spec.MinCompatibilityVersion.GreaterThan(spec.Version) {
					return fmt.Errorf("feature %q MinCompatibilityVersion %s greater than Version %s", name, spec.MinCompatibilityVersion, spec.Version)
				}
			}
			// 初始状态即为 Deprecated 的特性必须声明其此前的状态。
			if i == 0 && spec.PreRelease == Deprecated && spec.Version.Minor() != 0 {
				return fmt.Errorf("feature %q introduced as deprecated must provide a 1.0 entry indicating initial state", name)
			}
			if i > 0 {
				// Version 或 MinCompatibilityVersion 至少有一个必须递增。
				if spec.Version.LessThan(lastSpec.Version) {
					return fmt.Errorf("feature %q lists version transitions in decreasing order (%s < %s)", name, spec.Version, lastSpec.Version)
				}
				if spec.MinCompatibilityVersion != nil && lastSpec.MinCompatibilityVersion != nil && spec.MinCompatibilityVersion.LessThan(lastSpec.MinCompatibilityVersion) {
					return fmt.Errorf("feature %q lists min compatibility version transitions in decreasing order (%s < %s)", name, spec.MinCompatibilityVersion, lastSpec.MinCompatibilityVersion)
				}
				if spec.Version.EqualTo(lastSpec.Version) {
					if spec.MinCompatibilityVersion.EqualTo(lastSpec.MinCompatibilityVersion) {
						return fmt.Errorf("feature %q lists duplicate entries with the same versions (%s = %s)", name, spec.Version, lastSpec.Version)
					}
					if spec.PreRelease != lastSpec.PreRelease {
						return fmt.Errorf("feature %q lists stability changes with the same version (%s -> %s)", name, lastSpec.PreRelease, spec.PreRelease)
					}
					if spec.LockToDefault != lastSpec.LockToDefault {
						return fmt.Errorf("feature %q lists locked to default changes with the same version (%v -> %v)", name, lastSpec.LockToDefault, spec.LockToDefault)
					}
				}
				if spec.PreRelease == lastSpec.PreRelease && spec.Default == lastSpec.Default && spec.LockToDefault == lastSpec.LockToDefault {
					return fmt.Errorf("feature %q lists transition without stability change (%s -> %s)", name, lastSpec.Version, spec.Version)
				}
				if wasLockedToDefault && !spec.LockToDefault {
					return fmt.Errorf("feature %q must not unlock after locking to default", name)
				}
				// 成熟度不能从 GA 回退到 Beta/Alpha，也不能从 Beta 回退到 Alpha；
				// Deprecated 必须是特性的最终状态。
				switch {
				case spec.PreRelease != Deprecated && wasDeprecated:
					return fmt.Errorf("deprecated feature %q must not resurrect from its terminal state", name)
				case spec.PreRelease == Alpha && (wasBeta || wasGA):
					return fmt.Errorf("feature %q regresses stability from more stable level to %s in %s", name, spec.PreRelease, spec.Version)
				case spec.PreRelease == Beta && wasGA:
					return fmt.Errorf("feature %q regresses stability from more stable level to %s in %s", name, spec.PreRelease, spec.Version)
				}
			}
			completedSpec := spec
			// 如果未指定 MinCompatibilityVersion，则沿用上一个规格中的值。
			// 这样可以确保 MinCompatibilityVersion 不会递减。
			// 例如：
			// 	FeatureA: {
			// 		{Version: version.MustParse("1.35"), Default: false, PreRelease: Beta},
			// 		{Version: version.MustParse("1.35"), Default: true, PreRelease: Beta, MinCompatibilityVersion: 1.35},
			//      {Version: version.MustParse("1.37"), Default: true, PreRelease: GA},
			// 	},
			// FeatureA 进入 GA 阶段时，其 MinCompatibilityVersion 至少应为 1.35。
			if completedSpec.MinCompatibilityVersion == nil && i > 0 {
				completedSpec.MinCompatibilityVersion = lastSpec.MinCompatibilityVersion
			}
			completedSpecs = append(completedSpecs, completedSpec)
			lastSpec = completedSpec
			wasBeta = wasBeta || spec.PreRelease == Beta
			wasGA = wasGA || spec.PreRelease == GA
			wasDeprecated = wasDeprecated || spec.PreRelease == Deprecated
			wasLockedToDefault = wasLockedToDefault || spec.LockToDefault
		}
		if existingSpec, found := known[name]; found {
			// 幂等：重复注册完全相同的规格是允许的（多个包可能都声明同一 gate）。
			if reflect.DeepEqual(existingSpec, completedSpecs) {
				continue
			}
			return fmt.Errorf("feature gate %q with different spec already exists: %v", name, existingSpec)
		}
		known[name] = completedSpecs
	}
	// 提交更新后的状态。
	// 所有特性校验通过后一次性提交，避免注册到一半的中间状态。
	f.known.Store(known)

	return nil
}

// AddDependencies 为特性开关添加依赖关系。
// 依赖在注册时校验并保存；Enabled 不会再次检查，因此错误的依赖必须在启动期暴露。
func (f *featureGate) AddDependencies(dependencies map[Feature][]Feature) error {
	f.lock.Lock()
	defer f.lock.Unlock()

	if f.closed {
		return fmt.Errorf("cannot add a feature gate dependency after adding it to the flag set")
	}
	// 复制现有状态。
	known := f.GetAllVersioned()

	// 合并已有的依赖关系。
	existing := *f.dependencies.Load()
	for k, v := range existing {
		dependencies[k] = append(dependencies[k], v...)
	}

	// 对所有依赖列表排序并去重。
	// 去重并排序，使校验结果与注册顺序无关。
	for k, v := range dependencies {
		slices.Sort(v)
		dependencies[k] = slices.Compact(v)
	}

	// 针对每个模拟版本校验依赖关系：
	// 1. 特性及其依赖项都必须已经注册。
	// 2. 特性不能依赖成熟度级别更低的特性。
	// 3. 已启用的特性不能依赖已禁用的特性。
	// 4. 默认值已锁定的特性不能依赖默认值未锁定的特性。
	for feature, deps := range dependencies {
		versionedFeature, ok := known[feature]
		if !ok {
			return fmt.Errorf("cannot add dependency for unknown feature %s", feature)
		}

		for _, dep := range deps {
			versionedDep, ok := known[dep]
			if !ok {
				return fmt.Errorf("cannot add dependency from %s to unknown feature %s", feature, dep)
			}

			// 从最新版本开始检查，以便生成更直观的错误信息。
			for _, spec := range slices.Backward(versionedFeature) {
				// depSpec 是依赖项在当前依赖方 FeatureSpec 所声明版本下实际生效的 FeatureSpec。
				depSpec := featureSpecAtEmulationAndMinCompatVersion(versionedDep, spec.Version, spec.MinCompatibilityVersion)

				if stabilityOrder(spec.PreRelease) > stabilityOrder(depSpec.PreRelease) {
					return fmt.Errorf("%s feature %s cannot depend on %s feature %s at version %s", spec.PreRelease, feature, depSpec.PreRelease, dep, spec.Version)
				}
				if spec.Default && !depSpec.Default {
					return fmt.Errorf("default-enabled feature %s cannot depend on default-disabled feature %s at version %s", feature, dep, spec.Version)
				}
				if spec.LockToDefault && !depSpec.LockToDefault {
					return fmt.Errorf("locked-to-default feature %s cannot depend on unlocked feature %s at version %s", feature, dep, spec.Version)
				}
			}
		}
	}

	// 检查是否存在循环依赖。
	// 用 DFS 三色标记检测环：visited 为当前递归栈，finished 为已完成节点。
	visited := map[Feature]bool{}
	finished := map[Feature]bool{}
	var detectCycles func(Feature) error
	detectCycles = func(feature Feature) error {
		if finished[feature] {
			return nil
		}
		if visited[feature] {
			return fmt.Errorf("cycle detected with feature %s", feature)
		}
		visited[feature] = true
		for _, dep := range dependencies[feature] {
			if err := detectCycles(dep); err != nil {
				return err
			}
		}
		finished[feature] = true
		return nil
	}
	for feature := range dependencies {
		if err := detectCycles(feature); err != nil {
			return err
		}
	}

	// 提交更新后的状态。
	f.dependencies.Store(&dependencies)

	return nil
}

// stabilityOrder 将预发布成熟度转换为数值，以便比较依赖关系。
// 特性不能依赖成熟度级别低于自身的特性。
func stabilityOrder(p prerelease) int {
	switch p {
	case Deprecated:
		return 0 // 非 Deprecated 特性不能依赖已弃用的特性。
	case PreAlpha, Alpha: // Alpha 特性允许依赖 PreAlpha 特性。
		return 1
	case Beta:
		return 2
	case GA:
		return 3
	default:
		return -1 // 未知的预发布成熟度。
	}
}

// Dependencies 返回已知依赖的副本，调用方修改不会影响 gate 内部状态。
func (f *featureGate) Dependencies() map[Feature][]Feature {
	return maps.Clone(*f.dependencies.Load())
}

//logcheck:context // 请改用 OverrideDefaultWithLogger。
func (f *featureGate) OverrideDefault(name Feature, override bool) error {
	return f.OverrideDefaultWithLogger(klog.Background(), name, override)
}

// OverrideDefaultWithLogger 在显式 logger 下覆盖某个特性的默认值。
// 不允许在 AddFlag 之后调用，避免命令行帮助与实际默认值不一致。
func (f *featureGate) OverrideDefaultWithLogger(logger klog.Logger, name Feature, override bool) error {
	helper, logger := logger.WithCallStackHelper()
	helper()
	return f.overrideDefaultAtEmulationAndMinCompatVersion(logger, name, override, f.EmulationVersion(), f.MinCompatibilityVersion())
}

//logcheck:context // 请改用 OverrideDefaultAtVersionWithLogger。
func (f *featureGate) OverrideDefaultAtVersion(name Feature, override bool, ver *version.Version) error {
	return f.OverrideDefaultAtVersionWithLogger(klog.Background(), name, override, ver)
}

func (f *featureGate) OverrideDefaultAtVersionWithLogger(logger klog.Logger, name Feature, override bool, ver *version.Version) error {
	helper, logger := logger.WithCallStackHelper()
	helper()
	return f.overrideDefaultAtEmulationAndMinCompatVersion(logger, name, override, ver, nil)
}

// overrideDefaultAtEmulationAndMinCompatVersion 是各类 OverrideDefault 的公共实现。
// 只修改选中规格的 Default，不影响历史版本区间。
func (f *featureGate) overrideDefaultAtEmulationAndMinCompatVersion(logger klog.Logger, name Feature, override bool, emuVer, minCompatVer *version.Version) error {
	helper, logger := logger.WithCallStackHelper()
	helper()

	f.lock.Lock()
	defer f.lock.Unlock()

	if f.closed {
		return fmt.Errorf("cannot override default for feature %q: gates already added to a flag set", name)
	}

	// 复制现有状态。
	known := f.GetAllVersioned()

	specs, ok := known[name]
	if !ok {
		return fmt.Errorf("cannot override default: feature %q is not registered", name)
	}
	spec := featureSpecAtEmulationAndMinCompatVersion(specs, emuVer, minCompatVer)
	switch {
	case spec.LockToDefault:
		return fmt.Errorf("cannot override default: feature %q default is locked to %t", name, spec.Default)
	case spec.PreRelease == PreAlpha:
		return fmt.Errorf("cannot override default: feature %q is not available before version %s", name, emuVer)
	case spec.PreRelease == Deprecated:
		logger.Info("Warning: overriding default of deprecated feature gate. It will be removed in a future release.", "featureGate", name, "value", override)
	case spec.PreRelease == GA:
		logger.Info("Warning: overriding default of GA feature gate. It will be removed in a future release.", "featureGate", name, "value", override)
	}

	spec.Default = override
	known[name] = specs
	f.known.Store(known)

	return nil
}

// GetAll 返回当前 emulationVersion 下已知特性名称到特性规格映射的副本。
// PreAlpha 特性不会出现在结果中，因为它们在当前模拟版本下不存在。
func (f *featureGate) GetAll() map[Feature]FeatureSpec {
	retval := map[Feature]FeatureSpec{}
	f.lock.Lock()
	versionedSpecs := f.GetAllVersioned()
	emuVer := f.EmulationVersion()
	minCompatVer := f.MinCompatibilityVersion()
	f.lock.Unlock()
	for k, v := range versionedSpecs {
		spec := featureSpecAtEmulationAndMinCompatVersion(v, emuVer, minCompatVer)
		if spec.PreRelease == PreAlpha {
			// 该特性在当前模拟版本下不可用。
			continue
		}
		retval[k] = *spec
	}
	return retval
}

// GetAllVersioned 返回已知特性名称到版本化特性规格映射的副本。
// 返回浅拷贝的切片副本，调用方可以安全地增删元素。
func (f *featureGate) GetAllVersioned() map[Feature]VersionedSpecs {
	retval := map[Feature]VersionedSpecs{}
	for k, v := range f.known.Load().(map[Feature]VersionedSpecs) {
		vCopy := make([]FeatureSpec, len(v))
		_ = copy(vCopy, v)
		retval[k] = vCopy
	}
	return retval
}

//logcheck:context // 请改用 SetEmulationVersionWithLogger。
func (f *featureGate) SetEmulationVersion(emulationVersion *version.Version) error {
	return f.SetEmulationVersionWithLogger(klog.Background(), emulationVersion)
}

func (f *featureGate) SetEmulationVersionWithLogger(logger klog.Logger, emulationVersion *version.Version) error {
	helper, logger := logger.WithCallStackHelper()
	helper()
	return f.SetEmulationVersionAndMinCompatibilityVersionWithLogger(logger, emulationVersion, emulationVersion.SubtractMinor(1))
}

//logcheck:context // 请改用 SetEmulationVersionAndMinCompatibilityVersionWithLogger。
func (f *featureGate) SetEmulationVersionAndMinCompatibilityVersion(emulationVersion *version.Version, minCompatibilityVersion *version.Version) error {
	return f.SetEmulationVersionAndMinCompatibilityVersionWithLogger(klog.Background(), emulationVersion, minCompatibilityVersion)
}

func (f *featureGate) SetEmulationVersionAndMinCompatibilityVersionWithLogger(logger klog.Logger, emulationVersion *version.Version, minCompatibilityVersion *version.Version) error {
	helper, logger := logger.WithCallStackHelper()
	helper()

	// 无变化时直接返回，避免无谓地重建 enabled 并丢弃 queriedFeatures。
	if emulationVersion.EqualTo(f.EmulationVersion()) && minCompatibilityVersion.EqualTo(f.MinCompatibilityVersion()) {
		return nil
	}
	f.lock.Lock()
	defer f.lock.Unlock()
	logger.V(1).Info("Set feature gate emulation", "emulationVersion", emulationVersion, "minCompatibilityVersion", minCompatibilityVersion)

	// 复制现有状态。
	enabledRaw := map[string]bool{}
	for k, v := range f.enabledRaw.Load().(map[string]bool) {
		enabledRaw[k] = v
	}
	// 每次修改 emulationVersion 时，都必须重置 enabled 映射。
	// 因为默认值随版本变化，必须在新的版本下重新展开 enabledRaw。
	enabled := map[Feature]bool{}
	errs := f.unsafeSetFromMap(logger, enabled, enabledRaw, emulationVersion, minCompatibilityVersion)

	// 对已经查询过的特性做差异告警：值变了意味着组件行为可能在切版本后悄然改变。
	queriedFeatures := f.queriedFeatures.Load().(sets.Set[Feature])
	known := f.known.Load().(map[Feature]VersionedSpecs)
	for feature := range queriedFeatures {
		newVal := featureEnabled(feature, enabled, known, emulationVersion, minCompatibilityVersion)
		oldVal := featureEnabled(feature, f.enabled.Load().(map[Feature]bool), known, f.EmulationVersion(), f.MinCompatibilityVersion())
		if newVal != oldVal {
			logger.Info("Warning: SetEmulationVersionAndMinCompatibilityVersion will change already queried feature", "featureGate", feature, "oldValue", oldVal, "newValue", newVal)
		}
	}

	if len(errs) == 0 {
		// 提交变更。
		f.enabled.Store(enabled)
		f.emulationVersion.Store(emulationVersion)
		f.minCompatibilityVersion.Store(minCompatibilityVersion)
		f.queriedFeatures.Store(sets.Set[Feature]{})
	}
	return utilerrors.NewAggregate(errs)
}

func (f *featureGate) EmulationVersion() *version.Version {
	return f.emulationVersion.Load()
}

// MinCompatibilityVersion 返回当前维护兼容性的最低控制面版本。
func (f *featureGate) MinCompatibilityVersion() *version.Version {
	return f.minCompatibilityVersion.Load()
}

// featureSpec 在指定键存在时返回其 EmulationVersion 对应的 FeatureSpec，否则返回错误。
// 这样可以根据 PreRelease 或 Version 信息，为同一特性保留多种实现方式。
func (f *featureGate) featureSpec(key Feature) (FeatureSpec, error) {
	if v, ok := f.known.Load().(map[Feature]VersionedSpecs)[key]; ok {
		featureSpec := f.featureSpecAtEmulationAndMinCompatVersion(v)
		return *featureSpec, nil
	}
	return FeatureSpec{}, fmt.Errorf("feature %q is not registered in FeatureGate %q", key, f.featureGateName)
}

// unsafeRecordQueried 记录被查询过的特性。复制后再写入原子值，
// 避免并发 Enabled 调用竞争同一个 set。
func (f *featureGate) unsafeRecordQueried(key Feature) {
	queriedFeatures := f.queriedFeatures.Load().(sets.Set[Feature])
	if _, ok := queriedFeatures[key]; ok {
		return
	}
	// 修改前先克隆 queriedFeatures 中的元素。
	newQueriedFeatures := queriedFeatures.Clone()
	newQueriedFeatures.Insert(key)
	f.queriedFeatures.Store(newQueriedFeatures)
}

// featureEnabled 解析特性的最终状态：显式设置优先，否则取当前版本规格的默认值。
// 未注册的特性直接 panic，这是调用方编程错误而非运行时可恢复条件。
func featureEnabled(key Feature, enabled map[Feature]bool, known map[Feature]VersionedSpecs, emulationVersion, minCompatibilityVersion *version.Version) bool {
	// 检查是否存在显式设置的启用值。
	if v, ok := enabled[key]; ok {
		return v
	}
	if v, ok := known[key]; ok {
		featureSpec := featureSpecAtEmulationAndMinCompatVersion(v, emulationVersion, minCompatibilityVersion)
		return featureSpec.Default
	}

	panic(fmt.Errorf("feature %q is not registered in FeatureGate", key))
}

// Enabled 在指定特性已启用时返回 true；如果该特性未注册，则会触发 panic。
func (f *featureGate) Enabled(key Feature) bool {
	// TODO：理想情况下应在此处锁定 FeatureGate 以保证安全，但需要评估加锁带来的性能影响。
	v := featureEnabled(key, f.enabled.Load().(map[Feature]bool), f.known.Load().(map[Feature]VersionedSpecs), f.EmulationVersion(), f.MinCompatibilityVersion())
	f.unsafeRecordQueried(key)
	return v
}

func (f *featureGate) featureSpecAtEmulationAndMinCompatVersion(v VersionedSpecs) *FeatureSpec {
	return featureSpecAtEmulationAndMinCompatVersion(v, f.EmulationVersion(), f.MinCompatibilityVersion())
}

// featureSpecAtEmulationAndMinCompatVersion 二分（线性）查找适用于给定版本的规格：
// 选择 Version 不大于模拟版本、且 MinCompatibilityVersion 已满足的最高条目；
// 都不满足时返回 PreAlpha，表示该版本下特性不可用。
func featureSpecAtEmulationAndMinCompatVersion(v VersionedSpecs, emulationVersion, minCompatibilityVersion *version.Version) *FeatureSpec {
	i := len(v) - 1
	if minCompatibilityVersion == nil {
		minCompatibilityVersion = emulationVersion.SubtractMinor(1)
	}
	for ; i >= 0; i-- {
		if v[i].Version.GreaterThan(emulationVersion) {
			continue
		}
		specMinCompatibilityVersion := v[i].MinCompatibilityVersion
		if specMinCompatibilityVersion != nil && !minCompatibilityVersion.AtLeast(specMinCompatibilityVersion) {
			continue
		}
		return &v[i]
	}
	return &FeatureSpec{
		Default:    false,
		PreRelease: PreAlpha,
		Version:    version.MajorMinor(0, 0),
	}
}

// Close 将 closed 设为 true，阻止后续调用 Add 注册新特性。
func (f *featureGate) Close() {
	f.lock.Lock()
	f.closed = true
	f.lock.Unlock()
}

// AddFlag 向指定 FlagSet 注册用于设置全局特性开关的命令行参数。
func (f *featureGate) AddFlag(fs *pflag.FlagSet) {
	// TODO(mtaufen)：是否应该在首次调用 Set/SetFromMap 时再关闭注册？
	// 并非所有组件都通过 AddFlag 方法暴露特性开关参数，
	// 并且未来所有组件都将不再直接暴露特性开关参数，
	// 而是统一使用 componentconfig。
	// 先冻结 gate 集合，确保 --help 列出的选项与运行时注册的特性一致。
	f.Close()

	known := f.KnownFeatures()
	fs.Var(f, flagName, ""+
		"A set of key=value pairs that describe feature gates for alpha/experimental features. "+
		"Options are:\n"+strings.Join(known, "\n"))
}

// AddMetrics 把当前特性状态上报为指标，供监控特性使用情况。
func (f *featureGate) AddMetrics() {
	for feature, featureSpec := range f.GetAll() {
		featuremetrics.RecordFeatureInfo(context.Background(), string(feature), string(featureSpec.PreRelease), f.Enabled(feature))
	}
}

// KnownFeatures 返回描述当前 FeatureGate 已知特性的字符串切片。
// PreAlpha、Deprecated 和 GA 阶段的特性不会显示在列表中。
func (f *featureGate) KnownFeatures() []string {
	var known []string
	for k, v := range f.known.Load().(map[Feature]VersionedSpecs) {
		if k == "AllAlpha" || k == "AllBeta" {
			// 虚拟 gate 使用首个规格描述（其版本固定为 0.0）。
			known = append(known, fmt.Sprintf("%s=true|false (%s - default=%t)", k, v[0].PreRelease, v[0].Default))
			continue
		}
		featureSpec := f.featureSpecAtEmulationAndMinCompatVersion(v)
		featureSpecStr := featureSpec.HelpString(k, false)
		// 同时计算“最低兼容版本等于模拟版本”时的描述，用于展示依赖兼容版本的默认值。
		featureSpecMinCompatMode := featureSpecAtEmulationAndMinCompatVersion(v, f.EmulationVersion(), f.EmulationVersion())
		featureSpecMinCompatModeStr := featureSpecMinCompatMode.HelpString(k, true)
		if reflect.DeepEqual(featureSpec, featureSpecMinCompatMode) {
			if featureSpecStr != "" {
				known = append(known, featureSpecStr)
			}
			continue
		}
		components := []string{}
		if featureSpecStr != "" {
			components = append(components, featureSpecStr)
		}
		if featureSpecMinCompatModeStr != "" {
			components = append(components, featureSpecMinCompatModeStr)
		}
		if len(components) > 0 {
			known = append(known, strings.Join(components, ", or "))
		}
	}
	sort.Strings(known)
	return known
}

// DeepCopyAndReset 复制 FeatureGate 对象中所有已注册特性、已知特性定义及覆盖配置，
// 并重置新 FeatureGate 中所有特性的启用状态。
// 适用于创建新的 FeatureGate 实例，同时避免继承基础实例中的启用配置。
// 与 DeepCopy 不同，它不复制 enabled/enabledRaw，适合“只继承定义、重新应用配置”的场景。
func (f *featureGate) DeepCopyAndReset() MutableVersionedFeatureGate {
	fg := NewVersionedFeatureGateWithMinCompatibility(f.EmulationVersion(), f.MinCompatibilityVersion())
	known := f.GetAllVersioned()
	fg.known.Store(known)
	return fg
}

// DeepCopy 返回 FeatureGate 对象的深拷贝，修改副本中的开关不会影响原对象。
// 这适用于在正式提交配置变更前，先验证配置是否能够适配
// 潜在的 Feature Gate 变更。
func (f *featureGate) DeepCopy() MutableVersionedFeatureGate {
	f.lock.Lock()
	defer f.lock.Unlock()
	// 复制现有状态。.
	known := f.GetAllVersioned()
	enabled := map[Feature]bool{}
	for k, v := range f.enabled.Load().(map[Feature]bool) {
		enabled[k] = v
	}
	enabledRaw := map[string]bool{}
	for k, v := range f.enabledRaw.Load().(map[string]bool) {
		enabledRaw[k] = v
	}
	dependencies := map[Feature][]Feature{}
	for k, v := range *f.dependencies.Load() {
		dependencies[k] = append([]Feature{}, v...)
	}

	// 使用复制后的状态构造新的 featureGate。
	// 注意：按照约定，specialFeatures 被视为不可变数据，
	// 并且复制时会保留 f.closed 的值。
	// queriedFeatures 重置：副本尚未被任何调用方查询。
	fg := &featureGate{
		special: specialFeatures,
		closed:  f.closed,
	}
	fg.emulationVersion.Store(f.EmulationVersion())
	fg.minCompatibilityVersion.Store(f.MinCompatibilityVersion())
	fg.known.Store(known)
	fg.enabled.Store(enabled)
	fg.dependencies.Store(&dependencies)
	fg.enabledRaw.Store(enabledRaw)
	fg.queriedFeatures.Store(sets.Set[Feature]{})
	return fg
}

// ExplicitlySet 在特性值由用户显式设置，而不是
// 根据默认值或特殊特性推导得出时返回 true。
func (f *featureGate) ExplicitlySet(name Feature) bool {
	// enabledRaw 只保存用户输入，因此可直接作为“是否显式设置”的依据。
	enabledRaw := f.enabledRaw.Load().(map[string]bool)
	_, ok := enabledRaw[string(name)]
	return ok
}

// ResetFeatureValueToDefault 将特性值重置为默认值。
// 从 enabled 与 enabledRaw 中同时删除，使其重新由默认值或 AllAlpha/AllBeta 展开。
func (f *featureGate) ResetFeatureValueToDefault(name Feature) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	enabled := map[Feature]bool{}
	for k, v := range f.enabled.Load().(map[Feature]bool) {
		enabled[k] = v
	}
	enabledRaw := map[string]bool{}
	for k, v := range f.enabledRaw.Load().(map[string]bool) {
		enabledRaw[k] = v
	}
	_, inEnabled := enabled[name]
	if inEnabled {
		delete(enabled, name)
	}
	_, inEnabledRaw := enabledRaw[string(name)]
	if inEnabledRaw {
		delete(enabledRaw, string(name))
	}
	// 某些特性可能存在于 enabled 映射中，却不存在于 enabledRaw 映射中，
	// 例如设置 AllAlpha 后被批量启用的某些 Alpha 特性。
	if inEnabledRaw && !inEnabled {
		return fmt.Errorf("feature:%s was explicitly set, but not in enabled map", name)
	}
	f.enabled.Store(enabled)
	f.enabledRaw.Store(enabledRaw)
	return nil
}
