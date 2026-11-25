/*
Copyright 2024 The Kubernetes Authors.

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

// Package compatibility 维护各控制平面组件的“模拟版本/最小兼容版本/特性门控”全局状态，
// 使组件能够以自身二进制版本之外的版本语义运行，从而支持版本偏差（version skew）。
package compatibility

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/spf13/pflag"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/version"
	cliflag "k8s.io/component-base/cli/flag"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-base/metrics/prometheus/compatversion"
	"k8s.io/klog/v2"
)

const (
	// DefaultKubeComponent 是 Kubernetes 控制平面组件的默认名称。
	// 未在 --emulated-version / --feature-gates 中写组件前缀时默认归到 "kube"。
	DefaultKubeComponent = "kube"

	// klogLevel 统一调试日志级别，避免在各处重复魔法数字。
	klogLevel = 2
)

// VersionMapping 把源组件版本映射为目标组件版本；返回 nil 视为映射错误。
type VersionMapping func(from *version.Version) *version.Version

// ComponentGlobals 集中保存一个组件的全局变量，便于访问其特性门控和有效版本。
type ComponentGlobals struct {
	// effectiveVersion 描述组件的二进制、模拟与最小兼容版本。
	effectiveVersion MutableEffectiveVersion
	// featureGate 是受版本约束的特性门控，其可用阶段随 effectiveVersion 变化。
	featureGate featuregate.MutableVersionedFeatureGate

	// componentVersionMapping 保存本组件版本到其他组件版本的映射。
	componentVersionMapping map[string]VersionMapping
	// dependentComponentVersion 表示本组件版本是否通过映射依赖其他组件。
	// 为 true 时，模拟版本和最低兼容版本不能由标志或另一组映射设置。
	// 单向依赖标志可避免两个来源冲突。
	dependentComponentVersion bool
}

// ComponentGlobalsRegistry 集中保存不同组件的全局变量，便于访问各组件的特性门控和有效版本。
type ComponentGlobalsRegistry interface {
	// EffectiveVersionFor 返回组件注册的 EffectiveVersion；未注册时返回 nil。
	EffectiveVersionFor(component string) EffectiveVersion
	// FeatureGateFor 返回组件注册的 FeatureGate；未注册时返回 nil。
	FeatureGateFor(component string) featuregate.FeatureGate
	// Register 注册组件的 EffectiveVersion 和 FeatureGate；重复注册返回错误。
	Register(component string, effectiveVersion MutableEffectiveVersion, featureGate featuregate.MutableVersionedFeatureGate) error
	// ComponentGlobalsOrRegister 返回组件已注册的全局变量；否则注册传入变量并原样返回。
	ComponentGlobalsOrRegister(component string, effectiveVersion MutableEffectiveVersion, featureGate featuregate.MutableVersionedFeatureGate) (MutableEffectiveVersion, featuregate.MutableVersionedFeatureGate)
	// AddFlags 添加 --emulated-version、--min-compatibility-version 和 --feature-gates 标志。
	AddFlags(fs *pflag.FlagSet)
	// Set 为所有已注册组件设置标志值；调用前组件的门控和有效版本不会更新。
	Set() error
	// SetFallback 在 Set 从未调用过时调用它。
	SetFallback() error
	// Validate 对所有已注册组件的全部全局变量调用 Validate。
	Validate() []error
	// Reset 删除全部 ComponentGlobals、配置和版本映射。
	Reset()
	// SetVersionMapping 将一个组件的模拟/最低兼容版本映射到另一个组件。
	// 设置后，目标组件版本由源组件决定，不再接受命令行标志；每个组件最多依赖一个其他组件。
	SetVersionMapping(fromComponent, toComponent string, f VersionMapping) error
	// AddMetrics 添加组件模拟版本的指标。
	AddMetrics()
}

type componentGlobalsRegistry struct {
	// componentGlobals 按组件名保存全局变量；所有读写都需持有 mutex。
	componentGlobals map[string]*ComponentGlobals
	mutex            sync.RWMutex
	// emulationVersionConfig 保存从标志解析的组件名与模拟版本列表，直到 Set() 才生效，
	// 以强制按模拟版本、最低兼容版本、特性门控的顺序设置。
	emulationVersionConfig []string
	// minCompatibilityVersionConfig 保存从标志解析的组件名与最低兼容版本列表，直到 Set() 才生效，
	// 以强制按模拟版本、最低兼容版本、特性门控的顺序设置。
	minCompatibilityVersionConfig []string
	// featureGatesConfig 保存从标志解析的组件名到特性门控列表的映射，直到 Set() 才生效，
	// 以强制按模拟版本、最低兼容版本、特性门控的顺序设置。
	featureGatesConfig map[string][]string
	// featureGatesConfigFlags 缓存标志值对象，允许其他命令追加而非覆盖门控配置，
	// 并保证重复调用 AddFlags 时共享同一份解析结果。
	featureGatesConfigFlags *cliflag.ColonSeparatedMultimapStringString
	// set 记录注册表的 Set 函数是否已调用过。
	set bool
}

// NewComponentGlobalsRegistry 创建空的注册表；调用方随后需 Register 各组件并调用 Set 生效。
func NewComponentGlobalsRegistry() *componentGlobalsRegistry {
	return &componentGlobalsRegistry{
		componentGlobals:              make(map[string]*ComponentGlobals),
		emulationVersionConfig:        nil,
		minCompatibilityVersionConfig: nil,
		featureGatesConfig:            nil,
	}
}

// AddMetrics 为每个已注册组件记录一次版本信息指标，通常在启动后期调用一次。
// 无 effectiveVersion 的条目跳过，避免空指针。
func (r *componentGlobalsRegistry) AddMetrics() {
	for name, globals := range r.componentGlobals {
		effectiveVersion := globals.effectiveVersion
		if effectiveVersion == nil {
			continue
		}
		compatversion.RecordCompatVersionInfo(context.Background(), name, effectiveVersion.BinaryVersion().String(), effectiveVersion.EmulationVersion().String(), effectiveVersion.MinCompatibilityVersion().String())
	}
}

// Reset 清空全部注册状态；主要用于测试或需要重新解析标志的场景。
func (r *componentGlobalsRegistry) Reset() {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.componentGlobals = make(map[string]*ComponentGlobals)
	r.emulationVersionConfig = nil
	r.minCompatibilityVersionConfig = nil
	r.featureGatesConfig = nil
	r.featureGatesConfigFlags = nil
	r.set = false
}

// EffectiveVersionFor 返回组件已注册的 EffectiveVersion；未注册返回 nil。
func (r *componentGlobalsRegistry) EffectiveVersionFor(component string) EffectiveVersion {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	globals, ok := r.componentGlobals[component]
	if !ok {
		return nil
	}
	return globals.effectiveVersion
}

// FeatureGateFor 返回组件已注册的特性门控；未注册返回 nil。
func (r *componentGlobalsRegistry) FeatureGateFor(component string) featuregate.FeatureGate {
	r.mutex.RLock()
	defer r.mutex.RUnlock()
	globals, ok := r.componentGlobals[component]
	if !ok {
		return nil
	}
	return globals.featureGate
}

// unsafeRegister 在调用方已持锁的前提下执行注册。
// 注册时即把版本信息同步给特性门控，保证在任何 Enabled 查询之前门控已知晓版本约束。
func (r *componentGlobalsRegistry) unsafeRegister(component string, effectiveVersion MutableEffectiveVersion, featureGate featuregate.MutableVersionedFeatureGate) error {
	if _, ok := r.componentGlobals[component]; ok {
		return fmt.Errorf("component globals of %s already registered", component)
	}
	if featureGate != nil {
		if err := featureGate.SetEmulationVersionAndMinCompatibilityVersion(
			effectiveVersion.EmulationVersion(), effectiveVersion.MinCompatibilityVersion()); err != nil {
			return err
		}
	}
	c := ComponentGlobals{
		effectiveVersion:        effectiveVersion,
		featureGate:             featureGate,
		componentVersionMapping: make(map[string]VersionMapping),
	}
	r.componentGlobals[component] = &c
	return nil
}

// Register 注册组件的版本与门控；重复注册或 effectiveVersion 为 nil 返回错误。
func (r *componentGlobalsRegistry) Register(component string, effectiveVersion MutableEffectiveVersion, featureGate featuregate.MutableVersionedFeatureGate) error {
	if effectiveVersion == nil {
		return fmt.Errorf("cannot register nil effectiveVersion")
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	return r.unsafeRegister(component, effectiveVersion, featureGate)
}

// ComponentGlobalsOrRegister 返回已存在的全局变量，否则注册并返回传入值。
// 供多个初始化路径以幂等方式共享同一组件注册。
func (r *componentGlobalsRegistry) ComponentGlobalsOrRegister(component string, effectiveVersion MutableEffectiveVersion, featureGate featuregate.MutableVersionedFeatureGate) (MutableEffectiveVersion, featuregate.MutableVersionedFeatureGate) {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	globals, ok := r.componentGlobals[component]
	if ok {
		return globals.effectiveVersion, globals.featureGate
	}
	utilruntime.Must(r.unsafeRegister(component, effectiveVersion, featureGate))
	return effectiveVersion, featureGate
}

// unsafeKnownFeatures 汇总所有组件的特性名（形如 component:Feature），排序后用于帮助文本。
func (r *componentGlobalsRegistry) unsafeKnownFeatures() []string {
	var known []string
	for component, globals := range r.componentGlobals {
		if globals.featureGate == nil {
			continue
		}
		for _, f := range globals.featureGate.KnownFeatures() {
			known = append(known, component+":"+f)
		}
	}
	sort.Strings(known)
	return known
}

// unsafeVersionFlagOptions 生成“component=允许范围”列表；跳过被版本映射接管的组件，
// 因为这些组件不接受来自标志的版本设置。
func (r *componentGlobalsRegistry) unsafeVersionFlagOptions(isEmulation bool) []string {
	var vs []string
	for component, globals := range r.componentGlobals {
		if globals.dependentComponentVersion {
			continue
		}
		if isEmulation {
			vs = append(vs, fmt.Sprintf("%s=%s", component, globals.effectiveVersion.AllowedEmulationVersionRange()))
		} else {
			vs = append(vs, fmt.Sprintf("%s=%s", component, globals.effectiveVersion.AllowedMinCompatibilityVersionRange()))
		}
	}
	sort.Strings(vs)
	return vs
}

// AddFlags 注册 --emulated-version、--min-compatibility-version 与 --feature-gates。
//
// 解析阶段只把原始字符串暂存到配置字段，真正的生效推迟到 Set()，以满足
// “先版本、再最小兼容版本、最后特性门控”的顺序要求。注册前会关闭已有门控，
// 防止在标志值确定前被并发读取而造成不一致。
func (r *componentGlobalsRegistry) AddFlags(fs *pflag.FlagSet) {
	if r == nil {
		return
	}
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for _, globals := range r.componentGlobals {
		if globals.featureGate != nil {
			globals.featureGate.Close()
		}
	}

	fs.StringSliceVar(&r.emulationVersionConfig, "emulated-version", r.emulationVersionConfig, ""+
		"The versions different components emulate their capabilities (APIs, features, ...) of.\n"+
		"If set, the component will emulate the behavior of this version instead of the underlying binary version.\n"+
		"Version format could only be major.minor, for example: '--emulated-version=wardle=1.2,kube=1.31'.\nOptions are: "+strings.Join(r.unsafeVersionFlagOptions(true), ",")+
		"\nIf the component is not specified, defaults to \"kube\"")

	// min-compatibility-version 通常是服务器预期在其生命周期内共存的所有控制平面组件的
	// 最低二进制版本。若低于服务器模拟版本，则不会启用该版本之后引入的最新 CEL 特性、
	// 库和参数；MinCompatibilityVersion 高于此值的特性阶段会被跳过，资源存储版本也按此设置。
	// 所有控制平面组件升级后可将其设为二进制版本，此后可启用有兼容性影响的特性及最新
	// CEL 特性、库和参数，但不再支持回滚。
	fs.StringSliceVar(&r.minCompatibilityVersionConfig, "min-compatibility-version", r.minCompatibilityVersionConfig, ""+
		"The min version of control plane components the server should be compatible with.\n"+
		"Must be less or equal to the emulated-version. Version format could only be major.minor, for example: '--min-compatibility-version=wardle=1.2,kube=1.31'.\n"+
		"Options are: "+strings.Join(r.unsafeVersionFlagOptions(false), ",")+
		"\nIf the component is not specified, defaults to \"kube\"")

	if r.featureGatesConfigFlags == nil {
		r.featureGatesConfigFlags = cliflag.NewColonSeparatedMultimapStringStringAllowDefaultEmptyKey(&r.featureGatesConfig)
	}
	fs.Var(r.featureGatesConfigFlags, "feature-gates", "Comma-separated list of component:key=value pairs that describe feature gates for alpha/experimental features of different components.\n"+
		"If the component is not specified, defaults to \"kube\". This flag can be repeatedly invoked. For example: --feature-gates 'wardle:featureA=true,wardle:featureB=false' --feature-gates 'kube:featureC=true'"+
		"Options are:\n"+strings.Join(r.unsafeKnownFeatures(), "\n"))
}

// componentVersion 是版本传播队列中的元素，把组件名与待设置版本配对。
type componentVersion struct {
	component string
	ver       *version.Version
}

// getFullVersionConfig 用已注册的版本映射展开给定版本配置，返回组件到 Version 的映射。
// 以 BFS 方式沿版本映射传播：显式配置的组件入队，其映射出的下游组件继续入队。
// 若同一组件被设置两次，说明映射图存在环或重复依赖，直接报错。
func (r *componentGlobalsRegistry) getFullVersionConfig(
	versionConfigMap map[string]*version.Version) (map[string]*version.Version, error) {
	result := map[string]*version.Version{}
	setQueue := []componentVersion{}
	for comp, ver := range versionConfigMap {
		if _, ok := r.componentGlobals[comp]; !ok {
			return result, fmt.Errorf("component not registered: %s", comp)
		}
		klog.V(klogLevel).Infof("setting version %s=%s", comp, ver.String())
		setQueue = append(setQueue, componentVersion{comp, ver})
	}
	for len(setQueue) > 0 {
		cv := setQueue[0]
		// 已访问过仍再次入队 = 映射成环，必须拒绝以避免无限循环。
		if _, visited := result[cv.component]; visited {
			return result, fmt.Errorf("setting version of %s more than once, probably version mapping loop", cv.component)
		}
		setQueue = setQueue[1:]
		result[cv.component] = cv.ver
		for toComp, f := range r.componentGlobals[cv.component].componentVersionMapping {
			toVer := f(cv.ver)
			if toVer == nil {
				return result, fmt.Errorf("got nil version from mapping of %s=%s to component:%s", cv.component, cv.ver.String(), toComp)
			}
			klog.V(klogLevel).Infof("setting version %s=%s from version mapping of %s=%s", toComp, toVer.String(), cv.component, cv.ver.String())
			setQueue = append(setQueue, componentVersion{toComp, toVer})
		}
	}
	return result, nil
}

// toVersionMap 解析 "component=major.minor" 形式的标志值；无组件前缀时归到 kube。
// 仅允许 major.minor：补丁号在版本偏差语义中无意义，也会让映射变得脆弱。
func toVersionMap(versionConfig []string) (map[string]*version.Version, error) {
	m := map[string]*version.Version{}
	for _, compVer := range versionConfig {
		// 未指定组件时默认使用 "kube"。
		k := "kube"
		v := compVer
		if strings.Contains(compVer, "=") {
			arr := strings.SplitN(compVer, "=", 2)
			if len(arr) != 2 {
				return m, fmt.Errorf("malformed pair, expect string=string")
			}
			k = strings.TrimSpace(arr[0])
			v = strings.TrimSpace(arr[1])
		}
		ver, err := version.Parse(v)
		if err != nil {
			return m, err
		}
		// 补丁版本不参与兼容性判定，出现即为配置错误。
		if ver.Patch() != 0 {
			return m, fmt.Errorf("patch version not allowed, got: %s=%s", k, ver.String())
		}
		// 同一组件重复出现会让结果依赖顺序，直接拒绝。
		if existingVer, ok := m[k]; ok {
			return m, fmt.Errorf("duplicate version flag, %s=%s and %s=%s", k, existingVer.String(), k, ver.String())
		}
		m[k] = ver
	}
	return m, nil
}

// SetFallback 在 Set 尚未调用时补做一次，并输出警告。
// 这是给未正确初始化注册表的调用方的兜底，避免零值语义静默生效。
func (r *componentGlobalsRegistry) SetFallback() error {
	r.mutex.Lock()
	set := r.set
	r.mutex.Unlock()
	if set {
		return nil
	}
	klog.Warning("setting componentGlobalsRegistry in SetFallback. We recommend calling componentGlobalsRegistry.Set()" +
		" right after parsing flags to avoid using feature gates before their final values are set by the flags.")
	return r.Set()
}

// Set 解析暂存的标志值并依次应用：模拟版本 → 最小兼容版本 → 特性门控。
//
// 顺序不可颠倒：门控的可用阶段取决于前两者，提前设置会得到错误结果。
// 返回错误后注册表可能处于部分应用状态，调用方不应继续启动。
func (r *componentGlobalsRegistry) Set() error {
	r.mutex.Lock()
	defer r.mutex.Unlock()
	r.set = true
	emulationVersionConfigMap, err := toVersionMap(r.emulationVersionConfig)
	if err != nil {
		return err
	}
	for comp := range emulationVersionConfigMap {
		if _, ok := r.componentGlobals[comp]; !ok {
			return fmt.Errorf("component not registered: %s", comp)
		}
		// 只能从标志设置无依赖的组件；被映射接管的组件若同时由标志设置，会产生两个事实来源。
		if r.componentGlobals[comp].dependentComponentVersion {
			return fmt.Errorf("EmulationVersion of %s is set by mapping, cannot set it by flag", comp)
		}
	}
	if emulationVersions, err := r.getFullVersionConfig(emulationVersionConfigMap); err != nil {
		return err
	} else {
		// 先设置模拟版本，供后续最小兼容版本与门控计算使用。
		for comp, ver := range emulationVersions {
			r.componentGlobals[comp].effectiveVersion.SetEmulationVersion(ver)
		}
	}

	minCompatibilityVersionConfigMap, err := toVersionMap(r.minCompatibilityVersionConfig)
	if err != nil {
		return err
	}
	for comp := range minCompatibilityVersionConfigMap {
		if _, ok := r.componentGlobals[comp]; !ok {
			return fmt.Errorf("component not registered: %s", comp)
		}
		// 只能从标志设置无依赖的组件。
		if r.componentGlobals[comp].dependentComponentVersion {
			return fmt.Errorf("MinCompatibilityVersion of %s is set by mapping, cannot set it by flag", comp)
		}
	}
	if minCompatibilityVersions, err := r.getFullVersionConfig(minCompatibilityVersionConfigMap); err != nil {
		return err
	} else {
		for comp, ver := range minCompatibilityVersions {
			r.componentGlobals[comp].effectiveVersion.SetMinCompatibilityVersion(ver)
		}
	}
	// 设置特性门控标志值前，先设置其模拟版本。
	// 门控内部按版本过滤特性，必须先知道版本再应用布尔值，否则可能启用不该存在的特性。
	for comp, globals := range r.componentGlobals {
		if globals.featureGate == nil {
			continue
		}
		klog.V(klogLevel).Infof("setting %s:feature gate emulation version to %s, min compatibility version to %s", comp, globals.effectiveVersion.EmulationVersion().String(), globals.effectiveVersion.MinCompatibilityVersion().String())
		if err := globals.featureGate.SetEmulationVersionAndMinCompatibilityVersion(globals.effectiveVersion.EmulationVersion(), globals.effectiveVersion.MinCompatibilityVersion()); err != nil {
			return err
		}
	}
	for comp, fg := range r.featureGatesConfig {
		if comp == "" {
			// 空前缀等价于 kube；两种写法混用会让同一组件出现两组配置，故拒绝。
			if _, ok := r.featureGatesConfig[DefaultKubeComponent]; ok {
				return fmt.Errorf("set kube feature gates with default empty prefix or kube: prefix consistently, do not mix use")
			}
			comp = DefaultKubeComponent
		}
		if _, ok := r.componentGlobals[comp]; !ok {
			return fmt.Errorf("component not registered: %s", comp)
		}
		featureGate := r.componentGlobals[comp].featureGate
		if featureGate == nil {
			return fmt.Errorf("component featureGate not registered: %s", comp)
		}
		flagVal := strings.Join(fg, ",")
		klog.V(klogLevel).Infof("setting %s:feature-gates=%s", comp, flagVal)
		if err := featureGate.Set(flagVal); err != nil {
			return err
		}
	}
	return nil
}

// Validate 对所有组件执行版本与门控校验，并汇总错误。
//
// 另外对“模拟旧版本却启用 alpha 特性”的组合发出警告：这通常意味着用户
// 显式打开了模拟版本中不存在的特性，属于不受支持的配置。
func (r *componentGlobalsRegistry) Validate() []error {
	var errs []error
	r.mutex.Lock()
	defer r.mutex.Unlock()
	for _, globals := range r.componentGlobals {
		errs = append(errs, globals.effectiveVersion.Validate()...)
		var features map[featuregate.Feature]featuregate.FeatureSpec
		if globals.featureGate != nil {
			errs = append(errs, globals.featureGate.Validate()...)
			features = globals.featureGate.GetAll()
		}
		binaryVersion := globals.effectiveVersion.BinaryVersion()
		emulatedVersion := globals.effectiveVersion.EmulationVersion()
		if binaryVersion.GreaterThan(emulatedVersion) {
			// 只有向下模拟时才可能出现“旧版本 + 新 alpha 特性”的矛盾组合。
			if enabled := enabledAlphaFeatures(features, globals); len(enabled) != 0 {
				klog.Warningf("component has alpha features enabled in emulated version, this is unsupported: features=%v", enabled)
			}
		}
	}
	return errs
}

// enabledAlphaFeatures 返回当前已启用的 alpha 特性名，供 Validate 生成告警。
func enabledAlphaFeatures(features map[featuregate.Feature]featuregate.FeatureSpec, globals *ComponentGlobals) []string {
	var enabled []string
	for feat, featSpec := range features {
		if featSpec.PreRelease == featuregate.Alpha && globals.featureGate.Enabled(feat) {
			enabled = append(enabled, string(feat))
		}
	}
	return enabled
}

// SetVersionMapping 建立 fromComponent → toComponent 的版本映射。
//
// 一旦建立，toComponent 的版本只能由映射推导，不再接受命令行标志；
// 同时限制每个组件最多有一个上游，防止依赖关系含糊。
// f 为 nil 时视作无操作，便于调用方按需传入可选映射。
func (r *componentGlobalsRegistry) SetVersionMapping(fromComponent, toComponent string, f VersionMapping) error {
	if f == nil {
		return nil
	}
	klog.V(klogLevel).Infof("setting EmulationVersion mapping from %s to %s", fromComponent, toComponent)
	r.mutex.Lock()
	defer r.mutex.Unlock()
	if _, ok := r.componentGlobals[fromComponent]; !ok {
		return fmt.Errorf("component not registered: %s", fromComponent)
	}
	if _, ok := r.componentGlobals[toComponent]; !ok {
		return fmt.Errorf("component not registered: %s", toComponent)
	}
	// 检查重复依赖：每个组件只允许一个上游，否则版本来源不确定。
	if r.componentGlobals[toComponent].dependentComponentVersion {
		return fmt.Errorf("mapping of %s already exists from another component", toComponent)
	}
	r.componentGlobals[toComponent].dependentComponentVersion = true

	versionMapping := r.componentGlobals[fromComponent].componentVersionMapping
	if _, ok := versionMapping[toComponent]; ok {
		return fmt.Errorf("EmulationVersion from %s to %s already exists", fromComponent, toComponent)
	}
	versionMapping[toComponent] = f
	klog.V(klogLevel).Infof("setting the default EmulationVersion of %s based on mapping from the default EmulationVersion of %s", toComponent, fromComponent)
	// 立即用源组件当前版本推导一次，使映射在 Set 之前就有一致初值。
	defaultFromVersion := r.componentGlobals[fromComponent].effectiveVersion.EmulationVersion()
	emulationVersions, err := r.getFullVersionConfig(map[string]*version.Version{fromComponent: defaultFromVersion})
	if err != nil {
		return err
	}
	for comp, ver := range emulationVersions {
		r.componentGlobals[comp].effectiveVersion.SetEmulationVersion(ver)
	}
	return nil
}
