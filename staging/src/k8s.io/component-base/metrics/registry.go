/*
Copyright 2019 The Kubernetes Authors.

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

package metrics

import (
	"strings"
	"sync"

	"github.com/blang/semver/v4"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	apimachineryversion "k8s.io/apimachinery/pkg/version"
	"k8s.io/component-base/version"
)

var (
	registries     []*kubeRegistry // 保存 NewKubeRegistry 创建的所有注册表
	registriesLock sync.RWMutex

	registeredMetricsTotal = NewCounterVec(
		&CounterOpts{
			Name:           "registered_metrics_total",
			Help:           "The count of registered metrics broken by stability level and deprecation version.",
			StabilityLevel: BETA,
		},
		[]string{"stability_level", "deprecated_version"},
	)
)

// shouldHide 根据指标弃用生命周期判断指定指标是否应在当前版本隐藏。
//
// 隐藏规则：超过弃用宽限期后隐藏（不注册、不暴露），但仍保留定义以便版本升级时
// 有机会展示并提醒用户更新监控。宽限期按稳定性级别分别为 STABLE 3 个、BETA 1 个、
// ALPHA/INTERNAL 0 个 minor 版本。
func shouldHide(stabilityLevel StabilityLevel, currentVersion *semver.Version, deprecatedVersion *semver.Version) bool {
	hiddenMinor := deprecatedVersion.Minor + deprecationPeriodMinorVersions(stabilityLevel)

	switch {
	case deprecatedVersion.Major < currentVersion.Major:
		return true
	case deprecatedVersion.Major > currentVersion.Major:
		return false

	// 主版本号相同
	case hiddenMinor < currentVersion.Minor:
		return true
	case hiddenMinor > currentVersion.Minor:
		return false

	// 弃用版本与当前版本的次版本号相同
	case strings.Contains(currentVersion.String(), "alpha.0"):
		// 对于宽限期在当前次版本结束的指标，需等到该次版本开发周期越过 alpha.0 后再隐藏。
		// 讨论见 https://github.com/kubernetes/kubernetes/issues/133429#issuecomment-3165551443
		return false
	default:
		return true
	}
}

// deprecationPeriodMinorVersions 根据稳定性级别返回指标弃用后应继续提供的次版本数。
func deprecationPeriodMinorVersions(stabilityLevel StabilityLevel) uint64 {
	switch stabilityLevel {
	case STABLE:
		return 3
	case BETA:
		return 1
	default: // ALPHA、INTERNAL
		return 0
	}
}

// isDeprecated 忽略预发布标记，判断当前版本是否已达到或超过弃用版本。
func isDeprecated(currentVersion, deprecatedVersion semver.Version) bool {
	switch {
	case currentVersion.Major < deprecatedVersion.Major:
		return false
	case currentVersion.Major > deprecatedVersion.Major:
		return true
	}

	// 主版本号相同
	return currentVersion.Minor >= deprecatedVersion.Minor
}

// Registerable 是注册到 KubeRegistry 的指标收集器接口。
//
// Create 由注册表在注册时调用，决定该指标在当前版本下是可见还是隐藏。
type Registerable interface {
	prometheus.Collector

	// Create 标记收集器的弃用状态，并返回其当前是否可见。
	Create(version *semver.Version) bool

	// ClearState 清除 Create 标记的所有状态。
	ClearState()

	// FQName 返回收集器的完整指标名称。
	FQName() string
}

// resettable 标记可清零的指标；注册表在 Reset 时统一调用。
type resettable interface {
	Reset()
}

// KubeRegistry 实现 prometheus.Registerer 和 prometheus.Gatherer 接口的部分功能。
type KubeRegistry interface {
	// Deprecated: 已弃用。
	RawMustRegister(...prometheus.Collector)
	// CustomRegister 是 Prometheus registry.Register 的内部变体，用于注册自定义收集器。
	CustomRegister(c StableCollector) error
	// CustomMustRegister 是 Prometheus registry.MustRegister 的内部变体，用于注册自定义收集器。
	CustomMustRegister(cs ...StableCollector)
	// Register 注册指标收集器，语义与 Prometheus registry.Register 一致。
	Register(Registerable) error
	// MustRegister 注册一个或多个指标收集器，语义与 Prometheus registry.MustRegister 一致。
	MustRegister(...Registerable)
	// Unregister 注销指定收集器，语义与 Prometheus registry.Unregister 一致。
	Unregister(collector Collector) bool
	// Gather 收集并汇总指标，语义与 Prometheus gatherer.Gather 一致。
	Gather() ([]*dto.MetricFamily, error)
	// Reset 调用注册表中所有可重置指标的 Reset 方法。
	Reset()
	// RegisterMetaMetrics 注册描述已注册指标数量的元指标。
	RegisterMetaMetrics()
	// Registerer 返回底层的 prometheus.Registerer。
	Registerer() prometheus.Registerer
	// Gatherer 返回底层的 prometheus.Gatherer。
	Gatherer() prometheus.Gatherer
}

// kubeRegistry 封装 Prometheus 注册表，并在初始化时载入 Kubernetes 二进制版本信息，
// 以配置指标版本相关的自动行为。
//
// hiddenCollectors 保存当前版本应隐藏的指标，stableCollectors 保存需要按 StableCollector
// 语义重新评估的自定义收集器；两者分开是因为重新注册方式不同。
type kubeRegistry struct {
	PromRegistry
	version              semver.Version
	hiddenCollectors     map[string]Registerable // 保存所有已隐藏的收集器
	stableCollectors     []StableCollector       // 保存所有稳定收集器
	hiddenCollectorsLock sync.RWMutex
	stableCollectorsLock sync.RWMutex
	resetLock            sync.RWMutex
	resettables          []resettable
}

// Register 注册一个参与指标收集的新收集器。如果其描述符无效，或与已注册收集器的
// 描述符组合后不满足 metric.Desc 文档规定的一致性和唯一性要求，则返回错误。
func (kr *kubeRegistry) Register(c Registerable) error {
	// Create 返回 false 表示该指标在当前版本被隐藏：记录名称，等待
	// ShowHiddenMetricsForVersion 时再启用。
	if c.Create(&kr.version) {
		defer kr.addResettable(c)
		return kr.PromRegistry.Register(c)
	}

	kr.trackHiddenCollector(c)
	return nil
}

// Registerer 返回底层的 prometheus.Registerer。
func (kr *kubeRegistry) Registerer() prometheus.Registerer {
	return kr.PromRegistry
}

// Gatherer 返回底层的 prometheus.Gatherer。
func (kr *kubeRegistry) Gatherer() prometheus.Gatherer {
	return kr.PromRegistry
}

// MustRegister 注册任意数量的收集器；与 Register 不同，首次注册出错时会 panic。
func (kr *kubeRegistry) MustRegister(cs ...Registerable) {
	metrics := make([]prometheus.Collector, 0, len(cs))
	for _, c := range cs {
		// 先筛选可见指标，再一次性交给 prometheus，避免部分注册的状态。
		if c.Create(&kr.version) {
			metrics = append(metrics, c)
			kr.addResettable(c)
		} else {
			kr.trackHiddenCollector(c)
		}
	}
	kr.PromRegistry.MustRegister(metrics...)
}

// CustomRegister 注册一个新的自定义收集器。
func (kr *kubeRegistry) CustomRegister(c StableCollector) error {
	// 先登记到 stableCollectors，确保隐藏指标在重新启用时仍能找到该收集器。
	kr.trackStableCollectors(c)
	defer kr.addResettable(c)
	if c.Create(&kr.version, c) {
		return kr.PromRegistry.Register(c)
	}
	return nil
}

// CustomMustRegister 注册任意数量的 StableCollector；与 CustomRegister 不同，
// 首次注册出错时会 panic。
func (kr *kubeRegistry) CustomMustRegister(cs ...StableCollector) {
	kr.trackStableCollectors(cs...)
	collectors := make([]prometheus.Collector, 0, len(cs))
	for _, c := range cs {
		if c.Create(&kr.version, c) {
			kr.addResettable(c)
			collectors = append(collectors, c)
		}
	}
	kr.PromRegistry.MustRegister(collectors...)
}

// RawMustRegister 将原生 prometheus.Collector 注册到注册表。
// 此方法绕过指标安全检查，只应用于注册自定义 Prometheus 收集器。
//
// Deprecated: 已弃用。
func (kr *kubeRegistry) RawMustRegister(cs ...prometheus.Collector) {
	// 绕过版本/稳定性检查，直接注册；仍加入 reset 列表以保持 Reset 语义一致。
	kr.PromRegistry.MustRegister(cs...)
	for _, c := range cs {
		kr.addResettable(c)
	}
}

// addResettable 在传入对象实现相应接口时，将其自动加入重置列表。
func (kr *kubeRegistry) addResettable(i interface{}) {
	kr.resetLock.Lock()
	defer kr.resetLock.Unlock()
	// 只有实现了 resettable 的收集器才需要登记，其他类型静默跳过。
	if resettable, ok := i.(resettable); ok {
		kr.resettables = append(kr.resettables, resettable)
	}
}

// Unregister 注销与参数相等的收集器，并返回是否成功注销。两个收集器的 Describe 方法
// 返回相同描述符集合时视为相等。未检查的收集器无法注销，因为其 Describe 不返回描述符。
func (kr *kubeRegistry) Unregister(collector Collector) bool {
	return kr.PromRegistry.Unregister(collector)
}

// Gather 调用已注册收集器的 Collect 方法，并将收集到的指标汇总为按字典序排列、
// 名称唯一的 MetricFamily protobuf 切片。返回切片必须有效且内部一致，以便正确输出。
// 作为 metric.Desc 严格一致性要求的例外，Gather 允许同一指标族使用不同的标签名称集合。
func (kr *kubeRegistry) Gather() ([]*dto.MetricFamily, error) {
	return kr.PromRegistry.Gather()
}

// trackHiddenCollector 保存所有已隐藏的收集器。
func (kr *kubeRegistry) trackHiddenCollector(c Registerable) {
	kr.hiddenCollectorsLock.Lock()
	defer kr.hiddenCollectorsLock.Unlock()

	kr.hiddenCollectors[c.FQName()] = c
	hiddenMetricsTotal.Inc()
}

// trackStableCollectors 保存所有自定义收集器。
func (kr *kubeRegistry) trackStableCollectors(cs ...StableCollector) {
	kr.stableCollectorsLock.Lock()
	defer kr.stableCollectorsLock.Unlock()

	kr.stableCollectors = append(kr.stableCollectors, cs...)
}

// enableHiddenCollectors 重新注册所有已隐藏的收集器。
//
// 在 --show-hidden-metrics-for-version 生效路径上调用：先 ClearState 清除隐藏标记，
// 再通过 MustRegister 重新走可见性判定。
func (kr *kubeRegistry) enableHiddenCollectors() {
	if len(kr.hiddenCollectors) == 0 {
		return
	}

	kr.hiddenCollectorsLock.Lock()
	cs := make([]Registerable, 0, len(kr.hiddenCollectors))

	for _, c := range kr.hiddenCollectors {
		c.ClearState()
		cs = append(cs, c)
	}

	kr.hiddenCollectors = make(map[string]Registerable)
	kr.hiddenCollectorsLock.Unlock()
	kr.MustRegister(cs...)
}

// enableHiddenStableCollectors 在稳定收集器包含一个或多个隐藏指标时重新注册它。
// 由于同一指标不能注册两次，必须先注销，再重新注册。
func (kr *kubeRegistry) enableHiddenStableCollectors() {
	if len(kr.stableCollectors) == 0 {
		return
	}

	kr.stableCollectorsLock.Lock()

	cs := make([]StableCollector, 0, len(kr.stableCollectors))
	for _, c := range kr.stableCollectors {
		if len(c.HiddenMetrics()) > 0 {
			kr.Unregister(c) // 必须先注销再清除状态，否则将无法注销指标
			c.ClearState()
			cs = append(cs, c)
		}
	}

	kr.stableCollectors = nil
	kr.stableCollectorsLock.Unlock()
	kr.CustomMustRegister(cs...)
}

// Reset 调用所有可重置指标的 Reset 方法。
func (kr *kubeRegistry) Reset() {
	// RLock 即可：Reset 只遍历切片，登记新指标时由写锁保护追加。
	kr.resetLock.RLock()
	defer kr.resetLock.RUnlock()
	for _, r := range kr.resettables {
		r.Reset()
	}
}

// BuildVersion 是便于模拟的版本信息辅助函数。
var BuildVersion = version.Get

// newKubeRegistry 以指定版本创建注册表并登记到全局列表，
// 供 show-hidden-metrics 配置对所有注册表统一生效。
func newKubeRegistry(v apimachineryversion.Info) *kubeRegistry {
	r := &kubeRegistry{
		PromRegistry:     prometheus.NewRegistry(),
		version:          parseVersion(v),
		hiddenCollectors: make(map[string]Registerable),
		resettables:      make([]resettable, 0),
	}

	registriesLock.Lock()
	defer registriesLock.Unlock()
	registries = append(registries, r)

	return r
}

// NewKubeRegistry 创建一个新的基础注册表。
//
// 使用当前二进制的构建版本初始化，指标可见性据此判定。
func NewKubeRegistry() KubeRegistry {
	r := newKubeRegistry(BuildVersion())
	return r
}

// RegisterMetaMetrics 注册描述注册表自身的元指标（已注册、已禁用、隐藏、基数等）。
func (r *kubeRegistry) RegisterMetaMetrics() {
	r.MustRegister(registeredMetricsTotal)
	r.MustRegister(disabledMetricsTotal)
	r.MustRegister(hiddenMetricsTotal)
	r.MustRegister(cardinalityEnforcementUnexpectedCategorizationsTotal)
}
