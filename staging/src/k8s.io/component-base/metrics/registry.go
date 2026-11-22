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
	registries     []*kubeRegistry // stores all registries created by NewKubeRegistry()
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

// shouldHide is used to check if a specific metric with deprecated version should be hidden
// according to metrics deprecation lifecycle.
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

	// deprecatedVersion.Major == currentVersion.Major
	case hiddenMinor < currentVersion.Minor:
		return true
	case hiddenMinor > currentVersion.Minor:
		return false

	// deprecatedVersion.Minor == currentVersion.Minor
	case strings.Contains(currentVersion.String(), "alpha.0"):
		// Wait until we're past the alpha.0 period of a minor development cycle to hide metrics whose deprecation period ends in that minor version.
		// See discussion in https://github.com/kubernetes/kubernetes/issues/133429#issuecomment-3165551443
		return false
	default:
		return true
	}
}

// getDeprecationReleaseWindow returns the number of minor releases a metric should be served
// after its deprecated version, based on its stability level.
func deprecationPeriodMinorVersions(stabilityLevel StabilityLevel) uint64 {
	switch stabilityLevel {
	case STABLE:
		return 3
	case BETA:
		return 1
	default: // ALPHA, INTERNAL
		return 0
	}
}

// isDeprecated returns true if the current version, ignoring pre-release tags,
// is greater than or equal to the deprecated version.
func isDeprecated(currentVersion, deprecatedVersion semver.Version) bool {
	switch {
	case currentVersion.Major < deprecatedVersion.Major:
		return false
	case currentVersion.Major > deprecatedVersion.Major:
		return true
	}

	// currentVersion.Major == deprecatedVersion.Major
	return currentVersion.Minor >= deprecatedVersion.Minor
}

// Registerable is an interface for a collector metric which we
// will register with KubeRegistry.
//
// Create 由注册表在注册时调用，决定该指标在当前版本下是可见还是隐藏。
type Registerable interface {
	prometheus.Collector

	// Create will mark deprecated state for the collector
	Create(version *semver.Version) bool

	// ClearState will clear all the states marked by Create.
	ClearState()

	// FQName returns the fully-qualified metric name of the collector.
	FQName() string
}

// resettable 标记可清零的指标；注册表在 Reset 时统一调用。
type resettable interface {
	Reset()
}

// KubeRegistry is an interface which implements a subset of prometheus.Registerer and
// prometheus.Gatherer interfaces
type KubeRegistry interface {
	// Deprecated
	RawMustRegister(...prometheus.Collector)
	// CustomRegister is our internal variant of Prometheus registry.Register
	CustomRegister(c StableCollector) error
	// CustomMustRegister is our internal variant of Prometheus registry.MustRegister
	CustomMustRegister(cs ...StableCollector)
	// Register conforms to Prometheus registry.Register
	Register(Registerable) error
	// MustRegister conforms to Prometheus registry.MustRegister
	MustRegister(...Registerable)
	// Unregister conforms to Prometheus registry.Unregister
	Unregister(collector Collector) bool
	// Gather conforms to Prometheus gatherer.Gather
	Gather() ([]*dto.MetricFamily, error)
	// Reset invokes the Reset() function on all items in the registry
	// which are added as resettables.
	Reset()
	// RegisterMetaMetrics registers metrics about the number of registered metrics.
	RegisterMetaMetrics()
	// Registerer exposes the underlying prometheus registerer
	Registerer() prometheus.Registerer
	// Gatherer exposes the underlying prometheus gatherer
	Gatherer() prometheus.Gatherer
}

// kubeRegistry is a wrapper around a prometheus registry-type object. Upon initialization
// the kubernetes binary version information is loaded into the registry object, so that
// automatic behavior can be configured for metric versioning.
//
// hiddenCollectors 保存当前版本应隐藏的指标，stableCollectors 保存需要按 StableCollector
// 语义重新评估的自定义收集器；两者分开是因为重新注册方式不同。
type kubeRegistry struct {
	PromRegistry
	version              semver.Version
	hiddenCollectors     map[string]Registerable // stores all collectors that has been hidden
	stableCollectors     []StableCollector       // stores all stable collector
	hiddenCollectorsLock sync.RWMutex
	stableCollectorsLock sync.RWMutex
	resetLock            sync.RWMutex
	resettables          []resettable
}

// Register registers a new Collector to be included in metrics
// collection. It returns an error if the descriptors provided by the
// Collector are invalid or if they — in combination with descriptors of
// already registered Collectors — do not fulfill the consistency and
// uniqueness criteria described in the documentation of metric.Desc.
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

// Registerer exposes the underlying prometheus.Registerer
func (kr *kubeRegistry) Registerer() prometheus.Registerer {
	return kr.PromRegistry
}

// Gatherer exposes the underlying prometheus.Gatherer
func (kr *kubeRegistry) Gatherer() prometheus.Gatherer {
	return kr.PromRegistry
}

// MustRegister works like Register but registers any number of
// Collectors and panics upon the first registration that causes an
// error.
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

// CustomRegister registers a new custom collector.
func (kr *kubeRegistry) CustomRegister(c StableCollector) error {
	// 先登记到 stableCollectors，确保隐藏指标在重新启用时仍能找到该收集器。
	kr.trackStableCollectors(c)
	defer kr.addResettable(c)
	if c.Create(&kr.version, c) {
		return kr.PromRegistry.Register(c)
	}
	return nil
}

// CustomMustRegister works like CustomRegister but registers any number of
// StableCollectors and panics upon the first registration that causes an
// error.
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

// RawMustRegister takes a native prometheus.Collector and registers the collector
// to the registry. This bypasses metrics safety checks, so should only be used
// to register custom prometheus collectors.
//
// Deprecated
func (kr *kubeRegistry) RawMustRegister(cs ...prometheus.Collector) {
	// 绕过版本/稳定性检查，直接注册；仍加入 reset 列表以保持 Reset 语义一致。
	kr.PromRegistry.MustRegister(cs...)
	for _, c := range cs {
		kr.addResettable(c)
	}
}

// addResettable will automatically add our metric to our reset
// list if it satisfies the interface
func (kr *kubeRegistry) addResettable(i interface{}) {
	kr.resetLock.Lock()
	defer kr.resetLock.Unlock()
	// 只有实现了 resettable 的收集器才需要登记，其他类型静默跳过。
	if resettable, ok := i.(resettable); ok {
		kr.resettables = append(kr.resettables, resettable)
	}
}

// Unregister unregisters the Collector that equals the Collector passed
// in as an argument.  (Two Collectors are considered equal if their
// Describe method yields the same set of descriptors.) The function
// returns whether a Collector was unregistered. Note that an unchecked
// Collector cannot be unregistered (as its Describe method does not
// yield any descriptor).
func (kr *kubeRegistry) Unregister(collector Collector) bool {
	return kr.PromRegistry.Unregister(collector)
}

// Gather calls the Collect method of the registered Collectors and then
// gathers the collected metrics into a lexicographically sorted slice
// of uniquely named MetricFamily protobufs. Gather ensures that the
// returned slice is valid and self-consistent so that it can be used
// for valid exposition. As an exception to the strict consistency
// requirements described for metric.Desc, Gather will tolerate
// different sets of label names for metrics of the same metric family.
func (kr *kubeRegistry) Gather() ([]*dto.MetricFamily, error) {
	return kr.PromRegistry.Gather()
}

// trackHiddenCollector stores all hidden collectors.
func (kr *kubeRegistry) trackHiddenCollector(c Registerable) {
	kr.hiddenCollectorsLock.Lock()
	defer kr.hiddenCollectorsLock.Unlock()

	kr.hiddenCollectors[c.FQName()] = c
	hiddenMetricsTotal.Inc()
}

// trackStableCollectors stores all custom collectors.
func (kr *kubeRegistry) trackStableCollectors(cs ...StableCollector) {
	kr.stableCollectorsLock.Lock()
	defer kr.stableCollectorsLock.Unlock()

	kr.stableCollectors = append(kr.stableCollectors, cs...)
}

// enableHiddenCollectors will re-register all the hidden collectors.
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

// enableHiddenStableCollectors will re-register the stable collectors if there is one or more hidden metrics in it.
// Since we can not register a metrics twice, so we have to unregister first then register again.
func (kr *kubeRegistry) enableHiddenStableCollectors() {
	if len(kr.stableCollectors) == 0 {
		return
	}

	kr.stableCollectorsLock.Lock()

	cs := make([]StableCollector, 0, len(kr.stableCollectors))
	for _, c := range kr.stableCollectors {
		if len(c.HiddenMetrics()) > 0 {
			kr.Unregister(c) // unregister must happens before clear state, otherwise no metrics would be unregister
			c.ClearState()
			cs = append(cs, c)
		}
	}

	kr.stableCollectors = nil
	kr.stableCollectorsLock.Unlock()
	kr.CustomMustRegister(cs...)
}

// Reset invokes Reset on all metrics that are resettable.
func (kr *kubeRegistry) Reset() {
	// RLock 即可：Reset 只遍历切片，登记新指标时由写锁保护追加。
	kr.resetLock.RLock()
	defer kr.resetLock.RUnlock()
	for _, r := range kr.resettables {
		r.Reset()
	}
}

// BuildVersion is a helper function that can be easily mocked.
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

// NewKubeRegistry creates a new vanilla Registry
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
