/*
Copyright 2018 The Kubernetes Authors.

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
	"sync"

	"k8s.io/component-base/metrics"
	apiregistrationv1 "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1"
	apiregistrationv1apihelper "k8s.io/kube-aggregator/pkg/apis/apiregistration/v1/helper"
)

/*
 * By default, all the following metrics are defined as falling under
 * ALPHA stability level https://github.com/kubernetes/enhancements/blob/master/keps/sig-instrumentation/1209-metrics-stability/kubernetes-control-plane-metrics-stability.md#stability-classes)
 *
 * Promoting the stability level of the metric is a responsibility of the component owner, since it
 * involves explicitly acknowledging support for the metric across multiple releases, in accordance with
 * the metric stability policy.
 */
// 所有指标默认都是 ALPHA 稳定性：升稳定级别意味着承诺跨多个版本兼容，
// 因此由组件所有者显式决定，而不是在代码里随手提升。
var (
	unavailableGaugeDesc = metrics.NewDesc(
		"aggregator_unavailable_apiservice",
		"Gauge of APIServices which are marked as unavailable broken down by APIService name.",
		[]string{"name"},
		nil,
		metrics.ALPHA,
		"",
	)
)

// 把一次性计数器与持续采集的 gauge 放在同一结构里，是因为它们都由同一个
// APIService 可用性控制器驱动；嵌入 collector 让本类型直接满足 StableCollector。
type Metrics struct {
	unavailableCounter *metrics.CounterVec

	*availabilityCollector
}

// 构造函数不注册指标，由调用方决定注册到哪个 registry，便于测试里使用独立 registry。
func New() *Metrics {
	return &Metrics{
		unavailableCounter: metrics.NewCounterVec(
			&metrics.CounterOpts{
				Name:           "aggregator_unavailable_apiservice_total",
				Help:           "Counter of APIServices which are marked as unavailable broken down by APIService name and reason.",
				StabilityLevel: metrics.ALPHA,
			},
			[]string{"name", "reason"},
		),
		availabilityCollector: newAvailabilityCollector(),
	}
}

// Register registers apiservice availability metrics.
// 两类指标走不同注册入口：普通计数器走 registrationFunc，
// 而需要自定义 Describe/Collect 的 gauge 走 StableCollector 入口。
func (m *Metrics) Register(
	registrationFunc func(metrics.Registerable) error,
	customRegistrationFunc func(metrics.StableCollector) error,
) error {
	err := registrationFunc(m.unavailableCounter)
	if err != nil {
		return err
	}

	err = customRegistrationFunc(m.availabilityCollector)
	if err != nil {
		return err
	}

	return nil
}

// UnavailableCounter returns a counter to track apiservices marked as unavailable.
func (m *Metrics) UnavailableCounter(apiServiceName, reason string) metrics.CounterMetric {
	return m.unavailableCounter.WithLabelValues(apiServiceName, reason)
}

// 用一个 map 保存每个 APIService 的布尔可用性，Collect 时再转换成 gauge；
// 这样 gauge 数量随对象数量变化，无需预先为每个可能的对象创建指标。
type availabilityCollector struct {
	metrics.BaseStableCollector

	mtx            sync.RWMutex
	availabilities map[string]bool
}

// SetUnavailableGauge set the metrics so that it reflect the current state base on availability of the given service
// gauge 表示“当前状态”，所以每次收到 APIService 都无条件覆盖旧值，
// 不存在累加语义。
func (m *Metrics) SetUnavailableGauge(newAPIService *apiregistrationv1.APIService) {
	if apiregistrationv1apihelper.IsAPIServiceConditionTrue(newAPIService, apiregistrationv1.Available) {
		m.SetAPIServiceAvailable(newAPIService.Name)
		return
	}

	m.SetAPIServiceUnavailable(newAPIService.Name)
}

// SetUnavailableCounter increases the metrics only if the given service is unavailable and its APIServiceCondition has changed
// 计数器只在“从可用变为不可用”的瞬间自增，避免同一段不可用时间内重复计数；
// 因此必须同时拿到新旧对象来判断状态是否真的翻转。
func (m *Metrics) SetUnavailableCounter(originalAPIService, newAPIService *apiregistrationv1.APIService) {
	wasAvailable := apiregistrationv1apihelper.IsAPIServiceConditionTrue(originalAPIService, apiregistrationv1.Available)
	isAvailable := apiregistrationv1apihelper.IsAPIServiceConditionTrue(newAPIService, apiregistrationv1.Available)
	statusChanged := isAvailable != wasAvailable

	if statusChanged && !isAvailable {
		// 新条件可能尚未写入 Reason，退化到 UnknownReason 而不是丢掉这次计数。
		reason := "UnknownReason"
		if newCondition := apiregistrationv1apihelper.GetAPIServiceConditionByType(newAPIService, apiregistrationv1.Available); newCondition != nil {
			reason = newCondition.Reason
		}
		m.UnavailableCounter(newAPIService.Name, reason).Inc()
	}
}

// Check if apiServiceStatusCollector implements necessary interface.
// 编译期断言，防止接口签名变化后 collector 仍被注册却不被调用。
var _ metrics.StableCollector = &availabilityCollector{}

func newAvailabilityCollector() *availabilityCollector {
	return &availabilityCollector{
		availabilities: make(map[string]bool),
	}
}

// DescribeWithStability implements the metrics.StableCollector interface.
func (c *availabilityCollector) DescribeWithStability(ch chan<- *metrics.Desc) {
	ch <- unavailableGaugeDesc
}

// CollectWithStability implements the metrics.StableCollector interface.
func (c *availabilityCollector) CollectWithStability(ch chan<- metrics.Metric) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	for apiServiceName, isAvailable := range c.availabilities {
		// 指标名是“unavailable”，所以不可用时报 1、可用时报 0；
		// 反过来看起来更直观，但与指标语义保持一致更重要。
		gaugeValue := 1.0
		if isAvailable {
			gaugeValue = 0.0
		}
		ch <- metrics.NewLazyConstMetric(
			unavailableGaugeDesc,
			metrics.GaugeValue,
			gaugeValue,
			apiServiceName,
		)
	}
}

// SetAPIServiceAvailable sets the given apiservice availability gauge to available.
func (c *availabilityCollector) SetAPIServiceAvailable(apiServiceKey string) {
	c.setAPIServiceAvailability(apiServiceKey, true)
}

// SetAPIServiceUnavailable sets the given apiservice availability gauge to unavailable.
func (c *availabilityCollector) SetAPIServiceUnavailable(apiServiceKey string) {
	c.setAPIServiceAvailability(apiServiceKey, false)
}

// 写操作统一走这里，保证对 map 的访问都在同一把写锁下完成；
// 读侧 Collect 用 RLock，允许并发抓取指标。
func (c *availabilityCollector) setAPIServiceAvailability(apiServiceKey string, availability bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.availabilities[apiServiceKey] = availability
}

// ForgetAPIService removes the availability gauge of the given apiservice.
// APIService 删除后必须清理，否则 gauge 会永远停留在最后一次看到的状态，
// 让监控看到已不存在的对象。
func (c *availabilityCollector) ForgetAPIService(apiServiceKey string) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	delete(c.availabilities, apiServiceKey)
}
