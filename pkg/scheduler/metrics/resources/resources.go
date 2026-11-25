/*
Copyright 2020 The Kubernetes Authors.

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

// Package resources 提供资源指标收集器，按调度器和 kubelet 的视角
// 上报集群中 Pod 的资源消耗（requests 和 limits）。
package resources

import (
	"net/http"
	"strconv"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/component-base/metrics"

	resourcehelper "k8s.io/component-helpers/resource"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
)

// resourceLifecycleDescriptors 保存某类资源（request 或 limit）的指标描述符。
type resourceLifecycleDescriptors struct {
	total *metrics.Desc
}

// Describe 将本类资源的描述符写入采集通道。
func (d resourceLifecycleDescriptors) Describe(ch chan<- *metrics.Desc) {
	ch <- d.total
}

// resourceMetricsDescriptors 聚合 request 与 limit 两类资源的描述符。
type resourceMetricsDescriptors struct {
	requests resourceLifecycleDescriptors
	limits   resourceLifecycleDescriptors
}

// Describe 依次注册 request 与 limit 的描述符。
func (d resourceMetricsDescriptors) Describe(ch chan<- *metrics.Desc) {
	d.requests.Describe(ch)
	d.limits.Describe(ch)
}

// podResourceDesc 定义 Pod 资源指标的元数据。
// 指标按命名空间、Pod、节点、调度器、优先级、资源名和单位拆分。
var podResourceDesc = resourceMetricsDescriptors{
	requests: resourceLifecycleDescriptors{
		total: metrics.NewDesc("kube_pod_resource_request",
			"Resources requested by workloads on the cluster, broken down by pod. This shows the resource usage the scheduler and kubelet expect per pod for resources along with the unit for the resource if any.",
			[]string{"namespace", "pod", "node", "scheduler", "priority", "resource", "unit"},
			nil,
			metrics.STABLE,
			""),
	},
	limits: resourceLifecycleDescriptors{
		total: metrics.NewDesc("kube_pod_resource_limit",
			"Resources limit for workloads on the cluster, broken down by pod. This shows the resource usage the scheduler and kubelet expect per pod for resources along with the unit for the resource if any.",
			[]string{"namespace", "pod", "node", "scheduler", "priority", "resource", "unit"},
			nil,
			metrics.STABLE,
			""),
	},
}

// Handler 基于给定的 podLister 创建收集器，并返回按 Prometheus 格式
// 上报所需指标的 http.Handler。该 Handler 只包含本包的指标，不含其他指标。
func Handler(podLister corelisters.PodLister) http.Handler {
	collector := NewPodResourcesMetricsCollector(podLister)
	registry := metrics.NewKubeRegistry()
	registry.CustomMustRegister(collector)
	return metrics.HandlerWithReset(registry, metrics.HandlerOpts{})
}

// 编译期校验 podResourceCollector 实现了 StableCollector 接口。
var _ metrics.StableCollector = &podResourceCollector{}

// NewPodResourcesMetricsCollector 创建一个基数与 Pod 数量同阶（O(pods)）的指标收集器，
// 按 Kubernetes 资源模型上报集群中所有 Pod 当前请求的资源量。
// 指标按 Pod、节点、资源和生命周期阶段拆分：每个 Pod 的每种资源会产生
// 聚合用量（调度所需）和特定阶段用量两条序列，
// 便于管理员评估不同启动阶段的资源开销并与实际用量对比。
func NewPodResourcesMetricsCollector(podLister corelisters.PodLister) metrics.StableCollector {
	return &podResourceCollector{
		lister: podLister,
	}
}

// podResourceCollector 从 Pod lister 收集资源指标。
type podResourceCollector struct {
	metrics.BaseStableCollector
	lister corelisters.PodLister
}

// DescribeWithStability 注册所有资源指标的描述符。
func (c *podResourceCollector) DescribeWithStability(ch chan<- *metrics.Desc) {
	podResourceDesc.Describe(ch)
}

// CollectWithStability 遍历所有 Pod，上报其 request 与 limit 指标。
func (c *podResourceCollector) CollectWithStability(ch chan<- metrics.Metric) {
	pods, err := c.lister.List(labels.Everything())
	if err != nil {
		return
	}
	reuseReqs, reuseLimits := make(v1.ResourceList, 4), make(v1.ResourceList, 4)
	for _, p := range pods {
		reqs, limits, terminal := podRequestsAndLimitsByLifecycle(p, reuseReqs, reuseLimits)
		if terminal {
			// 终态 Pod 不再占用调度资源，排除在统计之外
			continue
		}
		for _, t := range []struct {
			desc  resourceLifecycleDescriptors
			total v1.ResourceList
		}{
			{
				desc:  podResourceDesc.requests,
				total: reqs,
			},
			{
				desc:  podResourceDesc.limits,
				total: limits,
			},
		} {
			for resourceName, val := range t.total {
				var unitName string
				switch resourceName {
				case v1.ResourceCPU:
					unitName = "cores"
				case v1.ResourceMemory:
					unitName = "bytes"
				case v1.ResourceStorage:
					unitName = "bytes"
				case v1.ResourceEphemeralStorage:
					unitName = "bytes"
				default:
					switch {
					case v1helper.IsHugePageResourceName(resourceName):
						unitName = "bytes"
					case v1helper.IsAttachableVolumeResourceName(resourceName):
						unitName = "integer"
					}
				}
				var priority string
				if p.Spec.Priority != nil {
					priority = strconv.FormatInt(int64(*p.Spec.Priority), 10)
				}
				recordMetricWithUnit(ch, t.desc.total, p.Namespace, p.Name, p.Spec.NodeName, p.Spec.SchedulerName, priority, resourceName, unitName, val)
			}
		}
	}
}

// recordMetricWithUnit 上报单个资源的指标，附带单位标签。
// 零值不产生序列，避免指标中出现无意义的条目。
func recordMetricWithUnit(
	ch chan<- metrics.Metric,
	desc *metrics.Desc,
	namespace, name, nodeName, schedulerName, priority string,
	resourceName v1.ResourceName,
	unit string,
	val resource.Quantity,
) {
	if val.IsZero() {
		return
	}
	ch <- metrics.NewLazyConstMetric(desc, metrics.GaugeValue,
		val.AsApproximateFloat64(),
		namespace, name, nodeName, schedulerName, priority, string(resourceName), unit,
	)
}

// podRequestsAndLimitsByLifecycle 汇总 Pod 所有容器的资源 request 与 limit，
// 并将 Pod overhead 计入非零的 request/limit 总量。
// 调用方可传入复用的 resource list 以避免分配，函数使用前会清空它。
// 语义与 resourcehelper.PodRequestsAndLimits 相同，但通过复用减少了若干场景下的内存分配。
// 终态 Pod 返回 terminal=true，调用方应跳过统计。
func podRequestsAndLimitsByLifecycle(pod *v1.Pod, reuseReqs, reuseLimits v1.ResourceList) (reqs, limits v1.ResourceList, terminal bool) {
	switch {
	case len(pod.Spec.NodeName) == 0:
		// 未调度的 Pod 不可能是终态
	case pod.Status.Phase == v1.PodSucceeded, pod.Status.Phase == v1.PodFailed:
		terminal = true
		// TODO: 待 https://github.com/kubernetes/kubernetes/issues/96515 解决后，在此补充终态判定条件。
	}
	if terminal {
		return
	}

	reqs = resourcehelper.PodRequests(pod, resourcehelper.PodResourcesOptions{Reuse: reuseReqs})
	limits = resourcehelper.PodLimits(pod, resourcehelper.PodResourcesOptions{Reuse: reuseLimits})
	return
}
