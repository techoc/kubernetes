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

package podautoscaler

import (
	"context"
	"fmt"
	"math"
	"time"

	autoscaling "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/util/feature"
	corelisters "k8s.io/client-go/listers/core/v1"
	resourcehelpers "k8s.io/component-helpers/resource"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	metricsclient "k8s.io/kubernetes/pkg/controller/podautoscaler/metrics"
	"k8s.io/kubernetes/pkg/features"
)

const (
	// defaultTestingTolerance 是计算何时扩容/缩容时使用的默认容差值。
	defaultTestingTolerance = 0.1
	// defaultTestingCPUInitializationPeriod 默认 CPU 指标初始化期。
	defaultTestingCPUInitializationPeriod = 2 * time.Minute
	// defaultTestingDelayOfInitialReadinessStatus 默认初始就绪状态判定的延迟时间。
	defaultTestingDelayOfInitialReadinessStatus = 10 * time.Second
)

// Tolerances 包含指标使用率在扩容和缩容方向上的容差范围。
type Tolerances struct {
	scaleDown float64
	scaleUp   float64
}

// String 以百分比形式返回缩容与扩容容差的字符串表示。
func (t Tolerances) String() string {
	return fmt.Sprintf("[down:%.1f%%, up:%.1f%%]", t.scaleDown*100., t.scaleUp*100.)
}

// isWithin 判断使用率是否落在容差区间内，即变化幅度是否小到无需调整副本数。
func (t Tolerances) isWithin(usageRatio float64) bool {
	return (1.0-t.scaleDown) <= usageRatio && usageRatio <= (1.0+t.scaleUp)
}

// ReplicaCalculator 汇集计算目标副本数所需的全部信息。
type ReplicaCalculator struct {
	metricsClient                 metricsclient.MetricsClient
	podLister                     corelisters.PodLister
	cpuInitializationPeriod       time.Duration
	delayOfInitialReadinessStatus time.Duration
}

// NewReplicaCalculator 创建一个新的 ReplicaCalculator，并将所有必要信息传递给新实例。
func NewReplicaCalculator(metricsClient metricsclient.MetricsClient, podLister corelisters.PodLister, cpuInitializationPeriod, delayOfInitialReadinessStatus time.Duration) *ReplicaCalculator {
	return &ReplicaCalculator{
		metricsClient:                 metricsClient,
		podLister:                     podLister,
		cpuInitializationPeriod:       cpuInitializationPeriod,
		delayOfInitialReadinessStatus: delayOfInitialReadinessStatus,
	}
}

// GetResourceReplicas 根据指定命名空间中匹配给定选择器的 Pod 的目标资源利用率百分比，
// 以及当前副本数，计算期望的副本数量。
func (c *ReplicaCalculator) GetResourceReplicas(ctx context.Context, currentReplicas int32, targetUtilization int32, resource v1.ResourceName, tolerances Tolerances, namespace string, selector labels.Selector, container string) (replicaCount int32, utilization int32, rawUtilization int64, timestamp time.Time, err error) {
	metrics, timestamp, err := c.metricsClient.GetResourceMetric(ctx, resource, namespace, selector, container)
	if err != nil {
		return 0, 0, 0, time.Time{}, fmt.Errorf("unable to get metrics for resource %s: %w", resource, err)
	}
	podList, err := c.podLister.Pods(namespace).List(selector)
	if err != nil {
		return 0, 0, 0, time.Time{}, fmt.Errorf("unable to get pods while calculating replica count: %w", err)
	}
	if len(podList) == 0 {
		return 0, 0, 0, time.Time{}, fmt.Errorf("no pods returned by selector while calculating replica count")
	}

	readyPodCount, unreadyPods, missingPods, ignoredPods := groupPods(podList, metrics, resource, c.cpuInitializationPeriod, c.delayOfInitialReadinessStatus)
	removeMetricsForPods(metrics, ignoredPods)
	removeMetricsForPods(metrics, unreadyPods)
	if len(metrics) == 0 {
		return 0, 0, 0, time.Time{}, fmt.Errorf("did not receive metrics for targeted pods (pods might be unready)")
	}

	requests, err := calculateRequests(podList, container, resource)
	if err != nil {
		return 0, 0, 0, time.Time{}, err
	}

	usageRatio, utilization, rawUtilization, err := metricsclient.GetResourceUtilizationRatio(metrics, requests, targetUtilization)
	if err != nil {
		return 0, 0, 0, time.Time{}, err
	}

	scaleUpWithUnready := len(unreadyPods) > 0 && usageRatio > 1.0
	if !scaleUpWithUnready && len(missingPods) == 0 {
		if tolerances.isWithin(usageRatio) {
			// 变化幅度太小则不调整，避免频繁扩缩造成抖动。
			return currentReplicas, utilization, rawUtilization, timestamp, nil
		}

		// 没有未就绪或缺失指标的 Pod 时，可直接按当前指标计算目标副本数。
		return int32(math.Ceil(usageRatio * float64(readyPodCount))), utilization, rawUtilization, timestamp, nil
	}

	if len(missingPods) > 0 {
		if usageRatio < 1.0 {
			// 缩容时将缺失指标的 Pod 按 100%（或更高的目标利用率）估算，
			// 以免低估用量而过度缩容。
			fallbackUtilization := int64(max(100, targetUtilization))
			for podName := range missingPods {
				metrics[podName] = metricsclient.PodMetric{Value: requests[podName] * fallbackUtilization / 100}
			}
		} else if usageRatio > 1.0 {
			// 扩容时将缺失指标的 Pod 按 0% 估算，确保计算出的扩容趋势不受影响。
			for podName := range missingPods {
				metrics[podName] = metricsclient.PodMetric{Value: 0}
			}
		}
	}

	if scaleUpWithUnready {
		// 扩容时将未就绪 Pod 按 0% 估算，因为它们尚未承担实际负载。
		for podName := range unreadyPods {
			metrics[podName] = metricsclient.PodMetric{Value: 0}
		}
	}

	// 用修正后的指标重新计算使用率，得到更贴近真实负载的比例。
	newUsageRatio, _, _, err := metricsclient.GetResourceUtilizationRatio(metrics, requests, targetUtilization)
	if err != nil {
		return 0, utilization, rawUtilization, time.Time{}, err
	}

	if tolerances.isWithin(newUsageRatio) || (usageRatio < 1.0 && newUsageRatio > 1.0) || (usageRatio > 1.0 && newUsageRatio < 1.0) {
		// 变化太小或修正后扩缩方向发生反转时保持现状，避免做出与目标相反的调整。
		return currentReplicas, utilization, rawUtilization, timestamp, nil
	}

	newReplicas := int32(math.Ceil(newUsageRatio * float64(len(metrics))))
	if (newUsageRatio < 1.0 && newReplicas > currentReplicas) || (newUsageRatio > 1.0 && newReplicas < currentReplicas) {
		// 指标数量变化导致扩缩方向反转时保持现状，避免被不完整的数据误导。
		return currentReplicas, utilization, rawUtilization, timestamp, nil
	}

	// 返回结果，其中参与计算的副本数以实际纳入指标统计的数量为准。
	return newReplicas, utilization, rawUtilization, timestamp, nil
}

// GetRawResourceReplicas 根据指定命名空间中匹配给定选择器的 Pod 的目标资源用量
// （以原始毫值为单位）以及当前副本数，计算期望的副本数量。
func (c *ReplicaCalculator) GetRawResourceReplicas(ctx context.Context, currentReplicas int32, targetUsage int64, resource v1.ResourceName, tolerances Tolerances, namespace string, selector labels.Selector, container string) (replicaCount int32, usage int64, timestamp time.Time, err error) {
	metrics, timestamp, err := c.metricsClient.GetResourceMetric(ctx, resource, namespace, selector, container)
	if err != nil {
		return 0, 0, time.Time{}, fmt.Errorf("unable to get metrics for resource %s: %w", resource, err)
	}

	replicaCount, usage, err = c.calcPlainMetricReplicas(metrics, currentReplicas, targetUsage, tolerances, namespace, selector, resource)
	return replicaCount, usage, timestamp, err
}

// GetMetricReplicas 根据指定命名空间中匹配给定选择器的 Pod 的目标指标用量
// （以毫值为单位）以及当前副本数，计算期望的副本数量。
func (c *ReplicaCalculator) GetMetricReplicas(currentReplicas int32, targetUsage int64, metricName string, tolerances Tolerances, namespace string, selector labels.Selector, metricSelector labels.Selector) (replicaCount int32, usage int64, timestamp time.Time, err error) {
	metrics, timestamp, err := c.metricsClient.GetRawMetric(metricName, namespace, selector, metricSelector)
	if err != nil {
		return 0, 0, time.Time{}, fmt.Errorf("unable to get metric %s: %w", metricName, err)
	}

	replicaCount, usage, err = c.calcPlainMetricReplicas(metrics, currentReplicas, targetUsage, tolerances, namespace, selector, "")
	return replicaCount, usage, timestamp, err
}

// calcPlainMetricReplicas 为普通指标（即非使用率百分比的指标）计算期望副本数。
func (c *ReplicaCalculator) calcPlainMetricReplicas(metrics metricsclient.PodMetricsInfo, currentReplicas int32, targetUsage int64, tolerances Tolerances, namespace string, selector labels.Selector, resource v1.ResourceName) (replicaCount int32, usage int64, err error) {

	podList, err := c.podLister.Pods(namespace).List(selector)
	if err != nil {
		return 0, 0, fmt.Errorf("unable to get pods while calculating replica count: %w", err)
	}

	if len(podList) == 0 {
		return 0, 0, fmt.Errorf("no pods returned by selector while calculating replica count")
	}

	readyPodCount, unreadyPods, missingPods, ignoredPods := groupPods(podList, metrics, resource, c.cpuInitializationPeriod, c.delayOfInitialReadinessStatus)
	removeMetricsForPods(metrics, ignoredPods)
	removeMetricsForPods(metrics, unreadyPods)

	if len(metrics) == 0 {
		return 0, 0, fmt.Errorf("did not receive metrics for targeted pods (pods might be unready)")
	}

	usageRatio, usage := metricsclient.GetMetricUsageRatio(metrics, targetUsage)

	scaleUpWithUnready := len(unreadyPods) > 0 && usageRatio > 1.0

	if !scaleUpWithUnready && len(missingPods) == 0 {
		if tolerances.isWithin(usageRatio) {
			// 变化幅度太小则不调整，避免频繁扩缩造成抖动。
			return currentReplicas, usage, nil
		}

		// 没有未就绪或缺失指标的 Pod 时，可直接按当前指标计算目标副本数。
		return int32(math.Ceil(usageRatio * float64(readyPodCount))), usage, nil
	}

	if len(missingPods) > 0 {
		if usageRatio < 1.0 {
			// 缩容时将缺失指标的 Pod 按目标值估算，以免低估用量而过度缩容。
			for podName := range missingPods {
				metrics[podName] = metricsclient.PodMetric{Value: targetUsage}
			}
		} else if usageRatio > 1.0 {
			// 扩容时将缺失指标的 Pod 按 0% 估算，确保扩容趋势不受影响。
			for podName := range missingPods {
				metrics[podName] = metricsclient.PodMetric{Value: 0}
			}
		}
	}

	if scaleUpWithUnready {
		// 扩容时将未就绪 Pod 按 0% 估算，因为它们尚未承担实际负载。
		for podName := range unreadyPods {
			metrics[podName] = metricsclient.PodMetric{Value: 0}
		}
	}

	// 用修正后的指标重新计算使用率，得到更贴近真实负载的比例。
	newUsageRatio, _ := metricsclient.GetMetricUsageRatio(metrics, targetUsage)

	if tolerances.isWithin(newUsageRatio) || (usageRatio < 1.0 && newUsageRatio > 1.0) || (usageRatio > 1.0 && newUsageRatio < 1.0) {
		// 变化太小或修正后扩缩方向发生反转时保持现状，避免做出与目标相反的调整。
		return currentReplicas, usage, nil
	}

	newReplicas := int32(math.Ceil(newUsageRatio * float64(len(metrics))))
	if (newUsageRatio < 1.0 && newReplicas > currentReplicas) || (newUsageRatio > 1.0 && newReplicas < currentReplicas) {
		// 指标数量变化导致扩缩方向反转时保持现状，避免被不完整的数据误导。
		return currentReplicas, usage, nil
	}

	// 返回结果，其中参与计算的副本数以实际纳入指标统计的数量为准。
	return newReplicas, usage, nil
}

// GetObjectMetricReplicas 根据指定命名空间中给定对象的目标指标用量（以毫值为单位）
// 以及当前副本数，计算期望的副本数量。
func (c *ReplicaCalculator) GetObjectMetricReplicas(currentReplicas int32, targetUsage int64, metricName string, tolerances Tolerances, namespace string, objectRef *autoscaling.CrossVersionObjectReference, selector labels.Selector, metricSelector labels.Selector) (replicaCount int32, usage int64, timestamp time.Time, err error) {
	usage, _, err = c.metricsClient.GetObjectMetric(metricName, namespace, objectRef, metricSelector)
	if err != nil {
		return 0, 0, time.Time{}, fmt.Errorf("unable to get metric %s on %s %s/%s: %w", metricName, objectRef.Kind, namespace, objectRef.Name, err)
	}

	usageRatio := float64(usage) / float64(targetUsage)
	replicaCount, timestamp, err = c.getUsageRatioReplicaCount(currentReplicas, usageRatio, tolerances, namespace, selector)
	return replicaCount, usage, timestamp, err
}

// getUsageRatioReplicaCount 根据使用率和使用率对应的就绪 Pod 数量计算期望副本数。
// 当 currentReplicas=0 时不考虑就绪 Pod 数量和容差，以支持缩容到零个 Pod。
func (c *ReplicaCalculator) getUsageRatioReplicaCount(currentReplicas int32, usageRatio float64, tolerances Tolerances, namespace string, selector labels.Selector) (replicaCount int32, timestamp time.Time, err error) {
	if currentReplicas != 0 {
		if tolerances.isWithin(usageRatio) {
			// 变化幅度太小则不调整，避免频繁扩缩造成抖动。
			return currentReplicas, timestamp, nil
		}
		readyPodCount := int64(0)
		readyPodCount, err = c.getReadyPodsCount(namespace, selector)
		if err != nil {
			return 0, time.Time{}, fmt.Errorf("unable to calculate ready pods: %s", err)
		}
		// 先用 float64 计算，避免转换过程中过早丢失精度。
		replicaCountFloat := usageRatio * float64(readyPodCount)
		// 结果可能超出 int32 范围，需提前做上限保护。
		if replicaCountFloat > math.MaxInt32 {
			replicaCount = math.MaxInt32
		} else {
			replicaCount = int32(math.Ceil(replicaCountFloat))
		}
	} else {
		// 当前无副本时直接按使用率决定扩到 n 个还是保持零个。
		replicaCount = int32(math.Ceil(usageRatio))
	}

	return replicaCount, timestamp, err
}

// GetObjectPerPodMetricReplicas 根据指定命名空间中给定对象的每 Pod 目标指标用量
// （以毫值为单位）以及当前副本数，计算期望的副本数量。
func (c *ReplicaCalculator) GetObjectPerPodMetricReplicas(statusReplicas int32, targetAverageUsage int64, metricName string, tolerances Tolerances, namespace string, objectRef *autoscaling.CrossVersionObjectReference, metricSelector labels.Selector) (replicaCount int32, usage int64, timestamp time.Time, err error) {
	usage, timestamp, err = c.metricsClient.GetObjectMetric(metricName, namespace, objectRef, metricSelector)
	if err != nil {
		return 0, 0, time.Time{}, fmt.Errorf("unable to get metric %s on %s %s/%s: %w", metricName, objectRef.Kind, namespace, objectRef.Name, err)
	}

	replicaCount = statusReplicas
	usageRatio := float64(usage) / (float64(targetAverageUsage) * float64(replicaCount))
	if !tolerances.isWithin(usageRatio) {
		// 变化足够大时才更新副本数，避免小幅波动引起抖动。
		replicaCount = int32(math.Ceil(float64(usage) / float64(targetAverageUsage)))
	}
	usage = int64(math.Ceil(float64(usage) / float64(statusReplicas)))
	return replicaCount, usage, timestamp, nil
}

// getReadyPodsCount 统计指定命名空间中匹配选择器的运行中且已就绪的 Pod 数量。
// @TODO(mattjmcnaughton) 本模块中多个函数都使用了此函数的变体。
// 应将此函数通用化，以免在多个地方重复相同的逻辑。
func (c *ReplicaCalculator) getReadyPodsCount(namespace string, selector labels.Selector) (int64, error) {
	podList, err := c.podLister.Pods(namespace).List(selector)
	if err != nil {
		return 0, fmt.Errorf("unable to get pods while calculating replica count: %w", err)
	}

	if len(podList) == 0 {
		return 0, fmt.Errorf("no pods returned by selector while calculating replica count")
	}

	readyPodCount := 0

	for _, pod := range podList {
		if pod.Status.Phase == v1.PodRunning && podutil.IsPodReady(pod) {
			readyPodCount++
		}
	}

	return int64(readyPodCount), nil
}

// GetExternalMetricReplicas 根据指定命名空间中外部指标的目标值（以毫值为单位）
// 以及当前副本数，计算期望的副本数量。
func (c *ReplicaCalculator) GetExternalMetricReplicas(currentReplicas int32, targetUsage int64, metricName string, tolerances Tolerances, namespace string, metricSelector *metav1.LabelSelector, podSelector labels.Selector) (replicaCount int32, usage int64, timestamp time.Time, err error) {
	metricLabelSelector, err := metav1.LabelSelectorAsSelector(metricSelector)
	if err != nil {
		return 0, 0, time.Time{}, err
	}
	metrics, _, err := c.metricsClient.GetExternalMetric(metricName, namespace, metricLabelSelector)
	if err != nil {
		return 0, 0, time.Time{}, fmt.Errorf("unable to get external metric %s/%s/%+v: %s", namespace, metricName, metricSelector, err)
	}

	usage = 0
	for _, val := range metrics {
		// 正数相加可能溢出 int64，累加前先检查上限。
		if val > 0 && usage > math.MaxInt64-val {
			usage = math.MaxInt64
			break
		}
		usage = usage + val
	}

	usageRatio := float64(usage) / float64(targetUsage)
	replicaCount, timestamp, err = c.getUsageRatioReplicaCount(currentReplicas, usageRatio, tolerances, namespace, podSelector)
	return replicaCount, usage, timestamp, err
}

// GetExternalPerPodMetricReplicas 根据指定命名空间中外部指标的每 Pod 目标值
// （以毫值为单位）以及当前副本数，计算期望的副本数量。
func (c *ReplicaCalculator) GetExternalPerPodMetricReplicas(statusReplicas int32, targetUsagePerPod int64, metricName string, tolerances Tolerances, namespace string, metricSelector *metav1.LabelSelector) (replicaCount int32, usage int64, timestamp time.Time, err error) {
	metricLabelSelector, err := metav1.LabelSelectorAsSelector(metricSelector)
	if err != nil {
		return 0, 0, time.Time{}, err
	}
	metrics, timestamp, err := c.metricsClient.GetExternalMetric(metricName, namespace, metricLabelSelector)
	if err != nil {
		return 0, 0, time.Time{}, fmt.Errorf("unable to get external metric %s/%s/%+v: %s", namespace, metricName, metricSelector, err)
	}
	usage = 0
	for _, val := range metrics {
		usage = usage + val
		if usage < 0 {
			// 累加出现负数只可能是发生了 int64 溢出。
			usage = math.MaxInt64
			break
		}
	}

	replicaCount = statusReplicas
	usageRatio := float64(usage) / (float64(targetUsagePerPod) * float64(replicaCount))
	if !tolerances.isWithin(usageRatio) {
		// 变化足够大时才更新副本数，避免小幅波动引起抖动。
		replicaCountResult := math.Ceil(float64(usage) / float64(targetUsagePerPod))
		// 计算结果可能超出 int32 范围，需做上限保护。
		if replicaCountResult > float64(math.MaxInt32) {
			replicaCount = math.MaxInt32
		} else {
			replicaCount = int32(replicaCountResult)
		}
	}
	// 用量溢出时直接返回上限值，避免转换过程中丢失精度。
	if float64(usage) >= float64(math.MaxInt64) {
		usage = math.MaxInt64
	} else {
		usage = int64(math.Ceil(float64(usage) / float64(statusReplicas)))
	}
	return replicaCount, usage, timestamp, nil
}

// groupPods 按指标可用性将 Pod 分组，返回就绪 Pod 数量以及未就绪、缺失指标和被忽略的 Pod 集合。
func groupPods(pods []*v1.Pod, metrics metricsclient.PodMetricsInfo, resource v1.ResourceName, cpuInitializationPeriod, delayOfInitialReadinessStatus time.Duration) (readyPodCount int, unreadyPods, missingPods, ignoredPods sets.Set[string]) {
	missingPods = sets.New[string]()
	unreadyPods = sets.New[string]()
	ignoredPods = sets.New[string]()
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil || pod.Status.Phase == v1.PodFailed {
			ignoredPods.Insert(pod.Name)
			continue
		}
		// 处于 Pending 的 Pod 尚未运行，不参与指标统计。
		if pod.Status.Phase == v1.PodPending {
			unreadyPods.Insert(pod.Name)
			continue
		}
		// 没有指标数据的 Pod 单独归类，后续按扩缩方向做保守估算。
		metric, found := metrics[pod.Name]
		if !found {
			missingPods.Insert(pod.Name)
			continue
		}
		if resource == v1.ResourceCPU {
			var unready bool
			_, condition := podutil.GetPodCondition(&pod.Status, v1.PodReady)
			if condition == nil || pod.Status.StartTime == nil {
				unready = true
			} else {
				// Pod 可能仍处于启动初始化期，此时指标波动不代表真实负载。
				if pod.Status.StartTime.Add(cpuInitializationPeriod).After(time.Now()) {
					// 未就绪或自上次状态转换以来缺少完整窗口数据时忽略该样本。
					unready = condition.Status == v1.ConditionFalse || metric.Timestamp.Before(condition.LastTransitionTime.Time.Add(metric.Window))
				} else {
					// 初始化期过后，仅当 Pod 未就绪且从未就绪过时忽略其指标。
					unready = condition.Status == v1.ConditionFalse && pod.Status.StartTime.Add(delayOfInitialReadinessStatus).After(condition.LastTransitionTime.Time)
				}
			}
			if unready {
				unreadyPods.Insert(pod.Name)
				continue
			}
		}
		readyPodCount++
	}
	return
}

// calculateRequests 计算每个 Pod 指定资源的 request 值。
// 如果 container 非空，则使用该指定容器的 request。
// 如果 container 为空且 Pod 设置了 Pod 级 request，则使用 Pod 级 request。
// 否则累加 Pod 中所有容器的 request（包括可重启的 init 容器）。
// 返回 Pod 名称到其 request 值的映射。
func calculateRequests(pods []*v1.Pod, container string, resource v1.ResourceName) (map[string]int64, error) {
	podLevelResourcesEnabled := feature.DefaultFeatureGate.Enabled(features.PodLevelResources)
	requests := make(map[string]int64, len(pods))
	for _, pod := range pods {
		var request int64
		var err error
		// 是否使用 Pod 级 request 参见 KEP-2837：
		// https://github.com/kubernetes/enhancements/blob/master/keps/sig-node/2837-pod-level-resource-spec/README.md
		usePodLevelRequests := podLevelResourcesEnabled &&
			resourcehelpers.IsPodLevelRequestsSet(pod) &&
			// HPA 中指定了容器名时优先使用该容器，而非 Pod 级 request。
			container == ""

		if usePodLevelRequests {
			request, err = calculatePodLevelRequests(pod, resource)
		} else {
			request, err = calculatePodRequestsFromContainers(pod, container, resource)
		}
		if err != nil {
			return nil, err
		}
		requests[pod.Name] = request
	}
	return requests, nil
}

// calculatePodLevelRequests 计算 Pod 级别指定资源的 request 值。
func calculatePodLevelRequests(pod *v1.Pod, resource v1.ResourceName) (int64, error) {
	podLevelRequests := resourcehelpers.PodRequests(pod, resourcehelpers.PodResourcesOptions{})
	podRequest, ok := podLevelRequests[resource]
	if !ok {
		return 0, fmt.Errorf("missing pod-level request for %s in Pod %s", resource, pod.Name)
	}
	return podRequest.MilliValue(), nil
}

// calculatePodRequestsFromContainers 通过累加 Pod 中所有容器的 request 计算指定资源的 request。
// 如果指定了容器名，则只使用该容器的 request。
func calculatePodRequestsFromContainers(pod *v1.Pod, container string, resource v1.ResourceName) (int64, error) {
	containers := append([]v1.Container{}, pod.Spec.Containers...)
	for _, c := range pod.Spec.InitContainers {
		// 可重启的 init 容器会持续运行，其 request 应计入长期资源占用。
		if c.RestartPolicy != nil && *c.RestartPolicy == v1.ContainerRestartPolicyAlways {
			containers = append(containers, c)
		}
	}

	request := int64(0)
	for _, c := range containers {
		if container == "" || container == c.Name {
			containerRequest, ok := c.Resources.Requests[resource]
			if !ok {
				return 0, fmt.Errorf("missing request for %s in container %s of Pod %s", resource, c.Name, pod.Name)
			}
			request += containerRequest.MilliValue()
		}
		// Pod 内容器名唯一，找到目标容器即可提前返回。
		if container == c.Name {
			return request, nil
		}
	}

	// 指定了容器名却遍历完仍未找到，说明该容器不存在。
	if container != "" {
		return 0, fmt.Errorf("container %s not found in Pod %s", container, pod.Name)
	}

	return request, nil
}

// removeMetricsForPods 从指标映射中删除给定 Pod 的指标，避免脏数据参与后续计算。
func removeMetricsForPods(metrics metricsclient.PodMetricsInfo, pods sets.Set[string]) {
	for _, pod := range pods.UnsortedList() {
		delete(metrics, pod)
	}
}
