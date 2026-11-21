/*
Copyright 2017 The Kubernetes Authors.

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

package preemption

import (
	"context"
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/api/v1/resource"
	v1qos "k8s.io/kubernetes/pkg/apis/core/v1/helper/qos"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/kubelet/eviction"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
)

// message 是驱逐关键 Pod 时写入事件和 Pod 状态的固定说明。
const message = "Preempted in order to admit critical pod"

// CriticalPodAdmissionHandler 是一个处理关键 Pod 准入失败的 AdmissionFailureHandler。
// 如果唯一的准入失败原因是资源不足，它会驱逐一些 Pod，腾出足够的资源请求量，
// 使关键 Pod 得以准入。驱逐集合的选择以影响最小为原则，优先级如下：
// Guaranteed Pod 的最小影响 > Burstable Pod 的最小影响 > BestEffort Pod 的最小影响。
// "最小影响" 的定义是：驱逐的 Pod 数量最少优先，其次是 Pod 总请求量最少。
// 在 Pod 数量最少的前提下寻找总请求量最少的方案被视为 BestEffort。
type CriticalPodAdmissionHandler struct {
	getPodsFunc eviction.ActivePodsFunc
	killPodFunc eviction.KillPodFunc
	recorder    record.EventRecorder
}

// 编译期断言：确保 CriticalPodAdmissionHandler 实现了 AdmissionFailureHandler 接口。
var _ lifecycle.AdmissionFailureHandler = &CriticalPodAdmissionHandler{}

// NewCriticalPodAdmissionHandler 创建关键 Pod 准入失败处理器。
func NewCriticalPodAdmissionHandler(getPodsFunc eviction.ActivePodsFunc, killPodFunc eviction.KillPodFunc, recorder record.EventRecorder) *CriticalPodAdmissionHandler {
	return &CriticalPodAdmissionHandler{
		getPodsFunc: getPodsFunc,
		killPodFunc: killPodFunc,
		recorder:    recorder,
	}
}

// HandleAdmissionFailure 以更宽容的方式处理准入拒绝：在满足条件时驱逐其他 Pod，
// 使先前准入失败的 Pod 也能被接纳。
func (c *CriticalPodAdmissionHandler) HandleAdmissionFailure(ctx context.Context, admitPod *v1.Pod, failureReasons []lifecycle.PredicateFailureReason, operation lifecycle.Operation) ([]lifecycle.PredicateFailureReason, error) {
	// 只有关键 Pod 才触发抢占，普通 Pod 保留原有准入失败结果。
	if !kubetypes.IsCriticalPod(admitPod) {
		return failureReasons, nil
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingSchedulerPreemption) {
		if operation == lifecycle.ResizeOperation {
			// 原地调整 Pod 资源时，抢占决策完全由调度器负责。
			// 节点暂时无法容纳调整后的资源时，kubelet 推迟该请求，
			// 由调度器异步完成抢占后再重试。
			return failureReasons, nil
		}
	}
	// 资源不足不应成为拒绝关键 Pod 的理由。如果不存在其他拒绝原因，
	// 这里改为腾出资源来接纳它。
	nonResourceReasons := []lifecycle.PredicateFailureReason{}
	resourceReasons := []*admissionRequirement{}
	for _, reason := range failureReasons {
		if r, ok := reason.(*lifecycle.InsufficientResourceError); ok {
			resourceReasons = append(resourceReasons, &admissionRequirement{
				resourceName: r.ResourceName,
				quantity:     r.GetInsufficientAmount(),
			})
		} else {
			nonResourceReasons = append(nonResourceReasons, reason)
		}
	}
	if len(nonResourceReasons) > 0 {
		// 只返回非资源性原因：关键 Pod 不应因资源不足被拒绝。
		return nonResourceReasons, nil
	}
	err := c.evictPodsToFreeRequests(ctx, admitPod, admissionRequirementList(resourceReasons))
	// 返回 nil 错误说明抢占成功，Pod 可以被安全准入。
	return nil, err
}

// evictPodsToFreeRequests 接收一组资源不足量，并按请求量驱逐 Pod 来释放资源。
// 例如唯一不足的资源是 200MB 内存时，可能驱逐一个 request=250MB 的 Pod。
func (c *CriticalPodAdmissionHandler) evictPodsToFreeRequests(ctx context.Context, admitPod *v1.Pod, insufficientResources admissionRequirementList) error {
	logger := klog.FromContext(ctx)
	podsToPreempt, err := getPodsToPreempt(admitPod, c.getPodsFunc(), insufficientResources)
	if err != nil {
		return fmt.Errorf("preemption: error finding a set of pods to preempt: %v", err)
	}
	for _, pod := range podsToPreempt {
		// 先记录驱逐事件，让用户知道 Pod 消失的原因。
		c.recorder.Eventf(pod, v1.EventTypeWarning, events.PreemptContainer, message)
		// 这是阻塞调用，只有 Pod 及其容器都被杀死后才会返回。
		logger.V(2).Info("Preempting pod to free up resources", "pod", klog.KObj(pod), "podUID", pod.UID, "insufficientResources", insufficientResources.toString(), "requestingPod", klog.KObj(admitPod))
		// 在状态回调中把 Pod 标记为失败，并写入 DisruptionTarget 条件。
		err := c.killPodFunc(pod, true, nil, func(status *v1.PodStatus) {
			status.Phase = v1.PodFailed
			status.Reason = events.PreemptContainer
			status.Message = message
			podutil.UpdatePodCondition(status, &v1.PodCondition{
				Type:               v1.DisruptionTarget,
				ObservedGeneration: podutil.CalculatePodConditionObservedGeneration(status, pod.Generation, v1.DisruptionTarget),
				Status:             v1.ConditionTrue,
				Reason:             v1.PodReasonTerminationByKubelet,
				Message:            "Pod was preempted by Kubelet to accommodate a critical pod.",
			})
		})
		if err != nil {
			logger.Error(err, "Failed to evict pod", "pod", klog.KObj(pod))
			// 在后续 syncPod 循环中，kubelet 会重试此前卡住的删除步骤。
			// 这里继续处理下一个 Pod，避免一个异常 Pod 阻塞整个抢占流程。
			continue
		}
		// 指标按首个不足资源打标签；没有资源项时使用空标签。
		if len(insufficientResources) > 0 {
			metrics.Preemptions.WithLabelValues(insufficientResources[0].resourceName.String()).Inc()
		} else {
			metrics.Preemptions.WithLabelValues("").Inc()
		}
		logger.Info("Pod evicted successfully", "pod", klog.KObj(pod))
	}
	return nil
}

// getPodsToPreempt 返回一组可被驱逐的 Pod，其请求量之和 >= requirements。
func getPodsToPreempt(pod *v1.Pod, pods []*v1.Pod, requirements admissionRequirementList) ([]*v1.Pod, error) {
	// 按 QoS 分组，驱逐影响从小到大依次为 BestEffort、Burstable、Guaranteed。
	bestEffortPods, burstablePods, guaranteedPods := sortPodsByQOS(pod, pods)

	// 先确认现有 Pod 的请求量足以覆盖全部需求，否则直接失败。
	unableToMeetRequirements := requirements.subtract(append(append(bestEffortPods, burstablePods...), guaranteedPods...)...)
	if len(unableToMeetRequirements) > 0 {
		return nil, fmt.Errorf("no set of running pods found to reclaim resources: %v", unableToMeetRequirements.toString())
	}
	// 假设已驱逐全部 Burstable 和 BestEffort Pod，计算仍需驱逐的 Guaranteed Pod。
	guaranteedToEvict, err := getPodsToPreemptByDistance(guaranteedPods, requirements.subtract(append(bestEffortPods, burstablePods...)...))
	if err != nil {
		return nil, err
	}
	// 假设已驱逐全部 BestEffort Pod 以及上面选出的 Guaranteed Pod，计算仍需驱逐的 Burstable Pod。
	burstableToEvict, err := getPodsToPreemptByDistance(burstablePods, requirements.subtract(append(bestEffortPods, guaranteedToEvict...)...))
	if err != nil {
		return nil, err
	}
	// 假设已驱逐所需的 Guaranteed 和 Burstable Pod，计算仍需驱逐的 BestEffort Pod。
	bestEffortToEvict, err := getPodsToPreemptByDistance(bestEffortPods, requirements.subtract(append(burstableToEvict, guaranteedToEvict...)...))
	if err != nil {
		return nil, err
	}
	// 返回顺序即驱逐顺序：BestEffort 最先，Guaranteed 最后。
	return append(append(bestEffortToEvict, burstableToEvict...), guaranteedToEvict...), nil
}

// getPodsToPreemptByDistance 找出请求量能覆盖准入需求的 Pod。
// 选择 "距离" 需求最小的 Pod；若多个 Pod 都能满足剩余需求，
// 则选择资源请求更小的那个。通过反复挑选可满足最多需求的 Pod，
// 该方法尽量最小化返回的 Pod 数量。
func getPodsToPreemptByDistance(pods []*v1.Pod, requirements admissionRequirementList) ([]*v1.Pod, error) {
	podsToEvict := []*v1.Pod{}
	// 每轮选择距离剩余需求最近的 Pod，并同步更新剩余需求。
	for len(requirements) > 0 {
		if len(pods) == 0 {
			return nil, fmt.Errorf("no set of running pods found to reclaim resources: %v", requirements.toString())
		}
		// 所有距离都必然小于 len(requirements)，因为单个需求的最大距离为 1。
		bestDistance := float64(len(requirements) + 1)
		bestPodIndex := 0
		// 找到距离需求最近的 Pod；若两个 Pod 距离相同，则选择资源请求更小的那个。
		for i, pod := range pods {
			dist := requirements.distance(pod)
			if dist < bestDistance || (bestDistance == dist && smallerResourceRequest(pod, pods[bestPodIndex])) {
				bestDistance = dist
				bestPodIndex = i
			}
		}
		// 从需求中扣除该 Pod，并把该 Pod 从输入列表转移到待驱逐列表。
		requirements = requirements.subtract(pods[bestPodIndex])
		podsToEvict = append(podsToEvict, pods[bestPodIndex])
		pods[bestPodIndex] = pods[len(pods)-1]
		pods = pods[:len(pods)-1]
	}
	return podsToEvict, nil
}

// admissionRequirement 表示某项资源需要被释放的缺口。
type admissionRequirement struct {
	// resourceName 是存在缺口的资源名。
	resourceName v1.ResourceName
	// quantity 是需要释放的资源量，单位与资源的 base unit 一致。
	quantity int64
}

// admissionRequirementList 是一组资源缺口，实现了距离计算和扣减操作。
type admissionRequirementList []*admissionRequirement

// distance 返回 Pod 请求量相对准入需求的 "距离"。
// 距离按 Pod 满足需求的比例度量，因此每项需求权重相同，与绝对量级无关。
func (a admissionRequirementList) distance(pod *v1.Pod) float64 {
	dist := float64(0)
	for _, req := range a {
		remainingRequest := float64(req.quantity - resource.GetResourceRequest(pod, req.resourceName))
		if remainingRequest > 0 {
			dist += math.Pow(remainingRequest/float64(req.quantity), 2)
		}
	}
	return dist
}

// subtract 返回假设给定 Pod 都已被驱逐后剩余的资源需求。
func (a admissionRequirementList) subtract(pods ...*v1.Pod) admissionRequirementList {
	newList := []*admissionRequirement{}
	for _, req := range a {
		newQuantity := req.quantity
		for _, pod := range pods {
			newQuantity -= resource.GetResourceRequest(pod, req.resourceName)
			if newQuantity <= 0 {
				break
			}
		}
		if newQuantity > 0 {
			newList = append(newList, &admissionRequirement{
				resourceName: req.resourceName,
				quantity:     newQuantity,
			})
		}
	}
	return newList
}

// toString 以可读形式输出资源需求列表，用于日志和错误信息。
func (a admissionRequirementList) toString() string {
	s := "["
	for _, req := range a {
		s += fmt.Sprintf("(res: %v, q: %d), ", req.resourceName, req.quantity)
	}
	return s + "]"
}

// sortPodsByQOS 返回可被 preemptor Pod 抢占的 BestEffort、Burstable 和 Guaranteed Pod 列表。
func sortPodsByQOS(preemptor *v1.Pod, pods []*v1.Pod) (bestEffort, burstable, guaranteed []*v1.Pod) {
	for _, pod := range pods {
		// Preemptable 会过滤掉不可被抢占的 Pod（例如静态 Pod 或受 PDB 保护）。
		if kubetypes.Preemptable(preemptor, pod) {
			switch v1qos.GetPodQOS(pod) {
			case v1.PodQOSBestEffort:
				bestEffort = append(bestEffort, pod)
			case v1.PodQOSBurstable:
				burstable = append(burstable, pod)
			case v1.PodQOSGuaranteed:
				guaranteed = append(guaranteed, pod)
			default:
			}
		}
	}

	return
}

// smallerResourceRequest 判断 pod1 的资源请求是否小于 pod2。
func smallerResourceRequest(pod1 *v1.Pod, pod2 *v1.Pod) bool {
	// 先比较内存再比较 CPU，保证排序结果可预测。
	priorityList := []v1.ResourceName{
		v1.ResourceMemory,
		v1.ResourceCPU,
	}
	for _, res := range priorityList {
		req1 := resource.GetResourceRequest(pod1, res)
		req2 := resource.GetResourceRequest(pod2, res)
		if req1 < req2 {
			return true
		} else if req1 > req2 {
			return false
		}
	}
	return true
}
