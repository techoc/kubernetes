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

package devicetainteviction

import (
	"context"
	"fmt"
	"iter"
	"maps"
	"math"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/diff"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	resourceac "k8s.io/client-go/applyconfigurations/resource/v1"
	coreinformers "k8s.io/client-go/informers/core/v1"
	resourceinformers "k8s.io/client-go/informers/resource/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	resourcelisters "k8s.io/client-go/listers/resource/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/klog/v2"
	apipod "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/controller/devicetainteviction/metrics"
	"k8s.io/kubernetes/pkg/controller/tainteviction"
	"k8s.io/kubernetes/pkg/features"
	utilpod "k8s.io/kubernetes/pkg/util/pod"
)

// 控制器内部常量：状态更新周期、UID 缓存容量与 informer 索引名。
const (
	// ruleStatusPeriod 是驱逐进行期间两次 DeviceTaintRule 状态更新之间的最短间隔。
	// 驱逐结束后不再继续更新状态，直到再次进入驱逐进行中。
	ruleStatusPeriod = 10 * time.Second

	// maxUIDCacheEntries 限制已驱逐 Pod 的 UID 缓存条目数，防止缓存随集群生命周期无限增长。
	maxUIDCacheEntries = 500

	// podGroupIndex 是 Pod informer 中按 PodGroup 对 Pod 建立索引时使用的索引名。
	podGroupIndex = "podGroupPerPod"
)

// Controller 监听 DRA 设备的 Taint 变化以及 ResourceClaim 的 Toleration 变化，
// 并删除那些使用了不容忍 NoExecute taint 的 ResourceClaim 的 Pod。
// 已经处于终态（即已终止）的 Pod 无需删除。
//
// 所有用于识别需要驱逐的 Pod 的逻辑都运行在 handle* 事件处理函数中，
// 它们不会调用任何阻塞方法。所有阻塞调用都发生在
// [tainteviction.TimedWorkerQueue] 中，使用传给 Run 的 context。
//
// [resourceslicetracker] 负责把 DeviceTaintRule 中定义的 taint 应用到 ResourceSlice 上。
// 本控制器接收的是经过该 tracker 修改、带有所有适用 taint 的 ResourceSlice，
// 因此无需关心某个 taint 究竟来自 DRA 驱动还是 DeviceTaintRule。
type Controller struct {
	name string

	// logger 是后台活动使用的通用日志器。
	logger klog.Logger

	// eventLogger 专门用于事件处理期间的日志记录。如果不需要这类日志，它可以为 nil。
	eventLogger *klog.Logger

	// client 用于与 API server 通信，执行 Pod 删除与状态更新等写操作。
	client clientset.Interface
	// recorder 用于发送 Pod 驱逐相关的事件。
	recorder record.EventRecorder
	// podInformer 提供 Pod 的 informer，用于注册事件处理函数与索引。
	podInformer coreinformers.PodInformer
	// podLister 提供 Pod 的本地缓存读取，事件处理函数用它查找消费方 Pod。
	podLister corelisters.PodLister
	// claimInformer 提供 ResourceClaim 的 informer。
	claimInformer resourceinformers.ResourceClaimInformer
	// sliceInformer 提供 ResourceSlice 的 informer。
	sliceInformer resourceinformers.ResourceSliceInformer
	// ruleInformer 提供 DeviceTaintRule 的 informer，仅在对应特性启用时使用。
	ruleInformer resourceinformers.DeviceTaintRuleInformer
	// classInformer 提供 DeviceClass 的 informer，这里只为确保其被启动。
	classInformer resourceinformers.DeviceClassInformer
	// ruleLister 提供 DeviceTaintRule 的本地缓存读取。
	ruleLister resourcelisters.DeviceTaintRuleLister
	// haveSynced 收集所有需要等待同步的 informer 与事件处理函数。
	haveSynced []cache.DoneChecker
	// hasSynced 标记控制器是否已经完成初始同步。
	hasSynced atomic.Int32
	// metrics 保存控制器对外暴露的指标。
	metrics metrics.Metrics
	// workqueue 保存需要延迟或重试执行的工作项。
	workqueue workqueue.TypedRateLimitingInterface[workItem]

	// workloadResourceClaimsEnabled 表示是否启用了工作负载级 ResourceClaim 特性（PodGroup）。
	workloadResourceClaimsEnabled bool

	// evictedPods 缓存记录那些我们已知已经被驱逐过的 Pod。
	evictedPods *uidCache

	// evictPodHook 是测试注入的钩子，在设置驱逐时被调用。
	evictPodHook func(pod tainteviction.NamespacedObject, eviction evictionAndReason)
	// cancelEvictHook 是测试注入的钩子，在取消驱逐时被调用，返回 true 表示确实取消成功。
	cancelEvictHook func(pod tainteviction.NamespacedObject) bool

	// mutex 保护下面这些共享数据结构。
	mutex sync.Mutex

	// deletePodAt 把 Pod 映射到其预定被驱逐的时间。
	//
	// 当不再需要驱逐时删除该条目；当时间发生变化时更新该条目。
	deletePodAt map[tainteviction.NamespacedObject]evictionAndReason

	// maybeDeletePodCount 统计 worker 检查某个 Pod 的次数。
	// 这对单元测试有用，但作为公开指标可能并不合适。
	maybeDeletePodCount int64

	// allocatedClaims 保存当前已知的所有已分配 ResourceClaim。
	allocatedClaims map[types.NamespacedName]allocatedClaim // 使用值比使用指针稍高效（在 BenchmarkTaintUntaint 中分配次数更少！）。

	// pools 按驱动名和池名对所有 ResourceSlice 建立索引。
	pools map[poolID]pool

	// evictingRules 按名称记录所有会导致 Pod 被驱逐的 DeviceTaintRule。
	evictingRules map[string]*resourceapi.DeviceTaintRule

	// taintRuleStats 记录针对某个具体 DeviceTaintRule 实例所做工作的统计信息。
	//
	// 这里的规则集合可能与 evictingRules 不同：一条规则当前可能已不再触发驱逐，
	// 但它过去曾驱逐过 Pod，因此仍保留着有价值的统计信息。
	taintRuleStats map[types.UID]taintRuleStats
}

// taintRuleStats 保存单条 DeviceTaintRule 的运行时统计信息。
type taintRuleStats struct {
	// numEvictedPods 是自控制器启动以来因为该规则而被驱逐的 Pod 数量。
	numEvictedPods int64
}

// poolID 通过驱动名和池名唯一标识一个设备池。
type poolID struct {
	driverName, poolName string
}

// pool 汇总属于同一个设备池的所有 ResourceSlice。
type pool struct {
	// slices 把 ResourceSlice 的全局名称映射到该名称下的当前实例。
	slices map[string]*resourceapi.ResourceSlice
	// maxGeneration 是当前所有切片中最大的生成编号，只有该世代的切片才代表最新设备状态。
	maxGeneration int64
}

// addSlice 向池中添加一个 ResourceSlice。
func (p *pool) addSlice(slice *resourceapi.ResourceSlice) {
	if slice == nil {
		return
	}
	if p.slices == nil {
		p.slices = make(map[string]*resourceapi.ResourceSlice)
		p.maxGeneration = math.MinInt64
	}
	p.slices[slice.Name] = slice

	// 添加切片只会让生成编号变大，不可能变小，因此直接取较大值即可。
	if slice.Spec.Pool.Generation > p.maxGeneration {
		p.maxGeneration = slice.Spec.Pool.Generation
	}
}

// removeSlice 从池中移除一个 ResourceSlice，该切片此前必须已被添加过。
func (p *pool) removeSlice(slice *resourceapi.ResourceSlice) {
	if slice == nil {
		return
	}
	delete(p.slices, slice.Name)

	// 移除的若是生成编号最大的切片，最大值可能下降到其他切片的生成编号，
	// 此时需要重新扫描剩余切片。
	if slice.Spec.Pool.Generation == p.maxGeneration {
		maxGeneration := int64(math.MinInt64)
		for _, slice := range p.slices {
			if slice.Spec.Pool.Generation > maxGeneration {
				maxGeneration = slice.Spec.Pool.Generation
			}
		}
		p.maxGeneration = maxGeneration
	}
}

// getTaintedDevices 收集所有带 NoExecute 效果的设备 taint，并按设备名排序。
func (p pool) getTaintedDevices() []taintedDevice {
	var buffer []taintedDevice
	for _, slice := range p.slices {
		// 只有生成编号最新的切片才反映当前设备状态，旧切片必须忽略。
		if slice.Spec.Pool.Generation != p.maxGeneration {
			continue
		}
		for _, device := range slice.Spec.Devices {
			for _, taint := range device.Taints {
				if taint.Effect != resourceapi.DeviceTaintEffectNoExecute {
					continue
				}
				buffer = append(buffer, taintedDevice{deviceName: device.Name, taint: taint})
			}
		}
	}

	// 这里 slices.SortFunc 比 sort.Slice 更高效。
	slices.SortFunc(buffer, func(a, b taintedDevice) int {
		return strings.Compare(a.deviceName, b.deviceName)
	})
	return buffer
}

// getDevice 按名称查找一个设备，过期的 ResourceSlice 会被忽略。
func (p pool) getDevice(deviceName string) (*resourceapi.ResourceSlice, *resourceapi.Device) {
	for _, slice := range p.slices {
		if slice.Spec.Pool.Generation != p.maxGeneration {
			continue
		}
		for i := range slice.Spec.Devices {
			if slice.Spec.Devices[i].Name == deviceName {
				return slice, &slice.Spec.Devices[i]
			}
		}
	}

	return nil, nil
}

// taintedDevice 把一个设备名与它身上某个 NoExecute taint 关联起来。
type taintedDevice struct {
	// deviceName 是带 taint 的设备名。
	deviceName string
	// taint 是设备上效果为 NoExecute 的那条 taint。
	taint resourceapi.DeviceTaint
}

// allocatedClaim 是带有分配结果的 ResourceClaim。
// 它可能因 taint 而需要驱逐 Pod，也可能不需要。
type allocatedClaim struct {
	*resourceapi.ResourceClaim

	// eviction 非 nil 时表示使用该 ResourceClaim 的 Pod 需要被驱逐的时间。
	// 该值是所有按设备计算出的时间中的最小值。
	// 每个设备上的时间计算方式为：`<设置 taint 的时间> + <容忍秒数，未设置则为 0>`。
	eviction *evictionAndReason
}

// evictionAndReason 把需要开始驱逐的时间与导致驱逐的全部原因组合在一起。
type evictionAndReason struct {
	// when 是开始执行驱逐的时间。
	when metav1.Time
	// reason 收集导致驱逐的全部 taint。
	reason evictionReason
}

// String 以“时间（原因）”的形式返回驱逐信息，便于日志阅读。
func (et evictionAndReason) String() string {
	return fmt.Sprintf("%s (%s)", et.when, et.reason)
}

// equal 判断两组驱逐信息是否相同，两个都为 nil 时才视为相等。
func (et *evictionAndReason) equal(other *evictionAndReason) bool {
	if (et == nil) != (other == nil) ||
		et == nil {
		return false
	}
	return et.when.Equal(&other.when) &&
		slices.Equal(et.reason, other.reason)
}

// evictionReason 收集所有导致驱逐的 taint。
// 它支持友好打印，可直接用于日志和面向用户的描述信息。
type evictionReason []trackedTaint

// String 把所有驱逐原因拼接成逗号分隔的可读字符串。
func (er evictionReason) String() string {
	var parts []string
	for _, taint := range er {
		parts = append(parts, taint.String())
	}
	return strings.Join(parts, ", ")
}

// trackedTaint 在 DeviceTaint 的基础上记录了它的来源。
// rule 和 slice 互斥，二者中总是恰好设置一个。
type trackedTaint struct {
	// rule 非 nil 时表示该 taint 来自 DeviceTaintRule。
	rule *resourceapi.DeviceTaintRule
	// slice 在 rule 为 nil 时指向 ResourceSlice 中具体的 taint。
	slice sliceDeviceTaint
}

// deviceTaint 返回该 taint 对应的 DeviceTaint 对象。
func (tt trackedTaint) deviceTaint() *resourceapi.DeviceTaint {
	if tt.rule != nil {
		// TODO when GA: directly point to rule.Spec.Taint.
		return &resourceapi.DeviceTaint{
			Key:       tt.rule.Spec.Taint.Key,
			Value:     tt.rule.Spec.Taint.Value,
			Effect:    resourceapi.DeviceTaintEffect(tt.rule.Spec.Taint.Effect),
			TimeAdded: tt.rule.Spec.Taint.TimeAdded,
		}
	}
	if tt.slice.slice != nil {
		index := slices.IndexFunc(tt.slice.slice.Spec.Devices, func(d resourceapi.Device) bool { return d.Name == tt.slice.deviceName })
		return &tt.slice.slice.Spec.Devices[index].Taints[tt.slice.taintIndex]
	}

	// 到这里说明来源信息缺失，无法还原 taint。
	return nil
}

// String 返回 taint 来源的可读描述，便于在日志中定位规则或切片。
func (tt trackedTaint) String() string {
	if tt.rule != nil {
		return fmt.Sprintf("DeviceTaintRule %s", newObject(tt.rule))
	}
	return fmt.Sprintf("ResourceSlice %s %s/%s/%s taint #%d", newObject(tt.slice.slice), tt.slice.slice.Spec.Driver, tt.slice.slice.Spec.Pool.Name, tt.slice.deviceName, tt.slice.taintIndex)
}

// Compare 对两个 taint 的来源进行确定性排序：规则排在切片前面，
// 规则之间按名称排序，切片之间按驱动/池/设备名和 taint 下标排序。
func (tt trackedTaint) Compare(b trackedTaint) int {
	// 规则优先于来自切片的 taint。
	if tt.rule != nil && b.rule == nil {
		return -1
	}
	if tt.rule == nil && b.rule != nil {
		return 1
	}
	if tt.rule != nil {
		// 规则之间按名称排序。
		return strings.Compare(tt.rule.Name, b.rule.Name)
	}

	if tt.slice.slice != nil && b.slice.slice != nil {
		// 先按驱动/池/设备排序，再按 taint 下标排序。
		if cmp := strings.Compare(tt.slice.slice.Spec.Driver, b.slice.slice.Spec.Driver); cmp != 0 {
			return cmp
		}
		if cmp := strings.Compare(tt.slice.slice.Spec.Pool.Name, b.slice.slice.Spec.Pool.Name); cmp != 0 {
			return cmp
		}
		if cmp := strings.Compare(tt.slice.deviceName, b.slice.deviceName); cmp != 0 {
			return cmp
		}
		return tt.slice.taintIndex - b.slice.taintIndex
	}

	// 两者都为空（或来源不可比），无法继续区分。
	return 0
}

// sliceDeviceTaint 引用 ResourceSlice 中某台设备上的一条 taint。
type sliceDeviceTaint struct {
	// slice 是 taint 所在的 ResourceSlice。
	slice *resourceapi.ResourceSlice
	// deviceName 是 taint 所在设备的名称。
	deviceName string
	// taintIndex 是 taint 在设备 taints 列表中的下标。
	taintIndex int
}

// workItem 是存放在工作队列中的待办事项，描述一项需要完成的工作。
type workItem struct {
	// podRef 非空时引用一个可能需要被删除的 Pod。
	//
	// 是否真的需要删除以及何时删除，以 Controller.deletePodAt 为准。
	podRef tainteviction.NamespacedObject

	// ruleRef 非空时引用一个状态可能需要更新的 DeviceTaintRule。
	//
	// 首次更新会尽快执行以提供即时反馈，之后的更新按固定间隔进行（见 ruleStatusPeriod）。
	ruleRef tainteviction.NamespacedObject
}

// workItemForRule 为指定 DeviceTaintRule 构造状态更新工作项。
func workItemForRule(rule *resourceapi.DeviceTaintRule) workItem {
	return workItem{ruleRef: tainteviction.NamespacedObject{NamespacedName: types.NamespacedName{Name: rule.Name}, UID: rule.UID}}
}

// maybeDeletePod 检查 Pod 现在是否需要被删除，需要时执行删除。
// 返回值有三种可能：
// - 发生错误，操作需要重试；
// - 正数延迟，操作需要在将来再次执行；
// - 零延迟，删除已完成或不再需要。
func (tc *Controller) maybeDeletePod(ctx context.Context, podRef tainteviction.NamespacedObject) (againAfter time.Duration, finalErr error) {
	logger := klog.FromContext(ctx)
	logger = klog.LoggerWithValues(logger, "pod", podRef)

	// 执行阻塞式 API 调用时绝不能持有该互斥锁。
	tc.mutex.Lock()
	tc.maybeDeletePodCount++
	eviction, ok := tc.deletePodAt[podRef]
	evicted := tc.evictedPods.has(podRef.UID)
	tc.mutex.Unlock()
	logger.V(5).Info("Processing pod deletion work item", "active", ok, "eviction", eviction, "evicted", evicted)

	if !ok || evicted {
		logger.V(5).Info("Work item for pod deletion obsolete, nothing to do")
		return 0, nil
	}

	now := time.Now()
	againAfter = eviction.when.Sub(now)
	if againAfter > 0 {
		// 还没到时间，驱逐时间可能已经被更新，按新时间稍后再来。
		return againAfter, nil
	}

	defer func() {
		if finalErr == nil {
			// 删除成功（或确认无需删除）后忘记该删除时间。
			// 同时记住该 Pod 已被驱逐，即使期间它再次被加入队列也不必重复检查。
			tc.mutex.Lock()
			delete(tc.deletePodAt, podRef)
			tc.evictedPods.add(podRef.UID)
			tc.mutex.Unlock()
		}
	}()

	err := tc.addConditionAndDeletePod(ctx, podRef)
	if apierrors.IsNotFound(err) {
		// 这不是错误：工作已完成，只是并非由我们完成，因此不计入指标。
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	podDeletionLatency := time.Since(eviction.when.Time)
	logger.V(2).Info("Evicted pod by deleting it", "latency", podDeletionLatency, "reason", eviction.reason)
	tc.metrics.PodDeletionsTotal.Inc()
	tc.metrics.PodDeletionsLatency.Observe(float64(podDeletionLatency.Seconds()))
	tc.mutex.Lock()
	defer tc.mutex.Unlock()
	for _, reason := range eviction.reason {
		if reason.rule != nil {
			stats := tc.taintRuleStats[reason.rule.UID]
			stats.numEvictedPods++
			tc.taintRuleStats[reason.rule.UID] = stats

			// 确保状态最终会被更新。立即更新没有意义：
			// 那样只会与 informer 的更新竞争（规则状态读取的是缓存！）。
			tc.logger.V(5).Info("Adding delayed status update because of pod eviction", "deviceTaintRule", klog.KObj(reason.rule), "delay", ruleStatusPeriod)
			tc.workqueue.AddAfter(workItemForRule(reason.rule), ruleStatusPeriod)
		}
	}

	return 0, nil
}

// addConditionAndDeletePod 给 Pod 打上 DisruptionTarget 条件后将其删除。
func (tc *Controller) addConditionAndDeletePod(ctx context.Context, podRef tainteviction.NamespacedObject) (err error) {
	pod, err := tc.client.CoreV1().Pods(podRef.Namespace).Get(ctx, podRef.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	if pod.UID != podRef.UID {
		// 返回这个特殊错误可以抑制调用方的事件记录并阻止进一步重试：
		// 原本要驱逐的 Pod 已经消失，现在同名的是另一个 Pod。
		return apierrors.NewNotFound(v1.SchemeGroupVersion.WithResource("pods").GroupResource(), pod.Name)
	}

	if pod.DeletionTimestamp != nil {
		// 已经在删除中，无需再次驱逐。
		return nil
	}

	// 只有确实要执行操作时才发送事件。
	tc.emitPodDeletionEvent(podRef)

	newStatus := pod.Status.DeepCopy()
	updated := apipod.UpdatePodCondition(newStatus, &v1.PodCondition{
		Type:    v1.DisruptionTarget,
		Status:  v1.ConditionTrue,
		Reason:  "DeletionByDeviceTaintManager",
		Message: "Device Taint manager: deleting due to NoExecute taint",
	})
	if updated {
		if _, _, _, err := utilpod.PatchPodStatus(ctx, tc.client, pod.Namespace, pod.Name, pod.UID, pod.Status, *newStatus); err != nil {
			return err
		}
	}
	// 虽然概率很低，但上面取到的 Pod 可能在此期间被另一个同名 Pod 替换。
	// 通过 UID 前置条件避免这种竞态：删除会失败，下一次重试时会发现新 Pod 并停止。
	return tc.client.CoreV1().Pods(podRef.Namespace).Delete(ctx, podRef.Name, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{
			UID: &podRef.UID,
		},
	})
}

// maybeUpdateRuleStatus 检查并按需更新 DeviceTaintRule 的 EvictionInProgress 状态条件。
func (tc *Controller) maybeUpdateRuleStatus(ctx context.Context, ruleRef tainteviction.NamespacedObject) (time.Duration, error) {
	logger := klog.FromContext(ctx)
	logger = klog.LoggerWithValues(logger, "deviceTaintRule", ruleRef)
	logger.V(5).Info("Processing DeviceTaintRule status work item")

	tc.mutex.Lock()
	defer tc.mutex.Unlock()

	rule, err := tc.ruleLister.Get(ruleRef.Name)
	if apierrors.IsNotFound(err) {
		logger.V(5).Info("DeviceTaintRule got deleted, removing from work queue")
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("get DeviceTaintRule %s: %w", ruleRef.Name, err)
	}
	if rule.UID != ruleRef.UID {
		logger.V(5).Info("DeviceTaintRule got replaced, removing old one from work queue")
		return 0, nil
	}

	// 条件是否已经设置好了？
	index := slices.IndexFunc(rule.Status.Conditions, func(condition metav1.Condition) bool {
		return condition.Type == resourceapi.DeviceTaintConditionEvictionInProgress
	})

	// 只要我们修改条件（哪怕只改消息），LastTransitionTime 都会推进，
	// 我们用它来判断何时该再次更新。
	//
	// 这里刻意放在统计待驱逐 Pod 之前检查，因为后者可能开销较大。
	// 效果是 “驱逐进行中” 会在删除最后一个 Pod 之后延迟一段时间才变为 false，而不是立刻变化。
	var existingCondition metav1.Condition
	now := metav1.Now()
	if index >= 0 {
		existingCondition = rule.Status.Conditions[index]
		since := now.Time.Sub(existingCondition.LastTransitionTime.Time)
		if existingCondition.ObservedGeneration == rule.Generation &&
			since < ruleStatusPeriod {
			// 暂时还不更新，按剩余时间再次排队。
			return ruleStatusPeriod - since, nil
		}
	}

	// 遍历所有 Pod 的开销可能很大，只在确实需要时才做。
	var numTaintedSliceDevices, numTaintedAllocatedDevices, numPendingPods, numPendingNamespaces int64
	switch rule.Spec.Taint.Effect {
	case resourceapi.DeviceTaintEffectNone:
		// 临时把效果从 None 改成 NoExecute 进行模拟。
		// 我们假装这是通过 informer 事件触发的；由于持有锁，
		// 不会与真实的 informer 事件或其他 goroutine 竞争。
		//
		// 为避免对真实的控制器实例产生持续影响，这里使用临时副本。
		ruleEvict := rule.DeepCopy()
		ruleEvict.Spec.Taint.Effect = resourceapi.DeviceTaintEffectNoExecute
		tc := &Controller{
			logger:                        klog.LoggerWithName(logger, "simulation"),
			podInformer:                   tc.podInformer,
			podLister:                     tc.podLister,
			ruleLister:                    nil, // 由 simulateRule 代替。
			deletePodAt:                   make(map[tainteviction.NamespacedObject]evictionAndReason),
			allocatedClaims:               maps.Clone(tc.allocatedClaims),
			pools:                         tc.pools,
			evictingRules:                 make(map[string]*resourceapi.DeviceTaintRule),
			workqueue:                     &NOPQueue[workItem]{},
			workloadResourceClaimsEnabled: tc.workloadResourceClaimsEnabled,
		}
		defer tc.workqueue.ShutDown()

		tc.handleRuleChange(rule, ruleEvict)
		numPendingPods, numPendingNamespaces, err = tc.countPendingPods(rule)
		numTaintedSliceDevices, numTaintedAllocatedDevices = tc.countTaintedDevices(rule)
	case resourceapi.DeviceTaintEffectNoExecute:
		numPendingPods, numPendingNamespaces, err = tc.countPendingPods(rule)
	default:
		err = nil
	}
	if err != nil {
		return 0, fmt.Errorf("determine pending pods: %w", err)
	}

	// 部分字段是临时值，后面会更新。
	newCondition := metav1.Condition{
		Type:               resourceapi.DeviceTaintConditionEvictionInProgress,
		Status:             metav1.ConditionFalse,
		Reason:             string("Effect" + rule.Spec.Taint.Effect),
		ObservedGeneration: rule.Generation,
		LastTransitionTime: existingCondition.LastTransitionTime, // 先沿用旧时间，避免比较时误判为“有变化”，之后再更新。
	}
	switch rule.Spec.Taint.Effect {
	case resourceapi.DeviceTaintEffectNoExecute:
		switch {
		case numPendingPods > 0:
			newCondition.Reason = "PodsPendingEviction"
			newCondition.Status = metav1.ConditionTrue
			if numPendingPods == 1 {
				newCondition.Message = "1 pod needs to be evicted in "
			} else {
				newCondition.Message = fmt.Sprintf("%d pods need to be evicted in ", numPendingPods)
			}
			if numPendingNamespaces == 1 {
				newCondition.Message += "1 namespace."
			} else {
				newCondition.Message += fmt.Sprintf("%d different namespaces.", numPendingNamespaces)
			}
		case tc.taintRuleStats[rule.UID].numEvictedPods > 0:
			newCondition.Reason = "Completed"
		default:
			newCondition.Reason = "NotStarted"
		}
	case resourceapi.DeviceTaintEffectNone:
		newCondition.Reason = "NoEffect"
		if numTaintedSliceDevices == 1 {
			newCondition.Message += "1 published device selected. "
		} else {
			newCondition.Message += fmt.Sprintf("%d published devices selected. ", numTaintedSliceDevices)
		}
		if numTaintedAllocatedDevices == 1 {
			newCondition.Message += "1 allocated device selected. "
		} else {
			newCondition.Message += fmt.Sprintf("%d allocated devices selected. ", numTaintedAllocatedDevices)
		}
		if numPendingPods == 1 {
			newCondition.Message += "1 pod would be evicted in "
		} else {
			newCondition.Message += fmt.Sprintf("%d pods would be evicted in ", numPendingPods)
		}
		if numPendingNamespaces == 1 {
			newCondition.Message += "1 namespace "
		} else {
			newCondition.Message += fmt.Sprintf("%d different namespaces ", numPendingNamespaces)
		}
		newCondition.Message += "if the effect was NoExecute. This information will not be updated again. Recreate the DeviceTaintRule to trigger an update."
	default:
		newCondition.Reason = "OtherEffect"
		newCondition.Message = "Eviction only happens for the NoExecute effect."
	}
	if numEvictedPods := tc.taintRuleStats[rule.UID].numEvictedPods; numEvictedPods > 0 {
		if newCondition.Message != "" {
			newCondition.Message += " "
		}
		if numEvictedPods == 1 {
			newCondition.Message += "1 pod "
		} else {
			newCondition.Message += fmt.Sprintf("%d pods ", numEvictedPods)
		}
		newCondition.Message += "evicted since starting the controller."
	}

	if newCondition != existingCondition {
		newCondition.LastTransitionTime = now
		logger.V(4).Info("Calculated new condition", "condition", newCondition)

		// 应用新条件，但必须通过 UID 前置条件确保规则未被替换。
		ruleAC := resourceac.DeviceTaintRule(rule.Name).WithUID(rule.UID).WithStatus(resourceac.DeviceTaintRuleStatus().WithConditions(&metav1ac.ConditionApplyConfiguration{
			Type:               &newCondition.Type,
			Status:             &newCondition.Status,
			Reason:             &newCondition.Reason,
			Message:            &newCondition.Message,
			ObservedGeneration: &newCondition.ObservedGeneration,
			LastTransitionTime: &newCondition.LastTransitionTime,
		}))
		if _, err := tc.client.ResourceV1().DeviceTaintRules().ApplyStatus(ctx, ruleAC, metav1.ApplyOptions{FieldManager: tc.name, Force: true}); err != nil {
			return 0, fmt.Errorf("add condition to DeviceTaintRule status: %w", err)
		}
	}

	// 在更多 Pod 被驱逐或变为待驱逐之前，不需要再次更新。
	return 0, nil
}

// countPendingPods 统计因指定规则而需要驱逐的 Pod 数量，以及这些 Pod 所在的命名空间数量。
func (tc *Controller) countPendingPods(rule *resourceapi.DeviceTaintRule) (int64, int64, error) {
	pods, err := tc.podLister.List(labels.Everything())
	if err != nil {
		return -1, -1, fmt.Errorf("list pod: %w", err)
	}

	namespaces := sets.New[string]()
	var numPendingPods int64
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil {
			continue
		}
		eviction := tc.podEvictionTime(pod)
		if eviction == nil {
			continue
		}
		for _, reason := range eviction.reason {
			if reason.rule != nil &&
				reason.rule.UID == rule.UID {
				numPendingPods++
				namespaces.Insert(pod.Namespace)
			}
		}
	}

	return numPendingPods, int64(namespaces.Len()), nil
}

// countTaintedDevices 统计 ResourceSlice 中匹配该规则的设备数量，以及已分配设备中匹配该规则的数量。
func (tc *Controller) countTaintedDevices(rule *resourceapi.DeviceTaintRule) (numTaintedSliceDevices int64, numTaintedAllocatedDevices int64) {
	for poolID, pool := range tc.pools {
		for _, slice := range pool.slices {
			// 旧世代的切片不再代表设备当前状态，跳过。
			if slice.Spec.Pool.Generation != pool.maxGeneration {
				continue
			}
			for _, device := range slice.Spec.Devices {
				if ruleMatchesDevice(rule, poolID.driverName, poolID.poolName, device.Name) {
					numTaintedSliceDevices++
				}
			}
		}
	}

	for _, claim := range tc.allocatedClaims {
		for _, allocatedDevice := range claim.Status.Allocation.Devices.Results {
			if ruleMatchesDevice(rule, allocatedDevice.Driver, allocatedDevice.Pool, allocatedDevice.Device) {
				numTaintedAllocatedDevices++
			}
		}
	}

	return
}

// New 创建一个新的 Controller，使用传入的 clientset 与 API server 通信。
// 该函数不会启动任何 goroutine，启动工作由 Run 完成。
func New(c clientset.Interface, podInformer coreinformers.PodInformer, claimInformer resourceinformers.ResourceClaimInformer, sliceInformer resourceinformers.ResourceSliceInformer, ruleInformer resourceinformers.DeviceTaintRuleInformer, classInformer resourceinformers.DeviceClassInformer, controllerName string) *Controller {
	return newWithFeatures(c, podInformer, claimInformer, sliceInformer, ruleInformer, classInformer, controllerName, utilfeature.DefaultFeatureGate.Enabled(features.DRAWorkloadResourceClaims))
}

// newWithFeatures 按显式给出的特性开关构造 Controller，便于测试不同特性组合。
func newWithFeatures(c clientset.Interface, podInformer coreinformers.PodInformer, claimInformer resourceinformers.ResourceClaimInformer, sliceInformer resourceinformers.ResourceSliceInformer, ruleInformer resourceinformers.DeviceTaintRuleInformer, classInformer resourceinformers.DeviceClassInformer, controllerName string, workloadResourceClaimsEnabled bool) *Controller {
	metrics.Register() // 在这里传入控制器名会更好，但那样可能会破坏 https://kubernetes.io/docs/reference/instrumentation/metrics 文档生成。

	tc := &Controller{
		name: controllerName,

		client:          c,
		podInformer:     podInformer,
		podLister:       podInformer.Lister(),
		claimInformer:   claimInformer,
		sliceInformer:   sliceInformer,
		classInformer:   classInformer,
		deletePodAt:     make(map[tainteviction.NamespacedObject]evictionAndReason),
		allocatedClaims: make(map[types.NamespacedName]allocatedClaim),
		pools:           make(map[poolID]pool),
		evictingRules:   make(map[string]*resourceapi.DeviceTaintRule),
		taintRuleStats:  make(map[types.UID]taintRuleStats),
		// 现在就实例化所有 informer，以确保它们会被启动。
		haveSynced: []cache.DoneChecker{
			podInformer.Informer().HasSyncedChecker(),
			claimInformer.Informer().HasSyncedChecker(),
			sliceInformer.Informer().HasSyncedChecker(),
			classInformer.Informer().HasSyncedChecker(),
		},
		metrics:     metrics.Global,
		evictedPods: newUIDCache(maxUIDCacheEntries),

		workloadResourceClaimsEnabled: workloadResourceClaimsEnabled,
	}

	// DeviceTaintRule 的 informer 只在对应特性启用时才实例化。
	// 特性关闭时，不对任何 DeviceTaintRule 做驱逐处理，也不更新其状态。
	if utilfeature.DefaultFeatureGate.Enabled(features.DRADeviceTaintRules) {
		tc.ruleInformer = ruleInformer
		tc.ruleLister = ruleInformer.Lister()
		tc.haveSynced = append(tc.haveSynced, ruleInformer.Informer().HasSyncedChecker())
	}

	return tc
}

// Run 启动控制器并一直运行，直到 context 结束。
// 启动失败时会返回错误。
func (tc *Controller) Run(ctx context.Context, numWorkers int) error {
	defer utilruntime.HandleCrash()
	logger := klog.FromContext(ctx)
	logger.Info("Starting", "controller", tc.name, "numWorkers", numWorkers)
	defer logger.Info("Shut down controller", "controller", tc.name, "reason", context.Cause(ctx))
	tc.logger = logger

	var wg sync.WaitGroup
	defer wg.Wait()

	// 是否开启了调试级别的日志？
	if loggerV := logger.V(6); loggerV.Enabled() {
		tc.eventLogger = &loggerV
	}

	// 延迟构造 broadcaster，因为它会启动 goroutine。
	// tc.recorder.Eventf 只是内存中的本地操作，永远不会阻塞，
	// 因此可以安全地在事件处理函数中调用；真正的 API 调用发生在它启动的 goroutine 中。
	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	tc.recorder = eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: tc.name}).WithLogger(logger)
	defer eventBroadcaster.Shutdown()

	queueLogger := klog.LoggerWithName(logger, "workqueue")
	delayingQueue := workqueue.NewTypedDelayingQueueWithConfig(workqueue.TypedDelayingQueueConfig[workItem]{
		Logger: &queueLogger,
		Name:   tc.name,
	})
	tc.workqueue = workqueue.NewTypedRateLimitingQueueWithConfig(
		workqueue.DefaultTypedControllerRateLimiter[workItem](),
		workqueue.TypedRateLimitingQueueConfig[workItem]{
			Name:          tc.name,
			DelayingQueue: delayingQueue,
		},
	)
	defer func() {
		logger.V(3).Info("Shutting down work queue")
		tc.workqueue.ShutDown()
	}()

	// 启动事件处理流水线。
	eventBroadcaster.StartStructuredLogging(3)
	if tc.client != nil {
		logger.Info("Sending events to api server")
		eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: tc.client.CoreV1().Events("")})
	} else {
		logger.Error(nil, "kubeClient is nil", "controller", tc.name)
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}
	defer eventBroadcaster.Shutdown()

	if err := tc.addIndexers(); err != nil {
		return err
	}

	claimHandler, err := tc.claimInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			claim, ok := obj.(*resourceapi.ResourceClaim)
			if !ok {
				logger.Error(nil, "Expected ResourceClaim", "actual", fmt.Sprintf("%T", obj))
				return
			}
			// 事件处理函数内只做内存计算，用锁保护共享状态，绝不调用阻塞 API。
			tc.mutex.Lock()
			defer tc.mutex.Unlock()
			tc.handleClaimChange(nil, claim)
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldClaim, ok := oldObj.(*resourceapi.ResourceClaim)
			if !ok {
				logger.Error(nil, "Expected ResourceClaim", "actual", fmt.Sprintf("%T", oldObj))
				return
			}
			newClaim, ok := newObj.(*resourceapi.ResourceClaim)
			if !ok {
				logger.Error(nil, "Expected ResourceClaim", "actual", fmt.Sprintf("%T", newObj))
			}
			tc.mutex.Lock()
			defer tc.mutex.Unlock()
			tc.handleClaimChange(oldClaim, newClaim)
		},
		DeleteFunc: func(obj any) {
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tombstone.Obj
			}
			claim, ok := obj.(*resourceapi.ResourceClaim)
			if !ok {
				logger.Error(nil, "Expected ResourceClaim", "actual", fmt.Sprintf("%T", obj))
				return
			}
			tc.mutex.Lock()
			defer tc.mutex.Unlock()
			tc.handleClaimChange(claim, nil)
		},
	}, cache.HandlerOptions{Logger: &logger})
	if err != nil {
		return fmt.Errorf("adding claim event handler:%w", err)
	}
	defer func() {
		_ = tc.claimInformer.Informer().RemoveEventHandler(claimHandler)
	}()
	tc.haveSynced = append(tc.haveSynced, claimHandler.HasSyncedChecker())

	podHandler, err := tc.podInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				logger.Error(nil, "Expected Pod", "actual", fmt.Sprintf("%T", obj))
				return
			}
			tc.mutex.Lock()
			defer tc.mutex.Unlock()
			tc.handlePodChange(nil, pod)
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldPod, ok := oldObj.(*v1.Pod)
			if !ok {
				logger.Error(nil, "Expected Pod", "actual", fmt.Sprintf("%T", oldObj))
				return
			}
			newPod, ok := newObj.(*v1.Pod)
			if !ok {
				logger.Error(nil, "Expected Pod", "actual", fmt.Sprintf("%T", newObj))
			}
			tc.mutex.Lock()
			defer tc.mutex.Unlock()
			tc.handlePodChange(oldPod, newPod)
		},
		DeleteFunc: func(obj any) {
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tombstone.Obj
			}
			pod, ok := obj.(*v1.Pod)
			if !ok {
				logger.Error(nil, "Expected Pod", "actual", fmt.Sprintf("%T", obj))
				return
			}
			tc.mutex.Lock()
			defer tc.mutex.Unlock()
			tc.handlePodChange(pod, nil)
		},
	}, cache.HandlerOptions{Logger: &logger})
	if err != nil {
		return fmt.Errorf("adding pod event handler: %w", err)
	}
	defer func() {
		_ = tc.podInformer.Informer().RemoveEventHandler(podHandler)
	}()
	tc.haveSynced = append(tc.haveSynced, podHandler.HasSyncedChecker())

	if tc.ruleInformer != nil {
		ruleHandler, err := tc.ruleInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {
				rule, ok := obj.(*resourceapi.DeviceTaintRule)
				if !ok {
					logger.Error(nil, "Expected DeviceTaintRule", "actual", fmt.Sprintf("%T", obj))
					return
				}
				tc.mutex.Lock()
				defer tc.mutex.Unlock()
				tc.handleRuleChange(nil, rule)
			},
			UpdateFunc: func(oldObj, newObj any) {
				oldRule, ok := oldObj.(*resourceapi.DeviceTaintRule)
				if !ok {
					logger.Error(nil, "Expected DeviceTaintRule", "actual", fmt.Sprintf("%T", oldObj))
					return
				}
				newRule, ok := newObj.(*resourceapi.DeviceTaintRule)
				if !ok {
					logger.Error(nil, "Expected DeviceTaintRule", "actual", fmt.Sprintf("%T", newObj))
				}
				tc.mutex.Lock()
				defer tc.mutex.Unlock()
				tc.handleRuleChange(oldRule, newRule)
			},
			DeleteFunc: func(obj any) {
				if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
					obj = tombstone.Obj
				}
				rule, ok := obj.(*resourceapi.DeviceTaintRule)
				if !ok {
					logger.Error(nil, "Expected DeviceTaintRule", "actual", fmt.Sprintf("%T", obj))
					return
				}
				tc.mutex.Lock()
				defer tc.mutex.Unlock()
				tc.handleRuleChange(rule, nil)
			},
		}, cache.HandlerOptions{Logger: &logger})
		if err != nil {
			return fmt.Errorf("adding DeviceTaintRule event handler: %w", err)
		}
		defer func() {
			_ = tc.ruleInformer.Informer().RemoveEventHandler(ruleHandler)
		}()
		tc.haveSynced = append(tc.haveSynced, ruleHandler.HasSyncedChecker())
	}

	sliceHandler, err := tc.sliceInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			slice, ok := obj.(*resourceapi.ResourceSlice)
			if !ok {
				logger.Error(nil, "Expected ResourceSlice", "actual", fmt.Sprintf("%T", obj))
				return
			}
			tc.mutex.Lock()
			defer tc.mutex.Unlock()
			tc.handleSliceChange(nil, slice)
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldSlice, ok := oldObj.(*resourceapi.ResourceSlice)
			if !ok {
				logger.Error(nil, "Expected ResourceSlice", "actual", fmt.Sprintf("%T", oldObj))
				return
			}
			newSlice, ok := newObj.(*resourceapi.ResourceSlice)
			if !ok {
				logger.Error(nil, "Expected ResourceSlice", "actual", fmt.Sprintf("%T", newObj))
			}
			tc.mutex.Lock()
			defer tc.mutex.Unlock()
			tc.handleSliceChange(oldSlice, newSlice)
		},
		DeleteFunc: func(obj any) {
			// 这里不需要处理 DeletedFinalStateUnknown，resourceslicetracker 不会使用该类型。
			slice, ok := obj.(*resourceapi.ResourceSlice)
			if !ok {
				logger.Error(nil, "Expected ResourceSlice", "actual", fmt.Sprintf("%T", obj))
				return
			}
			tc.mutex.Lock()
			defer tc.mutex.Unlock()
			tc.handleSliceChange(slice, nil)
		},
	}, cache.HandlerOptions{Logger: &logger})
	if err != nil {
		return fmt.Errorf("adding slice event handler: %w", err)
	}
	defer func() {
		_ = tc.sliceInformer.Informer().RemoveEventHandler(sliceHandler)
	}()
	tc.haveSynced = append(tc.haveSynced, sliceHandler.HasSyncedChecker())

	if !cache.WaitFor(ctx, "cache and event handler sync", tc.haveSynced...) {
		// 能走到这里说明调用方取消了 context，这不是错误。
		return nil
	}
	logger.V(1).Info("Underlying informers have synced")
	tc.hasSynced.Store(1)

	for i := range numWorkers {
		wg.Go(func() {
			tc.worker(klog.NewContext(ctx, klog.LoggerWithName(queueLogger, fmt.Sprintf("worker-%d", i))))
		})
	}

	<-ctx.Done()
	return nil
}

// addIndexers 为 Pod informer 注册所需的索引，仅在 PodGroup 特性启用时生效。
func (tc *Controller) addIndexers() error {
	if !tc.workloadResourceClaimsEnabled {
		return nil
	}

	err := tc.podInformer.Informer().AddIndexers(cache.Indexers{podGroupIndex: func(obj any) ([]string, error) {
		pod, ok := obj.(*v1.Pod)
		if !ok {
			return nil, fmt.Errorf("unexpected type %T reached Pod informer indexer", obj)
		}
		if pod.Spec.SchedulingGroup == nil || pod.Spec.SchedulingGroup.PodGroupName == nil {
			return nil, nil
		}
		return []string{pod.Namespace + "/" + *pod.Spec.SchedulingGroup.PodGroupName}, nil
	}})
	if err != nil {
		return fmt.Errorf("adding %s indexer for pods: %w", podGroupIndex, err)
	}

	return nil
}

// evictPod 确保 Pod 在指定时间被驱逐，该方法不会阻塞。
func (tc *Controller) evictPod(podRef tainteviction.NamespacedObject, eviction evictionAndReason) {
	tc.deletePodAt[podRef] = eviction
	now := time.Now()
	delay := eviction.when.Sub(now)
	if delay <= 0 {
		tc.logger.V(3).Info("Adding immediate pod eviction", "pod", podRef, "eviction", eviction)
		tc.workqueue.Add(workItem{podRef: podRef})
	} else {
		tc.logger.V(3).Info("Adding delayed pod eviction", "pod", podRef, "eviction", eviction, "delay", delay)
		tc.workqueue.AddAfter(workItem{podRef: podRef}, delay)
	}

	if tc.evictPodHook != nil {
		tc.evictPodHook(podRef, eviction)
	}
}

// cancelEvict 取消此前通过 evictPod 设置的驱逐。
// 该操作是幂等的；若没有可取消的驱逐则返回 false。
func (tc *Controller) cancelEvict(podRef tainteviction.NamespacedObject) bool {
	_, ok := tc.deletePodAt[podRef]
	if !ok {
		// 没有需要取消的驱逐。
		return false
	}
	delete(tc.deletePodAt, podRef)

	if tc.cancelEvictHook != nil {
		tc.cancelEvictHook(podRef)
	}

	// 无法从工作队列中移除条目，worker 会通过检查 deletePodAt 发现该条目已失效。
	return true
}

// worker 一直阻塞直到工作队列关闭。
// context 取消只会中止正在执行的工作，不会让 worker 退出。
func (tc *Controller) worker(ctx context.Context) {
	logger := klog.FromContext(ctx)
	defer utilruntime.HandleCrashWithLogger(logger)

	for {
		item, shutdown := tc.workqueue.Get()
		if shutdown {
			return
		}

		func() {
			defer tc.workqueue.Done(item)

			againAfter, err := tc.handleWork(ctx, item)
			switch {
			case err != nil:
				logger.V(3).Info("Processing work item failed, will retry", "err", err)
				tc.workqueue.AddRateLimited(item)
			case againAfter > 0:
				logger.V(5).Info("Checking work item again later", "delay", againAfter)
				tc.workqueue.AddAfter(item, againAfter)
			default:
				tc.workqueue.Forget(item)
			}
		}()
	}
}

// handleWork 根据工作项的类型分派到 Pod 删除或规则状态更新逻辑。
func (tc *Controller) handleWork(ctx context.Context, item workItem) (time.Duration, error) {
	if item.podRef.Name != "" {
		return tc.maybeDeletePod(ctx, item.podRef)
	}
	return tc.maybeUpdateRuleStatus(ctx, item.ruleRef)
}

// handleClaimChange 根据 ResourceClaim 的增删改更新已分配声明缓存，并重新检查其使用方 Pod。
func (tc *Controller) handleClaimChange(oldClaim, newClaim *resourceapi.ResourceClaim) {
	claim := newClaim
	if claim == nil {
		claim = oldClaim
	}
	name := newNamespacedName(claim)
	if tc.eventLogger != nil {
		// 这里刻意记录得非常详细，用于调试。
		tc.eventLogger.Info("ResourceClaim changed", "claimObject", name, "oldClaim", klog.Format(oldClaim), "newClaim", klog.Format(newClaim), "diff", diff.Diff(oldClaim, newClaim))
	}

	// 已删除？
	if newClaim == nil {
		delete(tc.allocatedClaims, name)
		tc.handlePods(claim)
		return
	}

	// 新增？
	if oldClaim == nil {
		if claim.Status.Allocation == nil {
			return
		}
		tc.allocatedClaims[name] = allocatedClaim{
			ResourceClaim: claim,
			eviction:      tc.claimEvictionTime(claim),
		}
		tc.handlePods(claim)
		return
	}

	// 即使名字相同，两个声明的 UID 也可能不同。虽然不太可能，但并非不可能，
	// 此时按“先删除再添加”处理。
	if oldClaim.UID != newClaim.UID {
		tc.handleClaimChange(oldClaim, nil)
		tc.handleClaimChange(nil, newClaim)
		return
	}

	syncBothClaims := func() {
		// ReservedFor 可能发生了变化。若变化则要同步新旧两个列表，
		// 否则处理一次即可（两者相同）。
		if !slices.Equal(oldClaim.Status.ReservedFor, newClaim.Status.ReservedFor) {
			tc.handlePods(oldClaim)
			tc.handlePods(newClaim)
		} else {
			tc.handlePods(claim)
		}
	}

	// 新增了分配结果？
	if oldClaim.Status.Allocation == nil && newClaim.Status.Allocation != nil {
		tc.allocatedClaims[name] = allocatedClaim{
			ResourceClaim: claim,
			eviction:      tc.claimEvictionTime(claim),
		}
		syncBothClaims()
		return
	}

	// 分配结果被移除了？
	if oldClaim.Status.Allocation != nil && newClaim.Status.Allocation == nil {
		delete(tc.allocatedClaims, name)
		syncBothClaims()
		return
	}

	// 更新前后都已分配？
	if claim.Status.Allocation != nil {
		// Allocation 是不可变的，因此无需重新计算驱逐时间，
		// 只保存较新的 ResourceClaim 即可。
		tc.allocatedClaims[name] = allocatedClaim{
			ResourceClaim: claim,
			eviction:      tc.allocatedClaims[name].eviction,
		}
		syncBothClaims()
		return
	}

	// 走到这里说明什么都没变。
}

// claimEvictionTime 返回已分配设备中任意 NoExecute taint 最早的 TimeAdded，
// 被容忍的 taint 不计入；若没有则返回 nil。只能对已分配的 ResourceClaim 调用。
func (tc *Controller) claimEvictionTime(claim *resourceapi.ResourceClaim) *evictionAndReason {
	var when *metav1.Time
	var taints sets.Set[trackedTaint]

	allocation := claim.Status.Allocation
	for _, allocatedDevice := range allocation.Devices.Results {
		id := poolID{driverName: allocatedDevice.Driver, poolName: allocatedDevice.Pool}
		slice, device := tc.pools[id].getDevice(allocatedDevice.Device)

	nextTaint:
		for taint := range tc.allEvictingDeviceTaints(allocatedDevice, slice, device) {
			newEvictionTime := taint.deviceTaint().TimeAdded
			haveToleration := false
			tolerationSeconds := int64(math.MaxInt64)
			for _, toleration := range allocatedDevice.Tolerations {
				if toleration.Effect == resourceapi.DeviceTaintEffectNoExecute &&
					resourceclaim.ToleratesTaint(toleration, *taint.deviceTaint()) {
					if toleration.TolerationSeconds == nil {
						// 永久容忍，直接忽略这条 taint。
						continue nextTaint
					}
					newTolerationSeconds := max(*toleration.TolerationSeconds, 0)
					if newTolerationSeconds < tolerationSeconds {
						tolerationSeconds = newTolerationSeconds
					}
					haveToleration = true
				}
			}
			if haveToleration {
				newEvictionTime = &metav1.Time{Time: newEvictionTime.Add(time.Duration(tolerationSeconds) * time.Second)}
			}
			if taints == nil {
				taints = sets.New[trackedTaint]()
			}
			taints.Insert(taint)
			if when == nil {
				when = newEvictionTime
				tc.logger.V(5).Info("Claim is affected by device taint", "claim", klog.KObj(claim), "device", allocatedDevice, "taint", taint, "evictionTime", when)
				continue
			}
			if newEvictionTime != nil && newEvictionTime.Before(when) {
				when = newEvictionTime
				tc.logger.V(5).Info("Claim is affected by device taint", "claim", klog.KObj(claim), "device", allocatedDevice, "taint", taint, "evictionTime", when)
			}
		}
	}

	if when == nil {
		return nil
	}
	eviction := &evictionAndReason{when: *when, reason: taints.UnsortedList()}
	slices.SortFunc(eviction.reason, func(a, b trackedTaint) int { return a.Compare(b) })
	return eviction
}

// allEvictingDeviceTaints 返回一个迭代器，用于遍历所有影响该已分配设备、
// 且效果为 NoExecute 的 DeviceTaintRule。
// taint 可能来自 ResourceSlice informer（不是 tracker！）或 DeviceTaintRule，但不会同时来自两者。
func (tc *Controller) allEvictingDeviceTaints(allocatedDevice resourceapi.DeviceRequestAllocationResult, slice *resourceapi.ResourceSlice, device *resourceapi.Device) iter.Seq[trackedTaint] {
	evictingRules := tc.evictingRules

	return func(yield func(trackedTaint) bool) {
		if device != nil {
			for i := range device.Taints {
				taint := &device.Taints[i]
				if taint.Effect != resourceapi.DeviceTaintEffectNoExecute {
					continue
				}
				if !yield(trackedTaint{slice: sliceDeviceTaint{slice: slice, deviceName: device.Name, taintIndex: i}}) {
					return
				}
			}
		}

		for _, rule := range evictingRules {
			if !ruleMatchesDevice(rule, allocatedDevice.Driver, allocatedDevice.Pool, allocatedDevice.Device) {
				continue
			}

			if !yield(trackedTaint{rule: rule}) {
				return
			}
		}
	}
}

// ruleMatchesDevice 判断规则的设备选择器是否匹配给定的驱动、池和设备名。
func ruleMatchesDevice(rule *resourceapi.DeviceTaintRule, driverName, poolName, deviceName string) bool {
	selector := rule.Spec.DeviceSelector
	if selector == nil {
		return false
	}
	if selector.Driver != nil && *selector.Driver != driverName ||
		selector.Pool != nil && *selector.Pool != poolName ||
		selector.Device != nil && *selector.Device != deviceName {
		return false
	}
	return true
}

// handleRuleChange 处理 DeviceTaintRule 的增删改，更新驱逐规则集合、状态更新队列，
// 并在规则变化时重新计算所有已分配声明的驱逐时间。
func (tc *Controller) handleRuleChange(oldRule, newRule *resourceapi.DeviceTaintRule) {
	rule := newRule
	if rule == nil {
		rule = oldRule
	}
	name := newNamespacedName(rule)
	if tc.eventLogger != nil {
		// 这里刻意记录得非常详细，用于调试。
		tc.eventLogger.Info("DeviceTaintRule changed", "ruleObject", name, "oldRule", klog.Format(oldRule), "newRule", klog.Format(newRule), "diff", diff.Diff(oldRule, newRule))
	}

	if oldRule == nil {
		// 至少立即更新一次状态，并且要在驱逐任何 Pod 之前完成。
		tc.logger.V(5).Info("Adding immediate status update because of new rule", "deviceTaintRule", klog.KObj(newRule))
		tc.workqueue.Add(workItemForRule(newRule))
	}

	if newRule == nil {
		// 清理统计数据，避免内存泄漏。
		delete(tc.taintRuleStats, oldRule.UID)
		delete(tc.evictingRules, oldRule.Name)
		// 从工作队列中移除由 worker 处理工作项时完成，工作队列本身不支持取消工作。
	}

	if oldRule != nil &&
		newRule != nil &&
		oldRule.UID == newRule.UID {
		if apiequality.Semantic.DeepEqual(&oldRule.Spec, &newRule.Spec) {
			return
		}
		// 至少立即更新一次状态，并且要在驱逐任何 Pod 之前完成。
		tc.logger.V(5).Info("Adding immediate status update because of modified rule spec", "deviceTaintRule", klog.KObj(newRule))
		tc.workqueue.Add(workItemForRule(newRule))
	}

	// 规则 Spec 的变化应该很少见，所以直接对所有已分配声明做暴力重新评估。
	// 试图避免 evictingRules 中先删后加也一样，只会让逻辑无谓地复杂化。
	if oldRule != nil && oldRule.Spec.Taint.Effect == resourceapi.DeviceTaintEffectNoExecute {
		delete(tc.evictingRules, oldRule.Name)
	}
	if newRule != nil && newRule.Spec.Taint.Effect == resourceapi.DeviceTaintEffectNoExecute {
		tc.evictingRules[newRule.Name] = newRule
	}
	for name, oldAllocatedClaim := range tc.allocatedClaims {
		newAllocatedClaim := allocatedClaim{
			ResourceClaim: oldAllocatedClaim.ResourceClaim,
			eviction:      tc.claimEvictionTime(oldAllocatedClaim.ResourceClaim),
		}
		tc.allocatedClaims[name] = newAllocatedClaim
		if !newAllocatedClaim.eviction.equal(oldAllocatedClaim.eviction) {
			tc.handlePods(newAllocatedClaim.ResourceClaim)
		}
	}
}

// handleSliceChange 处理 ResourceSlice 的增删改，找出 taint 发生变化的设备，
// 并针对使用这些设备的 ResourceClaim 重新计算驱逐时间。
func (tc *Controller) handleSliceChange(oldSlice, newSlice *resourceapi.ResourceSlice) {
	slice := newSlice
	if slice == nil {
		slice = oldSlice
	}
	poolID := poolID{
		driverName: slice.Spec.Driver,
		poolName:   slice.Spec.Pool.Name,
	}
	if tc.eventLogger != nil {
		// 这里刻意记录得非常详细，用于调试。
		tc.eventLogger.Info("ResourceSlice changed", "pool", poolID, "oldSlice", klog.Format(oldSlice), "newSlice", klog.Format(newSlice), "diff", diff.Diff(oldSlice, newSlice))
	}

	// 先确定新旧设备 taint。只有发生变化的设备才需要进一步检查使用它们的 ResourceClaim。
	p := tc.pools[poolID]
	oldDeviceTaints := p.getTaintedDevices()
	p.removeSlice(oldSlice)
	p.addSlice(newSlice)
	if len(p.slices) == 0 {
		delete(tc.pools, poolID)
	} else {
		tc.pools[poolID] = p
	}
	newDeviceTaints := p.getTaintedDevices()

	// 现在计算差异，这依赖于两个列表都已按设备名排序。
	if len(oldDeviceTaints) == 0 && len(newDeviceTaints) == 0 {
		// 两边都为空，没有变化。
		return
	}
	modifiedDevices := sets.New[string]()
	o, n := 0, 0
	for o < len(oldDeviceTaints) || n < len(newDeviceTaints) {
		// 按相同设备名配对遍历新旧列表。
		for o < len(oldDeviceTaints) && n < len(newDeviceTaints) && oldDeviceTaints[o].deviceName == newDeviceTaints[n].deviceName {
			if !apiequality.Semantic.DeepEqual(oldDeviceTaints[o].taint, newDeviceTaints[n].taint) { // TODO: hard-code the comparison?
				modifiedDevices.Insert(oldDeviceTaints[o].deviceName)
			}
			o++
			n++
		}

		// 跳过已被移除的旧设备。
		newDeviceName := ""
		if n < len(newDeviceTaints) {
			newDeviceName = newDeviceTaints[n].deviceName
		}
		for o < len(oldDeviceTaints) && oldDeviceTaints[o].deviceName != newDeviceName {
			modifiedDevices.Insert(oldDeviceTaints[o].deviceName)
			o++
		}

		// 跳过新增的设备。
		oldDeviceName := ""
		if o < len(oldDeviceTaints) {
			oldDeviceName = oldDeviceTaints[o].deviceName
		}
		if n < len(newDeviceTaints) && newDeviceTaints[n].deviceName != oldDeviceName {
			modifiedDevices.Insert(newDeviceTaints[n].deviceName)
			n++
		}
	}

	// 找出所有使用了至少一个变化设备的 ResourceClaim，更新其驱逐时间并处理其消费方 Pod。
	for name, claim := range tc.allocatedClaims {
		if !usesDevice(claim.Status.Allocation, poolID, modifiedDevices) {
			continue
		}
		newEvictionTime := tc.claimEvictionTime(claim.ResourceClaim)
		if newEvictionTime.equal(claim.eviction) {
			// 没有变化。
			continue
		}
		claim.eviction = newEvictionTime
		tc.allocatedClaims[name] = claim
		// 也可以先收集依赖发生变化声明的 Pod 再统一处理，
		// 但实际上大多数 Pod 只依赖一个声明，
		// 为避免构建这种映射、让常见路径保持简单，这里直接逐个处理。
		tc.handlePods(claim.ResourceClaim)
	}
}

// usesDevice 判断分配结果中是否用到了给定池内发生变化的设备。
func usesDevice(allocation *resourceapi.AllocationResult, pool poolID, modifiedDevices sets.Set[string]) bool {
	for _, device := range allocation.Devices.Results {
		if device.Driver == pool.driverName &&
			device.Pool == pool.poolName &&
			modifiedDevices.Has(device.Device) {
			return true
		}
	}
	return false
}

// handlePodChange 处理 Pod 的增删改：删除时取消驱逐，调度或声明状态变化时才重新检查。
func (tc *Controller) handlePodChange(oldPod, newPod *v1.Pod) {
	pod := newPod
	if pod == nil {
		pod = oldPod
	}
	if tc.eventLogger != nil {
		// 这里刻意记录得非常详细，用于调试。
		tc.eventLogger.Info("Pod changed", "pod", klog.KObj(pod), "oldPod", klog.Format(oldPod), "newPod", klog.Format(newPod), "diff", diff.Diff(oldPod, newPod))
	}
	if newPod == nil {
		// Pod 已消失，无需再做任何处理，也不必发送事件。
		tc.cancelEvict(newObject(oldPod))
		return
	}

	// Pod 更新非常频繁。除非其声明相关状态或调度结果发生变化，否则无需重新检查。
	//
	// 这一点尤其能防止在刚给 Pod 加上驱逐条件之后立刻又把它重新加入队列。
	if oldPod != nil &&
		oldPod.Spec.NodeName == newPod.Spec.NodeName &&
		resourceclaim.PodStatusEqual(oldPod.Status.ResourceClaimStatuses, newPod.Status.ResourceClaimStatuses) &&
		resourceclaim.PodExtendedStatusEqual(oldPod.Status.ExtendedResourceClaimStatus, newPod.Status.ExtendedResourceClaimStatus) {
		return
	}

	tc.handlePod(newPod)
}

// handlePods 找出 ResourceClaim 的所有消费方（Pod 或 PodGroup 中的 Pod），并逐一重新检查。
func (tc *Controller) handlePods(claim *resourceapi.ResourceClaim) {
	for _, consumer := range claim.Status.ReservedFor {
		switch {
		case consumer.APIGroup == "" && consumer.Resource == "pods":
			pod, err := tc.podLister.Pods(claim.Namespace).Get(consumer.Name)
			if err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				// 正常情况下不应发生。
				utilruntime.HandleErrorWithLogger(tc.logger, err, "retrieve pod from cache")
				continue
			}
			if pod.UID != consumer.UID {
				// 不是我们要找的那个 Pod。
				continue
			}
			tc.handlePod(pod)
		case consumer.APIGroup == schedulingapi.GroupName && consumer.Resource == "podgroups":
			// 如果 DRAWorkloadResourceClaims 特性先启用后被禁用，
			// PodGroup 可能仍残留在 ResourceClaim 的 status.reservedFor 中，
			// 因此这里要再次确认该特性当前处于启用状态。
			if !tc.workloadResourceClaimsEnabled {
				continue
			}
			podGroupKey := claim.Namespace + "/" + consumer.Name
			pods, err := tc.podInformer.Informer().GetIndexer().ByIndex(podGroupIndex, podGroupKey)
			if err != nil {
				utilruntime.HandleErrorWithLogger(tc.logger, err, "retrieve Pods for PodGroup %s from cache", podGroupKey)
				continue
			}
			for _, obj := range pods {
				pod, ok := obj.(*v1.Pod)
				if !ok {
					continue
				}
				tc.handlePod(pod)
			}
		}
	}
}

// handlePod 根据 Pod 当前使用的 ResourceClaim 计算驱逐时间，并设置或取消驱逐。
func (tc *Controller) handlePod(pod *v1.Pod) {
	eviction := tc.podEvictionTime(pod)
	podRef := newObject(pod)
	if eviction == nil {
		if tc.cancelWorkWithEvent(podRef) {
			tc.logger.V(3).Info("Canceled pod eviction", "pod", podRef)
		}
		return
	}

	tc.evictPod(podRef, *eviction)

	// 只要任一原因是 taint，就说明驱逐正在进行，状态可能需要更新。
	// 但不立即更新，因为后续可能还有更多 Pod 变化进来。
	for _, reason := range eviction.reason {
		if reason.rule != nil {
			tc.logger.V(5).Info("Adding delayed status update because of pod change", "deviceTaintRule", klog.KObj(reason.rule), "delay", ruleStatusPeriod)
			tc.workqueue.AddAfter(workItemForRule(reason.rule), ruleStatusPeriod)
		}
	}
}

// podEvictionTime 计算 Pod 最早的驱逐时间和全部原因；不需要驱逐时返回 nil。
func (tc *Controller) podEvictionTime(pod *v1.Pod) *evictionAndReason {
	// 还没被调度，自然无需驱逐。
	if pod.Spec.NodeName == "" {
		return nil
	}

	// 如果 Pod 使用的任一 ResourceClaim 被 taint 且不被容忍，就需要驱逐该 Pod。
	var eviction *evictionAndReason
	for claimName, mustCheckOwner := range resourceclaim.PodClaims(pod) {
		allocatedClaim, ok := tc.allocatedClaims[types.NamespacedName{Namespace: pod.Namespace, Name: claimName}]
		if !ok {
			// 声明被引用但不存在或尚未分配，也就没有 taint。
			continue
		}
		if mustCheckOwner && resourceclaim.IsForPod(pod, allocatedClaim.ResourceClaim, tc.workloadResourceClaimsEnabled) != nil {
			// 声明与 Pod 不匹配，忽略该声明。
			continue
		}
		if !resourceclaim.IsReservedForPod(pod, allocatedClaim.ResourceClaim, tc.workloadResourceClaimsEnabled) {
			// 该 Pod 不是被允许和/或预期使用此声明的那个 Pod。
			// 也许旧 Pod 实例已被删除，现在看到的是同名的替代实例，无论如何都忽略。
			continue
		}
		if allocatedClaim.eviction == nil {
			continue
		}
		if eviction == nil {
			// 第一个原因的时间直接采用即可。
			eviction = allocatedClaim.eviction
		} else {
			// 合并所有原因并计算新的最早时间，最终结果可能与原来相同。
			newEvictionTime := &evictionAndReason{
				when:   allocatedClaim.eviction.when,
				reason: slices.Clone(allocatedClaim.eviction.reason),
			}
			// 同一个 Pod 同时受到多个原因影响的情况非常罕见，
			// 使用简单的插入排序就足够。
			for _, reason := range eviction.reason {
				index, found := slices.BinarySearchFunc(newEvictionTime.reason, reason, func(a, b trackedTaint) int { return a.Compare(b) })
				if !found {
					newEvictionTime.reason = slices.Insert(newEvictionTime.reason, index, reason)
				}
			}
			if eviction.when.Before(&newEvictionTime.when) {
				newEvictionTime.when = eviction.when
			}
			if !eviction.equal(newEvictionTime) {
				eviction = newEvictionTime
			}
		}
	}

	return eviction
}

// cancelWorkWithEvent 取消 Pod 的驱逐，并在确实取消成功时发送取消事件。
func (tc *Controller) cancelWorkWithEvent(podRef tainteviction.NamespacedObject) bool {
	if tc.cancelEvict(podRef) {
		tc.emitCancelPodDeletionEvent(podRef)
		return true
	}
	return false
}

// emitPodDeletionEvent 记录一条 Pod 即将因设备 taint 被删除的 Normal 事件。
func (tc *Controller) emitPodDeletionEvent(podRef tainteviction.NamespacedObject) {
	if tc.recorder == nil {
		return
	}
	ref := &v1.ObjectReference{
		APIVersion: "v1",
		Kind:       "Pod",
		Name:       podRef.Name,
		Namespace:  podRef.Namespace,
		UID:        podRef.UID,
	}
	tc.recorder.Eventf(ref, v1.EventTypeNormal, "DeviceTaintManagerEviction", "Marking for deletion")
}

// emitCancelPodDeletionEvent 记录一条 Pod 删除计划被取消的 Normal 事件。
func (tc *Controller) emitCancelPodDeletionEvent(podRef tainteviction.NamespacedObject) {
	if tc.recorder == nil {
		return
	}
	ref := &v1.ObjectReference{
		APIVersion: "v1",
		Kind:       "Pod",
		Name:       podRef.Name,
		Namespace:  podRef.Namespace,
		UID:        podRef.UID,
	}
	tc.recorder.Eventf(ref, v1.EventTypeNormal, "DeviceTaintManagerEviction", "Cancelling deletion")
}

// newNamespacedName 从对象中提取命名空间和名称。
func newNamespacedName(obj metav1.Object) types.NamespacedName {
	return types.NamespacedName{
		Namespace: obj.GetNamespace(),
		Name:      obj.GetName(),
	}
}

// TODO: 用 klog.ObjectInstance 替换（https://github.com/kubernetes/klog/issues/422#issuecomment-3454948091）。

// newObject 构造带 UID 的对象引用，用于精确标识对象实例而非仅凭名称。
func newObject(obj metav1.Object) tainteviction.NamespacedObject {
	if obj == nil {
		return tainteviction.NamespacedObject{}
	}
	return tainteviction.NamespacedObject{
		NamespacedName: newNamespacedName(obj),
		UID:            obj.GetUID(),
	}
}
