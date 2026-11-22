/*
Copyright 2022 The Kubernetes Authors.

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

package dynamicresources

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"slices"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/diff"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	"k8s.io/component-helpers/nodedeclaredfeatures/features/draoptionalnodeoperations"
	"k8s.io/component-helpers/resource"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/dynamic-resource-allocation/cel"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	"k8s.io/dynamic-resource-allocation/structured"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/validation"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/helper"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	schedmetrics "k8s.io/kubernetes/pkg/scheduler/metrics"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"
	"k8s.io/utils/ptr"
)

const (
	// Name 是插件在注册表和配置中使用的名字。
	Name = names.DynamicResources

	stateKey fwk.StateKey = Name
)

// stateData 在 PreFilter 阶段初始化。由于存进 fwk.CycleState 的是指针，
// 后续阶段直接修改指针指向的内容即可，无需再调用 Write 更新状态。
type stateData struct {
	// claims 保存该 Pod 所有 ResourceClaim 的副本（与 pod.Spec.ResourceClaims 一一对应），
	// 初始状态取自调度周期开始时的状态。每个 claim 实例是只读的，因为它可能来自 informer 缓存；
	// 只有在插件自身成功执行 Update 后，实例才会被替换。
	//
	// DRAExtendedResource 特性开启时，代表扩展资源请求的那个特殊 ResourceClaim 也存这里。
	// 插件代码应把该字段当黑盒，只通过它的方法访问，尤其：
	// - all 方法遍历所有 claim（包括特殊 claim）；
	// - allUserClaims 方法排除特殊 claim。
	//
	// Pod 没有 claim、也没有 DRA 扩展资源的特殊 claim 时该字段为空，此时插件对该 Pod 无事可做。
	claims claimStore

	// draExtendedResource 保存 DRA 支撑的扩展资源数据。
	// DRAExtendedResource 特性关闭时该字段为空。
	draExtendedResource draExtendedResource

	// allocator 处理带结构化参数的 claim，如今所有 claim 都是这种。
	allocator structured.Allocator

	// 访问下面所有字段前必须持有 mutex。
	mutex sync.Mutex

	// unavailableClaims 记录满足以下条件的 claim 下标：
	// - 已分配；
	// - 在至少一个节点上不可用。
	//
	// 这些下标在 Filter 中并行写入，必须持 mutex 保护。供 PostFilter 使用。
	unavailableClaims sets.Set[int]

	// informationsForClaim 与 claims 中的每个 claim 一一对应。
	informationsForClaim []informationForClaim

	// nodeAllocations 缓存各节点的 Filter 结果，以节点名为键。
	nodeAllocations map[string]nodeAllocation

	// claimHasNodeAllocatableMappedDevice 记录某个 claim 的分配是否被映射。
	// 当 claim 被分配到某个设备，且该设备通过 ResourceSlice 内 Device spec 的
	// NodeAllocatableResources.Mapping 字段建模了节点可分配资源（如 cpu、内存）时，即为被映射。
	// Filter 阶段用该 map 防止多个 Pod 共享这类被映射的 claim。
	// 在 PreFilter 填充，在 Filter 读取。
	claimHasNodeAllocatableMappedDevice map[types.UID]bool
}

// Clone 实现 StateData 接口。状态原地可变，直接返回自身指针。
func (d *stateData) Clone() fwk.StateData {
	return d
}

// podGroupStateData 保存在 PodGroup 的 CycleState 中。由于存进 fwk.CycleState 的是指针，
// 后续阶段直接修改指针指向的内容即可，无需再调用 Write 更新状态。
type podGroupStateData struct {
	// pendingAllocations 记录本轮 PodGroup 调度周期中处于 pending 的 ResourceClaim UID，
	// 映射到共享该 pending 分配的 Pod UID 集合。跨周期的 pending 分配共享情况
	// （分配本身及共享 Pod 数量）仍由 DRAManager 继续跟踪。
	//
	// 无需加锁：PodGroup 的 CycleState 只在同步调度阶段可用，异步阶段开始前就会被清空。
	pendingAllocations map[types.UID]sets.Set[types.UID]

	// podsStateData 保存 PodGroup 中每个 Pod 的 stateData。
	podsStateData map[types.NamespacedName]*stateData
}

// Clone 实现 StateData 接口。状态原地可变，直接返回自身指针。
func (d *podGroupStateData) Clone() fwk.StateData {
	return d
}

// informationForClaim 保存单个 claim 在调度过程中派生出的信息。
type informationForClaim struct {
	// availableOnNodes 是已分配时根据 claim 状态得到的节点选择器。
	availableOnNodes *nodeaffinity.NodeSelector

	// allocation 由 Reserve 设置、PreBind 发布；无需分配时为空。
	// 若分配由前一个 Pod 的 Reserve 完成但尚未在 PreBind 发布，PreFilter 也可能设置它。
	allocation *resourceapi.AllocationResult
}

// nodeAllocation 保存单个节点上的分配结果和扩展资源 claim。
type nodeAllocation struct {
	// allocationResults 保存分配结果，顺序与需要分配的 claim 一一对应。
	allocationResults []resourceapi.AllocationResult
	// extendedResourceClaim 是 Filter 阶段为该节点创建的 DRA 扩展资源特殊 claim。
	extendedResourceClaim *resourceapi.ResourceClaim
	// containerResourceRequestMappings 是 Filter 阶段算出的容器、扩展资源与设备请求映射，
	// 供 PreBind 阶段使用。
	containerResourceRequestMappings []v1.ContainerExtendedResourceRequest
	// nodeAllocatableResourceClaimStatuses 保存通过 DRA 计算出的节点可分配资源分配情况。
	// 在 Filter 阶段填充，传递给 PreBind。
	nodeAllocatableResourceClaimStatuses []v1.NodeAllocatableResourceClaimStatus
}

// DynamicResources 是确保 ResourceClaim 被分配的插件。
type DynamicResources struct {
	enabled               bool
	fts                   feature.Features
	filterTimeout         time.Duration
	bindingTimeout        time.Duration
	fh                    fwk.Handle
	clientset             kubernetes.Interface
	celCache              *cel.Cache
	draManager            fwk.SharedDRAManager
	podIndexer            cache.Indexer
	podResourceClaimIndex string
}

const (
	// podResourceClaimIndexPrefix 是按 Pod 引用的 ResourceClaim 建立索引的索引函数名前缀。
	// 查找 Pod 用的键格式为 "<namespace>/<name>"。
	podResourceClaimIndexPrefix = "schedulerPodResourceClaim"
)

var (
	// ErrDeviceBindingTimeout 表示等待设备绑定条件满足时超时。
	ErrDeviceBindingTimeout = errors.New("device binding timeout")
	// ErrDeviceBindingFailed 表示设备绑定条件被判定为失败。
	ErrDeviceBindingFailed = errors.New("device binding failed")
)

// New 初始化并返回 DynamicResources 插件实例。
func New(ctx context.Context, plArgs runtime.Object, fh fwk.Handle, fts feature.Features) (fwk.Plugin, error) {
	if !fts.EnableDynamicResourceAllocation {
		// 特性关闭时返回一个空壳插件，所有扩展点都不做事。
		return &DynamicResources{}, nil
	}

	args, ok := plArgs.(*config.DynamicResourcesArgs)
	if !ok {
		return nil, fmt.Errorf("got args of type %T, want *DynamicResourcesArgs", plArgs)
	}
	if err := validation.ValidateDynamicResourcesArgs(nil, args, fts); err != nil {
		return nil, err
	}

	pl := &DynamicResources{
		enabled:       true,
		fts:           fts,
		filterTimeout: ptr.Deref(args.FilterTimeout, metav1.Duration{}).Duration,
		bindingTimeout: ptr.Deref(
			args.BindingTimeout,
			metav1.Duration{Duration: config.DynamicResourcesBindingTimeoutDefault},
		).Duration,
		fh:        fh,
		clientset: fh.ClientSet(),
		// 编译后的 CEL 表达式 LRU 缓存：最近 10 条可跨调度周期复用。
		celCache: cel.NewCache(10, cel.Features{
			EnableConsumableCapacity: fts.EnableDRAConsumableCapacity,
			EnableListTypeAttributes: fts.EnableDRAListTypeAttributes,
		}),
		draManager: fh.SharedDRAManager(),
	}

	// 建立 Pod 索引，供 PreQueueingHint 按 claim 反查受影响的 Pod。
	podInformer := fh.SharedInformerFactory().Core().V1().Pods()
	podResourceClaimIndex := podResourceClaimIndexPrefix + "-" + fh.ProfileName()
	if err := podInformer.Informer().AddIndexers(cache.Indexers{podResourceClaimIndex: podResourceClaimIndexFunc}); err != nil {
		return nil, fmt.Errorf("failed to add pod resource claim indexer: %w", err)
	}
	pl.podIndexer = podInformer.Informer().GetIndexer()
	pl.podResourceClaimIndex = podResourceClaimIndex

	return pl, nil
}

var _ fwk.PreEnqueuePlugin = &DynamicResources{}
var _ fwk.PreFilterPlugin = &DynamicResources{}
var _ fwk.FilterPlugin = &DynamicResources{}
var _ fwk.PostFilterPlugin = &DynamicResources{}
var _ fwk.PodGroupPostFilterPlugin = &DynamicResources{}
var _ fwk.ScorePlugin = &DynamicResources{}
var _ fwk.ReservePlugin = &DynamicResources{}
var _ fwk.EnqueueExtensions = &DynamicResources{}
var _ fwk.PreBindPlugin = &DynamicResources{}
var _ fwk.SignPlugin = &DynamicResources{}

// Name 返回插件名，用于日志等场景。
func (pl *DynamicResources) Name() string {
	return Name
}

// SignPod 为 Pod 生成签名片段。
// 判断 DRA claim 是单主机还是更复杂并不简单，这里保守地把带 DRA claim 的 Pod 排除在签名之外。
// See https://github.com/kubernetes/kubernetes/issues/134986
func (pl *DynamicResources) SignPod(ctx context.Context, pod *v1.Pod) ([]fwk.SignFragment, *fwk.Status) {
	if len(pod.Spec.ResourceClaims) > 0 {
		return nil, fwk.NewStatus(fwk.Unschedulable, "pods with dra resource claims are not signable")
	}
	return nil, nil
}

// EventsToRegister 返回可能让被本插件判定为不可调度的 Pod 重新变得可调度的事件。
func (pl *DynamicResources) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	if !pl.enabled {
		return nil, nil
	}

	events := []fwk.ClusterEventWithHint{
		// 资源拓扑过滤可能依赖节点标签，节点新增或标签/可分配量更新都可能让 Pod 可调度。
		{Event: fwk.ClusterEvent{Resource: fwk.Node, ActionType: fwk.Add | fwk.UpdateNodeLabel | fwk.UpdateNodeAllocatable}},
		// 分配状态记录在 ResourceClaim 中，其任何变化都可能让 Pod 可调度。
		{Event: fwk.ClusterEvent{Resource: fwk.ResourceClaim, ActionType: fwk.Add | fwk.Update | fwk.Delete}, QueueingHintFn: pl.isSchedulableAfterClaimChange, PreQueueingHintFn: pl.preQueueingHint},
		// 把生成的 ResourceClaim 名字写入 Pod status，会让等待该 claim 的 Pod 可调度。
		{Event: fwk.ClusterEvent{Resource: fwk.TargetPod, ActionType: fwk.UpdatePodGeneratedResourceClaim}, QueueingHintFn: pl.isSchedulableAfterTargetPodUpdate},
		// Pod 可能在等设备类被创建或修改。
		{Event: fwk.ClusterEvent{Resource: fwk.DeviceClass, ActionType: fwk.Add | fwk.Update}},
		// 新增或更新 ResourceSlice 意味着有新资源可用，可能让 Pod 可调度。
		{Event: fwk.ClusterEvent{Resource: fwk.ResourceSlice, ActionType: fwk.Add | fwk.Update}},
	}

	return events, nil
}

// PreEnqueue 检查是否存在已知原因导致 Pod 当前无法调度。
// 返回失败后，注册的事件之一会触发新的调度尝试。
func (pl *DynamicResources) PreEnqueue(ctx context.Context, pod *v1.Pod) (status *fwk.Status) {
	if !pl.enabled {
		return nil
	}

	if err := pl.foreachPodResourceClaim(pod, nil); err != nil {
		return statusUnschedulable(klog.FromContext(ctx), err.Error())
	}
	return nil
}

// podResourceClaimIndexFunc 是索引函数，返回 Pod 引用的所有 ResourceClaim 的键（namespace/name）。
// pod.Status.ExtendedResourceClaimStatus 中的扩展资源 claim 故意不建索引：
// 它们是调度器在 PreBind 阶段创建的，调度器不会等待其创建。
func podResourceClaimIndexFunc(obj interface{}) ([]string, error) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return nil, nil
	}
	keySet := sets.New[string]()
	for _, podClaim := range pod.Spec.ResourceClaims {
		claimName, _, err := resourceclaim.Name(pod, &podClaim)
		if err != nil || claimName == nil {
			continue
		}
		keySet.Insert(pod.Namespace + "/" + *claimName)
	}
	return keySet.UnsortedList(), nil
}

// preQueueingHint 返回受某个 ResourceClaim 事件影响的 Pod 集合。
func (pl *DynamicResources) preQueueingHint(logger klog.Logger, oldObj, newObj interface{}) (fwk.PreQueueingHintResult, error) {
	if oldObj != nil && newObj != nil {
		if oldClaim, ok := oldObj.(*resourceapi.ResourceClaim); ok {
			if newClaim, ok := newObj.(*resourceapi.ResourceClaim); ok {
				// 取消分配释放了资源，可能让其他 Pod 变为可调度。
				if oldClaim.Status.Allocation != nil && newClaim.Status.Allocation == nil {
					return fwk.PreQueueingHintResult{AllPods: true}, nil
				}
			}
		}
	}

	// 删除已分配的 claim 同样释放资源，可能让其他 Pod 可调度。
	if newObj == nil && oldObj != nil {
		if oldClaim, ok := oldObj.(*resourceapi.ResourceClaim); ok {
			if oldClaim.Status.Allocation != nil {
				return fwk.PreQueueingHintResult{AllPods: true}, nil
			}
		}
	}

	obj := newObj
	if obj == nil {
		obj = oldObj
	}
	claim, ok := obj.(*resourceapi.ResourceClaim)
	if !ok {
		return fwk.PreQueueingHintResult{AllPods: true}, nil
	}
	objs, err := pl.podIndexer.ByIndex(pl.podResourceClaimIndex, claim.Namespace+"/"+claim.Name)
	if err != nil {
		return fwk.PreQueueingHintResult{}, fmt.Errorf("failed to look up pods by claim %s/%s: %w", claim.Namespace, claim.Name, err)
	}
	pods := make([]types.NamespacedName, 0, len(objs))
	for _, obj := range objs {
		if pod, ok := obj.(*v1.Pod); ok {
			pods = append(pods, types.NamespacedName{Name: pod.Name, Namespace: pod.Namespace})
		}
	}
	return fwk.PreQueueingHintResult{Pods: pods}, nil
}

// isSchedulableAfterClaimChange 在 informer 报告 ResourceClaim 新增/更新事件时调用，
// 判断该变化是否让之前不可调度的 Pod 变为可调度。判断不准时宁可让 Pod 重试一次。
func (pl *DynamicResources) isSchedulableAfterClaimChange(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	if newObj == nil {
		if _, ok := (oldObj).(cache.DeletedFinalStateUnknown); ok {
			// 正常情况下应先收到移除分配的更新事件，此时被删对象没有分配，可以跳过入队。
			// 但这里明确告知被删 ResourceClaim 的最终状态未知，内容无从得知，甚至可能根本取不到。
			// 我们可能漏掉了添加和移除分配的更新事件，保守起见检查等待中的 Pod。
			logger.V(6).Info("claim with structured parameters got deleted, final state unknown")
			return fwk.Queue, nil
		}
	}

	originalClaim, modifiedClaim, err := schedutil.As[*resourceapi.ResourceClaim](oldObj, newObj)
	if err != nil {
		// 理论上不会发生。
		return fwk.Queue, fmt.Errorf("unexpected object in isSchedulableAfterClaimChange: %w", err)
	}

	if modifiedClaim == nil {
		// ResourceClaim 被删除且最终状态已知（上面已确认）。
		// 此时预期分配为 nil，但检查代价很低，还是确认一下。
		//
		// 这依赖于不漏掉「分配被移除」的更新事件。普通 informer 可能把
		// 「添加分配」和「移除分配」合并为一次更新，前后看起来分配都是 nil，
		// 就像一次无关变更。但我们用的是 assume cache，且自己是唯一添加分配的实体，
		// 因此不会漏掉「添加分配」事件，也就一定能看到「移除分配」。
		if originalClaim.Status.Allocation == nil {
			logger.V(6).Info("claim with structured parameters got deleted, had no allocation")
			return fwk.QueueSkip, nil
		}
		// 这种情况很反常，实际不应发生。
		logger.V(6).Info("claim with structured parameters got deleted, had an allocation")
		return fwk.Queue, nil
	}

	usesClaim := false
	if err := pl.foreachPodResourceClaim(pod, func(_ string, claim *resourceapi.ResourceClaim) {
		if claim.UID == modifiedClaim.UID {
			usesClaim = true
		}
	}); err != nil {
		// 这不是意外错误：foreachPodResourceClaim 只会在 Pod 不可调度时返回错误。
		if loggerV := logger.V(6); loggerV.Enabled() {
			owner := metav1.GetControllerOf(modifiedClaim)
			loggerV.Info("pod is not schedulable after resource claim change", "pod", klog.KObj(pod), "claim", klog.KObj(modifiedClaim), "claimOwner", owner, "reason", err.Error())
		}
		return fwk.QueueSkip, nil
	}

	if originalClaim != nil &&
		originalClaim.Status.Allocation != nil &&
		modifiedClaim.Status.Allocation == nil {
		// 带结构化参数的 claim 被取消分配，可能为其他 Pod 释放了资源。
		logger.V(6).Info("claim with structured parameters got deallocated", "pod", klog.KObj(pod), "claim", klog.KObj(modifiedClaim))
		return fwk.Queue, nil
	}

	if !usesClaim {
		// 不是 Pod 正在等待的那个 claim。
		logger.V(6).Info("unrelated claim got modified", "pod", klog.KObj(pod), "claim", klog.KObj(modifiedClaim))
		return fwk.QueueSkip, nil
	}

	if originalClaim == nil {
		logger.V(5).Info("claim for pod got created", "pod", klog.KObj(pod), "claim", klog.KObj(modifiedClaim))
		return fwk.Queue, nil
	}

	// 修改不一定与调度有关：若整个 status 与之前相同，变化的只是 Pod 不关心的字段
	// （实际中通常是资源驱动加了 finalizer）。
	if apiequality.Semantic.DeepEqual(&originalClaim.Status, &modifiedClaim.Status) {
		if loggerV := logger.V(7); loggerV.Enabled() {
			// 详细日志才打印对象差异。
			loggerV.Info("claim for pod got modified where the pod doesn't care", "pod", klog.KObj(pod), "claim", klog.KObj(modifiedClaim), "diff", diff.Diff(originalClaim, modifiedClaim))
		} else {
			logger.V(6).Info("claim for pod got modified where the pod doesn't care", "pod", klog.KObj(pod), "claim", klog.KObj(modifiedClaim))
		}
		return fwk.QueueSkip, nil
	}

	logger.V(5).Info("status of claim for pod got updated", "pod", klog.KObj(pod), "claim", klog.KObj(modifiedClaim))
	return fwk.Queue, nil
}

// isSchedulableAfterTargetPodUpdate 在 informer 报告 Pod 更新事件时调用，
// 检查这次更新是否补上了 Pod 一直在等待的 ResourceClaim 名字。
func (pl *DynamicResources) isSchedulableAfterTargetPodUpdate(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	_, modifiedPod, err := schedutil.As[*v1.Pod](nil, newObj)
	if err != nil {
		// 理论上不会发生。
		return fwk.Queue, fmt.Errorf("unexpected object in isSchedulableAfterTargetPodUpdate: %w", err)
	}

	if err := pl.foreachPodResourceClaim(modifiedPod, nil); err != nil {
		// 这不是意外错误：foreachPodResourceClaim 只会在 Pod 不可调度时返回错误。
		logger.V(6).Info("pod is not schedulable after being updated", "pod", klog.KObj(pod))
		return fwk.QueueSkip, nil
	}

	logger.V(5).Info("pod got updated and is schedulable", "pod", klog.KObj(pod))
	return fwk.Queue, nil
}

// podResourceClaims 返回 pod.Spec.ResourceClaims 引用的所有 ResourceClaim。
func (pl *DynamicResources) podResourceClaims(pod *v1.Pod) ([]*resourceapi.ResourceClaim, error) {
	claims := make([]*resourceapi.ResourceClaim, 0, len(pod.Spec.ResourceClaims))
	if err := pl.foreachPodResourceClaim(pod, func(_ string, claim *resourceapi.ResourceClaim) {
		// 直接保存 lister 返回的指针。假设代码执行期间 claim 被修改时，
		// 缓存会换成新指针，而不会原地改掉我们手里这个对象。
		claims = append(claims, claim)
	}); err != nil {
		return nil, err
	}
	return claims, nil
}

// podResourceClaimBindings 返回每个 Pod claim 应绑定到的对象引用列表
// （对应 ResourceClaim 的 Status.ReservedFor），引用对象要么是 Pod 自身，要么是其 PodGroup。
func (pl *DynamicResources) podResourceClaimBindings(pod *v1.Pod) ([]resourceapi.ResourceClaimConsumerReference, error) {
	if !pl.fts.EnableDRAWorkloadResourceClaims {
		return nil, nil
	}

	bindings := make([]resourceapi.ResourceClaimConsumerReference, 0, len(pod.Spec.ResourceClaims))
	podGroup, err := pl.getPodGroupSnapshot(pod)
	if err != nil {
		return nil, err
	}
	for _, podClaim := range pod.Spec.ResourceClaims {
		bindTo, err := resourceclaim.BindTo(pod, podGroup, &podClaim)
		if err != nil {
			return nil, err
		}
		bindings = append(bindings, bindTo)
	}
	return bindings, nil
}

// foreachPodResourceClaim 检查 Pod 引用的每个 ResourceClaim 都存在，
// 并对找到的 claim 调用可选回调 cb。
func (pl *DynamicResources) foreachPodResourceClaim(pod *v1.Pod, cb func(podResourceName string, claim *resourceapi.ResourceClaim)) error {
	for _, resource := range pod.Spec.ResourceClaims {
		claimName, mustCheckOwner, err := resourceclaim.Name(pod, &resource)
		if err != nil {
			return err
		}
		// 被引用的 claim 可能还没有生成对应的底层 ResourceClaim，此时名字为 nil。
		// 这是合法的使用场景，直接跳过。
		if claimName == nil {
			continue
		}
		claim, err := pl.draManager.ResourceClaims().Get(pod.Namespace, *claimName)
		if err != nil {
			return err
		}

		if claim.DeletionTimestamp != nil {
			return fmt.Errorf("resourceclaim %q is being deleted", claim.Name)
		}

		if mustCheckOwner {
			if err := resourceclaim.IsForPod(pod, claim, pl.fts.EnableDRAWorkloadResourceClaims); err != nil {
				return err
			}
		}
		if cb != nil {
			cb(resource.Name, claim)
		}
	}
	return nil
}

// PreFilter 在 prefilter 扩展点调用，检查 Pod 所需的所有 immediate claim 是否已绑定。
// 若 Pod 当前在任何节点上都无法调度，返回 UnschedulableAndUnresolvable。
func (pl *DynamicResources) PreFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, nodes []fwk.NodeInfo) (*fwk.PreFilterResult, *fwk.Status) {
	if pl.fts.EnableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	if !pl.enabled {
		return nil, fwk.NewStatus(fwk.Skip)
	}
	logger := klog.FromContext(ctx)

	// 先初始化一个空状态：即使 Pod 不引用任何 claim，也要让后续阶段知道这一点。
	// 若确有需要处理的 claim，下面的流程会继续填充它。
	s := &stateData{
		nodeAllocations: make(map[string]nodeAllocation),
	}
	state.Write(stateKey, s)

	podGroupState, err := getOrCreatePodGroupStateData(state)
	if err != nil {
		return nil, statusError(logger, err)
	}
	// 存在 PodGroup 状态时挂上本 Pod 的状态，供 PodGroupPostFilter 使用。
	if podGroupState != nil {
		if podGroupState.podsStateData == nil {
			podGroupState.podsStateData = make(map[types.NamespacedName]*stateData)
		}
		podGroupState.podsStateData[types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}] = s
	}

	userClaims, err := pl.podResourceClaims(pod)
	if err != nil {
		return nil, statusUnschedulable(logger, err.Error())
	}
	logger.V(5).Info("pod resource claims", "pod", klog.KObj(pod), "resourceclaims", klog.KObjSlice(userClaims))
	extendedResourceClaim, status := pl.preFilterExtendedResources(pod, logger, s)
	if status != nil {
		return nil, status
	}
	claimBindings, err := pl.podResourceClaimBindings(pod)
	if err != nil {
		return nil, statusUnschedulable(logger, err.Error())
	}
	claims := newClaimStore(userClaims, extendedResourceClaim, claimBindings)

	// 该检查同时覆盖用户 claim 和扩展资源 claim。
	if claims.empty() {
		return nil, fwk.NewStatus(fwk.Skip)
	}

	// 统计需要调度器自己分配的 claim 数量。
	numClaimsToAllocate := 0
	s.informationsForClaim = make([]informationForClaim, claims.len())
	for index, claim := range claims.all() {
		if claim.Status.Allocation != nil &&
			!resourceclaim.CanBeReserved(claim) &&
			!resourceclaim.IsReservedForPod(pod, claim, pl.fts.EnableDRAWorkloadResourceClaims) {
			// 资源已被占用，Pod 只能等待。
			return nil, statusUnschedulable(logger, "resourceclaim in use", "pod", klog.KObj(pod), "resourceclaim", klog.KObj(claim))
		}

		if claim.Status.Allocation != nil {
			nodeSelector, err := nodeSelectorFromAllocation(claim.Status.Allocation)
			if err != nil {
				return nil, statusError(logger, err)
			}
			s.informationsForClaim[index].availableOnNodes = nodeSelector
		} else {
			numClaimsToAllocate++

			// Gang 调度的做法是等 gang 中所有 Pod 都通过 Reserve，再一起绑定。
			// 由于 claim 的分配直到 PreBind 才写入 [stateData.claims]，这里查找
			// Reserve 阶段创建的 pending 分配，避免当前 Pod 卡在等待分配完成上。
			//
			// 有 pending 分配的 claim 仍视为「未分配」，因为其 Status.Allocation 尚待填充。
			// 这类 claim 不会作为分配器的输入，但仍会增加 numClaimsToAllocate 以创建分配器，
			// 表示有 claim 需要在 PreBind 更新 status。
			//
			// 目前共享仅限于同一个 PodGroup 调度周期。为控制 alpha 阶段 GenericWorkload
			// feature gate 之外的影响，未分组的 Pod 以及跨 PodGroup 周期的 Pod 暂不能共享
			// pending 分配的 claim。更广泛地共享这类 claim 可能有益：
			// https://github.com/kubernetes/kubernetes/issues/137932
			if podGroupState != nil && podGroupState.pendingAllocations[claim.UID] != nil {
				if pendingAllocation := pl.draManager.ResourceClaims().GetPendingAllocation(claim.UID); pendingAllocation != nil {
					s.informationsForClaim[index].allocation = pendingAllocation
					nodeSelector, err := nodeSelectorFromAllocation(pendingAllocation)
					if err != nil {
						return nil, statusError(logger, err)
					}
					s.informationsForClaim[index].availableOnNodes = nodeSelector
					logger.V(5).Info("reusing pending allocation", "pod", klog.KObj(pod), "resourceclaim", klog.KObj(claim), "uid", claim.UID, "allocation", klog.Format(pendingAllocation))
					continue
				}
			}

			// 分配正在进行中时最好等它完成，详见 inFlightAllocations 的说明。
			if pl.draManager.ResourceClaims().GetPendingAllocation(claim.UID) != nil {
				return nil, statusUnschedulable(logger, fmt.Sprintf("resource claim %s is in the process of being allocated", klog.KObj(claim)))
			}

			// DRA 扩展资源的特殊 claim 不在这里校验：claim 模板此时还没分配也没有 spec；
			// 上个调度周期留下的 claim 则留到 Filter 阶段校验。
			if claim == extendedResourceClaim {
				continue
			}

			// 检查所有请求及其设备类：类不存在时无论 claim 如何分配都无法继续调度。
			// 使用控制面 controller 时，设备类可能带节点过滤器，可在向驱动询问之前
			// 先缩小候选节点范围。
			for _, request := range claim.Spec.Devices.Requests {
				// firstAvailable 中是否定义备选子请求，决定了校验方式。
				switch {
				case request.Exactly != nil:
					if status := pl.validateDeviceClass(logger, request.Exactly.DeviceClassName, request.Name); status != nil {
						return nil, status
					}
				case len(request.FirstAvailable) > 0:
					if !pl.fts.EnableDRAPrioritizedList {
						return nil, statusUnschedulable(logger, fmt.Sprintf("resource claim %s, request %s: has subrequests, but the DRAPrioritizedList feature is disabled", klog.KObj(claim), request.Name))
					}
					for _, subRequest := range request.FirstAvailable {
						qualRequestName := strings.Join([]string{request.Name, subRequest.Name}, "/")
						if status := pl.validateDeviceClass(logger, subRequest.DeviceClassName, qualRequestName); status != nil {
							return nil, status
						}
					}
				default:
					return nil, statusUnschedulable(logger, fmt.Sprintf("resource claim %s, request %s: unknown request type", klog.KObj(claim), request.Name))
				}
			}
		}
	}

	// 预先算出每个 claim 是否分配了节点可分配资源的映射设备。
	// 结果存入 CycleState，Filter 阶段据此判断 Pod 是否在共享另一个 Pod 的直接映射 claim。
	if pl.fts.EnableDRANodeAllocatableResources {
		s.claimHasNodeAllocatableMappedDevice = make(map[types.UID]bool)
		for index, claim := range claims.all() {
			allocation := claim.Status.Allocation
			if allocation == nil {
				// claim 尚未提交到 API server（没有 Status.Allocation）时，
				// 要检查本调度周期内前一个 Pod 留下的 pending 分配。
				allocation = s.informationsForClaim[index].allocation
			}
			if allocation != nil {
				isMapped := false
				for _, result := range allocation.Devices.Results {
					device, err := getDeviceFromManager(pl.draManager, &result)
					if err == nil && device != nil && device.NodeAllocatableResources != nil {
						for _, mapping := range device.NodeAllocatableResources {
							if mapping.Mapping != nil {
								isMapped = true
								break
							}
						}
					}
					if isMapped {
						break
					}
				}
				s.claimHasNodeAllocatableMappedDevice[claim.UID] = isMapped
			}
		}
	}

	if numClaimsToAllocate > 0 {
		if loggerV := logger.V(5); loggerV.Enabled() {
			claimsToAllocate := make([]*resourceapi.ResourceClaim, 0, claims.len())
			for _, claim := range claims.toAllocate() {
				claimsToAllocate = append(claimsToAllocate, claim)
			}
			loggerV.Info("Preparing allocation with structured parameters", "pod", klog.KObj(pod), "resourceclaims", klog.KObjSlice(claimsToAllocate))
		}

		// 本可以只构建一次分配器，随后随变化增量更新，避免每个 Pod 都重复构建。
		// 但那样会让插件难以在 Cluster Autoscaler 中使用。如果这里开销变大，
		// 就得考虑更持久地维护和更新状态。
		//
		// claim（及其设备）只要进入 assume cache 或分配正在进行就算「已分配」。
		// 过滤阶段这个集合不变，因此只需确定一次。
		//
		// 万一并发修改让结果失效（极少见），这里会重试。
		var allocatedState *structured.AllocatedState
		err = wait.PollUntilContextTimeout(ctx, time.Microsecond, 5*time.Second, true /* immediate */, func(context.Context) (bool, error) {
			if pl.fts.EnableDRAConsumableCapacity {
				allocatedState, err = pl.draManager.ResourceClaims().GatherAllocatedState()
				if err != nil {
					if errors.Is(err, errClaimTrackerConcurrentModification) {
						logger.V(6).Info("Conflicting modification during GatherAllocatedState, trying again")
						return false, nil
					}
					return false, err
				}
				if allocatedState == nil {
					return false, errors.New("nil allocated state")
				}
				// 完成。
				return true, nil
			} else {
				allocatedDevices, err := pl.draManager.ResourceClaims().ListAllAllocatedDevices()
				if err != nil {
					if errors.Is(err, errClaimTrackerConcurrentModification) {
						logger.V(6).Info("Conflicting modification during ListAllAllocatedDevices, trying again")
						return false, nil
					}
					return false, err
				}
				allocatedState = &structured.AllocatedState{
					AllocatedDevices:         allocatedDevices,
					AllocatedSharedDeviceIDs: sets.New[structured.SharedDeviceID](),
					AggregatedCapacity:       structured.NewConsumedCapacityCollection(),
				}
				// 完成。
				return true, nil
			}
		})
		if err != nil {
			return nil, statusError(logger, fmt.Errorf("gather allocation state: %w", err))
		}
		slices, err := pl.draManager.ResourceSlices().ListWithDeviceTaintRules()
		if err != nil {
			return nil, statusError(logger, err)
		}
		features := AllocatorFeatures(pl.fts)
		allocator, err := structured.NewAllocator(ctx, features, *allocatedState, pl.draManager.DeviceClasses(), slices, pl.celCache)
		if err != nil {
			return nil, statusError(logger, err)
		}
		s.allocator = allocator
	}
	s.claims = claims
	return nil, nil
}

// AllocatorFeatures 把调度器的 feature gate 集合映射为结构化分配器所需的特性集合。
func AllocatorFeatures(fts feature.Features) structured.Features {
	return structured.Features{
		AdminAccess:             fts.EnableDRAAdminAccess,
		PrioritizedList:         fts.EnableDRAPrioritizedList,
		PartitionableDevices:    fts.EnableDRAPartitionableDevices,
		DeviceTaints:            fts.EnableDRADeviceTaints,
		DeviceBindingAndStatus:  fts.EnableDRADeviceBindingConditions && fts.EnableDRAResourceClaimDeviceStatus,
		ConsumableCapacity:      fts.EnableDRAConsumableCapacity,
		FractionalCapacityRange: fts.EnableDRAFractionalCapacityRange,
		ListTypeAttributes:      fts.EnableDRAListTypeAttributes,
		OptionalNodeOperations:  fts.EnableDRAOptionalNodeOperations,
		DerivedAttributes:       fts.EnableDRADerivedAttributes,
		CompatibilityGroups:     fts.EnableDRADeviceCompatibilityGroups,
	}
}

// validateDeviceClass 校验请求引用的设备类存在；类不存在时返回不可调度状态。
func (pl *DynamicResources) validateDeviceClass(logger klog.Logger, deviceClassName, requestName string) *fwk.Status {
	if deviceClassName == "" {
		return statusError(logger, fmt.Errorf("request %s: unsupported request type", requestName))
	}

	_, err := pl.draManager.DeviceClasses().Get(deviceClassName)
	if err != nil {
		// 取不到类就无法继续分配。
		if apierrors.IsNotFound(err) {
			// 标记为不可调度，Pod 会留在不可调度队列中，直到收到 DeviceClass 事件。
			return statusUnschedulable(logger, fmt.Sprintf("request %s: device class %s does not exist", requestName, deviceClassName))
		}
	}
	return nil
}

// PreFilterExtensions 返回 nil：本插件不需要 Pod 增删时的增量更新。
func (pl *DynamicResources) PreFilterExtensions() fwk.PreFilterExtensions {
	return nil
}

// getStateData 从 CycleState 读取 PreFilter 写入的动态资源状态。
func getStateData(cs fwk.CycleState) (*stateData, error) {
	state, err := cs.Read(stateKey)
	if err != nil {
		return nil, err
	}
	s, ok := state.(*stateData)
	if !ok {
		return nil, errors.New("unable to convert state into stateData")
	}
	return s, nil
}

// getPodGroupStateDataFromPodGroupCycleState 从 PodGroup 的 CycleState 读取动态资源状态。
func getPodGroupStateDataFromPodGroupCycleState(pgState fwk.PodGroupCycleState) (*podGroupStateData, error) {
	if pgState == nil {
		return nil, nil
	}
	state, err := pgState.Read(stateKey)
	if err != nil {
		return nil, err
	}
	s, ok := state.(*podGroupStateData)
	if !ok {
		return nil, errors.New("state is not podGroupStateData")
	}
	return s, nil
}

// getPodGroupStateData 从 Pod 的 CycleState 取关联的 PodGroup 动态资源状态。
// GenericWorkload 特性关闭、Pod 不属于任何 PodGroup、或 PodGroup 处于异步绑定阶段时返回 nil。
func getPodGroupStateData(cs fwk.CycleState) (*podGroupStateData, error) {
	podGroupCycleState := cs.GetPodGroupSchedulingCycle()
	return getPodGroupStateDataFromPodGroupCycleState(podGroupCycleState)
}

// getOrCreatePodGroupStateData 读取 PodGroup 动态资源状态，不存在时创建并写入空状态。
func getOrCreatePodGroupStateData(cs fwk.CycleState) (*podGroupStateData, error) {
	podGroupState, err := getPodGroupStateData(cs)
	if errors.Is(err, fwk.ErrNotFound) {
		podGroupCycleState := cs.GetPodGroupSchedulingCycle()
		if podGroupCycleState == nil {
			// 理论上不会发生：[getPodGroupStateData] 只有在
			// [fwk.CycleState.GetPodGroupSchedulingCycle] 返回非空后才会报 ErrNotFound。
			return nil, err
		}
		podGroupState := &podGroupStateData{
			pendingAllocations: make(map[types.UID]sets.Set[types.UID]),
			podsStateData:      make(map[types.NamespacedName]*stateData),
		}
		podGroupCycleState.Write(stateKey, podGroupState)
		return podGroupState, nil
	}
	return podGroupState, err
}

// Filter 在 filter 扩展点调用，判断 Pod 请求的资源（无论 claim 已分配还是未分配）
// 能否放到节点上。
// 对已绑定的 claim，检查节点是否满足其节点亲和性；
// 对未绑定的 claim，检查它是否有可能在该节点上被分配。
func (pl *DynamicResources) Filter(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) *fwk.Status {
	if pl.fts.EnableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil
	}
	if !pl.enabled {
		return nil
	}
	state, err := getStateData(cs)
	if err != nil {
		return statusError(klog.FromContext(ctx), err)
	}
	if state.claims.empty() {
		return nil
	}

	var podGroupState *podGroupStateData
	if pl.fts.EnableDRANodeAllocatableResources {
		var err error
		podGroupState, err = getPodGroupStateData(cs)
		if err != nil {
			return statusError(klog.FromContext(ctx), err)
		}
	}

	logger := klog.FromContext(ctx)
	node := nodeInfo.Node()
	nodeExtendedResourceClaim, containerResourceRequestMappings, status := pl.filterExtendedResources(state, pod, nodeInfo, logger)
	if status != nil {
		return status
	}
	// Pod 没有用户 claim；即便有扩展资源 claim，也已由 device plugin 满足，插件无事可做。
	if nodeExtendedResourceClaim == nil && state.claims.noUserClaim() {
		return nil
	}

	var unavailableClaims []int
	for index, claim := range state.claims.all() {
		logger.V(10).Info("filtering based on resource claims of the pod", "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaim", klog.KObj(claim))

		// 只有 claim 已分配时才会设置该节点选择器。
		if nodeSelector := state.informationsForClaim[index].availableOnNodes; nodeSelector != nil && !nodeSelector.Match(node) {
			logger.V(5).Info("allocation's node selector does not match", "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaim", klog.KObj(claim))
			unavailableClaims = append(unavailableClaims, index)
			continue
		}

		// 已分配的 claim 若需要 DRAOptionalNodeOperations，节点必须声明支持该特性。
		if allocationResultRequiresDRAOptionalNodeOperations(claim.Status.Allocation) && !slices.Contains(node.Status.DeclaredFeatures, draoptionalnodeoperations.DRAOptionalNodeOperationsFeatureGate) {
			logger.V(5).Info("allocation's device requires DRAOptionalNodeOperations feature, but node lacks it", "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaim", klog.KObj(claim))
			unavailableClaims = append(unavailableClaims, index)
			continue
		}

		// 已分配的 claim 要检查是否就绪可绑定。
		// 若尚未就绪（ready=false 且无错误）但已超时，或绑定判定失败（err 非空），
		// PostFilter 应考虑取消该 claim 的分配，以便尝试其他设备。
		if claim.Status.Allocation != nil && pl.fts.EnableDRADeviceBindingConditions && pl.fts.EnableDRAResourceClaimDeviceStatus {
			ready, err := pl.isClaimReadyForBinding(claim)
			if err != nil {
				logger.V(5).Info("Claim failed binding conditions check", "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaim", klog.KObj(claim), "err", err)
				unavailableClaims = append(unavailableClaims, index)
			} else if !ready && pl.isClaimTimeout(claim) {
				logger.V(5).Info("Claim timed out waiting for binding conditions", "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaim", klog.KObj(claim))
				unavailableClaims = append(unavailableClaims, index)
			}
		}

		// 检查是否是已分配的节点可分配资源 claim。
		if pl.fts.EnableDRANodeAllocatableResources {
			allocation := claim.Status.Allocation
			if allocation == nil {
				// 检查本周期前一个 Pod 留下的 pending 分配。
				allocation = state.informationsForClaim[index].allocation
			}
			if allocation == nil {
				// 既未分配也没有 pending 分配，跳过。
				continue
			}
			status := pl.validateNodeAllocatableDRAClaimSharing(pod, claim, state, podGroupState)
			if status != nil {
				return status
			}
		}
	}
	// 用分配器检查该节点，并缓存结果以备该节点最终被选中。
	var allocations []resourceapi.AllocationResult
	var nodeAllocatableClaimStatus []v1.NodeAllocatableResourceClaimStatus
	allocationsMap := make(map[types.UID]*resourceapi.AllocationResult)
	if state.allocator != nil {
		allocCtx := ctx
		if loggerV := logger.V(5); loggerV.Enabled() {
			allocCtx = klog.NewContext(allocCtx, klog.LoggerWithValues(logger, "node", klog.KObj(node)))
		}

		// 特性开启且配置了超时时间时，为分配操作加上超时限制。
		if pl.fts.EnableDRASchedulerFilterTimeout && pl.filterTimeout > 0 {
			c, cancel := context.WithTimeout(allocCtx, pl.filterTimeout)
			defer cancel()
			allocCtx = c
		}

		// 确定哪些 claim 需要分配，并把扩展资源的特殊 claim 换成与当前节点匹配的那个。
		claimsToAllocate := make([]*resourceapi.ResourceClaim, 0, state.claims.len())
		// pendingResult 保存前一个 Pod 在 Reserve 阶段创建、但尚未被 PreBind
		// 写入 status 的 pending 分配。
		var pendingResult []resourceapi.AllocationResult
		extendedResourceClaim := state.claims.extendedResourceClaim()
		for index, claim := range state.claims.toAllocate() {
			if claim == extendedResourceClaim && nodeExtendedResourceClaim != nil {
				claim = nodeExtendedResourceClaim
			}
			if pendingAlloc := state.informationsForClaim[index].allocation; pendingAlloc != nil {
				if allocationResultRequiresDRAOptionalNodeOperations(pendingAlloc) && !slices.Contains(node.Status.DeclaredFeatures, draoptionalnodeoperations.DRAOptionalNodeOperationsFeatureGate) {
					logger.V(5).Info("allocation's device requires DRAOptionalNodeOperations feature, but node lacks it", "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaim", klog.KObj(claim))
					return statusUnschedulable(logger, fmt.Sprintf("resource claim %s allocation requires DRAOptionalNodeOperations feature on the node", klog.KObj(claim)), "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaim", klog.KObj(claim))
				}
				pendingResult = append(pendingResult, *pendingAlloc)
				allocationsMap[claim.UID] = state.informationsForClaim[index].allocation
				continue
			}
			claimsToAllocate = append(claimsToAllocate, claim)
		}
		allocationResult, err := state.allocator.Allocate(allocCtx, node, claimsToAllocate)
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			// 超时可能是暂时的，返回 Error 让 Pod 走退避重试，
			// 而不是留在不可调度队列里等集群事件。
			// 超时可能由该节点的 ResourceSlice 引起，带上节点名便于排查。
			return statusError(logger, fmt.Errorf("node %s: timed out trying to allocate devices", node.Name), "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaims", klog.KObjSlice(claimsToAllocate))
		case errors.Is(err, structured.ErrFailedAllocationOnNode):
			// 非致命错误：其他节点仍可能分配成功，只有所有节点都失败时才对外报错。
			return statusUnschedulable(logger, err.Error(), "pod", klog.KObj(pod), "node", klog.KObj(node))
		case ctx.Err() != nil:
			return statusUnschedulable(logger, fmt.Sprintf("asked by caller to stop allocating devices: %v", context.Cause(ctx)), "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaims", klog.KObjSlice(claimsToAllocate))
		case err != nil:
			// 这种错误只应来自 claim 或设备类本身有问题，返回 Error 中止调度。
			//
			// 这会触发重试。理想情况是既标记不可调度又中止调度，这样只有
			// CEL 表达式有问题的 claim/类被更新时才重新调度。但两者不可兼得；
			// 由于这种错误不该常见，直接中止比「先返回不可调度、记住错误、
			// 之后再择机抛出」的复杂方案更好。
			return statusError(logger, err, "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaims", klog.KObjSlice(claimsToAllocate))
		}
		// 保险起见确认长度一致；实际上分配是全有或全无的。
		if len(allocationResult) != len(claimsToAllocate) {
			return statusUnschedulable(logger, "cannot allocate all claims", "pod", klog.KObj(pod), "node", klog.KObj(node), "resourceclaims", klog.KObjSlice(claimsToAllocate))
		}

		// Reserve 阶段会用到这些信息。
		allocations = append(allocationResult, pendingResult...)

		// allocations 由 allocationResult 后接 pendingResult 组成，
		// 前 len(claimsToAllocate) 个元素与 claimsToAllocate 一一对应。
		for i, claim := range claimsToAllocate {
			allocationsMap[claim.UID] = &allocations[i]
		}
	}

	if pl.fts.EnableDRANodeAllocatableResources {
		var status *fwk.Status
		nodeAllocatableClaimStatus, status = pl.calculateAndCheckNodeAllocatableResources(ctx, state, pod, nodeInfo, allocationsMap)
		if status != nil {
			return status
		}
	}

	// 写状态前持锁：Filter 是并行执行的，这些字段会被并发访问。
	if state.allocator != nil || len(unavailableClaims) > 0 || len(nodeAllocatableClaimStatus) > 0 {
		state.mutex.Lock()
		defer state.mutex.Unlock()
	}

	if len(unavailableClaims) > 0 {
		// 记录所有不可用的 claim，供 PostFilter 使用；写前必须持锁。

		if state.unavailableClaims == nil {
			state.unavailableClaims = sets.New[int]()
		}

		for _, index := range unavailableClaims {
			state.unavailableClaims.Insert(index)
		}

		return statusUnschedulable(logger, "resourceclaim not available on the node", "pod", klog.KObj(pod))
	}

	if state.allocator != nil || len(nodeAllocatableClaimStatus) > 0 {
		state.nodeAllocations[node.Name] = nodeAllocation{
			allocationResults:                    allocations,
			extendedResourceClaim:                nodeExtendedResourceClaim,
			containerResourceRequestMappings:     containerResourceRequestMappings,
			nodeAllocatableResourceClaimStatuses: nodeAllocatableClaimStatus,
		}
	}

	return nil
}

// PostFilter 检查是否有已分配的 claim 可以取消分配，从而让 Pod 变得可调度。
// 若有则选一个请求取消。仅在过滤阶段找不到合适节点时才会调用。
func (pl *DynamicResources) PostFilter(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, _ fwk.NodeToStatusReader) (*fwk.PostFilterResult, *fwk.Status) {
	logger := klog.FromContext(ctx)
	if !pl.enabled {
		logger.V(5).Info("Nothing to do in PostFilter, plugin disabled", "pod", klog.KObj(pod))
		return nil, fwk.NewStatus(fwk.Unschedulable)
	}
	if pl.fts.EnableInPlacePodVerticalScalingSchedulerPreemption && resource.IsPodResizeDeferred(pod) {
		return nil, fwk.NewStatus(fwk.Unschedulable)
	}

	state, err := getStateData(cs)
	if err != nil {
		return nil, statusError(logger, err)
	}
	// Pod 没有任何 resource claim 时无需进一步处理，走快速路径省掉无用计算。
	if state.claims.empty() {
		logger.V(5).Info("No new claims to deallocate", "pod", klog.KObj(pod))
		return nil, fwk.NewStatus(fwk.Unschedulable)
	}

	_, status := pl.deallocateOrDeletePodClaims(ctx, pod, state)
	return nil, status
}

// deallocateOrDeletePodClaims 尝试取消或删除 Pod 的一个不可用 claim。
// 返回值的 bool 表示是否对 claim 做了处理。
func (pl *DynamicResources) deallocateOrDeletePodClaims(ctx context.Context, pod *v1.Pod, state *stateData) (bool, *fwk.Status) {
	logger := klog.FromContext(ctx)

	extendedResourceClaim := state.claims.extendedResourceClaim()
	// 遍历 map 的顺序是随机的，这里正是想要的：没有更好的启发式，就随机挑一个 claim。
	for index := range state.unavailableClaims {
		claim := state.claims.get(index)
		if claim == extendedResourceClaim {
			if extendedResourceClaim != nil && !isSpecialClaimName(extendedResourceClaim.Name) {
				break
			}
			continue
		}

		if len(claim.Status.ReservedFor) == 0 ||
			(len(claim.Status.ReservedFor) == 1 && claim.Status.ReservedFor[0].UID == pod.UID) {
			claim := claim.DeepCopy()
			claim.Status.ReservedFor = nil
			claim.Status.Allocation = nil
			claim.Status.Devices = nil
			logger.V(5).Info("Deallocation of ResourceClaim", "pod", klog.KObj(pod), "resourceclaim", klog.KObj(claim))
			if _, err := pl.clientset.ResourceV1().ResourceClaims(claim.Namespace).UpdateStatus(ctx, claim, metav1.UpdateOptions{}); err != nil {
				return true, statusError(logger, err)
			}
			return true, fwk.NewStatus(fwk.Unschedulable, "deallocation of ResourceClaim completed")
		}
	}

	if extendedResourceClaim != nil && !isSpecialClaimName(extendedResourceClaim.Name) {
		// DRA 扩展资源的特殊 claim 若在上个调度周期已被预留或分配，就应删除。
		extendedResourceClaim := extendedResourceClaim.DeepCopy()
		if err := pl.deleteClaim(ctx, extendedResourceClaim); err != nil {
			return true, statusError(logger, err)
		}
		return true, fwk.NewStatus(fwk.Unschedulable, "deletion of ResourceClaim completed")
	}

	return false, fwk.NewStatus(fwk.Unschedulable)
}

// PodGroupPostFilter 检查是否有已分配的 claim 可以取消分配/删除/解除预留，
// 从而让 PodGroup 变得可调度。仅在 PodGroup 调度周期找不到放置位置时才调用。
func (pl *DynamicResources) PodGroupPostFilter(
	ctx context.Context,
	state fwk.PodGroupCycleState,
	pgInfo fwk.PodGroupInfo,
	_ fwk.PodGroupSchedulingFunc,
) (*fwk.PodGroupPostFilterResult, *fwk.Status) {
	logger := klog.FromContext(ctx)
	if !pl.enabled {
		logger.V(5).Info("Nothing to do in PodGroupPostFilter, plugin disabled", "podGroup", pgInfo.GetName())
		return nil, fwk.NewStatus(fwk.Unschedulable)
	}

	if pgInfo.GetType() == fwk.CompositePodGroupKeyType {
		logger.V(5).Info("DRA PodGroupPostFilter is not supporting CompositePodGroups", "podGroup", pgInfo.GetName())
		return nil, fwk.NewStatus(fwk.Unschedulable)
	}

	if pl.fts.EnableTopologyAwareWorkloadScheduling && pgInfo.GetPodGroup() != nil && pgInfo.GetPodGroup().Spec.SchedulingConstraints != nil {
		logger.V(5).Info("DRA PodGroupPostFilter is not supporting topology-aware workloads", "podGroup", pgInfo.GetName())
		return nil, fwk.NewStatus(fwk.Unschedulable)
	}

	podGroupState, err := getPodGroupStateDataFromPodGroupCycleState(state)
	if err != nil {
		return nil, statusError(logger, err)
	}

	unreservePodGroup := false
	// 1. 先处理 Pod 级别 claim 的取消分配/删除，
	// 相当于对 PodGroup 中每个 Pod 分别执行 PostFilter。
	if podGroupState != nil && len(podGroupState.podsStateData) > 0 {
		for _, pod := range pgInfo.GetUnscheduledPods() {
			podState := podGroupState.podsStateData[types.NamespacedName{Namespace: pod.GetNamespace(), Name: pod.GetName()}]
			if podState == nil {
				continue
			}
			foundClaim, status := pl.deallocateOrDeletePodClaims(ctx, pod, podState)
			if status.IsError() {
				return nil, status
			}
			if !foundClaim {
				// 走到这里说明 PodGroup 中还有 Pod 没能调度成功。
				unreservePodGroup = true
			}
		}
	}

	// 2. 若至少有一个带 claim 的 Pod 没能取消分配/删除，尝试解除 PodGroup claim 的预留。
	if unreservePodGroup && pl.fts.EnableDRAWorkloadResourceClaims && len(pgInfo.GetUnscheduledPods()) > 0 {
		// 若上个周期绑定当前分配失败，为了让调度器尝试寻找新设备：
		// 取消所有节点上都不可用的 PodGroup 级 claim，并从其 status.reservedFor 中移除 PodGroup。
		// 注意 Pod 数可能超过 PodGroup 的 minCount；只要还没有 assume 或已分配的 Pod，
		// 对上个周期做这种清理就是安全的。
		status := pl.deallocatePodGroupClaims(ctx, podGroupState, pgInfo.GetUnscheduledPods()[0])
		if status != nil {
			return nil, status
		}
	}

	return nil, fwk.NewStatus(fwk.Unschedulable, "deallocation and deletion of ResourceClaims completed")
}

func (pl *DynamicResources) deallocatePodGroupClaims(ctx context.Context, state *podGroupStateData, pod *v1.Pod) *fwk.Status {
	if pod.Spec.SchedulingGroup == nil || pod.Spec.SchedulingGroup.PodGroupName == nil {
		return nil
	}

	logger := klog.FromContext(ctx)

	// PodGroupState lister 基于实时缓存，不包含本轮 PodGroup 调度周期内 assume 的 Pod，
	// 只反映周期之前或之外发生的情况。这里用它确认没有其他已 assume/已分配的 Pod
	// 会使用该 ResourceClaim。
	podGroupState, err := pl.fh.PodGroupManager().PodGroupStates().Get(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName)
	if err != nil {
		return statusError(logger, err)
	}
	// 处于同步 PodGroup 调度周期中：若 PodGroup 当前处于非活跃状态，它在本次周期内
	// 不会再变活跃，因此可以安全地取消其 claim 的分配。
	//
	// 若状态尚未更新、误显示活跃，后续调度周期最终也会读到更新后的状态并取消分配。
	if podGroupState.ScheduledPodsCount() > 0 {
		return nil
	}
	podGroup, err := pl.getPodGroupSnapshot(pod)
	if err != nil {
		return statusError(logger, err)
	}

	// 遍历 map 顺序随机，这里正想随机挑一个 claim，因为没有更好的启发式。
	for _, podState := range state.podsStateData {
		for index := range podState.unavailableClaims {
			claim := podState.claims.get(index)

			reservedForNobody := len(claim.Status.ReservedFor) == 0
			reservedForOnlyThisPodGroup := podGroup != nil &&
				len(claim.Status.ReservedFor) == 1 &&
				claim.Status.ReservedFor[0].UID == podGroup.UID

			if reservedForNobody || reservedForOnlyThisPodGroup {
				claim := claim.DeepCopy()
				claim.Status.ReservedFor = nil
				claim.Status.Allocation = nil
				claim.Status.Devices = nil
				logger.V(5).Info("Deallocation of PodGroup ResourceClaim", "pod", klog.KObj(pod), "podgroup", klog.KObj(podGroup), "resourceclaim", klog.KObj(claim))
				if _, err := pl.clientset.ResourceV1().ResourceClaims(claim.Namespace).UpdateStatus(ctx, claim, metav1.UpdateOptions{}); err != nil {
					return statusError(logger, err)
				}
				return fwk.NewStatus(fwk.Unschedulable, "deallocation of PodGroup ResourceClaim completed")
			}
		}
	}

	for _, podGroupClaim := range podGroup.Spec.ResourceClaims {
		claimName, mustCheckOwner, err := resourceclaim.NameFromPodGroup(podGroup, &podGroupClaim)
		if err != nil {
			return statusError(logger, err)
		}
		if claimName == nil {
			continue
		}
		claim, err := pl.draManager.ResourceClaims().Get(podGroup.Namespace, *claimName)
		if err != nil {
			return statusError(logger, err)
		}
		if mustCheckOwner {
			if err := resourceclaim.IsForPodGroup(podGroup, claim); err != nil {
				continue
			}
		}

		if claim.Status.Allocation != nil {
			if resourceclaim.IsReservedForPod(pod, claim, pl.fts.EnableDRAWorkloadResourceClaims) {
				// 从 ReservedFor 中移除 PodGroup。用 strategic-merge-patch 是因为它能在
				// 不持有最新版本 ResourceClaim 的情况下删除单个条目。
				patch := fmt.Sprintf(`{"metadata": {"uid": %q}, "status": { "reservedFor": [ {"$patch": "delete", "uid": %q} ] }}`,
					claim.UID,
					podGroup.UID,
				)
				logger.V(5).Info("postfilter unreserved claim", "resourceclaim", klog.KObj(claim), "pod", klog.KObj(pod), "podgroup", klog.KObj(podGroup))
				claim, err := pl.clientset.ResourceV1().ResourceClaims(claim.Namespace).Patch(ctx, claim.Name, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{}, "status")
				if err != nil {
					// Pod 重试调度时会再次走到这里。
					logger.Error(err, "postfilter unreserving claim", "resourceclaim", klog.KObj(claim))
					continue
				} else {
					return statusUnschedulable(logger, "ResourceClaim unreserved for PodGroup")
				}
			}
		}
	}

	return nil
}

// Score 在 score 扩展点调用，按优先备选列表给节点打分。
func (pl *DynamicResources) Score(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	if !pl.enabled || !pl.fts.EnableDRAPrioritizedList {
		return 0, nil
	}
	logger := klog.FromContext(ctx)

	state, err := getStateData(cs)
	if err != nil {
		return 0, statusError(logger, err)
	}

	// 没有 claim 就无需打分。
	if state.claims.empty() {
		return 0, nil
	}

	allocations, found := state.nodeAllocations[nodeInfo.Node().Name]
	if !found {
		return 0, nil
	}

	score, err := computeScore(state.claims.all(), allocations)
	if err != nil {
		return 0, statusError(logger, err)
	}
	return score, nil
}

// computeScore 根据各 claim 在该节点上的分配结果计算优先列表得分。
func computeScore(iterator iter.Seq2[int, *resourceapi.ResourceClaim], allocations nodeAllocation) (int64, error) {
	var score int64
	unallocatedIndex := 0
	for _, claim := range iterator {
		// 收集所有已分配子请求的名字。
		allocatedSubRequests := sets.New[string]()

		var allocation *resourceapi.AllocationResult
		// claim 的分配结果可能来自两处：
		// 1. 上个周期分配的（如 PodGroup claim），已在 claim.Status.Allocation 中；
		// 2. 本周期 Filter 阶段分配的，在 allocations.allocationResults 中。
		// 这里遍历所有 claim，必须同时检查两处，并为本周期新分配的 claim 单独维护索引。
		if claim.Status.Allocation != nil {
			allocation = claim.Status.Allocation
		} else {
			if unallocatedIndex >= len(allocations.allocationResults) {
				return 0, fmt.Errorf("number of allocations %d is smaller than number of claims needing allocation", len(allocations.allocationResults))
			}
			allocation = &allocations.allocationResults[unallocatedIndex]
			unallocatedIndex++
		}

		for _, res := range allocation.Devices.Results {
			request := res.Request
			if resourceclaim.IsSubRequestRef(request) {
				allocatedSubRequests.Insert(request)
			}
		}

		for _, req := range claim.Spec.Devices.Requests {
			if req.Exactly != nil {
				continue
			}
			for i, subReq := range req.FirstAvailable {
				subRequestRef := resourceclaim.CreateSubRequestRef(req.Name, subReq.Name)
				if allocatedSubRequests.Has(subRequestRef) {
					score += int64(resourceapi.FirstAvailableDeviceRequestMaxSize - i)
				}
			}
		}
	}
	return score, nil
}

func (pl *DynamicResources) ScoreExtensions() fwk.ScoreExtensions {
	return pl
}

func (pl *DynamicResources) NormalizeScore(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, scores fwk.NodeScoreList) *fwk.Status {
	if !pl.enabled {
		return nil
	}
	return helper.DefaultNormalizeScore(fwk.MaxNodeScore, false, scores)
}

// Reserve 为 Pod 预留 claim：把 Filter 阶段算出的分配结果标记为 pending，
// 真正的预留留到绑定阶段落实。
func (pl *DynamicResources) Reserve(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeName string) (status *fwk.Status) {
	if !pl.enabled {
		return nil
	}
	state, err := getStateData(cs)
	if err != nil {
		return statusError(klog.FromContext(ctx), err)
	}
	if state.claims.empty() {
		return nil
	}

	logger := klog.FromContext(ctx)

	podGroupState, err := getPodGroupStateData(cs)
	if err != nil {
		return statusError(logger, err)
	}

	numClaimsWithAllocator := 0
	for _, claim := range state.claims.all() {
		if claim.Status.Allocation != nil {
			// 已分配但可能尚未预留。PreFilter 已确认本 Pod 可以预留该 claim。
			// 这里不真的更新 ResourceClaim status 做预留，而是假定预留会成功，
			// 到绑定阶段才落实；若届时失败，说明被别的 Pod 抢先，重新调度即可。
			continue
		}

		numClaimsWithAllocator++
	}

	if numClaimsWithAllocator == 0 {
		// 无需分配任何 claim。
		return nil
	}
	extendedResourceClaim := state.claims.extendedResourceClaim()
	numClaimsToAllocate := 0
	needToAllocateUserClaims := false
	for _, claim := range state.claims.toAllocate() {
		numClaimsToAllocate++
		if claim != extendedResourceClaim {
			needToAllocateUserClaims = true
		}
	}

	// 准备调度器负责处理的 claim 的分配结果。
	if state.allocator != nil {
		// 该节点的分配结果与待分配 claim 一一对应。
		allocations, ok := state.nodeAllocations[nodeName]
		if !ok || len(allocations.allocationResults) == 0 {
			// 只有待分配项仅为 DRA 扩展资源的特殊 claim 模板、而该资源由节点上的
			// device plugin 满足时才会走到这里（此时没有 DRA 分配）。
			if !needToAllocateUserClaims {
				return nil
			}

			// 前面已检查该节点可用，到这里却没有分配结果，属于内部错误。
			return statusError(logger, errors.New("claim allocation not found for node"))
		}

		// 完整性检查：每个 pending claim 都有对应结果？
		if len(allocations.allocationResults) != numClaimsToAllocate ||
			len(allocations.allocationResults) != numClaimsWithAllocator {
			return statusError(logger, fmt.Errorf("internal error, have %d allocations, %d claims to allocate, want %d claims", len(allocations.allocationResults), numClaimsToAllocate, numClaimsWithAllocator))
		}

		allocIndex := 0
		for index, claim := range state.claims.toAllocate() {
			// 返回的 index 是底层 claim store 中的原始下标，可能并不连续。
			allocation := &allocations.allocationResults[allocIndex]
			state.informationsForClaim[index].allocation = allocation

			if claim == extendedResourceClaim {
				// 把 DRA 扩展资源的特殊 claim 模板替换成真正实例化的 claim。
				claim = allocations.extendedResourceClaim
			}

			// 严格来说只需要存分配结果就够，但保存完整对象更便于调试、测试和分配器使用。
			claim = claim.DeepCopy()
			if !slices.Contains(claim.Finalizers, resourceapi.Finalizer) {
				claim.Finalizers = append(claim.Finalizers, resourceapi.Finalizer)
			}
			claim.Status.Allocation = allocation
			err := pl.draManager.ResourceClaims().SignalClaimPendingAllocation(claim.UID, claim)
			if err != nil {
				return statusError(logger, fmt.Errorf("internal error, couldn't signal allocation for claim %s: %w", claim.Name, err))
			}
			logger.V(5).Info("Reserved resource in allocation result", "claim", klog.KObj(claim), "uid", claim.UID, "resourceVersion", claim.ResourceVersion, "allocation", klog.Format(allocation))
			allocIndex++

			if podGroupState != nil {
				if _, ok := podGroupState.pendingAllocations[claim.UID]; !ok {
					podGroupState.pendingAllocations[claim.UID] = sets.New[types.UID]()
				}
				podGroupState.pendingAllocations[claim.UID].Insert(pod.UID)
			}
		}
	}

	return nil
}

// Unreserve 清除所有 claim 的 ReservedFor 字段。
// 该操作幂等：找不到给定 Pod 的状态时什么都不做。
func (pl *DynamicResources) Unreserve(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeName string) {
	if !pl.enabled {
		return
	}
	state, err := getStateData(cs)
	if err != nil {
		return
	}
	if state.claims.empty() {
		return
	}
	podGroupState, err := getPodGroupStateData(cs)
	if err != nil {
		return
	}

	logger := klog.FromContext(ctx)

	// 先处理用户 claim，扩展资源 claim 如有则在后面处理。
	for _, claim := range state.claims.allUserClaims() {
		// 分配处于 in-flight 时，若已无 Pod 需要这个 pending 分配，就可以撤销。
		// 撤销后需要把 assume cache 中的 claim 对象恢复原状。
		// 只有共享该分配的最后一个 Pod 完成 Bound 或 Unreserve 后，分配才会被移除。
		if deleted := pl.draManager.ResourceClaims().MaybeRemoveClaimPendingAllocation(claim.UID, false); deleted {
			logger.V(5).Info("Released resource in allocation result", "claim", klog.KObj(claim), "uid", claim.UID, "resourceVersion", claim.ResourceVersion, "allocation", klog.Format(claim.Status.Allocation))
			pl.draManager.ResourceClaims().AssumedClaimRestore(claim.Namespace, claim.Name)

			// 若当前正在异步绑定 PodGroup 的 Pod，则无需更新 pendingAllocations 集合。
			// 新的 PodGroup 调度周期会从空集合开始，不会共享本周期创建的 pending 分配，
			// 直到所有共享该分配的 Pod 完成 Unreserve 并在别的周期重新 Reserve。
			if podGroupState != nil {
				delete(podGroupState.pendingAllocations, claim.UID)
			}
		}

		// 为 PodGroup 预留的 claim 不在这里处理，因为组内其他 Pod 可能还需要它。
		// 若 PodGroup 不可调度（在 PostFilter 中判定）且尚未被使用，下个周期会
		// 解除预留并在 API server 中取消分配。
		if claim.Status.Allocation != nil &&
			resourceclaim.IsReservedForPod(pod, claim, false /* acceptPodGroupReservation */) {
			// Remove pod from ReservedFor. A strategic-merge-patch is used
			// because that allows removing an individual entry without having
			// the latest ResourceClaim.
			patch := fmt.Sprintf(`{"metadata": {"uid": %q}, "status": { "reservedFor": [ {"$patch": "delete", "uid": %q} ] }}`,
				claim.UID,
				pod.UID,
			)
			logger.V(5).Info("unreserve", "resourceclaim", klog.KObj(claim), "pod", klog.KObj(pod))
			claim, err := pl.clientset.ResourceV1().ResourceClaims(claim.Namespace).Patch(ctx, claim.Name, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{}, "status")
			if err != nil {
				// Pod 重试调度时会再次走到这里。
				logger.Error(err, "unreserve", "resourceclaim", klog.KObj(claim))
			}
		}
	}
	pl.unreserveExtendedResourceClaim(ctx, pod, state)

	if pl.fts.EnableDRANodeAllocatableResources {
		nodeAllocations, ok := state.nodeAllocations[nodeName]
		if ok && len(nodeAllocations.nodeAllocatableResourceClaimStatuses) > 0 {
			pl.clearNodeAllocatableResourceClaimStatus(ctx, pod)
		}
	}
}

// PreBind 在确定 Pod 应绑定到该节点后，于独立 goroutine 中调用。
// 由于 Reserve 并未真正预留 claim，这里才落实预留；对内建 controller 管理的 claim，
// 这里同时处理分配。
//
// 任何失败都会返回错误，Pod 进入退避队列，调度器在错误处理中调用 Unreserve。
func (pl *DynamicResources) PreBind(ctx context.Context, cs fwk.CycleState, pod *v1.Pod, nodeName string) (retStatus *fwk.Status) {
	if !pl.enabled {
		return nil
	}
	state, err := getStateData(cs)
	if err != nil {
		return statusError(klog.FromContext(ctx), err)
	}
	if state.claims.empty() {
		return nil
	}

	logger := klog.FromContext(ctx)

	podGroupState, err := getPodGroupStateData(cs)
	if err != nil {
		return statusError(logger, err)
	}

	for index, claim := range state.claims.all() {
		if !resourceclaim.IsReservedForPod(pod, claim, pl.fts.EnableDRAWorkloadResourceClaims) {
			claim, successCleanup, err := pl.bindClaim(ctx, state, podGroupState, index, pod, nodeName)
			if err != nil {
				return statusError(logger, err)
			}
			defer func() {
				if retStatus.IsSuccess() && successCleanup != nil {
					successCleanup()
				}
			}()
			// Updated here such that Unreserve can work with patched claim.
			state.claims.set(index, claim)
		}
	}

	if pl.fts.EnableDRANodeAllocatableResources {
		nodeAllocations, ok := state.nodeAllocations[nodeName]
		if ok && len(nodeAllocations.nodeAllocatableResourceClaimStatuses) > 0 {
			if status := pl.patchNodeAllocatableResourceClaimStatus(ctx, pod, nodeAllocations.nodeAllocatableResourceClaimStatuses); status != nil {
				return status
			}
		}
	}

	if !pl.fts.EnableDRADeviceBindingConditions || !pl.fts.EnableDRAResourceClaimDeviceStatus {
		// 不需要等待绑定条件时直接返回：claim 已为 Pod 预留，可以继续绑定。
		return nil
	}

	// 判断是否有设备需要等待绑定条件就绪。
	needToWait := hasBindingConditions(state)

	// 没有设备需要准备时直接返回。
	if !needToWait {
		return nil
	}

	// 等待设备就绪期间记录事件和耗时指标。
	pl.fh.EventRecorder().WithLogger(logger).Eventf(pod, nil, v1.EventTypeNormal, "BindingConditionsPending", "Scheduling", "waiting for binding conditions for device on node %s", nodeName)
	start := time.Now()
	err = wait.PollUntilContextTimeout(ctx, 5*time.Second, pl.bindingTimeout, true,
		func(ctx context.Context) (bool, error) {
			return pl.isPodReadyForBinding(state)
		})
	// GOAL: Calculate duration since start for metrics recording
	duration := time.Since(start)
	if errors.Is(err, context.DeadlineExceeded) {
		err = fmt.Errorf("%w: %w", ErrDeviceBindingTimeout, err)
	}

	// Determine status label based on error
	statusLabel := schedmetrics.BindingConditionsStatusSuccess
	switch {
	case err == nil:
		logger.V(5).Info("BindingConditions is met",
			"pod", klog.KObj(pod),
			"node", nodeName,
			"devices", bindingConditionsStatusFormatter{state, statusLabel},
		)
		// keep success
	case errors.Is(err, ErrDeviceBindingTimeout):
		statusLabel = schedmetrics.BindingConditionsStatusTimeout
	case errors.Is(err, ErrDeviceBindingFailed):
		statusLabel = schedmetrics.BindingConditionsStatusFailed
	default:
		statusLabel = schedmetrics.BindingConditionsStatusError
	}

	// Record metrics for binding conditions outcome (success/failure/timeout/error)
	profileLabel := pl.fh.ProfileName()
	for _, claim := range state.claims.all() {
		if claim.Status.Allocation == nil {
			continue
		}
		for _, res := range claim.Status.Allocation.Devices.Results {
			schedmetrics.DRABindingConditionsPreBindDuration.
				WithLabelValues(profileLabel, res.Driver, statusLabel).
				Observe(duration.Seconds())
		}

		// Count scheduling attempts that used devices with BindingConditions.
		for _, bcStatus := range listBindingConditionsStatus(claim) {
			schedmetrics.DRABindingConditionsAllocationsTotal.
				WithLabelValues(profileLabel, bcStatus.Driver, statusLabel).
				Inc()
		}
	}

	if err != nil {
		// 这里返回错误会触发新一轮调度尝试；下一轮 PreFilter 会检测到超时或错误并尝试恢复。
		return statusError(logger, err, "devices", formatBCStatusOneLine(getBindingConditionsStatusForStatus(state, statusLabel)))
	}

	// 能走到这里说明 claim 已成功为 Pod 预留，可以继续绑定。
	return nil
}

// PreBindPreFlight 在 PreBind 之前调用，判断 PreBind 是否需要为该 Pod 做事。
// 只检查 state.claims：有 claim 时插件才需要在 PreBind 处理。
func (pl *DynamicResources) PreBindPreFlight(ctx context.Context, cs fwk.CycleState, p *v1.Pod, nodeName string) (*fwk.PreBindPreFlightResult, *fwk.Status) {
	result := &fwk.PreBindPreFlightResult{AllowParallel: true}
	if !pl.enabled {
		return result, fwk.NewStatus(fwk.Skip)
	}
	state, err := getStateData(cs)
	if err != nil {
		return result, statusError(klog.FromContext(ctx), err)
	}
	if state.claims.empty() {
		return result, fwk.NewStatus(fwk.Skip)
	}
	return result, nil
}

// bindClaim gets called by PreBind for claim which is not reserved for the pod yet.
// It might not even be allocated. bindClaim then ensures that the allocation
// and reservation are recorded. This finishes the work started in Reserve.
// Returns the updated claim, a function which (if not nil) should run when the
// pod has been successfully bound, and an error if one occurred.
func (pl *DynamicResources) bindClaim(ctx context.Context, state *stateData, podGroupState *podGroupStateData, index int, pod *v1.Pod, nodeName string) (*resourceapi.ResourceClaim, func(), error) {
	logger := klog.FromContext(ctx)
	claim := state.claims.get(index)
	binding := state.claims.getBinding(index, pod)
	allocation := state.informationsForClaim[index].allocation
	isExtendedResourceClaim := false
	if claim == state.claims.extendedResourceClaim() {
		// extended resource requests satisfied by device plugin
		if allocation == nil && claim.Spec.Devices.Requests == nil {
			return claim, nil, nil
		}
		isExtendedResourceClaim = true
	}
	claimUIDs := []types.UID{claim.UID}
	resourceClaimModified := false
	var resourceVersion string
	defer func() {
		// Creating the claim may have failed.
		claimUID := types.UID("")
		if claim != nil {
			resourceVersion = claim.ResourceVersion
			claimUID = claim.UID
		}

		// 调度器此前在处理分配，现在无论成功还是失败都已结束。

		if resourceClaimModified {
			if isExtendedResourceClaim {
				pl.waitForExtendedClaimInAssumeCache(ctx, claim)
			} else {
				// 这里可能失败，但只会因为并发删除或更新这类可接受的理由；此场景下不该发生。
				if err := pl.draManager.ResourceClaims().AssumeClaimAfterAPICall(claim); err != nil {
					logger.V(5).Info("Claim not stored in assume cache", "err", err, "claim", klog.KObj(claim), "uid", claimUID, "resourceVersion", resourceVersion)
				} else {
					logger.V(5).Info("Claim stored in assume cache", "claim", klog.KObj(claim), "uid", claimUID, "resourceVersion", resourceVersion)
				}
			}
		}
	}()

	// 这段清理逻辑推迟到 [DynamicResources.PreBind] 末尾、且确定返回成功时才执行：
	// 把已成功绑定的分配从 in-flight 中移除。
	prebindSuccessCleanup := func() {
		if allocation != nil {
			for _, claimUID := range claimUIDs {
				if deleted := pl.draManager.ResourceClaims().MaybeRemoveClaimPendingAllocation(claimUID, true); deleted {
					// 若正在异步绑定 PodGroup 的 Pod，则无需更新 pendingAllocations 集合。
					// 新周期从空集合开始，不会共享本周期的 pending 分配，
					// 直到所有共享该分配的 Pod 完成 Unreserve 并在别的周期重新 Reserve。
					if podGroupState != nil {
						delete(podGroupState.pendingAllocations, claim.UID)
					}
					logger.V(5).Info("Removed claim from in-flight claims", "claim", klog.KObj(claim), "uid", claimUID, "resourceVersion", resourceVersion, "allocation", klog.Format(allocation))
				}
			}
		}
	}

	// Create the special claim for extended resource backed by DRA
	if isExtendedResourceClaim && isSpecialClaimName(claim.Name) {
		var err error
		claim, err = pl.createExtendedResourceClaimInAPI(ctx, pod, nodeName, state)
		if err != nil {
			return nil, nil, err
		}

		resourceClaimModified = true
		// Track the actual extended ResourceClaim from now.
		// Relevant if we need to delete again in Unreserve.
		if err := state.claims.updateExtendedResourceClaim(claim); err != nil {
			return nil, nil, fmt.Errorf("internal error: update extended ResourceClaim: %w", err)
		}
	}

	logger.V(5).Info("preparing claim status update", "claim", klog.KObj(state.claims.get(index)), "allocation", klog.Format(allocation))

	// 无害的并发更新可能导致 ResourceVersion 冲突，此时取最新 claim 重试。
	refreshClaim := false
	claim = claim.DeepCopy()
	retryErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if refreshClaim {
			updatedClaim, err := pl.clientset.ResourceV1().ResourceClaims(claim.Namespace).Get(ctx, claim.Name, metav1.GetOptions{})
			if err != nil {
				return fmt.Errorf("get updated claim %s after conflict: %w", klog.KObj(claim), err)
			}
			logger.V(5).Info("retrying update after conflict", "claim", klog.KObj(claim))
			claim = updatedClaim
		} else {
			// All future retries must get a new claim first.
			refreshClaim = true
		}

		if claim.DeletionTimestamp != nil {
			return fmt.Errorf("claim %s got deleted in the meantime", klog.KObj(claim))
		}

		// 是否需要把 Reserve 中的分配结果写回？
		if allocation != nil {
			if claim.Status.Allocation != nil {
				return fmt.Errorf("claim %s got allocated elsewhere in the meantime", klog.KObj(claim))
			}

			// finalizer 必须通过普通 Update 添加。
			// 若之前被中断过，它可能已经存在，那就直接继续。
			if !slices.Contains(claim.Finalizers, resourceapi.Finalizer) {
				claim.Finalizers = append(claim.Finalizers, resourceapi.Finalizer)
				updatedClaim, err := pl.clientset.ResourceV1().ResourceClaims(claim.Namespace).Update(ctx, claim, metav1.UpdateOptions{})
				if err != nil {
					return fmt.Errorf("add finalizer to claim %s: %w", klog.KObj(claim), err)
				}
				claim = updatedClaim
			}
			claim.Status.Allocation = allocation
		}

		// 无需预先检查条件，直接尝试添加 Pod：条件不满足时 apiserver
		// 会用非冲突错误告知我们。
		claim.Status.ReservedFor = append(claim.Status.ReservedFor, binding)
		if pl.fts.EnableDRADeviceBindingConditions &&
			pl.fts.EnableDRAResourceClaimDeviceStatus {
			// 待 feature gate 检查移除后，这段代码可并入上面 `if allocation != nil` 的 else 分支。
			// 目前先放在这里，避免改变正常行为。
			if claim.Status.Allocation == nil {
				return fmt.Errorf("claim %s got deallocated elsewhere in the meantime", klog.KObj(claim))
			}
			if claim.Status.Allocation.AllocationTimestamp == nil {
				claim.Status.Allocation.AllocationTimestamp = &metav1.Time{Time: time.Now()}
			}
		}
		updatedClaim, err := pl.clientset.ResourceV1().ResourceClaims(claim.Namespace).UpdateStatus(ctx, claim, metav1.UpdateOptions{})
		if err != nil {
			if allocation != nil {
				return fmt.Errorf("add allocation and reservation to claim %s: %w", klog.KObj(claim), err)
			}
			return fmt.Errorf("add reservation to claim %s: %w", klog.KObj(claim), err)
		}
		claim = updatedClaim
		resourceClaimModified = true
		return nil
	})

	if retryErr != nil {
		return nil, nil, retryErr
	}

	logger.V(5).Info("reserved", "pod", klog.KObj(pod), "node", nodeName, "resourceclaim", klog.Format(claim))

	// Patch the pod status with the new information about the generated
	// special resource claim.
	if isExtendedResourceClaim {
		err := pl.patchPodExtendedResourceClaimStatus(ctx, pod, claim, nodeName, state)
		if err != nil {
			return nil, nil, err
		}
	}

	return claim, prebindSuccessCleanup, nil
}

// allocationResultRequiresDRAOptionalNodeOperations checks whether an allocation result
// has any allocated device that specifies SkipNodeOperations.
func allocationResultRequiresDRAOptionalNodeOperations(alloc *resourceapi.AllocationResult) bool {
	if alloc == nil {
		return false
	}
	for _, result := range alloc.Devices.Results {
		if len(result.SkipNodeOperations) > 0 {
			return true
		}
	}
	return false
}

// isClaimReadyForBinding checks whether a given resource claim is
// ready for binding.
// It returns an error if the claim is not ready for binding.
// It returns true if (and only if) all binding conditions are true,
// and no binding failure conditions are true,
// which includes the case that there are no binding conditions.
func (pl *DynamicResources) isClaimReadyForBinding(claim *resourceapi.ResourceClaim) (bool, error) {
	if claim.Status.Allocation == nil {
		return false, nil
	}
	for _, deviceRequest := range claim.Status.Allocation.Devices.Results {
		if len(deviceRequest.BindingConditions) == 0 {
			continue
		}
		deviceStatus := getAllocatedDeviceStatus(claim, &deviceRequest)
		if deviceStatus == nil {
			return false, nil
		}
		for _, cond := range deviceRequest.BindingFailureConditions {
			failedCond := apimeta.FindStatusCondition(deviceStatus.Conditions, cond)
			if failedCond != nil && failedCond.Status == metav1.ConditionTrue {
				return false, fmt.Errorf("%w: claim=%s, reason=%s, message=%q",
					ErrDeviceBindingFailed,
					claim.Name,
					failedCond.Reason,
					failedCond.Message)
			}
		}
		for _, cond := range deviceRequest.BindingConditions {
			if !apimeta.IsStatusConditionTrue(deviceStatus.Conditions, cond) {
				return false, nil
			}
		}
	}
	return true, nil
}

// isClaimTimeout checks whether a given resource claim has
// reached the binding timeout.
// It returns true if the binding timeout is reached.
// It returns false if the binding timeout is not reached.
func (pl *DynamicResources) isClaimTimeout(claim *resourceapi.ResourceClaim) bool {
	if !pl.fts.EnableDRADeviceBindingConditions || !pl.fts.EnableDRAResourceClaimDeviceStatus {
		return false
	}
	if claim.Status.Allocation == nil || claim.Status.Allocation.AllocationTimestamp == nil {
		return false
	}
	// check if the binding timeout is reached
	for _, deviceRequest := range claim.Status.Allocation.Devices.Results {
		if deviceRequest.BindingConditions == nil {
			continue
		}
		if claim.Status.Allocation.AllocationTimestamp.Add(pl.bindingTimeout).Before(time.Now()) {
			return true
		}
	}
	return false
}

// isPodReadyForBinding checks the binding status of devices within the given state claims.
// It returns true if (and only if) all binding conditions are true,
// and no binding failure conditions are true,
// which includes the case when there are no binding conditions.
// It returns an error if any binding failure condition is set.
func (pl *DynamicResources) isPodReadyForBinding(state *stateData) (bool, error) {
	for claimIndex, claim := range state.claims.all() {
		claim, err := pl.draManager.ResourceClaims().Get(claim.Namespace, claim.Name)
		if err != nil {
			return false, err
		}
		state.claims.set(claimIndex, claim)
		ready, err := pl.isClaimReadyForBinding(claim)
		if err != nil {
			return false, err
		}
		if !ready {
			if pl.isClaimTimeout(claim) {
				return false, fmt.Errorf("%w: claim=%s", ErrDeviceBindingTimeout, claim.Name)
			}
			return false, nil
		}
	}
	return true, nil
}

func (pl *DynamicResources) getPodGroupSnapshot(pod *v1.Pod) (*schedulingapi.PodGroup, error) {
	if !pl.fts.EnableDRAWorkloadResourceClaims ||
		pod.Spec.SchedulingGroup == nil || pod.Spec.SchedulingGroup.PodGroupName == nil {
		return nil, nil
	}
	return pl.fh.SnapshotSharedLister().PodGroups().Get(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName)
}

// hasBindingConditions checks whether any of the claims in the state
// has binding conditions.
// It returns true if at least one claim has binding conditions.
// It returns false if no claim has binding conditions.
func hasBindingConditions(state *stateData) bool {
	for _, claim := range state.claims.all() {
		if claim.Status.Allocation == nil {
			continue
		}
		for _, device := range claim.Status.Allocation.Devices.Results {
			if len(device.BindingConditions) > 0 {
				return true
			}
		}
	}
	return false
}

// statusUnschedulable ensures that there is a log message associated with the
// line where the status originated.
func statusUnschedulable(logger klog.Logger, reason string, kv ...interface{}) *fwk.Status {
	if loggerV := logger.V(5); loggerV.Enabled() {
		helper, loggerV := loggerV.WithCallStackHelper()
		helper()
		kv = append(kv, "reason", reason)
		// nolint: logcheck // warns because it cannot check key/values
		loggerV.Info("pod unschedulable", kv...)
	}
	return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, reason)
}

// statusError ensures that there is a log message associated with the
// line where the error originated.
func statusError(logger klog.Logger, err error, kv ...interface{}) *fwk.Status {
	if loggerV := logger.V(5); loggerV.Enabled() {
		helper, loggerV := loggerV.WithCallStackHelper()
		helper()
		// nolint: logcheck // warns because it cannot check key/values
		loggerV.Error(err, "dynamic resource plugin failed", kv...)
	}
	return fwk.AsStatus(err)
}

func getAllocatedDeviceStatus(claim *resourceapi.ResourceClaim, deviceRequest *resourceapi.DeviceRequestAllocationResult) *resourceapi.AllocatedDeviceStatus {
	for _, device := range claim.Status.Devices {
		if deviceRequest.Device == device.Device && deviceRequest.Driver == device.Driver && deviceRequest.Pool == device.Pool {
			return &device
		}
	}
	return nil
}

// bindingConditionsStatusFormatter 实现 fmt.Stringer，延迟计算设备状态格式化结果。
// 日志关闭时可避免这部分昂贵处理。
type bindingConditionsStatusFormatter struct {
	state       *stateData
	statusLabel string
}

func (d bindingConditionsStatusFormatter) String() string {
	return formatBCStatusOneLine(getBindingConditionsStatusForStatus(d.state, d.statusLabel))
}

// BindingConditionsStatus 是 BindingConditions 日志中单个设备的紧凑摘要。
type BindingConditionsStatus struct {
	Driver  string   `json:"driver"`
	Pool    string   `json:"pool"`
	Device  string   `json:"device"`
	Pending []string `json:"pending,omitempty"`
	Failed  []string `json:"failed,omitempty"` // 只记录置为 true 的失败条件
}

// listBindingConditionsStatus 检查单个 claim，为带 BindingConditions 的每个已分配设备返回一份摘要。
// - Pending：尚未置为 true 的 BindingConditions（设备状态缺失时列出所有条件）
// - Failed：置为 true 的 BindingFailureConditions
func listBindingConditionsStatus(claim *resourceapi.ResourceClaim) []BindingConditionsStatus {
	if claim == nil || claim.Status.Allocation == nil {
		return nil
	}

	var out []BindingConditionsStatus
	for _, res := range claim.Status.Allocation.Devices.Results {
		if len(res.BindingConditions) == 0 {
			continue
		}

		sum := BindingConditionsStatus{
			Driver: res.Driver,
			Pool:   res.Pool,
			Device: res.Device,
		}

		ds := getAllocatedDeviceStatus(claim, &res)
		if ds == nil {
			// No status yet => all BindingConditions are pending.
			sum.Pending = append(sum.Pending, res.BindingConditions...)
			out = append(out, sum)
			continue
		}

		// Failed conditions that are true.
		for _, c := range res.BindingFailureConditions {
			if apimeta.IsStatusConditionTrue(ds.Conditions, c) {
				sum.Failed = append(sum.Failed, c)
			}
		}

		// Pending conditions: those not yet true.
		for _, c := range res.BindingConditions {
			if !apimeta.IsStatusConditionTrue(ds.Conditions, c) {
				sum.Pending = append(sum.Pending, c)
			}
		}

		out = append(out, sum)
	}

	return out
}

// getBindingConditionsStatusForStatus selects which devices to log across ALL claims in the state.
func getBindingConditionsStatusForStatus(state *stateData, statusLabel string) []BindingConditionsStatus {
	var out []BindingConditionsStatus
	for _, claim := range state.claims.all() {
		for _, s := range listBindingConditionsStatus(claim) {
			switch statusLabel {
			case schedmetrics.BindingConditionsStatusSuccess:
				// success: show all BC devices
				if len(s.Failed) == 0 && len(s.Pending) == 0 {
					out = append(out, s)
				}
			case schedmetrics.BindingConditionsStatusFailed:
				// failure: only devices that have at least one failure condition true
				if len(s.Failed) > 0 {
					out = append(out, BindingConditionsStatus{
						Driver: s.Driver,
						Pool:   s.Pool,
						Device: s.Device,
						Failed: append([]string(nil), s.Failed...),
					})
				}

			case schedmetrics.BindingConditionsStatusTimeout:
				// timeout/error: only devices still pending, and not failed
				if len(s.Pending) > 0 && len(s.Failed) == 0 {
					out = append(out, s)
				}

			default:
				// Treat unknown like timeout/error
				if len(s.Pending) > 0 && len(s.Failed) == 0 {
					out = append(out, s)
				}
			}
		}
	}
	return out
}

// formatBCStatusOneLine formats the selected devices into a single log-friendly line.
func formatBCStatusOneLine(devs []BindingConditionsStatus) string {
	if len(devs) == 0 {
		return ""
	}
	parts := make([]string, 0, len(devs))
	for _, d := range devs {
		base := fmt.Sprintf("<driver=%s, pool=%s> %s", d.Driver, d.Pool, d.Device)
		if len(d.Failed) > 0 {
			base += fmt.Sprintf(" failed=[%s]", strings.Join(d.Failed, ","))
		}
		if len(d.Pending) > 0 {
			base += fmt.Sprintf(" pending=[%s]", strings.Join(d.Pending, ","))
		}
		parts = append(parts, base)
	}
	return strings.Join(parts, "; ")
}

// nodeSelectorFromAllocation returns a NodeSelector for the given allocation,
// or nil if the allocation has no NodeSelector (meaning available on all nodes).
func nodeSelectorFromAllocation(allocation *resourceapi.AllocationResult) (*nodeaffinity.NodeSelector, error) {
	if allocation.NodeSelector == nil {
		return nil, nil
	}
	return nodeaffinity.NewNodeSelector(allocation.NodeSelector)
}
