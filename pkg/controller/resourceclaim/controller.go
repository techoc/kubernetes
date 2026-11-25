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

package resourceclaim

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	corev1apply "k8s.io/client-go/applyconfigurations/core/v1"
	schedulingapply "k8s.io/client-go/applyconfigurations/scheduling/v1beta1"
	v1informers "k8s.io/client-go/informers/core/v1"
	resourceinformers "k8s.io/client-go/informers/resource/v1"
	schedulinginformers "k8s.io/client-go/informers/scheduling/v1beta1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	v1listers "k8s.io/client-go/listers/core/v1"
	resourcelisters "k8s.io/client-go/listers/resource/v1"
	schedulinglisters "k8s.io/client-go/listers/scheduling/v1beta1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/component-base/metrics"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
	resourceclaimmetrics "k8s.io/dynamic-resource-allocation/resourceclaim/metrics"
	"k8s.io/klog/v2"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	controllermetrics "k8s.io/kubernetes/pkg/controller/resourceclaim/metrics"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
)

const (
	// podResourceClaimIndex 是按 Pod 的 ResourceClaim 建立索引时使用的索引名称。
	podResourceClaimIndex = "podResourceClaim"

	// podResourceClaimTemplateIndex 是仅按 Pod 的 ResourceClaimTemplate 建立索引时使用的索引名称。
	podResourceClaimTemplateIndex = "podResourceClaimTemplate"

	// podGroupResourceClaimIndex 是按 PodGroup 的 ResourceClaim 建立索引时使用的索引名称。
	podGroupResourceClaimIndex = "podGroupResourceClaim"

	// podGroupResourceClaimTemplateIndex 是仅按 PodGroup 的 ResourceClaimTemplate 建立索引时使用的索引名称。
	podGroupResourceClaimTemplateIndex = "podGroupResourceClaimTemplate"

	// claimPodOwnerIndex 用于查找以特定 Pod 为所有者的 ResourceClaim。
	// 索引值为 Pod 的 "namespace/UID"。
	claimPodOwnerIndex = "claimPodOwner"

	// claimPodGroupOwnerIndex 用于查找以特定 PodGroup 为所有者的 ResourceClaim。
	// 索引值为 PodGroup 的 "namespace/UID"。
	claimPodGroupOwnerIndex = "claimPodGroupOwner"

	// podGroupMembersIndex 用于查找属于指定 PodGroup 的 Pod。
	podGroupMembersIndex = "podGroupMembers"

	// claimMutationCacheSize 是 claim 变更缓存中 LRU 的大小。
	claimMutationCacheSize = 1000

	// fieldManager 是更新 Pod/PodGroup 状态时使用的字段管理器名称，
	// 用于在 server-side apply 中标明本控制器拥有的字段。
	fieldManager = "ResourceClaimController"

	// maxUIDCacheEntries 限制已删除对象 UID 缓存的容量，
	// 既能覆盖近期删除的对象，又不会无界增长。
	maxUIDCacheEntries = 500

	// schedulingAPIGroupPrefix 用于匹配调度 API 组前缀，
	// 避免把其他组中同名的类型误认为调度对象。
	schedulingAPIGroupPrefix = schedulingapi.GroupName + "/"
)

// Controller 负责为 Pod spec 中的 ResourceClaimTemplate 创建对应的 ResourceClaim，
// 并维护 Pod/PodGroup 对已分配 claim 的预留及陈旧预留的清理。
type Controller struct {
	// features 记录本控制器启用的特性开关，由启动时的 feature gate 决定。
	features controllerFeatures

	// kubeClient 是与 API server 通信的客户端，用于创建、更新 claim 及更新状态。
	kubeClient clientset.Interface

	// claimLister 是获取 ResourceClaim 的共享 lister；由于与其他控制器共享，
	// 其中的对象必须视为不可变，禁止就地修改。
	claimLister resourcelisters.ResourceClaimLister
	// claimsSynced 反映 claim informer 的同步状态。
	claimsSynced cache.InformerSynced
	// claimCache 在 informer 缓存之上叠加变更缓存，写入后立即可读，
	// 避免 informer 追上前因看不到新 claim 而重复创建。
	claimCache cache.MutationCache

	// podLister 是获取 Pod 的共享 lister，对象必须视为不可变。
	podLister v1listers.PodLister
	// podSynced 反映 Pod informer 的同步状态。
	podSynced cache.InformerSynced

	// podGroupLister 是获取 PodGroup 的共享 lister，对象必须视为不可变。
	podGroupLister schedulinglisters.PodGroupLister
	// podGroupSynced 在未启用工作负载 claim 时始终返回 true，
	// 因为此时无需等待 PodGroup informer 同步。
	podGroupSynced cache.InformerSynced

	// templateLister 是获取 ResourceClaimTemplate 的共享 lister，对象必须视为不可变。
	templateLister resourcelisters.ResourceClaimTemplateLister
	// templatesSynced 反映模板 informer 的同步状态。
	templatesSynced cache.InformerSynced

	// podIndexer 安装按 claim/模板/PodGroup 查询 Pod 的索引，
	// 避免每次事件都遍历全部 Pod。
	podIndexer cache.Indexer

	// podGroupIndexer 安装按 claim/模板查询 PodGroup 的索引，缩小遍历范围。
	podGroupIndexer cache.Indexer

	// recorder 用于向 API server 记录 Warning 事件（如 claim 创建失败）。
	recorder record.EventRecorder

	// syncHandlerFunc 是处理工作队列每个条目的函数，测试工作队列时可替换。
	syncHandlerFunc func(context.Context, string) error
	// queue 保存待处理对象的 key（claim:/pod:/podgroup: 前缀），失败时按限速重试。
	queue workqueue.TypedRateLimitingInterface[string]

	// deletedObjects 记录已确认删除的对象 UID（Pod 与 PodGroup）。
	// 命中缓存即可确定对应 ReservedFor 条目应被移除，无需再查 API server。
	deletedObjects *uidCache
}

const (
	// 工作队列 key 的前缀，syncHandler 据此分派到不同的同步函数。
	claimKeyPrefix    = "claim:"
	podKeyPrefix      = "pod:"
	podGroupKeyPrefix = "podgroup:"
)

// controllerFeatures 定义控制器中启用的特性集合，便于测试直接构造不同组合。
type controllerFeatures struct {
	// AdminAccess 允许模板请求管理员权限。
	AdminAccess bool
	// GenericWorkload 允许 Pod 引用 PodGroup（通用工作负载）。
	GenericWorkload bool
	// PrioritizedList 允许模板使用 FirstAvailable 优先列表。
	PrioritizedList bool
	// WorkloadResourceClaims 允许 PodGroup 声明和使用自己的 ResourceClaim。
	WorkloadResourceClaims bool
}

// String 用各特性开关的启用/禁用状态生成适合作为单元测试名称的字符串。
func (f controllerFeatures) String() string {
	// enabled 将布尔开关转成可读的文本片段。
	enabled := func(b bool) string {
		if b {
			return "enabled"
		}
		return "disabled"
	}
	return fmt.Sprintf(
		"adminaccess-%s-genericworkload-%s-prioritizedlist-%s-workloadresourceclaims-%s",
		enabled(f.AdminAccess),
		enabled(f.GenericWorkload),
		enabled(f.PrioritizedList),
		enabled(f.WorkloadResourceClaims),
	)
}

// nonRetryableError 标记不应重试的工作队列条目：
// 例如依赖特性被关闭，重试永远不会成功，直接遗忘即可。
type nonRetryableError struct {
	// error 保存底层错误原因。
	error
}

// NewController 创建 ResourceClaim 控制器，特性开关取自进程级 feature gate。
func NewController(
	logger klog.Logger,
	kubeClient clientset.Interface,
	podInformer v1informers.PodInformer,
	podGroupInformer schedulinginformers.PodGroupInformer,
	claimInformer resourceinformers.ResourceClaimInformer,
	templateInformer resourceinformers.ResourceClaimTemplateInformer) (*Controller, error) {
	return newControllerWithFeatures(
		logger,
		kubeClient,
		podInformer,
		podGroupInformer,
		claimInformer,
		templateInformer,
		controllerFeatures{
			AdminAccess:            utilfeature.DefaultFeatureGate.Enabled(features.DRAAdminAccess),
			GenericWorkload:        utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload),
			PrioritizedList:        utilfeature.DefaultFeatureGate.Enabled(features.DRAPrioritizedList),
			WorkloadResourceClaims: utilfeature.DefaultFeatureGate.Enabled(features.DRAWorkloadResourceClaims),
		},
	)
}

// newControllerWithFeatures 按显式传入的特性组合创建控制器，供单元测试使用。
func newControllerWithFeatures(
	logger klog.Logger,
	kubeClient clientset.Interface,
	podInformer v1informers.PodInformer,
	podGroupInformer schedulinginformers.PodGroupInformer,
	claimInformer resourceinformers.ResourceClaimInformer,
	templateInformer resourceinformers.ResourceClaimTemplateInformer,
	features controllerFeatures) (*Controller, error) {

	ec := &Controller{
		features:        features,
		kubeClient:      kubeClient,
		podLister:       podInformer.Lister(),
		podIndexer:      podInformer.Informer().GetIndexer(),
		podSynced:       podInformer.Informer().HasSynced,
		claimLister:     claimInformer.Lister(),
		claimsSynced:    claimInformer.Informer().HasSynced,
		templateLister:  templateInformer.Lister(),
		templatesSynced: templateInformer.Informer().HasSynced,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "resource_claim"},
		),
		deletedObjects: newUIDCache(maxUIDCacheEntries),
	}
	// 默认使用真实同步逻辑；测试工作队列时可覆盖该函数。
	ec.syncHandlerFunc = ec.syncHandler

	resourceclaimmetrics.RegisterMetrics()
	controllermetrics.RegisterMetrics(newCustomCollector(ec.claimLister, getAdminAccessMetricLabel, logger))

	if _, err := podInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			ec.enqueuePod(logger, obj, false)
		},
		UpdateFunc: func(old, updated any) {
			ec.enqueuePod(logger, updated, false)
		},
		DeleteFunc: func(obj any) {
			ec.enqueuePod(logger, obj, true)
		},
	}, cache.HandlerOptions{Logger: &logger}); err != nil {
		return nil, err
	}
	if _, err := claimInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			logger.V(6).Info("New claim", "claimDump", obj)
			ec.enqueueResourceClaim(logger, nil, obj)
		},
		UpdateFunc: func(old, updated any) {
			logger.V(6).Info("Updated claim", "claimDump", updated)
			ec.enqueueResourceClaim(logger, old, updated)
		},
		DeleteFunc: func(obj any) {
			logger.V(6).Info("Deleted claim", "claimDump", obj)
			ec.enqueueResourceClaim(logger, obj, nil)
		},
	}, cache.HandlerOptions{Logger: &logger}); err != nil {
		return nil, err
	}
	if _, err := templateInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			logger.V(6).Info("New claim template", "claimTemplateDump", obj)
			ec.enqueueResourceClaimTemplate(logger, obj)
		},
		UpdateFunc: func(old, updated any) {
			logger.V(6).Info("Updated claim template", "claimTemplateDump", updated)
			ec.enqueueResourceClaimTemplate(logger, updated)
		},
		DeleteFunc: func(obj any) {
			logger.V(6).Info("Deleted claim template", "claimTemplateDump", obj)
		},
	}, cache.HandlerOptions{Logger: &logger}); err != nil {
		return nil, err
	}
	if err := ec.podIndexer.AddIndexers(cache.Indexers{podResourceClaimIndex: podResourceClaimIndexFunc}); err != nil {
		return nil, fmt.Errorf("could not initialize ResourceClaim controller: %w", err)
	}

	if err := ec.podIndexer.AddIndexers(cache.Indexers{podResourceClaimTemplateIndex: podResourceClaimTemplateIndexFunc}); err != nil {
		return nil, fmt.Errorf("could not initialize ResourceClaim controller: %w", err)
	}

	if features.GenericWorkload {
		ec.podGroupLister = podGroupInformer.Lister()
	}
	ec.podGroupSynced = func() bool { return true }
	if features.WorkloadResourceClaims {
		ec.podGroupSynced = podGroupInformer.Informer().HasSynced
		ec.podGroupIndexer = podGroupInformer.Informer().GetIndexer()

		if _, err := podGroupInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj any) {
				logger.V(6).Info("New PodGroup", "podGroupDump", obj)
				ec.enqueuePodGroup(logger, obj, false)
			},
			UpdateFunc: func(old, updated any) {
				logger.V(6).Info("Updated PodGroup", "podGroupDump", updated)
				ec.enqueuePodGroup(logger, updated, false)
			},
			DeleteFunc: func(obj any) {
				logger.V(6).Info("Deleted PodGroup", "podGroupDump", obj)
				ec.enqueuePodGroup(logger, obj, true)
			},
		}, cache.HandlerOptions{Logger: &logger}); err != nil {
			return nil, err
		}

		if err := ec.podGroupIndexer.AddIndexers(cache.Indexers{podGroupResourceClaimIndex: podGroupResourceClaimIndexFunc}); err != nil {
			return nil, fmt.Errorf("could not initialize ResourceClaim controller: %w", err)
		}
		if err := ec.podGroupIndexer.AddIndexers(cache.Indexers{podGroupResourceClaimTemplateIndex: podGroupResourceClaimTemplateIndexFunc}); err != nil {
			return nil, fmt.Errorf("could not initialize ResourceClaim controller: %w", err)
		}

		if err := ec.podIndexer.AddIndexers(cache.Indexers{podGroupMembersIndex: podGroupMemberIndexFunc}); err != nil {
			return nil, fmt.Errorf("could not initialize ResourceClaim controller: %w", err)
		}

		if err := claimInformer.Informer().GetIndexer().AddIndexers(cache.Indexers{claimPodGroupOwnerIndex: claimPodGroupOwnerIndexFunc}); err != nil {
			return nil, fmt.Errorf("could not initialize ResourceClaim controller: %w", err)
		}
	}

	// 变更缓存叠加在 informer 缓存之上：控制器创建 claim 后立刻能读到它，
	// 直到 informer 追上。否则若创建成功但 Pod 状态更新失败，
	// 重试时 informer 仍看不到该 claim，就会重复创建第二个 claim。
	claimInformerCache := claimInformer.Informer().GetIndexer()
	if err := claimInformerCache.AddIndexers(cache.Indexers{claimPodOwnerIndex: claimPodOwnerIndexFunc}); err != nil {
		return nil, fmt.Errorf("could not initialize ResourceClaim controller: %w", err)
	}
	ec.claimCache = cache.NewIntegerResourceVersionMutationCacheWithOptions(logger, claimInformerCache, cache.MutationCacheOptions{
		Indexer: claimInformerCache,
		// TTL 设得很长只是兜底；正常情况下 informer 很快就会同步。
		TTL: time.Hour,
		// IncludeAdds 允许缓存底层没有的新对象，这正是该缓存的用途。
		// 竞态下（claim 在缓存中但在集群里被删）写出的错误状态，
		// 会在 informer 追上后被检测并修正，因此安全。
		IncludeAdds: true,
		// 容量比默认的 100 更大，使 informer 落后 etcd 时仍能本地命中，
		// 避免回退到对 API server 的 GET 请求。
		MaxCacheSize: claimMutationCacheSize,
	},
	)

	return ec, nil
}

// enqueueResourceClaimTemplate 在模板变化时把所有引用该模板的 Pod
// （以及启用工作负载 claim 时的 PodGroup）重新入队，
// 因为模板内容变化可能需要创建或修正对应的 claim。
func (ec *Controller) enqueueResourceClaimTemplate(logger klog.Logger, obj any) {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		// 删除事件可能只给出墓碑对象，取出其携带的原始对象。
		obj = d.Obj
	}
	template, ok := obj.(*resourceapi.ResourceClaimTemplate)
	if !ok {
		// 不是模板对象，忽略。
		runtime.HandleErrorWithLogger(logger, nil, "EnqueueResourceClaimTemplate called for unexpected object", "type", fmt.Sprintf("%T", obj))
		return
	}

	logger.V(6).Info("ResourceClaimTemplate added or updated", "resourceClaimTemplate", klog.KObj(template))

	// 通过模板索引找出所有引用该模板的 Pod 并入队。
	objects, err := ec.podIndexer.ByIndex(podResourceClaimTemplateIndex, fmt.Sprintf("%s/%s", template.Namespace, template.Name))
	if err != nil {
		runtime.HandleErrorWithLogger(logger, err, "Unable to list pods for claim template", "resourceClaimTemplate", klog.KObj(template))
		return
	}
	if len(objects) == 0 {
		logger.V(6).Info("ResourceClaimTemplate change unrelated to any known pod", "resourceClaimTemplate", klog.KObj(template))
	}

	for _, object := range objects {
		pod, ok1 := object.(*v1.Pod)
		if !ok1 {
			// 不是 Pod？！
			runtime.HandleErrorWithLogger(logger, nil, "unexpected type in Pod cache", "type", fmt.Sprintf("%T", object))
			return
		}
		logger.V(4).Info(
			"Enqueuing pod due to ResourceClaimTemplate change",
			"resourceClaimTemplate", klog.KObj(template),
			"pod", klog.KObj(pod),
		)
		ec.enqueuePod(logger, object, false)
	}

	if ec.features.WorkloadResourceClaims {
		// 同样处理引用该模板的 PodGroup。
		objects, err := ec.podGroupIndexer.ByIndex(podGroupResourceClaimTemplateIndex, fmt.Sprintf("%s/%s", template.Namespace, template.Name))
		if err != nil {
			runtime.HandleErrorWithLogger(logger, err, "Unable to list PodGroups for claim template", "resourceClaimTemplate", klog.KObj(template))
			return
		}
		if len(objects) == 0 {
			logger.V(6).Info("ResourceClaimTemplate change unrelated to any known PodGroup", "resourceClaimTemplate", klog.KObj(template))
		}
		for _, object := range objects {
			podGroup, ok := object.(*schedulingapi.PodGroup)
			if !ok {
				// 不是 PodGroup？！
				runtime.HandleErrorWithLogger(logger, nil, "unexpected type in PodGroup cache", "type", fmt.Sprintf("%T", object))
				return
			}
			logger.V(4).Info(
				"Enqueuing PodGroup due to ResourceClaimTemplate change",
				"resourceClaimTemplate", klog.KObj(template),
				"podgroup", klog.KObj(podGroup),
			)
			ec.enqueuePodGroup(logger, object, false)
		}
	}
}

// enqueuePod 处理 Pod 的增改删事件：
//   - 记录已删除 Pod 的 UID，供后续清理 ReservedFor 时快速判断；
//   - Pod 不再需要 claim 时，把其 claim 入队以清理预留；
//   - Pod 自身仍需处理时，把 Pod 入队以创建 claim 或建立预留。
func (ec *Controller) enqueuePod(logger klog.Logger, obj any, deleted bool) {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = d.Obj
	}
	pod, ok := obj.(*v1.Pod)
	if !ok {
		// 不是 Pod？！
		logger.Error(nil, "EnqueuePod called for unexpected object", "type", fmt.Sprintf("%T", obj))
		return
	}

	// 需同时检查 spec 中的普通 claim 和 status 中的扩展 claim，
	// 后者由调度器在 PreBind 阶段创建，不出现在 spec 中。
	hasResourceClaims := len(pod.Spec.ResourceClaims) > 0
	// 即使特性当前已关闭，只要 status 里还有扩展 claim 就必须考虑清理：
	// 它可能是特性开启时创建的，关闭后仍需收拾残局。
	hasExtendedResourceClaims := pod.Status.ExtendedResourceClaimStatus != nil
	if !hasResourceClaims && !hasExtendedResourceClaims {
		// 没有声明任何 claim，无需处理。
		return
	}

	if deleted {
		logger.V(6).Info("Pod got deleted", "pod", klog.KObj(pod))
		ec.deletedObjects.Add(pod.UID)
	}

	logger.V(6).Info("Pod with resource claims changed", "pod", klog.KObj(pod), "deleted", deleted)

	// 已删除或已结束的 Pod 不再需要预留，需要清理其 claim。
	needsClaims, reason := podNeedsClaims(pod, deleted)
	if needsClaims {
		logger.V(6).Info("Not touching claims", "pod", klog.KObj(pod), "reason", reason)
	} else {
		for claimName := range resourceclaim.PodClaims(pod) {
			key := claimKeyPrefix + pod.Namespace + "/" + claimName
			logger.V(6).Info("Process claim", "pod", klog.KObj(pod), "claim", klog.KRef(pod.Namespace, claimName), "key", key, "reason", reason)
			ec.queue.Add(key)
		}
	}

	needsWork, reason := ec.podNeedsWork(pod)
	if needsWork {
		logger.V(6).Info("Enqueueing pod", "pod", klog.KObj(pod), "reason", reason)
		ec.queue.Add(podKeyPrefix + pod.Namespace + "/" + pod.Name)
		return
	}
	logger.V(6).Info("Not enqueueing pod", "pod", klog.KObj(pod), "reason", reason)
}

// podNeedsClaims 判断 Pod 是否仍需要 claim：已删除、已进入终态、
// 或在调度前就被删除的 Pod 都应释放预留；其余情况返回需要及原因。
func podNeedsClaims(pod *v1.Pod, deleted bool) (bool, string) {
	if deleted {
		return false, "pod got removed"
	}
	if podutil.IsPodTerminal(pod) {
		return false, "pod has terminated"
	}
	if pod.DeletionTimestamp != nil && pod.Spec.NodeName == "" {
		return false, "pod got deleted before scheduling"
	}
	// 仍有可能运行，因此保留 claim。
	return true, "pod might run"
}

// podNeedsWork 检查新增或修改的 Pod 是否还需要 worker 进一步处理，
// 返回是否需要以及原因字符串。
func (ec *Controller) podNeedsWork(pod *v1.Pod) (bool, string) {
	if pod.DeletionTimestamp != nil {
		// 正在删除的 Pod 无需再创建 claim 或建立预留。
		return false, "pod is deleted"
	}

	var doNothingReasons []string

	for _, podClaim := range pod.Spec.ResourceClaims {
		claimName, checkOwner, err := resourceclaim.Name(pod, &podClaim)
		if err != nil {
			return true, err.Error()
		}
		// claimName 为 nil 表示此前已判定该 claim 不需要处理。
		if claimName == nil {
			doNothingReasons = append(doNothingReasons, fmt.Sprintf("claim %s is not needed", podClaim.Name))
			continue
		}
		claim, err := ec.claimLister.ResourceClaims(pod.Namespace).Get(*claimName)
		if apierrors.IsNotFound(err) {
			if podClaim.ResourceClaimTemplateName != nil {
				podGroup, err := ec.getPodGroup(pod)
				if err != nil {
					// 无法确定 PodGroup 时宁可入队重试，也不能漏掉需要创建的 claim。
					return true, "ResourceClaim might have to be created from template: " + err.Error()
				}
				bindTo, err := resourceclaim.BindTo(pod, podGroup, &podClaim)
				if err != nil {
					doNothingReasons = append(doNothingReasons, fmt.Sprintf("not sure what to do for ResourceClaim for claim %s: %v", podClaim.Name, err))
					continue
				}
				if bindTo.APIGroup == schedulingapi.GroupName && bindTo.Resource == "podgroups" {
					doNothingReasons = append(doNothingReasons, fmt.Sprintf("ResourceClaim needs to be created for the PodGroup claim %s", podClaim.Name))
					continue
				}
				return true, "must create ResourceClaim from template"
			}
			// 非模板 claim 由用户自行创建，控制器不负责，也不重试。
			doNothingReasons = append(doNothingReasons, "ResourceClaim for claim %s is missing and must be created by user", podClaim.Name)
			continue
		}
		if err != nil {
			// 其他查询错误不应发生，入队重试以便可观测并等待恢复。
			return true, fmt.Sprintf("internal error while checking for claim: %v", err)
		}

		if checkOwner {
			if err := resourceclaim.IsForPod(pod, claim, ec.features.WorkloadResourceClaims); err != nil {
				// 归属不匹配时 Pod 无法继续，等待归属冲突解除后再处理。
				doNothingReasons = append(doNothingReasons, fmt.Sprintf("claim %s: %v", podClaim.Name, err))
				continue
			}
		}

		// 尚未调度时无需预留，但仍要继续遍历其余 claim，
		// 因为后面可能存在需要从模板创建的 claim。
		if pod.Spec.NodeName == "" {
			continue
		}

		if claim.Status.Allocation != nil &&
			!resourceclaim.IsReservedForPod(pod, claim, ec.features.WorkloadResourceClaims) &&
			resourceclaim.CanBeReserved(claim) {
			// 已分配、可预留且尚未为该 Pod 预留，需要建立预留。
			return true, fmt.Sprintf("need to reserve claim %s for pod", klog.KObj(claim))
		}
	}

	return false, "nothing to do for any claim: " + strings.Join(doNothingReasons, ", ")
}

// enqueuePodGroup 处理 PodGroup 的增改删事件：清理已删除 PodGroup 的 claim、
// 将需要处理的 PodGroup 入队，并把其成员 Pod 入队以同步 claim 状态。
func (ec *Controller) enqueuePodGroup(logger klog.Logger, obj any, deleted bool) {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		obj = d.Obj
	}
	podGroup, ok := obj.(*schedulingapi.PodGroup)
	if !ok {
		// 不是 PodGroup？！
		logger.Error(nil, "enqueuePodGroup called for unexpected object", "type", fmt.Sprintf("%T", obj))
		return
	}

	// 没有声明 resource claim 的 PodGroup 无需任何处理。
	hasResourceClaims := len(podGroup.Spec.ResourceClaims) > 0
	if !hasResourceClaims {
		// 没有声明任何 claim，无需处理。
		return
	}

	if deleted {
		logger.V(6).Info("PodGroup got deleted", "podgroup", klog.KObj(podGroup))
		ec.deletedObjects.Add(podGroup.UID)
	}

	logger.V(6).Info("PodGroup with resource claims changed", "podgroup", klog.KObj(podGroup), "deleted", deleted)

	// 已删除的 PodGroup 不再需要 claim，触发预留清理。
	needsClaims, reason := podGroupNeedsClaims(podGroup, deleted)
	if needsClaims {
		logger.V(6).Info("Not touching claims", "podgroup", klog.KObj(podGroup), "reason", reason)
	} else {
		for _, podGroupClaim := range podGroup.Spec.ResourceClaims {
			claimName, _, err := resourceclaim.NameFromPodGroup(podGroup, &podGroupClaim)
			switch {
			case err != nil:
				// 出错可能只是 claim 尚未创建，也可能是 API 发生变化；
				// 后者会在别处报告，这里仅记录调试日志。
				logger.V(6).Info("Nothing to do for claim during PodGroup change", "podgroup", klog.KObj(podGroup), "podGroupClaim", podGroupClaim.Name, "err", err, "reason", reason)
			case claimName != nil:
				key := claimKeyPrefix + podGroup.Namespace + "/" + *claimName
				logger.V(6).Info("Process claim", "podgroup", klog.KObj(podGroup), "claim", klog.KRef(podGroup.Namespace, *claimName), "key", key, "reason", reason)
				ec.queue.Add(key)
			default:
				// claimName 为 nil 表示该 claim 被跳过，无需处理。
				logger.V(6).Info("Nothing to do for skipped claim during PodGroup change", "podgroup", klog.KObj(podGroup), "podGroupClaim", podGroupClaim.Name, "reason", reason)
			}
		}
	}

	needsWork, reason := ec.podGroupNeedsWork(podGroup)
	if needsWork {
		logger.V(6).Info("Enqueueing PodGroup", "podgroup", klog.KObj(podGroup), "reason", reason)
		ec.queue.Add(podGroupKeyPrefix + podGroup.Namespace + "/" + podGroup.Name)
	} else {
		logger.V(6).Info("Not enqueueing PodGroup", "podgroup", klog.KObj(podGroup), "reason", reason)
	}

	// PodGroup 的状态更新可能让分配给它的 claim 对其成员 Pod 可用。
	// 这里把成员 Pod 入队，使其同步匹配的 claim 状态，
	// 从而建立 ResourceClaim 到 Pod 的索引关系。
	objects, err := ec.podIndexer.ByIndex(podGroupMembersIndex, podGroup.Namespace+"/"+podGroup.Name)
	if err != nil {
		runtime.HandleErrorWithLogger(logger, err, "Unable to list Pods for PodGroup", "podgroup", klog.KObj(podGroup))
		return
	}
	if len(objects) == 0 {
		logger.V(6).Info("PodGroup change unrelated to any known Pod", "podgroup", klog.KObj(podGroup))
	}
	for _, object := range objects {
		pod, ok := object.(*v1.Pod)
		if !ok {
			// 不是 Pod？！
			runtime.HandleErrorWithLogger(logger, nil, "unexpected object in Pod indexer", "type", fmt.Sprintf("%T", object))
			return
		}
		logger.V(6).Info(
			"Enqueuing Pod due to PodGroup change",
			"podgroup", klog.KObj(podGroup),
			"pod", klog.KObj(pod),
		)
		ec.enqueuePod(logger, object, false)
	}
}

// podGroupNeedsClaims 判断 PodGroup 是否仍需要 claim：
// 只有已删除的 PodGroup 才需要释放，其余情况组成员 Pod 仍可能运行。
func podGroupNeedsClaims(_ *schedulingapi.PodGroup, deleted bool) (bool, string) {
	if deleted {
		return false, "PodGroup got removed"
	}
	// PodGroup 中的 Pod 仍可能运行，保留 claim。
	return true, "Pods in the PodGroup might run"
}

// podGroupNeedsWork 检查新增或修改的 PodGroup 是否还需要 worker 处理，
// 主要判断是否有需要从模板创建的 claim。
func (ec *Controller) podGroupNeedsWork(podGroup *schedulingapi.PodGroup) (bool, string) {
	if podGroup.DeletionTimestamp != nil {
		// 正在删除的 PodGroup 无需创建 claim。
		return false, "PodGroup is deleted"
	}

	var doNothingReasons []string

	for _, podGroupClaim := range podGroup.Spec.ResourceClaims {
		claimName, checkOwner, err := resourceclaim.NameFromPodGroup(podGroup, &podGroupClaim)
		if err != nil {
			return true, err.Error()
		}
		// claimName 为 nil 表示此前已判定该 claim 不需要处理。
		if claimName == nil {
			doNothingReasons = append(doNothingReasons, fmt.Sprintf("claim %s is not needed", podGroupClaim.Name))
			continue
		}
		claim, err := ec.claimLister.ResourceClaims(podGroup.Namespace).Get(*claimName)
		if apierrors.IsNotFound(err) {
			if podGroupClaim.ResourceClaimTemplateName != nil {
				return true, "must create ResourceClaim from template"
			}
			// 非模板 claim 由用户自行创建，控制器不负责，也不重试。
			doNothingReasons = append(doNothingReasons, "ResourceClaim claim %s is missing and must be created by user", *claimName, podGroupClaim.Name)
			continue
		}
		if err != nil {
			// 其他查询错误不应发生，入队重试以便可观测并等待恢复。
			return true, fmt.Sprintf("internal error while checking for claim: %v", err)
		}

		if checkOwner {
			if err := resourceclaim.IsForPodGroup(podGroup, claim); err != nil {
				// 归属不匹配时 PodGroup 无法继续，等待冲突解除。
				doNothingReasons = append(doNothingReasons, fmt.Sprintf("claim %s: %v", podGroupClaim.Name, err))
				continue
			}
		}
	}

	return false, "nothing to do for any claim: " + strings.Join(doNothingReasons, ", ")
}

// enqueueResourceClaim 处理 claim 变化：
//   - 新增或更新时把 claim 入队，以清理陈旧的 ReservedFor；
//   - 同时找出使用该 claim 的 Pod/PodGroup 并入队，让它们同步状态。
func (ec *Controller) enqueueResourceClaim(logger klog.Logger, oldObj, newObj any) {
	deleted := newObj == nil
	if d, ok := oldObj.(cache.DeletedFinalStateUnknown); ok {
		oldObj = d.Obj
	}
	oldClaim, ok := oldObj.(*resourceapi.ResourceClaim)
	if oldObj != nil && !ok {
		return
	}
	newClaim, ok := newObj.(*resourceapi.ResourceClaim)
	if newObj != nil && !ok {
		return
	}

	// DeletedFinalStateUnknown.Obj 可能为空，需防止两个 claim 都是 nil 导致解引用崩溃。
	if oldClaim == nil && newClaim == nil {
		return
	}

	claim := newClaim
	if claim == nil {
		claim = oldClaim
	}
	if !deleted {
		// 启动时必须检查所有 claim 清理陈旧预留；
		// 更新时也可能新增一个其实已不存在的 Pod 引用。
		key := claimKeyPrefix + claim.Namespace + "/" + claim.Name
		logger.V(6).Info("Enqueueing new or updated claim", "claim", klog.KObj(claim), "key", key)
		ec.queue.Add(key)
	} else {
		logger.V(6).Info("Not enqueueing deleted claim", "claim", klog.KObj(claim))
	}

	// claim 状态变化可能让使用它的 Pod 需要建立预留或更新状态。
	objs, err := ec.podIndexer.ByIndex(podResourceClaimIndex, fmt.Sprintf("%s/%s", claim.Namespace, claim.Name))
	if err != nil {
		logger.Error(err, "Failed to list pods from cache")
		return
	}
	if len(objs) == 0 {
		logger.V(6).Info("ResourceClaim change unrelated to any known pod", "claim", klog.KObj(claim))
	}
	for _, obj := range objs {
		ec.enqueuePod(logger, obj, false)
	}

	if ec.features.WorkloadResourceClaims {
		// 同理检查使用该 claim 的 PodGroup。
		podGroups, err := ec.podGroupIndexer.ByIndex(podGroupResourceClaimIndex, fmt.Sprintf("%s/%s", claim.Namespace, claim.Name))
		if err != nil {
			logger.Error(err, "Failed to list PodGroups from cache")
			return
		}
		if len(podGroups) == 0 {
			logger.V(6).Info("ResourceClaim change unrelated to any known PodGroup", "claim", klog.KObj(claim))
		}
		for _, podGroup := range podGroups {
			ec.enqueuePodGroup(logger, podGroup, false)
		}
	}
}

// Run 启动控制器：初始化事件 recorder、等待各 informer 同步，
// 然后运行 workers 个 worker 直到 context 结束。
func (ec *Controller) Run(ctx context.Context, workers int) {
	// worker 内 panic 时记录并交回 context 处理。
	defer runtime.HandleCrashWithContext(ctx)

	eventBroadcaster := record.NewBroadcaster(record.WithContext(ctx))
	eventBroadcaster.StartLogging(klog.Infof)
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: ec.kubeClient.CoreV1().Events("")})
	ec.recorder = eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "resource_claim"})
	// 退出时停止事件广播，避免 goroutine 泄漏。
	defer eventBroadcaster.Shutdown()

	logger := klog.FromContext(ctx)
	logger.Info("Starting resource claim controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down resource claim controller")
		ec.queue.ShutDown()
		wg.Wait()
	}()

	// 必须等所有 informer 同步完再处理队列，否则 lister 数据不全会误判。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, ec.podSynced, ec.podGroupSynced, ec.claimsSynced, ec.templatesSynced) {
		return
	}

	for range workers {
		wg.Go(func() {
			wait.UntilWithContext(ctx, ec.runWorker, time.Second)
		})
	}
	<-ctx.Done()
}

// runWorker 持续处理工作队列，直到队列关闭。
func (ec *Controller) runWorker(ctx context.Context) {
	for ec.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 取出一个 key 调用 syncHandler：
// 成功或不可重试的错误会被 Forget，其余错误按限速重新入队。
func (ec *Controller) processNextWorkItem(ctx context.Context) bool {
	key, shutdown := ec.queue.Get()
	if shutdown {
		return false
	}
	// 确保无论成功失败都标记该条目处理完毕。
	defer ec.queue.Done(key)

	err := ec.syncHandlerFunc(ctx, key)
	// 不可重试错误（如依赖特性被关闭）重试也不会成功，直接丢弃。
	_, nonRetryable := errors.AsType[nonRetryableError](err)
	if nonRetryable {
		runtime.HandleErrorWithContext(ctx, err, "Work item failed, will not retry", "item", key)
	}
	if err == nil || nonRetryable {
		ec.queue.Forget(key)
		return true
	}

	runtime.HandleErrorWithContext(ctx, err, "Work item failed", "item", key)
	ec.queue.AddRateLimited(key)

	return true
}

// syncHandler 按 key 前缀把工作项分派给 syncPod/syncPodGroup/syncClaim。
// 返回错误会导致该条目重新入队（不可重试错误除外）。
func (ec *Controller) syncHandler(ctx context.Context, key string) error {
	// key 形如 "pod:namespace/name"，先按冒号拆出类型和对象名。
	sep := strings.Index(key, ":")
	if sep < 0 {
		return fmt.Errorf("unexpected key: %s", key)
	}
	prefix, object := key[0:sep+1], key[sep+1:]
	namespace, name, err := cache.SplitMetaNamespaceKey(object)
	if err != nil {
		return err
	}

	switch prefix {
	case podKeyPrefix:
		return ec.syncPod(ctx, namespace, name)
	case podGroupKeyPrefix:
		return ec.syncPodGroup(ctx, namespace, name)
	case claimKeyPrefix:
		return ec.syncClaim(ctx, namespace, name)
	default:
		return fmt.Errorf("unexpected key prefix: %s", prefix)
	}

}

// syncPod 同步单个 Pod：为模板 claim 创建 ResourceClaim 并写入 Pod 状态，
// 随后为已调度 Pod 的已分配 claim 建立 ReservedFor 预留。
func (ec *Controller) syncPod(ctx context.Context, namespace, name string) error {
	logger := klog.LoggerWithValues(klog.FromContext(ctx), "pod", klog.KRef(namespace, name))
	ctx = klog.NewContext(ctx, logger)
	pod, err := ec.podLister.Pods(namespace).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.V(5).Info("Nothing to do for pod, it is gone")
			return nil
		}
		return err
	}

	// 正在删除的 Pod 不再创建 claim，交由删除流程处理。
	if pod.DeletionTimestamp != nil {
		logger.V(5).Info("Nothing to do for pod, it is marked for deletion")
		return nil
	}

	podGroup, err := ec.getPodGroup(pod)
	if err != nil {
		return fmt.Errorf("get PodGroup for Pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	if podGroup != nil {
		logger = klog.LoggerWithValues(logger, "podgroup", klog.KObj(podGroup))
		ctx = klog.NewContext(ctx, logger)
	}

	var newPodClaims map[string]string
	for _, podClaim := range pod.Spec.ResourceClaims {
		if err := ec.handleClaim(ctx, pod, podGroup, podClaim, &newPodClaims); err != nil {
			if ec.recorder != nil {
				ec.recorder.Event(pod, v1.EventTypeWarning, "FailedResourceClaimCreation", fmt.Sprintf("PodResourceClaim %s: %v", podClaim.Name, err))
			}
			return fmt.Errorf("pod %s/%s, PodResourceClaim %s: %v", namespace, name, podClaim.Name, err)
		}
	}

	if newPodClaims != nil {
		// 将新建的 claim 信息合并进 Pod 状态：先收集已有条目，再覆盖新值，
		// 并按名称排序以保证 apply 内容稳定、避免无谓的状态更新。
		mergedStatuses := make(map[string]string)
		for _, status := range pod.Status.ResourceClaimStatuses {
			if status.ResourceClaimName != nil {
				mergedStatuses[status.Name] = *status.ResourceClaimName
			}
		}
		maps.Copy(mergedStatuses, newPodClaims)

		names := make([]string, 0, len(mergedStatuses))
		for name := range mergedStatuses {
			names = append(names, name)
		}
		slices.Sort(names)

		statuses := make([]*corev1apply.PodResourceClaimStatusApplyConfiguration, 0, len(names))
		for _, name := range names {
			statuses = append(statuses, corev1apply.PodResourceClaimStatus().WithName(name).WithResourceClaimName(mergedStatuses[name]))
		}

		podApply := corev1apply.Pod(name, namespace).WithStatus(corev1apply.PodStatus().WithResourceClaimStatuses(statuses...))
		if _, err := ec.kubeClient.CoreV1().Pods(namespace).ApplyStatus(ctx, podApply, metav1.ApplyOptions{FieldManager: fieldManager, Force: true}); err != nil {
			return fmt.Errorf("update pod %s/%s ResourceClaimStatuses: %v", namespace, name, err)
		}
	}

	if pod.Spec.NodeName == "" {
		// 未调度的 Pod 由调度器决定节点并负责预留，此处不抢先处理。
		logger.V(5).Info("Nothing to do for pod, scheduler will deal with it")
		return nil
	}

	for _, podClaim := range pod.Spec.ResourceClaims {
		claimName, checkOwner, err := resourceclaim.Name(pod, &podClaim)
		if err != nil {
			return err
		}
		// claimName 为 nil 表示已判定不需要该 claim，跳过。
		if claimName == nil {
			continue
		}
		claim, err := ec.claimLister.ResourceClaims(pod.Namespace).Get(*claimName)
		if err != nil {
			if apierrors.IsNotFound(err) {
				return nil
			}
			return fmt.Errorf("retrieve claim: %v", err)
		}
		if checkOwner {
			if err := resourceclaim.IsForPod(pod, claim, ec.features.WorkloadResourceClaims); err != nil {
				return err
			}
		}
		if claim.Status.Allocation != nil &&
			!resourceclaim.IsReservedForPod(pod, claim, ec.features.WorkloadResourceClaims) &&
			resourceclaim.CanBeReserved(claim) {
			bindTo, err := resourceclaim.BindTo(pod, podGroup, &podClaim)
			if err != nil {
				return err
			}
			if isPodGroupClaim(bindTo) && !ec.features.WorkloadResourceClaims {
				// 特性关闭时跳过 PodGroup claim 的预留。用较低的日志级别（V3）
				// 记录，方便生产集群管理员定位长期分配未预留的问题。
				logger.V(3).Info("Not reserving PodGroup ResourceClaim because feature is disabled", "feature", features.DRAWorkloadResourceClaims, "resourceClaim", klog.KObj(claim))
				continue
			}
			logger.V(5).Info("Reserve claim", "resourceClaim", klog.KObj(claim), "reservedForResource", bindTo.Resource)
			if err := ec.reserveFor(ctx, bindTo, claim); err != nil {
				return err
			}
		}
	}

	return nil
}

// handleClaim 处理 Pod 的单个 ResourceClaim 声明：
//   - 若状态中已有对应 claim 且归属正确，直接返回；
//   - 否则查找孤儿 claim，或从模板创建一个（以 Pod 为 owner）；
//   - 新 claim 记录到 newPodClaims，由调用方批量更新 Pod 状态。
func (ec *Controller) handleClaim(ctx context.Context, pod *v1.Pod, podGroup *schedulingapi.PodGroup, podClaim v1.PodResourceClaim, newPodClaims *map[string]string) error {
	logger := klog.LoggerWithValues(klog.FromContext(ctx), "podClaim", podClaim.Name)
	ctx = klog.NewContext(ctx, logger)
	logger.V(5).Info("Checking", "podClaim", podClaim.Name)

	// 即使没有模板也要调用 Name：它负责识别客户端不认识的未来 API 字段，
	// 避免新增字段时控制器静默跳过本应处理的 claim。
	claimName, mustCheckOwner, err := resourceclaim.Name(pod, &podClaim)
	switch {
	case errors.Is(err, resourceclaim.ErrClaimNotFound):
		// 尚未解析出名称，继续往下走创建逻辑。
	case err != nil:
		return fmt.Errorf("checking for claim before creating it: %v", err)
	case claimName == nil:
		// 已判定该 claim 无需创建。
		return nil
	case *claimName != "":
		claimName := *claimName
		// 状态里已记录该 claim，正常情况下它应存在；若被误删则需要重建。
		checkClaimOwnedByPod := func(claim *resourceapi.ResourceClaim) bool {
			if mustCheckOwner {
				if err := resourceclaim.IsForPod(pod, claim, ec.features.WorkloadResourceClaims); err != nil {
					// 归属已被篡改，不能复用，记录原因并当作不存在处理。
					logger.Error(err, "Claim that was created for the pod is no longer owned by the pod, creating a new one",
						"podClaim", podClaim.Name, "resourceClaim", claimName)
					return false
				}
			}

			return true
		}

		exists, err := ec.claimExists(ctx, pod.Namespace, claimName, checkClaimOwnedByPod)
		if err != nil {
			return fmt.Errorf("checking claim existence: %w", err)
		}
		if exists {
			// 已存在且归属正确，无需创建。
			logger.V(5).Info("Claim already created", "podClaim", podClaim.Name, "resourceClaim", claimName)
			return nil
		}
	}

	templateName := podClaim.ResourceClaimTemplateName
	if templateName == nil {
		// 没有模板且 claim 不存在，只能由用户创建，控制器无事可做。
		return nil
	}

	bindTo, err := resourceclaim.BindTo(pod, podGroup, &podClaim)
	if err != nil {
		return err
	}
	var claim *resourceapi.ResourceClaim
	if isPodGroupClaim(bindTo) {
		if !ec.features.WorkloadResourceClaims {
			// 特性关闭时该 claim 永远无法正确创建，标记不可重试避免无意义循环。
			return nonRetryableError{fmt.Errorf("claim %s is a PodGroup claim but the %s feature is disabled", podClaim.Name, features.DRAWorkloadResourceClaims)}
		}
		claim, err = ec.findPodGroupResourceClaim(podGroup, schedulingapi.PodGroupResourceClaim{
			Name: podClaim.Name,
		})
		if err != nil {
			return fmt.Errorf("finding ResourceClaim for claim %s in PodGroup %s/%s failed: %w", podClaim.Name, podGroup.Namespace, podGroup.Name, err)
		}
	} else {
		// 创建前先找孤儿 claim：控制器可能已创建成功但在更新 Pod 状态前失败，
		// 若不查找就会重复创建。
		claim, err = ec.findPodResourceClaim(pod, podClaim)
		if err != nil {
			return fmt.Errorf("finding ResourceClaim for claim %s in pod %s/%s failed: %w", podClaim.Name, pod.Namespace, pod.Name, err)
		}

		if claim == nil {
			// 确实没有孤儿 claim，从模板创建新的。
			template, err := ec.templateLister.ResourceClaimTemplates(pod.Namespace).Get(*templateName)
			if err != nil {
				return fmt.Errorf("resource claim template %q: %w", *templateName, err)
			}

			if !ec.features.AdminAccess && needsAdminAccess(template) {
				return errors.New("admin access is requested, but the feature is disabled")
			}

			if !ec.features.PrioritizedList && hasPrioritizedList(template) {
				return errors.New("template includes a prioritized list of subrequests, but the feature is disabled")
			}

			// 以 Pod 为 owner 创建 claim，生成名前缀为 <pod>-<claim name>，
			// 便于定位归属并让多个 claim 名称互不冲突。
			isTrue := true
			annotations := maps.Clone(template.Spec.ObjectMeta.Annotations)
			if annotations == nil {
				annotations = make(map[string]string)
			}
			annotations[resourceapi.PodResourceClaimAnnotation] = podClaim.Name
			generateName := pod.Name + "-" + podClaim.Name + "-"
			// 名称上限 63 字符，预留连字符和 5 个随机字符。
			maxBaseLen := 57
			if len(generateName) > maxBaseLen {
				// 不能交给 apiserver 从尾部截断，否则 Pod 名过长时 claim 名信息会全丢。
				// 这里按两段各自长度比例截断，保留双方的可辨识信息。
				generateName = pod.Name[0:len(pod.Name)*maxBaseLen/len(generateName)] +
					"-" +
					podClaim.Name[0:len(podClaim.Name)*maxBaseLen/len(generateName)]
			}
			claim = &resourceapi.ResourceClaim{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: generateName,
					OwnerReferences: []metav1.OwnerReference{
						{
							APIVersion: "v1",
							Kind:       "Pod",
							Name:       pod.Name,
							UID:        pod.UID,
							Controller: &isTrue,
						},
					},
					Annotations: annotations,
					Labels:      template.Spec.ObjectMeta.Labels,
				},
				Spec: template.Spec.Spec,
			}
			getMetricLabels := getClaimCreateMetricLabels(claim)
			// GenerateName 的占位名不能作为日志中的对象名，先留存空名。
			claimName := claim.Name
			claim, err = ec.kubeClient.ResourceV1().ResourceClaims(pod.Namespace).Create(ctx, claim, metav1.CreateOptions{})
			if err != nil {
				resourceclaimmetrics.ResourceClaimCreate.WithLabelValues(getMetricLabels("failure")...).Inc()
				return fmt.Errorf("create ResourceClaim %s: %w", claimName, err)
			}
			resourceclaimmetrics.ResourceClaimCreate.WithLabelValues(getMetricLabels("success")...).Inc()
			logger.V(4).Info("Created ResourceClaim", "claim", klog.KObj(claim), "pod", klog.KObj(pod))
			// 立即写入变更缓存，防止 informer 尚未同步时下一次同步重复创建。
			ec.claimCache.Mutation(claim)
		}
	}

	// 把新 claim 交给调用方，批量更新 Pod 状态。
	if claim != nil {
		if *newPodClaims == nil {
			*newPodClaims = make(map[string]string)
		}
		(*newPodClaims)[podClaim.Name] = claim.Name
	}

	return nil
}

// needsAdminAccess 判断模板中是否有请求管理员权限的精确设备请求。
func needsAdminAccess(claimTemplate *resourceapi.ResourceClaimTemplate) bool {
	for _, request := range claimTemplate.Spec.Spec.Devices.Requests {
		if request.Exactly != nil && ptr.Deref(request.Exactly.AdminAccess, false) {
			return true
		}
	}
	return false
}

// hasPrioritizedList 判断模板是否使用了 FirstAvailable 优先子请求列表。
func hasPrioritizedList(claimTemplate *resourceapi.ResourceClaimTemplate) bool {
	for _, request := range claimTemplate.Spec.Spec.Devices.Requests {
		if len(request.FirstAvailable) > 0 {
			return true
		}
	}
	return false
}

// findPodResourceClaim 查找注解与 owner 都匹配指定 Pod claim 的 ResourceClaim。
func (ec *Controller) findPodResourceClaim(pod *v1.Pod, podClaim v1.PodResourceClaim) (*resourceapi.ResourceClaim, error) {
	// 通过 owner 索引查询，保证只返回 Pod 自己拥有的 claim。
	claims, err := ec.claimCache.ByIndex(claimPodOwnerIndex, pod.Namespace+"/"+string(pod.UID))
	if err != nil {
		return nil, err
	}
	for _, claimObj := range claims {
		claim, ok := claimObj.(*resourceapi.ResourceClaim)
		if !ok {
			return nil, fmt.Errorf("unexpected object of type %T returned by claim cache", claimObj)
		}
		podClaimName, ok := claim.Annotations[resourceapi.PodResourceClaimAnnotation]
		// 没有注解说明不是控制器自动生成的 claim，跳过。
		if !ok {
			continue
		}

		// 注解中的 pod claim 名称不匹配，不是我们要找的那个。
		if podClaimName != podClaim.Name {
			continue
		}

		// 正常只会有一个匹配；若有多个，取第一个，其余等 Pod 删除时由 GC 一并清理。
		return claim, nil
	}

	return nil, nil
}

// reserveFor 把消费者追加到 claim 的 ReservedFor 并通过 UpdateStatus 写回。
// 使用完整对象更新而非 patch，确保期间若有其他变更则本次更新失败，避免覆盖。
func (ec *Controller) reserveFor(ctx context.Context, consumer resourceapi.ResourceClaimConsumerReference, claim *resourceapi.ResourceClaim) error {
	claim = claim.DeepCopy()
	claim.Status.ReservedFor = append(claim.Status.ReservedFor, consumer)
	if _, err := ec.kubeClient.ResourceV1().ResourceClaims(claim.Namespace).UpdateStatus(ctx, claim, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("reserve claim %s: %w", klog.KObj(claim), err)
	}
	return nil
}

// syncPodGroup 同步单个 PodGroup：为其模板 claim 创建 ResourceClaim，
// 并把结果批量写入 PodGroup 状态。
func (ec *Controller) syncPodGroup(ctx context.Context, namespace, name string) error {
	logger := klog.LoggerWithValues(klog.FromContext(ctx), "podgroup", klog.KRef(namespace, name))
	ctx = klog.NewContext(ctx, logger)
	podGroup, err := ec.podGroupLister.PodGroups(namespace).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.V(5).Info("Nothing to do for PodGroup, it is gone")
			return nil
		}
		return err
	}

	// 正在删除的 PodGroup 不再创建 claim。
	if podGroup.DeletionTimestamp != nil {
		logger.V(5).Info("Nothing to do for PodGroup, it is marked for deletion")
		return nil
	}

	var newPodGroupClaims map[string]string
	for _, podGroupClaim := range podGroup.Spec.ResourceClaims {
		if err := ec.handlePodGroupClaim(ctx, podGroup, podGroupClaim, &newPodGroupClaims); err != nil {
			if ec.recorder != nil {
				ec.recorder.Event(podGroup, v1.EventTypeWarning, "FailedResourceClaimCreation", fmt.Sprintf("PodGroupResourceClaim %s: %v", podGroupClaim.Name, err))
			}
			return fmt.Errorf("PodGroup %s/%s, PodGroupResourceClaim %s: %w", namespace, name, podGroupClaim.Name, err)
		}
	}

	if newPodGroupClaims != nil {
		// 用 server-side apply 将新建 claim 信息写入 PodGroup 状态。
		statuses := make([]*schedulingapply.PodGroupResourceClaimStatusApplyConfiguration, 0, len(newPodGroupClaims))
		for podGroupClaimName, resourceClaimName := range newPodGroupClaims {
			statuses = append(statuses, schedulingapply.PodGroupResourceClaimStatus().WithName(podGroupClaimName).WithResourceClaimName(resourceClaimName))
		}
		podGroupApply := schedulingapply.PodGroup(name, namespace).WithStatus(schedulingapply.PodGroupStatus().WithResourceClaimStatuses(statuses...))
		if _, err := ec.kubeClient.SchedulingV1beta1().PodGroups(namespace).ApplyStatus(ctx, podGroupApply, metav1.ApplyOptions{FieldManager: fieldManager, Force: true}); err != nil {
			return fmt.Errorf("update PodGroup %s/%s ResourceClaimStatuses: %w", namespace, name, err)
		}
	}

	return nil
}

// handlePodGroupClaim 处理 PodGroup 的单个 ResourceClaim 声明，
// 逻辑与 handleClaim 类似，但 owner 是 PodGroup 而非 Pod。
func (ec *Controller) handlePodGroupClaim(ctx context.Context, podGroup *schedulingapi.PodGroup, podGroupClaim schedulingapi.PodGroupResourceClaim, newPodGroupClaims *map[string]string) error {
	logger := klog.LoggerWithValues(klog.FromContext(ctx), "podGroupClaim", podGroupClaim.Name)
	ctx = klog.NewContext(ctx, logger)
	logger.V(5).Info("Checking", "podGroupClaim", podGroupClaim.Name)

	// 即使没有模板也要调用 Name：它负责识别客户端不认识的未来 API 字段，
	// 避免新增字段时控制器静默跳过本应处理的 claim。
	claimName, mustCheckOwner, err := resourceclaim.NameFromPodGroup(podGroup, &podGroupClaim)
	switch {
	case errors.Is(err, resourceclaim.ErrClaimNotFound):
		// 尚未解析出名称，继续往下走创建逻辑。
	case err != nil:
		return fmt.Errorf("checking for claim before creating it: %w", err)
	case claimName == nil:
		// 已判定该 claim 无需创建。
		return nil
	case *claimName != "":
		claimName := *claimName
		// 状态中已记录该 claim，如被误删则需要重建。
		checkClaimOwnedByPodGroup := func(claim *resourceapi.ResourceClaim) bool {
			if mustCheckOwner {
				if err := resourceclaim.IsForPodGroup(podGroup, claim); err != nil {
					// 归属已失效，不能复用，记录原因并重建。
					logger.Error(err, "Claim that was created for the PodGroup is no longer owned by the PodGroup, creating a new one",
						"podGroupClaim", podGroupClaim.Name, "resourceClaim", claimName)
					return false
				}
			}

			return true
		}

		exists, err := ec.claimExists(ctx, podGroup.Namespace, claimName, checkClaimOwnedByPodGroup)
		if err != nil {
			return fmt.Errorf("checking claim existence: %w", err)
		}
		if exists {
			// 已存在且归属正确，无需创建。
			logger.V(5).Info("Claim already created", "podGroupClaim", podGroupClaim.Name, "resourceClaim", claimName)
			return nil
		}
	}

	templateName := podGroupClaim.ResourceClaimTemplateName
	if templateName == nil {
		// 无模板且 claim 缺失，只能由用户创建，控制器无需处理。
		return nil
	}

	// 先找可能存在的孤儿 claim：控制器或许已创建成功但未能更新状态。
	claim, err := ec.findPodGroupResourceClaim(podGroup, podGroupClaim)
	if err != nil {
		return fmt.Errorf("finding ResourceClaim for claim %s in PodGroup %s/%s failed: %w", podGroupClaim.Name, podGroup.Namespace, podGroup.Name, err)
	}

	if claim == nil {
		// 没有孤儿 claim，从模板创建新的。
		template, err := ec.templateLister.ResourceClaimTemplates(podGroup.Namespace).Get(*templateName)
		if err != nil {
			return fmt.Errorf("resource claim template %q: %w", *templateName, err)
		}

		if !ec.features.AdminAccess && needsAdminAccess(template) {
			return errors.New("admin access is requested, but the feature is disabled")
		}

		if !ec.features.PrioritizedList && hasPrioritizedList(template) {
			return errors.New("template includes a prioritized list of subrequests, but the feature is disabled")
		}

		// 以 PodGroup 为 owner 创建 claim，生成名前缀为 <podgroup>-<claim name>。
		isTrue := true
		annotations := maps.Clone(template.Spec.ObjectMeta.Annotations)
		if annotations == nil {
			annotations = make(map[string]string)
		}
		annotations[resourceapi.PodResourceClaimAnnotation] = podGroupClaim.Name
		generateName := podGroup.Name + "-" + podGroupClaim.Name + "-"
		// 名称上限 63 字符，为连字符和 5 位随机后缀留出空间。
		maxBaseLen := 57
		if len(generateName) > maxBaseLen {
			// 不能让 apiserver 从尾部截断，否则 PodGroup 名过长时 claim 名信息会丢失；
			// 按两段长度比例截断，保留双方信息。
			generateName = podGroup.Name[0:len(podGroup.Name)*maxBaseLen/len(generateName)] +
				"-" +
				podGroupClaim.Name[0:len(podGroupClaim.Name)*maxBaseLen/len(generateName)]
		}
		claim = &resourceapi.ResourceClaim{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: generateName,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion: schedulingapi.SchemeGroupVersion.String(),
						Kind:       "PodGroup",
						Name:       podGroup.Name,
						UID:        podGroup.UID,
						Controller: &isTrue,
					},
				},
				Annotations: annotations,
				Labels:      template.Spec.ObjectMeta.Labels,
			},
			Spec: template.Spec.Spec,
		}
		getMetricLabels := getClaimCreateMetricLabels(claim)
		// 记录 GenerateName 之前的空名称用于失败日志。
		claimName := claim.Name
		claim, err = ec.kubeClient.ResourceV1().ResourceClaims(podGroup.Namespace).Create(ctx, claim, metav1.CreateOptions{})
		if err != nil {
			resourceclaimmetrics.ResourceClaimCreate.WithLabelValues(getMetricLabels("failure")...).Inc()
			return fmt.Errorf("create ResourceClaim %s: %w", claimName, err)
		}
		resourceclaimmetrics.ResourceClaimCreate.WithLabelValues(getMetricLabels("success")...).Inc()
		logger.V(4).Info("Created ResourceClaim", "claim", klog.KObj(claim), "podgroup", klog.KObj(podGroup))
		// 立即写入变更缓存，避免 informer 追上前重复创建。
		ec.claimCache.Mutation(claim)
	}

	// 把新 claim 交给调用方，批量更新 Pod 状态。
	if *newPodGroupClaims == nil {
		*newPodGroupClaims = make(map[string]string)
	}
	(*newPodGroupClaims)[podGroupClaim.Name] = claim.Name

	return nil
}

// checkClaimOwnerFunc 校验 claim 是否仍由预期的 Pod/PodGroup 拥有，
// 返回 false 时调用方将视其为失效并重新创建。
type checkClaimOwnerFunc func(claim *resourceapi.ResourceClaim) bool

// claimExists 判断指定 ResourceClaim 是否仍存在且归属预期对象：
//   - 先查变更缓存，未命中再回退到 API server；
//   - 返回 false 表示 claim 已消失（如被误删）或归属失效，调用方应重新创建。
func (ec *Controller) claimExists(ctx context.Context, namespace, claimName string, checkClaimOwner checkClaimOwnerFunc) (bool, error) {
	key := cache.NewObjectName(namespace, claimName)

	// 变更缓存可能暂时返回一个在 apiserver 上已删除的 claim；
	// 缓存自身的 TTL 校验和 informer 事件最终会纠正这种短暂不一致。
	claim, exists, err := ec.claimCache.GetByKey(key.String())
	if err != nil && !apierrors.IsNotFound(err) {
		return false, err
	}

	if !exists {
		// 缓存和 informer 中都没有（如被驱逐），回退到 API server 确认。
		claim, err = ec.kubeClient.ResourceV1().ResourceClaims(namespace).Get(ctx, claimName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return false, nil
			}

			return false, err
		}
	}

	if !checkClaimOwner(claim.(*resourceapi.ResourceClaim)) {
		return false, nil
	}

	return true, nil
}

// findPodGroupResourceClaim 查找注解与 owner 都匹配指定 PodGroup claim 的 ResourceClaim。
func (ec *Controller) findPodGroupResourceClaim(podGroup *schedulingapi.PodGroup, podGroupClaim schedulingapi.PodGroupResourceClaim) (*resourceapi.ResourceClaim, error) {
	// 通过 owner 索引查询，只返回 PodGroup 拥有的 claim。
	claims, err := ec.claimCache.ByIndex(claimPodGroupOwnerIndex, podGroup.Namespace+"/"+string(podGroup.UID))
	if err != nil {
		return nil, err
	}
	for _, claimObj := range claims {
		claim, ok := claimObj.(*resourceapi.ResourceClaim)
		if !ok {
			return nil, fmt.Errorf("unexpected object of type %T returned by claim cache", claimObj)
		}
		podGroupClaimName, ok := claim.Annotations[resourceapi.PodResourceClaimAnnotation]
		// 没有注解说明不是控制器自动生成的 claim，跳过。
		if !ok {
			continue
		}

		// 注解名称与当前 PodGroup claim 不符，跳过。
		if podGroupClaimName != podGroupClaim.Name {
			continue
		}

		// 正常情况下只有一个匹配；多个时取第一个，其余在 PodGroup 删除时由 GC 清理。
		return claim, nil
	}
	return nil, nil
}

// syncClaim 校验并清理 claim 的 ReservedFor 条目：
//   - Pod/PodGroup 消费者被删除、被替换或已结束时移除对应条目；
//   - 未知消费者保留，避免误删尚不支持的类型；
//   - 最后一个消费者离开时清空 allocation、移除 finalizer，并在必要时删除生成的 claim。
func (ec *Controller) syncClaim(ctx context.Context, namespace, name string) error {
	logger := klog.LoggerWithValues(klog.FromContext(ctx), "claim", klog.KRef(namespace, name))
	ctx = klog.NewContext(ctx, logger)
	claim, err := ec.claimLister.ResourceClaims(namespace).Get(name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logger.V(5).Info("Nothing to do for claim, it is gone")
			return nil
		}
		return err
	}

	// 逐条校验 ReservedFor：仅处理本控制器能识别的 Pod/PodGroup 消费者，
	// 其他类型原样保留，防止误删未知消费者的预留。
	remaining := make([]resourceapi.ResourceClaimConsumerReference, 0, len(claim.Status.ReservedFor))
	for _, reservedFor := range claim.Status.ReservedFor {
		if reservedFor.APIGroup == "" &&
			reservedFor.Resource == "pods" {
			// Pod 消费者分三类处理：
			//   - 缓存里有 -> 保留，直到收到删除事件；
			//   - 缓存里没有但曾见过 -> 已删除，移除；
			//   - 既不在缓存也没见过 -> 先查 API server 再决定。

			keepEntry := true

			// 用 LRU 缓存记录已删除 Pod 是优化：否则每个被删 Pod 都要调用 API server
			// 确认；命中缓存即可直接判定，省去昂贵的远程查询。
			if ec.deletedObjects.Has(reservedFor.UID) {
				// 本地缓存查询最快，先检查已删除记录。
				keepEntry = false
			} else {
				pod, err := ec.podLister.Pods(claim.Namespace).Get(reservedFor.Name)
				switch {
				case err != nil && !apierrors.IsNotFound(err):
					return err
				case err != nil:
					// informer 可能还没同步到该 Pod。调度期间误删预留会很糟糕，
					// 必须向 API server 二次确认后才能移除。
					pod, err := ec.kubeClient.CoreV1().Pods(claim.Namespace).Get(ctx, reservedFor.Name, metav1.GetOptions{})
					if err != nil && !apierrors.IsNotFound(err) {
						return err
					}
					if pod == nil || pod.UID != reservedFor.UID {
						logger.V(6).Info("Remove reservation because pod is gone or got replaced", "pod", klog.KObj(pod), "claim", klog.KRef(namespace, name))
						keepEntry = false
					}
				case pod.UID != reservedFor.UID:
					logger.V(6).Info("Remove reservation because pod got replaced with new instance", "pod", klog.KObj(pod), "claim", klog.KRef(namespace, name))
					keepEntry = false
				case isPodDone(pod):
					logger.V(6).Info("Remove reservation because pod will not run anymore", "pod", klog.KObj(pod), "claim", klog.KRef(namespace, name))
					keepEntry = false
				}
			}

			if keepEntry {
				remaining = append(remaining, reservedFor)
			}
			continue
		}
		if ec.features.WorkloadResourceClaims &&
			reservedFor.APIGroup == schedulingapi.GroupName &&
			reservedFor.Resource == "podgroups" {
			// PodGroup 消费者同样分三类：缓存里保留、曾见过则删除、
			// 都没见过则查 API server 确认，避免调度期间误删。

			keepEntry := true

			// LRU 缓存记录已删除 PodGroup，避免为每个删除事件都调用 API server 确认。
			if ec.deletedObjects.Has(reservedFor.UID) {
				// 先查本地缓存，命中即可确定已删除。
				keepEntry = false
			} else {
				podGroup, err := ec.podGroupLister.PodGroups(claim.Namespace).Get(reservedFor.Name)
				switch {
				case err != nil && !apierrors.IsNotFound(err):
					return err
				case err != nil:
					// informer 可能尚未同步；调度器正在为其 Pod 调度时误删预留后果严重，
					// 因此必须向 API server 确认。
					podGroup, err := ec.kubeClient.SchedulingV1beta1().PodGroups(claim.Namespace).Get(ctx, reservedFor.Name, metav1.GetOptions{})
					if err != nil && !apierrors.IsNotFound(err) {
						return err
					}
					if podGroup == nil || podGroup.UID != reservedFor.UID {
						logger.V(6).Info("Remove reservation because PodGroup is gone or got replaced", "podgroup", klog.KObj(podGroup), "claim", klog.KRef(namespace, name))
						keepEntry = false
					}
				case podGroup.UID != reservedFor.UID:
					logger.V(6).Info("Remove reservation because PodGroup got replaced with new instance", "podgroup", klog.KObj(podGroup), "claim", klog.KRef(namespace, name))
					keepEntry = false
				}
			}

			if keepEntry {
				remaining = append(remaining, reservedFor)
			}
			continue
		}

		// 未知消费者类型无法校验，保守保留，避免误删尚不支持的预留。
		remaining = append(remaining, reservedFor)
	}

	builtinControllerFinalizer := slices.Index(claim.Finalizers, resourceapi.Finalizer)
	logger.V(5).Info("Claim reserved for counts", "currentCount", len(claim.Status.ReservedFor), "claim", klog.KRef(namespace, name), "updatedCount", len(remaining), "builtinController", builtinControllerFinalizer >= 0)
	if len(remaining) < len(claim.Status.ReservedFor) {
		// 用完整 UpdateStatus 而非 patch：若期间有其他变更则冲突失败，
		// 避免覆盖调度器或其他控制器刚写入的内容。
		claim := claim.DeepCopy()
		claim.Status.ReservedFor = remaining

		// DRA 总是延迟分配，并在最后一个消费者停止使用时立即释放：
		// 这样未来的消费者可以重新分配，而不必被调度到上一个消费者的节点，
		// 同时底层资源也能被其他 claim 使用。
		//
		// 释放必须由"正在使用 -> 不再使用"的转变触发：DRA driver 不保证
		// 同时设置 reservedFor 与 allocation，因此当前未使用的 claim 不应被释放。
		//
		// 对为 Pod 创建的 claim 来说这无关紧要（Pod 结束时控制器会删除它），
		// 但一并触发释放也无妨，省去特判使逻辑更简单。
		if len(remaining) == 0 {
			// 兜底检查：目前只有调度器通过内置 finalizer 分配 claim，
			// 不应存在没有该 finalizer 却已分配的 claim（经典 DRA 已移除）。
			if builtinControllerFinalizer >= 0 {
				// 由调度器使用结构化参数分配，清空 allocation 即完成释放。
				claim.Status.Allocation = nil
			}
		}

		claim, err := ec.kubeClient.ResourceV1().ResourceClaims(claim.Namespace).UpdateStatus(ctx, claim, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		logger.V(5).Info("Removed consumers", "claim", klog.KRef(namespace, name), "currentCount", len(claim.Status.ReservedFor), "allocated", claim.Status.Allocation != nil)

		// UpdateStatus 可能触发 finalizer 变化，须重新查找其索引。
		builtinControllerFinalizer := slices.Index(claim.Finalizers, resourceapi.Finalizer)
		if builtinControllerFinalizer >= 0 && claim.Status.Allocation == nil {
			claim.Finalizers = slices.Delete(claim.Finalizers, builtinControllerFinalizer, builtinControllerFinalizer+1)
			if _, err := ec.kubeClient.ResourceV1().ResourceClaims(claim.Namespace).Update(ctx, claim, metav1.UpdateOptions{}); err != nil {
				return err
			}
			logger.V(5).Info("Removed finalizer after removing consumers", "claim", klog.KRef(namespace, name))
		}
	} else if builtinControllerFinalizer >= 0 && claim.DeletionTimestamp != nil && len(remaining) == 0 {
		// 分支：没有条目被移除，但 claim 正在删除且已无消费者，需清理 finalizer。
		claim := claim.DeepCopy()
		if claim.Status.Allocation != nil {
			// 立即分配的 claim 可能停止使用后仍保持分配，随后被删除；
			// 与上面的分支一样需要先清空 allocation。
			claim.Status.Allocation = nil
			var err error
			claim, err = ec.kubeClient.ResourceV1().ResourceClaims(claim.Namespace).UpdateStatus(ctx, claim, metav1.UpdateOptions{})
			if err != nil {
				return err
			}
			logger.V(5).Info("Removed allocation because not needed", "claim", klog.KRef(namespace, name))
		}
		// 无论是否已分配，都要移除 finalizer 才能完成对象删除。
		claim.Finalizers = slices.Delete(claim.Finalizers, builtinControllerFinalizer, builtinControllerFinalizer+1)
		_, err := ec.kubeClient.ResourceV1().ResourceClaims(claim.Namespace).Update(ctx, claim, metav1.UpdateOptions{})
		if err != nil {
			return err
		}
		logger.V(5).Info("Removed finalizer because not needed", "claim", klog.KRef(namespace, name))
	}

	if len(remaining) == 0 {
		// 无预留时，为 Pod 生成的 claim 若其 owner Pod 已结束或被替换，可提前删除。
		// 通常由 GC 负责，但 Pod 对象本身可能迟迟不被删除，故此处主动清理。
		podName, podUID := owningPod(claim)
		if podName != "" {
			pod, err := ec.podLister.Pods(claim.Namespace).Get(podName)
			switch {
			case err == nil:
				// UID 变化说明 Pod 被替换，或已进入终态，claim 不再需要。
				if pod.UID != podUID || isPodDone(pod) {
					// 已确认 owner Pod 不再需要该 claim，直接删除。
					err := ec.kubeClient.ResourceV1().ResourceClaims(claim.Namespace).Delete(ctx, claim.Name, metav1.DeleteOptions{})
					if err != nil {
						return fmt.Errorf("delete unused generated claim %s: %w", klog.KObj(claim), err)
					}
					logger.V(5).Info("Deleted unused generated claim", "claim", klog.KObj(claim), "pod", klog.KObj(pod))
				} else {
					logger.V(6).Info("Wrong pod content, not deleting claim", "claim", klog.KObj(claim), "podUID", podUID, "podContent", pod)
				}
			case apierrors.IsNotFound(err):
				// lister 里可能暂时没有该 Pod（informer 滞后），不值得为此调用 API server；
				// 若 Pod 真的已不存在，交由 GC 处理。
				logger.V(5).Info("Pod for claim not found", "claim", klog.KObj(claim), "pod", klog.KRef(claim.Namespace, podName))
			default:
				return fmt.Errorf("lookup pod: %v", err)
			}
		} else {
			logger.V(5).Info("Claim not generated for a pod", "claim", klog.KObj(claim))
		}
	}

	return nil
}

// getPodGroup 返回 Pod 所属的 PodGroup：未声明返回 nil；
// 声明了但通用工作负载特性关闭时返回不可重试错误。
func (ec *Controller) getPodGroup(pod *v1.Pod) (*schedulingapi.PodGroup, error) {
	if pod.Spec.SchedulingGroup == nil ||
		pod.Spec.SchedulingGroup.PodGroupName == nil {
		// Pod 不属于任何 PodGroup。
		return nil, nil
	}
	if !ec.features.GenericWorkload {
		return nil, nonRetryableError{fmt.Errorf("Pod is a member of PodGroup %s but %s feature is disabled", *pod.Spec.SchedulingGroup.PodGroupName, features.GenericWorkload)}
	}
	return ec.podGroupLister.PodGroups(pod.Namespace).Get(*pod.Spec.SchedulingGroup.PodGroupName)
}

// owningPod 返回 claim 的控制型 Pod owner 的名称与 UID，没有则返回空串。
func owningPod(claim *resourceapi.ResourceClaim) (string, types.UID) {
	for _, owner := range claim.OwnerReferences {
		if ptr.Deref(owner.Controller, false) &&
			owner.APIVersion == "v1" &&
			owner.Kind == "Pod" {
			return owner.Name, owner.UID
		}
	}
	return "", ""
}

// podResourceClaimTemplateIndexFunc 为 Pod 建立其引用的 ResourceClaimTemplate
// 键（namespace/name）索引，便于模板变化时快速找到受影响的 Pod。
func podResourceClaimTemplateIndexFunc(obj any) ([]string, error) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return nil, nil
	}

	keySet := sets.New[string]()

	for _, podClaim := range pod.Spec.ResourceClaims {
		if podClaim.ResourceClaimTemplateName != nil {
			resourceTemplate := *podClaim.ResourceClaimTemplateName
			keySet.Insert(fmt.Sprintf("%s/%s", pod.Namespace, resourceTemplate))
		}
	}

	return sets.List(keySet), nil
}

// podResourceClaimIndexFunc 返回 Pod 使用的 ResourceClaim 键（namespace/name）。
// 有意不索引 status 中的扩展 claim：它们由调度器在 PreBind 阶段创建并预留，
// 清理直接由 Pod 或 claim 事件触发，无需本索引。
func podResourceClaimIndexFunc(obj any) ([]string, error) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return nil, nil
	}
	keySet := sets.New[string]()
	for _, podClaim := range pod.Spec.ResourceClaims {
		claimName, _, err := resourceclaim.Name(pod, &podClaim)
		if err != nil || claimName == nil {
			// 索引函数不允许返回错误（调用方会 panic）。claim 尚未创建或 API 未知时，
			// 选择不建立索引，而不是报错。
			continue
		}
		keySet.Insert(fmt.Sprintf("%s/%s", pod.Namespace, *claimName))
	}
	return sets.List(keySet), nil
}

// podGroupMemberIndexFunc 返回 Pod 所属 PodGroup 的键（namespace/name），
// 用于按 PodGroup 查找其成员 Pod。
func podGroupMemberIndexFunc(obj any) ([]string, error) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return nil, nil
	}
	var keys []string
	if pod.Spec.SchedulingGroup != nil && pod.Spec.SchedulingGroup.PodGroupName != nil {
		keys = []string{pod.Namespace + "/" + *pod.Spec.SchedulingGroup.PodGroupName}
	}
	return keys, nil
}

// podGroupResourceClaimTemplateIndexFunc 返回 PodGroup 引用的模板键（namespace/name）。
func podGroupResourceClaimTemplateIndexFunc(obj any) ([]string, error) {
	podGroup, ok := obj.(*schedulingapi.PodGroup)
	if !ok {
		return nil, nil
	}

	keySet := sets.New[string]()

	for _, podGroupClaim := range podGroup.Spec.ResourceClaims {
		if podGroupClaim.ResourceClaimTemplateName != nil {
			resourceTemplate := *podGroupClaim.ResourceClaimTemplateName
			keySet.Insert(fmt.Sprintf("%s/%s", podGroup.Namespace, resourceTemplate))
		}
	}

	return sets.List(keySet), nil
}

// podGroupResourceClaimIndexFunc 返回 PodGroup 使用的 ResourceClaim 键（namespace/name）。
func podGroupResourceClaimIndexFunc(obj any) ([]string, error) {
	podGroup, ok := obj.(*schedulingapi.PodGroup)
	if !ok {
		return nil, nil
	}
	keySet := sets.New[string]()
	for _, podGroupClaim := range podGroup.Spec.ResourceClaims {
		claimName, _, err := resourceclaim.NameFromPodGroup(podGroup, &podGroupClaim)
		if err != nil || claimName == nil {
			// 索引函数不允许返回错误（调用方会 panic）。claim 尚未创建或 API 未知时，
			// 选择不建立索引，而不是报错。
			continue
		}
		keySet.Insert(fmt.Sprintf("%s/%s", podGroup.Namespace, *claimName))
	}
	return sets.List(keySet), nil
}

// isPodDone 判断 Pod 是否确定已结束（终态相位），或已被删除且尚未调度，
// 两种情况都意味着容器不会再运行。
func isPodDone(pod *v1.Pod) bool {
	return podutil.IsPodPhaseTerminal(pod.Status.Phase) ||
		// 已删除且从未调度，容器不可能再运行。
		pod.DeletionTimestamp != nil && pod.Spec.NodeName == ""
}

// claimPodOwnerIndexFunc 返回 claim 的 Pod owner 的 "namespace/UID" 索引键，
// 正常情况下只会有一个 owner。
func claimPodOwnerIndexFunc(obj any) ([]string, error) {
	claim, ok := obj.(*resourceapi.ResourceClaim)
	if !ok {
		return nil, nil
	}
	var keys []string
	for _, owner := range claim.OwnerReferences {
		if owner.Controller != nil &&
			*owner.Controller &&
			owner.APIVersion == "v1" &&
			owner.Kind == "Pod" {
			keys = append(keys, claim.Namespace+"/"+string(owner.UID))
		}
	}
	return keys, nil
}

// claimPodGroupOwnerIndexFunc 返回 claim 的 PodGroup owner 的 "namespace/UID" 索引键，
// 正常情况下只会有一个 owner。
func claimPodGroupOwnerIndexFunc(obj any) ([]string, error) {
	claim, ok := obj.(*resourceapi.ResourceClaim)
	if !ok {
		return nil, nil
	}
	controller := metav1.GetControllerOfNoCopy(claim)
	if controller == nil || controller.Kind != "PodGroup" || !strings.HasPrefix(controller.APIVersion, schedulingAPIGroupPrefix) {
		return nil, nil
	}
	return []string{claim.Namespace + "/" + string(controller.UID)}, nil
}

// getClaimCreateMetricLabels 返回一个函数，为 ResourceClaimCreate 指标生成标签：
// 状态、是否请求管理员权限、owner 的 API 组和 Kind。
func getClaimCreateMetricLabels(claim *resourceapi.ResourceClaim) func(string) []string {
	return func(status string) []string {
		adminAccess := getAdminAccessMetricLabel(claim)
		ownerGroup, ownerKind := getOwnerKindMetricLabels(claim)
		return []string{status, adminAccess, ownerGroup, ownerKind}
	}
}

// getAdminAccessMetricLabel 返回 claim 是否请求管理员权限的指标标签值。
func getAdminAccessMetricLabel(claim *resourceapi.ResourceClaim) string {
	if claim == nil {
		return "false"
	}
	for _, request := range claim.Spec.Devices.Requests {
		// FirstAvailable 中的子请求不允许请求管理员权限，故只检查 Exactly。
		if request.Exactly != nil && ptr.Deref(request.Exactly.AdminAccess, false) {
			return "true"
		}
	}
	return "false"
}

// getOwnerKindMetricLabels 从 claim 的 controller owner 提取 API 组与 Kind，
// 用于给指标打上来源标签；无 owner 时返回空串。
func getOwnerKindMetricLabels(claim *resourceapi.ResourceClaim) (group string, kind string) {
	controller := metav1.GetControllerOfNoCopy(claim)
	if controller == nil {
		return
	}
	group, _, found := strings.Cut(controller.APIVersion, "/")
	if !found {
		group = ""
	}
	return group, controller.Kind
}

// isPodGroupClaim 判断消费者引用是否指向调度组的 podgroups 资源。
func isPodGroupClaim(bindTo resourceapi.ResourceClaimConsumerReference) bool {
	return bindTo.APIGroup == schedulingapi.GroupName && bindTo.Resource == "podgroups"
}

// newCustomCollector 创建一个稳定的指标收集器，按 claim 的分配状态、
// 管理员权限和来源统计 ResourceClaim 数量。
func newCustomCollector(rcLister resourcelisters.ResourceClaimLister, adminAccessFunc func(*resourceapi.ResourceClaim) string, logger klog.Logger) metrics.StableCollector {
	return &customCollector{
		rcLister:        rcLister,
		adminAccessFunc: adminAccessFunc,
		logger:          logger,
	}
}

// customCollector 实现 metrics.StableCollector，从 lister 读取 claim 并汇总指标。
type customCollector struct {
	// BaseStableCollector 提供稳定性描述与收集的默认实现。
	metrics.BaseStableCollector
	// rcLister 列出全部 claim，作为指标数据来源。
	rcLister resourcelisters.ResourceClaimLister
	// adminAccessFunc 计算 claim 的管理员权限标签。
	adminAccessFunc func(*resourceapi.ResourceClaim) string
	// logger 用于记录列取失败。
	logger klog.Logger
}

// 编译期断言：customCollector 必须实现 StableCollector 接口。
var _ metrics.StableCollector = &customCollector{}

// DescribeWithStability 声明本收集器贡献的指标描述符。
func (collector *customCollector) DescribeWithStability(ch chan<- *metrics.Desc) {
	ch <- controllermetrics.NumResourceClaimsDesc
}

// CollectWithStability 收集各维度下 ResourceClaim 的数量指标。
func (collector *customCollector) CollectWithStability(ch chan<- metrics.Metric) {
	rcMetrics := make(map[controllermetrics.NumResourceClaimLabels]int)
	rcList, err := collector.rcLister.List(labels.Everything())
	if err != nil {
		collector.logger.Error(err, "failed to list resource claims for metrics collection")
		return
	}
	for _, rc := range rcList {
		// 根据 allocation 是否为空判定 claim 是否已分配。
		allocated := "false"
		if rc.Status.Allocation != nil {
			allocated = "true"
		}
		adminAccess := collector.adminAccessFunc(rc)
		source := ""
		if val, ok := rc.Annotations[resourceapi.ExtendedResourceClaimAnnotation]; ok && val == "true" {
			source = "extended_resource"
		} else if val, ok := rc.Annotations[resourceapi.PodResourceClaimAnnotation]; ok && val != "" {
			source = "resource_claim_template"
		}
		rcMetrics[controllermetrics.NumResourceClaimLabels{Allocated: allocated, AdminAccess: adminAccess, Source: source}]++
	}
	for rcLabels, count := range rcMetrics {
		ch <- metrics.NewLazyConstMetric(
			controllermetrics.NumResourceClaimsDesc,
			metrics.GaugeValue,
			float64(count),
			rcLabels.Allocated,
			rcLabels.AdminAccess,
			rcLabels.Source,
		)
	}
}
