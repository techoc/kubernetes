/*
Copyright 2015 The Kubernetes Authors.

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

package deletion

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/discovery"
	v1clientset "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/metadata"
)

// NamespacedResourcesDeleterInterface 定义删除命名空间及其内部所有资源的接口。
type NamespacedResourcesDeleterInterface interface {
	Delete(ctx context.Context, nsName string) error
}

// NewNamespacedResourcesDeleter 构造一个 namespacedResourcesDeleter 并初始化操作能力缓存。
func NewNamespacedResourcesDeleter(ctx context.Context, nsClient v1clientset.NamespaceInterface,
	metadataClient metadata.Interface, podsGetter v1clientset.PodsGetter,
	discoverResourcesFn func() ([]*metav1.APIResourceList, error),
	finalizerToken v1.FinalizerName) NamespacedResourcesDeleterInterface {
	d := &namespacedResourcesDeleter{
		nsClient:       nsClient,
		metadataClient: metadataClient,
		podsGetter:     podsGetter,
		opCache: &operationNotSupportedCache{
			m: make(map[operationKey]bool),
		},
		discoverResourcesFn: discoverResourcesFn,
		finalizerToken:      finalizerToken,
	}
	// 构造时就通过 discovery 预填缓存，避免删除过程中反复 encountering 405 后才记录不支持。
	d.initOpCache(ctx)
	return d
}

var _ NamespacedResourcesDeleterInterface = &namespacedResourcesDeleter{}

// namespacedResourcesDeleter 负责删除指定命名空间内的全部资源。
type namespacedResourcesDeleter struct {
	// nsClient 用于操作 Namespace 对象本身（状态与 finalizer）。
	nsClient v1clientset.NamespaceInterface
	// metadataClient 是动态客户端，用于列举并删除命名空间内所有类型的资源。
	metadataClient metadata.Interface
	// podsGetter 用于读取 Pod，以估算优雅终止所需时间。
	podsGetter v1clientset.PodsGetter
	// opCache 缓存各 GVR 上支持的操作，避免对不支持 list/deletecollection 的资源反复失败重试。
	opCache             *operationNotSupportedCache
	discoverResourcesFn func() ([]*metav1.APIResourceList, error)
	// finalizerToken 是本控制器负责移除的 finalizer；
	// 只有命名空间内资源全部删完后才会移除它。
	finalizerToken v1.FinalizerName
}

// Delete 删除给定命名空间中的全部资源。删除前：
//   - 确保命名空间已设置删除时间戳（若尚未设置则不做任何事）；
//   - 确认命名空间处于 Terminating 阶段（若未标记则更新其状态）。
//
// 删除资源后：
// * 从命名空间上移除本控制器的 finalizer。
//
// 任一步骤失败都会返回错误。若已删除部分资源但仍需等待其消失，
// 返回 ResourcesRemainingError。调用方应持续调用本方法直到成功。
func (d *namespacedResourcesDeleter) Delete(ctx context.Context, nsName string) error {
	// 命名空间终止期间可能有多个控制器同时修改它，
	// 因此先读取最新状态；若已被删除则无需处理。
	namespace, err := d.nsClient.Get(ctx, nsName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	// 未设置删除时间戳说明并未进入删除流程（例如只是普通更新事件），直接返回。
	if namespace.DeletionTimestamp == nil {
		return nil
	}

	klog.FromContext(ctx).V(5).Info("Namespace controller - syncNamespace", "namespace", namespace.Name, "finalizerToken", d.finalizerToken)

	// 把状态更新为最新：若返回 NotFound，说明命名空间确实已消失。
	namespace, err = d.retryOnConflictError(ctx, namespace, d.updateNamespaceStatusFunc)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}

	// 重新读取后若删除时间戳被清空，说明删除已被取消，停止处理。
	if namespace.DeletionTimestamp.IsZero() {
		return nil
	}

	// finalizer 已被清空，说明该命名空间已终结，无需再处理。
	if finalized(namespace) {
		return nil
	}

	// 仍可能有资源需要删除，执行实际的内容清理。
	estimate, err := d.deleteAllContent(ctx, namespace)
	if err != nil {
		return err
	}
	// 仍有资源在等待删除，返回预估等待时间，让调用方稍后重试。
	if estimate > 0 {
		return &ResourcesRemainingError{estimate}
	}

	// 资源已清空，移除本控制器的 finalizer，标记命名空间可真正被删除。
	_, err = d.retryOnConflictError(ctx, namespace, d.finalizeNamespace)
	if err != nil {
		// 正常流程下不应出现 NotFound；但若两个控制器共用同一 finalizer token，
		// 对方可能已完成删除，此时视作成功。
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	return nil
}

// initOpCache 用 discovery 返回的动词信息预填操作能力缓存，
// 使不支持 list/deletecollection 的资源能在真正请求前就被跳过。
func (d *namespacedResourcesDeleter) initOpCache(ctx context.Context) {
	// 用 discovery 信息预填 opCache。
	//
	// TODO(sttts): 去掉 opCache 及其配套的 HTTP 405 判断逻辑，直接信任 discovery 信息。
	resources, err := d.discoverResourcesFn()
	if err != nil {
		utilruntime.HandleErrorWithContext(ctx, err, "Unable to get all supported resources from server")
	}
	logger := klog.FromContext(ctx)
	// 一个资源都发现不了说明 API 严重异常，记录错误后主动退出，避免空转。
	if len(resources) == 0 {
		logger.Error(err, "Unable to get any supported resources from server")
		klog.FlushAndExit(klog.ExitFlushTimeout, 1)
	}

	for _, rl := range resources {
		gv, err := schema.ParseGroupVersion(rl.GroupVersion)
		if err != nil {
			logger.Error(err, "Failed to parse GroupVersion, skipping", "groupVersion", rl.GroupVersion)
			continue
		}

		for _, r := range rl.APIResources {
			gvr := schema.GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: r.Name}
			verbs := sets.NewString([]string(r.Verbs)...)

			// 不支持 delete 的资源无法清理，记录后跳过。
			if !verbs.Has("delete") {
				logger.V(6).Info("Skipping resource because it cannot be deleted", "resource", gvr)
			}

			// 只缓存 list 与 deletecollection 的能力，供后续删除路径快速判断。
			for _, op := range []operation{operationList, operationDeleteCollection} {
				if !verbs.Has(string(op)) {
					d.opCache.setNotSupported(operationKey{operation: op, gvr: gvr})
				}
			}
		}
	}
}

// ResourcesRemainingError 告知调用方命名空间中仍有资源未完全删除。
type ResourcesRemainingError struct {
	// Estimate 是预估还需等待的秒数。
	Estimate int64
}

// Error 返回可读的剩余资源提示，包含预估等待时间。
func (e *ResourcesRemainingError) Error() string {
	return fmt.Sprintf("some content remains in the namespace, estimate %d seconds before it is removed", e.Estimate)
}

// operation 表示要在动态客户端上执行的操作类型，用于能力缓存的键。
type operation string

const (
	// deletecollection 表示批量删除整个集合。
	operationDeleteCollection operation = "deletecollection"
	// list 表示列举资源。
	operationList operation = "list"
	// 发现资源仍带 finalizer 时，默认预估 15 秒等其完成。
	finalizerEstimateSeconds = int64(15)
)

// operationKey 是操作能力缓存的键：操作类型加资源 GVR。
// operation 是要在资源上执行的操作。
type operationKey struct {
	// gvr 是目标资源的 GroupVersionResource。
	operation operation
	gvr       schema.GroupVersionResource
}

// operationNotSupportedCache 记录某资源是否不支持某个操作。
// 若 operationKey 映射到 true，表示该操作不受支持。该缓存被多个 worker 共享，因此需要加锁。
type operationNotSupportedCache struct {
	lock sync.RWMutex
	m    map[operationKey]bool
}

// isSupported 判断 key 对应的操作是否受支持；缓存未记录时默认支持。
func (o *operationNotSupportedCache) isSupported(key operationKey) bool {
	o.lock.RLock()
	defer o.lock.RUnlock()
	return !o.m[key]
}

// setNotSupported 把操作标记为不支持；写操作需加锁保护共享 map。
func (o *operationNotSupportedCache) setNotSupported(key operationKey) {
	o.lock.Lock()
	defer o.lock.Unlock()
	o.m[key] = true
}

// updateNamespaceFunc 定义对 Namespace 执行一次更新的函数签名。
type updateNamespaceFunc func(ctx context.Context, namespace *v1.Namespace) (*v1.Namespace, error)

// retryOnConflictError 在遇到冲突错误时重新读取并重试 fn。
// 若重试期间对象 UID 发生变化（对象被删除后重建），则返回错误，避免更新到错误对象。
// TODO RetryOnConflict 应当成为 client 代码中的通用能力。
func (d *namespacedResourcesDeleter) retryOnConflictError(ctx context.Context, namespace *v1.Namespace, fn updateNamespaceFunc) (result *v1.Namespace, err error) {
	latestNamespace := namespace
	for {
		// 冲突后用最新对象重试，直到成功、遇到非冲突错误或 UID 变化。
		result, err = fn(ctx, latestNamespace)
		if err == nil {
			return result, nil
		}
		if !errors.IsConflict(err) {
			return nil, err
		}
		prevNamespace := latestNamespace
		latestNamespace, err = d.nsClient.Get(ctx, latestNamespace.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		// UID 变化说明原对象已被删除并重建，继续重试可能更新到无关对象，必须中止。
		if prevNamespace.UID != latestNamespace.UID {
			return nil, fmt.Errorf("namespace uid has changed across retries")
		}
	}
}

// updateNamespaceStatusFunc 确保命名空间状态被标记为 Terminating。
func (d *namespacedResourcesDeleter) updateNamespaceStatusFunc(ctx context.Context, namespace *v1.Namespace) (*v1.Namespace, error) {
	if namespace.DeletionTimestamp.IsZero() || namespace.Status.Phase == v1.NamespaceTerminating {
		return namespace, nil
	}
	newNamespace := namespace.DeepCopy()
	newNamespace.Status.Phase = v1.NamespaceTerminating
	return d.nsClient.UpdateStatus(ctx, newNamespace, metav1.UpdateOptions{})
}

// finalized 判断命名空间的 finalizer 列表是否为空（为空表示已终结）。
func finalized(namespace *v1.Namespace) bool {
	return len(namespace.Spec.Finalizers) == 0
}

// finalizeNamespace 从命名空间上移除本控制器的 finalizer。
func (d *namespacedResourcesDeleter) finalizeNamespace(ctx context.Context, namespace *v1.Namespace) (*v1.Namespace, error) {
	namespaceFinalize := v1.Namespace{}
	namespaceFinalize.ObjectMeta = namespace.ObjectMeta
	namespaceFinalize.Spec = namespace.Spec
	finalizerSet := sets.NewString()
	// 保留其他控制器设置的 finalizer，只移除自己负责的那个。
	for i := range namespace.Spec.Finalizers {
		if namespace.Spec.Finalizers[i] != d.finalizerToken {
			finalizerSet.Insert(string(namespace.Spec.Finalizers[i]))
		}
	}
	namespaceFinalize.Spec.Finalizers = make([]v1.FinalizerName, 0, len(finalizerSet))
	for _, value := range finalizerSet.List() {
		namespaceFinalize.Spec.Finalizers = append(namespaceFinalize.Spec.Finalizers, v1.FinalizerName(value))
	}
	namespace, err := d.nsClient.Finalize(ctx, &namespaceFinalize, metav1.UpdateOptions{})
	if err != nil {
		// NotFound 说明 finalizer 已被移除（可能由对方控制器完成），视作成功。
		if errors.IsNotFound(err) {
			return namespace, nil
		}
	}
	return namespace, err
}

// deleteCollection 尝试一次性删除某 GVR 在命名空间下的全部资源。
// 第一个返回值表示服务端是否支持该操作，第二个返回值表示操作是否失败。
func (d *namespacedResourcesDeleter) deleteCollection(ctx context.Context, gvr schema.GroupVersionResource, namespace string) (bool, error) {
	logger := klog.FromContext(ctx)
	logger.V(5).Info("Namespace controller - deleteCollection", "namespace", namespace, "resource", gvr)

	key := operationKey{operation: operationDeleteCollection, gvr: gvr}
	// 缓存判定不支持时直接跳过，避免每次都发出注定失败的请求。
	if !d.opCache.isSupported(key) {
		logger.V(5).Info("Namespace controller - deleteCollection ignored since not supported", "namespace", namespace, "resource", gvr)
		return false, nil
	}

	// 使用 Background 传播策略：命名空间控制器会通用地删除所有资源，
	// 不希望 GC 插入 orphan finalizer；它会先清空命名空间内所有资源再释放命名空间本身。
	background := metav1.DeletePropagationBackground
	opts := metav1.DeleteOptions{PropagationPolicy: &background}
	err := d.metadataClient.Resource(gvr).Namespace(namespace).DeleteCollection(ctx, opts, metav1.ListOptions{})
	if err == nil {
		return true, nil
	}

	// discovery 返回的资源可能不支持任何顶层动词，此时服务端会返回 NotFound 而非 MethodNotSupported，
	// 因此两种情况都要按"不支持"处理。详见 issue #22413。
	if errors.IsMethodNotSupported(err) || errors.IsNotFound(err) {
		logger.V(5).Info("Namespace controller - deleteCollection not supported", "namespace", namespace, "resource", gvr)
		return false, nil
	}

	logger.V(5).Info("Namespace controller - deleteCollection unexpected error", "namespace", namespace, "resource", gvr, "err", err)
	return true, err
}

// listCollection 列举指定命名空间下某 GVR 的所有条目。
// 返回值依次为：列举结果、操作是否受支持、操作是否出错（仅受支持但失败时非空）。
func (d *namespacedResourcesDeleter) listCollection(ctx context.Context, gvr schema.GroupVersionResource, namespace string) (*metav1.PartialObjectMetadataList, bool, error) {
	logger := klog.FromContext(ctx)
	logger.V(5).Info("Namespace controller - listCollection", "namespace", namespace, "resource", gvr)

	key := operationKey{operation: operationList, gvr: gvr}
	if !d.opCache.isSupported(key) {
		logger.V(5).Info("Namespace controller - listCollection ignored since not supported", "namespace", namespace, "resource", gvr)
		return nil, false, nil
	}

	partialList, err := d.metadataClient.Resource(gvr).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err == nil {
		return partialList, true, nil
	}

	// 与 deleteCollection 同理：不支持顶层动词的资源会返回 NotFound，
	// 需与 MethodNotSupported 一并视作不支持。详见 issue #22413。
	if errors.IsMethodNotSupported(err) || errors.IsNotFound(err) {
		logger.V(5).Info("Namespace controller - listCollection not supported", "namespace", namespace, "resource", gvr)
		return nil, false, nil
	}

	return nil, true, err
}

// deleteEachItem 在服务端不支持 deletecollection 时，逐个列举并删除资源。
func (d *namespacedResourcesDeleter) deleteEachItem(ctx context.Context, gvr schema.GroupVersionResource, namespace string) error {
	klog.FromContext(ctx).V(5).Info("Namespace controller - deleteEachItem", "namespace", namespace, "resource", gvr)

	partialList, listSupported, err := d.listCollection(ctx, gvr, namespace)
	if err != nil {
		return err
	}
	if !listSupported {
		return nil
	}
	// NotFound 与 MethodNotSupported 都视为该条目已无法再操作，跳过而不中断整批删除。
	for _, item := range partialList.Items {
		background := metav1.DeletePropagationBackground
		opts := metav1.DeleteOptions{PropagationPolicy: &background}
		if err = d.metadataClient.Resource(gvr).Namespace(namespace).Delete(ctx, item.GetName(), opts); err != nil && !errors.IsNotFound(err) && !errors.IsMethodNotSupported(err) {
			return err
		}
	}
	return nil
}

// gvrDeletionMetadata 汇总某个 GVR 的删除进度，用于计算整体等待时间。
type gvrDeletionMetadata struct {
	// finalizerEstimateSeconds 是预估还需等待的时间；0 表示尚未做出预估，
	// 并不代表内容已全部删除。
	finalizerEstimateSeconds int64
	// numRemaining 是该 GVR 仍剩余的实例数。
	numRemaining int
	// finalizersToNumRemaining 记录每个 finalizer 卡住了多少资源。
	finalizersToNumRemaining map[string]int
}

// deleteAllContentForGroupVersionResource 用动态客户端删除某 GVR 的所有资源，
// 返回剩余资源被删完前的预估等待时间。若 estimate > 0，则不保证资源已全部消失。
func (d *namespacedResourcesDeleter) deleteAllContentForGroupVersionResource(
	ctx context.Context,
	gvr schema.GroupVersionResource, namespace string,
	namespaceDeletedAt metav1.Time) (gvrDeletionMetadata, error) {
	logger := klog.FromContext(ctx)
	logger.V(5).Info("Namespace controller - deleteAllContentForGroupVersionResource", "namespace", namespace, "resource", gvr)

	// 先估算优雅终止时间（支持优雅删除的对象需要等待）。
	estimate, err := d.estimateGracefulTermination(ctx, gvr, namespace, namespaceDeletedAt)
	if err != nil {
		logger.V(5).Info("Namespace controller - deleteAllContentForGroupVersionResource - unable to estimate", "namespace", namespace, "resource", gvr, "err", err)
		return gvrDeletionMetadata{}, err
	}
	logger.V(5).Info("Namespace controller - deleteAllContentForGroupVersionResource - estimate", "namespace", namespace, "resource", gvr, "estimate", estimate)

	// 优先尝试一次性删除整个集合。
	deleteCollectionSupported, err := d.deleteCollection(ctx, gvr, namespace)
	if err != nil {
		return gvrDeletionMetadata{finalizerEstimateSeconds: estimate}, err
	}

	// 服务端不支持批量删除时，退化为逐个列举并删除。
	if !deleteCollectionSupported {
		err = d.deleteEachItem(ctx, gvr, namespace)
		if err != nil {
			return gvrDeletionMetadata{finalizerEstimateSeconds: estimate}, err
		}
	}

	// 删除后再列举一次确认；若已有非零预估，仍有资源残留属于正常情况。
	logger.V(5).Info("Namespace controller - deleteAllContentForGroupVersionResource - checking for no more items in namespace", "namespace", namespace, "resource", gvr)
	unstructuredList, listSupported, err := d.listCollection(ctx, gvr, namespace)
	if err != nil {
		logger.V(5).Info("Namespace controller - deleteAllContentForGroupVersionResource - error verifying no items in namespace", "namespace", namespace, "resource", gvr, "err", err)
		return gvrDeletionMetadata{finalizerEstimateSeconds: estimate}, err
	}
	if !listSupported {
		return gvrDeletionMetadata{finalizerEstimateSeconds: estimate}, nil
	}
	logger.V(5).Info("Namespace controller - deleteAllContentForGroupVersionResource - items remaining", "namespace", namespace, "resource", gvr, "items", len(unstructuredList.Items))
	// 列表为空说明该 GVR 已清理完毕。
	if len(unstructuredList.Items) == 0 {
		// 已全部清理完毕。
		return gvrDeletionMetadata{finalizerEstimateSeconds: 0, numRemaining: 0}, nil
	}

	// 从仍存在的条目上收集 finalizer，统计每个 finalizer 卡住的资源数。
	finalizersToNumRemaining := map[string]int{}
	for _, item := range unstructuredList.Items {
		for _, finalizer := range item.GetFinalizers() {
			finalizersToNumRemaining[finalizer] = finalizersToNumRemaining[finalizer] + 1
		}
	}

	// 已有优雅终止预估时直接采用，并把残留资源信息一并返回。
	if estimate != int64(0) {
		logger.V(5).Info("Namespace controller - deleteAllContentForGroupVersionResource - estimate is present", "namespace", namespace, "resource", gvr, "finalizers", finalizersToNumRemaining)
		return gvrDeletionMetadata{
			finalizerEstimateSeconds: estimate,
			numRemaining:             len(unstructuredList.Items),
			finalizersToNumRemaining: finalizersToNumRemaining,
		}, nil
	}

	// 没有优雅终止预估但有 finalizer 时，采用默认预估给 GC 留出完成时间，属于正常情况。
	if len(finalizersToNumRemaining) > 0 {
		logger.V(5).Info("Namespace controller - deleteAllContentForGroupVersionResource - items remaining with finalizers", "namespace", namespace, "resource", gvr, "finalizers", finalizersToNumRemaining)
		return gvrDeletionMetadata{
			finalizerEstimateSeconds: finalizerEstimateSeconds,
			numRemaining:             len(unstructuredList.Items),
			finalizersToNumRemaining: finalizersToNumRemaining,
		}, nil
	}

	// 既没有预估也没有 finalizer，资源却仍在，属于异常情况，返回错误。
	return gvrDeletionMetadata{
		finalizerEstimateSeconds: estimate,
		numRemaining:             len(unstructuredList.Items),
	}, fmt.Errorf("unexpected items still remain in namespace: %s for gvr: %v", namespace, gvr)
}

// allGVRDeletionMetadata 汇总命名空间下所有 GVR 的删除进度。
type allGVRDeletionMetadata struct {
	// gvrToNumRemaining 记录每个 GVR 剩余的实例数。
	gvrToNumRemaining map[schema.GroupVersionResource]int
	// finalizersToNumRemaining 记录每个 finalizer 卡住了多少资源。
	finalizersToNumRemaining map[string]int
}

// deleteAllContent 用动态客户端删除 groupVersionResources 中的每种资源，
// 返回剩余资源删完前的预估等待时间；estimate > 0 时不保证已全部删除。
func (d *namespacedResourcesDeleter) deleteAllContent(ctx context.Context, ns *v1.Namespace) (int64, error) {
	namespace := ns.Name
	namespaceDeletedAt := *ns.DeletionTimestamp
	var errs []error
	conditionUpdater := namespaceConditionUpdater{}
	estimate := int64(0)
	logger := klog.FromContext(ctx)
	logger.V(4).Info("namespace controller - deleteAllContent", "namespace", namespace)

	// discovery 出错不算致命：即使列表不完整，通常仍能操作已知的资源。
	resources, err := d.discoverResourcesFn()
	if err != nil {
		errs = append(errs, err)
		conditionUpdater.ProcessDiscoverResourcesErr(err)
	}
	// TODO(sttts): 去掉 opCache，改为把动词信息（尤其是 deletecollection）直接传给 deleter。
	deletableResources := discovery.FilteredBy(discovery.SupportsAllVerbs{Verbs: []string{"delete"}}, resources)
	groupVersionResources, err := discovery.GroupVersionResources(deletableResources)
	if err != nil {
		// 同上，discovery 分组出错仍尽量继续处理已知资源。
		errs = append(errs, err)
		conditionUpdater.ProcessGroupVersionErr(err)
	}

	numRemainingTotals := allGVRDeletionMetadata{
		gvrToNumRemaining:        map[schema.GroupVersionResource]int{},
		finalizersToNumRemaining: map[string]int{},
	}
	// Pod 依赖其他资源（如网络、存储），必须先删完 Pod 再处理其他资源。
	podsGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "pods"}

	if _, hasPods := groupVersionResources[podsGVR]; hasPods {
		// 先确保命名空间内的 Pod 都被删除。
		gvrDeletionMetadata, err := d.deleteAllContentForGroupVersionResource(ctx, podsGVR, namespace, namespaceDeletedAt)
		if err != nil {
			errs = append(errs, fmt.Errorf("failed to delete pods for namespace: %s, err: %w", namespace, err))
			conditionUpdater.ProcessDeleteContentErr(err)
		}
		if gvrDeletionMetadata.finalizerEstimateSeconds > estimate {
			estimate = gvrDeletionMetadata.finalizerEstimateSeconds
		}
		if gvrDeletionMetadata.numRemaining > 0 {
			numRemainingTotals.gvrToNumRemaining[podsGVR] = gvrDeletionMetadata.numRemaining
			for finalizer, numRemaining := range gvrDeletionMetadata.finalizersToNumRemaining {
				if numRemaining == 0 {
					continue
				}
				numRemainingTotals.finalizersToNumRemaining[finalizer] += numRemaining
			}
		}

		// 只要还有 Pod 残留，就推迟其他资源的删除，避免依赖失效。
		if numRemainingTotals.gvrToNumRemaining[podsGVR] > 0 {
			logger.V(5).Info("Namespace controller - pods still remain, delaying deletion of other resources", "namespace", namespace)
			if hasChanged := conditionUpdater.Update(ns); hasChanged {
				if _, err = d.nsClient.UpdateStatus(ctx, ns, metav1.UpdateOptions{}); err != nil {
					utilruntime.HandleErrorWithContext(ctx, err, "Couldn't update status condition for namespace", "namespace", namespace)
				}
			}
			return estimate, utilerrors.NewAggregate(errs)
		}
	}

	// Pod 清空后，再删除命名空间内的其他资源。
	for gvr := range groupVersionResources {
		// Pod 已在前面单独处理，这里跳过以免重复。
		if gvr.Group == podsGVR.Group &&
			gvr.Version == podsGVR.Version && gvr.Resource == podsGVR.Resource {
			continue
		}
		gvrDeletionMetadata, err := d.deleteAllContentForGroupVersionResource(ctx, gvr, namespace, namespaceDeletedAt)
		if err != nil {
			// 某个 GVR 删除失败时记录错误但继续处理其余 GVR，
			// 避免单个资源阻塞整个命名空间的清理。
			errs = append(errs, err)
			conditionUpdater.ProcessDeleteContentErr(err)
		}
		if gvrDeletionMetadata.finalizerEstimateSeconds > estimate {
			estimate = gvrDeletionMetadata.finalizerEstimateSeconds
		}
		if gvrDeletionMetadata.numRemaining > 0 {
			numRemainingTotals.gvrToNumRemaining[gvr] = gvrDeletionMetadata.numRemaining
			for finalizer, numRemaining := range gvrDeletionMetadata.finalizersToNumRemaining {
				if numRemaining == 0 {
					continue
				}
				numRemainingTotals.finalizersToNumRemaining[finalizer] = numRemainingTotals.finalizersToNumRemaining[finalizer] + numRemaining
			}
		}
	}
	conditionUpdater.ProcessContentTotals(numRemainingTotals)

	// 始终检查 Condition 是否需要更新：此前若设置过"成功"，现在可能需要改回"失败"。
	// 命名空间上可能还有其他 finalizer，本控制器移除自己的 finalizer 并不意味着资源实例会被删除。
	if hasChanged := conditionUpdater.Update(ns); hasChanged {
		if _, err = d.nsClient.UpdateStatus(ctx, ns, metav1.UpdateOptions{}); err != nil {
			utilruntime.HandleErrorWithContext(ctx, err, "Couldn't update status condition for namespace", "namespace", namespace)
		}
	}

	// errs 为空时 NewAggregate 返回 nil。
	err = utilerrors.NewAggregate(errs)
	logger.V(4).Info("namespace controller - deleteAllContent", "namespace", namespace, "estimate", estimate, "err", err)
	return estimate, err
}

// estimateGracefulTermination 估算命名空间内某类资源优雅终止所需的时间。
func (d *namespacedResourcesDeleter) estimateGracefulTermination(ctx context.Context, gvr schema.GroupVersionResource, ns string, namespaceDeletedAt metav1.Time) (int64, error) {
	groupResource := gvr.GroupResource()
	klog.FromContext(ctx).V(5).Info("Namespace controller - estimateGracefulTermination", "group", groupResource.Group, "resource", groupResource.Resource)
	estimate := int64(0)
	var err error
	switch groupResource {
	// 目前只对 Pod 计算优雅终止时间；其他资源无特殊估计。
	case schema.GroupResource{Group: "", Resource: "pods"}:
		estimate, err = d.estimateGracefulTerminationForPods(ctx, ns)
	}
	if err != nil {
		return 0, err
	}
	// 如果从删除开始经过的时间已超过估计值，则无需再等待。
	duration := time.Since(namespaceDeletedAt.Time)
	allowedEstimate := time.Duration(estimate) * time.Second
	if duration >= allowedEstimate {
		estimate = int64(0)
	}
	return estimate, nil
}

// estimateGracefulTerminationForPods 取命名空间内所有非终态 Pod 中最长的优雅终止时间。
func (d *namespacedResourcesDeleter) estimateGracefulTerminationForPods(ctx context.Context, ns string) (int64, error) {
	klog.FromContext(ctx).V(5).Info("Namespace controller - estimateGracefulTerminationForPods", "namespace", ns)
	estimate := int64(0)
	podsGetter := d.podsGetter
	// podsGetter 为 nil 时无法估算，返回错误而非静默使用默认值。
	if podsGetter == nil || reflect.ValueOf(podsGetter).IsNil() {
		return 0, fmt.Errorf("unexpected: podsGetter is nil. Cannot estimate grace period seconds for pods")
	}
	items, err := podsGetter.Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return 0, err
	}
	for i := range items.Items {
		pod := items.Items[i]
		// 已成功或已失败的 Pod 处于终态，不参与优雅终止时间估算。
		phase := pod.Status.Phase
		if v1.PodSucceeded == phase || v1.PodFailed == phase {
			continue
		}
		// 取所有运行中 Pod 中最大的优雅终止秒数，确保等待足够久。
		if pod.Spec.TerminationGracePeriodSeconds != nil {
			grace := *pod.Spec.TerminationGracePeriodSeconds
			if grace > estimate {
				estimate = grace
			}
		}
	}
	return estimate, nil
}
