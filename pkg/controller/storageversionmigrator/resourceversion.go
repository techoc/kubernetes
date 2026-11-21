/*
Copyright 2024 The Kubernetes Authors.

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

package storageversionmigrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"

	svmv1 "k8s.io/api/storagemigration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	svminformers "k8s.io/client-go/informers/storagemigration/v1"
	clientset "k8s.io/client-go/kubernetes"
	svmlisters "k8s.io/client-go/listers/storagemigration/v1"
)

const (
	// 该名称不是合法的命名空间名，因此集群中不可能存在同名命名空间，
	// 用它查询可以拿到一个"空"列表的 resourceVersion 作为时间基准。
	fakeSVMNamespaceName          string = "@fake:svm_ns!"
	ResourceVersionControllerName string = "resource-version-controller"
)

// verbsRequiredForMigration 是判断资源能否迁移所需的最小动词集合：
// 迁移过程需要更新、打补丁和列举资源。
var verbsRequiredForMigration = []string{"update", "patch", "list"}

// ResourceVersionController 在迁移开始前，从一个随机不存在的命名空间获取
// resourceVersion 并写入 SVM 的 status。该 resourceVersion 作为时间基准，
// 供迁移前校验 GC（垃圾回收）缓存是否足够新。
type ResourceVersionController struct {
	discoveryClient discovery.DiscoveryInterfaceWithContext
	metadataClient  metadata.Interface
	svmListers      svmlisters.StorageVersionMigrationLister
	svmSynced       cache.InformerSynced
	queue           workqueue.TypedRateLimitingInterface[string]
	kubeClient      clientset.Interface
	mapper          meta.ResettableRESTMapper

	// lastResetLock 保护 lastReset，避免多个 worker 并发重置 mapper 时重复重置。
	lastResetLock sync.Mutex
	lastReset     time.Time // 上次重置 RESTMapper 的时间，用于限频
}

// NewResourceVersionController 创建 ResourceVersionController：
// 初始化 SVM informer 的 lister / synced、限速工作队列，并注册 Add/Update
// 事件处理器，使 SVM 变更都能入队处理。
func NewResourceVersionController(
	ctx context.Context,
	kubeClient clientset.Interface,
	discoveryClient discovery.DiscoveryInterface,
	metadataClient metadata.Interface,
	svmInformer svminformers.StorageVersionMigrationInformer,
	mapper meta.ResettableRESTMapper,
) *ResourceVersionController {
	logger := klog.FromContext(ctx)

	rvController := &ResourceVersionController{
		kubeClient:      kubeClient,
		discoveryClient: discovery.ToDiscoveryInterfaceWithContext(discoveryClient),
		metadataClient:  metadataClient,
		svmListers:      svmInformer.Lister(),
		svmSynced:       svmInformer.Informer().HasSynced,
		mapper:          mapper,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: ResourceVersionControllerName},
		),
	}

	// 监听新增与更新事件：两种情况都可能需要为 SVM 补写 resourceVersion。
	_, err := svmInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			rvController.addSVM(logger, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			rvController.updateSVM(logger, oldObj, newObj)
		},
	}, cache.HandlerOptions{Logger: &logger})
	utilruntime.Must(err)

	return rvController
}

// addSVM 处理 informer 的新增事件，将新 SVM 入队。
func (rv *ResourceVersionController) addSVM(logger klog.Logger, obj interface{}) {
	svm := obj.(*svmv1.StorageVersionMigration)
	logger.V(4).Info("Adding", "svm", klog.KObj(svm))
	rv.enqueue(svm)
}

// updateSVM 处理 informer 的更新事件：以新对象入队，
// 因为状态判断和资源查询都要基于最新的 SVM。
func (rv *ResourceVersionController) updateSVM(logger klog.Logger, oldObj, newObj interface{}) {
	oldSVM := oldObj.(*svmv1.StorageVersionMigration)
	newSVM := newObj.(*svmv1.StorageVersionMigration)
	logger.V(4).Info("Updating", "svm", klog.KObj(oldSVM))
	rv.enqueue(newSVM)
}

// enqueue 通过 controller.KeyFunc 计算 SVM 的命名空间/名称键并加入工作队列；
// 键计算失败时记录错误并跳过该对象。
func (rv *ResourceVersionController) enqueue(svm *svmv1.StorageVersionMigration) {
	key, err := controller.KeyFunc(svm)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %w", svm, err))
		return
	}

	rv.queue.Add(key)
}

// Run 启动控制器：等待 SVM informer 缓存同步后，以单 worker 持续处理队列，
// 直到 ctx 取消；退出时关闭队列并等待 worker 结束，避免泄漏。
func (rv *ResourceVersionController) Run(ctx context.Context) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting", "controller", ResourceVersionControllerName)

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down", "controller", ResourceVersionControllerName)
		rv.queue.ShutDown()
		wg.Wait()
	}()

	// 缓存未同步前处理队列会拿到空数据，因此先阻塞等待。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, rv.svmSynced) {
		return
	}

	wg.Go(func() {
		// worker 异常退出后每秒重试一次，保证循环不会因瞬时错误停摆。
		wait.UntilWithContext(ctx, rv.worker, time.Second)
	})
	<-ctx.Done()
}

func (rv *ResourceVersionController) worker(ctx context.Context) {
	for rv.processNext(ctx) {
	}
}

func (rv *ResourceVersionController) processNext(ctx context.Context) bool {
	key, quit := rv.queue.Get()
	if quit {
		return false
	}
	defer rv.queue.Done(key)

	err := rv.sync(ctx, key)
	if err == nil {
		rv.queue.Forget(key)
		return true
	}

	klog.FromContext(ctx).V(2).Info("Error syncing SVM resource, retrying", "svm", key, "err", err)
	rv.queue.AddRateLimited(key)

	return true
}

func (rv *ResourceVersionController) sync(ctx context.Context, key string) error {
	logger := klog.FromContext(ctx)
	startTime := time.Now()

	// SVM is a cluster scoped resource so we don't care about the namespace
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	svm, err := rv.svmListers.Get(name)
	if apierrors.IsNotFound(err) {
		// no work to do, don't fail and requeue
		return nil
	}
	if err != nil {
		return err
	}
	// working with copy to avoid race condition between this and migration controller
	toBeProcessedSVM := svm.DeepCopy()

	if !meta.IsStatusConditionTrue(toBeProcessedSVM.Status.Conditions, string(svmv1.MigrationRunning)) {
		logger.V(4).Info("Migration is not running yet, skipping", "svm", name)
		return nil
	}
	gr := toBeProcessedSVM.Spec.Resource

	if meta.IsStatusConditionTrue(toBeProcessedSVM.Status.Conditions, string(svmv1.MigrationSucceeded)) ||
		meta.IsStatusConditionTrue(toBeProcessedSVM.Status.Conditions, string(svmv1.MigrationFailed)) {
		logger.V(4).Info("Migration has already succeeded or failed previously, skipping", "svm", name)
		return nil
	}

	if len(toBeProcessedSVM.Status.ResourceVersion) != 0 {
		logger.V(4).Info("Resource version is already set", "svm", name)
		return nil
	}

	gvr, exists, err := resourceFor(rv.mapper, gr)
	if err != nil {
		return err
	}
	if !exists {
		// our GC cache could be missing a recently created custom resource, so give it some time to catch up
		// we resync discovery every 30 seconds so twice that should be sufficient
		if toBeProcessedSVM.CreationTimestamp.Add(time.Minute).After(time.Now()) {
			rv.resetMapperIfStale()
			return fmt.Errorf("resource does not exist in our rest mapper, requeuing to attempt again")
		}
		return rv.failMigration(ctx, toBeProcessedSVM, "resource does not exist in discovery")
	}

	isMigratable, err := rv.isResourceMigratable(ctx, *gvr)
	if err != nil {
		return err
	}
	// The resource does not support CRUD operations for migration.
	if !isMigratable {
		err := fmt.Errorf("resource %q does not support discovery operations: %v", gvr.String(), verbsRequiredForMigration)
		logger.Error(err, "resource is not able to be migrated, not retrying", "gvr", gvr.String())
		return rv.failMigration(ctx, toBeProcessedSVM, err.Error())
	}

	latestRV, err := rv.getLatestResourceVersion(*gvr, ctx)
	if err != nil {
		return err
	}

	// Compare the resource version against itself, if it fails then it is not a
	// well formed RV and we should fail migration.
	if _, err := resourceversion.CompareResourceVersion(latestRV, latestRV); err != nil {
		err := fmt.Errorf("latest resourceVersion for %s is not valid: %w", gvr.String(), err)
		return rv.failMigration(ctx, toBeProcessedSVM, err.Error())
	}
	toBeProcessedSVM.Status.ResourceVersion = latestRV

	_, err = rv.kubeClient.StoragemigrationV1().
		StorageVersionMigrations().
		UpdateStatus(ctx, toBeProcessedSVM, metav1.UpdateOptions{})
	if err != nil {
		return fmt.Errorf("error updating status for %s: %w", toBeProcessedSVM.Name, err)
	}

	logger.V(4).Info("Resource version has been successfully added", "svm", key, "elapsed", time.Since(startTime))
	return nil
}

func (rv *ResourceVersionController) getLatestResourceVersion(gvr schema.GroupVersionResource, ctx context.Context) (string, error) {
	isResourceNamespaceScoped, err := rv.isResourceNamespaceScoped(ctx, gvr)
	if err != nil {
		return "", err
	}

	var randomList *metav1.PartialObjectMetadataList
	if isResourceNamespaceScoped {
		// get list resourceVersion from random non-existent namesapce for the given GVR
		randomList, err = rv.metadataClient.Resource(gvr).
			Namespace(fakeSVMNamespaceName).
			List(ctx, metav1.ListOptions{
				Limit: 1,
			})
	} else {
		randomList, err = rv.metadataClient.Resource(gvr).
			List(ctx, metav1.ListOptions{
				Limit: 1,
			})
	}
	if err != nil {
		// error here is very abstract. adding additional context for better debugging
		return "", fmt.Errorf("error getting latest resourceVersion for %s: %w", gvr.String(), err)
	}

	return randomList.GetResourceVersion(), err
}

func resourceFor(mapper meta.RESTMapper, gr metav1.GroupResource) (*schema.GroupVersionResource, bool, error) {
	partial := schema.GroupVersionResource{
		Group:    gr.Group,
		Resource: gr.Resource,
	}
	mapperGVRs, err := mapper.ResourcesFor(partial)
	if meta.IsNoMatchError(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}

	for _, mapperGVR := range mapperGVRs {
		if mapperGVR.Group == gr.Group && mapperGVR.Resource == gr.Resource {
			return &mapperGVR, true, nil
		}
	}

	return nil, false, nil
}

func (rv *ResourceVersionController) isResourceNamespaceScoped(ctx context.Context, gvr schema.GroupVersionResource) (bool, error) {
	resourceList, err := rv.discoveryClient.ServerResourcesForGroupVersionWithContext(ctx, gvr.GroupVersion().String())
	if err != nil {
		return false, err
	}

	for _, resource := range resourceList.APIResources {
		if resource.Name == gvr.Resource {
			return resource.Namespaced, nil
		}
	}

	return false, fmt.Errorf("resource %q not found", gvr.String())
}

// isResourceMigratable checks if the GVR has the list of verbs required for
// migration. Returns true if all verbs are in the discovery document and false
// otherwise. If there is an error querying the discovery client or we fail to
// get the GVR, return an error.
func (rv *ResourceVersionController) isResourceMigratable(ctx context.Context, gvr schema.GroupVersionResource) (bool, error) {
	resourceList, err := rv.discoveryClient.ServerResourcesForGroupVersionWithContext(ctx, gvr.GroupVersion().String())
	if apierrors.IsNotFound(err) {
		return false, nil
	}

	if resourceList != nil {
		// even in case of an error above there might be a partial list for APIs that
		// were already successfully discovered.
		for _, resource := range resourceList.APIResources {
			if resource.Name == gvr.Resource {
				if resource.Verbs != nil && sets.NewString(resource.Verbs...).HasAll(verbsRequiredForMigration...) {
					return true, nil
				}
				return false, nil
			}
		}
	}

	if err != nil {
		return false, err
	}
	return false, fmt.Errorf("resource %q not found in discovery", gvr.String())
}

func (rv *ResourceVersionController) failMigration(ctx context.Context, svm *svmv1.StorageVersionMigration, message string) error {
	_, err := rv.kubeClient.StoragemigrationV1().
		StorageVersionMigrations().
		UpdateStatus(
			ctx,
			setStatusConditions(svm, svmv1.MigrationFailed, migrationFailedStatusReason, message),
			metav1.UpdateOptions{},
		)
	if err != nil {
		return err
	}
	return nil
}

func (rv *ResourceVersionController) resetMapperIfStale() {
	rv.lastResetLock.Lock()
	defer rv.lastResetLock.Unlock()
	if time.Since(rv.lastReset) > 10*time.Second {
		rv.mapper.Reset()
		rv.lastReset = time.Now()
	}
}
