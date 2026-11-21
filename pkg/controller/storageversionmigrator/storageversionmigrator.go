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
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/garbagecollector"

	svmv1 "k8s.io/api/storagemigration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	svminformers "k8s.io/client-go/informers/storagemigration/v1"
	svmlisters "k8s.io/client-go/listers/storagemigration/v1"
)

const (
	workers                      = 5
	migrationSuccessStatusReason = "StorageVersionMigrationSucceeded"
	migrationFailedStatusReason  = "StorageVersionMigrationFailed"
)

// graphBuilder 提供按资源获取 GC 监控器的能力，便于在测试中替换该依赖。
type graphBuilder interface {
	GetMonitor(ctx context.Context, gvr schema.GroupVersionResource) (*garbagecollector.Monitor, bool, error)
}

// SVMController 负责驱动存储版本迁移，并回写迁移状态。
type SVMController struct {
	controllerName         string
	kubeClient             kubernetes.Interface
	dynamicClient          dynamic.Interface
	svmListers             svmlisters.StorageVersionMigrationLister
	svmSynced              cache.InformerSynced
	queue                  workqueue.TypedRateLimitingInterface[string]
	rateLimiter            workqueue.TypedRateLimiter[string]
	restMapper             meta.RESTMapper
	dependencyGraphBuilder graphBuilder
}

// NewSVMController 创建 SVM 控制器，并订阅 SVM 资源的增改事件。
func NewSVMController(
	ctx context.Context,
	kubeClient kubernetes.Interface,
	dynamicClient *dynamic.DynamicClient,
	svmInformer svminformers.StorageVersionMigrationInformer,
	controllerName string,
	mapper meta.ResettableRESTMapper,
	dependencyGraphBuilder *garbagecollector.GraphBuilder,
) *SVMController {
	logger := klog.FromContext(ctx)
	rateLimiter := workqueue.DefaultTypedControllerRateLimiter[string]()

	svmController := &SVMController{
		kubeClient:             kubeClient,
		dynamicClient:          dynamicClient,
		controllerName:         controllerName,
		svmListers:             svmInformer.Lister(),
		svmSynced:              svmInformer.Informer().HasSynced,
		restMapper:             mapper,
		dependencyGraphBuilder: dependencyGraphBuilder,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			rateLimiter,
			workqueue.TypedRateLimitingQueueConfig[string]{Name: controllerName},
		),
		rateLimiter: rateLimiter,
	}

	_, err := svmInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			svmController.addSVM(logger, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			svmController.updateSVM(logger, oldObj, newObj)
		},
	}, cache.HandlerOptions{Logger: &logger})
	utilruntime.Must(err)

	return svmController
}

// Name 返回控制器名称。
func (svmc *SVMController) Name() string {
	return svmc.controllerName
}

// addSVM 将新增的 SVM 入队。
func (svmc *SVMController) addSVM(logger klog.Logger, obj interface{}) {
	svm := obj.(*svmv1.StorageVersionMigration)
	logger.V(4).Info("Adding", "svm", klog.KObj(svm))
	svmc.enqueue(svm)
}

// updateSVM 将更新的 SVM 入队，迁移开始后仍需靠新对象驱动状态同步。
func (svmc *SVMController) updateSVM(logger klog.Logger, oldObj, newObj interface{}) {
	oldSVM := oldObj.(*svmv1.StorageVersionMigration)
	newSVM := newObj.(*svmv1.StorageVersionMigration)
	logger.V(4).Info("Updating", "svm", klog.KObj(oldSVM))
	svmc.enqueue(newSVM)
}

// enqueue 按对象的命名空间/名称生成键并加入工作队列。
func (svmc *SVMController) enqueue(svm *svmv1.StorageVersionMigration) {
	key, err := controller.KeyFunc(svm)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %w", svm, err))
		return
	}

	svmc.queue.Add(key)
}

// Run 启动控制器，等待缓存同步后处理队列直到上下文结束。
func (svmc *SVMController) Run(ctx context.Context) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting", "controller", svmc.controllerName)

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down", "controller", svmc.controllerName)
		svmc.queue.ShutDown()
		wg.Wait()
	}()

	if !cache.WaitForNamedCacheSyncWithContext(ctx, svmc.svmSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, svmc.worker, time.Second)
		})
	}
	<-ctx.Done()
}

// worker 持续消费队列，直到 processNext 返回 false。
func (svmc *SVMController) worker(ctx context.Context) {
	for svmc.processNext(ctx) {
	}
}

// processNext 同步一个队首元素，失败时按退避或服务端建议延迟重试。
func (svmc *SVMController) processNext(ctx context.Context) bool {
	key, quit := svmc.queue.Get()
	if quit {
		return false
	}
	defer svmc.queue.Done(key)

	err := svmc.sync(ctx, key)
	if err == nil {
		svmc.queue.Forget(key)
		return true
	}

	klog.FromContext(ctx).V(2).Info("Error syncing SVM resource, retrying", "svm", key, "err", err)
	if suggestDelay, ok := apierrors.SuggestsClientDelay(err); ok {
		svmc.queue.AddAfter(key, max(time.Second*time.Duration(suggestDelay), svmc.rateLimiter.When(key)))
		return true
	}
	svmc.queue.AddRateLimited(key)

	return true
}

// sync 校验迁移前提（GVR、GC 缓存、resourceVersion）后执行一次迁移。
func (svmc *SVMController) sync(ctx context.Context, key string) error {
	logger := klog.FromContext(ctx)
	startTime := time.Now()

	// SVM 是集群级资源，键中没有命名空间。
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	svm, err := svmc.svmListers.Get(name)
	if apierrors.IsNotFound(err) {
		// 对象已删除，无需迁移，不能报错重入队。
		return nil
	}
	if err != nil {
		return err
	}
	// 使用副本处理，避免与其他控制器共同修改缓存中的对象。
	toBeProcessedSVM := svm.DeepCopy()

	if meta.IsStatusConditionTrue(toBeProcessedSVM.Status.Conditions, string(svmv1.MigrationSucceeded)) ||
		meta.IsStatusConditionTrue(toBeProcessedSVM.Status.Conditions, string(svmv1.MigrationFailed)) {
		logger.V(4).Info("Migration has already succeeded or failed previously, skipping", "svm", name)
		return nil
	}

	if len(toBeProcessedSVM.Status.ResourceVersion) == 0 {
		logger.V(4).Info("The latest resource version is empty. We will attempt to migrate once the resource version is available.")
		return nil
	}
	gvr, exists, err := resourceFor(svmc.restMapper, toBeProcessedSVM.Spec.Resource)
	if err != nil {
		return err
	}
	if !exists {
		logger.V(4).Info("resource does not exist in our rest mapper", "resource", toBeProcessedSVM.Spec.Resource.String())
		if toBeProcessedSVM.CreationTimestamp.Add(time.Minute).After(time.Now()) {
			return fmt.Errorf("resource does not exist in rest mapper, requeuing to attempt again: %w", err)
		}
		return svmc.failMigration(ctx, toBeProcessedSVM, fmt.Errorf("unable to obtain GVR from discovery"))
	}

	// 监控器未同步时会阻塞，短超时便于尽早失败并让其他迁移继续推进。
	monCtx, monCtxCancel := context.WithTimeout(ctx, 10*time.Second)
	defer monCtxCancel()

	resourceMonitor, hasSynced, errMonitor := svmc.dependencyGraphBuilder.GetMonitor(monCtx, *gvr)
	if errMonitor != nil {
		return errMonitor
	}
	if !hasSynced {
		logger.V(4).Info("resource does not exist in GC", "gvr", gvr.String())

		// 新建的自定义资源可能尚未进入 mapper/discovery，给其一个同步窗口。
		if toBeProcessedSVM.CreationTimestamp.Add(time.Minute).After(time.Now()) {
			return fmt.Errorf("resource does not exist in GC, requeuing to attempt again: %w", errMonitor)
		}
		return svmc.failMigration(ctx, toBeProcessedSVM, fmt.Errorf("resource does not exist in GC: %w", errMonitor))
	}

	gcListResourceVersion := resourceMonitor.Controller.LastSyncResourceVersion()
	listResourceVersion := toBeProcessedSVM.Status.ResourceVersion

	rvCmp, err := resourceversion.CompareResourceVersion(gcListResourceVersion, listResourceVersion)
	if err != nil {
		return svmc.failMigration(ctx, toBeProcessedSVM, fmt.Errorf("error comparing resource versions between GC and SVM resource: %w", err))
	}
	if rvCmp == -1 {
		return fmt.Errorf("GC cache is not up to date, requeuing to attempt again. gcListResourceVersion: %s, listResourceVersion: %s", gcListResourceVersion, listResourceVersion)
	}

	err, failedMigration := svmc.runMigration(ctx, *gvr, resourceMonitor, toBeProcessedSVM, listResourceVersion)
	if err != nil {
		return err
	}
	if failedMigration {
		return nil
	}

	logger.V(4).Info("Finished syncing svm resource", "key", key, "gr", toBeProcessedSVM.Spec.Resource.String(), "elapsed", time.Since(startTime))
	return nil
}

// runMigration 逐个 patch 候选对象完成迁移，返回错误及是否属于不可重试的迁移失败。
func (svmc *SVMController) runMigration(ctx context.Context, gvr schema.GroupVersionResource, resourceMonitor *garbagecollector.Monitor, toBeProcessedSVM *svmv1.StorageVersionMigration, listResourceVersion string) (err error, failed bool) {
	gvk, err := svmc.restMapper.KindFor(gvr)
	if err != nil {
		return svmc.failMigration(ctx, toBeProcessedSVM, err), true
	}
	logger := klog.FromContext(ctx)

	allObjects := resourceMonitor.Store.List()
	totalObjects := len(allObjects)

	// 跳过版本高于检查点的对象，避免迁移之后才写入的新对象。
	var candidateObjects []metav1.Object
	for _, obj := range allObjects {
		accessor, err := meta.Accessor(obj)
		if err != nil {
			return svmc.failMigration(ctx, toBeProcessedSVM, err), true
		}
		rvCmp, err := resourceversion.CompareResourceVersion(accessor.GetResourceVersion(), listResourceVersion)
		if err != nil {
			logger.Error(err, "Unable to compare the resource version of the resource", "namespace", accessor.GetNamespace(), "name", accessor.GetName(), "gvr", gvr.String(), "accessorRV", accessor.GetResourceVersion(), "listResourceVersion", listResourceVersion, "error", err.Error())
			return svmc.failMigration(ctx, toBeProcessedSVM, err), true
		}
		if rvCmp == 1 {
			logger.V(6).Info("Resource ignored due to resource version being greater than the SVM checkpoint", "namespace", accessor.GetNamespace(), "name", accessor.GetName(), "gvr", gvr.String(), "accessorRV", accessor.GetResourceVersion(), "listResourceVersion", listResourceVersion)
			continue
		}
		candidateObjects = append(candidateObjects, accessor)
	}

	candidatesToPatch := len(candidateObjects)

	for _, accessor := range candidateObjects {
		typeMeta := typeMetaUIDRV{}
		typeMeta.APIVersion, typeMeta.Kind = gvk.ToAPIVersionAndKind()
		typeMeta.ResourceVersion = accessor.GetResourceVersion()
		data, err := json.Marshal(typeMeta)
		if err != nil {
			return svmc.failMigrationWithObjectsMigrated(ctx, toBeProcessedSVM, err, candidatesToPatch, totalObjects), true
		}

		_, errPatch := svmc.dynamicClient.Resource(gvr).
			Namespace(accessor.GetNamespace()).
			Patch(ctx,
				accessor.GetName(),
				types.MergePatchType,
				data,
				metav1.PatchOptions{
					FieldManager: svmc.controllerName,
				},
			)

		// 冲突/不存在说明对象已被更新、删除或重建，无需再迁移。
		if apierrors.IsConflict(errPatch) || apierrors.IsNotFound(errPatch) {
			logger.V(6).Info("Resource ignored due to conflict", "namespace", accessor.GetNamespace(), "name", accessor.GetName(), "gvr", gvr.String(), "err", errPatch)
			candidatesToPatch--
			continue
		}

		// 限流等临时错误通过返回错误触发整次重试。
		if isRetriableError(errPatch) {
			logger.V(6).Info("Resource patch failed due to an error that can be retried", "namespace", accessor.GetNamespace(), "name", accessor.GetName(), "gvr", gvr.String(), "err", errPatch)
			_ = svmc.updateRunningCondition(ctx, toBeProcessedSVM, candidatesToPatch, totalObjects, errPatch)
			return errPatch, false
		}

		if errPatch != nil {
			logger.Error(errPatch, "Failed to migrate the resource", "namespace", accessor.GetNamespace(), "name", accessor.GetName(), "gvr", gvr.String(), "reason", apierrors.ReasonForError(errPatch))
			errStatus := svmc.failMigrationWithObjectsMigrated(ctx, toBeProcessedSVM, errPatch, candidatesToPatch, totalObjects)
			return errStatus, true
		}

		candidatesToPatch--
		logger.V(4).Info("Successfully migrated the resource", "namespace", accessor.GetNamespace(), "name", accessor.GetName(), "gvr", gvr.String())
	}

	updatedRunningCondition := meta.SetStatusCondition(&toBeProcessedSVM.Status.Conditions, metav1.Condition{
		Type:               string(svmv1.MigrationRunning),
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             migrationRunningReason,
		Message:            migrationRunningMessage(&candidatesToPatch, &totalObjects, nil),
	})

	updatedSucceededCondition := meta.SetStatusCondition(&toBeProcessedSVM.Status.Conditions, metav1.Condition{
		Type:               string(svmv1.MigrationSucceeded),
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             migrationSuccessStatusReason,
		Message:            "Migration completed",
	})

	if updatedRunningCondition || updatedSucceededCondition {
		_, err = svmc.kubeClient.StoragemigrationV1().
			StorageVersionMigrations().
			UpdateStatus(
				ctx,
				toBeProcessedSVM,
				metav1.UpdateOptions{},
			)
		if err != nil {
			return err, false
		}
	}

	return nil, false
}

// updateRunningCondition 更新迁移进度；条件未变化时不写 API，避免无谓更新。
func (svmc *SVMController) updateRunningCondition(ctx context.Context, svm *svmv1.StorageVersionMigration, candidatesToPatch, totalObjects int, err error) error {
	cond := &metav1.Condition{
		Type:               string(svmv1.MigrationRunning),
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             migrationRunningReason,
		Message:            migrationRunningMessage(&candidatesToPatch, &totalObjects, err),
	}

	if !meta.SetStatusCondition(&svm.Status.Conditions, *cond) {
		return nil
	}

	_, updateErr := svmc.kubeClient.StoragemigrationV1().
		StorageVersionMigrations().
		UpdateStatus(
			ctx,
			svm,
			metav1.UpdateOptions{},
		)
	return updateErr
}

// isRetriableError 判断错误是否可通过重试恢复（连接中断、限流、服务端超时等）。
func isRetriableError(k8sError error) bool {
	return utilnet.IsConnectionReset(k8sError) ||
		utilnet.IsHTTP2ConnectionLost(k8sError) ||
		utilnet.IsProbableEOF(k8sError) ||
		apierrors.IsServerTimeout(k8sError) ||
		apierrors.IsTooManyRequests(k8sError) ||
		apierrors.IsServiceUnavailable(k8sError) ||
		apierrors.IsInternalError(k8sError) ||
		apierrors.IsTimeout(k8sError)
}

// failMigration 将迁移标记为失败并回写状态。
func (svmc *SVMController) failMigration(ctx context.Context, toBeProcessedSVM *svmv1.StorageVersionMigration, err error) error {
	errMsg := fmt.Sprintf("migration encountered unhandled error: %s", err)

	cond := &metav1.Condition{
		Type:               string(svmv1.MigrationFailed),
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             migrationFailedStatusReason,
		Message:            errMsg,
	}
	if !meta.SetStatusCondition(&toBeProcessedSVM.Status.Conditions, *cond) {
		return nil
	}

	_, errStatus := svmc.kubeClient.StoragemigrationV1().
		StorageVersionMigrations().
		UpdateStatus(
			ctx,
			toBeProcessedSVM,
			metav1.UpdateOptions{},
		)
	return errStatus
}

// failMigrationWithObjectsMigrated 标记失败时保留已迁移对象的进度。
func (svmc *SVMController) failMigrationWithObjectsMigrated(ctx context.Context, toBeProcessedSVM *svmv1.StorageVersionMigration, err error, candidatesToPatch, totalObjects int) error {
	errMsg := fmt.Sprintf("%d/%d not yet migrated. migration encountered unhandled error: %s", candidatesToPatch, totalObjects, err)

	updatedRunningCondition := meta.SetStatusCondition(&toBeProcessedSVM.Status.Conditions, metav1.Condition{
		Type:               string(svmv1.MigrationRunning),
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             migrationRunningReason,
		Message:            migrationRunningMessage(&candidatesToPatch, &totalObjects, nil),
	})

	failedCond := meta.SetStatusCondition(&toBeProcessedSVM.Status.Conditions, metav1.Condition{
		Type:               string(svmv1.MigrationFailed),
		Status:             metav1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
		Reason:             migrationFailedStatusReason,
		Message:            errMsg,
	})

	if !updatedRunningCondition && !failedCond {
		return nil
	}

	_, errStatus := svmc.kubeClient.StoragemigrationV1().
		StorageVersionMigrations().
		UpdateStatus(
			ctx,
			toBeProcessedSVM,
			metav1.UpdateOptions{},
		)
	return errStatus
}

// typeMetaUIDRV 是 patch 请求体的最小结构，只需携带类型与版本信息。
type typeMetaUIDRV struct {
	metav1.TypeMeta `json:""`
	objectRV        `json:"metadata,omitempty"`
}

// objectRV 用于序列化 resourceVersion，作为 patch 的并发控制条件。
type objectRV struct {
	ResourceVersion string `json:"resourceVersion,omitempty"`
}
