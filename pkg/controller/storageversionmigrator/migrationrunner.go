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

package storageversionmigrator

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"k8s.io/apiextensions-apiserver/pkg/apihelpers"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	applyconfigurationapiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/client/applyconfiguration/apiextensions/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/typed/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	svmv1 "k8s.io/api/storagemigration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	svminformers "k8s.io/client-go/informers/storagemigration/v1"
	clientset "k8s.io/client-go/kubernetes"
	svmlisters "k8s.io/client-go/listers/storagemigration/v1"
)

const MigrationRunnerControllerName string = "migration-runner-controller"

const migrationRunningReason = "MigrationRunning"

// MigrationRunnerController 管理 StorageVersionMigration 的生命周期：对同一 GroupResource
// 只让一个 SVM 处于活跃状态（打上 MigrationRunning 条件），下游迁移控制器仅处理这些 SVM。
// 同时它负责给 CRD 添加 StorageMigrating 条件，并从 CRD status 中清理旧的 stored versions。
type MigrationRunnerController struct {
	svmListers svmlisters.StorageVersionMigrationLister
	svmSynced  cache.InformerSynced
	queue      workqueue.TypedRateLimitingInterface[metav1.GroupResource]
	kubeClient clientset.Interface
	crdClient  apiextensionsclient.CustomResourceDefinitionInterface
}

// NewCustomResourceController 创建迁移调度控制器，并订阅 SVM 的增改删事件。
func NewCustomResourceController(
	ctx context.Context,
	kubeClient clientset.Interface,
	svmInformer svminformers.StorageVersionMigrationInformer,
	crdClient apiextensionsclient.CustomResourceDefinitionInterface,
) *MigrationRunnerController {
	logger := klog.FromContext(ctx)

	crController := &MigrationRunnerController{
		kubeClient: kubeClient,
		svmListers: svmInformer.Lister(),
		svmSynced:  svmInformer.Informer().HasSynced,
		crdClient:  crdClient,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[metav1.GroupResource](),
			workqueue.TypedRateLimitingQueueConfig[metav1.GroupResource]{Name: ResourceVersionControllerName},
		),
	}

	_, err := svmInformer.Informer().AddEventHandlerWithOptions(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			crController.addSVM(logger, obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			crController.updateSVM(logger, oldObj, newObj)
		},
		DeleteFunc: func(obj interface{}) {
			crController.deleteSVM(logger, obj)
		},
	}, cache.HandlerOptions{Logger: &logger})
	utilruntime.Must(err)

	return crController
}

// addSVM 将新增 SVM 所属的资源入队。
func (crc *MigrationRunnerController) addSVM(logger klog.Logger, obj interface{}) {
	svm := obj.(*svmv1.StorageVersionMigration)
	logger.V(4).Info("Adding", "svm", klog.KObj(svm))
	crc.enqueue(svm)
}

// updateSVM 将更新 SVM 所属的资源入队。
func (crc *MigrationRunnerController) updateSVM(logger klog.Logger, oldObj, newObj interface{}) {
	oldSVM := oldObj.(*svmv1.StorageVersionMigration)
	newSVM := newObj.(*svmv1.StorageVersionMigration)
	logger.V(4).Info("Updating", "svm", klog.KObj(oldSVM))
	crc.enqueue(newSVM)
}

// deleteSVM 处理删除事件（含 tombstone），重新评估该资源的待迁移 SVM。
func (crc *MigrationRunnerController) deleteSVM(logger klog.Logger, obj interface{}) {
	svm, ok := obj.(*svmv1.StorageVersionMigration)
	if !ok {
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			logger.Info("could not cast obj to DeletedFinalStateUnknown", "object", obj)
			return
		}
		svm, ok = tombstone.Obj.(*svmv1.StorageVersionMigration)
		if !ok {
			logger.Info("could not cast tombstone to SVM", "object", obj)
			return
		}
	}
	logger.V(4).Info("Deleting", "svm", klog.KObj(svm))
	crc.enqueue(svm)
}

// enqueue 按 SVM 的目标资源入队，聚合同一资源上的所有 SVM。
func (crc *MigrationRunnerController) enqueue(svm *svmv1.StorageVersionMigration) {
	crc.queue.Add(svm.Spec.Resource)
}

// Run 启动控制器，等待缓存同步后处理队列直到上下文结束。
func (crc *MigrationRunnerController) Run(ctx context.Context) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting", "controller", MigrationRunnerControllerName)

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down", "controller", MigrationRunnerControllerName)
		crc.queue.ShutDown()
		wg.Wait()
	}()

	if !cache.WaitForNamedCacheSyncWithContext(ctx, crc.svmSynced) {
		return
	}

	wg.Go(func() {
		wait.UntilWithContext(ctx, crc.worker, time.Second)
	})
	<-ctx.Done()
}

// worker 持续消费队列，直到 processNext 返回 false。
func (crc *MigrationRunnerController) worker(ctx context.Context) {
	for crc.processNext(ctx) {
	}
}

// processNext 同步一个队首元素，失败时限速重试。
func (crc *MigrationRunnerController) processNext(ctx context.Context) bool {
	key, quit := crc.queue.Get()
	if quit {
		return false
	}
	defer crc.queue.Done(key)

	err := crc.sync(ctx, key)
	if err == nil {
		crc.queue.Forget(key)
		return true
	}

	utilruntime.HandleErrorWithContext(ctx, err, "Error syncing SVM resource, retrying", "svm", key)
	crc.queue.AddRateLimited(key)

	return true
}

// sync 清理已完成/冲突的迁移，并提升下一个可执行的 SVM。
func (crc *MigrationRunnerController) sync(ctx context.Context, resource metav1.GroupResource) error {
	// 先收集待清理与待提升的 SVM，再统一处理。
	svmsToCleanup, svmToPromote, err := crc.getSVMsForResource(resource)
	if err != nil {
		return err
	}

	for _, svm := range svmsToCleanup {
		if err := crc.cleanupAdmission(ctx, svm); err != nil {
			return err
		}
	}

	if svmToPromote != nil {
		if err := crc.markAsActive(ctx, svmToPromote); err != nil {
			return err
		}
	}

	return nil
}

// cleanupAdmission 清除 SVM 的运行中条件，并同步清理 CRD 迁移状态。
func (crc *MigrationRunnerController) cleanupAdmission(ctx context.Context, svm *svmv1.StorageVersionMigration) error {
	needsUpdate := false
	migratingCond := meta.FindStatusCondition(svm.Status.Conditions, string(svmv1.MigrationRunning))
	if migratingCond != nil && migratingCond.Status == metav1.ConditionTrue {
		newCond := &metav1.Condition{
			Type:               string(svmv1.MigrationRunning),
			Status:             metav1.ConditionFalse,
			Reason:             "MigrationCompleted",
			Message:            "Migration completed",
			ObservedGeneration: svm.Generation,
		}

		meta.SetStatusCondition(&svm.Status.Conditions, *newCond)
		needsUpdate = true
	}

	if needsUpdate {
		if err := crc.cleanupCRD(ctx, svm); err != nil {
			return err
		}

		_, err := crc.kubeClient.StoragemigrationV1().
			StorageVersionMigrations().
			UpdateStatus(
				ctx,
				svm,
				metav1.UpdateOptions{},
			)
		if err != nil {
			return err
		}
	}

	return nil
}

// markAsActive 将 CRD 与 SVM 标记为迁移中，使下游控制器开始处理该 SVM。
func (crc *MigrationRunnerController) markAsActive(ctx context.Context, svm *svmv1.StorageVersionMigration) error {
	// 先在 CRD 上标记迁移中，再激活 SVM，避免下游在 CRD 未就绪时启动。
	crd, exists, err := crc.crdForGroupResource(ctx, svm.Spec.Resource)
	if err != nil {
		return err
	}
	if exists {
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			crd, err := crc.crdClient.Get(ctx, crd.Name, metav1.GetOptions{})
			if err != nil {
				return err
			}
			applyConfig := applyconfigurationapiextensionsv1.CustomResourceDefinition(crd.Name).
				// 携带 resourceVersion，确保 get 与 apply 之间 CRD 未被改动，否则 generation 可能已失效。
				WithResourceVersion(crd.ResourceVersion).
				WithStatus(applyconfigurationapiextensionsv1.CustomResourceDefinitionStatus().
					WithConditions(applyconfigurationapiextensionsv1.CustomResourceDefinitionCondition().
						WithType(apiextensionsv1.StorageMigrating).
						WithStatus(apiextensionsv1.ConditionTrue).
						WithLastTransitionTime(metav1.Now()).
						WithReason("MigrationRunning").
						WithMessage(fmt.Sprintf("Migration %s is running", svm.Name)).
						WithObservedGeneration(crd.Generation)))

			// TODO: 研究如何把非冲突、非未找到错误暴露到 SVM Status，
			// 让用户知道迁移因控制器无法处理的问题而卡住。
			_, err = crc.crdClient.ApplyStatus(ctx, applyConfig, metav1.ApplyOptions{FieldManager: MigrationRunnerControllerName, Force: true})
			return err
		})
		if err != nil {
			return err
		}
	}

	svm = setStatusConditions(svm, svmv1.MigrationRunning, migrationRunningReason, migrationRunningMessage(nil, nil, nil))
	_, err = crc.kubeClient.StoragemigrationV1().
		StorageVersionMigrations().
		UpdateStatus(
			ctx,
			svm,
			metav1.UpdateOptions{},
		)
	if err != nil {
		return err
	}

	return nil
}

// compareSVM 按创建时间、名称排序，保证选择待提升 SVM 的顺序稳定。
func compareSVM(a, b *svmv1.StorageVersionMigration) int {
	if i := a.CreationTimestamp.Compare(b.CreationTimestamp.Time); i != 0 {
		return i
	}
	return strings.Compare(a.Name, b.Name)
}

// getSVMsForResource 找出需清理的 SVM，并选出唯一可提升的候选。
func (crc *MigrationRunnerController) getSVMsForResource(resource metav1.GroupResource) (svmsToCleanup []*svmv1.StorageVersionMigration, svmToPromote *svmv1.StorageVersionMigration, err error) {
	svms, err := crc.svmListers.List(labels.Everything())
	if err != nil {
		return nil, nil, err
	}
	svmInProgress := false
	for _, svm := range svms {
		if svm.Spec.Resource != resource {
			continue
		}
		svmRunning := isRunning(svm)
		svmTerminal := isTerminal(svm)
		promotable := !svmRunning && !svmTerminal && !svmInProgress
		switch {
		case svmRunning && svmTerminal:
			svmsToCleanup = append(svmsToCleanup, svm.DeepCopy())

		case svmRunning:
			svmInProgress = true
			svmToPromote = nil

		case promotable && (svmToPromote == nil || compareSVM(svm, svmToPromote) == -1):
			svmToPromote = svm
		}
	}

	if svmToPromote != nil {
		svmToPromote = svmToPromote.DeepCopy()
	}
	return svmsToCleanup, svmToPromote, nil
}

// isTerminal 判断迁移是否已进入成功或失败终态。
func isTerminal(svm *svmv1.StorageVersionMigration) bool {
	return meta.IsStatusConditionTrue(svm.Status.Conditions, string(svmv1.MigrationSucceeded)) ||
		meta.IsStatusConditionTrue(svm.Status.Conditions, string(svmv1.MigrationFailed))
}

// isRunning 判断 SVM 是否处于迁移中。
func isRunning(svm *svmv1.StorageVersionMigration) bool {
	return meta.IsStatusConditionTrue(svm.Status.Conditions, string(svmv1.MigrationRunning))
}

// cleanupCRD 结束 CRD 上的迁移标记，迁移成功时同时清理旧 stored versions。
func (crc *MigrationRunnerController) cleanupCRD(ctx context.Context, toBeProcessedSVM *svmv1.StorageVersionMigration) error {
	migratingSuccessfulCond := meta.FindStatusCondition(toBeProcessedSVM.Status.Conditions, string(svmv1.MigrationSucceeded))
	success := migratingSuccessfulCond != nil && migratingSuccessfulCond.Status == metav1.ConditionTrue
	crd, exists, err := crc.crdForGroupResource(ctx, toBeProcessedSVM.Spec.Resource)
	if err != nil {
		return err
	}
	if !exists {
		return nil
	}
	if err := crc.finishCRDMigration(ctx, crd, success); err != nil {
		return err
	}

	return nil
}

// finishCRDMigration 清除 CRD 的 StorageMigrating 条件；仅当 generation 未变时才更新 stored versions。
func (crc *MigrationRunnerController) finishCRDMigration(ctx context.Context, crd *apiextensionsv1.CustomResourceDefinition, success bool) error {
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		crd, err := crc.crdClient.Get(ctx, crd.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		migratingCond := apihelpers.FindCRDCondition(crd, apiextensionsv1.StorageMigrating)
		var canUpdateStoredVersions bool
		if migratingCond != nil && migratingCond.ObservedGeneration == crd.Generation {
			canUpdateStoredVersions = true
		}

		var reason, msg string
		if success {
			reason = "MigrationSucceeded"
			msg = "The migration has succeeded"
			if canUpdateStoredVersions {
				msg += " and the stored versions have been updated"
			} else {
				msg += " but the stored versions have not been updated due to a generation mismatch"
			}
		} else {
			reason = "MigrationFailed"
			msg = "The migration has failed"
		}

		applyConfig := applyconfigurationapiextensionsv1.CustomResourceDefinition(crd.Name).
			// 携带 resourceVersion，确保 get 与 apply 之间 CRD 未被改动，否则 stored versions 可能已失效。
			WithResourceVersion(crd.ResourceVersion).
			WithStatus(applyconfigurationapiextensionsv1.CustomResourceDefinitionStatus().
				WithConditions(applyconfigurationapiextensionsv1.CustomResourceDefinitionCondition().
					WithType(apiextensionsv1.StorageMigrating).
					WithStatus(apiextensionsv1.ConditionFalse).
					WithLastTransitionTime(metav1.Now()).
					WithReason(reason).
					WithMessage(msg).
					WithObservedGeneration(crd.Generation)))

		if canUpdateStoredVersions {
			// 仅当迁移期间无其他变更时，才将 stored versions 收敛到当前存储版本。
			for _, version := range crd.Spec.Versions {
				if version.Storage {
					applyConfig.Status.WithStoredVersions(version.Name)
					break
				}
			}
		}

		// TODO: 研究如何把非冲突、非未找到错误暴露到 SVM Status，
		// 让用户知道迁移因控制器无法处理的问题而卡住。
		_, err = crc.crdClient.ApplyStatus(ctx, applyConfig, metav1.ApplyOptions{FieldManager: MigrationRunnerControllerName, Force: true})
		return err
	})
}

// crdForGroupResource 按 GroupResource 查找对应 CRD；不存在时返回 false 而非错误。
func (crc *MigrationRunnerController) crdForGroupResource(ctx context.Context, gr metav1.GroupResource) (*apiextensionsv1.CustomResourceDefinition, bool, error) {
	crdName := fmt.Sprintf("%s.%s", gr.Resource, gr.Group)
	crd, err := crc.crdClient.Get(ctx, crdName, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return crd, true, nil
}
