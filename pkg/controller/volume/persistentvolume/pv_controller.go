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

package persistentvolume

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/util/slice"
	"k8s.io/utils/ptr"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	ref "k8s.io/client-go/tools/reference"
	"k8s.io/client-go/util/workqueue"
	volerr "k8s.io/cloud-provider/volume/errors"
	storagehelpers "k8s.io/component-helpers/storage/volume"
	"k8s.io/kubernetes/pkg/controller/volume/common"
	"k8s.io/kubernetes/pkg/controller/volume/events"
	"k8s.io/kubernetes/pkg/controller/volume/persistentvolume/metrics"
	"k8s.io/kubernetes/pkg/util/goroutinemap"
	"k8s.io/kubernetes/pkg/util/goroutinemap/exponentialbackoff"
	vol "k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/recyclerclient"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"

	"k8s.io/klog/v2"
)

// ==================================================================
// 请不要试图简化这段代码。
// 让航天飞机继续飞行。
//
// ==================================================================
//
// 本控制器刻意采用非常冗长的写法，你会注意到：
//
// 1. 每个 if 语句都有配套的 else（简单检查客户端 API 调用的错误除外）。
//
// 2. 即使看上去显而易见的事情，也会显式地写成注释。
//
// 我们把这种风格称为“航天飞机风格”。它的目的是确保每一个分支和条件都被考虑到、处理到 ——
// 就像 NASA 为航天飞机这类应用编写代码时所做的那样。
//
// 最初，这个控制器的工作由三个控制器分担。本控制器是简化 PV 子系统的一次大规模努力的成果。
// 在这个过程中，我们意识到必须确保代码中处理并覆盖了每一个条件，即使某些分支是空操作。
//
// 因此，控制器代码可能显得过于冗长、注释繁多且“分支重重”。但这里记录了大量的业务知识和背景，
// 以确保未来的维护者能够正确地推理绑定行为的复杂性。因此，对本文件的修改应当保留并延续航天飞机风格。
// ==================================================================
// 请不要试图简化这段代码。
// 让航天飞机继续飞行。
// ==================================================================

// 设计：
//
// 这个设计最根本的关键，是 PersistentVolume（PV）和 PersistentVolumeClaim（PVC）之间双向的“指针”，
// 在这里表现为 pvc.Spec.VolumeName 和 pv.Spec.ClaimRef。在无事务的系统中，双向性管理起来很复杂，
// 但缺少它就无法在面对各种故障时保证合理的行为。例如，一个失控的 HA 控制器实例最终可能竞态地
// 做出多个无法区分的绑定，从而造成潜在的数据丢失。
//
// 本控制器设计为在主动-被动（active-passive）高可用模式下工作。它也*可以*在主动-主动（active-active）
// HA 模式下工作，所有对象状态转换都设计成能应对这种情况；但性能可能较低，因为两个处于活动状态的
// 控制器会频繁地互相干扰。
//
// 本控制器支持两个方向上的（由创建者完成的）预绑定对象：想要特定 PV 的 PVC，或者为特定 PVC 预留的 PV。
//
// 绑定是两步过程：先修改 PV.Spec.ClaimRef，再修改 PVC.Spec.VolumeName。在这个事务的任意时刻，
// PV 或 PVC 都可能被用户或其他控制器修改，甚至被完全删除。此外，两个（或更多）控制器可能同时尝试
// 把不同的卷绑定到不同的声明上。控制器必须能从这些情况引发的任何冲突中恢复。

// 为已制备的卷创建 PV 对象时的重试次数。
const createProvisionedPVRetryCount = 5

// 为已制备的卷创建 PV 对象时，两次重试之间的间隔。
const createProvisionedPVInterval = 10 * time.Second

// CSINameTranslator 可以根据 in-tree 插件名称获取对应的 CSI 驱动名称。
type CSINameTranslator interface {
	GetCSINameFromInTreeName(pluginName string) (string, error)
}

// CSIMigratedPluginManager 负责跟踪某个插件的 CSI 迁移状态。
type CSIMigratedPluginManager interface {
	IsMigrationEnabledForPlugin(pluginName string) bool
}

// PersistentVolumeController 是负责同步 PersistentVolumeClaim 与 PersistentVolume 的控制器。
// 它会启动两个 cache.Controller，分别监听 PersistentVolume 和 PersistentVolumeClaim 的变化。
type PersistentVolumeController struct {
	volumeLister       corelisters.PersistentVolumeLister
	volumeListerSynced cache.InformerSynced
	claimLister        corelisters.PersistentVolumeClaimLister
	claimListerSynced  cache.InformerSynced
	classLister        storagelisters.StorageClassLister
	classListerSynced  cache.InformerSynced
	podLister          corelisters.PodLister
	podListerSynced    cache.InformerSynced
	podIndexer         cache.Indexer
	NodeLister         corelisters.NodeLister
	NodeListerSynced   cache.InformerSynced

	kubeClient                clientset.Interface
	eventBroadcaster          record.EventBroadcaster
	eventRecorder             record.EventRecorder
	volumePluginMgr           vol.VolumePluginMgr
	enableDynamicProvisioning bool
	resyncPeriod              time.Duration

	// 缓存已知最新的卷和声明版本。只要不修改其中的卷/声明，这个缓存就是线程安全的；
	// 任何修改之前都必须先克隆。这些缓存既会由来自 etcd 的 "xxx added/updated/deleted" 事件更新，
	// 也会在控制器把较新版本保存到 etcd 时由控制器自己更新。
	// 为什么需要本地缓存：把一个卷绑定到声明会产生 4 个事件，大致顺序如下（取决于 goroutine 的调度顺序）：
	// - volume.Spec 更新
	// - volume.Status 更新
	// - claim.Spec 更新
	// - claim.Status 更新
	// 有了这些缓存，当例如 volume.Spec 更新事件先于其他事件到达时，控制器可以检查到自己已经保存过
	// volume.Status 以及 claim.Spec+Status，从而无需做任何操作。
	// 没有这个缓存，控制器在 informer 中看到的将是旧版本的 volume.Status 和 claim
	// （它们还没有被 API server 的事件更新），从而试图“修复”这些对象让它们绑定在一起。
	// 而任何写入 API server 的操作都会因版本冲突而失败 —— 因为这些对象已经被写入过了。
	volumes persistentVolumeOrderedIndex
	claims  cache.Store

	// 待处理的声明和卷的工作队列。每个队列应当只有一个工作线程，尤其是 syncClaim() 是不可重入的。
	// 两个并发的 syncClaim 可能把两个不同的声明绑定到同一个卷，或者把一个声明绑定到两个卷。
	// 控制器可以从这种情况中恢复（依靠 API server 的版本错误以及控制器中的其他检查），
	// 但多工作线程控制器的整体速度会比单线程更低。
	claimQueue  workqueue.TypedRateLimitingInterface[string]
	volumeQueue workqueue.TypedRateLimitingInterface[string]

	// 已调度/正在运行的操作的映射。
	runningOperations goroutinemap.GoRoutineMap

	// 仅供测试使用：在异步操作开始前调用的钩子。设为 nil 时不生效。
	preOperationHook func(operationName string)

	createProvisionedPVRetryCount int
	createProvisionedPVInterval   time.Duration

	// operationTimestamps 缓存操作（目前是制备+绑定/删除）的开始时间戳，用于指标记录。
	// 各操作的详细生命周期/键如下：
	// 1. 制备 + 绑定
	//    键：        claimKey
	//    开始时间：用户在声明中未提供任何卷引用，且没有为该声明找到已有卷时，
	//              以有效的插件/外部制备器调用 "provisionClaim" 来制备卷
	//    结束时间：卷被成功制备并绑定到声明后，相应的时间戳条目会从缓存中删除
	//    中止：    声明尚未绑定到卷，但已从 API server 收到声明被删除的事件
	// 2. 删除
	//    键：        volumeName
	//    开始时间："reclaimVolume" 处理回收策略为 "PersistentVolumeReclaimDelete" 的卷时
	//    结束时间：从 API server 收到卷删除事件后，相应的时间戳条目会从缓存中删除
	//    中止：    不适用
	operationTimestamps metrics.OperationStartTimeCache

	translator               CSINameTranslator
	csiMigratedPluginManager CSIMigratedPluginManager
}

// syncClaim 是控制器决定如何处理某个 PVC 的主入口方法。
// 当声明被创建、更新或周期性同步时，由相应的 cache.Controller 回调调用；我们不区分这些事件。
// 为了便于阅读，它被拆分为 syncUnboundClaim 和 syncBoundClaim 两个方法。
func (ctrl *PersistentVolumeController) syncClaim(ctx context.Context, claim *v1.PersistentVolumeClaim) error {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Synchronizing PersistentVolumeClaim", "PVC", klog.KObj(claim), "claimStatus", getClaimStatusForLogging(claim))

	// 必要时修正 PVC 上正确的 "migrated-to" 注解，并更新到 API server。
	newClaim, err := ctrl.updateClaimMigrationAnnotations(ctx, claim)
	// 先修正 PVC 上“迁移到”的注解，避免后续绑定判断基于过期的迁移信息。
	if err != nil {
		// 什么都没保存成功；下次调用本方法时仍会回到同样的状态并重试。
		return err
	}
	claim = newClaim

	if !metav1.HasAnnotation(claim.ObjectMeta, storagehelpers.AnnBindCompleted) {
		// 用 AnnBindCompleted 注解区分未绑定/已绑定的声明，二者走不同的处理路径。
		return ctrl.syncUnboundClaim(ctx, claim)
	} else {
		// 卷被另一个声明占用，当前声明属于错误绑定，置为终态 Lost。
		// 控制器绑定的卷却指向其他声明，理论上不会发生（否则 AnnBindCompleted 会被移除），视为错误。
		return ctrl.syncBoundClaim(ctx, claim)
	}
}

// checkVolumeSatisfyClaim 检查声明所请求的卷是否满足该声明的要求。
// 依次检查：卷是否被标记删除、容量是否足够、StorageClass 是否匹配、
// VolumeAttributesClass 是否匹配（受 FeatureGate 控制）、volumeMode 和访问模式是否兼容。
func checkVolumeSatisfyClaim(volume *v1.PersistentVolume, claim *v1.PersistentVolumeClaim) error {
	requestedQty := claim.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	requestedSize := requestedQty.Value()

	// 检查 PV 是否设置了 DeletionTimeStamp，若已设置则返回错误。
	if volume.ObjectMeta.DeletionTimestamp != nil {
		// 正在删除的卷不能用于绑定，否则可能把一个即将消失的卷分配给声明。
		return fmt.Errorf("the volume is marked for deletion %q", volume.Name)
	}

	volumeQty := volume.Spec.Capacity[v1.ResourceStorage]
	volumeSize := volumeQty.Value()
	if volumeSize < requestedSize {
		return fmt.Errorf("requested PV is too small")
	}

	requestedClass := storagehelpers.GetPersistentVolumeClaimClass(claim)
	if storagehelpers.GetPersistentVolumeClass(volume) != requestedClass {
		return fmt.Errorf("storageClassName does not match")
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.VolumeAttributesClass) {
		// 特性开启时要求 VAC 严格一致；关闭时两边都不允许出现 VAC。
		requestedVAC := ptr.Deref(claim.Spec.VolumeAttributesClassName, "")
		volumeVAC := ptr.Deref(volume.Spec.VolumeAttributesClassName, "")
		if requestedVAC != volumeVAC {
			return fmt.Errorf("volumeAttributesClassName does not match")
		}
	} else {
		requestedVAC := ptr.Deref(claim.Spec.VolumeAttributesClassName, "")
		volumeVAC := ptr.Deref(volume.Spec.VolumeAttributesClassName, "")
		if requestedVAC != "" || volumeVAC != "" {
			return fmt.Errorf("volumeAttributesClassName is not supported when the feature-gate VolumeAttributesClass is disabled")
		}
	}

	if storagehelpers.CheckVolumeModeMismatches(&claim.Spec, &volume.Spec) {
		// volumeMode 不匹配时 findBestMatchForClaim 不会选中该卷，syncUnboundClaim 也不会绑定，这里直接跳过。
		return fmt.Errorf("incompatible volumeMode")
	}

	if !storagehelpers.CheckAccessModes(claim, volume) {
		return fmt.Errorf("incompatible accessMode")
	}

	return nil
}

// emitEventForUnboundDelayBindingClaim 为处于延迟绑定模式且尚未绑定的声明生成提示性事件。
// 若存在引用该 PVC 且尚未调度的 Pod，则事件会改为提示等待这些 Pod 被调度。
func (ctrl *PersistentVolumeController) emitEventForUnboundDelayBindingClaim(claim *v1.PersistentVolumeClaim) error {
	reason := events.WaitForFirstConsumer
	message := "waiting for first consumer to be created before binding"
	podNames, err := ctrl.findNonScheduledPodsByPVC(claim)
	if err != nil {
		return err
	}
	if len(podNames) > 0 {
		reason = events.WaitForPodScheduled
		if len(podNames) > 1 {
			// 卷调度时只考虑一个 Pod，但可能有多个 Pod 同时引用该 PVC；无法确定是哪一个，因此全部列出。
			// 卷调度时只考虑一个 Pod，
			// 但可能有多个 Pod 同时引用该 PVC；
			// 我们无法知道调度时用的是哪一个，
			// 因此把所有 Pod 都列出来。
			message = fmt.Sprintf("waiting for pods %s to be scheduled", strings.Join(podNames, ","))
		} else {
			message = fmt.Sprintf("waiting for pod %s to be scheduled", podNames[0])
		}
	}
	ctrl.eventRecorder.Event(claim, v1.EventTypeNormal, reason, message)
	return nil
}

// syncUnboundClaim 是控制器决定如何处理未绑定声明的主方法。
// 若声明没有指定卷名：先尝试找匹配的 PV，找不到则尝试分配默认 StorageClass、动态制备或保持 Pending；
// 若声明指定了卷名：校验该 PV 并绑定，或处理已被其他声明占用等冲突情况。
func (ctrl *PersistentVolumeController) syncUnboundClaim(ctx context.Context, claim *v1.PersistentVolumeClaim) error {
	// 这是一个尚未完成绑定新 PVC。
	// OBSERVATION：pvc 处于 "Pending"。
	logger := klog.FromContext(ctx)
	if claim.Spec.VolumeName == "" {
		// 声明曾绑定过但不再引用任何卷，数据可能已丢失，标记 Lost 并告警。
		// 用户没有指定想要哪个 PV。
		delayBinding, err := storagehelpers.IsDelayBindingMode(claim, ctrl.classLister)
		// 延迟绑定模式下不应立即为声明选择 PV，需等待 Pod 调度后再绑定。
		if err != nil {
			return err
		}

		// [单元测试组 1]
		volume, err := ctrl.volumes.findBestMatchForClaim(claim, delayBinding)
		if err != nil {
			logger.V(2).Info("Synchronizing unbound PersistentVolumeClaim, Error finding PV for claim", "PVC", klog.KObj(claim), "err", err)
			return fmt.Errorf("error finding PV for claim %q: %w", claimToClaimKey(claim), err)
		}
		if volume == nil {
			// 没有传入卷时清空 AccessModes/Capacity/CurrentVolumeAttributesClassName，避免遗留旧值。
			logger.V(4).Info("Synchronizing unbound PersistentVolumeClaim, no volume found", "PVC", klog.KObj(claim))
			// 找不到合适的 PV，尽可能尝试动态制备一个。
			// OBSERVATION：pvc 处于 "Pending"，稍后会重试。

			logger.V(4).Info("Attempting to assign storage class to unbound PersistentVolumeClaim", "PVC", klog.KObj(claim))
			updated, err := ctrl.assignDefaultStorageClass(ctx, claim)
			if err != nil {
				metrics.RecordRetroactiveStorageClassMetric(false)
				return fmt.Errorf("can't update PersistentVolumeClaim[%q]: %w", claimToClaimKey(claim), err)
			}
			if updated {
				// 刚给声明补上默认 StorageClass，本次同步到此为止，让下一次同步基于新声明继续处理。
				logger.V(4).Info("PersistentVolumeClaim update successful, restarting claim sync", "PVC", klog.KObj(claim))
				metrics.RecordRetroactiveStorageClassMetric(true)
				return nil
			}

			switch {
			case delayBinding && !storagehelpers.IsDelayBindingProvisioning(claim):
				// 调度器尚未观察到使用该声明的 Pod，不触发制备，只发出等待事件。
				// 调度器尚未观察到任何使用该声明的 Pod。
				if err = ctrl.emitEventForUnboundDelayBindingClaim(claim); err != nil {
					return err
				}
			case storagehelpers.GetPersistentVolumeClaimClass(claim) != "":
				// 制备是异步操作，声明会在异步流程中更新，因此这里直接返回，等待下一轮同步继续绑定。
				// provisionClaim 可能启动新的异步制备操作，
				// 或该操作已在运行；声明会在异步流程中更新，
				// 因此这里直接返回，预期绑定操作在下一轮同步中继续。
				// （接上行：下一轮同步循环。）
				if err = ctrl.provisionClaim(ctx, claim); err != nil {
					return err
				}
				return nil
			default:
				ctrl.eventRecorder.Event(claim, v1.EventTypeNormal, events.FailedBinding, "no persistent volumes available for this claim and no storage class is set")
			}

			// 把声明标记为 Pending，在下一次周期性 syncClaim 中再尝试寻找匹配。
			if _, err = ctrl.updateClaimStatus(ctx, claim, v1.ClaimPending, nil); err != nil {
				return err
			}
			return nil
		} else /* pv != nil */ {
			// 为该声明找到了一个 PV。
			// OBSERVATION：pvc 处于 "Pending"，pv 处于 "Available"。
			claimKey := claimToClaimKey(claim)
			logger.V(4).Info("Synchronizing unbound PersistentVolumeClaim, volume found", "PVC", klog.KObj(claim), "volumeName", volume.Name, "volumeStatus", getVolumeStatusForLogging(volume))
			if err = ctrl.bind(ctx, volume, claim); err != nil {
				// 保存卷或声明时若出现任何错误，后续的
				// syncClaim 会继续完成绑定。
				// 如果存在制备操作，则记录错误计数。
				// 时间戳条目会留在缓存中，直到绑定成功才清理。
				// 绑定保存失败时记录错误指标；时间戳条目保留，直到绑定成功才清理。
				metrics.RecordMetric(claimKey, &ctrl.operationTimestamps, err)
				return err
			}
			// OBSERVATION：claim 为 "Bound"，pv 为 "Bound"。
			// 若缓存中存在时间戳条目，记录端到端制备延迟并清理缓存。
			// 制备+绑定操作生命周期结束，缓存由 "RecordMetric" 清理。
			// [单元测试 12-1、12-2、12-4]
			// 绑定全部完成，记录端到端制备延迟并清理缓存中的时间戳条目。
			metrics.RecordMetric(claimKey, &ctrl.operationTimestamps, nil)
			return nil
		}
	} else /* pvc.Spec.VolumeName != nil */ {
		// [单元测试组 2]
		// 用户请求了一个指定的 PV。
		logger.V(4).Info("Synchronizing unbound PersistentVolumeClaim, volume requested", "PVC", klog.KObj(claim), "volumeName", claim.Spec.VolumeName)
		obj, found, err := ctrl.volumes.store.GetByKey(claim.Spec.VolumeName)
		if err != nil {
			return err
		}
		if !found {
			// 本地缓存未找到声明时，需要依次查 informer 缓存和 API server，避免误回收 PV。
			// 声明引用的卷不存在，同样标记为 Lost。
			// 用户请求了一个不存在的 PV。
			// OBSERVATION：pvc 处于 "Pending"。
			// 稍后重试。
			logger.V(4).Info("Synchronizing unbound PersistentVolumeClaim， volume requested and not found, will try again next time", "PVC", klog.KObj(claim), "volumeName", claim.Spec.VolumeName)
			if _, err = ctrl.updateClaimStatus(ctx, claim, v1.ClaimPending, nil); err != nil {
				return err
			}
			return nil
		} else {
			volume, ok := obj.(*v1.PersistentVolume)
			if !ok {
				return fmt.Errorf("cannot convert object from volume cache to volume %q!?: %+v", claim.Spec.VolumeName, obj)
			}
			logger.V(4).Info("Synchronizing unbound PersistentVolumeClaim, volume requested and found", "PVC", klog.KObj(claim), "volumeName", claim.Spec.VolumeName, "volumeStatus", getVolumeStatusForLogging(volume))
			if volume.Spec.ClaimRef == nil {
				// 没有 ClaimRef 的卷从未被绑定，谈不上释放。
				// 无法区分卷被解绑与缓存未更新两种情况，统一重新绑定并把状态恢复为 Bound。
				// 卷尚未被任何声明占用，先校验其是否满足声明要求再绑定。
				// 用户请求了一个尚未被任何声明占用的 PV。
				// OBSERVATION：pvc 处于 "Pending"，pv 处于 "Available"。
				logger.V(4).Info("Synchronizing unbound PersistentVolumeClaim, volume is unbound, binding", "PVC", klog.KObj(claim))
				if err = checkVolumeSatisfyClaim(volume, claim); err != nil {
					logger.V(4).Info("Can't bind the claim to volume", "volumeName", volume.Name, "err", err)
					// 发出一个事件。
					msg := fmt.Sprintf("Cannot bind to requested volume %q: %s", volume.Name, err)
					ctrl.eventRecorder.Event(claim, v1.EventTypeWarning, events.VolumeMismatch, msg)
					// 该卷不满足声明的要求。
					if _, err = ctrl.updateClaimStatus(ctx, claim, v1.ClaimPending, nil); err != nil {
						return err
					}
				} else if err = ctrl.bind(ctx, volume, claim); err != nil {
					// 保存卷或声明时若出现任何错误，后续的 syncClaim 会继续完成绑定。
					return err
				}
				// OBSERVATION：pvc 为 "Bound"，pv 为 "Bound"。
				return nil
			} else if storagehelpers.IsVolumeBoundToClaim(volume, claim) {
				// 卷已被本声明预留，调用 bind 补上 UID 等字段即可完成绑定。
				// 用户请求了一个已被本 PVC 占用的 PV。
				// OBSERVATION：pvc 处于 "Pending"，pv 处于 "Bound"。
				logger.V(4).Info("Synchronizing unbound PersistentVolumeClaim, volume already bound, finishing the binding", "PVC", klog.KObj(claim))

				// 通过补上声明的 UID 来完成卷绑定。
				if err = ctrl.bind(ctx, volume, claim); err != nil {
					return err
				}
				// OBSERVATION：pvc 为 "Bound"，pv 为 "Bound"。
				return nil
			} else {
				// 用户请求了一个已被其他声明占用的 PV。
				// OBSERVATION：pvc 处于 "Pending"，pv 处于 "Bound"。
				if !metav1.HasAnnotation(claim.ObjectMeta, storagehelpers.AnnBoundByController) {
					// 声明不是控制器绑定的，说明是用户指定的预绑定冲突，只能加事件并稍后重试。
					logger.V(4).Info("Synchronizing unbound PersistentVolumeClaim, volume already bound to different claim by user, will retry later", "PVC", klog.KObj(claim))
					claimMsg := fmt.Sprintf("volume %q already bound to a different claim.", volume.Name)
					ctrl.eventRecorder.Event(claim, v1.EventTypeWarning, events.FailedBinding, claimMsg)
					// 用户指定了特定的 PV，稍后重试。
					if _, err = ctrl.updateClaimStatus(ctx, claim, v1.ClaimPending, nil); err != nil {
						return err
					}
					return nil
				} else {
					// 这理论上不会发生，因为必须有人移除了声明上的 AnnBindCompleted 注解。
					logger.V(4).Info("Synchronizing unbound PersistentVolumeClaim, volume already bound to different claim by controller, THIS SHOULD NEVER HAPPEN", "PVC", klog.KObj(claim), "boundClaim", klog.KRef(volume.Spec.ClaimRef.Namespace, volume.Spec.ClaimRef.Name))
					claimMsg := fmt.Sprintf("volume %q already bound to a different claim.", volume.Name)
					ctrl.eventRecorder.Event(claim, v1.EventTypeWarning, events.FailedBinding, claimMsg)

					return fmt.Errorf("invalid binding of claim %q to volume %q: volume already claimed by %q", claimToClaimKey(claim), claim.Spec.VolumeName, claimrefToClaimKey(volume.Spec.ClaimRef))
				}
			}
		}
	}
}

// syncBoundClaim 是控制器决定如何处理已绑定声明的主方法。
// 若声明丢失卷引用或卷不存在，将声明置为 Lost；若卷被解绑或声明 UID 不匹配，重新绑定或判定为绑定错误。
func (ctrl *PersistentVolumeController) syncBoundClaim(ctx context.Context, claim *v1.PersistentVolumeClaim) error {
	// HasAnnotation(pvc, storagehelpers.AnnBindCompleted)：该 PVC 此前已经绑定过。
	// OBSERVATION：pvc 不处于 "Pending"。
	// [单元测试组 3]

	logger := klog.FromContext(ctx)

	if claim.Spec.VolumeName == "" {
		// 声明曾绑定过，但现在已经不再绑定。
		if _, err := ctrl.updateClaimStatusWithEvent(ctx, claim, v1.ClaimLost, nil, v1.EventTypeWarning, "ClaimLost", "Bound claim has lost reference to PersistentVolume. Data on the volume is lost!"); err != nil {
			return err
		}
		return nil
	}
	obj, found, err := ctrl.volumes.store.GetByKey(claim.Spec.VolumeName)
	if err != nil {
		return err
	}
	if !found {
		// 声明绑定到了一个不存在的卷。
		if _, err = ctrl.updateClaimStatusWithEvent(ctx, claim, v1.ClaimLost, nil, v1.EventTypeWarning, "ClaimLost", "Bound claim has lost its PersistentVolume. Data on the volume is lost!"); err != nil {
			return err
		}
		return nil
	} else {
		volume, ok := obj.(*v1.PersistentVolume)
		if !ok {
			return fmt.Errorf("cannot convert object from volume cache to volume %q!?: %#v", claim.Spec.VolumeName, obj)
		}

		logger.V(4).Info("Synchronizing bound PersistentVolumeClaim, volume found", "PVC", klog.KObj(claim), "volumeName", claim.Spec.VolumeName, "volumeStatus", getVolumeStatusForLogging(volume))
		if volume.Spec.ClaimRef == nil {
			// 声明已绑定，但卷变成了未绑定状态；
			// 也可能是声明已绑定而控制器尚未收到更新后的卷。
			// 我们无法区分这两种情况，
			// 统一重新绑定卷并把所有状态设为 Bound。
			logger.V(4).Info("Synchronizing bound PersistentVolumeClaim, volume is unbound, fixing", "PVC", klog.KObj(claim))
			if err = ctrl.bind(ctx, volume, claim); err != nil {
				// 对象未保存成功，下一次 syncPV 或 syncClaim 会重试。
				return err
			}
			return nil
		} else if volume.Spec.ClaimRef.UID == claim.UID {
			// 一切正常；bind 在大多数情况下不会改动对象，但保留调用以兜底修复。
			// 一切正常。
			// 注意：syncPV 可以处理这种情况，因此这里可省略。
			// 注意：这里调用 bind() 在大多数情况下不会改动对象，
			// 因为所有字段应该都已设置好。
			logger.V(4).Info("Synchronizing bound PersistentVolumeClaim, claim is already correctly bound", "PVC", klog.KObj(claim))
			if err = ctrl.bind(ctx, volume, claim); err != nil {
				// 对象未保存成功，下一次 syncPV 或 syncClaim 会重试。
				return err
			}
			return nil
		} else {
			// 声明已绑定，但卷被另一个声明占用。 把该声明的阶段设为终态 "Lost"。
			if _, err = ctrl.updateClaimStatusWithEvent(ctx, claim, v1.ClaimLost, nil, v1.EventTypeWarning, "ClaimMisbound", "Two claims are bound to the same volume, this one is bound incorrectly"); err != nil {
				return err
			}
			return nil
		}
	}
}

// syncVolume 是控制器决定如何处理某个卷的主入口方法。
// 当卷被创建、更新或周期性同步时，由相应的 cache.Controller 回调调用；我们不区分这些事件。
// 先修正卷上的迁移注解与 finalizer，再根据 ClaimRef 是否存在、声明是否匹配，
// 决定更新阶段、重新绑定、触发回收或解除绑定。
func (ctrl *PersistentVolumeController) syncVolume(ctx context.Context, volume *v1.PersistentVolume) error {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Synchronizing PersistentVolume", "volumeName", volume.Name, "volumeStatus", getVolumeStatusForLogging(volume))
	// 必要时修正 PV 上正确的 "migrated-to" 注解、修改 finalizer，并更新到 API server。
	newVolume, err := ctrl.updateVolumeMigrationAnnotationsAndFinalizers(ctx, volume)
	// 先同步迁移注解和 finalizer，避免基于陈旧状态做错误的回收/删除判断。
	if err != nil {
		// 什么都没保存成功；下次调用本方法时仍会回到同样的状态并重试。
		return err
	}
	volume = newVolume

	// [单元测试组 4]
	if volume.Spec.ClaimRef == nil {
		// 卷未被使用。
		logger.V(4).Info("Synchronizing PersistentVolume, volume is unused", "volumeName", volume.Name)
		if _, err := ctrl.updateVolumePhase(ctx, volume, v1.VolumeAvailable, ""); err != nil {
			// 什么都没保存成功；下次调用本方法时仍会回到同样的状态并重试。
			return err
		}
		return nil
	} else /* pv.Spec.ClaimRef != nil */ {
		// 卷已绑定到某个声明。
		if volume.Spec.ClaimRef.UID == "" {
			// ClaimRef 尚无 UID 表示 PV 为特定 PVC 预留，绑定由 PVC 同步流程完成。
			// PV 为某个 PVC 预留，但该 PVC 尚未
			// 绑定到该 PV，PVC 同步流程会处理。
			logger.V(4).Info("Synchronizing PersistentVolume, volume is pre-bound to claim", "PVC", klog.KRef(volume.Spec.ClaimRef.Namespace, volume.Spec.ClaimRef.Name), "volumeName", volume.Name)
			if _, err := ctrl.updateVolumePhase(ctx, volume, v1.VolumeAvailable, ""); err != nil {
				// 什么都没保存成功；下次调用本方法时仍会回到同样的状态并重试。
				return err
			}
			return nil
		}
		logger.V(4).Info("Synchronizing PersistentVolume, volume is bound to claim", "PVC", klog.KRef(volume.Spec.ClaimRef.Namespace, volume.Spec.ClaimRef.Name), "volumeName", volume.Name)
		// 按名称查找对应的 PVC。
		var claim *v1.PersistentVolumeClaim
		claimName := claimrefToClaimKey(volume.Spec.ClaimRef)
		obj, found, err := ctrl.claims.GetByKey(claimName)
		if err != nil {
			return err
		}
		if !found {
			// 如果 PV 是由外部制备器创建或由外部绑定器（如 kube-scheduler）绑定的，高负载下对应的 PVC 可能还没有同步到控制器本地缓存。
			// 因此需要依次确认 PVC 是否存在于： 1）informer 缓存；
			// 2）若 informer 中未找到，再查 API server；
			// 以确保不会错误地回收 PV。
			// 注意，当 PVC 不存在时，只有非 Released 和非 Failed 的卷才会被更新为 Released。
			if volume.Status.Phase != v1.VolumeReleased && volume.Status.Phase != v1.VolumeFailed {
				// 仅对非 Released/Failed 的卷做二次确认，避免把已处于终态的卷错误地回收。
				obj, err = ctrl.claimLister.PersistentVolumeClaims(volume.Spec.ClaimRef.Namespace).Get(volume.Spec.ClaimRef.Name)
				if err != nil && !apierrors.IsNotFound(err) {
					return err
				}
				found = !apierrors.IsNotFound(err)
				if !found {
					obj, err = ctrl.kubeClient.CoreV1().PersistentVolumeClaims(volume.Spec.ClaimRef.Namespace).Get(ctx, volume.Spec.ClaimRef.Name, metav1.GetOptions{})
					if err != nil && !apierrors.IsNotFound(err) {
						return err
					}
					found = !apierrors.IsNotFound(err)
				}
			}
		}
		if !found {
			logger.V(4).Info("Synchronizing PersistentVolume, claim not found", "PVC", klog.KRef(volume.Spec.ClaimRef.Namespace, volume.Spec.ClaimRef.Name), "volumeName", volume.Name)
			// 继续往下执行，此时 claim 为 nil。
		} else {
			var ok bool
			claim, ok = obj.(*v1.PersistentVolumeClaim)
			if !ok {
				return fmt.Errorf("cannot convert object from volume cache to volume %q!?: %#v", claim.Spec.VolumeName, obj)
			}
			logger.V(4).Info("Synchronizing PersistentVolume, claim found", "PVC", klog.KRef(volume.Spec.ClaimRef.Namespace, volume.Spec.ClaimRef.Name), "claimStatus", getClaimStatusForLogging(claim), "volumeName", volume.Name)
		}
		if claim != nil && claim.UID != volume.Spec.ClaimRef.UID {
			// 可能是同名声明被删除后重建；本地缓存可能过期，需向 API server 取最新声明核对 UID。
			// PV 原来指向的声明已被删除，
			// 之后又创建了一个同名声明。
			// 有时缓存中的声明不是最新的，而 volume.Spec.ClaimRef.UID 更新，
			// 因此需要向 API server 取最新声明再比较。
			logger.V(4).Info("Maybe cached claim is not the newest one, we should fetch it from apiserver", "PVC", klog.KRef(volume.Spec.ClaimRef.Namespace, volume.Spec.ClaimRef.Name))

			claim, err = ctrl.kubeClient.CoreV1().PersistentVolumeClaims(volume.Spec.ClaimRef.Namespace).Get(ctx, volume.Spec.ClaimRef.Name, metav1.GetOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				return err
			} else if claim != nil {
				// 把该卷视为绑定到一个已经不存在的声明。
				if claim.UID != volume.Spec.ClaimRef.UID {
					logger.V(4).Info("Synchronizing PersistentVolume, claim has a newer UID than pv.ClaimRef, the old one must have been deleted", "PVC", klog.KRef(volume.Spec.ClaimRef.Namespace, volume.Spec.ClaimRef.Name), "volumeName", volume.Name)
					claim = nil
				} else {
					logger.V(4).Info("Synchronizing PersistentVolume, claim has a same UID with pv.ClaimRef", "PVC", klog.KRef(volume.Spec.ClaimRef.Namespace, volume.Spec.ClaimRef.Name), "volumeName", volume.Name)
				}
			}
		}

		if claim == nil {
			// 进入此分支说明声明一定已被删除。 注意：reclaimVolume 可能把 PV 释放回资源池、回收它，或什么都不做（Retain）。

			// 不要覆盖之前的 Failed 状态：让用户看到曾出错，同时我们仍会继续尝试回收该卷。
			if volume.Status.Phase != v1.VolumeReleased && volume.Status.Phase != v1.VolumeFailed {
				// 不要覆盖之前的 Failed 状态，以便用户看到曾发生的错误；同时只记录一次日志。
				// 另外，这条日志只记录一次：
				logger.V(2).Info("Volume is released and reclaim policy will be executed", "volumeName", volume.Name, "reclaimPolicy", volume.Spec.PersistentVolumeReclaimPolicy)
				if volume, err = ctrl.updateVolumePhase(ctx, volume, v1.VolumeReleased, ""); err != nil {
					// 什么都没保存成功；下次调用本方法时仍会回到同样的状态并重试。
					return err
				}
			}
			if err = ctrl.reclaimVolume(ctx, volume); err != nil {
				// 释放失败，下次调用本方法时仍会回到同样的状态并重试。
				return err
			}
			if volume.Spec.PersistentVolumeReclaimPolicy == v1.PersistentVolumeReclaimRetain {
				// 卷的策略为 Retain，它仍引用着一个现在已经不存在的声明。
				logger.V(4).Info("PersistentVolume references a claim that is not found", "PVC", klog.KRef(volume.Spec.ClaimRef.Namespace, volume.Spec.ClaimRef.Name), "claimUID", volume.Spec.ClaimRef.UID, "volumeName", volume.Name)
			}
			return nil
		} else if claim.Spec.VolumeName == "" {
			if storagehelpers.CheckVolumeModeMismatches(&claim.Spec, &volume.Spec) {
				// syncUnboundClaim 中不会对该卷执行绑定，因为 volumeMode 不匹配时 findBestMatchForClaim 不会返回该卷。
				volumeMsg := fmt.Sprintf("Cannot bind PersistentVolume to requested PersistentVolumeClaim %q due to incompatible volumeMode.", claim.Name)
				ctrl.eventRecorder.Event(volume, v1.EventTypeWarning, events.VolumeMismatch, volumeMsg)
				claimMsg := fmt.Sprintf("Cannot bind PersistentVolume %q to requested PersistentVolumeClaim due to incompatible volumeMode.", volume.Name)
				ctrl.eventRecorder.Event(claim, v1.EventTypeWarning, events.VolumeMismatch, claimMsg)
				// 跳过本次 syncClaim。
				return nil
			}

			if metav1.HasAnnotation(volume.ObjectMeta, storagehelpers.AnnBoundByController) {
				// 控制器绑定的卷被解绑：这是控制器的正常修正逻辑，直接解除绑定。
				// 绑定尚未完成，交给 PVC 同步流程处理。
				logger.V(4).Info("Synchronizing PersistentVolume, volume not bound yet, waiting for syncClaim to fix it", "volumeName", volume.Name)
			} else {
				// 悬空的 PV，尝试在 PVC 同步流程中重新建立关联。
				logger.V(4).Info("Synchronizing PersistentVolume, volume was bound and got unbound (by user?), waiting for syncClaim to fix it", "volumeName", volume.Name)
			}
			// 两种情况下卷都是 Bound、声明都是 Pending，下一次 syncClaim 会修正它。
			// 为加快处理，这里把声明入队，从而很快在正确的工作 goroutine 中调用 syncClaim。
			// 这能加速已制备卷的绑定：制备器只保存新 PV，并期望下一次 syncClaim 把声明绑定到它。
			ctrl.claimQueue.Add(claimToClaimKey(claim))
			// 主动把声明入队以加快绑定，确保由正确的工作 goroutine 尽快调用 syncClaim。
			return nil
		} else if claim.Spec.VolumeName == volume.Name {
			// 卷已正确绑定到声明，必要时更新其状态。
			logger.V(4).Info("Synchronizing PersistentVolume, all is bound", "volumeName", volume.Name)
			if _, err = ctrl.updateVolumePhase(ctx, volume, v1.VolumeBound, ""); err != nil {
				// 什么都没保存成功；下次调用本方法时仍会回到同样的状态并重试。
				return err
			}
			return nil
		} else {
			// 卷绑定到了某个声明，但该声明却绑定到了别处。
			if metav1.HasAnnotation(volume.ObjectMeta, storagehelpers.AnnDynamicallyProvisioned) && volume.Spec.PersistentVolumeReclaimPolicy == v1.PersistentVolumeReclaimDelete {
				// 该卷是为这个声明动态制备的，而声明已绑定到别处，因此不再需要该卷，应删除它。 把卷标记为 Released，供外部删除器识别并提示用户；但不要覆盖已有的 Failed 状态！
				if volume.Status.Phase != v1.VolumeReleased && volume.Status.Phase != v1.VolumeFailed {
					// 动态制备的卷不再被需要时先标记 Released，供外部删除器识别并提示用户，但不覆盖 Failed。
					// 另外，这条日志只记录一次：
					logger.V(2).Info("Dynamically provisioned volume is released and it will be deleted", "volumeName", volume.Name)
					if volume, err = ctrl.updateVolumePhase(ctx, volume, v1.VolumeReleased, ""); err != nil {
						// 什么都没保存成功；下次调用本方法时仍会回到同样的状态并重试。
						return err
					}
				}
				if err = ctrl.reclaimVolume(ctx, volume); err != nil {
					// 删除失败，下次调用本方法时仍会回到同样的状态并重试。
					return err
				}
				return nil
			} else {
				// 卷绑定到了某个声明，但该声明绑定到了别处，且该卷不是动态制备的。
				if metav1.HasAnnotation(volume.ObjectMeta, storagehelpers.AnnBoundByController) {
					// 这是控制器正常操作的一部分：控制器曾尝试把该卷用于某个声明，但该声明后来由另一个卷满足了。 这是我们自己造成的，应当自行修正。
					logger.V(4).Info("Synchronizing PersistentVolume, volume is bound by controller to a claim that is bound to another volume, unbinding", "volumeName", volume.Name)
					if err = ctrl.unbindVolume(ctx, volume); err != nil {
						return err
					}
					return nil
				} else {
					// 这个 PV 必定是带着该 ClaimRef 创建的，保持原样、不要干预。
					logger.V(4).Info("Synchronizing PersistentVolume, volume is bound by user to a claim that is bound to another volume, waiting for the claim to get unbound", "volumeName", volume.Name)
					// 这里只更新卷阶段并清空 volume.Spec.ClaimRef.UID，仍保留卷到声明的预绑定关系。
					if err = ctrl.unbindVolume(ctx, volume); err != nil {
						return err
					}
					return nil
				}
			}
		}
	}
}

// updateClaimStatus 把新的 claim.Status 保存到 API server。
// 只有在确实发生变化时才写入；volume 不为 nil 时，同时把它的 Capacity 写入 claim.Status.Capacity，
// 把 VolumeAttributesClassName 写入 claim.Status.CurrentVolumeAttributesClassName。
// 参数：
//   - claim：要更新的声明
//   - phase：要设置的阶段
//   - volume：容量和卷属性类名称的来源卷（可为 nil，表示清空这些状态字段）
func (ctrl *PersistentVolumeController) updateClaimStatus(ctx context.Context, claim *v1.PersistentVolumeClaim, phase v1.PersistentVolumeClaimPhase, volume *v1.PersistentVolume) (*v1.PersistentVolumeClaim, error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Updating PersistentVolumeClaim status", "PVC", klog.KObj(claim), "setPhase", phase)

	dirty := false

	claimClone := claim.DeepCopy()
	// 传入的声明可能指向 informer 缓存，绝不能直接修改，必须先克隆。
	if claim.Status.Phase != phase {
		claimClone.Status.Phase = phase
		dirty = true
	}

	if volume == nil {
		// 需要清空 AccessModes、Capacity 和 CurrentVolumeAttributesClassName。
		if claim.Status.AccessModes != nil {
			claimClone.Status.AccessModes = nil
			dirty = true
		}
		if claim.Status.Capacity != nil {
			claimClone.Status.Capacity = nil
			dirty = true
		}
		if claim.Status.CurrentVolumeAttributesClassName != nil {
			claimClone.Status.CurrentVolumeAttributesClassName = nil
			dirty = true
		}
	} else {
		// 需要更新 AccessModes、Capacity 和 CurrentVolumeAttributesClassName。
		if !reflect.DeepEqual(claim.Status.AccessModes, volume.Spec.AccessModes) {
			claimClone.Status.AccessModes = volume.Spec.AccessModes
			dirty = true
		}

		// 只在声明即将进入 Bound 时更新 Capacity，而不是在它已经是 Bound 时更新。 容量不一致可能是有意的（PVC 文件系统大小与 PV 块设备大小不同），因此不要贸然覆盖。
		if claim.Status.Phase != phase {
			volumeCap, ok := volume.Spec.Capacity[v1.ResourceStorage]
			// 仅在声明新进入 Bound 时更新 Capacity：文件系统大小与块设备大小可能有意不同，不要随意覆盖。
			if !ok {
				return nil, fmt.Errorf("PersistentVolume %q is without a storage capacity", volume.Name)
			}
			claimCap, ok := claim.Status.Capacity[v1.ResourceStorage]
			// 如果 PV 带有扩容注解，则把声明的状态容量设为扩容前的值。
			if metav1.HasAnnotation(volume.ObjectMeta, util.AnnPreResizeCapacity) {
				// 卷需要文件系统扩容时，把 PVC 状态容量设为扩容前的值，以保持与实际文件系统一致。
				logger.V(2).Info("Volume requires filesystem resize: setting pvc status capacity", "PVC", klog.KObj(claim), "volumeName", volume.Name, "statusCapacity", volume.ObjectMeta.Annotations[util.AnnPreResizeCapacity])
				preQty, err := resource.ParseQuantity(volume.ObjectMeta.Annotations[util.AnnPreResizeCapacity])
				if err != nil {
					logger.Info("Parsing pre-resize-capacity from PV failed", "volumeName", volume.Name, "err", err)
					// 解析扩容前容量失败时回退到 PV 当前容量，不因注解格式错误而阻断状态更新。
					preQty = volume.Spec.Capacity[v1.ResourceStorage]
				}
				if claimClone.Status.Capacity == nil {
					claimClone.Status.Capacity = make(map[v1.ResourceName]resource.Quantity)
				}
				claimClone.Status.Capacity[v1.ResourceStorage] = preQty
				dirty = true
			} else if !ok || volumeCap.Cmp(claimCap) != 0 {
				claimClone.Status.Capacity = volume.Spec.Capacity
				dirty = true
			}
		}

		if utilfeature.DefaultFeatureGate.Enabled(features.VolumeAttributesClass) {
			// 更新当前 VAC 名称由两部分负责：本控制器和 external-resizer。
			// 控制器确保静态制备的卷在绑定时正确设置该字段。
			// 控制器只在绑定阶段设置该字段而不在其后修改，这样更安全；
			// 否则可能与调整器产生竞态 （调整器在控制器之后设置，或反过来）。
			// 此后该字段应只由调整器处理，除非管理员显式覆盖。
			if claim.Status.Phase == v1.ClaimPending && phase == v1.ClaimBound &&
				!reflect.DeepEqual(claim.Status.CurrentVolumeAttributesClassName, volume.Spec.VolumeAttributesClassName) {
				// VAC 字段只在本控制器完成绑定时设置：之后由 external-resizer 负责，避免与调整器产生竞态。
				claimClone.Status.CurrentVolumeAttributesClassName = volume.Spec.VolumeAttributesClassName
				dirty = true
			}
		}
	}

	if !dirty {
		// 无需处理。
		// 没有任何字段变化则直接返回，避免无意义的 API 写入。
		logger.V(4).Info("Updating PersistentVolumeClaim status, phase already set", "PVC", klog.KObj(claim), "phase", phase)
		return claim, nil
	}

	newClaim, err := ctrl.kubeClient.CoreV1().PersistentVolumeClaims(claimClone.Namespace).UpdateStatus(ctx, claimClone, metav1.UpdateOptions{})
	if err != nil {
		logger.V(4).Info("Updating PersistentVolumeClaim status, set phase failed", "PVC", klog.KObj(claim), "phase", phase, "err", err)
		return newClaim, err
	}
	_, err = ctrl.storeClaimUpdate(logger, newClaim)
	if err != nil {
		logger.V(4).Info("Updating PersistentVolumeClaim status: cannot update internal cache", "PVC", klog.KObj(claim), "err", err)
		return newClaim, err
	}
	logger.V(2).Info("Claim entered phase", "PVC", klog.KObj(claim), "phase", phase)
	return newClaim, nil
}

// updateClaimStatusWithEvent 把新的 claim.Status 保存到 API server，并在声明上发出指定事件。
// 只有当状态相对 API server 中的版本真正发生变化时才会保存状态并发出事件。
// 参数：
//   - claim：要更新的声明
//   - phase：要设置的阶段
//   - volume：容量和卷属性类名称的来源卷
//   - eventtype、reason、message：事件类型、原因和消息，见 EventRecorder.Event()
func (ctrl *PersistentVolumeController) updateClaimStatusWithEvent(ctx context.Context, claim *v1.PersistentVolumeClaim, phase v1.PersistentVolumeClaimPhase, volume *v1.PersistentVolume, eventtype, reason, message string) (*v1.PersistentVolumeClaim, error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Updating updateClaimStatusWithEvent", "PVC", klog.KObj(claim), "setPhase", phase)
	if claim.Status.Phase == phase {
		// 无需处理。
		// 阶段已是目标值时直接跳过，既避免重复写入也避免重复发事件。
		logger.V(4).Info("Updating updateClaimStatusWithEvent, phase already set", "PVC", klog.KObj(claim), "phase", phase)
		return claim, nil
	}

	newClaim, err := ctrl.updateClaimStatus(ctx, claim, phase, volume)
	if err != nil {
		return nil, err
	}

	// 事件只在状态真正发生变化时发出，而不是每次调用 syncClaim 都发。
	logger.V(3).Info("Claim changed status", "PVC", klog.KObj(claim), "phase", phase, "message", message)
	ctrl.eventRecorder.Event(newClaim, eventtype, reason, message)

	return newClaim, nil
}

// updateVolumePhase 把新的卷阶段保存到 API server。
// 若阶段已是目标值则直接返回；否则克隆卷、更新 Status.Phase 与 Status.Message，
// 写入 API server 后同步内部缓存。
func (ctrl *PersistentVolumeController) updateVolumePhase(ctx context.Context, volume *v1.PersistentVolume, phase v1.PersistentVolumePhase, message string) (*v1.PersistentVolume, error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Updating PersistentVolume", "volumeName", volume.Name, "setPhase", phase)
	if volume.Status.Phase == phase {
		// 无需处理。
		// 阶段未变化时不写状态也不发事件。
		// 阶段已是目标值时无需写入，直接返回。
		logger.V(4).Info("Updating PersistentVolume: phase already set", "volumeName", volume.Name, "phase", phase)
		return volume, nil
	}

	volumeClone := volume.DeepCopy()
	volumeClone.Status.Phase = phase
	volumeClone.Status.Message = message

	newVol, err := ctrl.kubeClient.CoreV1().PersistentVolumes().UpdateStatus(ctx, volumeClone, metav1.UpdateOptions{})
	if err != nil {
		logger.V(4).Info("Updating PersistentVolume: set phase failed", "volumeName", volume.Name, "phase", phase, "err", err)
		return newVol, err
	}
	_, err = ctrl.storeVolumeUpdate(logger, newVol)
	if err != nil {
		logger.V(4).Info("Updating PersistentVolume: cannot update internal cache", "volumeName", volume.Name, "err", err)
		return newVol, err
	}
	logger.V(2).Info("Volume entered phase", "volumeName", volume.Name, "phase", phase)
	return newVol, err
}

// updateVolumePhaseWithEvent 把新的卷阶段保存到 API server，并在卷上发出指定事件。
// 只有当阶段相对 API server 中的版本真正发生变化时才会保存并发出事件。
func (ctrl *PersistentVolumeController) updateVolumePhaseWithEvent(ctx context.Context, volume *v1.PersistentVolume, phase v1.PersistentVolumePhase, eventtype, reason, message string) (*v1.PersistentVolume, error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Updating updateVolumePhaseWithEvent", "volumeName", volume.Name, "setPhase", phase)
	if volume.Status.Phase == phase {
		// 无需处理。
		logger.V(4).Info("Updating updateVolumePhaseWithEvent: phase already set", "volumeName", volume.Name, "phase", phase)
		return volume, nil
	}

	newVol, err := ctrl.updateVolumePhase(ctx, volume, phase, message)
	if err != nil {
		return nil, err
	}

	// 事件只在状态真正发生变化时发出，而不是每次调用 syncClaim 都发。
	logger.V(3).Info("Volume changed status", "volumeName", volume.Name, "changedPhase", phase, "message", message)
	ctrl.eventRecorder.Event(newVol, eventtype, reason, message)

	return newVol, nil
}

// assignDefaultStorageClass 为没有指定 StorageClass 的声明补上默认 StorageClass，并写回 API server。
// 已指定 StorageClass 的声明会被忽略；找不到默认类时不做修改。
// TODO: 如果 resync 周期被调大，可能需要改变为已有未绑定声明设置默认类的方式。
func (ctrl *PersistentVolumeController) assignDefaultStorageClass(ctx context.Context, claim *v1.PersistentVolumeClaim) (bool, error) {
	logger := klog.FromContext(ctx)

	if storagehelpers.PersistentVolumeClaimHasClass(claim) {
		// 用户已指定 StorageClass，必须尊重其选择，不能覆盖。
		// 用户已指定 StorageClass。
		return false, nil
	}

	class, err := util.GetDefaultClass(ctrl.classLister)
	if err != nil {
		return false, err
	} else if class == nil {
		// 没有默认 StorageClass 时保持声明原样，由后续流程按无类情况处理。
		logger.V(4).Info("Can not assign storage class to PersistentVolumeClaim: default storage class not found", "PVC", klog.KObj(claim))
		return false, nil
	}

	logger.V(4).Info("Assigning StorageClass to PersistentVolumeClaim", "PVC", klog.KObj(claim), "storageClassName", class.Name)
	claim.Spec.StorageClassName = &class.Name
	_, err = ctrl.kubeClient.CoreV1().PersistentVolumeClaims(claim.GetNamespace()).Update(ctx, claim, metav1.UpdateOptions{})
	if err != nil {
		return false, err
	}

	logger.V(4).Info("Successfully assigned StorageClass to PersistentVolumeClaim", "PVC", klog.KObj(claim), "storageClassName", class.Name)
	return true, nil
}

// bindVolumeToClaim 修改给定卷使其绑定到声明，并保存到 API server。
// 该方法不会修改声明本身；若没有任何变化则跳过写入。
func (ctrl *PersistentVolumeController) bindVolumeToClaim(ctx context.Context, volume *v1.PersistentVolume, claim *v1.PersistentVolumeClaim) (*v1.PersistentVolume, error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Updating PersistentVolume: binding to claim", "PVC", klog.KObj(claim), "volumeName", volume.Name)

	volumeClone, dirty, err := storagehelpers.GetBindVolumeToClaim(volume, claim)
	if err != nil {
		return nil, err
	}

	// 只有确实发生改动时才保存卷。
	if dirty {
		return ctrl.updateBindVolumeToClaim(ctx, volumeClone, true)
		// 只有 ClaimRef 等字段确实变化时才写回，避免无意义的 API 调用。
	}

	logger.V(4).Info("Updating PersistentVolume: already bound to claim", "PVC", klog.KObj(claim), "volumeName", volume.Name)
	return volume, nil
}

// updateBindVolumeToClaim 把已经准备好的卷副本写入 API server 完成绑定。
// 与 bindVolumeToClaim 不同，它不再计算差异，直接执行更新；updateCache 控制是否同步内部缓存。
func (ctrl *PersistentVolumeController) updateBindVolumeToClaim(ctx context.Context, volumeClone *v1.PersistentVolume, updateCache bool) (*v1.PersistentVolume, error) {
	logger := klog.FromContext(ctx)
	logger.V(2).Info("Claim bound to volume", "PVC", klog.KRef(volumeClone.Spec.ClaimRef.Namespace, volumeClone.Spec.ClaimRef.Name), "volumeName", volumeClone.Name)
	newVol, err := ctrl.kubeClient.CoreV1().PersistentVolumes().Update(ctx, volumeClone, metav1.UpdateOptions{})
	if err != nil {
		logger.V(4).Info("Updating PersistentVolume: binding to claim failed", "PVC", klog.KRef(volumeClone.Spec.ClaimRef.Namespace, volumeClone.Spec.ClaimRef.Name), "volumeName", volumeClone.Name, "err", err)
		return newVol, err
	}
	if updateCache {
		// 某些调用场景不希望立刻更新缓存（例如随后还会修改卷），由调用方决定是否同步。
		_, err = ctrl.storeVolumeUpdate(logger, newVol)
		if err != nil {
			logger.V(4).Info("Updating PersistentVolume: cannot update internal cache", "volumeName", volumeClone.Name, "err", err)
			return newVol, err
		}
	}
	logger.V(4).Info("Updating PersistentVolume: bound to claim", "PVC", klog.KRef(volumeClone.Spec.ClaimRef.Namespace, volumeClone.Spec.ClaimRef.Name), "volumeName", newVol.Name)
	return newVol, nil
}

// bindClaimToVolume 修改给定声明使其绑定到卷，并保存到 API server。
// 该方法不会修改卷本身；会设置 Spec.VolumeName、AnnBoundByController 和 AnnBindCompleted 注解。
func (ctrl *PersistentVolumeController) bindClaimToVolume(ctx context.Context, claim *v1.PersistentVolumeClaim, volume *v1.PersistentVolume) (*v1.PersistentVolumeClaim, error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Updating PersistentVolumeClaim: binding to volume", "PVC", klog.KObj(claim), "volumeName", volume.Name)

	dirty := false

	// 检查声明是否已绑定（无论由控制器还是用户绑定）。
	shouldBind := false
	if volume.Name != claim.Spec.VolumeName {
		shouldBind = true
	}

	// 方法参数中的声明可能指向 watcher 缓存，绝不能直接修改，必须先创建副本。
	claimClone := claim.DeepCopy()

	if shouldBind {
		dirty = true
		// 把声明绑定到卷。
		claimClone.Spec.VolumeName = volume.Name

		// 若尚未设置 AnnBoundByController 注解，则补上。
		if !metav1.HasAnnotation(claimClone.ObjectMeta, storagehelpers.AnnBoundByController) {
			// 标记绑定由控制器完成，便于后续冲突时判断能否自动解绑。
			metav1.SetMetaDataAnnotation(&claimClone.ObjectMeta, storagehelpers.AnnBoundByController, "yes")
		}
	}

	// 若尚未设置 AnnBindCompleted 注解，则补上。
	if !metav1.HasAnnotation(claimClone.ObjectMeta, storagehelpers.AnnBindCompleted) {
		// AnnBindCompleted 用于区分声明是否已进入绑定流程，是 syncClaim 分派的关键标志。
		metav1.SetMetaDataAnnotation(&claimClone.ObjectMeta, storagehelpers.AnnBindCompleted, "yes")
		dirty = true
	}

	if dirty {
		logger.V(2).Info("Volume bound to claim", "PVC", klog.KObj(claim), "volumeName", volume.Name)
		newClaim, err := ctrl.kubeClient.CoreV1().PersistentVolumeClaims(claim.Namespace).Update(ctx, claimClone, metav1.UpdateOptions{})
		if err != nil {
			logger.V(4).Info("Updating PersistentVolumeClaim: binding to volume failed", "PVC", klog.KObj(claim), "volumeName", volume.Name, "err", err)
			return newClaim, err
		}
		_, err = ctrl.storeClaimUpdate(logger, newClaim)
		if err != nil {
			logger.V(4).Info("Updating PersistentVolumeClaim: cannot update internal cache", "PVC", klog.KObj(claim), "err", err)
			return newClaim, err
		}
		logger.V(4).Info("Updating PersistentVolumeClaim: bound to volume", "PVC", klog.KObj(claim), "volumeName", volume.Name)
		return newClaim, nil
	}

	logger.V(4).Info("Updating PersistentVolumeClaim: already bound to volume", "PVC", klog.KObj(claim), "volumeName", volume.Name)
	return claim, nil
}

// bind 把绑定信息同时写入卷和声明，并把两者都标记为 Bound。
// 按“卷 Spec → 卷 Status → 声明 Spec → 声明 Status”的顺序保存：先改卷，
// 这样即使中途失败，后续同步也能从卷侧恢复。遇到第一个错误就返回，重试由调用方负责。
func (ctrl *PersistentVolumeController) bind(ctx context.Context, volume *v1.PersistentVolume, claim *v1.PersistentVolumeClaim) error {
	var err error
	// 使用 updateClaim/updatedVolume 变量保存更新后的对象，以便在出错时仍能记录原始的声明/卷。
	var updatedClaim *v1.PersistentVolumeClaim
	var updatedVolume *v1.PersistentVolume

	logger := klog.FromContext(ctx)
	logger.V(4).Info("Binding volume to claim", "PVC", klog.KObj(claim), "volumeName", volume.Name)

	if updatedVolume, err = ctrl.bindVolumeToClaim(ctx, volume, claim); err != nil {
		// 先绑定卷是两步事务的第一步，失败时声明尚未被改动，可安全重试。
		logger.V(3).Info("Error binding volume to claim: failed saving the volume", "PVC", klog.KObj(claim), "volumeName", volume.Name, "err", err)
		return err
	}
	volume = updatedVolume

	if updatedVolume, err = ctrl.updateVolumePhase(ctx, volume, v1.VolumeBound, ""); err != nil {
		logger.V(3).Info("Error binding volume to claim: failed saving the volume status", "PVC", klog.KObj(claim), "volumeName", volume.Name, "err", err)
		return err
	}
	volume = updatedVolume

	if updatedClaim, err = ctrl.bindClaimToVolume(ctx, claim, volume); err != nil {
		// 卷侧完成后再改声明，避免声明先指向一个尚未预留的卷。
		logger.V(3).Info("Error binding volume to claim: failed saving the claim", "PVC", klog.KObj(claim), "volumeName", volume.Name, "err", err)
		return err
	}
	claim = updatedClaim

	if updatedClaim, err = ctrl.updateClaimStatus(ctx, claim, v1.ClaimBound, volume); err != nil {
		logger.V(3).Info("Error binding volume to claim: failed saving the claim status", "PVC", klog.KObj(claim), "volumeName", volume.Name, "err", err)
		return err
	}
	claim = updatedClaim

	logger.V(4).Info("Volume bound to claim", "PVC", klog.KObj(claim), "volumeName", volume.Name)
	logger.V(4).Info("Volume status after binding", "volumeName", volume.Name, "volumeStatus", getVolumeStatusForLogging(volume))
	logger.V(4).Info("Claim status after binding", "PVC", klog.KObj(claim), "claimStatus", getClaimStatusForLogging(claim))
	return nil
}

// unbindVolume 回滚卷上先前的绑定信息。
// 当两个控制器把两个卷绑定到同一个声明时，只有一个绑定会成功，另一个必须回滚。
// 该方法同时更新 Spec 和 Status；遇到第一个错误就返回，重试由调用方负责。
func (ctrl *PersistentVolumeController) unbindVolume(ctx context.Context, volume *v1.PersistentVolume) error {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("Updating PersistentVolume: rolling back binding from claim", "PVC", klog.KRef(volume.Spec.ClaimRef.Namespace, volume.Spec.ClaimRef.Name), "volumeName", volume.Name)

	// 只有确实需要修改时才保存 PV。
	volumeClone := volume.DeepCopy()

	if metav1.HasAnnotation(volume.ObjectMeta, storagehelpers.AnnBoundByController) {
		// 该卷由控制器绑定。
		// 控制器绑定的卷可以彻底清除 ClaimRef；用户预绑定的卷只需清空 UID，保留预绑定关系。
		volumeClone.Spec.ClaimRef = nil
		delete(volumeClone.Annotations, storagehelpers.AnnBoundByController)
		if len(volumeClone.Annotations) == 0 {
			// 空注解 map 不如 nil 直观，也难测试，因此清空为 nil。
			// 没有注解比空注解 map 更好（也更容易
			// 测试）。
			volumeClone.Annotations = nil
		}
	} else {
		// 该卷由用户预绑定，只清空绑定的 UID，保留其余预绑定信息。
		volumeClone.Spec.ClaimRef.UID = ""
	}

	newVol, err := ctrl.kubeClient.CoreV1().PersistentVolumes().Update(ctx, volumeClone, metav1.UpdateOptions{})
	if err != nil {
		logger.V(4).Info("Updating PersistentVolume: rollback failed", "volumeName", volume.Name, "err", err)
		return err
	}
	_, err = ctrl.storeVolumeUpdate(logger, newVol)
	if err != nil {
		logger.V(4).Info("Updating PersistentVolume: cannot update internal cache", "volumeName", volume.Name, "err", err)
		return err
	}
	logger.V(4).Info("Updating PersistentVolume: rolled back", "volumeName", newVol.Name)

	// 更新状态。
	_, err = ctrl.updateVolumePhase(ctx, newVol, v1.VolumeAvailable, "")
	return err
}

// reclaimVolume 根据 volume.Spec.PersistentVolumeReclaimPolicy 执行相应的回收动作：
// Retain 不处理，Recycle 和 Delete 调度为异步操作，未知策略则把卷标记为 Failed。
func (ctrl *PersistentVolumeController) reclaimVolume(ctx context.Context, volume *v1.PersistentVolume) error {
	logger := klog.FromContext(ctx)
	if migrated := volume.Annotations[storagehelpers.AnnMigratedTo]; len(migrated) > 0 {
		// 已迁移到 CSI 的 PV 应由外部制备器处理，PV 控制器必须让位，不做回收。
		// PV 已迁移，PV 控制器应当让位，
		// 由外部制备器处理该 PV。
		return nil
	}
	switch volume.Spec.PersistentVolumeReclaimPolicy {
	case v1.PersistentVolumeReclaimRetain:
		logger.V(4).Info("ReclaimVolume: policy is Retain, nothing to do", "volumeName", volume.Name)

	case v1.PersistentVolumeReclaimRecycle:
		logger.V(4).Info("ReclaimVolume: policy is Recycle", "volumeName", volume.Name)
		opName := fmt.Sprintf("recycle-%s[%s]", volume.Name, string(volume.UID))
		ctrl.scheduleOperation(logger, opName, func() error {
			ctrl.recycleVolumeOperation(ctx, volume)
			return nil
		})

	case v1.PersistentVolumeReclaimDelete:
		logger.V(4).Info("ReclaimVolume: policy is Delete", "volumeName", volume.Name)
		// Delete 策略需要记录删除操作的起始时间，以便后续统计删除延迟。
		opName := fmt.Sprintf("delete-%s[%s]", volume.Name, string(volume.UID))
		// 为删除操作记录起始时间戳（若尚无对应条目）：键为 volume.Name，pluginName 为制备器名称，operation 为 "delete"。
		ctrl.operationTimestamps.AddIfNotExist(volume.Name, ctrl.getProvisionerNameFromVolume(volume), "delete")
		ctrl.scheduleOperation(logger, opName, func() error {
			_, err := ctrl.deleteVolumeOperation(ctx, volume)
			if err != nil {
				// 这里只把错误计数上报到 "volume_operation_total_errors"；延迟指标会在卷最终被删除、 捕获到卷删除事件后再上报。
				metrics.RecordMetric(volume.Name, &ctrl.operationTimestamps, err)
			}
			return err
		})

	default:
		// 未知的 PersistentVolumeReclaimPolicy。
		// 未知回收策略无法自动处理，标记为 Failed 并发出告警事件让用户介入。
		if _, err := ctrl.updateVolumePhaseWithEvent(ctx, volume, v1.VolumeFailed, v1.EventTypeWarning, "VolumeUnknownReclaimPolicy", "Volume has unrecognized PersistentVolumeReclaimPolicy"); err != nil {
			return err
		}
	}
	return nil
}

// recycleVolumeOperation 回收一个卷。该方法运行在独立的 goroutine 中，已持有所有必要的锁。
// 它先重新读取卷的最新状态并确认仍需回收、且没有被 Pod 使用，再查找回收插件执行回收；
// 成功后解除绑定使卷重新可用，任何失败都只记录日志/事件并交给下一次同步重试。
func (ctrl *PersistentVolumeController) recycleVolumeOperation(ctx context.Context, volume *v1.PersistentVolume) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("RecycleVolumeOperation started", "volumeName", volume.Name)

	// 本方法可能已等待卷锁一段时间。
	// 上一次 recycleVolumeOperation 可能刚保存了更新版本，
	// 因此现在重新读取卷的最新状态。
	// 等待锁期间卷可能已被上一次操作更新，必须重新读取以避免基于陈旧状态操作。
	newVolume, err := ctrl.kubeClient.CoreV1().PersistentVolumes().Get(ctx, volume.Name, metav1.GetOptions{})
	if err != nil {
		logger.V(3).Info("Error reading persistent volume", "volumeName", volume.Name, "err", err)
		return
	}
	needsReclaim, err := ctrl.isVolumeReleased(logger, newVolume)
	if err != nil {
		logger.V(3).Info("Error reading claim for volume", "volumeName", volume.Name, "err", err)
		return
	}
	if !needsReclaim {
		logger.V(3).Info("Volume no longer needs recycling, skipping", "volumeName", volume.Name)
		// 卷可能已被其他流程处理，不再需要回收时直接跳过。
		return
	}
	pods, used, err := ctrl.isVolumeUsed(newVolume)
	if err != nil {
		logger.V(3).Info("Can't recycle volume", "volumeName", volume.Name, "err", err)
		return
	}

	// 确认缓存中是否存在该声明：若存在，则它必定是同名的新 PVC，
	// 因为此时已知该卷已 Released；缓存中的新 PVC 必须使用
	// 另一个 PV（isVolumeReleased 已确认本卷未被使用）。
	// 因此回收旧 PV 是安全的。
	// 若缓存中存在同名 PVC，它一定是新建的不同声明（旧 PV 已 Released 且未被使用），因此回收旧 PV 是安全的。
	claimName := claimrefToClaimKey(volume.Spec.ClaimRef)
	_, claimCached, err := ctrl.claims.GetByKey(claimName)
	if err != nil {
		logger.V(3).Info("Error getting the claim from cache", "PVC", klog.KRef(volume.Spec.ClaimRef.Namespace, volume.Spec.ClaimRef.Name))
		return
	}

	if used && !claimCached {
		// 卷仍被 Pod 使用且没有同名新声明时，回收会破坏数据，必须放弃。
		msg := fmt.Sprintf("Volume is used by pods: %s", strings.Join(pods, ","))
		logger.V(3).Info("Can't recycle volume", "volumeName", volume.Name, "msg", msg)
		ctrl.eventRecorder.Event(volume, v1.EventTypeNormal, events.VolumeFailedRecycle, msg)
		return
	}

	// 使用最新读取的卷副本，可避免保存时发生
	// 版本冲突。
	// 使用最新读取的副本可避免后续保存时发生版本冲突。
	volume = newVolume

	// 查找插件。
	spec := vol.NewSpecFromPersistentVolume(volume, false)
	plugin, err := ctrl.volumePluginMgr.FindRecyclablePluginBySpec(spec)
	if err != nil {
		// 未找到回收插件，发出事件并把卷标记为 Failed。
		// 找不到回收插件时把卷标记为 Failed 并发出事件；即使 Failed，仍会在每次 syncVolume 时重试。
		if _, err = ctrl.updateVolumePhaseWithEvent(ctx, volume, v1.VolumeFailed, v1.EventTypeWarning, events.VolumeFailedRecycle, "No recycler plugin found for the volume!"); err != nil {
			logger.V(4).Info("RecycleVolumeOperation: failed to mark volume as failed", "volumeName", volume.Name, "err", err)
			// 保存失败，下一次删除尝试时会重试。
			return
		}
		// 即使卷被标记为 Failed，控制器仍会在每次 syncVolume() 调用时重试回收。
		return
	}

	// 已找到插件。
	recorder := ctrl.newRecyclerEventRecorder(volume)

	if err = plugin.Recycle(volume.Name, spec, recorder); err != nil {
		// 回收失败。
		strerr := fmt.Sprintf("Recycle failed: %s", err)
		if _, err = ctrl.updateVolumePhaseWithEvent(ctx, volume, v1.VolumeFailed, v1.EventTypeWarning, events.VolumeFailedRecycle, strerr); err != nil {
			logger.V(4).Info("RecycleVolumeOperation: failed to mark volume as failed", "volumeName", volume.Name, "err", err)
			// 保存失败，下一次删除尝试时会重试。
			return
		}
		// 即使卷被标记为 Failed，控制器仍会在每次 syncVolume() 调用时重试回收。
		return
	}

	logger.V(2).Info("Volume recycled", "volumeName", volume.Name)
	// 发出事件。
	ctrl.eventRecorder.Event(volume, v1.EventTypeNormal, events.VolumeRecycled, "Volume recycled")
	// 让卷重新变为 Available。
	if err = ctrl.unbindVolume(ctx, volume); err != nil {
		// 糟糕，卷未能保存，因此控制器会
		// 解绑失败会导致下次同步再次回收；维护“最近已回收卷”缓存来避免重复是未来的优化点。
		// 在下次更新时再次回收。我们*可以*维护一个
		// “最近已回收卷”缓存来避免不必要的回收，
		// 这留作未来的优化。
		logger.V(3).Info("RecycleVolumeOperation: failed to make recycled volume 'Available', we will recycle the volume again", "volumeName", volume.Name, "err", err)
		return
	}
}

// deleteVolumeOperation 删除一个卷。该方法运行在独立的 goroutine 中，已持有所有必要的锁。
// 它查找删除插件、重新确认卷仍待删除、执行底层存储删除，最后从 API server 删除 PV 对象。
// 返回插件名供指标使用；底层删除失败时标记卷 Failed 或（对 VolumeInUse 错误）仅发普通事件。
func (ctrl *PersistentVolumeController) deleteVolumeOperation(ctx context.Context, volume *v1.PersistentVolume) (string, error) {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("DeleteVolumeOperation started", "volumeName", volume.Name)

	plugin, err := ctrl.findDeletablePlugin(volume)
	if err != nil {
		if _, err := ctrl.updateVolumePhaseWithEvent(ctx, volume, v1.VolumeFailed, v1.EventTypeWarning, events.VolumeFailedDelete, err.Error()); err != nil {
			logger.V(4).Info("DeleteVolumeOperation: failed to mark volume as failed", "volumeName", volume.Name, "err", err)
			// 保存失败，下一次删除尝试时会重试。
			return "", err
		}
	}
	if plugin == nil {
		// 已请求外部删除器，这里不做任何操作。
		// 找到外部删除器时不执行内部删除，由外部组件负责。
		logger.V(3).Info("External deleter for volume requested, ignoring", "volumeName", volume.Name)
		return "", nil
	}
	pluginName := plugin.GetPluginName()
	logger.V(5).Info("Found a deleter plugin for volume", "pluginName", pluginName, "volumeName", volume.Name)

	// 本方法可能已等待卷锁一段时间。
	// 上一次 deleteVolumeOperation 可能刚保存了更新版本，
	// 因此现在重新读取卷的最新状态。
	// 等待锁期间卷可能已被更新，重新读取最新状态再判断是否仍需删除。
	newVolume, err := ctrl.kubeClient.CoreV1().PersistentVolumes().Get(ctx, volume.Name, metav1.GetOptions{})
	if err != nil {
		logger.V(3).Info("Error reading persistent volume", "volumeName", volume.Name, "err", err)
		return "", nil
	}

	needsReclaim, err := ctrl.isVolumeReleased(logger, newVolume)
	if err != nil {
		logger.V(3).Info("Error reading claim for volume", "volumeName", volume.Name, "err", err)
		return "", nil
	}
	if !needsReclaim {
		logger.V(3).Info("Volume no longer needs deletion, skipping", "volumeName", volume.Name)
		return "", nil
	}

	err = ctrl.doDeleteVolume(ctx, plugin, volume)
	if err != nil {
		// 删除失败，更新卷状态并发出事件。
		logger.V(3).Info("Deletion of volume failed", "volumeName", volume.Name, "err", err)
		if volerr.IsDeletedVolumeInUse(err) {
			// 插件只是需要更多时间，不把卷标记为 Failed，
			// 卷仍被使用时只需插件再等等，不标记 Failed，只发普通事件。
			// 只发普通事件。
			ctrl.eventRecorder.Event(volume, v1.EventTypeNormal, events.VolumeDelete, err.Error())
		} else {
			// 插件执行失败，把卷标记为 Failed 并发出 Warning 事件。
			if _, err := ctrl.updateVolumePhaseWithEvent(ctx, volume, v1.VolumeFailed, v1.EventTypeWarning, events.VolumeFailedDelete, err.Error()); err != nil {
				logger.V(4).Info("DeleteVolumeOperation: failed to mark volume as failed", "volumeName", volume.Name, "err", err)
				// 保存失败，下一次删除尝试时会重试。
				return pluginName, err
			}
		}

		// 即使卷被标记为 Failed，控制器仍会在每次 syncVolume() 调用时重试删除。
		return pluginName, err
	}

	logger.V(4).Info("DeleteVolumeOperation: success", "volumeName", volume.Name)
	// 删除该卷。
	if err = ctrl.kubeClient.CoreV1().PersistentVolumes().Delete(ctx, volume.Name, metav1.DeleteOptions{}); err != nil {
		// 糟糕，卷未能删除，因此控制器会
		// 删除 PV 对象失败时下次同步会重试；维护“最近已删除卷”缓存是未来的优化点。
		// 在下次更新时再次尝试删除。我们*可以*维护一个
		// “最近已删除卷”缓存来避免不必要的删除，
		// 这留作未来的优化。
		logger.V(3).Info("Failed to delete volume from database", "volumeName", volume.Name, "err", err)
		return pluginName, nil
	}
	return pluginName, nil
}

// isVolumeReleased 判断给定卷是否已释放，可按其回收策略被回收或删除。
// 条件：卷有 ClaimRef，且该声明不存在、UID 不匹配，或声明已绑定到其他卷。
func (ctrl *PersistentVolumeController) isVolumeReleased(logger klog.Logger, volume *v1.PersistentVolume) (bool, error) {
	// 卷带有 ClaimRef 且对应声明不存在时，就需要执行回收。
	if volume.Spec.ClaimRef == nil {
		logger.V(4).Info("isVolumeReleased: ClaimRef is nil", "volumeName", volume.Name)
		return false, nil
	}
	if volume.Spec.ClaimRef.UID == "" {
		// 这是用户预绑定的卷，控制器尚未
		// UID 为空表示用户预绑定尚未完成，控制器还不能回收。
		// 完成到真实声明的绑定。
		logger.V(4).Info("isVolumeReleased: ClaimRef is not bound", "volumeName", volume.Name)
		return false, nil
	}

	var claim *v1.PersistentVolumeClaim
	claimName := claimrefToClaimKey(volume.Spec.ClaimRef)
	obj, found, err := ctrl.claims.GetByKey(claimName)
	if err != nil {
		return false, err
	}
	if !found {
		// 继续往下执行，此时 claim 为 nil。
	} else {
		var ok bool
		claim, ok = obj.(*v1.PersistentVolumeClaim)
		if !ok {
			return false, fmt.Errorf("cannot convert object from claim cache to claim!?: %#v", obj)
		}
	}
	if claim != nil && claim.UID == volume.Spec.ClaimRef.UID {
		// 声明仍然存在且 UID 正确。

		if len(claim.Spec.VolumeName) > 0 && claim.Spec.VolumeName != volume.Name {
			// 声明仍存在但已绑到其他卷，说明本卷已被释放。
			// 声明已绑定到另一个 PV，因此本 PV *确实*已释放。
			return true, nil
		}

		logger.V(4).Info("isVolumeReleased: ClaimRef is still valid, volume is not released", "volumeName", volume.Name)
		return false, nil
	}

	logger.V(2).Info("isVolumeReleased: volume is released", "volumeName", volume.Name)
	return true, nil
}

// findPodsByPVCKey 通过 PVC 索引查找所有引用该 PVC 的 Pod。
// 用索引而非遍历全部 Pod，避免回收 PV 时每次都做全量扫描。
// findPodsByPVCKey 根据 PVC 的 "namespace/name" 键，从 Pod 索引中找出所有引用该 PVC 的 Pod。
// 索引中类型不匹配的对象会被跳过。
func (ctrl *PersistentVolumeController) findPodsByPVCKey(key string) ([]*v1.Pod, error) {
	pods := []*v1.Pod{}
	objs, err := ctrl.podIndexer.ByIndex(common.PodPVCIndex, key)
	if err != nil {
		return pods, err
	}
	for _, obj := range objs {
		pod, ok := obj.(*v1.Pod)
		// 索引里可能有非 Pod 对象，忽略它们而不是报错。
		if !ok {
			continue
		}
		pods = append(pods, pod)
	}
	return pods, err
}

// isVolumeUsed 返回正在使用给定 PV 的活跃 Pod 列表。
// 通过 PVC 键查找引用该 PVC 的 Pod，过滤掉已终止的 Pod；返回名称列表以及是否确实有 Pod 在使用。
func (ctrl *PersistentVolumeController) isVolumeUsed(pv *v1.PersistentVolume) ([]string, bool, error) {
	if pv.Spec.ClaimRef == nil {
		return nil, false, nil
	}
	podNames := sets.NewString()
	pvcKey := fmt.Sprintf("%s/%s", pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name)
	pods, err := ctrl.findPodsByPVCKey(pvcKey)
	if err != nil {
		return nil, false, fmt.Errorf("error finding pods by pvc %q: %s", pvcKey, err)
	}
	for _, pod := range pods {
		if util.IsPodTerminated(pod, pod.Status) {
			continue
		}
		// 已终止的 Pod 不再占用卷，不应阻止回收。
		podNames.Insert(pod.Namespace + "/" + pod.Name)
	}
	return podNames.List(), podNames.Len() != 0, nil
}

// findNonScheduledPodsByPVC 返回引用给定 PVC 且尚未被调度的活跃 Pod 名称列表。
// 用于延迟绑定模式下提示用户等待 Pod 调度。
func (ctrl *PersistentVolumeController) findNonScheduledPodsByPVC(pvc *v1.PersistentVolumeClaim) ([]string, error) {
	pvcKey := fmt.Sprintf("%s/%s", pvc.Namespace, pvc.Name)
	pods, err := ctrl.findPodsByPVCKey(pvcKey)
	if err != nil {
		return nil, err
	}
	podNames := []string{}
	for _, pod := range pods {
		if util.IsPodTerminated(pod, pod.Status) {
			continue
		}
		if len(pod.Spec.NodeName) == 0 {
			// NodeName 为空表示 Pod 尚未被调度，正是延迟绑定要等待的对象。
			podNames = append(podNames, pod.Name)
		}
	}
	return podNames, nil
}

// doDeleteVolume 使用给定的可删除插件删除卷，并记录卷删除操作的耗时。
// 删除成功后会移除 in-tree 删除保护 finalizer，否则 PV 对象将无法被真正删除。
func (ctrl *PersistentVolumeController) doDeleteVolume(ctx context.Context, plugin vol.DeletableVolumePlugin, volume *v1.PersistentVolume) error {
	logger := klog.FromContext(ctx)
	logger.V(4).Info("doDeleteVolume", "volumeName", volume.Name)
	var err error

	pluginName := plugin.GetPluginName()
	spec := vol.NewSpecFromPersistentVolume(volume, false)
	deleter, err := plugin.NewDeleter(logger, spec)
	if err != nil {
		// 无法创建删除器。
		return fmt.Errorf("failed to create deleter for volume %q: %w", volume.Name, err)
	}

	opComplete := util.OperationCompleteHook(pluginName, "volume_delete")
	// 通过 hook 记录底层删除操作的耗时指标。
	err = deleter.Delete()
	opComplete(volumetypes.CompleteFuncParam{Err: &err})
	if err != nil {
		// 删除器执行失败。
		return err
	}
	logger.V(2).Info("Volume deleted", "volumeName", volume.Name)
	if err := ctrl.removeDeletionProtectionFinalizer(ctx, volume); err != nil {
		// 必须移除删除保护 finalizer，否则 PV 对象会一直卡在终止状态。
		return err
	}
	return nil
}

// removeDeletionProtectionFinalizer 在卷删除完成后移除树内删除保护 finalizer。
// 必须先从 API Server 重新读取最新对象再修改，否则可能覆盖其他控制器对
// finalizer 的并发变更；只有确实存在该 finalizer 时才发起更新。
// removeDeletionProtectionFinalizer 从 PV 上移除 in-tree 删除保护 finalizer。
// 先获取最新版本以避免基于陈旧对象更新，只有确实包含该 finalizer 时才写回并更新缓存。
func (ctrl *PersistentVolumeController) removeDeletionProtectionFinalizer(ctx context.Context, volume *v1.PersistentVolume) error {
	// 获取最新版本。
	// 必须先取最新版本，避免覆盖其他流程对 PV 的修改。
	logger := klog.FromContext(ctx)
	newVolume, err := ctrl.kubeClient.CoreV1().PersistentVolumes().Get(ctx, volume.Name, metav1.GetOptions{})
	if err != nil {
		logger.Error(err, "Error reading persistent volume", "volumeName", volume.Name)
		return err
	}
	volume = newVolume
	volumeClone := volume.DeepCopy()
	pvFinalizers := volumeClone.Finalizers
	if pvFinalizers != nil && slices.Contains(pvFinalizers, storagehelpers.PVDeletionInTreeProtectionFinalizer) {
		// 只有存在该 finalizer 时才更新，减少不必要的 API 写入。
		volumeClone.SetFinalizers(slice.RemoveString(pvFinalizers, storagehelpers.PVDeletionInTreeProtectionFinalizer, nil))
		_, err = ctrl.kubeClient.CoreV1().PersistentVolumes().Update(ctx, volumeClone, metav1.UpdateOptions{})
		if err != nil {
			return fmt.Errorf("persistent volume controller can't update finalizer: %v", err)
		}
		_, err = ctrl.storeVolumeUpdate(logger, volumeClone)
		if err != nil {
			return fmt.Errorf("persistent Volume Controller can't anneal migration finalizer: %v", err)
		}
		logger.V(2).Info("PV in-tree protection finalizer removed from volume", "volumeName", volume.Name)
	}
	return nil
}

// provisionClaim 在开启动态制备时，为声明启动一个异步的制备操作。
// 它先查找可用的制备插件（内部插件或外部制备器），再通过 scheduleOperation 异步执行；
// 查找插件失败时只发事件并返回 nil，由后续每次 syncUnboundClaim 重试。
func (ctrl *PersistentVolumeController) provisionClaim(ctx context.Context, claim *v1.PersistentVolumeClaim) error {
	if !ctrl.enableDynamicProvisioning {
		// 动态制备被禁用时直接跳过，不创建任何异步操作。
		return nil
	}
	logger := klog.FromContext(ctx)
	logger.V(4).Info("provisionClaim: started", "PVC", klog.KObj(claim))
	opName := fmt.Sprintf("provision-%s[%s]", claimToClaimKey(claim), string(claim.UID))
	plugin, storageClass, err := ctrl.findProvisionablePlugin(claim)
	// 对于外部制备器，findProvisionablePlugin 不会返回错误。
	if err != nil {
		// 外部制备器不返回错误；这里出错说明找不到请求的插件，保留原有返回 nil 的行为由后续同步重试。
		ctrl.eventRecorder.Event(claim, v1.EventTypeWarning, events.ProvisioningFailed, err.Error())
		logger.Error(err, "Error finding provisioning plugin for claim", "PVC", klog.KObj(claim))
		// 找不到请求的制备插件时，目前直接返回错误即可；控制器会在每次 syncUnboundClaim() 中重试制备。 保留 provisionClaim 调用原本返回 nil 的行为。
		return nil
	}
	ctrl.scheduleOperation(logger, opName, func() error {
		// 为制备操作记录起始时间戳（若尚无对应条目）：
		// 键为 claimKey，pluginName 为制备器名称，operation 为 "provision"。
		// 记录制备操作起始时间，供端到端延迟指标使用。
		claimKey := claimToClaimKey(claim)
		ctrl.operationTimestamps.AddIfNotExist(claimKey, ctrl.getProvisionerName(plugin, storageClass), "provision")
		var err error
		if plugin == nil {
			_, err = ctrl.provisionClaimOperationExternal(ctx, claim, storageClass)
			// plugin 为 nil 表示使用外部制备器，只需写注解等待外部组件，不执行内部制备。
		} else {
			_, err = ctrl.provisionClaimOperation(ctx, claim, plugin, storageClass)
		}
		// 若发生错误则记录错误计数指标；时间戳条目会一直留在缓存中，直到绑定成功才清理。
		if err != nil {
			metrics.RecordMetric(claimKey, &ctrl.operationTimestamps, err)
			// 出错时记录错误计数；时间戳条目保留到绑定成功才清理。
		}
		return err
	})
	return nil
}

// provisionClaimOperation 使用内部插件为声明制备卷。该方法运行在独立的 goroutine 中，已持有所有必要的锁。
// 它校验数据源与插件能力、制备底层存储、构造 PV 对象并写入 API server（带重试）；
// 若 PV 对象创建失败，会尽力清理已创建的底层存储，避免留下孤儿卷。
func (ctrl *PersistentVolumeController) provisionClaimOperation(
	ctx context.Context,
	claim *v1.PersistentVolumeClaim,
	plugin vol.ProvisionableVolumePlugin,
	storageClass *storage.StorageClass) (string, error) {
	claimClass := storagehelpers.GetPersistentVolumeClaimClass(claim)
	logger := klog.FromContext(ctx)
	logger.V(4).Info("provisionClaimOperation started", "PVC", klog.KObj(claim), "storageClassName", claimClass)

	// 由 provisionClaim() 调用，此时 plugin 必定不为 nil。
	// 注意：调用方已检查过 plugin/storageClass。
	// 调用方已确保 plugin 与 storageClass 有效，此处直接取用。
	pluginName := plugin.GetPluginName()
	if pluginName != "kubernetes.io/csi" && claim.Spec.DataSource != nil {
		// 只有 CSI 插件支持 DataSource，非 CSI 插件遇到数据源必须直接失败。
		// 只有 CSI 插件支持 DataSource，
		// 若声明带有数据源但插件非 CSI，则操作失败。
		strerr := fmt.Sprintf("plugin %q is not a CSI plugin. Only CSI plugin can provision a claim with a datasource", pluginName)
		logger.V(2).Info(strerr)
		ctrl.eventRecorder.Event(claim, v1.EventTypeWarning, events.ProvisioningFailed, strerr)
		return pluginName, errors.New(strerr)

	}
	provisionerName := storageClass.Provisioner
	logger.V(4).Info("provisionClaimOperation", "PVC", klog.KObj(claim), "pluginName", pluginName, "provisionerName", provisionerName)

	// 添加制备器注解，与外部制备器流程保持一致。
	// 写入制备器注解，与外部制备流程保持一致。
	newClaim, err := ctrl.setClaimProvisioner(ctx, claim, provisionerName)
	if err != nil {
		// 保存失败，控制器会在下一次同步时重试。
		logger.V(2).Info("Error saving claim", "PVC", klog.KObj(claim), "err", err)
		return pluginName, err
	}
	claim = newClaim

	// 内部制备流程。

	// 等待锁期间，上一次 provisionClaimOperation 可能刚完成；
	// PV 名称是确定性的，先检查它是否已被制备，
	// 以免重复创建。
	// 等待锁期间可能已有另一次制备完成；PV 名称是确定性的，先检查是否已存在以免重复制备。

	pvName := ctrl.getProvisionedVolumeNameForClaim(claim)
	volume, err := ctrl.kubeClient.CoreV1().PersistentVolumes().Get(ctx, pvName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		logger.V(3).Info("Error reading persistent volume", "PV", klog.KRef("", pvName), "err", err)
		return pluginName, err
	}
	if err == nil && volume != nil {
		// 卷已经被制备过了，无需处理。
		logger.V(4).Info("provisionClaimOperation: volume already exists, skipping", "PVC", klog.KObj(claim))
		return pluginName, err
	}

	// 提前构造指向声明的 ClaimRef，这样引用获取失败时
	// 就不会先创建出孤立的存储卷。
	// 提前构造 ClaimRef，这样引用获取失败时不会先创建出孤立存储。
	claimRef, err := ref.GetReference(scheme.Scheme, claim)
	if err != nil {
		logger.V(3).Info("Unexpected error getting claim reference", "err", err)
		return pluginName, err
	}

	options := vol.VolumeOptions{
		PersistentVolumeReclaimPolicy: *storageClass.ReclaimPolicy,
		MountOptions:                  storageClass.MountOptions,
		PVName:                        pvName,
		PVC:                           claim,
		Parameters:                    storageClass.Parameters,
	}

	// 插件不支持挂载选项时拒绝制备，因为这样创建出的 PV 无论如何都会被校验拒绝。
	if !plugin.SupportsMountOption() && len(options.MountOptions) > 0 {
		// 插件不支持挂载选项时提前失败，避免创建出会被 API 校验拒绝的 PV。
		strerr := fmt.Sprintf("Mount options are not supported by the provisioner but StorageClass %q has mount options %v", storageClass.Name, options.MountOptions)
		logger.V(2).Info("Mount options are not supported by the provisioner but claim's StorageClass has mount options", "PVC", klog.KObj(claim), "storageClassName", storageClass.Name, "options", options.MountOptions)
		ctrl.eventRecorder.Event(claim, v1.EventTypeWarning, events.ProvisioningFailed, strerr)
		return pluginName, fmt.Errorf("provisioner %q doesn't support mount options", plugin.GetPluginName())
	}

	// 制备该卷。
	provisioner, err := plugin.NewProvisioner(logger, options)
	if err != nil {
		strerr := fmt.Sprintf("Failed to create provisioner: %v", err)
		logger.V(2).Info("Failed to create provisioner for claim with StorageClass", "PVC", klog.KObj(claim), "storageClassName", storageClass.Name, "err", err)
		ctrl.eventRecorder.Event(claim, v1.EventTypeWarning, events.ProvisioningFailed, strerr)
		return pluginName, err
	}

	var selectedNode *v1.Node = nil
	if nodeName, ok := claim.Annotations[storagehelpers.AnnSelectedNode]; ok {
		// 存在 SelectedNode 注解说明是调度器触发的制备，需要把目标节点传给插件。
		selectedNode, err = ctrl.NodeLister.Get(nodeName)
		if err != nil {
			strerr := fmt.Sprintf("Failed to get target node: %v", err)
			logger.V(3).Info("Unexpected error getting target node for claim", "node", klog.KRef("", nodeName), "PVC", klog.KObj(claim), "err", err)
			ctrl.eventRecorder.Event(claim, v1.EventTypeWarning, events.ProvisioningFailed, strerr)
			return pluginName, err
		}
	}
	allowedTopologies := storageClass.AllowedTopologies

	opComplete := util.OperationCompleteHook(plugin.GetPluginName(), "volume_provision")
	volume, err = provisioner.Provision(selectedNode, allowedTopologies)
	opComplete(volumetypes.CompleteFuncParam{Err: &err})
	if err != nil {
		// 其他失败点与 VolumeScheduling 无关，
		// 只有底层制备真正失败才通知调度器重新选择节点，其他失败交给常规重试。
		// 交给控制器下次同步重试即可；只有在底层制备真正失败时
		// 才在这里调用 rescheduleProvisioning。
		ctrl.rescheduleProvisioning(ctx, claim)

		strerr := fmt.Sprintf("Failed to provision volume with StorageClass %q: %v", storageClass.Name, err)
		logger.V(2).Info("Failed to provision volume for claim with StorageClass", "PVC", klog.KObj(claim), "storageClassName", storageClass.Name, "err", err)
		ctrl.eventRecorder.Event(claim, v1.EventTypeWarning, events.ProvisioningFailed, strerr)
		return pluginName, err
	}

	logger.V(3).Info("Volume for claim created", "PVC", klog.KObj(claim), "volumeName", volume.Name)

	// 为该卷创建 Kubernetes PV 对象。
	if volume.Name == "" {
		volume.Name = pvName
	}
	// 把它绑定到声明。
	volume.Spec.ClaimRef = claimRef
	volume.Status.Phase = v1.VolumeBound
	volume.Spec.StorageClassName = claimClass

	// 添加 AnnBoundByController 注解（用于后续删除卷）。
	// 该注解用于后续判断卷能否由控制器解绑/删除。
	metav1.SetMetaDataAnnotation(&volume.ObjectMeta, storagehelpers.AnnBoundByController, "yes")
	metav1.SetMetaDataAnnotation(&volume.ObjectMeta, storagehelpers.AnnDynamicallyProvisioned, plugin.GetPluginName())

	if volume.Spec.PersistentVolumeReclaimPolicy == v1.PersistentVolumeReclaimDelete {
		// 仅 Delete 策略需要加 in-tree 删除保护 finalizer，防止 PV 被误删。
		// 仅在回收策略为 `Delete` 时添加 in-tree 保护 finalizer。
		volume.SetFinalizers([]string{storagehelpers.PVDeletionInTreeProtectionFinalizer})
	}

	// 多次尝试创建 PV 对象。
	for i := 0; i < ctrl.createProvisionedPVRetryCount; i++ {
		logger.V(4).Info("provisionClaimOperation: trying to save volume", "PVC", klog.KObj(claim), "volumeName", volume.Name)
		var newVol *v1.PersistentVolume
		if newVol, err = ctrl.kubeClient.CoreV1().PersistentVolumes().Create(ctx, volume, metav1.CreateOptions{}); err == nil || apierrors.IsAlreadyExists(err) {
			// AlreadyExists 也视为成功：确定性名称的 PV 已由并发操作创建，直接复用即可。
			// 保存成功。
			if err != nil {
				logger.V(3).Info("Volume for claim already exists, reusing", "PVC", klog.KObj(claim), "volumeName", volume.Name)
				err = nil
			} else {
				logger.V(3).Info("Volume for claim saved", "PVC", klog.KObj(claim), "volumeName", volume.Name)

				_, updateErr := ctrl.storeVolumeUpdate(logger, newVol)
				if updateErr != nil {
					// 很快会收到 volume added 事件，这不是严重错误。
					// 缓存更新失败不算严重，随后会收到 volume added 事件。
					logger.V(4).Info("provisionClaimOperation: cannot update internal cache", "volumeName", volume.Name, "err", updateErr)
				}
			}
			break
		}
		// 保存失败，稍后重试。
		logger.V(3).Info("Failed to save volume for claim", "PVC", klog.KObj(claim), "volumeName", volume.Name, "err", err)
		time.Sleep(ctrl.createProvisionedPVInterval)
	}

	if err != nil {
		// 保存失败。此时 Kubernetes 之外已经存在存储资产，而我们还没有对应的 PV 对象。 这里发出事件提示，并多次尝试删除该存储资产。
		strerr := fmt.Sprintf("Error creating provisioned PV object for claim %s: %v. Deleting the volume.", claimToClaimKey(claim), err)
		logger.V(3).Info(strerr)
		ctrl.eventRecorder.Event(claim, v1.EventTypeWarning, events.ProvisioningFailed, strerr)

		delPlugin, deleteErr := ctrl.findDeletablePlugin(volume)
		if deleteErr == nil {
			if delPlugin == nil {
				// 内部制备的卷必然有内部删除插件，走到这里说明代码路径异常。
				// 这是不可达代码：卷由内部插件制备，
				// 因此必定存在能删除它的内部插件。
				// （接上行。）
				logger.Error(nil, "Error finding internal deleter for volume plugin", "plugin", plugin.GetPluginName())
				return pluginName, nil
			}

			for i := 0; i < ctrl.createProvisionedPVRetryCount; i++ {
				deleteErr = ctrl.doDeleteVolume(ctx, delPlugin, volume)
				if deleteErr == nil {
					// 删除成功。
					logger.V(4).Info("provisionClaimOperation: cleaning volume succeeded", "PVC", klog.KObj(claim), "volumeName", volume.Name)
					break
				}
				// 删除失败，稍后重试。
				logger.V(3).Info("Failed to delete volume", "volumeName", volume.Name, "err", deleteErr)
				time.Sleep(ctrl.createProvisionedPVInterval)
			}
		}

		if deleteErr != nil {
			// 多次删除均失败，留下了孤儿卷，
			// 清理多次失败会留下孤儿卷，控制器无能为力，只能发事件提示人工删除。
			// 我们对此无能为力。
			strerr := fmt.Sprintf("Error cleaning provisioned volume for claim %s: %v. Please delete manually.", claimToClaimKey(claim), deleteErr)
			logger.V(2).Info(strerr)
			ctrl.eventRecorder.Event(claim, v1.EventTypeWarning, events.ProvisioningCleanupFailed, strerr)
		}
	} else {
		logger.V(2).Info("Volume provisioned for claim", "PVC", klog.KObj(claim), "volumeName", volume.Name)
		msg := fmt.Sprintf("Successfully provisioned volume %s using %s", volume.Name, plugin.GetPluginName())
		ctrl.eventRecorder.Event(claim, v1.EventTypeNormal, events.ProvisioningSucceeded, msg)
	}
	return pluginName, nil
}

// provisionClaimOperationExternal 触发外部制备器进行异步制备。
// 该方法运行在由 provisionClaim 调度的独立 goroutine 中：处理 CSI 迁移名称转换、
// 写入制备器注解作为启动信号，并发事件告知用户等待外部制备器创建卷。
func (ctrl *PersistentVolumeController) provisionClaimOperationExternal(
	ctx context.Context,
	claim *v1.PersistentVolumeClaim,
	storageClass *storage.StorageClass) (string, error) {
	claimClass := storagehelpers.GetPersistentVolumeClaimClass(claim)
	logger := klog.FromContext(ctx)
	logger.V(4).Info("provisionClaimOperationExternal started", "PVC", klog.KObj(claim), "storageClassName", claimClass)
	// 通过 setClaimProvisioner 把 provisionerName 设为外部制备器的名称。
	var err error
	provisionerName := storageClass.Provisioner
	if ctrl.csiMigratedPluginManager.IsMigrationEnabledForPlugin(storageClass.Provisioner) {
		// CSI 迁移场景下必须把 in-tree 制备器名称转换为 CSI 驱动名称。
		// 把制备器名称更新为迁移后的 CSI 插件名称。
		provisionerName, err = ctrl.translator.GetCSINameFromInTreeName(storageClass.Provisioner)
		if err != nil {
			strerr := fmt.Sprintf("error getting CSI name for In tree plugin %s: %v", storageClass.Provisioner, err)
			logger.V(2).Info(strerr)
			ctrl.eventRecorder.Event(claim, v1.EventTypeWarning, events.ProvisioningFailed, strerr)
			return provisionerName, err
		}
	}
	// 添加制备器注解，外部制备器据此知道何时开始。
	// 外部制备器通过监听该注解得知应该开始制备。
	newClaim, err := ctrl.setClaimProvisioner(ctx, claim, provisionerName)
	if err != nil {
		// 保存失败，控制器会在下一次同步时重试。
		strerr := fmt.Sprintf("Error saving claim: %v", err)
		logger.V(2).Info("Error saving claim", "PVC", klog.KObj(claim), "err", err)
		ctrl.eventRecorder.Event(claim, v1.EventTypeWarning, events.ProvisioningFailed, strerr)
		return provisionerName, err
	}
	claim = newClaim
	msg := fmt.Sprintf("Waiting for a volume to be created either by the external provisioner '%s' "+
		"or manually by the system administrator. If volume creation is delayed, please verify that "+
		"the provisioner is running and correctly registered.", provisionerName)
	// 已请求外部制备器来制备卷，发出事件后等待外部制备器完成。
	ctrl.eventRecorder.Event(claim, v1.EventTypeNormal, events.ExternalProvisioning, msg)
	logger.V(3).Info("provisionClaimOperationExternal provisioning claim", "PVC", klog.KObj(claim), "msg", msg)
	// 这里返回制备器名称，供指标上报使用。
	return provisionerName, nil
}

// rescheduleProvisioning 通过删除 AnnSelectedNode 注解通知调度器重新进行动态制备。
// 仅当该注解存在（即制备由调度器触发）时才处理；更新失败只记录日志。
func (ctrl *PersistentVolumeController) rescheduleProvisioning(ctx context.Context, claim *v1.PersistentVolumeClaim) {
	if _, ok := claim.Annotations[storagehelpers.AnnSelectedNode]; !ok {
		// 没有该注解说明制备不是调度器触发的，无需回讯。
		// 制备并非由调度器触发，跳过。
		return
	}

	// 方法参数中的声明可能指向 watcher 缓存，绝不能直接修改，必须先创建副本。
	newClaim := claim.DeepCopy()
	// 参数可能指向 informer 缓存，不能原地修改，必须先克隆。
	delete(newClaim.Annotations, storagehelpers.AnnSelectedNode)
	// 尝试更新 PVC 对象。
	logger := klog.FromContext(ctx)
	if _, err := ctrl.kubeClient.CoreV1().PersistentVolumeClaims(newClaim.Namespace).Update(ctx, newClaim, metav1.UpdateOptions{}); err != nil {
		logger.V(4).Info("Failed to delete annotation 'storagehelpers.AnnSelectedNode' for PersistentVolumeClaim", "PVC", klog.KObj(newClaim), "err", err)
		return
	}
	if _, err := ctrl.storeClaimUpdate(logger, newClaim); err != nil {
		// 很快会收到 claim updated 事件，这不是严重错误。
		// 缓存更新失败无关紧要，随后会收到 claim updated 事件。
		logger.V(4).Info("Updating PersistentVolumeClaim: cannot update internal cache", "PVC", klog.KObj(newClaim), "err", err)
	}
}

// getProvisionedVolumeNameForClaim 返回为声明制备的卷的 PV 名称。
// 使用 PVC 的 UID 保证名称唯一且可预测，便于并发制备时检测重复。
func (ctrl *PersistentVolumeController) getProvisionedVolumeNameForClaim(claim *v1.PersistentVolumeClaim) string {
	return "pvc-" + string(claim.UID)
}

// scheduleOperation 以指定名称调度一个异步操作，并保证同名操作不会重复运行。
// 若操作已在运行或因指数退避被推迟，只记录日志；测试钩子 preOperationHook 会在操作启动前被调用。
func (ctrl *PersistentVolumeController) scheduleOperation(logger klog.Logger, operationName string, operation func() error) {
	logger.V(4).Info("scheduleOperation", "operationName", operationName)

	// 通知测试代码：某个操作即将开始。
	if ctrl.preOperationHook != nil {
		// 测试用钩子，便于在异步操作启动前注入断言或同步点。
		ctrl.preOperationHook(operationName)
	}

	err := ctrl.runningOperations.Run(operationName, operation)
	if err != nil {
		switch {
		case goroutinemap.IsAlreadyExists(err):
			// 同名操作已在运行，直接跳过，避免并发操作互相干扰。
			logger.V(4).Info("Operation is already running, skipping", "operationName", operationName)
		case exponentialbackoff.IsExponentialBackoff(err):
			logger.V(4).Info("Operation postponed due to exponential backoff", "operationName", operationName)
		default:
			logger.Error(err, "Error scheduling operation", "operationName", operationName)
		}
	}
}

// newRecyclerEventRecorder 返回一个 RecycleEventRecorder，把回收 Pod 产生的所有事件
// 都转发到给定卷上。
func (ctrl *PersistentVolumeController) newRecyclerEventRecorder(volume *v1.PersistentVolume) recyclerclient.RecycleEventRecorder {
	return func(eventtype, message string) {
		ctrl.eventRecorder.Eventf(volume, eventtype, events.RecyclerPod, "Recycler pod: %s", message)
	}
}

// findProvisionablePlugin 为给定声明查找制备插件。
// 返回内部制备插件，或在使用外部制备器/CSI 迁移时返回 nil（同时返回 StorageClass）；找不到内部插件且名称非 kubernetes.io/ 前缀时视为外部制备器，不报错。
func (ctrl *PersistentVolumeController) findProvisionablePlugin(claim *v1.PersistentVolumeClaim) (vol.ProvisionableVolumePlugin, *storage.StorageClass, error) {
	// 调用到这里的 provisionClaim() 永远不会在 claimClass 为空时被调用，因此可以省去一些检查。
	claimClass := storagehelpers.GetPersistentVolumeClaimClass(claim)
	class, err := ctrl.classLister.Get(claimClass)
	if err != nil {
		return nil, nil, err
	}

	// 为该 StorageClass 查找插件。
	if ctrl.csiMigratedPluginManager.IsMigrationEnabledForPlugin(class.Provisioner) {
		// CSI 迁移场景，不依赖 in-tree 插件。
		// CSI 迁移场景下不依赖 in-tree 插件，交给外部 CSI 制备器。
		return nil, class, nil
	}
	plugin, err := ctrl.volumePluginMgr.FindProvisionablePluginByName(class.Provisioner)
	if err != nil {
		if !strings.HasPrefix(class.Provisioner, "kubernetes.io/") {
			// 请求的是外部制备器，不报错。
			// 非 kubernetes.io/ 前缀表示外部制备器，找不到对应内部插件是正常情况。
			return nil, class, nil
		}
		return nil, class, err
	}
	return plugin, class, nil
}

// findDeletablePlugin 为给定卷查找删除插件。
// 优先使用制备时的插件注解，其次处理 CSI 迁移/CSI 卷源（交给外部删除器），
// 最后按卷规格查找；找不到时返回错误。
func (ctrl *PersistentVolumeController) findDeletablePlugin(volume *v1.PersistentVolume) (vol.DeletableVolumePlugin, error) {
	// 查找插件，优先尝试找到当初制备该卷的同一个插件。
	var plugin vol.DeletableVolumePlugin
	if metav1.HasAnnotation(volume.ObjectMeta, storagehelpers.AnnDynamicallyProvisioned) {
		provisionPluginName := volume.Annotations[storagehelpers.AnnDynamicallyProvisioned]
		if provisionPluginName != "" {
			// 优先使用制备该卷的插件来删除，保证语义一致。
			plugin, err := ctrl.volumePluginMgr.FindDeletablePluginByName(provisionPluginName)
			if err != nil {
				if !strings.HasPrefix(provisionPluginName, "kubernetes.io/") {
					// 外部制备器名称找不到内部插件是正常情况，返回 nil 交给外部删除器。
					// 请求的是外部制备器，不报错。
					return nil, nil
				}
				return nil, err
			}
			return plugin, nil
		}
	}

	if metav1.HasAnnotation(volume.ObjectMeta, storagehelpers.AnnMigratedTo) {
		// CSI 迁移场景，不依赖 in-tree 插件。
		// 已迁移到 CSI 的卷不再依赖 in-tree 插件。
		return nil, nil
	}

	if volume.Spec.CSI != nil {
		// CSI 卷源场景，由外部制备器处理。
		// CSI 卷源必然由外部 CSI 驱动处理，控制器不做内部删除。
		return nil, nil
	}

	// 制备该卷的插件未找到，或该卷并非动态制备，尝试根据卷规格查找插件。
	spec := vol.NewSpecFromPersistentVolume(volume, false)
	plugin, err := ctrl.volumePluginMgr.FindDeletablePluginBySpec(spec)
	if err != nil {
		// 未找到删除器，发出事件并把卷标记为 Failed。
		return nil, fmt.Errorf("error getting deleter volume plugin for volume %q: %w", volume.Name, err)
	}
	return plugin, nil
}

// getProvisionerNameFromVolume 获取某个卷对应的制备器/删除器名称，用于指标上报。
// 优先用卷的删除插件名；外部制备器或 CSI 迁移场景则从 StorageClass 推导，
// 避免把任意注解值直接用作指标标签导致基数爆炸。失败时返回 "N/A"。
func (ctrl *PersistentVolumeController) getProvisionerNameFromVolume(volume *v1.PersistentVolume) string {
	plugin, err := ctrl.findDeletablePlugin(volume)
	if err != nil {
		return "N/A"
	}
	// 无法确定插件名时用 N/A，避免指标标签缺失或滥用卷上的任意注解值。
	if plugin != nil {
		// 非 CSI 迁移的 in-tree 插件，直接使用插件名。
		return plugin.GetPluginName()
	}
	// 执行到这里，说明要么使用了外部制备器，要么插件已迁移到 CSI。
	// 对于外部制备器（plugin == nil），不使用 AnnDynamicallyProvisioned 注解值，
	// 而改用 StorageClass 的 Provisioner 字段，以避免 local storage provisioner 给卷打上任意名称时造成指标标签基数爆炸。
	storageClass := storagehelpers.GetPersistentVolumeClass(volume)
	class, err := ctrl.classLister.Get(storageClass)
	if err != nil {
		return "N/A"
	}
	if ctrl.csiMigratedPluginManager.IsMigrationEnabledForPlugin(class.Provisioner) {
		provisionerName, err := ctrl.translator.GetCSINameFromInTreeName(class.Provisioner)
		if err != nil {
			return "N/A"
		}
		return provisionerName
	}
	return class.Provisioner
}

// getProvisionerName 根据插件和 StorageClass 获取用于时间戳指标记录的制备器名称。
// 内部插件直接返回插件名；外部制备器返回 StorageClass.Provisioner；
// CSI 迁移场景下返回转换后的 CSI 驱动名，失败时返回 "N/A"。
func (ctrl *PersistentVolumeController) getProvisionerName(plugin vol.ProvisionableVolumePlugin, storageClass *storage.StorageClass) string {
	// 非 CSI 迁移的 in-tree 插件，直接返回插件名称。
	if plugin != nil {
		return plugin.GetPluginName()
	}
	if ctrl.csiMigratedPluginManager.IsMigrationEnabledForPlugin(storageClass.Provisioner) {
		// 获取 in-tree StorageClass 制备器所迁移到的 CSI 插件名称。
		provisionerName, err := ctrl.translator.GetCSINameFromInTreeName(storageClass.Provisioner)
		if err != nil {
			return "N/A"
		}
		return provisionerName
	}
	return storageClass.Provisioner
}
