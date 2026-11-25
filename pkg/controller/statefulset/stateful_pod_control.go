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

package statefulset

import (
	"context"
	"fmt"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	errorutils "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	consistencyutil "k8s.io/kubernetes/pkg/controller/util/consistency"
)

// StatefulPodControlObjectManager 抽象了对 Pod 与 PVC 的操作。
// 真实控制器用 clientset 执行写操作、用 lister 执行读操作；测试中则提供桩实现。
type StatefulPodControlObjectManager interface {
	CreatePod(ctx context.Context, pod *v1.Pod, ss *apps.StatefulSet) error
	GetPod(namespace, podName string) (*v1.Pod, error)
	UpdatePod(pod *v1.Pod, ss *apps.StatefulSet) error
	DeletePod(pod *v1.Pod) error
	CreateClaim(claim *v1.PersistentVolumeClaim, ss *apps.StatefulSet) error
	GetClaim(namespace, claimName string) (*v1.PersistentVolumeClaim, error)
	UpdateClaim(claim *v1.PersistentVolumeClaim, ss *apps.StatefulSet) error
}

// StatefulPodControl 定义了 StatefulSetController 用来创建、更新、删除 Pod
// 以及更新 StatefulSet 状态的操作集合。它沿用 PodControl 的设计范式，
// 但实现上额外支持 PVC 创建、按序创建与终止 Pod，以及强制校验 Pod 身份。
// 对象的具体操作由 objectMgr 提供，从而在测试中可以 mock k8s API。
type StatefulPodControl struct {
	objectMgr StatefulPodControlObjectManager
	recorder  record.EventRecorder
}

// NewStatefulPodControl 使用给定的 clientset、lister 和 EventRecorder 构造一个
// 基于 realStatefulPodControlObjectManager 的 StatefulPodControl。
func NewStatefulPodControl(
	client clientset.Interface,
	podLister corelisters.PodLister,
	claimLister corelisters.PersistentVolumeClaimLister,
	recorder record.EventRecorder,
	consistencyStore consistencyutil.ConsistencyStore,
) *StatefulPodControl {
	return &StatefulPodControl{&realStatefulPodControlObjectManager{client, podLister, claimLister, consistencyStore}, recorder}
}

// NewStatefulPodControlFromManager 使用给定的 StatefulPodControlObjectManager 和事件记录器
// 创建 StatefulPodControl，便于测试中注入自定义的对象管理器。
func NewStatefulPodControlFromManager(om StatefulPodControlObjectManager, recorder record.EventRecorder) *StatefulPodControl {
	return &StatefulPodControl{om, recorder}
}

// realStatefulPodControlObjectManager 基于 clientset.Interface 执行写操作，基于 lister 执行读操作。
type realStatefulPodControlObjectManager struct {
	client           clientset.Interface
	podLister        corelisters.PodLister
	claimLister      corelisters.PersistentVolumeClaimLister
	consistencyStore consistencyutil.ConsistencyStore
}

// CreatePod 通过 clientset 创建 Pod，成功时把写入的资源版本记录到一致性存储，
// 以便后续检测缓存与 API 是否一致。
func (om *realStatefulPodControlObjectManager) CreatePod(ctx context.Context, pod *v1.Pod, ss *apps.StatefulSet) error {
	pod, err := om.client.CoreV1().Pods(pod.Namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err == nil {
		ssNamespacedName := types.NamespacedName{Namespace: ss.Namespace, Name: ss.Name}
		om.consistencyStore.WroteAt(
			ssNamespacedName,
			ss.UID,
			podGroupResource,
			pod.ResourceVersion,
		)
	}
	return err
}

// GetPod 从 Pod lister（本地缓存）中按命名空间和名称读取 Pod。
func (om *realStatefulPodControlObjectManager) GetPod(namespace, podName string) (*v1.Pod, error) {
	return om.podLister.Pods(namespace).Get(podName)
}

// UpdatePod 通过 clientset 更新 Pod，成功时同样记录写入的资源版本。
func (om *realStatefulPodControlObjectManager) UpdatePod(pod *v1.Pod, ss *apps.StatefulSet) error {
	pod, err := om.client.CoreV1().Pods(pod.Namespace).Update(context.TODO(), pod, metav1.UpdateOptions{})
	if err == nil {
		ssNamespacedName := types.NamespacedName{Namespace: ss.Namespace, Name: ss.Name}
		om.consistencyStore.WroteAt(
			ssNamespacedName,
			ss.UID,
			podGroupResource,
			pod.ResourceVersion,
		)
	}
	return err
}

// DeletePod 通过 clientset 删除 Pod；删除操作不写一致性存储。
func (om *realStatefulPodControlObjectManager) DeletePod(pod *v1.Pod) error {
	return om.client.CoreV1().Pods(pod.Namespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
}

// CreateClaim 通过 clientset 创建 PVC，成功时把写入的资源版本记录到一致性存储。
func (om *realStatefulPodControlObjectManager) CreateClaim(claim *v1.PersistentVolumeClaim, ss *apps.StatefulSet) error {
	claim, err := om.client.CoreV1().PersistentVolumeClaims(claim.Namespace).Create(context.TODO(), claim, metav1.CreateOptions{})
	if err == nil {
		ssNamespacedName := types.NamespacedName{Namespace: ss.Namespace, Name: ss.Name}
		om.consistencyStore.WroteAt(
			ssNamespacedName,
			ss.UID,
			persistentVolumeClaimGroupResource,
			claim.ResourceVersion,
		)
	}
	return err
}

// GetClaim 从 PVC lister（本地缓存）中按命名空间和名称读取 PVC。
func (om *realStatefulPodControlObjectManager) GetClaim(namespace, claimName string) (*v1.PersistentVolumeClaim, error) {
	return om.claimLister.PersistentVolumeClaims(namespace).Get(claimName)
}

// UpdateClaim 通过 clientset 更新 PVC，成功时记录写入的资源版本。
func (om *realStatefulPodControlObjectManager) UpdateClaim(claim *v1.PersistentVolumeClaim, ss *apps.StatefulSet) error {
	claim, err := om.client.CoreV1().PersistentVolumeClaims(claim.Namespace).Update(context.TODO(), claim, metav1.UpdateOptions{})
	if err == nil {
		ssNamespacedName := types.NamespacedName{Namespace: ss.Namespace, Name: ss.Name}
		om.consistencyStore.WroteAt(
			ssNamespacedName,
			ss.UID,
			persistentVolumeClaimGroupResource,
			claim.ResourceVersion,
		)
	}
	return err
}

// CreateStatefulPod 为 StatefulSet 创建 Pod：先创建其依赖的 PVC，再创建 Pod，
// 并尽可能设置 PVC 的保留策略；任何一步失败都会记录相应事件。
func (spc *StatefulPodControl) CreateStatefulPod(ctx context.Context, set *apps.StatefulSet, pod *v1.Pod) error {
	// Pod 的卷依赖必须先就绪，因此 PVC 必须在 Pod 之前创建。
	if err := spc.createPersistentVolumeClaims(set, pod); err != nil {
		spc.recordPodEvent("create", set, pod, err)
		return err
	}
	// PVC 就绪后尝试创建 Pod。
	err := spc.objectMgr.CreatePod(ctx, pod, set)
	// AlreadyExists 是幂等重试的正常结果，直接返回交给上层处理。
	if apierrors.IsAlreadyExists(err) {
		return err
	}
	// PVC 已就绪，此时尽快把保留策略写入，避免策略长时间滞后。
	if err := spc.UpdatePodClaimForRetentionPolicy(ctx, set, pod); err != nil {
		spc.recordPodEvent("update", set, pod, err)
		return err
	}
	spc.recordPodEvent("create", set, pod, err)
	return err
}

// UpdateStatefulPod 使 Pod 与 StatefulSet 的期望状态一致：修正身份、存储与 PVC 保留策略，
// 并在确有变更时提交更新；对冲突使用 RetryOnConflict 重试。
func (spc *StatefulPodControl) UpdateStatefulPod(ctx context.Context, set *apps.StatefulSet, pod *v1.Pod) error {
	attemptedUpdate := false
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		// 先假设 Pod 已一致，仅在发现差异时置为 false。
		consistent := true
		// 身份（名称、序号、标签等）不符时修正并标记为需要更新。
		if !identityMatches(set, pod) {
			updateIdentity(set, pod)
			consistent = false
		}
		// 存储声明不符时修正 Pod 的卷引用、标记需要更新，并补齐缺失的 PVC。
		if !storageMatches(set, pod) {
			updateStorage(set, pod)
			consistent = false
			if err := spc.createPersistentVolumeClaims(set, pod); err != nil {
				spc.recordPodEvent("update", set, pod, err)
				return err
			}
		}
		// PVC 的删除策略不一致时更新 PVC 属主引用，并标记 Pod 需要更新。
		if match, err := spc.ClaimsMatchRetentionPolicy(ctx, set, pod); err != nil {
			spc.recordPodEvent("update", set, pod, err)
			return err
		} else if !match {
			if err := spc.UpdatePodClaimForRetentionPolicy(ctx, set, pod); err != nil {
				spc.recordPodEvent("update", set, pod, err)
				return err
			}
			consistent = false
		}

		// 没有任何差异时无需写 API，直接返回以减少无谓更新。
		if consistent {
			return nil
		}

		attemptedUpdate = true
		// 提交更新；冲突时由外层 RetryOnConflict 重新读取最新 Pod 再试。

		updateErr := spc.objectMgr.UpdatePod(pod, set)
		if updateErr == nil {
			return nil
		}

		if updated, err := spc.objectMgr.GetPod(set.Namespace, pod.Name); err == nil {
			// 深拷贝后再修改，避免污染 lister 的共享缓存对象。
			pod = updated.DeepCopy()
		} else {
			utilruntime.HandleErrorWithContext(ctx, err, "Error getting updated Pod from lister", "pod", klog.KRef(set.Namespace, pod.Name))
		}

		return updateErr
	})
	if attemptedUpdate {
		spc.recordPodEvent("update", set, pod, err)
	}
	return err
}

// DeleteStatefulPod 删除 Pod。NotFound 视为成功，因为最终结果一致：Pod 已不存在。
func (spc *StatefulPodControl) DeleteStatefulPod(set *apps.StatefulSet, pod *v1.Pod) error {
	if err := spc.objectMgr.DeletePod(pod); err != nil {
		if !apierrors.IsNotFound(err) {
			spc.recordPodEvent("delete", set, pod, err)
			return err
		}
	}
	spc.recordPodEvent("delete", set, pod, nil)
	return nil
}

// ClaimsMatchRetentionPolicy 检查 Pod 的 PVC 是否与 StatefulSet 的 PVC 删除策略一致，
// 不一致时返回 false。读取 PVC 出错会返回 error；当 Pod 正被其他流程更新时这种错误可预期，
// 否则意味着异常（参见 UpdateStatefulPod 中对本方法的调用）。
func (spc *StatefulPodControl) ClaimsMatchRetentionPolicy(ctx context.Context, set *apps.StatefulSet, pod *v1.Pod) (bool, error) {
	logger := klog.FromContext(ctx)
	ordinal := getOrdinal(pod)
	templates := set.Spec.VolumeClaimTemplates
	for i := range templates {
		claimName := getPersistentVolumeClaimName(set, &templates[i], ordinal)
		claim, err := spc.objectMgr.GetClaim(set.Namespace, claimName)
		switch {
		case apierrors.IsNotFound(err):
			klog.FromContext(ctx).V(4).Info("Expected claim missing, continuing to pick up in next iteration", "PVC", klog.KObj(claim))
		case err != nil:
			return false, fmt.Errorf(
				"Could not retrieve claim %s for %s when checking PVC deletion policy: %w", claimName, pod.Name, err)
		default:
			if !isClaimOwnerUpToDate(logger, claim, set, pod) {
				return false, nil
			}
		}
	}
	return true, nil
}

// UpdatePodClaimForRetentionPolicy 更新 Pod 使用的 PVC 的属主引用，使其符合 StatefulSet 的 PVC 保留策略。
func (spc *StatefulPodControl) UpdatePodClaimForRetentionPolicy(ctx context.Context, set *apps.StatefulSet, pod *v1.Pod) error {
	logger := klog.FromContext(ctx)
	ordinal := getOrdinal(pod)
	templates := set.Spec.VolumeClaimTemplates
	for i := range templates {
		claimName := getPersistentVolumeClaimName(set, &templates[i], ordinal)
		claim, err := spc.objectMgr.GetClaim(set.Namespace, claimName)
		switch {
		case apierrors.IsNotFound(err):
			logger.V(4).Info("Expected claim missing, continuing to pick up in next iteration", "PVC", klog.KObj(claim))
		case err != nil:
			return fmt.Errorf("Could not retrieve claim %s not found for %s when checking PVC deletion policy: %w", claimName, pod.Name, err)
		default:
			if hasUnexpectedController(claim, set, pod) {
				// 发出事件告知用户配置异常；该 PVC 的属主引用会在下面被修正。
				msg := fmt.Sprintf("PersistentVolumeClaim %s has a conflicting OwnerReference that acts as a manging controller, the retention policy is ignored for this claim", claimName)
				spc.recorder.Event(set, v1.EventTypeWarning, "ConflictingController", msg)
			}
			if !isClaimOwnerUpToDate(logger, claim, set, pod) {
				claim = claim.DeepCopy() // 深拷贝以免修改 lister 共享缓存中的对象。
				updateClaimOwnerRefForSetAndPod(logger, claim, set, pod)
				if err := spc.objectMgr.UpdateClaim(claim, set); err != nil {
					return fmt.Errorf("could not update claim %s for delete policy ownerRefs: %w", claimName, err)
				}
			}
		}
	}
	return nil
}

// PodClaimIsStale 判断是否存在应阻止 Pod 创建的过期 PVC。
// 当缩容策略为删除时，若 PVC 的 ownerRef 与 Pod 不匹配则视为过期，
// 这也包括 Pod 尚未分配 UID 的情形。
func (spc *StatefulPodControl) PodClaimIsStale(set *apps.StatefulSet, pod *v1.Pod) (bool, error) {
	policy := getPersistentVolumeClaimRetentionPolicy(set)
	if policy.WhenScaled == apps.RetainPersistentVolumeClaimRetentionPolicyType {
		// 保留策略下 PVC 会被复用，因此不存在“过期”一说。
		return false, nil
	}
	for _, claim := range getPersistentVolumeClaims(set, pod) {
		pvc, err := spc.objectMgr.GetClaim(claim.Namespace, claim.Name)
		switch {
		case apierrors.IsNotFound(err):
			// PVC 尚未创建时自然谈不上过期。
			continue
		case err != nil:
			return false, err
		case err == nil:
			if hasStaleOwnerRef(pvc, pod, podKind) {
				return true, nil
			}
		}
	}
	return false, nil
}

// recordPodEvent 记录对 StatefulSet 中某 Pod 执行某操作的结果事件。
// err 为 nil 时生成 Normal 事件，否则生成 Warning 事件。
func (spc *StatefulPodControl) recordPodEvent(verb string, set *apps.StatefulSet, pod *v1.Pod, err error) {
	if err == nil {
		reason := fmt.Sprintf("Successful%s", cases.Title(language.English).String(verb))
		message := fmt.Sprintf("%s Pod %s in StatefulSet %s successful",
			cases.Title(language.English).String(verb), pod.Name, set.Name)
		spc.recorder.Event(set, v1.EventTypeNormal, reason, message)
	} else {
		reason := fmt.Sprintf("Failed%s", cases.Title(language.English).String(verb))
		message := fmt.Sprintf("%s Pod %s in StatefulSet %s failed error: %s",
			cases.Title(language.English).String(verb), pod.Name, set.Name, err)
		spc.recorder.Event(set, v1.EventTypeWarning, reason, message)
	}
}

// recordClaimEvent 记录对 StatefulSet 中某 Pod 的 PVC 执行某操作的结果事件。
// err 为 nil 时生成 Normal 事件，否则生成 Warning 事件。
func (spc *StatefulPodControl) recordClaimEvent(verb string, set *apps.StatefulSet, pod *v1.Pod, claim *v1.PersistentVolumeClaim, err error) {
	if err == nil {
		reason := fmt.Sprintf("Successful%s", cases.Title(language.English).String(verb))
		message := fmt.Sprintf("%s Claim %s Pod %s in StatefulSet %s success",
			cases.Title(language.English).String(verb), claim.Name, pod.Name, set.Name)
		spc.recorder.Event(set, v1.EventTypeNormal, reason, message)
	} else {
		reason := fmt.Sprintf("Failed%s", cases.Title(language.English).String(verb))
		message := fmt.Sprintf("%s Claim %s for Pod %s in StatefulSet %s failed error: %s",
			cases.Title(language.English).String(verb), claim.Name, pod.Name, set.Name, err)
		spc.recorder.Event(set, v1.EventTypeWarning, reason, message)
	}
}

// createMissingPersistentVolumeClaims 为 Pod 创建所有必需的 PVC，并同步更新其保留策略。
func (spc *StatefulPodControl) createMissingPersistentVolumeClaims(ctx context.Context, set *apps.StatefulSet, pod *v1.Pod) error {
	if err := spc.createPersistentVolumeClaims(set, pod); err != nil {
		return err
	}
	// PVC 创建完成后立即设置保留策略，避免策略长时间滞后。
	if err := spc.UpdatePodClaimForRetentionPolicy(ctx, set, pod); err != nil {
		spc.recordPodEvent("update", set, pod, err)
		return err
	}
	return nil
}

// createPersistentVolumeClaims 为属于 set 的 pod 创建所有必需的 PVC。
// 全部创建成功时返回 nil；失败时可重复调用直到无错误，
// 表示 Pod 的 PVC 已与 set 的 Spec 一致。
func (spc *StatefulPodControl) createPersistentVolumeClaims(set *apps.StatefulSet, pod *v1.Pod) error {
	var errs []error
	for _, claim := range getPersistentVolumeClaims(set, pod) {
		pvc, err := spc.objectMgr.GetClaim(claim.Namespace, claim.Name)
		switch {
		case apierrors.IsNotFound(err):
			err := spc.objectMgr.CreateClaim(&claim, set)
			if err != nil {
				errs = append(errs, fmt.Errorf("failed to create PVC %s: %w", claim.Name, err))
			}
			if err == nil || !apierrors.IsAlreadyExists(err) {
				spc.recordClaimEvent("create", set, pod, &claim, err)
			}
		case err != nil:
			errs = append(errs, fmt.Errorf("failed to retrieve PVC %s: %w", claim.Name, err))
			spc.recordClaimEvent("create", set, pod, &claim, err)
		default:
			if pvc.DeletionTimestamp != nil {
				errs = append(errs, fmt.Errorf("pvc %s is being deleted", claim.Name))
			}
		}
		// TODO: 检查资源需求与访问模式，必要时更新 PVC。
	}
	return errorutils.NewAggregate(errs)
}
