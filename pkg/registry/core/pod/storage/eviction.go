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

package storage

import (
	"context"
	goerrors "errors"
	"fmt"
	"reflect"
	"time"

	policyv1 "k8s.io/api/policy/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/util/dryrun"
	policyclient "k8s.io/client-go/kubernetes/typed/policy/v1"
	"k8s.io/client-go/util/retry"
	pdbhelper "k8s.io/component-helpers/apps/poddisruptionbudget"
	podutil "k8s.io/kubernetes/pkg/api/pod"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/policy"

	"k8s.io/utils/ptr"
)

const (
	// MaxDisruptedPodSize 限制 PodDisruptionBudgetStatus.DisruptedPods 映射的最大条目数。
	// 如果超过该值，API Server 的 eviction 子资源会拒绝驱逐受对应 PDB 保护的 Pod。
	// 这通常意味着 API Server 已批准大量驱逐，但 PDB Controller 尚未及时观测到这些驱逐。
	// 该情况通常会自行恢复：无论驱逐是否完成，PDB Controller 都会在 DeletionTimeout 到期后自动清理映射中的记录。
	MaxDisruptedPodSize = 2000
)

// EvictionsRetry 指多个客户端并发修改同一资源时，遇到冲突后的重试次数。
var EvictionsRetry = wait.Backoff{
	Steps:    20,
	Duration: 500 * time.Millisecond,
	Factor:   1.0,
	Jitter:   0.1,
}

func newEvictionStorage(store rest.StandardStorage, podDisruptionBudgetClient policyclient.PodDisruptionBudgetsGetter) *EvictionREST {
	return &EvictionREST{store: store, podDisruptionBudgetClient: podDisruptionBudgetClient}
}

// EvictionREST 实现从 Node 驱逐 Pod 的 REST 子资源端点。
type EvictionREST struct {
	store                     rest.StandardStorage
	podDisruptionBudgetClient policyclient.PodDisruptionBudgetsGetter
}

var _ = rest.NamedCreater(&EvictionREST{})
var _ = rest.GroupVersionKindProvider(&EvictionREST{})
var _ = rest.GroupVersionAcceptor(&EvictionREST{})

var v1Eviction = schema.GroupVersionKind{Group: "policy", Version: "v1", Kind: "Eviction"}

// GroupVersionKind 返回 Eviction 资源用于 API Discovery 的 GroupVersionKind。
func (r *EvictionREST) GroupVersionKind(containingGV schema.GroupVersion) schema.GroupVersionKind {
	return v1Eviction
}

// AcceptsGroupVersion 表示 v1 和 v1beta1 两个版本的 Eviction 对象均可被该端点接受。
func (r *EvictionREST) AcceptsGroupVersion(gv schema.GroupVersion) bool {
	switch gv {
	case policyv1.SchemeGroupVersion, policyv1beta1.SchemeGroupVersion:
		return true
	default:
		return false
	}
}

// New 返回一个空的 Eviction 对象。
func (r *EvictionREST) New() runtime.Object {
	return &policy.Eviction{}
}

// Destroy 在 API Server 关闭时清理该 REST 子资源拥有的资源。
func (r *EvictionREST) Destroy() {
	// 底层 Store 与 Pod 主资源 REST 共享，因此不能在此处重复销毁。
}

// propagateDryRun 将请求 CreateOptions 中的 dry-run 设置传递到 Eviction.DeleteOptions。
// 如果两处都指定了 dry-run 且取值不一致，则返回错误。
// propagateDryRun 合并请求头中的 CreateOptions 与 Eviction.DeleteOptions；两处同时指定时必须完全一致。
func propagateDryRun(eviction *policy.Eviction, options *metav1.CreateOptions) (*metav1.DeleteOptions, error) {
	if eviction.DeleteOptions == nil {
		return &metav1.DeleteOptions{DryRun: options.DryRun}, nil
	}
	if len(eviction.DeleteOptions.DryRun) == 0 {
		eviction.DeleteOptions.DryRun = options.DryRun
		return eviction.DeleteOptions, nil
	}
	if len(options.DryRun) == 0 {
		return eviction.DeleteOptions, nil
	}

	if !reflect.DeepEqual(options.DryRun, eviction.DeleteOptions.DryRun) {
		return nil, fmt.Errorf("Non-matching dry-run options in request and content: %v and %v", options.DryRun, eviction.DeleteOptions.DryRun)
	}
	return eviction.DeleteOptions, nil
}

// Create 处理 Eviction 请求，执行 Pod 驱逐流程。
// Create 实现 Eviction API：读取 Pod、判断是否可跳过 PDB、扣减 disruptionsAllowed，最后带前置条件删除 Pod。
func (r *EvictionREST) Create(ctx context.Context, name string, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	eviction, ok := obj.(*policy.Eviction)
	if !ok {
		return nil, errors.NewBadRequest(fmt.Sprintf("not a Eviction object: %T", obj))
	}

	if name != eviction.Name {
		return nil, errors.NewBadRequest("name in URL does not match name in Eviction object")
	}

	if eviction.DeleteOptions != nil && ptr.Deref(eviction.DeleteOptions.IgnoreStoreReadErrorWithClusterBreakingPotential, false) {
		errs := field.ErrorList{
			field.Invalid(field.NewPath("deleteOptions").Child("ignoreStoreReadErrorWithClusterBreakingPotential"), true, "can not be set for pod eviction, try after removing the option"),
		}
		return nil, errors.NewInvalid(v1Eviction.GroupKind(), name, errs)
	}

	originalDeleteOptions, err := propagateDryRun(eviction, options)
	if err != nil {
		return nil, err
	}

	if createValidation != nil {
		if err := createValidation(ctx, eviction.DeepCopyObject()); err != nil {
			return nil, err
		}
	}

	var pod *api.Pod
	deletedPod := false
	// PDB 允许的驱逐次数可能被并发请求修改，因此默认仅对资源冲突重试；用户给出 ResourceVersion 时必须尊重其前置条件。
	shouldRetry := errors.IsConflict
	if !resourceVersionIsUnset(originalDeleteOptions) {
		// 如果原始请求显式指定了 ResourceVersion 前置条件，则不重试，必须严格遵守客户端条件。
		shouldRetry = func(err error) bool { return false }
	}

	err = retry.OnError(EvictionsRetry, shouldRetry, func() error {
		pod, err = getPod(r, ctx, eviction.Name)
		if err != nil {
			return err
		}

		// Pending/终态/已在删除中的 Pod 不会再造成新的可用副本损失，可以跳过 PDB 扣减直接删除。
		// 当前 Pod 状态允许跳过 PDB 检查，无需查询 PodDisruptionBudget。
		if !canIgnorePDB(pod) {
			// Pod 当前状态不满足跳过 PDB 检查的条件，退出快速路径，继续执行完整预算检查。
			return nil
		}

		// 确定可忽略 PDB 后，仍尽可能补充 ResourceVersion 前置条件，避免检查 Pod 后发生状态变化。
		deleteOptions := originalDeleteOptions

		// 检查请求方是否已经指定 ResourceVersion；该版本可能早于刚读取到的 Pod，必须尊重客户端提供的前置条件。
		if shouldEnforceResourceVersion(pod) && resourceVersionIsUnset(originalDeleteOptions) {
			// 设置 deleteOptions.Preconditions.ResourceVersion，避免与其他影响 PDB 预算的并发操作发生竞态。
			deleteOptions = deleteOptions.DeepCopy()
			setPreconditionsResourceVersion(deleteOptions, &pod.ResourceVersion)
		}
		err = addConditionAndDeletePod(r, ctx, eviction.Name, rest.ValidateAllObjectFunc, deleteOptions)
		if err != nil {
			return err
		}
		deletedPod = true
		return nil
	})
	switch {
	case err != nil:
		// 即使该 Pod 可以跳过 PDB 检查，删除操作本身仍可能失败，例如冲突重试次数耗尽、权限不足或其他异常。
		return nil, err

	// 已在上面的快速路径成功删除，直接返回成功，不再查询 PodDisruptionBudget。
	case deletedPod:
		// 快速路径已成功删除 Pod，驱逐流程结束。
		return &metav1.Status{Status: metav1.StatusSuccess}, nil

	default:
		// 没有发生错误但 Pod 尚未删除，说明不能忽略该 Pod 对应的 PDB，需要继续执行预算检查。
	}

	var rtStatus *metav1.Status
	var pdbName string
	updateDeletionOptions := false

	err = func() error {
		// 查找所有匹配该 Pod 的 PDB；当前 Eviction 语义不支持一个 Pod 同时被多个 PDB 管理。
		pdbs, err := r.getPodDisruptionBudgets(ctx, pod)
		if err != nil {
			return err
		}

		if len(pdbs) > 1 {
			rtStatus = &metav1.Status{
				Status:  metav1.StatusFailure,
				Message: "This pod has more than one PodDisruptionBudget, which the eviction subresource does not support.",
				Code:    500,
			}
			return nil
		}
		if len(pdbs) == 0 {
			return nil
		}

		pdb := &pdbs[0]
		pdbName = pdb.Name

		// 当前以 IsPodReady 作为 IsHealthy 的实现。
		// Pod 已 Ready 时属于健康副本，应受 PDB 预算保护。
		// 对未 Ready 的 Pod 按 UnhealthyPodEvictionPolicy 决策；符合 AlwaysAllow 或健康预算有余量时不扣减 disruptionsAllowed。
		if !podutil.IsPodReady(pod) {
			if pdb.Spec.UnhealthyPodEvictionPolicy != nil && *pdb.Spec.UnhealthyPodEvictionPolicy == policyv1.AlwaysAllow {
				// 删除不健康 Pod：它不计入 currentHealthy 和 desiredHealthy，因此不应递减 disruptionsAllowed。
				updateDeletionOptions = true
				return nil
			}
			// 未指定策略时默认按 IfHealthyBudget 处理。
			if pdb.Status.CurrentHealthy >= pdb.Status.DesiredHealthy && pdb.Status.DesiredHealthy > 0 {
				// 删除不健康的 Pod：它不计入 currentHealthy 与 desiredHealthy，因此不应再扣减 disruptionsAllowed，
				// PDB 保护的应用当前没有受到实际中断，删除未 Ready 的 Pod 不会进一步影响应用可用性。
				updateDeletionOptions = true
				return nil
			}
			// 在 checkAndDecrement 中确认当前没有可用的驱逐预算。
		}

		// PDB Status 更新可能冲突；发生冲突后重新读取最新 PDB，再重新判断并扣减预算。
		refresh := false
		err = retry.RetryOnConflict(EvictionsRetry, func() error {
			if refresh {
				pdb, err = r.podDisruptionBudgetClient.PodDisruptionBudgets(pod.Namespace).Get(context.TODO(), pdbName, metav1.GetOptions{})
				if err != nil {
					return err
				}
			}
			// 尝试原子地校验并递减驱逐预算。

			// 如果预算原本就不可用，或在重试过程中变为不可用，则返回标记为 HTTP 429 的错误，提示客户端稍后重试。
			// 原子更新 PDB 的 Status：确认预算可用后递减数量，并记录正在驱逐的 Pod。
			if err = r.checkAndDecrement(pod.Namespace, pod.Name, *pdb, dryrun.IsDryRun(originalDeleteOptions.DryRun)); err != nil {
				refresh = true
				return err
			}
			return nil
		})
		return err
	}()
	if wait.Interrupted(err) {
		err = errors.NewTimeoutError(fmt.Sprintf("couldn't update PodDisruptionBudget %q due to conflicts", pdbName), 10)
	}
	if err != nil {
		return nil, err
	}

	if rtStatus != nil {
		return rtStatus, nil
	}

	// 到这里表示没有匹配 PDB、已成功扣减预算，或按不健康 Pod 策略允许删除；接下来才真正删除 Pod。
	// Pod 不健康（未 Ready），且当前健康副本数量足够，允许继续删除。

	deleteOptions := originalDeleteOptions

	// 设置删除请求的 ResourceVersion 前置条件，确保从计算预算到实际删除期间，Pod 没有变为健康（Ready）。
	if updateDeletionOptions {
		// 保存原始客户端选项副本，以便稍后比较 ResourceVersion 是否由客户端显式提供。
		deleteOptions = deleteOptions.DeepCopy()
		setPreconditionsResourceVersion(deleteOptions, &pod.ResourceVersion)
	}

	// 先前判断了 Ready 状态/预算，删除时带上读取时的 ResourceVersion，阻止检查和删除之间的竞态。
	err = addConditionAndDeletePod(r, ctx, eviction.Name, rest.ValidateAllObjectFunc, deleteOptions)
	if err != nil {
		if errors.IsConflict(err) && updateDeletionOptions &&
			(originalDeleteOptions.Preconditions == nil || originalDeleteOptions.Preconditions.ResourceVersion == nil) {
			// 如果发生资源冲突，且本次请求是服务端补充了 ResourceVersion（客户端原本未指定），
			// 则返回 TooManyRequests，促使客户端重新读取状态后重试。
			return nil, createTooManyRequestsError(pdbName)
		}
		return nil, err
	}

	// 驱逐成功。
	return &metav1.Status{Status: metav1.StatusSuccess}, nil
}

// addConditionAndDeletePod 先在最新 Pod 上写入 DisruptionTarget 条件，再执行 Delete，让 PDB 控制器及时排除即将删除的副本。
func addConditionAndDeletePod(r *EvictionREST, ctx context.Context, name string, validation rest.ValidateObjectFunc, options *metav1.DeleteOptions) error {
	if !dryrun.IsDryRun(options.DryRun) {
		// 更新前重新读取 etcd 最新对象，并检查 UID/ResourceVersion 前置条件，避免给已被替换的 Pod 标记驱逐。
		getLatestPod := func(_ context.Context, _, oldObj runtime.Object) (runtime.Object, error) {
			// 丢弃 newObj；这里只关心从 etcd 读取到的最新 Pod（oldObj），以便 conditionAppender 能基于最新对象追加 DisruptionTarget，避免冲突。
			latestPod := oldObj.(*api.Pod).DeepCopy()
			if options.Preconditions != nil {
				if uid := options.Preconditions.UID; uid != nil && len(*uid) > 0 && *uid != latestPod.UID {
					return nil, errors.NewConflict(
						schema.GroupResource{Group: "", Resource: "Pod"},
						latestPod.Name,
						fmt.Errorf("the UID in the precondition (%s) does not match the UID in record (%s). The object might have been deleted and then recreated", *uid, latestPod.UID),
					)
				}
				if rv := options.Preconditions.ResourceVersion; rv != nil && len(*rv) > 0 && *rv != latestPod.ResourceVersion {
					return nil, errors.NewConflict(
						schema.GroupResource{Group: "", Resource: "Pod"},
						latestPod.Name,
						fmt.Errorf("the ResourceVersion in the precondition (%s) does not match the ResourceVersion in record (%s). The object might have been modified", *rv, latestPod.ResourceVersion),
					)
				}
			}
			return latestPod, nil
		}

		// 在同一次更新链路中追加 DisruptionTarget，避免 PDB Controller 在 Pod 删除前仍将其视为健康副本。
		conditionAppender := func(_ context.Context, newObj, _ runtime.Object) (runtime.Object, error) {
			podObj := newObj.(*api.Pod)
			podutil.UpdatePodCondition(&podObj.Status, &api.PodCondition{
				Type:    api.DisruptionTarget,
				Status:  api.ConditionTrue,
				Reason:  "EvictionByEvictionAPI",
				Message: "Eviction API: evicting",
			})
			return podObj, nil
		}

		podUpdatedObjectInfo := rest.DefaultUpdatedObjectInfo(nil, getLatestPod, conditionAppender) // 顺序很重要

		updatedPodObject, _, err := r.store.Update(ctx, name, podUpdatedObjectInfo, rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{})
		if err != nil {
			return err
		}

		if !resourceVersionIsUnset(options) {
			newResourceVersion, err := meta.NewAccessor().ResourceVersion(updatedPodObject)
			if err != nil {
				return err
			}
			// 当前更新修改了 Pod，因此递增 ResourceVersion。
			options = options.DeepCopy()
			options.Preconditions.ResourceVersion = &newResourceVersion
		}
	}
	// 更新条件成功后再删除；如果 dry-run 则跳过上述状态写入，只执行存储层的 dry-run 删除。
	_, _, err := r.store.Delete(ctx, name, rest.ValidateAllObjectFunc, options)
	return err
}

func getPod(r *EvictionREST, ctx context.Context, name string) (*api.Pod, error) {
	obj, err := r.store.Get(ctx, name, &metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return obj.(*api.Pod), nil
}

func setPreconditionsResourceVersion(deleteOptions *metav1.DeleteOptions, resourceVersion *string) {
	if deleteOptions.Preconditions == nil {
		deleteOptions.Preconditions = &metav1.Preconditions{}
	}
	deleteOptions.Preconditions.ResourceVersion = resourceVersion
}

// canIgnorePDB 判断 Pod 当前状态是否允许跳过 PDB 检查并直接删除。
func canIgnorePDB(pod *api.Pod) bool {
	if pod.Status.Phase == api.PodSucceeded || pod.Status.Phase == api.PodFailed ||
		pod.Status.Phase == api.PodPending || !pod.ObjectMeta.DeletionTimestamp.IsZero() {
		return true
	}
	return false
}

func shouldEnforceResourceVersion(pod *api.Pod) bool {
	// 对于已经处于终态的 Pod，无需强制校验 ResourceVersion。
	if pod.Status.Phase == api.PodSucceeded || pod.Status.Phase == api.PodFailed || !pod.ObjectMeta.DeletionTimestamp.IsZero() {
		return false
	}
	// 其他状态的 Pod 必须执行 ResourceVersion 前置条件检查，防止 Pod 在检查期间变为 Ready，从而绕过 PDB 保护。
	return true
}

func resourceVersionIsUnset(options *metav1.DeleteOptions) bool {
	return options.Preconditions == nil || options.Preconditions.ResourceVersion == nil
}

func createTooManyRequestsError(name string) error {
	// TODO：未来支持基于时间的预算后，可以计算更合理的建议重试时间。
	// 即使尚无此能力，也应返回一个建议值（哪怕较短），避免遵守退避策略的客户端持续冲击 API Server。
	err := errors.NewTooManyRequests("Cannot evict pod as it would violate the pod's disruption budget.", 10)
	err.ErrStatus.Details.Causes = append(err.ErrStatus.Details.Causes, metav1.StatusCause{Type: policyv1.DisruptionBudgetCause, Message: fmt.Sprintf("The disruption budget %s is still being processed by the server.", name)})
	return err
}

// checkAndDecrement 检查指定 PodDisruptionBudget 是否允许本次驱逐，并在允许时原子递减预算。
// checkAndDecrement 校验 PDB 状态是否已观测到最新 Generation、预算是否为正，再扣减预算并登记 DisruptedPods。
func (r *EvictionREST) checkAndDecrement(namespace string, podName string, pdb policyv1.PodDisruptionBudget, dryRun bool) error {
	// Controller 尚未处理当前 PDB Spec 时，Status 不能作为可信预算，要求客户端稍后重试。
	if pdb.Status.ObservedGeneration < pdb.Generation {

		return createTooManyRequestsError(pdb.Name)
	}
	if pdb.Status.DisruptionsAllowed < 0 {
		err := errors.NewForbidden(policy.Resource("poddisruptionbudget"), pdb.Name, goerrors.New("pdb disruptions allowed is negative"))
		err.ErrStatus.Details.Causes = append(err.ErrStatus.Details.Causes, metav1.StatusCause{
			Type:    policyv1.DisruptionBudgetCause,
			Message: fmt.Sprintf("The disruption budget %s does not allow evicting pods currently: pdb disruptions allowed is negative", pdb.Name),
		})
		return err
	}
	if len(pdb.Status.DisruptedPods) > MaxDisruptedPodSize {
		err := errors.NewForbidden(policy.Resource("poddisruptionbudget"), pdb.Name, goerrors.New("DisruptedPods map too big - too many evictions not confirmed by PDB controller"))
		err.ErrStatus.Details.Causes = append(err.ErrStatus.Details.Causes, metav1.StatusCause{
			Type:    policyv1.DisruptionBudgetCause,
			Message: fmt.Sprintf("The disruption budget %s does not allow evicting pods currently: too many pending evictions not confirmed by PDB controller", pdb.Name),
		})
		return err
	}
	// 无可用预算时构造 429，并优先说明同步失败、健康副本不足或 PDB 当前不可用的具体原因。
	if pdb.Status.DisruptionsAllowed == 0 {
		err := errors.NewTooManyRequests("Cannot evict pod as it would violate the pod's disruption budget.", 0)
		condition := meta.FindStatusCondition(pdb.Status.Conditions, policyv1.DisruptionAllowedCondition)
		var msg string
		switch {
		// 优先检查同步是否失败：同步失败时 DesiredHealthy 和 CurrentHealthy 都不可信，不能据此判断预算。
		case condition != nil && condition.Status == metav1.ConditionFalse && len(condition.Message) > 0 && condition.Reason == policyv1.SyncFailedReason:
			msg = fmt.Sprintf("The disruption budget %s does not allow evicting pods currently because it failed sync: %s", pdb.Name, condition.Message)
		case pdb.Status.CurrentHealthy <= pdb.Status.DesiredHealthy:
			msg = fmt.Sprintf("The disruption budget %s needs %d healthy pods and has %d currently", pdb.Name, pdb.Status.DesiredHealthy, pdb.Status.CurrentHealthy)
		case condition != nil && condition.Status == metav1.ConditionFalse && len(condition.Message) > 0:
			msg = fmt.Sprintf("The disruption budget %s does not allow evicting pods currently (%s): %s", pdb.Name, condition.Reason, condition.Message)
		default:
			msg = fmt.Sprintf("The disruption budget %s does not allow evicting pods currently", pdb.Name)
		}
		err.ErrStatus.Details.Causes = append(
			err.ErrStatus.Details.Causes,
			metav1.StatusCause{
				Type:    policyv1.DisruptionBudgetCause,
				Message: msg,
			},
		)
		return err
	}

	// 先在本地对象中扣减预算；dry-run 仅验证扣减是否允许，不把修改写回 API Server。
	pdb.Status.DisruptionsAllowed--
	if pdb.Status.DisruptionsAllowed == 0 {
		pdbhelper.UpdateDisruptionAllowedCondition(&pdb)
	}

	// 如果是 dry-run，完成本地预算校验后即可返回，无需继续写入 API Server。
	if dryRun {
		return nil
	}

	if pdb.Status.DisruptedPods == nil {
		pdb.Status.DisruptedPods = make(map[string]metav1.Time)
	}

	// 把即将驱逐的 Pod 写入 DisruptedPods，通知 PDB Controller 暂时不要把它计入可用副本。超时后控制器会清理未完成的驱逐记录。
	// PDB Controller 更新 disruptionsAllowed 时，不应再将该 Pod 计为可用副本。
	// 如果 Pod 在合理时间内仍未删除，PDB Controller 会认为驱逐未实际发生，并从 DisruptedPods 映射中移除该记录。
	pdb.Status.DisruptedPods[podName] = metav1.Time{Time: time.Now()}
	if _, err := r.podDisruptionBudgetClient.PodDisruptionBudgets(namespace).UpdateStatus(context.TODO(), &pdb, metav1.UpdateOptions{}); err != nil {
		return err
	}

	return nil
}

// getPodDisruptionBudgets 查找与指定 Pod 匹配的所有 PDB；查询失败时返回错误。
// getPodDisruptionBudgets 列出 Pod 所在命名空间的 PDB，并按 LabelSelector 过滤出实际匹配该 Pod 的策略。
func (r *EvictionREST) getPodDisruptionBudgets(ctx context.Context, pod *api.Pod) ([]policyv1.PodDisruptionBudget, error) {
	pdbList, err := r.podDisruptionBudgetClient.PodDisruptionBudgets(pod.Namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	var pdbs []policyv1.PodDisruptionBudget
	for _, pdb := range pdbList.Items {
		if pdb.Namespace != pod.Namespace {
			continue
		}
		selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
		if err != nil {
			// 该 PDB 的 Selector 无效，因此不会匹配当前 Pod。
			continue
		}
		if !selector.Matches(labels.Set(pod.Labels)) {
			continue
		}

		pdbs = append(pdbs, pdb)
	}

	return pdbs, nil
}
