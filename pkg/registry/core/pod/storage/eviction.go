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
	// MaxDisruptedPodSize is the max size of PodDisruptionBudgetStatus.DisruptedPods. API server eviction
	// subresource handler will refuse to evict pods covered by the corresponding PDB
	// if the size of the map exceeds this value. It means a large number of
	// evictions have been approved by the API server but not noticed by the PDB controller yet.
	// This situation should self-correct because the PDB controller removes
	// entries from the map automatically after the PDB DeletionTimeout regardless.
	MaxDisruptedPodSize = 2000
)

// EvictionsRetry is the retry for a conflict where multiple clients
// are making changes to the same resource.
var EvictionsRetry = wait.Backoff{
	Steps:    20,
	Duration: 500 * time.Millisecond,
	Factor:   1.0,
	Jitter:   0.1,
}

func newEvictionStorage(store rest.StandardStorage, podDisruptionBudgetClient policyclient.PodDisruptionBudgetsGetter) *EvictionREST {
	return &EvictionREST{store: store, podDisruptionBudgetClient: podDisruptionBudgetClient}
}

// EvictionREST implements the REST endpoint for evicting pods from nodes
type EvictionREST struct {
	store                     rest.StandardStorage
	podDisruptionBudgetClient policyclient.PodDisruptionBudgetsGetter
}

var _ = rest.NamedCreater(&EvictionREST{})
var _ = rest.GroupVersionKindProvider(&EvictionREST{})
var _ = rest.GroupVersionAcceptor(&EvictionREST{})

var v1Eviction = schema.GroupVersionKind{Group: "policy", Version: "v1", Kind: "Eviction"}

// GroupVersionKind specifies a particular GroupVersionKind to discovery
func (r *EvictionREST) GroupVersionKind(containingGV schema.GroupVersion) schema.GroupVersionKind {
	return v1Eviction
}

// AcceptsGroupVersion indicates both v1 and v1beta1 Eviction objects are acceptable
func (r *EvictionREST) AcceptsGroupVersion(gv schema.GroupVersion) bool {
	switch gv {
	case policyv1.SchemeGroupVersion, policyv1beta1.SchemeGroupVersion:
		return true
	default:
		return false
	}
}

// New creates a new eviction resource
func (r *EvictionREST) New() runtime.Object {
	return &policy.Eviction{}
}

// Destroy cleans up resources on shutdown.
func (r *EvictionREST) Destroy() {
	// Given that underlying store is shared with REST,
	// we don't destroy it here explicitly.
}

// Propagate dry-run takes the dry-run option from the request and pushes it into the eviction object.
// It returns an error if they have non-matching dry-run options.
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

// Create attempts to create a new eviction.  That is, it tries to evict a pod.
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
		// if the original options included a resourceVersion precondition, don't retry
		shouldRetry = func(err error) bool { return false }
	}

	err = retry.OnError(EvictionsRetry, shouldRetry, func() error {
		pod, err = getPod(r, ctx, eviction.Name)
		if err != nil {
			return err
		}

		// Pending/终态/已在删除中的 Pod 不会再造成新的可用副本损失，可以跳过 PDB 扣减直接删除。
		// There is no need to check for pdb.
		if !canIgnorePDB(pod) {
			// Pod is not in a state where we can skip checking PDBs, exit the loop, and continue to PDB checks.
			return nil
		}

		// 确定可忽略 PDB 后，仍尽可能补充 ResourceVersion 前置条件，避免检查 Pod 后发生状态变化。
		deleteOptions := originalDeleteOptions

		// We should check if resourceVersion is already set by the requestor
		// as it might be older than the pod we just fetched and should be
		// honored.
		if shouldEnforceResourceVersion(pod) && resourceVersionIsUnset(originalDeleteOptions) {
			// Set deleteOptions.Preconditions.ResourceVersion to ensure we're not
			// racing with another PDB-impacting process elsewhere.
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
		// this can happen in cases where the PDB can be ignored, but there was a problem issuing the pod delete:
		// maybe we conflicted too many times or we didn't have permission or something else weird.
		return nil, err

	// 已在上面的快速路径成功删除，直接返回成功，不再查询 PodDisruptionBudget。
	case deletedPod:
		// this happens when we successfully deleted the pod.  In this case, we're done executing because we've evicted/deleted the pod
		return &metav1.Status{Status: metav1.StatusSuccess}, nil

	default:
		// this happens when we didn't have an error and we didn't delete the pod. The only branch that happens on is when
		// we cannot ignored the PDB for this pod, so this is the fall through case.
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

		// IsPodReady is the current implementation of IsHealthy
		// If the pod is healthy, it should be guarded by the PDB.
		// 对未 Ready 的 Pod 按 UnhealthyPodEvictionPolicy 决策；符合 AlwaysAllow 或健康预算有余量时不扣减 disruptionsAllowed。
		if !podutil.IsPodReady(pod) {
			if pdb.Spec.UnhealthyPodEvictionPolicy != nil && *pdb.Spec.UnhealthyPodEvictionPolicy == policyv1.AlwaysAllow {
				// Delete the unhealthy pod, it doesn't count towards currentHealthy and desiredHealthy and we should not decrement disruptionsAllowed.
				updateDeletionOptions = true
				return nil
			}
			// default nil and IfHealthyBudget policy
			if pdb.Status.CurrentHealthy >= pdb.Status.DesiredHealthy && pdb.Status.DesiredHealthy > 0 {
				// Delete the unhealthy pod, it doesn't count towards currentHealthy and desiredHealthy and we should not decrement disruptionsAllowed.
				// Application guarded by the PDB is not disrupted at the moment and deleting unhealthy (unready) pod will not disrupt it.
				updateDeletionOptions = true
				return nil
			}
			// confirm no disruptions allowed in checkAndDecrement
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
			// Try to verify-and-decrement

			// If it was false already, or if it becomes false during the course of our retries,
			// raise an error marked as a 429.
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
	// the pod was unhealthy (unready) and we have enough healthy replicas

	deleteOptions := originalDeleteOptions

	// Set deleteOptions.Preconditions.ResourceVersion to ensure
	// the pod hasn't been considered healthy (ready) since we calculated
	if updateDeletionOptions {
		// Take a copy so we can compare to client-provied Options later.
		deleteOptions = deleteOptions.DeepCopy()
		setPreconditionsResourceVersion(deleteOptions, &pod.ResourceVersion)
	}

	// 先前判断了 Ready 状态/预算，删除时带上读取时的 ResourceVersion，阻止检查和删除之间的竞态。
	err = addConditionAndDeletePod(r, ctx, eviction.Name, rest.ValidateAllObjectFunc, deleteOptions)
	if err != nil {
		if errors.IsConflict(err) && updateDeletionOptions &&
			(originalDeleteOptions.Preconditions == nil || originalDeleteOptions.Preconditions.ResourceVersion == nil) {
			// If we encounter a resource conflict error, we updated the deletion options to include them,
			// and the original deletion options did not specify ResourceVersion, we send back
			// TooManyRequests so clients will retry.
			return nil, createTooManyRequestsError(pdbName)
		}
		return nil, err
	}

	// Success!
	return &metav1.Status{Status: metav1.StatusSuccess}, nil
}

// addConditionAndDeletePod 先在最新 Pod 上写入 DisruptionTarget 条件，再执行 Delete，让 PDB 控制器及时排除即将删除的副本。
func addConditionAndDeletePod(r *EvictionREST, ctx context.Context, name string, validation rest.ValidateObjectFunc, options *metav1.DeleteOptions) error {
	if !dryrun.IsDryRun(options.DryRun) {
		// 更新前重新读取 etcd 最新对象，并检查 UID/ResourceVersion 前置条件，避免给已被替换的 Pod 标记驱逐。
		getLatestPod := func(_ context.Context, _, oldObj runtime.Object) (runtime.Object, error) {
			// Throwaway the newObj. We care only about the latest pod obtained from etcd (oldObj).
			// So we can add DisruptionTarget condition in conditionAppender without conflicts.
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

		podUpdatedObjectInfo := rest.DefaultUpdatedObjectInfo(nil, getLatestPod, conditionAppender) // order important

		updatedPodObject, _, err := r.store.Update(ctx, name, podUpdatedObjectInfo, rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{})
		if err != nil {
			return err
		}

		if !resourceVersionIsUnset(options) {
			newResourceVersion, err := meta.NewAccessor().ResourceVersion(updatedPodObject)
			if err != nil {
				return err
			}
			// bump the resource version, since we are the one who modified it via the update
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

// canIgnorePDB returns true for pod conditions that allow the pod to be deleted
// without checking PDBs.
func canIgnorePDB(pod *api.Pod) bool {
	if pod.Status.Phase == api.PodSucceeded || pod.Status.Phase == api.PodFailed ||
		pod.Status.Phase == api.PodPending || !pod.ObjectMeta.DeletionTimestamp.IsZero() {
		return true
	}
	return false
}

func shouldEnforceResourceVersion(pod *api.Pod) bool {
	// We don't need to enforce ResourceVersion for terminal pods
	if pod.Status.Phase == api.PodSucceeded || pod.Status.Phase == api.PodFailed || !pod.ObjectMeta.DeletionTimestamp.IsZero() {
		return false
	}
	// Return true for all other pods to ensure we don't race against a pod becoming
	// healthy (ready) and violating PDBs.
	return true
}

func resourceVersionIsUnset(options *metav1.DeleteOptions) bool {
	return options.Preconditions == nil || options.Preconditions.ResourceVersion == nil
}

func createTooManyRequestsError(name string) error {
	// TODO: Once there are time-based
	// budgets, we can sometimes compute a sensible suggested value.  But
	// even without that, we can give a suggestion (even if small) that
	// prevents well-behaved clients from hammering us.
	err := errors.NewTooManyRequests("Cannot evict pod as it would violate the pod's disruption budget.", 10)
	err.ErrStatus.Details.Causes = append(err.ErrStatus.Details.Causes, metav1.StatusCause{Type: policyv1.DisruptionBudgetCause, Message: fmt.Sprintf("The disruption budget %s is still being processed by the server.", name)})
	return err
}

// checkAndDecrement checks if the provided PodDisruptionBudget allows any disruption.
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
		// check whether sync is failed first because DesiredHealthy and CurrentHealthy are not trustworthy when the sync is failed
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

	// If this is a dry-run, we don't need to go any further than that.
	if dryRun {
		return nil
	}

	if pdb.Status.DisruptedPods == nil {
		pdb.Status.DisruptedPods = make(map[string]metav1.Time)
	}

	// 把即将驱逐的 Pod 写入 DisruptedPods，通知 PDB Controller 暂时不要把它计入可用副本。超时后控制器会清理未完成的驱逐记录。
	// so it should not consider it as available in calculations when updating PodDisruptions allowed.
	// If the pod is not deleted within a reasonable time limit PDB controller will assume that it won't
	// be deleted at all and remove it from DisruptedPod map.
	pdb.Status.DisruptedPods[podName] = metav1.Time{Time: time.Now()}
	if _, err := r.podDisruptionBudgetClient.PodDisruptionBudgets(namespace).UpdateStatus(context.TODO(), &pdb, metav1.UpdateOptions{}); err != nil {
		return err
	}

	return nil
}

// getPodDisruptionBudgets returns any PDBs that match the pod or err if there's an error.
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
			// This object has an invalid selector, it does not match the pod
			continue
		}
		if !selector.Matches(labels.Set(pod.Labels)) {
			continue
		}

		pdbs = append(pdbs, pdb)
	}

	return pdbs, nil
}
