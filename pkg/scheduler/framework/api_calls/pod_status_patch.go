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

package apicalls

import (
	"context"
	"fmt"
	"sync"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// PodStatusPatchCall 表示一次 Pod 状态补丁调用。
// 它在执行前可被 Sync 更新状态，执行后基于补丁前后的差异提交 Patch 请求。
type PodStatusPatchCall struct {
	lock sync.Mutex
	// executed 在 Execute 开始时置位，Sync 据此判断是否还需要刷新 podStatus；
	// 一旦执行开始，podStatus 就不再更新，避免与 Execute 中的读取竞争。
	executed bool

	// podUID 是目标 Pod 的 UID。
	podUID types.UID
	// podRef 是目标 Pod 的日志引用，用于日志输出和 Patch 时的名字/命名空间。
	podRef klog.ObjectRef
	// podStatus 保存执行时的 Pod 状态快照，作为生成补丁的基线。
	podStatus *v1.PodStatus
	// newConditions 是需要更新到 Pod 状态上的条件列表。
	newConditions []*v1.PodCondition
	// nominatingInfo 是需要更新的提名节点信息。
	nominatingInfo *fwk.NominatingInfo
}

// NewPodStatusPatchCall 基于 Pod 当前状态创建一个新的状态补丁调用。
// 状态在执行前可被后续 Sync 覆盖，因此这里保存的是深拷贝。
func NewPodStatusPatchCall(pod *v1.Pod, conditions []*v1.PodCondition, nominatingInfo *fwk.NominatingInfo) *PodStatusPatchCall {
	return &PodStatusPatchCall{
		podUID:         pod.UID,
		podRef:         klog.KObj(pod),
		podStatus:      pod.Status.DeepCopy(),
		newConditions:  conditions,
		nominatingInfo: nominatingInfo,
	}
}

// CallType 返回该调用的类型标识。
func (psuc *PodStatusPatchCall) CallType() fwk.APICallType {
	return PodStatusPatch
}

// UID 返回目标 Pod 的 UID，供 API 调用框架按对象聚合/去重使用。
func (psuc *PodStatusPatchCall) UID() types.UID {
	return psuc.podUID
}

// syncStatus 将 conditions 与 nominatingInfo 同步到给定的 status 上，
// 仅当确实产生变更时返回 true，避免提交无意义的补丁。
func syncStatus(status *v1.PodStatus, condition []*v1.PodCondition, nominatingInfo *fwk.NominatingInfo) bool {
	nnnNeedsUpdate := nominatingInfo.Mode() == fwk.ModeOverride && status.NominatedNodeName != nominatingInfo.NominatedNodeName
	anyUpdated := false
	for _, cond := range condition {
		updated := podutil.UpdatePodCondition(status, cond)
		if updated {
			anyUpdated = true
		}
	}
	if !anyUpdated && !nnnNeedsUpdate {
		return false
	}
	if nnnNeedsUpdate {
		status.NominatedNodeName = nominatingInfo.NominatedNodeName
	}
	return true
}

// Execute 计算补丁前后的状态差异并提交 Pod 状态补丁。
// 没有实际变更时直接返回，不发起 API 请求。
func (psuc *PodStatusPatchCall) Execute(ctx context.Context, client clientset.Interface) error {
	psuc.lock.Lock()
	// 先置位 executed，随后 Sync 就不会再写 podStatus，从而避免数据竞争。
	psuc.executed = true
	conditions := make([]*v1.PodCondition, 0, len(psuc.newConditions))
	for _, condition := range psuc.newConditions {
		conditions = append(conditions, condition.DeepCopy())
	}
	podStatusCopy := psuc.podStatus.DeepCopy()
	psuc.lock.Unlock()

	logger := klog.FromContext(ctx)
	for _, condition := range conditions {
		logger.V(3).Info("Updating pod condition", "pod", psuc.podRef, "conditionType", condition.Type, "conditionStatus", condition.Status, "conditionReason", condition.Reason)
	}

	// 在副本上应用 condition 与 nominatingInfo，得到补丁的最终状态。
	anySynced := syncStatus(podStatusCopy, conditions, psuc.nominatingInfo)
	if !anySynced {
		logger.V(5).Info("Pod status patch call does not need to be executed because it has no effect", "pod", psuc.podRef)
		return nil
	}

	// PatchPodStatus 基于 UID 和字段差异生成 patch，因此即便 Pod 对象已过时也安全。
	err := util.PatchPodStatus(ctx, client, psuc.podRef.Name, psuc.podRef.Namespace, psuc.podStatus, podStatusCopy)
	if err != nil {
		logger.Error(err, "Failed to patch pod status", "pod", psuc.podRef)
		return err
	}

	return nil
}

// Sync 在调用执行前用缓存中最新的 Pod 状态刷新快照，
// 并返回应用了待更新条件与提名信息后的 Pod 副本，供本地视图保持一致。
func (psuc *PodStatusPatchCall) Sync(obj metav1.Object) (metav1.Object, error) {
	pod, ok := obj.(*v1.Pod)
	if !ok {
		return obj, fmt.Errorf("unexpected error: object of type %T is not of type *v1.Pod", obj)
	}

	psuc.lock.Lock()
	if !psuc.executed {
		// 仅在尚未开始执行时更新 podStatus；执行开始后的状态已无意义，且会带来竞争。
		psuc.podStatus = pod.Status.DeepCopy()
	}
	newConditions := make([]*v1.PodCondition, 0, len(psuc.newConditions))
	for _, condition := range psuc.newConditions {
		newConditions = append(newConditions, condition.DeepCopy())
	}
	psuc.lock.Unlock()

	podCopy := pod.DeepCopy()
	// 在 Pod 副本上应用变更，使本地视图提前反映即将提交的补丁。
	anySynced := syncStatus(&podCopy.Status, newConditions, psuc.nominatingInfo)
	if !anySynced {
		return pod, nil
	}
	return podCopy, nil
}

// Merge 将旧的同类调用合并进当前调用：
// 保留旧的提名信息（当新调用为 no-op 时），并补全缺失的条件类型。
func (psuc *PodStatusPatchCall) Merge(oldCall fwk.APICall) error {
	oldPsuc, ok := oldCall.(*PodStatusPatchCall)
	if !ok {
		return fmt.Errorf("unexpected error: call of type %T is not of type *PodStatusPatchCall", oldCall)
	}
	if psuc.nominatingInfo.Mode() == fwk.ModeNoop && oldPsuc.nominatingInfo.Mode() == fwk.ModeOverride {
		// 新调用不覆盖提名节点时沿用旧调用的提名信息，避免丢失有效提名。
		psuc.nominatingInfo = oldPsuc.nominatingInfo
	}
	for _, cond := range oldPsuc.newConditions {
		found := false
		for _, newCond := range psuc.newConditions {
			if newCond.Type == cond.Type {
				found = true
				break
			}
		}
		if !found {
			psuc.newConditions = append(psuc.newConditions, cond)
		}
	}
	return nil
}

// conditionNeedsUpdate 判断给定条件与 Pod 当前状态中的同名条件是否不同，
// 以决定是否需要更新（不比较 LastTransitionTime，避免无意义更新）。
func conditionNeedsUpdate(status *v1.PodStatus, condition *v1.PodCondition) bool {
	// 先按类型查找已有的同名条件。
	_, oldCondition := podutil.GetPodCondition(status, condition.Type)
	if oldCondition == nil {
		return true
	}

	isEqual := condition.Status == oldCondition.Status &&
		condition.Reason == oldCondition.Reason &&
		condition.Message == oldCondition.Message &&
		condition.LastProbeTime.Equal(&oldCondition.LastProbeTime)

	// 任一关键字段发生变化都需要更新。
	return !isEqual
}

// IsNoOp 判断该调用是否不会产生任何状态变更，
// 供调用框架在入队前丢弃无意义的补丁请求。
func (psuc *PodStatusPatchCall) IsNoOp() bool {
	nnnNeedsUpdate := psuc.nominatingInfo.Mode() == fwk.ModeOverride && psuc.podStatus.NominatedNodeName != psuc.nominatingInfo.NominatedNodeName
	if nnnNeedsUpdate {
		return false
	}
	for _, condition := range psuc.newConditions {
		if conditionNeedsUpdate(psuc.podStatus, condition) {
			return false
		}
	}
	return true
}
