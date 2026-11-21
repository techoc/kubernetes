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

// 如果修改本文件，也应同步修改 ReplicationController 中对应的实现。
// 二者共享相似的 status 计算与 condition 维护逻辑，保持同步可避免行为漂移。

package replicaset

import (
	"context"
	"fmt"
	"reflect"
	"time"

	"k8s.io/klog/v2"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	appsclient "k8s.io/client-go/kubernetes/typed/apps/v1"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
)

// updateReplicaSetStatus 尝试更新给定 ReplicaSet 的 Status.Replicas 等状态字段，
// 冲突时最多重试 statusUpdateRetries 次，每次重试前重新 GET 以获取最新的 resourceVersion。
// 哪些状态字段参与写入由 controllerFeatures/feature gate 控制。
func updateReplicaSetStatus(logger klog.Logger, c appsclient.ReplicaSetInterface, rs *apps.ReplicaSet, newStatus apps.ReplicaSetStatus, controllerFeatures ReplicaSetControllerFeatures) (*apps.ReplicaSet, error) {
	// 稳定态：由于每 30s 会周期性 relist，即使没有任何期望变化也会来到这里。
	// 若 generation 不同但副本数相同，说明调用方可能把副本数改成了与当前相同的值，
	// 此时仍需继续向下写入 ObservedGeneration，因此判断条件中包含了 generation。
	if rs.Status.Replicas == newStatus.Replicas &&
		rs.Status.FullyLabeledReplicas == newStatus.FullyLabeledReplicas &&
		rs.Status.ReadyReplicas == newStatus.ReadyReplicas &&
		rs.Status.AvailableReplicas == newStatus.AvailableReplicas &&
		ptr.Equal(rs.Status.TerminatingReplicas, newStatus.TerminatingReplicas) &&
		rs.Generation == rs.Status.ObservedGeneration &&
		reflect.DeepEqual(rs.Status.Conditions, newStatus.Conditions) {
		return rs, nil
	}

	// 记录本次处理所依据的 generation，否则重试时可能错误地声称已观察到某次 spec 更新。
	// TODO: 如果允许多个 agent 写同一 status，这里可能覆盖其他更新。
	newStatus.ObservedGeneration = rs.Generation

	var getErr, updateErr error
	var updatedRS *apps.ReplicaSet
	for i, rs := 0, rs; ; i++ {
		terminatingReplicasUpdateInfo := ""
		if utilfeature.DefaultFeatureGate.Enabled(features.DeploymentReplicaSetTerminatingReplicas) && controllerFeatures.EnableStatusTerminatingReplicas {
			terminatingReplicasUpdateInfo = fmt.Sprintf("terminatingReplicas %s->%s, ", derefInt32ToStr(rs.Status.TerminatingReplicas), derefInt32ToStr(newStatus.TerminatingReplicas))
		}
		logger.V(4).Info(fmt.Sprintf("Updating status for %v: %s/%s, ", rs.Kind, rs.Namespace, rs.Name) +
			fmt.Sprintf("replicas %d->%d (need %d), ", rs.Status.Replicas, newStatus.Replicas, *(rs.Spec.Replicas)) +
			fmt.Sprintf("fullyLabeledReplicas %d->%d, ", rs.Status.FullyLabeledReplicas, newStatus.FullyLabeledReplicas) +
			fmt.Sprintf("readyReplicas %d->%d, ", rs.Status.ReadyReplicas, newStatus.ReadyReplicas) +
			fmt.Sprintf("availableReplicas %d->%d, ", rs.Status.AvailableReplicas, newStatus.AvailableReplicas) +
			terminatingReplicasUpdateInfo +
			fmt.Sprintf("sequence No: %v->%v", rs.Status.ObservedGeneration, newStatus.ObservedGeneration))

		rs.Status = newStatus
		updatedRS, updateErr = c.UpdateStatus(context.TODO(), rs, metav1.UpdateOptions{})
		if updateErr == nil {
			return updatedRS, nil
		}
		// 超过 statusUpdateRetries 后停止重试；ReplicaSet 会被限速重新入队，
		// 下次协调时再继续尝试，避免在本轮死循环。
		if i >= statusUpdateRetries {
			break
		}
		// 重新 GET 最新的 resourceVersion，供下一次 UpdateStatus 使用。
		if rs, getErr = c.Get(context.TODO(), rs.Name, metav1.GetOptions{}); getErr != nil {
			// GET 失败意味着我们已无法信任本地缓存的 status.Replicas；
			// 该错误比单纯的更新冲突更值得上报，因此直接返回它。
			return nil, getErr
		}
	}

	return nil, updateErr
}

// calculateStatus 根据当前 active/terminating Pod 列表和协调错误计算 ReplicaSet 的新 status。
// 同时维护 ReplicaFailure condition：管理副本出错且当前无该 condition 时新增，
// 错误消失且存在该 condition 时移除。terminatingReplicas 仅在 feature gate 与控制器配置均开启时统计。
func calculateStatus(rs *apps.ReplicaSet, activePods []*v1.Pod, terminatingPods []*v1.Pod, manageReplicasErr error, controllerFeatures ReplicaSetControllerFeatures, now time.Time) apps.ReplicaSetStatus {
	newStatus := rs.Status
	// 统计标签与 pod template 匹配的 Pod 数量；匹配 Pod 可能含有 template 之外的额外标签。
	// 由于 podTemplateSpec 的标签是 ReplicaSet selector 的超集，候选 Pod 必然已在 activePods 中，
	// 因此这里使用 template 标签做精确匹配来统计 fullyLabeledReplicas。
	fullyLabeledReplicasCount := 0
	readyReplicasCount := 0
	availableReplicasCount := 0
	templateLabel := labels.Set(rs.Spec.Template.Labels).AsSelectorPreValidated()
	for _, pod := range activePods {
		if templateLabel.Matches(labels.Set(pod.Labels)) {
			fullyLabeledReplicasCount++
		}
		if podutil.IsPodReady(pod) {
			readyReplicasCount++
			if podutil.IsPodAvailable(pod, rs.Spec.MinReadySeconds, metav1.Time{Time: now}) {
				availableReplicasCount++
			}
		}
	}

	var terminatingReplicasCount *int32
	if utilfeature.DefaultFeatureGate.Enabled(features.DeploymentReplicaSetTerminatingReplicas) && controllerFeatures.EnableStatusTerminatingReplicas {
		terminatingReplicasCount = ptr.To(int32(len(terminatingPods)))
	}

	failureCond := GetCondition(rs.Status, apps.ReplicaSetReplicaFailure)
	if manageReplicasErr != nil && failureCond == nil {
		// 根据副本缺口方向区分原因：少于期望说明创建失败，多于期望说明删除失败。
		var reason string
		if diff := len(activePods) - int(*(rs.Spec.Replicas)); diff < 0 {
			reason = "FailedCreate"
		} else if diff > 0 {
			reason = "FailedDelete"
		}
		cond := NewReplicaSetCondition(apps.ReplicaSetReplicaFailure, v1.ConditionTrue, reason, manageReplicasErr.Error())
		SetCondition(&newStatus, cond)
	} else if manageReplicasErr == nil && failureCond != nil {
		RemoveCondition(&newStatus, apps.ReplicaSetReplicaFailure)
	}

	newStatus.Replicas = int32(len(activePods))
	newStatus.FullyLabeledReplicas = int32(fullyLabeledReplicasCount)
	newStatus.ReadyReplicas = int32(readyReplicasCount)
	newStatus.AvailableReplicas = int32(availableReplicasCount)
	newStatus.TerminatingReplicas = terminatingReplicasCount
	return newStatus
}

// NewReplicaSetCondition 构造一个新的 ReplicaSet condition，转换时间取当前时间。
func NewReplicaSetCondition(condType apps.ReplicaSetConditionType, status v1.ConditionStatus, reason, msg string) apps.ReplicaSetCondition {
	return apps.ReplicaSetCondition{
		Type:               condType,
		Status:             status,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            msg,
	}
}

// GetCondition 返回指定类型的 ReplicaSet condition；不存在时返回 nil。
func GetCondition(status apps.ReplicaSetStatus, condType apps.ReplicaSetConditionType) *apps.ReplicaSetCondition {
	for _, c := range status.Conditions {
		if c.Type == condType {
			return &c
		}
	}
	return nil
}

// SetCondition 在 ReplicaSet status 中添加或替换给定的 condition。
// 若同类型 condition 已存在且 status 和 reason 均未变化，则直接返回，避免无谓写入导致转换时间被刷新。
func SetCondition(status *apps.ReplicaSetStatus, condition apps.ReplicaSetCondition) {
	currentCond := GetCondition(*status, condition.Type)
	if currentCond != nil && currentCond.Status == condition.Status && currentCond.Reason == condition.Reason {
		return
	}
	newConditions := filterOutCondition(status.Conditions, condition.Type)
	status.Conditions = append(newConditions, condition)
}

// RemoveCondition 从 ReplicaSet status 中移除指定类型的 condition。
func RemoveCondition(status *apps.ReplicaSetStatus, condType apps.ReplicaSetConditionType) {
	status.Conditions = filterOutCondition(status.Conditions, condType)
}

// filterOutCondition 返回一个新切片，剔除所有指定类型的 condition，保留其余项及其顺序。
func filterOutCondition(conditions []apps.ReplicaSetCondition, condType apps.ReplicaSetConditionType) []apps.ReplicaSetCondition {
	var newConditions []apps.ReplicaSetCondition
	for _, c := range conditions {
		if c.Type == condType {
			continue
		}
		newConditions = append(newConditions, c)
	}
	return newConditions
}

// derefInt32ToStr 将 *int32 转为字符串；nil 指针返回 "nil"，便于日志中区分“未设置”与 0。
func derefInt32ToStr(ptr *int32) string {
	if ptr == nil {
		return "nil"
	}
	return fmt.Sprintf("%d", *ptr)
}
