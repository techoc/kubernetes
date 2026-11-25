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

package deployment

import (
	"context"
	"fmt"
	"sort"

	apps "k8s.io/api/apps/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller"
	deploymentutil "k8s.io/kubernetes/pkg/controller/deployment/util"
)

// rolloutRolling 执行 RollingUpdate 策略下新 ReplicaSet 的滚动发布逻辑。
// 顺序固定为：先扩容新 RS，再缩容旧 RS，最后清理已完成的历史 RS 并同步状态；
// 每完成一步就返回，确保状态能及时反映中间进度。
func (dc *DeploymentController) rolloutRolling(ctx context.Context, d *apps.Deployment, rsList []*apps.ReplicaSet) error {
	newRS, oldRSs, err := dc.getAllReplicaSetsAndSyncRevision(ctx, d, rsList, true)
	if err != nil {
		return err
	}
	allRSs := append(oldRSs, newRS)

	// 先扩容新 RS：只有新版本有了可用副本，缩容旧版本才不会降低可用性。
	scaledUp, err := dc.reconcileNewReplicaSet(ctx, allRSs, newRS, d)
	if err != nil {
		return err
	}
	if scaledUp {
		// 更新 DeploymentStatus
		return dc.syncRolloutStatus(ctx, allRSs, newRS, d)
	}

	// 新 RS 已尽可能扩容后再缩容旧 RS，避免在发布中途扩大不可用副本数。
	scaledDown, err := dc.reconcileOldReplicaSets(ctx, allRSs, controller.FilterActiveReplicaSets(oldRSs), newRS, d)
	if err != nil {
		return err
	}
	if scaledDown {
		// 更新 DeploymentStatus
		return dc.syncRolloutStatus(ctx, allRSs, newRS, d)
	}

	if deploymentutil.DeploymentComplete(d, &d.Status) {
		if err := dc.cleanupDeployment(ctx, oldRSs, d); err != nil {
			return err
		}
	}

	// 同步 Deployment 状态
	return dc.syncRolloutStatus(ctx, allRSs, newRS, d)
}

// reconcileNewReplicaSet 调整新 ReplicaSet 的副本数以匹配 Deployment 期望值。
// 若新 RS 副本已超过期望值（例如回滚场景）则直接缩容；否则按 maxSurge/maxUnavailable 计算本次可扩容数量。
func (dc *DeploymentController) reconcileNewReplicaSet(ctx context.Context, allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet, deployment *apps.Deployment) (bool, error) {
	if *(newRS.Spec.Replicas) == *(deployment.Spec.Replicas) {
		// 无需调整副本数。
		return false, nil
	}
	if *(newRS.Spec.Replicas) > *(deployment.Spec.Replicas) {
		// 缩容。
		scaled, _, err := dc.scaleReplicaSet(ctx, newRS, *(deployment.Spec.Replicas), deployment, false)
		return scaled, err
	}
	newReplicasCount, err := deploymentutil.NewRSNewReplicas(deployment, allRSs, newRS)
	if err != nil {
		return false, err
	}
	scaled, _, err := dc.scaleReplicaSet(ctx, newRS, newReplicasCount, deployment, false)
	return scaled, err
}

// reconcileOldReplicaSets 判断并计算旧 ReplicaSet 的缩容数量。
// 缩容额度必须扣除新 RS 中不可用的副本数，否则新版本故障时会进一步削减旧版本、放大不可用范围。
func (dc *DeploymentController) reconcileOldReplicaSets(ctx context.Context, allRSs []*apps.ReplicaSet, oldRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet, deployment *apps.Deployment) (bool, error) {
	logger := klog.FromContext(ctx)
	oldPodsCount := deploymentutil.GetReplicaCountForReplicaSets(oldRSs)
	if oldPodsCount == 0 {
		// 旧 RS 已无副本，无法继续缩容。
		return false, nil
	}
	allPodsCount := deploymentutil.GetReplicaCountForReplicaSets(allRSs)
	logger.V(4).Info("New replica set", "replicaSet", klog.KObj(newRS), "availableReplicas", newRS.Status.AvailableReplicas)
	maxUnavailable := deploymentutil.MaxUnavailable(*deployment)

	// 判断是否可以缩容。以下两种情况可以缩容：
	// * 部分旧 ReplicaSet 存在不健康副本；删除这些副本不会进一步扩大不可用范围，因此可以安全缩容。
	// * 新 ReplicaSet 已扩容且副本就绪后，可以在后续步骤中缩容旧 ReplicaSet。
	//
	// maxScaledDown := allPodsCount - minAvailable - newReplicaSetPodsUnavailable
	// 计算时不仅要考虑 maxUnavailable 和已创建的突发副本，还要扣除新 RS 中不可用的副本，
	// 避免下一步因新 RS 的不可用副本而继续缩容旧 ReplicaSet，导致不可用范围扩大。
	//
	// 具体示例：
	//
	// * 10 个副本
	// * maxUnavailable 为 2（绝对数量，而非百分比）
	// * maxSurge 为 3（绝对数量，而非百分比）
	//
	// 情况 1：
	// * 更新 Deployment 后，创建有 3 个副本的 newRS，将 oldRS 缩容至 8 个副本，并将 newRS 扩容至 5 个副本。
	// * 新 ReplicaSet 的 Pod 持续崩溃重启，始终不可用。
	// * allPodsCount 为 13，minAvailable 为 8，newRSPodsUnavailable 为 5。
	// * 一个节点故障导致 oldRS 的一个 Pod 不可用。但 13 - 8 - 5 = 0，因此不会缩容 oldRS。
	// * 用户发现崩溃重启后执行 kubectl rollout undo 回滚。
	// * 已回滚到正常的 ReplicaSet，因此 newRSPodsUnavailable 为 1，maxScaledDown = 13 - 8 - 1 = 4。将缩容 4 个崩溃重启的 Pod。
	// * Pod 总数随后变为 9，newRS 可以扩容至 10 个副本。
	//
	// 情况 2：
	// * 沿用上述示例，但不回滚，而是推送新的 Pod 模板（即“滚动更新”）：
	// * 由于 allPodsCount 已达到 13，新建的 ReplicaSet 必须从 0 个副本开始。
	// * 但 newRSPodsUnavailable 也为 0，因此两个旧 ReplicaSet 可以缩容 5 个副本（13 - 8 - 0），
	//   随后新 ReplicaSet 可以扩容 5 个副本。
	minAvailable := *(deployment.Spec.Replicas) - maxUnavailable
	newRSUnavailablePodCount := *(newRS.Spec.Replicas) - newRS.Status.AvailableReplicas
	maxScaledDown := allPodsCount - minAvailable - newRSUnavailablePodCount
	if maxScaledDown <= 0 {
		return false, nil
	}

	// 优先清理不健康副本：它们会一直占用额度导致发布无法推进直至超时。
	// 见 https://github.com/kubernetes/kubernetes/issues/16737
	oldRSs, cleanupCount, err := dc.cleanupUnhealthyReplicas(ctx, oldRSs, deployment, maxScaledDown)
	if err != nil {
		return false, nil
	}
	logger.V(4).Info("Cleaned up unhealthy replicas from old RSes", "count", cleanupCount)

	// 缩容旧 ReplicaSet 前检查 maxUnavailable，确保仍有缩容空间。
	allRSs = append(oldRSs, newRS)
	scaledDownCount, err := dc.scaleDownOldReplicaSetsForRollingUpdate(ctx, allRSs, oldRSs, deployment)
	if err != nil {
		return false, nil
	}
	logger.V(4).Info("Scaled down old RSes", "deployment", klog.KObj(deployment), "count", scaledDownCount)

	totalScaledDown := cleanupCount + scaledDownCount
	return totalScaledDown > 0, nil
}

// cleanupUnhealthyReplicas 缩容包含不健康副本的旧 ReplicaSet，直到所有不健康副本都被删除。
// 按创建时间排序后先处理最老的 RS，以保证清理行为可预测且与历史保留策略一致。
func (dc *DeploymentController) cleanupUnhealthyReplicas(ctx context.Context, oldRSs []*apps.ReplicaSet, deployment *apps.Deployment, maxCleanupCount int32) ([]*apps.ReplicaSet, int32, error) {
	logger := klog.FromContext(ctx)
	sort.Sort(controller.ReplicaSetsByCreationTimestamp(oldRSs))
	// ReplicaSet 删除 Pod 时会优先删除未就绪的 Pod，因此先清理不健康副本不会进一步降低可用性。
	totalScaledDown := int32(0)
	for i, targetRS := range oldRSs {
		if totalScaledDown >= maxCleanupCount {
			break
		}
		if *(targetRS.Spec.Replicas) == 0 {
			// 该 ReplicaSet 已无副本，无法继续缩容。
			continue
		}
		logger.V(4).Info("Found available pods in old RS", "replicaSet", klog.KObj(targetRS), "availableReplicas", targetRS.Status.AvailableReplicas)
		if *(targetRS.Spec.Replicas) == targetRS.Status.AvailableReplicas {
			// 没有不健康副本，无需缩容。
			continue
		}

		scaledDownCount := min(maxCleanupCount-totalScaledDown, *(targetRS.Spec.Replicas)-targetRS.Status.AvailableReplicas)
		newReplicasCount := *(targetRS.Spec.Replicas) - scaledDownCount
		if newReplicasCount > *(targetRS.Spec.Replicas) {
			return nil, 0, fmt.Errorf("when cleaning up unhealthy replicas, got invalid request to scale down %s/%s %d -> %d", targetRS.Namespace, targetRS.Name, *(targetRS.Spec.Replicas), newReplicasCount)
		}
		_, updatedOldRS, err := dc.scaleReplicaSet(ctx, targetRS, newReplicasCount, deployment, false)
		if err != nil {
			return nil, totalScaledDown, err
		}
		totalScaledDown += scaledDownCount
		oldRSs[i] = updatedOldRS
	}
	return oldRSs, totalScaledDown, nil
}

// scaleDownOldReplicaSetsForRollingUpdate 在 RollingUpdate 策略下缩容旧 ReplicaSet。
// 缩容前校验 maxUnavailable，确保剩余可用副本不低于 minAvailable。
// 按创建时间从旧到新缩容，与 ReplicaSet 清理顺序一致，便于回滚时保留最近的副本。
func (dc *DeploymentController) scaleDownOldReplicaSetsForRollingUpdate(ctx context.Context, allRSs []*apps.ReplicaSet, oldRSs []*apps.ReplicaSet, deployment *apps.Deployment) (int32, error) {
	logger := klog.FromContext(ctx)
	maxUnavailable := deploymentutil.MaxUnavailable(*deployment)

	// 判断是否可以缩容。
	minAvailable := *(deployment.Spec.Replicas) - maxUnavailable
	// 统计可用 Pod 数量。
	availablePodCount := deploymentutil.GetAvailableReplicaCountForReplicaSets(allRSs)
	if availablePodCount <= minAvailable {
		// 无法继续缩容。
		return 0, nil
	}
	logger.V(4).Info("Found available pods in deployment, scaling down old RSes", "deployment", klog.KObj(deployment), "availableReplicas", availablePodCount)

	sort.Sort(controller.ReplicaSetsByCreationTimestamp(oldRSs))

	totalScaledDown := int32(0)
	totalScaleDownCount := availablePodCount - minAvailable
	for _, targetRS := range oldRSs {
		if totalScaledDown >= totalScaleDownCount {
			// 已达到缩容上限，无需继续处理。
			break
		}
		if *(targetRS.Spec.Replicas) == 0 {
			// 该 ReplicaSet 已无副本，无法继续缩容。
			continue
		}
		// 缩容。
		scaleDownCount := min(*(targetRS.Spec.Replicas), totalScaleDownCount-totalScaledDown)
		newReplicasCount := *(targetRS.Spec.Replicas) - scaleDownCount
		if newReplicasCount > *(targetRS.Spec.Replicas) {
			return 0, fmt.Errorf("when scaling down old RS, got invalid request to scale down %s/%s %d -> %d", targetRS.Namespace, targetRS.Name, *(targetRS.Spec.Replicas), newReplicasCount)
		}
		_, _, err := dc.scaleReplicaSet(ctx, targetRS, newReplicasCount, deployment, false)
		if err != nil {
			return totalScaledDown, err
		}

		totalScaledDown += scaleDownCount
	}

	return totalScaledDown, nil
}
