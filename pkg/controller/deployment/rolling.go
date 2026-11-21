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

	// Scale up, if we can.
	// 先扩容新 RS：只有新版本有了可用副本，缩容旧版本才不会降低可用性。
	scaledUp, err := dc.reconcileNewReplicaSet(ctx, allRSs, newRS, d)
	if err != nil {
		return err
	}
	if scaledUp {
		// Update DeploymentStatus
		return dc.syncRolloutStatus(ctx, allRSs, newRS, d)
	}

	// Scale down, if we can.
	// 新 RS 已尽可能扩容后再缩容旧 RS，避免在发布中途扩大不可用副本数。
	scaledDown, err := dc.reconcileOldReplicaSets(ctx, allRSs, controller.FilterActiveReplicaSets(oldRSs), newRS, d)
	if err != nil {
		return err
	}
	if scaledDown {
		// Update DeploymentStatus
		return dc.syncRolloutStatus(ctx, allRSs, newRS, d)
	}

	if deploymentutil.DeploymentComplete(d, &d.Status) {
		if err := dc.cleanupDeployment(ctx, oldRSs, d); err != nil {
			return err
		}
	}

	// Sync deployment status
	return dc.syncRolloutStatus(ctx, allRSs, newRS, d)
}

// reconcileNewReplicaSet 调整新 ReplicaSet 的副本数以匹配 Deployment 期望值。
// 若新 RS 副本已超过期望值（例如回滚场景）则直接缩容；否则按 maxSurge/maxUnavailable 计算本次可扩容数量。
func (dc *DeploymentController) reconcileNewReplicaSet(ctx context.Context, allRSs []*apps.ReplicaSet, newRS *apps.ReplicaSet, deployment *apps.Deployment) (bool, error) {
	if *(newRS.Spec.Replicas) == *(deployment.Spec.Replicas) {
		// Scaling not required.
		return false, nil
	}
	if *(newRS.Spec.Replicas) > *(deployment.Spec.Replicas) {
		// Scale down.
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
		// Can't scale down further
		return false, nil
	}
	allPodsCount := deploymentutil.GetReplicaCountForReplicaSets(allRSs)
	logger.V(4).Info("New replica set", "replicaSet", klog.KObj(newRS), "availableReplicas", newRS.Status.AvailableReplicas)
	maxUnavailable := deploymentutil.MaxUnavailable(*deployment)

	// Check if we can scale down. We can scale down in the following 2 cases:
	// * Some old replica sets have unhealthy replicas, we could safely scale down those unhealthy replicas since that won't further
	//  increase unavailability.
	// * New replica set has scaled up and it's replicas becomes ready, then we can scale down old replica sets in a further step.
	//
	// maxScaledDown := allPodsCount - minAvailable - newReplicaSetPodsUnavailable
	// take into account not only maxUnavailable and any surge pods that have been created, but also unavailable pods from
	// the newRS, so that the unavailable pods from the newRS would not make us scale down old replica sets in a further
	// step(that will increase unavailability).
	//
	// Concrete example:
	//
	// * 10 replicas
	// * 2 maxUnavailable (absolute number, not percent)
	// * 3 maxSurge (absolute number, not percent)
	//
	// case 1:
	// * Deployment is updated, newRS is created with 3 replicas, oldRS is scaled down to 8, and newRS is scaled up to 5.
	// * The new replica set pods crashloop and never become available.
	// * allPodsCount is 13. minAvailable is 8. newRSPodsUnavailable is 5.
	// * A node fails and causes one of the oldRS pods to become unavailable. However, 13 - 8 - 5 = 0, so the oldRS won't be scaled down.
	// * The user notices the crashloop and does kubectl rollout undo to rollback.
	// * newRSPodsUnavailable is 1, since we rolled back to the good replica set, so maxScaledDown = 13 - 8 - 1 = 4. 4 of the crashlooping pods will be scaled down.
	// * The total number of pods will then be 9 and the newRS can be scaled up to 10.
	//
	// case 2:
	// Same example, but pushing a new pod template instead of rolling back (aka "roll over"):
	// * The new replica set created must start with 0 replicas because allPodsCount is already at 13.
	// * However, newRSPodsUnavailable would also be 0, so the 2 old replica sets could be scaled down by 5 (13 - 8 - 0), which would then
	// allow the new replica set to be scaled up by 5.
	minAvailable := *(deployment.Spec.Replicas) - maxUnavailable
	newRSUnavailablePodCount := *(newRS.Spec.Replicas) - newRS.Status.AvailableReplicas
	maxScaledDown := allPodsCount - minAvailable - newRSUnavailablePodCount
	if maxScaledDown <= 0 {
		return false, nil
	}

	// Clean up unhealthy replicas first, otherwise unhealthy replicas will block deployment
	// and cause timeout. See https://github.com/kubernetes/kubernetes/issues/16737
	// 优先清理不健康副本：它们会一直占用额度导致发布无法推进直至超时。
	oldRSs, cleanupCount, err := dc.cleanupUnhealthyReplicas(ctx, oldRSs, deployment, maxScaledDown)
	if err != nil {
		return false, nil
	}
	logger.V(4).Info("Cleaned up unhealthy replicas from old RSes", "count", cleanupCount)

	// Scale down old replica sets, need check maxUnavailable to ensure we can scale down
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
	// Safely scale down all old replica sets with unhealthy replicas. Replica set will sort the pods in the order
	// such that not-ready < ready, unscheduled < scheduled, and pending < running. This ensures that unhealthy replicas will
	// been deleted first and won't increase unavailability.
	// ReplicaSet 删除 Pod 时会优先删除未就绪的 Pod，因此先清理不健康副本不会进一步降低可用性。
	totalScaledDown := int32(0)
	for i, targetRS := range oldRSs {
		if totalScaledDown >= maxCleanupCount {
			break
		}
		if *(targetRS.Spec.Replicas) == 0 {
			// cannot scale down this replica set.
			continue
		}
		logger.V(4).Info("Found available pods in old RS", "replicaSet", klog.KObj(targetRS), "availableReplicas", targetRS.Status.AvailableReplicas)
		if *(targetRS.Spec.Replicas) == targetRS.Status.AvailableReplicas {
			// no unhealthy replicas found, no scaling required.
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

	// Check if we can scale down.
	minAvailable := *(deployment.Spec.Replicas) - maxUnavailable
	// Find the number of available pods.
	availablePodCount := deploymentutil.GetAvailableReplicaCountForReplicaSets(allRSs)
	if availablePodCount <= minAvailable {
		// Cannot scale down.
		return 0, nil
	}
	logger.V(4).Info("Found available pods in deployment, scaling down old RSes", "deployment", klog.KObj(deployment), "availableReplicas", availablePodCount)

	sort.Sort(controller.ReplicaSetsByCreationTimestamp(oldRSs))

	totalScaledDown := int32(0)
	totalScaleDownCount := availablePodCount - minAvailable
	for _, targetRS := range oldRSs {
		if totalScaledDown >= totalScaleDownCount {
			// No further scaling required.
			break
		}
		if *(targetRS.Spec.Replicas) == 0 {
			// cannot scale down this ReplicaSet.
			continue
		}
		// Scale down.
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
