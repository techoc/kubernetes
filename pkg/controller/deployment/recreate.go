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

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controller/deployment/util"
)

// rolloutRecreate 实现 Recreate 策略下重建 ReplicaSet 的发布逻辑。
func (dc *DeploymentController) rolloutRecreate(ctx context.Context, d *apps.Deployment, rsList []*apps.ReplicaSet, podMap map[types.UID][]*v1.Pod) error {
	// 此时不创建新 RS，确保不会在旧 RS 缩容之前先扩容。
	newRS, oldRSs, err := dc.getAllReplicaSetsAndSyncRevision(ctx, d, rsList, false)
	if err != nil {
		return err
	}
	allRSs := append(oldRSs, newRS)
	activeOldRSs := controller.FilterActiveReplicaSets(oldRSs)

	// 先对旧 ReplicaSet 缩容。
	scaledDown, err := dc.scaleDownOldReplicaSetsForRecreate(ctx, activeOldRSs, d)
	if err != nil {
		return err
	}
	if scaledDown {
		// 缩容过程中先更新 DeploymentStatus，反映当前进展。
		return dc.syncRolloutStatus(ctx, allRSs, newRS, d)
	}

	// 若仍有旧 Pod 在运行，暂停后续发布步骤。
	if oldPodsRunning(newRS, oldRSs, podMap) {
		return dc.syncRolloutStatus(ctx, allRSs, newRS, d)
	}

	// 旧 Pod 全部退出后，才创建新的 ReplicaSet。
	if newRS == nil {
		newRS, oldRSs, err = dc.getAllReplicaSetsAndSyncRevision(ctx, d, rsList, true)
		if err != nil {
			return err
		}
		allRSs = append(oldRSs, newRS)
	}

	// 对新 ReplicaSet 扩容。
	if _, err := dc.scaleUpNewReplicaSetForRecreate(ctx, newRS, d); err != nil {
		return err
	}

	if util.DeploymentComplete(d, &d.Status) {
		if err := dc.cleanupDeployment(ctx, oldRSs, d); err != nil {
			return err
		}
	}

	// 最后同步 Deployment 状态。
	return dc.syncRolloutStatus(ctx, allRSs, newRS, d)
}

// scaleDownOldReplicaSetsForRecreate 在部署策略为 Recreate 时对旧 ReplicaSet 缩容。
func (dc *DeploymentController) scaleDownOldReplicaSetsForRecreate(ctx context.Context, oldRSs []*apps.ReplicaSet, deployment *apps.Deployment) (bool, error) {
	scaled := false
	for i := range oldRSs {
		rs := oldRSs[i]
		// 副本数已为 0，无需缩容。
		if *(rs.Spec.Replicas) == 0 {
			continue
		}
		scaledRS, updatedRS, err := dc.scaleReplicaSet(ctx, rs, 0, deployment, false)
		if err != nil {
			return false, err
		}
		if scaledRS {
			oldRSs[i] = updatedRS
			scaled = true
		}
	}
	return scaled, nil
}

// oldPodsRunning 判断是否存在运行中的旧 Pod，或旧 ReplicaSet 是否仍认为其持有 Pod。
func oldPodsRunning(newRS *apps.ReplicaSet, oldRSs []*apps.ReplicaSet, podMap map[types.UID][]*v1.Pod) bool {
	if oldPods := util.GetActualReplicaCountForReplicaSets(oldRSs); oldPods > 0 {
		return true
	}
	for rsUID, podList := range podMap {
		// 属于新 ReplicaSet 的 Pod 不计入旧 Pod。
		if newRS != nil && newRS.UID == rsUID {
			continue
		}
		for _, pod := range podList {
			switch pod.Status.Phase {
			case v1.PodFailed, v1.PodSucceeded:
				// 已终止的 Pod 不算在运行。
				continue
			case v1.PodUnknown:
				// v1.PodUnknown 是已废弃的状态。
				// 保留此逻辑以兼容旧版本行为。
				// 过去节点与集群短暂失联时会出现该状态。
				// 无法确认 Pod 未运行时，必须保守地视作仍在运行。
				return true
			default:
				// Pod 尚未进入终止阶段，视为仍在运行。
				return true
			}
		}
	}
	return false
}

// scaleUpNewReplicaSetForRecreate 在部署策略为 Recreate 时对新 ReplicaSet 扩容。
func (dc *DeploymentController) scaleUpNewReplicaSetForRecreate(ctx context.Context, newRS *apps.ReplicaSet, deployment *apps.Deployment) (bool, error) {
	scaled, _, err := dc.scaleReplicaSet(ctx, newRS, *(deployment.Spec.Replicas), deployment, false)
	return scaled, err
}
