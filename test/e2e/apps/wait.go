/*
Copyright 2019 The Kubernetes Authors.

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

package apps

import (
	"context"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"

	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/test/e2e/framework"
	e2estatefulset "k8s.io/kubernetes/test/e2e/framework/statefulset"
)

// waitForPartitionedRollingUpdate 等待 set 中所有 Pod 存在且版本正确。set 必须使用
// RollingUpdateStatefulSetStrategyType，且 RollingUpdate 和 Partition 非 nil。序号小于等于 Partition 的 Pod
// 应处于当前版本，其余 Pod 应处于更新版本。
func waitForPartitionedRollingUpdate(ctx context.Context, c clientset.Interface, set *appsv1.StatefulSet) (*appsv1.StatefulSet, *v1.PodList) {
	var pods *v1.PodList
	if set.Spec.UpdateStrategy.Type != appsv1.RollingUpdateStatefulSetStrategyType {
		framework.Failf("StatefulSet %s/%s attempt to wait for partitioned update with updateStrategy %s",
			set.Namespace,
			set.Name,
			set.Spec.UpdateStrategy.Type)
	}
	if set.Spec.UpdateStrategy.RollingUpdate == nil || set.Spec.UpdateStrategy.RollingUpdate.Partition == nil {
		framework.Failf("StatefulSet %s/%s attempt to wait for partitioned update with nil RollingUpdate or nil Partition",
			set.Namespace,
			set.Name)
	}
	e2estatefulset.WaitForState(ctx, c, set, e2estatefulset.StatefulSetPoll, func(set2 *appsv1.StatefulSet, pods2 *v1.PodList) (bool, error) {
		set = set2
		pods = pods2
		partition := int(*set.Spec.UpdateStrategy.RollingUpdate.Partition)
		if len(pods.Items) < int(*set.Spec.Replicas) {
			return false, nil
		}
		if partition <= 0 && set.Status.UpdateRevision != set.Status.CurrentRevision {
			framework.Logf("Waiting for StatefulSet %s/%s to complete update",
				set.Namespace,
				set.Name,
			)
			e2estatefulset.SortStatefulPods(pods)
			for i := range pods.Items {
				if pods.Items[i].Labels[appsv1.StatefulSetRevisionLabel] != set.Status.UpdateRevision {
					framework.Logf("Waiting for Pod %s/%s to have revision %s update revision %s",
						pods.Items[i].Namespace,
						pods.Items[i].Name,
						set.Status.UpdateRevision,
						pods.Items[i].Labels[appsv1.StatefulSetRevisionLabel])
				}
			}
			return false, nil
		}
		for i := int(*set.Spec.Replicas) - 1; i >= partition; i-- {
			if pods.Items[i].Labels[appsv1.StatefulSetRevisionLabel] != set.Status.UpdateRevision {
				framework.Logf("Waiting for Pod %s/%s to have revision %s update revision %s",
					pods.Items[i].Namespace,
					pods.Items[i].Name,
					set.Status.UpdateRevision,
					pods.Items[i].Labels[appsv1.StatefulSetRevisionLabel])
				return false, nil
			}
		}
		return true, nil
	})
	return set, pods
}

// waitForStatus 等待 StatefulSetStatus 的 ObservedGeneration 不小于 set 的 Generation。
// 返回的 StatefulSet 包含满足该条件的 StatefulSetStatus
func waitForStatus(ctx context.Context, c clientset.Interface, set *appsv1.StatefulSet) *appsv1.StatefulSet {
	e2estatefulset.WaitForState(ctx, c, set, e2estatefulset.StatefulSetPoll, func(set2 *appsv1.StatefulSet, pods *v1.PodList) (bool, error) {
		if set2.Status.ObservedGeneration >= set.Generation {
			set = set2
			return true, nil
		}
		return false, nil
	})
	return set
}

// waitForPodNames 等待 StatefulSet 的 Pod 名称与预期一致。
func waitForPodNames(ctx context.Context, c clientset.Interface, set *appsv1.StatefulSet, expectedPodNames []string) {
	e2estatefulset.WaitForState(ctx, c, set, e2estatefulset.StatefulSetPoll,
		func(intSet *appsv1.StatefulSet, pods *v1.PodList) (bool, error) {
			if err := expectPodNames(pods, expectedPodNames); err != nil {
				framework.Logf("Currently %v", err)
				return false, nil
			}
			return true, nil
		})
}

// waitForStatus 等待 StatefulSetStatus 的 CurrentReplicas 等于 expectedReplicas。
// 返回的 StatefulSet 包含满足该条件的 StatefulSetStatus
func waitForStatusCurrentReplicas(ctx context.Context, c clientset.Interface, set *appsv1.StatefulSet, expectedReplicas int32) *appsv1.StatefulSet {
	e2estatefulset.WaitForState(ctx, c, set, e2estatefulset.StatefulSetPoll, func(set2 *appsv1.StatefulSet, pods *v1.PodList) (bool, error) {
		if set2.Status.ObservedGeneration >= set.Generation && set2.Status.CurrentReplicas == expectedReplicas {
			set = set2
			return true, nil
		}
		return false, nil
	})
	return set
}

// waitForPodNotReady 等待 set 中名为 podName 的 Pod 存在且没有 Ready 条件。
func waitForPodNotReady(ctx context.Context, c clientset.Interface, set *appsv1.StatefulSet, podName string) (*appsv1.StatefulSet, *v1.PodList) {
	var pods *v1.PodList
	e2estatefulset.WaitForState(ctx, c, set, e2estatefulset.StatefulSetPoll, func(set2 *appsv1.StatefulSet, pods2 *v1.PodList) (bool, error) {
		set = set2
		pods = pods2
		for i := range pods.Items {
			if pods.Items[i].Name == podName {
				return !podutil.IsPodReady(&pods.Items[i]), nil
			}
		}
		return false, nil
	})
	return set, pods
}

// waitForRollingUpdate 等待 set 中所有 Pod 存在且版本正确，并等待 RollingUpdate 完成。
// set 必须使用 RollingUpdateStatefulSetStrategyType。
func waitForRollingUpdate(ctx context.Context, c clientset.Interface, set *appsv1.StatefulSet) (*appsv1.StatefulSet, *v1.PodList) {
	var pods *v1.PodList
	if set.Spec.UpdateStrategy.Type != appsv1.RollingUpdateStatefulSetStrategyType {
		framework.Failf("StatefulSet %s/%s attempt to wait for rolling update with updateStrategy %s",
			set.Namespace,
			set.Name,
			set.Spec.UpdateStrategy.Type)
	}
	e2estatefulset.WaitForState(ctx, c, set, e2estatefulset.StatefulSetPoll, func(set2 *appsv1.StatefulSet, pods2 *v1.PodList) (bool, error) {
		set = set2
		pods = pods2
		if len(pods.Items) < int(*set.Spec.Replicas) {
			return false, nil
		}
		if set.Status.UpdateRevision != set.Status.CurrentRevision {
			framework.Logf("Waiting for StatefulSet %s/%s to complete update",
				set.Namespace,
				set.Name,
			)
			e2estatefulset.SortStatefulPods(pods)
			for i := range pods.Items {
				if pods.Items[i].Labels[appsv1.StatefulSetRevisionLabel] != set.Status.UpdateRevision {
					framework.Logf("Waiting for Pod %s/%s to have revision %s update revision %s",
						pods.Items[i].Namespace,
						pods.Items[i].Name,
						set.Status.UpdateRevision,
						pods.Items[i].Labels[appsv1.StatefulSetRevisionLabel])
				}
			}
			return false, nil
		}
		return true, nil
	})
	return set, pods
}

// waitForMaxUnavailableRollingUpdate 等待 set 中所有 Pod 存在且版本正确，并在遵守 maxUnavailable
// 约束的前提下完成 RollingUpdate。set 必须使用配置了 MaxUnavailable 的
// RollingUpdateStatefulSetStrategyType。
func waitForMaxUnavailableRollingUpdate(ctx context.Context, c clientset.Interface, set *appsv1.StatefulSet, maxUnavailable int) (*appsv1.StatefulSet, *v1.PodList) {
	var pods *v1.PodList
	if set.Spec.UpdateStrategy.Type != appsv1.RollingUpdateStatefulSetStrategyType {
		framework.Failf("StatefulSet %s/%s attempt to wait for maxUnavailable rolling update with updateStrategy %s",
			set.Namespace,
			set.Name,
			set.Spec.UpdateStrategy.Type)
	}
	e2estatefulset.WaitForState(ctx, c, set, 1*time.Second, func(set2 *appsv1.StatefulSet, pods2 *v1.PodList) (bool, error) {
		set = set2
		pods = pods2
		if len(pods.Items) < int(*set.Spec.Replicas) {
			return false, nil
		}

		// 统计不可用的 Pod
		unavailablePods := 0
		e2estatefulset.SortStatefulPods(pods)
		for i := range pods.Items {
			if !podutil.IsPodReady(&pods.Items[i]) {
				unavailablePods++
			}
		}

		// 验证从未违反 maxUnavailable 约束
		if unavailablePods > maxUnavailable {
			framework.Failf("StatefulSet %s/%s rolling update violated maxUnavailable constraint: %d unavailable pods > %d maxUnavailable",
				set.Namespace, set.Name, unavailablePods, maxUnavailable)
		}

		if set.Status.UpdateRevision != set.Status.CurrentRevision {
			framework.Logf("Waiting for StatefulSet %s/%s to complete update, %d unavailable (max %d)",
				set.Namespace,
				set.Name,
				unavailablePods,
				maxUnavailable,
			)
			for i := range pods.Items {
				if pods.Items[i].Labels[appsv1.StatefulSetRevisionLabel] != set.Status.UpdateRevision {
					framework.Logf("Waiting for Pod %s/%s to have revision %s update revision %s",
						pods.Items[i].Namespace,
						pods.Items[i].Name,
						set.Status.UpdateRevision,
						pods.Items[i].Labels[appsv1.StatefulSetRevisionLabel])
				}
			}
			return false, nil
		}
		return true, nil
	})
	return set, pods
}

// waitForRunningAndNotReady 等待 ss 中 numStatefulPods 个 Pod 处于 Running 但未 Ready。
func waitForRunningAndNotReady(ctx context.Context, c clientset.Interface, numStatefulPods int32, ss *appsv1.StatefulSet) {
	e2estatefulset.WaitForRunning(ctx, c, numStatefulPods, 0, ss)
}
