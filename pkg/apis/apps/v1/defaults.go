/*
Copyright 2017 The Kubernetes Authors.

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

package v1

import (
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
)

// addDefaultingFuncs 向 scheme 注册 v1 apps 资源的默认值设置函数。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_Deployment 设置 Deployment 的默认值，与 extensions 版本相比额外补充：
// - 滚动更新的 MaxUnavailable 为 25%（extensions 为 1）
// - 滚动更新的 MaxSurge 为 25%（extensions 为 1）
// - RevisionHistoryLimit 为 10（extensions 不设置）
// - ProgressDeadlineSeconds 为 600 秒（extensions 不设置）
func SetDefaults_Deployment(obj *appsv1.Deployment) {
	// 未指定副本数时默认 1 个，保证创建后有一个可用实例。
	if obj.Spec.Replicas == nil {
		obj.Spec.Replicas = new(int32)
		*obj.Spec.Replicas = 1
	}
	strategy := &obj.Spec.Strategy
	// 默认使用滚动更新，避免重建策略造成服务中断。
	if strategy.Type == "" {
		strategy.Type = appsv1.RollingUpdateDeploymentStrategyType
	}
	if strategy.Type == appsv1.RollingUpdateDeploymentStrategyType {
		if strategy.RollingUpdate == nil {
			rollingUpdate := appsv1.RollingUpdateDeployment{}
			strategy.RollingUpdate = &rollingUpdate
		}
		if strategy.RollingUpdate.MaxUnavailable == nil {
			// 默认最多 25% 副本不可用，在升级速度与可用性之间取平衡。
			maxUnavailable := intstr.FromString("25%")
			strategy.RollingUpdate.MaxUnavailable = &maxUnavailable
		}
		if strategy.RollingUpdate.MaxSurge == nil {
			// 默认最多额外创建 25% 副本，与 MaxUnavailable 配合限制升级影响面。
			maxSurge := intstr.FromString("25%")
			strategy.RollingUpdate.MaxSurge = &maxSurge
		}
	}
	if obj.Spec.RevisionHistoryLimit == nil {
		obj.Spec.RevisionHistoryLimit = new(int32)
		*obj.Spec.RevisionHistoryLimit = 10
	}
	if obj.Spec.ProgressDeadlineSeconds == nil {
		obj.Spec.ProgressDeadlineSeconds = new(int32)
		*obj.Spec.ProgressDeadlineSeconds = 600
	}
}

// SetDefaults_DaemonSet 设置 DaemonSet 的更新策略与修订历史默认值。
func SetDefaults_DaemonSet(obj *appsv1.DaemonSet) {
	updateStrategy := &obj.Spec.UpdateStrategy
	if updateStrategy.Type == "" {
		updateStrategy.Type = appsv1.RollingUpdateDaemonSetStrategyType
	}
	if updateStrategy.Type == appsv1.RollingUpdateDaemonSetStrategyType {
		if updateStrategy.RollingUpdate == nil {
			rollingUpdate := appsv1.RollingUpdateDaemonSet{}
			updateStrategy.RollingUpdate = &rollingUpdate
		}
		if updateStrategy.RollingUpdate.MaxUnavailable == nil {
			// 默认允许 1 个 Pod 不可用，逐节点更新以免整体中断。
			updateStrategy.RollingUpdate.MaxUnavailable = ptr.To(intstr.FromInt32(1))
		}
		if updateStrategy.RollingUpdate.MaxSurge == nil {
			// 默认不允许超额创建，避免同一节点上出现两个 DaemonSet Pod。
			updateStrategy.RollingUpdate.MaxSurge = ptr.To(intstr.FromInt32(0))
		}
	}
	if obj.Spec.RevisionHistoryLimit == nil {
		obj.Spec.RevisionHistoryLimit = new(int32)
		*obj.Spec.RevisionHistoryLimit = 10
	}
}

// SetDefaults_StatefulSet 设置 StatefulSet 的 Pod 管理、更新策略、PVC 保留及副本数默认值。
func SetDefaults_StatefulSet(obj *appsv1.StatefulSet) {
	if len(obj.Spec.PodManagementPolicy) == 0 {
		obj.Spec.PodManagementPolicy = appsv1.OrderedReadyPodManagement
	}

	if obj.Spec.UpdateStrategy.Type == "" {
		obj.Spec.UpdateStrategy.Type = appsv1.RollingUpdateStatefulSetStrategyType

		if obj.Spec.UpdateStrategy.RollingUpdate == nil {
			// 先构造空的滚动更新参数，后续逻辑会补齐具体默认值。
			obj.Spec.UpdateStrategy.RollingUpdate = &appsv1.RollingUpdateStatefulSetStrategy{}
		}
	}

	if obj.Spec.UpdateStrategy.Type == appsv1.RollingUpdateStatefulSetStrategyType &&
		obj.Spec.UpdateStrategy.RollingUpdate != nil {

		if obj.Spec.UpdateStrategy.RollingUpdate.Partition == nil {
			obj.Spec.UpdateStrategy.RollingUpdate.Partition = ptr.To[int32](0)
		}
		// MaxUnavailable 受特性开关控制，关闭时不能写入该字段。
		if utilfeature.DefaultFeatureGate.Enabled(features.MaxUnavailableStatefulSet) {
			if obj.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable == nil {
				obj.Spec.UpdateStrategy.RollingUpdate.MaxUnavailable = ptr.To(intstr.FromInt32(1))
			}
		}
	}

	// PVC 默认保留，避免 StatefulSet 删除或缩容时误删用户数据。
	if obj.Spec.PersistentVolumeClaimRetentionPolicy == nil {
		obj.Spec.PersistentVolumeClaimRetentionPolicy = &appsv1.StatefulSetPersistentVolumeClaimRetentionPolicy{}
	}
	if len(obj.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted) == 0 {
		obj.Spec.PersistentVolumeClaimRetentionPolicy.WhenDeleted = appsv1.RetainPersistentVolumeClaimRetentionPolicyType
	}
	if len(obj.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled) == 0 {
		obj.Spec.PersistentVolumeClaimRetentionPolicy.WhenScaled = appsv1.RetainPersistentVolumeClaimRetentionPolicyType
	}

	if obj.Spec.Replicas == nil {
		obj.Spec.Replicas = new(int32)
		*obj.Spec.Replicas = 1
	}
	if obj.Spec.RevisionHistoryLimit == nil {
		obj.Spec.RevisionHistoryLimit = new(int32)
		*obj.Spec.RevisionHistoryLimit = 10
	}
}

// SetDefaults_ReplicaSet 为未指定副本数的 ReplicaSet 设置默认副本数 1。
func SetDefaults_ReplicaSet(obj *appsv1.ReplicaSet) {
	if obj.Spec.Replicas == nil {
		obj.Spec.Replicas = new(int32)
		*obj.Spec.Replicas = 1
	}
}
