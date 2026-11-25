/*
Copyright 2015 The Kubernetes Authors.

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

package v1beta1

import (
	"math"

	v1 "k8s.io/api/core/v1"
	extensionsv1beta1 "k8s.io/api/extensions/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// addDefaultingFuncs 向 scheme 注册本版本的默认值函数，
// 让解码后的对象在校验前补齐未指定字段。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_DaemonSet 为 DaemonSet 补齐选择器、更新策略、最大不可用数及修订历史上限。
// 用 Pod 模板的标签推导 Selector 和对象 Labels，是为了兼容早期未显式填写这些字段的对象。
func SetDefaults_DaemonSet(obj *extensionsv1beta1.DaemonSet) {
	labels := obj.Spec.Template.Labels

	// TODO: support templates defined elsewhere when we support them in the API
	// 仅在标签存在且用户未显式设置时推导，避免覆盖用户配置。
	if labels != nil {
		if obj.Spec.Selector == nil {
			obj.Spec.Selector = &metav1.LabelSelector{
				MatchLabels: labels,
			}
		}
		if len(obj.Labels) == 0 {
			obj.Labels = labels
		}
	}
	updateStrategy := &obj.Spec.UpdateStrategy
	// 默认使用 OnDelete：滚动更新是显式选择，不应悄悄改变升级行为。
	if updateStrategy.Type == "" {
		updateStrategy.Type = extensionsv1beta1.OnDeleteDaemonSetStrategyType
	}
	// 只有滚动更新策略才需要 RollingUpdate 参数。
	if updateStrategy.Type == extensionsv1beta1.RollingUpdateDaemonSetStrategyType {
		if updateStrategy.RollingUpdate == nil {
			rollingUpdate := extensionsv1beta1.RollingUpdateDaemonSet{}
			updateStrategy.RollingUpdate = &rollingUpdate
		}
		if updateStrategy.RollingUpdate.MaxUnavailable == nil {
			// 默认最多一个节点不可用，兼顾可用性与升级速度。
			maxUnavailable := intstr.FromInt32(1)
			updateStrategy.RollingUpdate.MaxUnavailable = &maxUnavailable
		}
		if updateStrategy.RollingUpdate.MaxSurge == nil {
			// DaemonSet 每节点只运行一个 Pod，无法超额创建，故默认 surge 为 0。
			maxSurge := intstr.FromInt32(0)
			updateStrategy.RollingUpdate.MaxSurge = &maxSurge
		}
	}
	// 保留 10 个历史版本用于回滚，未指定时补上该默认值。
	if obj.Spec.RevisionHistoryLimit == nil {
		obj.Spec.RevisionHistoryLimit = new(int32)
		*obj.Spec.RevisionHistoryLimit = 10
	}
}

// SetDefaults_Deployment 为 Deployment 补齐标签/选择器、副本数、更新策略及超时/历史限制。
// 从 Pod 模板推导标签和选择器保持了旧版本 API 的行为；MaxInt32 的超时与历史上限
// 等价于「未设置」，这样默认逻辑不会改变用户期望的语义。
func SetDefaults_Deployment(obj *extensionsv1beta1.Deployment) {
	// 对象标签和 selector 默认取自 pod 模板，保证选择范围一致。
	// 标签与选择器默认取 Pod 模板的标签，保证控制器能选中模板创建的 Pod。
	labels := obj.Spec.Template.Labels

	if labels != nil {
		if obj.Spec.Selector == nil {
			obj.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		}
		if len(obj.Labels) == 0 {
			obj.Labels = labels
		}
	}
	// 若 extensionsv1beta1.DeploymentSpec.Replicas 未设置则置为 1，
	// 未指定副本数时按单副本处理，这是最保守的默认行为。
	if obj.Spec.Replicas == nil {
		obj.Spec.Replicas = new(int32)
		*obj.Spec.Replicas = 1
	}
	strategy := &obj.Spec.Strategy
	// 将 extensionsv1beta1.DeploymentStrategyType 默认设为 RollingUpdate，
	// 滚动更新可避免升级期间服务中断。
	if strategy.Type == "" {
		strategy.Type = extensionsv1beta1.RollingUpdateDeploymentStrategyType
	}
	// 即使用户显式写了 RollingUpdate 但策略类型为空，也需要补齐参数。
	if strategy.Type == extensionsv1beta1.RollingUpdateDeploymentStrategyType || strategy.RollingUpdate != nil {
		if strategy.RollingUpdate == nil {
			rollingUpdate := extensionsv1beta1.RollingUpdateDeployment{}
			strategy.RollingUpdate = &rollingUpdate
		}
		if strategy.RollingUpdate.MaxUnavailable == nil {
			// 默认允许 1 个 Pod 不可用，逐节点更新以免整体中断。
			// 默认最多一个副本不可用，平衡可用性与发布速度。
			maxUnavailable := intstr.FromInt32(1)
			strategy.RollingUpdate.MaxUnavailable = &maxUnavailable
		}
		if strategy.RollingUpdate.MaxSurge == nil {
			// 默认将 MaxSurge 设为 1，允许额外创建一个副本，
			// 保证滚动更新期间仍有足够可用副本。
			maxSurge := intstr.FromInt32(1)
			strategy.RollingUpdate.MaxSurge = &maxSurge
		}
	}
	// 将 extensionsv1beta1.DeploymentSpec.ProgressDeadlineSeconds 默认设为 MaxInt，
	// 用 MaxInt32 表示「无截止时间」，语义与不设置一致，同时避免 nil 判断扩散。
	if obj.Spec.ProgressDeadlineSeconds == nil {
		obj.Spec.ProgressDeadlineSeconds = new(int32)
		*obj.Spec.ProgressDeadlineSeconds = math.MaxInt32
	}
	// 将 extensionsv1beta1.DeploymentSpec.RevisionHistoryLimit 默认设为 MaxInt32，
	// 语义与不设置相同，保留所有历史版本，等价于不限制。
	if obj.Spec.RevisionHistoryLimit == nil {
		obj.Spec.RevisionHistoryLimit = new(int32)
		*obj.Spec.RevisionHistoryLimit = math.MaxInt32
	}
}

// SetDefaults_ReplicaSet 为 ReplicaSet 补齐选择器、对象标签与副本数。
// 与 DaemonSet/Deployment 一样，标签和选择器默认从 Pod 模板推导以兼容旧对象。
func SetDefaults_ReplicaSet(obj *extensionsv1beta1.ReplicaSet) {
	labels := obj.Spec.Template.Labels

	// TODO: support templates defined elsewhere when we support them in the API
	if labels != nil {
		if obj.Spec.Selector == nil {
			obj.Spec.Selector = &metav1.LabelSelector{
				MatchLabels: labels,
			}
		}
		if len(obj.Labels) == 0 {
			obj.Labels = labels
		}
	}
	if obj.Spec.Replicas == nil {
		obj.Spec.Replicas = new(int32)
		*obj.Spec.Replicas = 1
	}
}

// SetDefaults_NetworkPolicy 为 NetworkPolicy 补齐默认协议与默认策略类型。
// 未指定协议时按 TCP 处理；未指定 policyTypes 时按旧版 API 语义推断：
// 至少包含 Ingress，若存在 Egress 规则则同时包含 Egress。
func SetDefaults_NetworkPolicy(obj *extensionsv1beta1.NetworkPolicy) {
	// Default any undefined Protocol fields to TCP.
	for _, i := range obj.Spec.Ingress {
		for _, p := range i.Ports {
			if p.Protocol == nil {
				proto := v1.ProtocolTCP
				p.Protocol = &proto
			}
		}
	}

	if len(obj.Spec.PolicyTypes) == 0 {
		// 未指定 policyTypes 的策略至少隐含 "Ingress"，
		// 旧版本 API 没有该字段，按 Ingress 隔离推断，保持向后兼容。
		obj.Spec.PolicyTypes = []extensionsv1beta1.PolicyType{extensionsv1beta1.PolicyTypeIngress}
		// 存在 Egress 规则时也需要启用 Egress 隔离，否则这些规则会被忽略。
		if len(obj.Spec.Egress) != 0 {
			obj.Spec.PolicyTypes = append(obj.Spec.PolicyTypes, extensionsv1beta1.PolicyTypeEgress)
		}
	}
}

func SetDefaults_HTTPIngressPath(obj *extensionsv1beta1.HTTPIngressPath) {
	var defaultPathType = extensionsv1beta1.PathTypeImplementationSpecific
	// 旧版 Ingress 路径都按 ImplementationSpecific 解释，保持原有匹配行为。
	if obj.PathType == nil {
		obj.PathType = &defaultPathType
	}
}
