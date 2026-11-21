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

package v1beta1

import (
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/api/scheduling/v1beta1"
	runtime "k8s.io/apimachinery/pkg/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/features"
)

// addDefaultingFuncs 向 scheme 注册 v1beta1 版本的默认值函数。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_PriorityClass 相比 extensions 中的同名实现补充了额外默认值。
func SetDefaults_PriorityClass(obj *v1beta1.PriorityClass) {
	// 未指定抢占策略时沿用“抢占低优先级 Pod”的历史行为。
	if obj.PreemptionPolicy == nil {
		preemptLowerPriority := apiv1.PreemptLowerPriority
		obj.PreemptionPolicy = &preemptLowerPriority
	}
}

// SetDefaults_PodGroup 仅在特性门控开启时为 PodGroup 补齐抢占策略。
func SetDefaults_PodGroup(in *v1beta1.PodGroup) {
	// 门控关闭时保持字段为空，以维持旧版本行为。
	if utilfeature.DefaultFeatureGate.Enabled(features.PodGroupPreemptionPolicy) {
		if in.Spec.PreemptionPolicy == nil {
			preemptLowerPriority := v1beta1.PreemptLowerPriority
			in.Spec.PreemptionPolicy = &preemptLowerPriority
		}
	}
}
