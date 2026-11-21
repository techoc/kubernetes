/*
Copyright The Kubernetes Authors.

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

package v1alpha3

import (
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	runtime "k8s.io/apimachinery/pkg/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/features"
)

// addDefaultingFuncs 向 scheme 注册本版本的默认值函数。
// scheme 在解码对象后调用这些函数，把未设置的字段补成默认值。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_PodGroup 为 PodGroup 设置默认抢占策略。
// 只有在 PodGroupPreemptionPolicy 特性开启时才补默认值：关闭时保持字段为 nil，
// 避免旧客户端看到未被请求的新字段，也便于特性回滚后对象恢复原貌。
func SetDefaults_PodGroup(in *schedulingv1alpha3.PodGroup) {
	if utilfeature.DefaultFeatureGate.Enabled(features.PodGroupPreemptionPolicy) {
		// 不覆盖用户显式指定的策略，仅填补未设置的字段。
		if in.Spec.PreemptionPolicy == nil {
			preemptLowerPriority := schedulingv1alpha3.PreemptLowerPriority
			in.Spec.PreemptionPolicy = &preemptLowerPriority
		}
	}
}

// SetDefaults_CompositePodGroup 为 CompositePodGroup 设置默认抢占策略，
// 语义与 SetDefaults_PodGroup 相同：受 PodGroupPreemptionPolicy 门控，
// 且只补空值、不覆盖用户配置。
func SetDefaults_CompositePodGroup(in *schedulingv1alpha3.CompositePodGroup) {
	if utilfeature.DefaultFeatureGate.Enabled(features.PodGroupPreemptionPolicy) {
		if in.Spec.PreemptionPolicy == nil {
			preemptLowerPriority := schedulingv1alpha3.PreemptLowerPriority
			in.Spec.PreemptionPolicy = &preemptLowerPriority
		}
	}
}
