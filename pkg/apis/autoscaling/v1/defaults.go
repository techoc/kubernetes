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

package v1

import (
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
)

// addDefaultingFuncs 向 scheme 注册 v1 HPA 的默认值函数，
// 由生成的 register/defaults 代码在 init 时调用。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_HorizontalPodAutoscaler 为 v1 HPA 设置默认值。
// CPU 利用率的默认值不在这里设置：v1 没有指标类型字段，
// 必须在转换时结合注解判断有无其他指标后再应用，见 conversion.go。
func SetDefaults_HorizontalPodAutoscaler(obj *autoscalingv1.HorizontalPodAutoscaler) {
	if obj.Spec.MinReplicas == nil {
		obj.Spec.MinReplicas = ptr.To[int32](1)
	}

	// CPU 利用率的默认值在转换时应用，因为那里才能看到注解，
	// 从而判断是否存在 v1 字段无法表达的其他指标。
}
