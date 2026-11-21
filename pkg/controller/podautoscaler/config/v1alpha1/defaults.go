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

package v1alpha1

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubectrlmgrconfigv1alpha1 "k8s.io/kube-controller-manager/config/v1alpha1"
)

// RecommendedDefaultHPAControllerConfiguration 为 HPAControllerConfiguration 设置推荐默认值。
// 默认值可能随 API 版本变化。该函数故意不注册为 scheme 中普通的 `SetDefaults_Foo`：
// scheme 的默认值处理发生在转换过程中，强制套用会使用户无法为自己的嵌入配置
// 选择不同默认值。需要这些默认值的调用方应在自己的包装结构上、
// 通过 `SetDefaults_` 方法显式调用此函数。
func RecommendedDefaultHPAControllerConfiguration(obj *kubectrlmgrconfigv1alpha1.HPAControllerConfiguration) {
	zero := metav1.Duration{}
	// 零值表示用户未配置，此时套用默认值；非零值优先尊重用户配置。
	if obj.ConcurrentHorizontalPodAutoscalerSyncs == 0 {
		obj.ConcurrentHorizontalPodAutoscalerSyncs = 5
	}
	if obj.HorizontalPodAutoscalerSyncPeriod == zero {
		obj.HorizontalPodAutoscalerSyncPeriod = metav1.Duration{Duration: 15 * time.Second}
	}
	if obj.HorizontalPodAutoscalerDownscaleStabilizationWindow == zero {
		obj.HorizontalPodAutoscalerDownscaleStabilizationWindow = metav1.Duration{Duration: 5 * time.Minute}
	}
	if obj.HorizontalPodAutoscalerCPUInitializationPeriod == zero {
		obj.HorizontalPodAutoscalerCPUInitializationPeriod = metav1.Duration{Duration: 5 * time.Minute}
	}
	if obj.HorizontalPodAutoscalerInitialReadinessDelay == zero {
		obj.HorizontalPodAutoscalerInitialReadinessDelay = metav1.Duration{Duration: 30 * time.Second}
	}
	if obj.HorizontalPodAutoscalerTolerance == 0 {
		obj.HorizontalPodAutoscalerTolerance = 0.1
	}
}
