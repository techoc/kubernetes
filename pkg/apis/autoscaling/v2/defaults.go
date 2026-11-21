/*
Copyright 2021 The Kubernetes Authors.

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

package v2

import (
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
	"k8s.io/utils/ptr"
)

var (
	// 这些常量复刻了旧版 HPA 的默认伸缩行为，保证行为兼容。
	scaleUpLimitPercent         int32 = 100
	scaleUpLimitMinimumPods     int32 = 4
	scaleUpPeriod               int32 = 15
	scaleUpStabilizationSeconds int32
	maxPolicy                   = autoscalingv2.MaxChangePolicySelect
	defaultHPAScaleUpRules      = autoscalingv2.HPAScalingRules{
		StabilizationWindowSeconds: &scaleUpStabilizationSeconds,
		SelectPolicy:               &maxPolicy,
		Policies: []autoscalingv2.HPAScalingPolicy{
			{
				Type:          autoscalingv2.PodsScalingPolicy,
				Value:         scaleUpLimitMinimumPods,
				PeriodSeconds: scaleUpPeriod,
			},
			{
				Type:          autoscalingv2.PercentScalingPolicy,
				Value:         scaleUpLimitPercent,
				PeriodSeconds: scaleUpPeriod,
			},
		},
	}
	scaleDownPeriod int32 = 15
	// 缩容稳定窗口可以通过命令行设置，因此这里不提供默认值覆盖它，
	// 保留 nil 让 controller 使用命令行配置。
	scaleDownLimitPercent    int32 = 100
	defaultHPAScaleDownRules       = autoscalingv2.HPAScalingRules{
		StabilizationWindowSeconds: nil,
		SelectPolicy:               &maxPolicy,
		Policies: []autoscalingv2.HPAScalingPolicy{
			{
				Type:          autoscalingv2.PercentScalingPolicy,
				Value:         scaleDownLimitPercent,
				PeriodSeconds: scaleDownPeriod,
			},
		},
	}
)

// addDefaultingFuncs 向 scheme 注册 v2 HPA 的默认值函数，
// 由生成的 register/defaults 代码在 init 时调用。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_HorizontalPodAutoscaler 为 v2 HPA 设置默认值：
// MinReplicas 默认为 1；没有任何指标时补一条默认的 CPU 利用率指标；
// 若设置了 Behavior，再补齐其中未指定的伸缩规则字段。
func SetDefaults_HorizontalPodAutoscaler(obj *autoscalingv2.HorizontalPodAutoscaler) {
	if obj.Spec.MinReplicas == nil {
		obj.Spec.MinReplicas = ptr.To[int32](1)
	}

	// 没有指标时 HPA 无法工作，补一条 CPU 利用率指标作为默认。
	if len(obj.Spec.Metrics) == 0 {
		utilizationDefaultVal := int32(autoscaling.DefaultCPUUtilization)
		obj.Spec.Metrics = []autoscalingv2.MetricSpec{
			{
				Type: autoscalingv2.ResourceMetricSourceType,
				Resource: &autoscalingv2.ResourceMetricSource{
					Name: v1.ResourceCPU,
					Target: autoscalingv2.MetricTarget{
						Type:               autoscalingv2.UtilizationMetricType,
						AverageUtilization: &utilizationDefaultVal,
					},
				},
			},
		}
	}
	SetDefaults_HorizontalPodAutoscalerBehavior(obj)
}

// SetDefaults_HorizontalPodAutoscalerBehavior 在 Behavior 非 nil 时，
// 为其中未指定的伸缩方向补齐默认规则（包括扩容和缩容两个方向）。
func SetDefaults_HorizontalPodAutoscalerBehavior(obj *autoscalingv2.HorizontalPodAutoscaler) {
	// 只要设置了 Behavior，就用默认值补齐 ScaleUp/ScaleDown 中未指定的字段
	// （StabilizationWindowSeconds、SelectPolicy、Policies），
	// 让 controller 端不必再处理 nil 字段。
	if obj.Spec.Behavior != nil {
		obj.Spec.Behavior.ScaleUp = GenerateHPAScaleUpRules(obj.Spec.Behavior.ScaleUp)
		obj.Spec.Behavior.ScaleDown = GenerateHPAScaleDownRules(obj.Spec.Behavior.ScaleDown)
	}
}

// GenerateHPAScaleUpRules 返回一份完整初始化的扩容规则。
// 保证返回结构中没有任何指针字段为 nil，调用方可以安全解引用。
func GenerateHPAScaleUpRules(scalingRules *autoscalingv2.HPAScalingRules) *autoscalingv2.HPAScalingRules {
	defaultScalingRules := defaultHPAScaleUpRules.DeepCopy()
	return copyHPAScalingRules(scalingRules, defaultScalingRules)
}

// GenerateHPAScaleDownRules 返回一份完整初始化的缩容规则。
// 保证除 StabilizationWindowSeconds 外没有指针字段为 nil；
// 它保持 nil 的原因见 defaultHPAScaleDownRules 处的说明（命令行可配置）。
func GenerateHPAScaleDownRules(scalingRules *autoscalingv2.HPAScalingRules) *autoscalingv2.HPAScalingRules {
	defaultScalingRules := defaultHPAScaleDownRules.DeepCopy()
	return copyHPAScalingRules(scalingRules, defaultScalingRules)
}

// copyHPAScalingRules 把 from 中所有非 nil 字段覆盖到 to 上，
// 返回合并后的规则：用户显式设置的值优先，未设置的字段沿用默认值。
func copyHPAScalingRules(from, to *autoscalingv2.HPAScalingRules) *autoscalingv2.HPAScalingRules {
	if from == nil {
		// 没有用户配置时直接返回默认规则，避免无意义的逐字段判断。
		return to
	}
	if from.SelectPolicy != nil {
		to.SelectPolicy = from.SelectPolicy
	}
	if from.StabilizationWindowSeconds != nil {
		to.StabilizationWindowSeconds = from.StabilizationWindowSeconds
	}
	if from.Policies != nil {
		to.Policies = from.Policies
	}
	if from.Tolerance != nil {
		to.Tolerance = from.Tolerance
	}
	return to
}
