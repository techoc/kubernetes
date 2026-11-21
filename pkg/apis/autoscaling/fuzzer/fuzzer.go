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

package fuzzer

import (
	"sigs.k8s.io/randfill"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/utils/ptr"
)

// Funcs 返回 autoscaling API 组的 fuzz 函数集合。
// 这些函数把随机生成的对象修正为满足 API 约束的合法对象，
// 避免 fuzz 测试因为非法值而失败，而不是因为真正的转换 bug 失败。
var Funcs = func(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		// fuzz ScaleStatus 时保证 Selector 是合法的标签选择器字符串，
		// 直接随机生成的字符串无法通过反序列化校验。
		func(s *autoscaling.ScaleStatus, c randfill.Continue) {
			c.FillNoCustom(s) // 递归填充自身字段，但不再次调用当前 fuzz 函数，避免无限递归

			// 重新生成一个合法的标签选择器，并转换成字符串形式
			metaSelector := &metav1.LabelSelector{}
			c.Fill(metaSelector)
			labelSelector, _ := metav1.LabelSelectorAsSelector(metaSelector)
			s.Selector = labelSelector.String()
		},
		// fuzz HPA Spec 时，用合法取值覆盖随机生成的指标与伸缩行为字段，
		// 确保后续转换和序列化测试拿到结构完整的对象。
		func(s *autoscaling.HorizontalPodAutoscalerSpec, c randfill.Continue) {
			c.FillNoCustom(s) // 递归填充自身字段，但不再次调用当前 fuzz 函数，避免无限递归
			s.MinReplicas = ptr.To[int32](c.Rand.Int31())

			randomQuantity := func() resource.Quantity {
				var q resource.Quantity
				c.Fill(&q)
				// 预先计算一次字符串形式，避免基准测试中重复计算影响测量
				_ = q.String()
				return q
			}

			var podMetricID autoscaling.MetricIdentifier
			var objMetricID autoscaling.MetricIdentifier
			c.Fill(&podMetricID)
			c.Fill(&objMetricID)

			targetUtilization := int32(c.Uint64())
			averageValue := randomQuantity()
			s.Metrics = []autoscaling.MetricSpec{
				{
					Type: autoscaling.PodsMetricSourceType,
					Pods: &autoscaling.PodsMetricSource{
						Metric: podMetricID,
						Target: autoscaling.MetricTarget{
							Type:         autoscaling.AverageValueMetricType,
							AverageValue: &averageValue,
						},
					},
				},
				{
					Type: autoscaling.ObjectMetricSourceType,
					Object: &autoscaling.ObjectMetricSource{
						Metric: objMetricID,
						Target: autoscaling.MetricTarget{
							Type:  autoscaling.ValueMetricType,
							Value: &averageValue,
						},
					},
				},
				{
					Type: autoscaling.ResourceMetricSourceType,
					Resource: &autoscaling.ResourceMetricSource{
						Name: api.ResourceCPU,
						Target: autoscaling.MetricTarget{
							Type:               autoscaling.UtilizationMetricType,
							AverageUtilization: &targetUtilization,
						},
					},
				},
			}
			stabilizationWindow := int32(c.Uint64())
			maxPolicy := autoscaling.MaxPolicySelect
			minPolicy := autoscaling.MinPolicySelect
			s.Behavior = &autoscaling.HorizontalPodAutoscalerBehavior{
				ScaleUp: &autoscaling.HPAScalingRules{
					StabilizationWindowSeconds: &stabilizationWindow,
					SelectPolicy:               &maxPolicy,
					Policies: []autoscaling.HPAScalingPolicy{
						{
							Type:          autoscaling.PodsScalingPolicy,
							Value:         int32(c.Uint64()),
							PeriodSeconds: int32(c.Uint64()),
						},
						{
							Type:          autoscaling.PercentScalingPolicy,
							Value:         int32(c.Uint64()),
							PeriodSeconds: int32(c.Uint64()),
						},
					},
				},
				ScaleDown: &autoscaling.HPAScalingRules{
					StabilizationWindowSeconds: &stabilizationWindow,
					SelectPolicy:               &minPolicy,
					Policies: []autoscaling.HPAScalingPolicy{
						{
							Type:          autoscaling.PodsScalingPolicy,
							Value:         int32(c.Uint64()),
							PeriodSeconds: int32(c.Uint64()),
						},
						{
							Type:          autoscaling.PercentScalingPolicy,
							Value:         int32(c.Uint64()),
							PeriodSeconds: int32(c.Uint64()),
						},
					},
				},
			}
		},
		// fuzz HPA Status 时，用固定的指标类型组合覆盖随机字段，
		// 保证 CurrentMetrics 里的每种指标都有对应的合法取值。
		func(s *autoscaling.HorizontalPodAutoscalerStatus, c randfill.Continue) {
			c.FillNoCustom(s) // 递归填充自身字段，但不再次调用当前 fuzz 函数，避免无限递归
			randomQuantity := func() resource.Quantity {
				var q resource.Quantity
				c.Fill(&q)
				// 预先计算一次字符串形式，避免基准测试中重复计算影响测量
				_ = q.String()
				return q
			}
			averageValue := randomQuantity()
			currentUtilization := int32(c.Uint64())
			s.CurrentMetrics = []autoscaling.MetricStatus{
				{
					Type: autoscaling.PodsMetricSourceType,
					Pods: &autoscaling.PodsMetricStatus{
						Metric: autoscaling.MetricIdentifier{
							Name: c.String(0),
						},
						Current: autoscaling.MetricValueStatus{
							AverageValue: &averageValue,
						},
					},
				},
				{
					Type: autoscaling.ResourceMetricSourceType,
					Resource: &autoscaling.ResourceMetricStatus{
						Name: api.ResourceCPU,
						Current: autoscaling.MetricValueStatus{
							AverageUtilization: &currentUtilization,
							AverageValue:       &averageValue,
						},
					},
				},
			}
		},
	}
}
