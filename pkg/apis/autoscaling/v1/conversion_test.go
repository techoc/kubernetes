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
	"testing"

	"github.com/stretchr/testify/assert"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/utils/ptr"
)

// 该测试针对 issue #101370：修复 CPU 指标同时存在 AverageValue 和 Utilization 时，
// 转换到 v1 应优先取 Utilization 值，且不能被目标对象中已有的旧值干扰。
// TestConvert_autoscaling_HorizontalPodAutoscalerSpec_To_v1_HorizontalPodAutoscalerSpec
// 用表驱动方式验证 internal HPA Spec 到 v1 Spec 的转换：
// 包括 CPU 利用率优先于 AverageValue、目标对象旧值被覆盖、
// 无指标时保留目标字段，以及只有利用率指标的情况。
func TestConvert_autoscaling_HorizontalPodAutoscalerSpec_To_v1_HorizontalPodAutoscalerSpec(t *testing.T) {

	// args 保存一次用例的输入、输出对象和期望结果。
	type args struct {
		in        *autoscaling.HorizontalPodAutoscalerSpec
		out       *autoscalingv1.HorizontalPodAutoscalerSpec
		expectOut *autoscalingv1.HorizontalPodAutoscalerSpec
		s         conversion.Scope
	}
	// 前两个用例验证同一组指标在不同初始 out 值下都能得到 Utilization=70；
	// 后两个用例分别验证无指标和只有利用率指标时目标字段不被误改。
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			"TestConversionWithCPUAverageValueAndUtilizationBoth1",
			args{
				in: &autoscaling.HorizontalPodAutoscalerSpec{
					MinReplicas: ptr.To[int32](1),
					MaxReplicas: 3,
					Metrics: []autoscaling.MetricSpec{
						{
							Type: autoscaling.ResourceMetricSourceType,
							Resource: &autoscaling.ResourceMetricSource{
								Name: api.ResourceCPU,
								Target: autoscaling.MetricTarget{
									Type:         autoscaling.AverageValueMetricType,
									AverageValue: resource.NewMilliQuantity(300, resource.DecimalSI),
								},
							},
						},
						{
							Type: autoscaling.ResourceMetricSourceType,
							Resource: &autoscaling.ResourceMetricSource{
								Name: api.ResourceCPU,
								Target: autoscaling.MetricTarget{
									Type:               autoscaling.UtilizationMetricType,
									AverageUtilization: ptr.To[int32](70),
								},
							},
						},
					},
				},
				out: &autoscalingv1.HorizontalPodAutoscalerSpec{},
				expectOut: &autoscalingv1.HorizontalPodAutoscalerSpec{
					MinReplicas:                    ptr.To[int32](1),
					MaxReplicas:                    3,
					TargetCPUUtilizationPercentage: ptr.To[int32](70),
				},
				s: nil,
			},
			false,
		},
		{
			"TestConversionWithCPUAverageValueAndUtilizationBoth2",
			args{
				in: &autoscaling.HorizontalPodAutoscalerSpec{
					MinReplicas: ptr.To[int32](1),
					MaxReplicas: 3,
					Metrics: []autoscaling.MetricSpec{
						{
							Type: autoscaling.ResourceMetricSourceType,
							Resource: &autoscaling.ResourceMetricSource{
								Name: api.ResourceCPU,
								Target: autoscaling.MetricTarget{
									Type:         autoscaling.AverageValueMetricType,
									AverageValue: resource.NewMilliQuantity(300, resource.DecimalSI),
								},
							},
						},
						{
							Type: autoscaling.ResourceMetricSourceType,
							Resource: &autoscaling.ResourceMetricSource{
								Name: api.ResourceCPU,
								Target: autoscaling.MetricTarget{
									Type:               autoscaling.UtilizationMetricType,
									AverageUtilization: ptr.To[int32](70),
								},
							},
						},
					},
				},
				out: &autoscalingv1.HorizontalPodAutoscalerSpec{
					MinReplicas:                    ptr.To[int32](2),
					MaxReplicas:                    4,
					TargetCPUUtilizationPercentage: ptr.To[int32](60),
				},
				expectOut: &autoscalingv1.HorizontalPodAutoscalerSpec{
					MinReplicas:                    ptr.To[int32](1),
					MaxReplicas:                    3,
					TargetCPUUtilizationPercentage: ptr.To[int32](70),
				},
				s: nil,
			},
			false,
		},
		{
			"TestConversionWithoutMetrics",
			args{
				in: &autoscaling.HorizontalPodAutoscalerSpec{
					MinReplicas: ptr.To[int32](1),
					MaxReplicas: 3,
					Metrics:     []autoscaling.MetricSpec{},
				},
				out: &autoscalingv1.HorizontalPodAutoscalerSpec{
					MinReplicas:                    ptr.To[int32](1),
					MaxReplicas:                    4,
					TargetCPUUtilizationPercentage: ptr.To[int32](60),
				},
				expectOut: &autoscalingv1.HorizontalPodAutoscalerSpec{
					MinReplicas:                    ptr.To[int32](1),
					MaxReplicas:                    3,
					TargetCPUUtilizationPercentage: ptr.To[int32](60),
				},
				s: nil,
			},
			false,
		},
		{
			"TestConversionWithCPUUtilizationOnly",
			args{
				in: &autoscaling.HorizontalPodAutoscalerSpec{
					MinReplicas: ptr.To[int32](1),
					MaxReplicas: 3,
					Metrics: []autoscaling.MetricSpec{
						{
							Type: autoscaling.ResourceMetricSourceType,
							Resource: &autoscaling.ResourceMetricSource{
								Name: api.ResourceCPU,
								Target: autoscaling.MetricTarget{
									Type:               autoscaling.UtilizationMetricType,
									AverageUtilization: ptr.To[int32](60),
								},
							},
						},
					},
				},
				out: &autoscalingv1.HorizontalPodAutoscalerSpec{},
				expectOut: &autoscalingv1.HorizontalPodAutoscalerSpec{
					MinReplicas:                    ptr.To[int32](1),
					MaxReplicas:                    3,
					TargetCPUUtilizationPercentage: ptr.To[int32](60),
				},
				s: nil,
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 同时检查错误是否符合预期，以及输出对象与期望值是否完全一致。
			if err := Convert_autoscaling_HorizontalPodAutoscalerSpec_To_v1_HorizontalPodAutoscalerSpec(tt.args.in, tt.args.out, tt.args.s); (err != nil) != tt.wantErr {
				t.Errorf("Convert_autoscaling_HorizontalPodAutoscalerSpec_To_v1_HorizontalPodAutoscalerSpec() error = %v, wantErr %v", err, tt.wantErr)
			}

			assert.Equal(t, tt.args.expectOut, tt.args.out)
		})
	}
}
