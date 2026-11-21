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

package validation

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/utils/ptr"
)

var (
	// hpaOpts 构造测试用的校验选项：scaleTargetRef 必须给出合法的 apiVersion，
	// 而对象指标沿用历史行为，允许 apiGroup 为空（老对象里常见这种写法）。
	hpaOpts = func(minReplica int32) HorizontalPodAutoscalerSpecValidationOptions {
		return HorizontalPodAutoscalerSpecValidationOptions{
			MinReplicasLowerBound: minReplica,
			ScaleTargetRefValidationOptions: CrossVersionObjectReferenceValidationOptions{
				AllowInvalidAPIVersion: false,
				AllowEmptyAPIGroup:     false,
			},
			ObjectMetricsValidationOptions: CrossVersionObjectReferenceValidationOptions{
				AllowInvalidAPIVersion: false,
				AllowEmptyAPIGroup:     true,
			},
		}
	}
	hpaSpecValidationOpts            = hpaOpts(1)
	hpaScaleToZeroSpecValidationOpts = hpaOpts(0)
)

// TestValidateScale 验证 Scale 副本数校验：0 副本是允许的（缩容到零合法），负数必须被拒绝；
// 同时覆盖 metadata.managedFields 中非法 operation 也会被通用元数据校验拦下，
// 说明 Scale 校验并非只看 spec.replicas。
func TestValidateScale(t *testing.T) {
	successCases := []autoscaling.Scale{{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "frontend",
			Namespace: metav1.NamespaceDefault,
		},
		Spec: autoscaling.ScaleSpec{
			Replicas: 1,
		},
	}, {
		ObjectMeta: metav1.ObjectMeta{
			Name:      "frontend",
			Namespace: metav1.NamespaceDefault,
		},
		Spec: autoscaling.ScaleSpec{
			Replicas: 10,
		},
	}, {
		ObjectMeta: metav1.ObjectMeta{
			Name:      "frontend",
			Namespace: metav1.NamespaceDefault,
		},
		Spec: autoscaling.ScaleSpec{
			Replicas: 0,
		},
	}}

	// 成功用例特意包含 replicas=0：缩容到零是合法状态，不能被当成负数一并拒绝。
	for _, successCase := range successCases {
		if errs := ValidateScale(&successCase); len(errs) != 0 {
			t.Errorf("expected success: %v", errs)
		}
	}

	// 错误用例分别针对负数副本和元数据字段，确保两部分校验都生效。
	errorCases := []struct {
		scale autoscaling.Scale
		msg   string
	}{{
		scale: autoscaling.Scale{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "frontend",
				Namespace: metav1.NamespaceDefault,
			},
			Spec: autoscaling.ScaleSpec{
				Replicas: -1,
			},
		},
		msg: "must be greater than or equal to 0",
	}, {
		scale: autoscaling.Scale{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "frontend",
				Namespace: metav1.NamespaceDefault,
				ManagedFields: []metav1.ManagedFieldsEntry{{
					Operation:  "InvalidOperation",
					FieldsType: "FieldsV1",
					APIVersion: "v1",
				}},
			},
			Spec: autoscaling.ScaleSpec{
				Replicas: 1,
			},
		},
		msg: "metadata.managedFields[0].operation: Unsupported value",
	}}

	for _, c := range errorCases {
		if errs := ValidateScale(&c.scale); len(errs) == 0 {
			t.Errorf("expected failure for %s", c.msg)
		} else if !strings.Contains(errs[0].Error(), c.msg) {
			t.Errorf("unexpected error: %v, expected: %s", errs[0], c.msg)
		}
	}
}

// TestValidateBehavior 通过完整的 HPA 校验来测试 behavior（扩缩容行为）规则。
// 成功场景包括 behavior 为空、合法的 Min/Max/Disabled 选择策略、多个策略并存；
// 失败场景覆盖 scaleUp 与 scaleDown 两条路径上的典型错误：缺少策略、稳定窗口超过 3600 秒、
// 策略周期超过 1800 秒、非法 selectPolicy、非法策略类型、周期或数值为零/负数。
// 两侧对称地测一遍，是为了防止只在一侧实现校验。
func TestValidateBehavior(t *testing.T) {
	maxPolicy := autoscaling.MaxPolicySelect
	minPolicy := autoscaling.MinPolicySelect
	disabledPolicy := autoscaling.DisabledPolicySelect
	incorrectPolicy := autoscaling.ScalingPolicySelect("incorrect")
	simplePoliciesList := []autoscaling.HPAScalingPolicy{{
		Type:          autoscaling.PercentScalingPolicy,
		Value:         10,
		PeriodSeconds: 1,
	}, {
		Type:          autoscaling.PodsScalingPolicy,
		Value:         1,
		PeriodSeconds: 1800,
	}}
	successCases := []autoscaling.HorizontalPodAutoscalerBehavior{{
		ScaleUp:   nil,
		ScaleDown: nil,
	}, {
		ScaleUp: &autoscaling.HPAScalingRules{
			StabilizationWindowSeconds: ptr.To[int32](3600),
			SelectPolicy:               &minPolicy,
			Policies:                   simplePoliciesList,
		},
		ScaleDown: &autoscaling.HPAScalingRules{
			StabilizationWindowSeconds: ptr.To[int32](0),
			SelectPolicy:               &disabledPolicy,
			Policies:                   simplePoliciesList,
		},
	}, {
		ScaleUp: &autoscaling.HPAScalingRules{
			StabilizationWindowSeconds: ptr.To[int32](120),
			SelectPolicy:               &maxPolicy,
			Policies: []autoscaling.HPAScalingPolicy{{
				Type:          autoscaling.PodsScalingPolicy,
				Value:         1,
				PeriodSeconds: 2,
			}, {
				Type:          autoscaling.PercentScalingPolicy,
				Value:         3,
				PeriodSeconds: 4,
			}, {
				Type:          autoscaling.PodsScalingPolicy,
				Value:         5,
				PeriodSeconds: 6,
			}, {
				Type:          autoscaling.PercentScalingPolicy,
				Value:         7,
				PeriodSeconds: 8,
			}},
		},
		ScaleDown: &autoscaling.HPAScalingRules{
			StabilizationWindowSeconds: ptr.To[int32](120),
			SelectPolicy:               &maxPolicy,
			Policies: []autoscaling.HPAScalingPolicy{{
				Type:          autoscaling.PodsScalingPolicy,
				Value:         1,
				PeriodSeconds: 2,
			}, {
				Type:          autoscaling.PercentScalingPolicy,
				Value:         3,
				PeriodSeconds: 4,
			}, {
				Type:          autoscaling.PodsScalingPolicy,
				Value:         5,
				PeriodSeconds: 6,
			}, {
				Type:          autoscaling.PercentScalingPolicy,
				Value:         7,
				PeriodSeconds: 8,
			}},
		},
	}}
	// 把 behavior 塞进一个其余字段都合法的 HPA，隔离出 behavior 本身的校验结果。
	for _, behavior := range successCases {
		hpa := prepareHPAWithBehavior(behavior)
		if errs := ValidateHorizontalPodAutoscaler(&hpa, hpaSpecValidationOpts); len(errs) != 0 {
			t.Errorf("expected success: %v", errs)
		}
	}
	// 每个错误用例只制造一处违规，便于断言第一条错误就是预期的那条。
	errorCases := []struct {
		behavior autoscaling.HorizontalPodAutoscalerBehavior
		msg      string
	}{{
		behavior: autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleUp: &autoscaling.HPAScalingRules{
				SelectPolicy: &minPolicy,
			},
		},
		msg: "spec.behavior.scaleUp.policies: Required value: must specify at least one Policy",
	}, {
		behavior: autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleUp: &autoscaling.HPAScalingRules{
				StabilizationWindowSeconds: ptr.To[int32](3601),
				SelectPolicy:               &minPolicy,
				Policies:                   simplePoliciesList,
			},
		},
		msg: "spec.behavior.scaleUp.stabilizationWindowSeconds: Invalid value: 3601: must be less than or equal to 3600",
	}, {
		behavior: autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleUp: &autoscaling.HPAScalingRules{
				Policies: []autoscaling.HPAScalingPolicy{{
					Type:          autoscaling.PodsScalingPolicy,
					Value:         7,
					PeriodSeconds: 1801,
				}},
			},
		},
		msg: "spec.behavior.scaleUp.policies[0].periodSeconds: Invalid value: 1801: must be less than or equal to 1800",
	}, {
		behavior: autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleUp: &autoscaling.HPAScalingRules{
				SelectPolicy: &incorrectPolicy,
				Policies: []autoscaling.HPAScalingPolicy{{
					Type:          autoscaling.PodsScalingPolicy,
					Value:         7,
					PeriodSeconds: 8,
				}},
			},
		},
		msg: `spec.behavior.scaleUp.selectPolicy: Unsupported value: "incorrect": supported values: "Disabled", "Max", "Min"`,
	}, {
		behavior: autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleUp: &autoscaling.HPAScalingRules{
				Policies: []autoscaling.HPAScalingPolicy{{
					Type:          autoscaling.HPAScalingPolicyType("hm"),
					Value:         7,
					PeriodSeconds: 8,
				}},
			},
		},
		msg: `spec.behavior.scaleUp.policies[0].type: Unsupported value: "hm": supported values: "Percent", "Pods"`,
	}, {
		behavior: autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleUp: &autoscaling.HPAScalingRules{
				Policies: []autoscaling.HPAScalingPolicy{{
					Type:  autoscaling.PodsScalingPolicy,
					Value: 8,
				}},
			},
		},
		msg: "spec.behavior.scaleUp.policies[0].periodSeconds: Invalid value: 0: must be greater than zero",
	}, {
		behavior: autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleUp: &autoscaling.HPAScalingRules{
				Policies: []autoscaling.HPAScalingPolicy{{
					Type:          autoscaling.PodsScalingPolicy,
					PeriodSeconds: 8,
				}},
			},
		},
		msg: "spec.behavior.scaleUp.policies[0].value: Invalid value: 0: must be greater than zero",
	}, {
		behavior: autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleUp: &autoscaling.HPAScalingRules{
				Policies: []autoscaling.HPAScalingPolicy{{
					Type:          autoscaling.PodsScalingPolicy,
					PeriodSeconds: -1,
					Value:         1,
				}},
			},
		},
		msg: "spec.behavior.scaleUp.policies[0].periodSeconds: Invalid value: -1: must be greater than zero",
	}, {
		behavior: autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleUp: &autoscaling.HPAScalingRules{
				Policies: []autoscaling.HPAScalingPolicy{{
					Type:          autoscaling.PodsScalingPolicy,
					PeriodSeconds: 1,
					Value:         -1,
				}},
			},
		},
		msg: "spec.behavior.scaleUp.policies[0].value: Invalid value: -1: must be greater than zero",
	}, {
		behavior: autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleDown: &autoscaling.HPAScalingRules{
				SelectPolicy: &minPolicy,
			},
		},
		msg: "spec.behavior.scaleDown.policies: Required value: must specify at least one Policy",
	}, {
		behavior: autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleDown: &autoscaling.HPAScalingRules{
				StabilizationWindowSeconds: ptr.To[int32](3601),
				SelectPolicy:               &minPolicy,
				Policies:                   simplePoliciesList,
			},
		},
		msg: "spec.behavior.scaleDown.stabilizationWindowSeconds: Invalid value: 3601: must be less than or equal to 3600",
	}, {
		behavior: autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleDown: &autoscaling.HPAScalingRules{
				Policies: []autoscaling.HPAScalingPolicy{{
					Type:          autoscaling.PercentScalingPolicy,
					Value:         7,
					PeriodSeconds: 1801,
				}},
			},
		},
		msg: "spec.behavior.scaleDown.policies[0].periodSeconds: Invalid value: 1801: must be less than or equal to 1800",
	}, {
		behavior: autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleDown: &autoscaling.HPAScalingRules{
				SelectPolicy: &incorrectPolicy,
				Policies: []autoscaling.HPAScalingPolicy{{
					Type:          autoscaling.PodsScalingPolicy,
					Value:         7,
					PeriodSeconds: 8,
				}},
			},
		},
		msg: `spec.behavior.scaleDown.selectPolicy: Unsupported value: "incorrect": supported values: "Disabled", "Max", "Min"`,
	}, {
		behavior: autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleDown: &autoscaling.HPAScalingRules{
				Policies: []autoscaling.HPAScalingPolicy{{
					Type:          autoscaling.HPAScalingPolicyType("hm"),
					Value:         7,
					PeriodSeconds: 8,
				}},
			},
		},
		msg: `spec.behavior.scaleDown.policies[0].type: Unsupported value: "hm": supported values: "Percent", "Pods"`,
	}, {
		behavior: autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleDown: &autoscaling.HPAScalingRules{
				Policies: []autoscaling.HPAScalingPolicy{{
					Type:  autoscaling.PodsScalingPolicy,
					Value: 8,
				}},
			},
		},
		msg: "spec.behavior.scaleDown.policies[0].periodSeconds: Invalid value: 0: must be greater than zero",
	}, {
		behavior: autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleDown: &autoscaling.HPAScalingRules{
				Policies: []autoscaling.HPAScalingPolicy{{
					Type:          autoscaling.PodsScalingPolicy,
					PeriodSeconds: 8,
				}},
			},
		},
		msg: "spec.behavior.scaleDown.policies[0].value: Invalid value: 0: must be greater than zero",
	}, {
		behavior: autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleDown: &autoscaling.HPAScalingRules{
				Policies: []autoscaling.HPAScalingPolicy{{
					Type:          autoscaling.PodsScalingPolicy,
					PeriodSeconds: -1,
					Value:         1,
				}},
			},
		},
		msg: "spec.behavior.scaleDown.policies[0].periodSeconds: Invalid value: -1: must be greater than zero",
	}, {
		behavior: autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleDown: &autoscaling.HPAScalingRules{
				Policies: []autoscaling.HPAScalingPolicy{{
					Type:          autoscaling.PodsScalingPolicy,
					PeriodSeconds: 1,
					Value:         -1,
				}},
			},
		},
		msg: "spec.behavior.scaleDown.policies[0].value: Invalid value: -1: must be greater than zero",
	}}
	// 只检查第一条错误：这些用例各自只包含一个违规点，断言首条即可避免顺序噪声。
	for _, c := range errorCases {
		hpa := prepareHPAWithBehavior(c.behavior)
		if errs := ValidateHorizontalPodAutoscaler(&hpa, hpaSpecValidationOpts); len(errs) == 0 {
			t.Errorf("expected failure for %s", c.msg)
		} else if !strings.Contains(errs[0].Error(), c.msg) {
			t.Errorf("unexpected error: %v, expected: %s", errs[0], c.msg)
		}
	}
}

// prepareHPAWithBehavior 构造一个除 behavior 外全部合法的 HPA，
// 让 behavior 相关测试不必重复写 scaleTargetRef、minReplicas、metrics 等样板字段。
func prepareHPAWithBehavior(b autoscaling.HorizontalPodAutoscalerBehavior) autoscaling.HorizontalPodAutoscaler {
	return autoscaling.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "myautoscaler",
			Namespace:       metav1.NamespaceDefault,
			ResourceVersion: "1",
		},
		Spec: autoscaling.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscaling.CrossVersionObjectReference{
				APIVersion: "apps/v1",
				Kind:       "Deployment",
				Name:       "mydeployment",
			},
			MinReplicas: ptr.To[int32](1),
			MaxReplicas: 5,
			Metrics: []autoscaling.MetricSpec{{
				Type: autoscaling.ResourceMetricSourceType,
				Resource: &autoscaling.ResourceMetricSource{
					Name: api.ResourceCPU,
					Target: autoscaling.MetricTarget{
						Type:               autoscaling.UtilizationMetricType,
						AverageUtilization: ptr.To[int32](70),
					},
				},
			}},
			Behavior: &b,
		},
	}
}

// TestValidateHorizontalPodAutoscaler 是 HPA spec 校验的主表驱动测试。
// 成功用例覆盖 Resource、ContainerResource、Pods、Object、External 各类指标源及其
// Utilization/Value/AverageValue 目标形式，以及不设置 metrics 的合法情形；
// 失败用例覆盖 scaleTargetRef 必填项与格式、minReplicas 下界、maxReplicas 不小于 minReplicas、
// 资源名与容器名校验、指标目标值互斥与正数约束、指标名、数值合法性、apiVersion 格式、
// 指标源类型以及同一指标填写多个源等违规。
// 末尾的双层循环把四种指标源的类型与内容两两错配，验证每种错配都会被拒绝。
func TestValidateHorizontalPodAutoscaler(t *testing.T) {
	metricLabelSelector, err := metav1.ParseToLabelSelector("label=value")
	if err != nil {
		t.Errorf("unable to parse label selector: %v", err)
	}

	successCases := []autoscaling.HorizontalPodAutoscaler{
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "myautoscaler",
				Namespace: metav1.NamespaceDefault,
			},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{
					Kind: "ReplicationController",
					Name: "myrc",
				},
				MinReplicas: ptr.To[int32](1),
				MaxReplicas: 5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ResourceMetricSourceType,
					Resource: &autoscaling.ResourceMetricSource{
						Name: api.ResourceCPU,
						Target: autoscaling.MetricTarget{
							Type:               autoscaling.UtilizationMetricType,
							AverageUtilization: ptr.To[int32](70),
						},
					},
				}},
			},
		}, {
			ObjectMeta: metav1.ObjectMeta{
				Name:      "myautoscaler",
				Namespace: metav1.NamespaceDefault,
			},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{
					Kind: "ReplicationController",
					Name: "myrc",
				},
				MinReplicas: ptr.To[int32](1),
				MaxReplicas: 5,
			},
		}, {
			ObjectMeta: metav1.ObjectMeta{
				Name:      "myautoscaler",
				Namespace: metav1.NamespaceDefault,
			},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{
					Kind: "ReplicationController",
					Name: "myrc",
				},
				MinReplicas: ptr.To[int32](1),
				MaxReplicas: 5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ResourceMetricSourceType,
					Resource: &autoscaling.ResourceMetricSource{
						Name: api.ResourceCPU,
						Target: autoscaling.MetricTarget{
							Type:         autoscaling.AverageValueMetricType,
							AverageValue: resource.NewMilliQuantity(300, resource.DecimalSI),
						},
					},
				}},
			},
		}, {
			ObjectMeta: metav1.ObjectMeta{
				Name:      "myautoscaler",
				Namespace: metav1.NamespaceDefault,
			},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{
					Kind: "ReplicationController",
					Name: "myrc",
				},
				MinReplicas: ptr.To[int32](1),
				MaxReplicas: 5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.PodsMetricSourceType,
					Pods: &autoscaling.PodsMetricSource{
						Metric: autoscaling.MetricIdentifier{
							Name: "somemetric",
						},
						Target: autoscaling.MetricTarget{
							Type:         autoscaling.AverageValueMetricType,
							AverageValue: resource.NewMilliQuantity(300, resource.DecimalSI),
						},
					},
				}},
			},
		}, {
			ObjectMeta: metav1.ObjectMeta{
				Name:      "myautoscaler",
				Namespace: metav1.NamespaceDefault,
			},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{
					Kind: "ReplicationController",
					Name: "myrc",
				},
				MinReplicas: ptr.To[int32](1),
				MaxReplicas: 5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ContainerResourceMetricSourceType,
					ContainerResource: &autoscaling.ContainerResourceMetricSource{
						Name:      api.ResourceCPU,
						Container: "test-container",
						Target: autoscaling.MetricTarget{
							Type:               autoscaling.UtilizationMetricType,
							AverageUtilization: ptr.To[int32](70),
						},
					},
				}},
			},
		}, {
			ObjectMeta: metav1.ObjectMeta{
				Name:      "myautoscaler",
				Namespace: metav1.NamespaceDefault,
			},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{
					Kind: "ReplicationController",
					Name: "myrc",
				},
				MinReplicas: ptr.To[int32](1),
				MaxReplicas: 5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ContainerResourceMetricSourceType,
					ContainerResource: &autoscaling.ContainerResourceMetricSource{
						Name:      api.ResourceCPU,
						Container: "test-container",
						Target: autoscaling.MetricTarget{
							Type:         autoscaling.AverageValueMetricType,
							AverageValue: resource.NewMilliQuantity(300, resource.DecimalSI),
						},
					},
				}},
			},
		}, {
			ObjectMeta: metav1.ObjectMeta{
				Name:      "myautoscaler",
				Namespace: metav1.NamespaceDefault,
			},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{
					Kind: "ReplicationController",
					Name: "myrc",
				},
				MinReplicas: ptr.To[int32](1),
				MaxReplicas: 5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ObjectMetricSourceType,
					Object: &autoscaling.ObjectMetricSource{
						DescribedObject: autoscaling.CrossVersionObjectReference{
							Kind: "ReplicationController",
							Name: "myrc",
						},
						Metric: autoscaling.MetricIdentifier{
							Name: "somemetric",
						},
						Target: autoscaling.MetricTarget{
							Type:  autoscaling.ValueMetricType,
							Value: resource.NewMilliQuantity(300, resource.DecimalSI),
						},
					},
				}},
			},
		}, {
			ObjectMeta: metav1.ObjectMeta{
				Name:      "myautoscaler",
				Namespace: metav1.NamespaceDefault,
			},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{
					Kind: "ReplicationController",
					Name: "myrc",
				},
				MinReplicas: ptr.To[int32](1),
				MaxReplicas: 5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ObjectMetricSourceType,
					Object: &autoscaling.ObjectMetricSource{
						DescribedObject: autoscaling.CrossVersionObjectReference{
							APIVersion: "networking.k8s.io/v1",
							Name:       "my-ingress",
							Kind:       "Ingress",
						},
						Metric: autoscaling.MetricIdentifier{
							Name: "requests-per-second",
						},
						Target: autoscaling.MetricTarget{
							Type:  autoscaling.ValueMetricType,
							Value: resource.NewMilliQuantity(300, resource.DecimalSI),
						},
					},
				}},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "myautoscaler",
				Namespace: metav1.NamespaceDefault,
			},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{
					Kind: "ReplicationController",
					Name: "myrc",
				},
				MinReplicas: ptr.To[int32](1),
				MaxReplicas: 5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ExternalMetricSourceType,
					External: &autoscaling.ExternalMetricSource{
						Metric: autoscaling.MetricIdentifier{
							Name:     "somemetric",
							Selector: metricLabelSelector,
						},
						Target: autoscaling.MetricTarget{
							Type:  autoscaling.ValueMetricType,
							Value: resource.NewMilliQuantity(300, resource.DecimalSI),
						},
					},
				}},
			},
		}, {
			ObjectMeta: metav1.ObjectMeta{
				Name:      "myautoscaler",
				Namespace: metav1.NamespaceDefault,
			},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{
					Kind: "ReplicationController",
					Name: "myrc",
				},
				MinReplicas: ptr.To[int32](1),
				MaxReplicas: 5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ExternalMetricSourceType,
					External: &autoscaling.ExternalMetricSource{
						Metric: autoscaling.MetricIdentifier{
							Name:     "somemetric",
							Selector: metricLabelSelector,
						},
						Target: autoscaling.MetricTarget{
							Type:         autoscaling.AverageValueMetricType,
							AverageValue: resource.NewMilliQuantity(300, resource.DecimalSI),
						},
					},
				}},
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "myautoscaler",
				Namespace: metav1.NamespaceDefault,
			},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{
					Kind:       "Deployment",
					Name:       "mydeployment",
					APIVersion: "apps/v1",
				},
				MinReplicas: ptr.To[int32](1),
				MaxReplicas: 5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ExternalMetricSourceType,
					External: &autoscaling.ExternalMetricSource{
						Metric: autoscaling.MetricIdentifier{
							Name:     "somemetric",
							Selector: metricLabelSelector,
						},
						Target: autoscaling.MetricTarget{
							Type:         autoscaling.AverageValueMetricType,
							AverageValue: resource.NewMilliQuantity(300, resource.DecimalSI),
						},
					},
				}},
			},
		},
	}

	// ReplicationController 属于核心组，允许 apiVersion 留空；其余目标必须显式给出 API 组。
	for _, successCase := range successCases {
		hpaOpts := hpaSpecValidationOpts
		if successCase.Spec.ScaleTargetRef.Kind == "ReplicationController" {
			hpaOpts.ScaleTargetRefValidationOptions.AllowEmptyAPIGroup = true
		}
		if errs := ValidateHorizontalPodAutoscaler(&successCase, hpaOpts); len(errs) != 0 {
			t.Errorf("expected success: %v", errs)
		}
	}

	// 失败表按 msg 断言首条错误，覆盖必填字段、非法取值和各字段间的约束。
	errorCases := []struct {
		horizontalPodAutoscaler autoscaling.HorizontalPodAutoscaler
		msg                     string
	}{{
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", APIVersion: "apps/v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ResourceMetricSourceType,
					Resource: &autoscaling.ResourceMetricSource{
						Name: api.ResourceCPU,
						Target: autoscaling.MetricTarget{
							Type:               autoscaling.UtilizationMetricType,
							AverageUtilization: ptr.To[int32](70),
						},
					},
				}},
			},
		},
		msg: "scaleTargetRef.kind: Required",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "myrc", APIVersion: "v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ContainerResourceMetricSourceType,
					ContainerResource: &autoscaling.ContainerResourceMetricSource{
						Name:      api.ResourceCPU,
						Container: "test-application",
						Target: autoscaling.MetricTarget{
							Type:               autoscaling.UtilizationMetricType,
							AverageUtilization: ptr.To[int32](70),
						},
					},
				}},
			},
		},
		msg: "scaleTargetRef.kind: Required",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Kind: "..", Name: "myrc", APIVersion: "v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ResourceMetricSourceType,
					Resource: &autoscaling.ResourceMetricSource{
						Name: api.ResourceCPU,
						Target: autoscaling.MetricTarget{
							Type:               autoscaling.UtilizationMetricType,
							AverageUtilization: ptr.To[int32](70),
						},
					},
				}},
			},
		},
		msg: "scaleTargetRef.kind: Invalid",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Kind: "..", Name: "myrc", APIVersion: "v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ContainerResourceMetricSourceType,
					ContainerResource: &autoscaling.ContainerResourceMetricSource{
						Name:      api.ResourceCPU,
						Container: "test-application",
						Target: autoscaling.MetricTarget{
							Type:               autoscaling.UtilizationMetricType,
							AverageUtilization: ptr.To[int32](70),
						},
					},
				}},
			},
		},
		msg: "scaleTargetRef.kind: Invalid",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Kind: "Deployment", APIVersion: "apps/v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ResourceMetricSourceType,
					Resource: &autoscaling.ResourceMetricSource{
						Name: api.ResourceCPU,
						Target: autoscaling.MetricTarget{
							Type:               autoscaling.UtilizationMetricType,
							AverageUtilization: ptr.To[int32](70),
						},
					},
				}},
			},
		},
		msg: "scaleTargetRef.name: Required",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Kind: "Deployment", APIVersion: "apps/v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ContainerResourceMetricSourceType,
					ContainerResource: &autoscaling.ContainerResourceMetricSource{
						Name:      api.ResourceCPU,
						Container: "test-application",
						Target: autoscaling.MetricTarget{
							Type:               autoscaling.UtilizationMetricType,
							AverageUtilization: ptr.To[int32](70),
						},
					},
				}},
			},
		},
		msg: "scaleTargetRef.name: Required",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "..", Kind: "Deployment", APIVersion: "apps/v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ContainerResourceMetricSourceType,
					ContainerResource: &autoscaling.ContainerResourceMetricSource{
						Name:      api.ResourceCPU,
						Container: "test-application",
						Target: autoscaling.MetricTarget{
							Type:               autoscaling.UtilizationMetricType,
							AverageUtilization: ptr.To[int32](70),
						},
					},
				}},
			},
		},
		msg: "scaleTargetRef.name: Invalid",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Kind: "Deployment", Name: "mydeployment"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ContainerResourceMetricSourceType,
					ContainerResource: &autoscaling.ContainerResourceMetricSource{
						Name:      api.ResourceCPU,
						Container: "test-application",
						Target: autoscaling.MetricTarget{
							Type:               autoscaling.UtilizationMetricType,
							AverageUtilization: ptr.To[int32](70),
						},
					},
				}},
			},
		},
		msg: "spec.scaleTargetRef.apiVersion: Invalid value: \"\": apiVersion must specify API group",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "myautoscaler",
				Namespace: metav1.NamespaceDefault,
			},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{},
				MinReplicas:    ptr.To[int32](-1),
				MaxReplicas:    5,
			},
		},
		msg: "must be greater than or equal to 1",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "myautoscaler",
				Namespace: metav1.NamespaceDefault,
			},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{},
				MinReplicas:    ptr.To[int32](7),
				MaxReplicas:    5,
			},
		},
		msg: "must be greater than or equal to `minReplicas`",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "myautoscaler",
				Namespace: metav1.NamespaceDefault,
			},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ResourceMetricSourceType,
					Resource: &autoscaling.ResourceMetricSource{
						Name: api.ResourceCPU,
						Target: autoscaling.MetricTarget{
							Type:               autoscaling.UtilizationMetricType,
							AverageUtilization: ptr.To[int32](70),
							AverageValue:       resource.NewMilliQuantity(300, resource.DecimalSI),
						},
					},
				}},
			},
		},
		msg: "may not set both a target raw value and a target utilization",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "myautoscaler",
				Namespace: metav1.NamespaceDefault,
			},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ContainerResourceMetricSourceType,
					ContainerResource: &autoscaling.ContainerResourceMetricSource{
						Name:      api.ResourceCPU,
						Container: "test-application",
						Target: autoscaling.MetricTarget{
							Type:               autoscaling.UtilizationMetricType,
							AverageUtilization: ptr.To[int32](70),
							AverageValue:       resource.NewMilliQuantity(300, resource.DecimalSI),
						},
					},
				}},
			},
		},
		msg: "may not set both a target raw value and a target utilization",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ResourceMetricSourceType,
					Resource: &autoscaling.ResourceMetricSource{
						Target: autoscaling.MetricTarget{
							Type:               autoscaling.UtilizationMetricType,
							AverageUtilization: ptr.To[int32](70),
						},
					},
				}},
			},
		},
		msg: "must specify a resource name",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ContainerResourceMetricSourceType,
					ContainerResource: &autoscaling.ContainerResourceMetricSource{
						Container: "test-application",
						Target: autoscaling.MetricTarget{
							Type:               autoscaling.UtilizationMetricType,
							AverageUtilization: ptr.To[int32](70),
						},
					},
				}},
			},
		},
		msg: "must specify a resource name",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ContainerResourceMetricSourceType,
					ContainerResource: &autoscaling.ContainerResourceMetricSource{
						Name:      "InvalidResource",
						Container: "test-application",
						Target: autoscaling.MetricTarget{
							Type:               autoscaling.UtilizationMetricType,
							AverageUtilization: ptr.To[int32](70),
						},
					},
				}},
			},
		},
		msg: "Invalid value: \"InvalidResource\": must be a standard resource type or fully qualified",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ResourceMetricSourceType,
					Resource: &autoscaling.ResourceMetricSource{
						Name: api.ResourceCPU,
						Target: autoscaling.MetricTarget{
							Type:               autoscaling.UtilizationMetricType,
							AverageUtilization: ptr.To[int32](-10),
						},
					},
				}},
			},
		},
		msg: "must be greater than 0",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ContainerResourceMetricSourceType,
					ContainerResource: &autoscaling.ContainerResourceMetricSource{
						Name:      api.ResourceCPU,
						Container: "test-application",
						Target: autoscaling.MetricTarget{
							Type:               autoscaling.UtilizationMetricType,
							AverageUtilization: ptr.To[int32](-10),
						},
					},
				}},
			},
		},
		msg: "must be greater than 0",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ContainerResourceMetricSourceType,
					ContainerResource: &autoscaling.ContainerResourceMetricSource{
						Name: api.ResourceCPU,
						Target: autoscaling.MetricTarget{
							Type:               autoscaling.UtilizationMetricType,
							AverageUtilization: ptr.To[int32](-10),
						},
					},
				}},
			},
		},
		msg: "must specify a container",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ContainerResourceMetricSourceType,
					ContainerResource: &autoscaling.ContainerResourceMetricSource{
						Name:      api.ResourceCPU,
						Container: "---***",
						Target: autoscaling.MetricTarget{
							Type:               autoscaling.UtilizationMetricType,
							AverageUtilization: ptr.To[int32](-10),
						},
					},
				}},
			},
		},
		msg: "Invalid value: \"---***\"",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ResourceMetricSourceType,
					Resource: &autoscaling.ResourceMetricSource{
						Name: api.ResourceCPU,
						Target: autoscaling.MetricTarget{
							Type: autoscaling.ValueMetricType,
						},
					},
				}},
			},
		},
		msg: "must set either a target raw value or a target utilization",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ContainerResourceMetricSourceType,
					ContainerResource: &autoscaling.ContainerResourceMetricSource{
						Name:      api.ResourceCPU,
						Container: "test-application",
						Target: autoscaling.MetricTarget{
							Type: autoscaling.ValueMetricType,
						},
					},
				}},
			},
		},
		msg: "must set either a target raw value or a target utilization",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.PodsMetricSourceType,
					Pods: &autoscaling.PodsMetricSource{
						Metric: autoscaling.MetricIdentifier{},
						Target: autoscaling.MetricTarget{
							Type:         autoscaling.ValueMetricType,
							AverageValue: resource.NewMilliQuantity(100, resource.DecimalSI),
						},
					},
				}},
			},
		},
		msg: "must specify a metric name",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.PodsMetricSourceType,
					Pods: &autoscaling.PodsMetricSource{
						Metric: autoscaling.MetricIdentifier{
							Name: "somemetric",
						},
						Target: autoscaling.MetricTarget{
							Type: autoscaling.ValueMetricType,
						},
					},
				}},
			},
		},
		msg: "must specify a positive target averageValue",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ObjectMetricSourceType,
					Object: &autoscaling.ObjectMetricSource{
						DescribedObject: autoscaling.CrossVersionObjectReference{
							Kind: "ReplicationController",
							Name: "myrc",
						},
						Metric: autoscaling.MetricIdentifier{
							Name: "somemetric",
						},
						Target: autoscaling.MetricTarget{
							Type: autoscaling.ValueMetricType,
						},
					},
				}},
			},
		},
		msg: "must set either a target value or averageValue",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ObjectMetricSourceType,
					Object: &autoscaling.ObjectMetricSource{
						DescribedObject: autoscaling.CrossVersionObjectReference{
							Name: "myrc",
						},
						Metric: autoscaling.MetricIdentifier{
							Name: "somemetric",
						},
						Target: autoscaling.MetricTarget{
							Type:  autoscaling.ValueMetricType,
							Value: resource.NewMilliQuantity(100, resource.DecimalSI),
						},
					},
				}},
			},
		},
		msg: "object.describedObject.kind: Required",
	}, {
		horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
			Spec: autoscaling.HorizontalPodAutoscalerSpec{
				ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "myrc", Kind: "ReplicationController", APIVersion: "apps/v1/v3"},
				MinReplicas:    ptr.To[int32](1),
				MaxReplicas:    5,
				Metrics: []autoscaling.MetricSpec{{
					Type: autoscaling.ObjectMetricSourceType,
					Object: &autoscaling.ObjectMetricSource{
						DescribedObject: autoscaling.CrossVersionObjectReference{
							Name: "myrc",
							Kind: "Deployment",
						},
						Metric: autoscaling.MetricIdentifier{
							Name: "somemetric",
						},
						Target: autoscaling.MetricTarget{
							Type:  autoscaling.ValueMetricType,
							Value: resource.NewMilliQuantity(100, resource.DecimalSI),
						},
					},
				}},
			},
		},
		msg: "spec.scaleTargetRef.apiVersion: Invalid value: \"apps/v1/v3\": unexpected GroupVersion string: apps/v1/v3",
	},
		{
			horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
					MinReplicas:    ptr.To[int32](1),
					MaxReplicas:    5,
					Metrics: []autoscaling.MetricSpec{{
						Type: autoscaling.ObjectMetricSourceType,
						Object: &autoscaling.ObjectMetricSource{
							DescribedObject: autoscaling.CrossVersionObjectReference{
								Kind: "ReplicationController",
								Name: "myrc",
							},
							Target: autoscaling.MetricTarget{
								Type:  autoscaling.ValueMetricType,
								Value: resource.NewMilliQuantity(100, resource.DecimalSI),
							},
						},
					}},
				},
			},
			msg: "must specify a metric name",
		}, {
			horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
					MinReplicas:    ptr.To[int32](1),
					MaxReplicas:    5,
					Metrics: []autoscaling.MetricSpec{{
						Type: autoscaling.ExternalMetricSourceType,
						External: &autoscaling.ExternalMetricSource{
							Metric: autoscaling.MetricIdentifier{
								Selector: metricLabelSelector,
							},
							Target: autoscaling.MetricTarget{
								Type:  autoscaling.ValueMetricType,
								Value: resource.NewMilliQuantity(300, resource.DecimalSI),
							},
						},
					}},
				},
			},
			msg: "must specify a metric name",
		}, {
			horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
					MinReplicas:    ptr.To[int32](1),
					MaxReplicas:    5,
					Metrics: []autoscaling.MetricSpec{{
						Type: autoscaling.ExternalMetricSourceType,
						External: &autoscaling.ExternalMetricSource{
							Metric: autoscaling.MetricIdentifier{
								Name:     "foo/../",
								Selector: metricLabelSelector,
							},
							Target: autoscaling.MetricTarget{
								Type:  autoscaling.ValueMetricType,
								Value: resource.NewMilliQuantity(300, resource.DecimalSI),
							},
						},
					}},
				},
			},
			msg: "'/'",
		},

		{
			horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
					MinReplicas:    ptr.To[int32](1),
					MaxReplicas:    5,
					Metrics: []autoscaling.MetricSpec{{
						Type: autoscaling.ExternalMetricSourceType,
						External: &autoscaling.ExternalMetricSource{
							Metric: autoscaling.MetricIdentifier{
								Name:     "somemetric",
								Selector: metricLabelSelector,
							},
							Target: autoscaling.MetricTarget{
								Type: autoscaling.ValueMetricType,
							},
						},
					}},
				},
			},
			msg: "must set either a target value for metric or a per-pod target",
		}, {
			horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
					MinReplicas:    ptr.To[int32](1),
					MaxReplicas:    5,
					Metrics: []autoscaling.MetricSpec{{
						Type: autoscaling.ExternalMetricSourceType,
						External: &autoscaling.ExternalMetricSource{
							Metric: autoscaling.MetricIdentifier{
								Name:     "somemetric",
								Selector: metricLabelSelector,
							},
							Target: autoscaling.MetricTarget{
								Type:  autoscaling.ValueMetricType,
								Value: resource.NewMilliQuantity(-300, resource.DecimalSI),
							},
						},
					}},
				},
			},
			msg: "must be positive",
		}, {
			horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
					MinReplicas:    ptr.To[int32](1),
					MaxReplicas:    5,
					Metrics: []autoscaling.MetricSpec{{
						Type: autoscaling.ExternalMetricSourceType,
						External: &autoscaling.ExternalMetricSource{
							Metric: autoscaling.MetricIdentifier{
								Name:     "somemetric",
								Selector: metricLabelSelector,
							},
							Target: autoscaling.MetricTarget{
								Type:         autoscaling.ValueMetricType,
								AverageValue: resource.NewMilliQuantity(-300, resource.DecimalSI),
							},
						},
					}},
				},
			},
			msg: "must be positive",
		}, {
			horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
					MinReplicas:    ptr.To[int32](1),
					MaxReplicas:    5,
					Metrics: []autoscaling.MetricSpec{{
						Type: autoscaling.ExternalMetricSourceType,
						External: &autoscaling.ExternalMetricSource{
							Metric: autoscaling.MetricIdentifier{
								Name:     "somemetric",
								Selector: metricLabelSelector,
							},
							Target: autoscaling.MetricTarget{
								Type:         autoscaling.ValueMetricType,
								Value:        resource.NewMilliQuantity(300, resource.DecimalSI),
								AverageValue: resource.NewMilliQuantity(300, resource.DecimalSI),
							},
						},
					}},
				},
			},
			msg: "may not set both a target value for metric and a per-pod target",
		}, {
			horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
					MinReplicas:    ptr.To[int32](1),
					MaxReplicas:    5,
					Metrics: []autoscaling.MetricSpec{{
						Type: autoscaling.ExternalMetricSourceType,
						External: &autoscaling.ExternalMetricSource{
							Metric: autoscaling.MetricIdentifier{
								Name:     "somemetric",
								Selector: metricLabelSelector,
							},
							Target: autoscaling.MetricTarget{
								Type:  "boogity",
								Value: resource.NewMilliQuantity(300, resource.DecimalSI),
							},
						},
					}},
				},
			},
			msg: "must be either Utilization, Value, or AverageValue",
		}, {
			horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
					MinReplicas:    ptr.To[int32](1),
					MaxReplicas:    5,
					Metrics: []autoscaling.MetricSpec{{
						Type: autoscaling.ExternalMetricSourceType,
						External: &autoscaling.ExternalMetricSource{
							Metric: autoscaling.MetricIdentifier{
								Name:     "somemetric",
								Selector: metricLabelSelector,
							},
							Target: autoscaling.MetricTarget{
								Value: resource.NewMilliQuantity(300, resource.DecimalSI),
							},
						},
					}},
				},
			},
			msg: "must specify a metric target type",
		}, {
			horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
					MinReplicas:    ptr.To[int32](1),
					MaxReplicas:    5,
					Metrics: []autoscaling.MetricSpec{{
						Type: autoscaling.ExternalMetricSourceType,
						External: &autoscaling.ExternalMetricSource{
							Metric: autoscaling.MetricIdentifier{
								Name:     "somemetric",
								Selector: metricLabelSelector,
							},
						},
					}},
				},
			},
			msg: "must specify a metric target",
		}, {
			horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
					MinReplicas:    ptr.To[int32](1),
					MaxReplicas:    5,
					Metrics: []autoscaling.MetricSpec{
						{},
					},
				},
			},
			msg: "must specify a metric source type",
		}, {
			horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
					MinReplicas:    ptr.To[int32](1),
					MaxReplicas:    5,
					Metrics: []autoscaling.MetricSpec{{
						Type: autoscaling.MetricSourceType("InvalidType"),
					}},
				},
			},
			msg: "type: Unsupported value",
		}, {
			horizontalPodAutoscaler: autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{Name: "mydeployment", Kind: "Deployment", APIVersion: "apps/v1"},
					MinReplicas:    ptr.To[int32](1),
					MaxReplicas:    5,
					Metrics: []autoscaling.MetricSpec{{
						Type: autoscaling.ResourceMetricSourceType,
						Resource: &autoscaling.ResourceMetricSource{
							Name: api.ResourceCPU,
							Target: autoscaling.MetricTarget{
								Type:         autoscaling.AverageValueMetricType,
								AverageValue: resource.NewMilliQuantity(100, resource.DecimalSI),
							},
						},
						Pods: &autoscaling.PodsMetricSource{
							Metric: autoscaling.MetricIdentifier{
								Name: "somemetric",
							},
							Target: autoscaling.MetricTarget{
								Type:         autoscaling.AverageValueMetricType,
								AverageValue: resource.NewMilliQuantity(100, resource.DecimalSI),
							},
						},
					}},
				},
			},
			msg: "must populate the given metric source only",
		},
	}

	// 每个失败用例只改坏一处字段，因此首条错误应当包含对应的 msg。
	for _, c := range errorCases {
		errs := ValidateHorizontalPodAutoscaler(&c.horizontalPodAutoscaler, hpaSpecValidationOpts)
		if len(errs) == 0 {
			t.Errorf("expected failure for %q", c.msg)
		} else if !strings.Contains(errs[0].Error(), c.msg) {
			t.Errorf("unexpected error:\n  expected: %q\n  got: %q", c.msg, errs[0])
		}
	}

	// 这张表把四种指标源的名字与其合法内容配对，下面的双层循环再故意打乱配对。
	sourceTypes := map[autoscaling.MetricSourceType]autoscaling.MetricSpec{
		autoscaling.ResourceMetricSourceType: {
			Resource: &autoscaling.ResourceMetricSource{
				Name: api.ResourceCPU,
				Target: autoscaling.MetricTarget{
					Type:         autoscaling.AverageValueMetricType,
					AverageValue: resource.NewMilliQuantity(100, resource.DecimalSI),
				},
			},
		},
		autoscaling.ContainerResourceMetricSourceType: {
			ContainerResource: &autoscaling.ContainerResourceMetricSource{
				Name:      api.ResourceCPU,
				Container: "test-application",
				Target: autoscaling.MetricTarget{
					Type:         autoscaling.AverageValueMetricType,
					AverageValue: resource.NewMilliQuantity(100, resource.DecimalSI),
				},
			},
		},
		autoscaling.PodsMetricSourceType: {
			Pods: &autoscaling.PodsMetricSource{
				Metric: autoscaling.MetricIdentifier{
					Name: "somemetric",
				},
				Target: autoscaling.MetricTarget{
					Type:         autoscaling.AverageValueMetricType,
					AverageValue: resource.NewMilliQuantity(100, resource.DecimalSI),
				},
			},
		},
		autoscaling.ObjectMetricSourceType: {
			Object: &autoscaling.ObjectMetricSource{
				DescribedObject: autoscaling.CrossVersionObjectReference{
					Kind:       "Deployment",
					Name:       "mydeployment",
					APIVersion: "apps/v1",
				},
				Metric: autoscaling.MetricIdentifier{
					Name: "somemetric",
				},
				Target: autoscaling.MetricTarget{
					Type:  autoscaling.ValueMetricType,
					Value: resource.NewMilliQuantity(100, resource.DecimalSI),
				},
			},
		},
	}

	// 跳过正确配对，只测类型与内容不一致的组合；这类错误必须由类型互斥校验兜住。
	for correctType, spec := range sourceTypes {
		for incorrectType := range sourceTypes {
			if correctType == incorrectType {
				continue
			}

			spec.Type = incorrectType

			errs := ValidateHorizontalPodAutoscaler(&autoscaling.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{Name: "myautoscaler", Namespace: metav1.NamespaceDefault},
				Spec: autoscaling.HorizontalPodAutoscalerSpec{
					ScaleTargetRef: autoscaling.CrossVersionObjectReference{
						Kind:       "Deployment",
						Name:       "mydeployment",
						APIVersion: "apps/v1",
					},
					MinReplicas: ptr.To[int32](1),
					MaxReplicas: 5, Metrics: []autoscaling.MetricSpec{spec},
				},
			}, hpaSpecValidationOpts)

			expectedMsg := "must populate information for the given metric source"

			if len(errs) == 0 {
				t.Errorf("expected failure with type of %v and spec for %v", incorrectType, correctType)
			} else if !strings.Contains(errs[0].Error(), expectedMsg) {
				t.Errorf("unexpected error: %q, expected %q", errs[0], expectedMsg)
			}
		}
	}
}

// prepareMinReplicasCases 按给定的 minReplicas 生成两个 HPA（对象指标与外部指标各一个），
// 供缩容到零的启用/禁用测试复用，避免每种场景重复构造完整对象。
func prepareMinReplicasCases(t *testing.T, minReplicas int32) []autoscaling.HorizontalPodAutoscaler {
	metricLabelSelector, err := metav1.ParseToLabelSelector("label=value")
	if err != nil {
		t.Errorf("unable to parse label selector: %v", err)
	}
	minReplicasCases := []autoscaling.HorizontalPodAutoscaler{{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "myautoscaler",
			Namespace:       metav1.NamespaceDefault,
			ResourceVersion: "theversion",
		},
		Spec: autoscaling.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscaling.CrossVersionObjectReference{
				Kind:       "Deployment",
				Name:       "mydeployment",
				APIVersion: "apps/v1",
			},
			MinReplicas: ptr.To[int32](minReplicas),
			MaxReplicas: 5,
			Metrics: []autoscaling.MetricSpec{{
				Type: autoscaling.ObjectMetricSourceType,
				Object: &autoscaling.ObjectMetricSource{
					DescribedObject: autoscaling.CrossVersionObjectReference{
						Kind: "ReplicationController",
						Name: "myrc",
					},
					Metric: autoscaling.MetricIdentifier{
						Name: "somemetric",
					},
					Target: autoscaling.MetricTarget{
						Type:  autoscaling.ValueMetricType,
						Value: resource.NewMilliQuantity(300, resource.DecimalSI),
					},
				},
			}},
		},
	}, {
		ObjectMeta: metav1.ObjectMeta{
			Name:            "myautoscaler",
			Namespace:       metav1.NamespaceDefault,
			ResourceVersion: "theversion",
		},
		Spec: autoscaling.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscaling.CrossVersionObjectReference{
				Kind:       "Deployment",
				Name:       "mydeployment",
				APIVersion: "apps/v1",
			},
			MinReplicas: ptr.To[int32](minReplicas),
			MaxReplicas: 5,
			Metrics: []autoscaling.MetricSpec{{
				Type: autoscaling.ExternalMetricSourceType,
				External: &autoscaling.ExternalMetricSource{
					Metric: autoscaling.MetricIdentifier{
						Name:     "somemetric",
						Selector: metricLabelSelector,
					},
					Target: autoscaling.MetricTarget{
						Type:         autoscaling.AverageValueMetricType,
						AverageValue: resource.NewMilliQuantity(300, resource.DecimalSI),
					},
				},
			}},
		},
	}}
	return minReplicasCases
}

// TestValidateHorizontalPodAutoscalerScaleToZeroEnabled 验证选项放开下界后 minReplicas=0 合法，
// 两种指标源都要通过，说明该约束与指标类型无关。
func TestValidateHorizontalPodAutoscalerScaleToZeroEnabled(t *testing.T) {
	zeroMinReplicasCases := prepareMinReplicasCases(t, 0)
	for _, successCase := range zeroMinReplicasCases {
		if errs := ValidateHorizontalPodAutoscaler(&successCase, hpaScaleToZeroSpecValidationOpts); len(errs) != 0 {
			t.Errorf("expected success: %v", errs)
		}
	}
}

// TestValidateHorizontalPodAutoscalerScaleToZeroDisabled 验证默认下界为 1 时 0 副本被拒绝，
// 同时确认 minReplicas=1 仍然通过——防止收紧校验时误伤正常配置。
func TestValidateHorizontalPodAutoscalerScaleToZeroDisabled(t *testing.T) {
	zeroMinReplicasCases := prepareMinReplicasCases(t, 0)
	// 断言的是下界错误而不是泛泛的“失败”，确保拒绝原因确实是副本数下界。
	errorMsg := "must be greater than or equal to 1"

	for _, errorCase := range zeroMinReplicasCases {
		errs := ValidateHorizontalPodAutoscaler(&errorCase, hpaSpecValidationOpts)
		if len(errs) == 0 {
			t.Errorf("expected failure for %q", errorMsg)
		} else if !strings.Contains(errs[0].Error(), errorMsg) {
			t.Errorf("unexpected error: %q, expected: %q", errs[0], errorMsg)
		}
	}

	nonZeroMinReplicasCases := prepareMinReplicasCases(t, 1)

	for _, successCase := range nonZeroMinReplicasCases {
		successCase.Spec.MinReplicas = ptr.To[int32](1)
		if errs := ValidateHorizontalPodAutoscaler(&successCase, hpaSpecValidationOpts); len(errs) != 0 {
			t.Errorf("expected success: %v", errs)
		}
	}
}

// TestValidateHorizontalPodAutoscalerUpdateScaleToZeroEnabled 验证开关打开时，
// minReplicas 在 0 与其他值之间双向切换都允许，这是缩容到零功能可演进的前提。
func TestValidateHorizontalPodAutoscalerUpdateScaleToZeroEnabled(t *testing.T) {
	zeroMinReplicasCases := prepareMinReplicasCases(t, 0)
	nonZeroMinReplicasCases := prepareMinReplicasCases(t, 1)

	for i, zeroCase := range zeroMinReplicasCases {
		nonZeroCase := nonZeroMinReplicasCases[i]

		if errs := ValidateHorizontalPodAutoscalerUpdate(&nonZeroCase, &zeroCase, hpaScaleToZeroSpecValidationOpts); len(errs) != 0 {
			t.Errorf("expected success: %v", errs)
		}

		if errs := ValidateHorizontalPodAutoscalerUpdate(&zeroCase, &nonZeroCase, hpaScaleToZeroSpecValidationOpts); len(errs) != 0 {
			t.Errorf("expected success: %v", errs)
		}
	}
}

// TestValidateHorizontalPodAutoscalerScaleToZeroUpdateDisabled 验证开关关闭时：
// 把对象改成 0 副本会被拒绝；反向把历史遗留的 0 副本对象改回合法值则允许，
// 否则存量对象将永远无法修正。
func TestValidateHorizontalPodAutoscalerScaleToZeroUpdateDisabled(t *testing.T) {
	zeroMinReplicasCases := prepareMinReplicasCases(t, 0)
	nonZeroMinReplicasCases := prepareMinReplicasCases(t, 1)
	errorMsg := "must be greater than or equal to 1"

	for i, zeroCase := range zeroMinReplicasCases {
		nonZeroCase := nonZeroMinReplicasCases[i]
		errs := ValidateHorizontalPodAutoscalerUpdate(&zeroCase, &nonZeroCase, hpaSpecValidationOpts)

		if len(errs) == 0 {
			t.Errorf("expected failure for %q", errorMsg)
		} else if !strings.Contains(errs[0].Error(), errorMsg) {
			t.Errorf("unexpected error: %q, expected: %q", errs[0], errorMsg)
		}

		// 反向更新必须放行：旧对象即使不符合当前规则，也应允许被修正为合法状态。
		if errs := ValidateHorizontalPodAutoscalerUpdate(&nonZeroCase, &zeroCase, hpaSpecValidationOpts); len(errs) != 0 {
			t.Errorf("expected success: %v", errs)
		}
	}
}

// TestValidateHorizontalPodAutoscalerConfigurableToleranceEnabled 验证可调容差字段的合法取值：
// 小数、大于 1 的值、零、不同 Quantity 表示以及未设置都通过；
// 负容差必须被拒绝，缺少策略仍要按原有规则报错。
func TestValidateHorizontalPodAutoscalerConfigurableToleranceEnabled(t *testing.T) {
	policiesList := []autoscaling.HPAScalingPolicy{{
		Type:          autoscaling.PodsScalingPolicy,
		Value:         1,
		PeriodSeconds: 1800,
	}}

	successCases := []autoscaling.HPAScalingRules{
		{
			Policies:  policiesList,
			Tolerance: ptr.To(resource.MustParse("0.1")),
		},
		{
			Policies:  policiesList,
			Tolerance: ptr.To(resource.MustParse("10")),
		},
		{
			Policies:  policiesList,
			Tolerance: ptr.To(resource.MustParse("0")),
		},
		{
			Policies:  policiesList,
			Tolerance: resource.NewMilliQuantity(100, resource.DecimalSI),
		},
		{
			Policies:  policiesList,
			Tolerance: resource.NewScaledQuantity(1, resource.Milli),
		},
		{
			Policies: policiesList,
		},
	}
	// 容差只是附加字段，不能替代策略列表；下面的失败用例专门验证这一点。
	for _, c := range successCases {
		b := autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleDown: &c,
		}
		hpa := prepareHPAWithBehavior(b)
		if errs := ValidateHorizontalPodAutoscaler(&hpa, hpaScaleToZeroSpecValidationOpts); len(errs) != 0 {
			t.Errorf("expected success: %v", errs)
		}
	}

	failureCases := []struct {
		rule autoscaling.HPAScalingRules
		msg  string
	}{
		{
			rule: autoscaling.HPAScalingRules{},
			msg:  "at least one Policy",
		},
		{
			rule: autoscaling.HPAScalingRules{
				Policies:  policiesList,
				Tolerance: ptr.To(resource.MustParse("-0.001")),
			},
			msg: "greater than or equal to 0",
		},
		{
			rule: autoscaling.HPAScalingRules{
				Policies:  policiesList,
				Tolerance: resource.NewMilliQuantity(-10, resource.DecimalSI),
			},
			msg: "greater than or equal to 0",
		},
		{
			rule: autoscaling.HPAScalingRules{
				StabilizationWindowSeconds: ptr.To[int32](60),
			},
			msg: "at least one Policy",
		},
		{
			rule: autoscaling.HPAScalingRules{
				Tolerance:                  resource.NewMilliQuantity(1, resource.DecimalSI),
				StabilizationWindowSeconds: ptr.To[int32](60),
			},
			msg: "at least one Policy",
		},
	}
	for _, c := range failureCases {
		b := autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleUp: &c.rule,
		}
		hpa := prepareHPAWithBehavior(b)
		errs := ValidateHorizontalPodAutoscaler(&hpa, hpaScaleToZeroSpecValidationOpts)
		if len(errs) != 1 {
			t.Fatalf("expected exactly one error, got: %v", errs)
		}
		if !strings.Contains(errs[0].Error(), c.msg) {
			t.Errorf("unexpected error: %q, expected: %q", errs[0], c.msg)
		}
	}
}

// TestValidateHorizontalPodAutoscalerConfigurableToleranceDisabled 验证功能关闭时容差字段被忽略：
// 带着容差不会报错，也不会改变“必须有策略”这一既有约束。
func TestValidateHorizontalPodAutoscalerConfigurableToleranceDisabled(t *testing.T) {
	maxPolicy := autoscaling.MaxPolicySelect
	policiesList := []autoscaling.HPAScalingPolicy{{
		Type:          autoscaling.PodsScalingPolicy,
		Value:         1,
		PeriodSeconds: 1800,
	}}

	successCases := []autoscaling.HPAScalingRules{
		{
			Policies: policiesList,
		},
		{
			SelectPolicy: &maxPolicy,
			Policies:     policiesList,
		},
		{
			StabilizationWindowSeconds: ptr.To[int32](60),
			Policies:                   policiesList,
		},
	}
	for _, c := range successCases {
		b := autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleDown: &c,
		}
		hpa := prepareHPAWithBehavior(b)
		if errs := ValidateHorizontalPodAutoscaler(&hpa, hpaSpecValidationOpts); len(errs) != 0 {
			t.Errorf("expected success: %v", errs)
		}
	}

	failureCases := []struct {
		rule autoscaling.HPAScalingRules
		msg  string
	}{
		{
			rule: autoscaling.HPAScalingRules{},
			msg:  "at least one Policy",
		},
		{
			rule: autoscaling.HPAScalingRules{
				StabilizationWindowSeconds: ptr.To[int32](60),
			},
			msg: "at least one Policy",
		},
	}
	for _, c := range failureCases {
		b := autoscaling.HorizontalPodAutoscalerBehavior{
			ScaleUp: &c.rule,
		}
		hpa := prepareHPAWithBehavior(b)
		errs := ValidateHorizontalPodAutoscaler(&hpa, hpaSpecValidationOpts)
		if len(errs) != 1 {
			t.Fatalf("expected exactly one error, got: %v", errs)
		}
		if !strings.Contains(errs[0].Error(), c.msg) {
			t.Errorf("unexpected error: %q, expected: %q", errs[0], c.msg)
		}
	}
}

// TestValidateHorizontalPodAutoscalerUpdateConfigurableToleranceEnabled 验证更新时容差的增删
// 在两个方向上都不算字段变更违规，功能可以平滑开启和关闭。
func TestValidateHorizontalPodAutoscalerUpdateConfigurableToleranceEnabled(t *testing.T) {
	policiesList := []autoscaling.HPAScalingPolicy{{
		Type:          autoscaling.PodsScalingPolicy,
		Value:         1,
		PeriodSeconds: 1800,
	}}

	withToleranceHPA := prepareHPAWithBehavior(autoscaling.HorizontalPodAutoscalerBehavior{
		ScaleUp: &autoscaling.HPAScalingRules{
			Policies:  policiesList,
			Tolerance: resource.NewMilliQuantity(10, resource.DecimalSI),
		}})
	withoutToleranceHPA := prepareHPAWithBehavior(autoscaling.HorizontalPodAutoscalerBehavior{
		ScaleUp: &autoscaling.HPAScalingRules{
			Policies: policiesList,
		}})

	if errs := ValidateHorizontalPodAutoscalerUpdate(&withToleranceHPA, &withoutToleranceHPA, hpaScaleToZeroSpecValidationOpts); len(errs) != 0 {
		t.Errorf("expected success: %v", errs)
	}

	if errs := ValidateHorizontalPodAutoscalerUpdate(&withoutToleranceHPA, &withToleranceHPA, hpaScaleToZeroSpecValidationOpts); len(errs) != 0 {
		t.Errorf("expected success: %v", errs)
	}
}

// TestValidateHorizontalPodAutoscalerUpdateInvalidHPA 验证兼容性策略：
// 存量的非法对象（这里 apiVersion 格式非法）在其他字段变更时应继续允许更新；
// 一旦本次修改碰到了之前被容忍的非法字段，就必须按当前规则校验并拒绝，
// 防止借更新路径写入新的非法值。
func TestValidateHorizontalPodAutoscalerUpdateInvalidHPA(t *testing.T) {
	existingHPA := autoscaling.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "myautoscaler",
			Namespace:       metav1.NamespaceDefault,
			ResourceVersion: "theversion",
		},
		Spec: autoscaling.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscaling.CrossVersionObjectReference{
				Kind:       "ReplicationController",
				Name:       "myrc",
				APIVersion: "invalidAPIVersion/v1/v2",
			},
			MinReplicas: ptr.To[int32](3),
			MaxReplicas: 5,
			Metrics: []autoscaling.MetricSpec{{
				Type: autoscaling.ObjectMetricSourceType,
				Object: &autoscaling.ObjectMetricSource{
					DescribedObject: autoscaling.CrossVersionObjectReference{
						Kind:       "ReplicationController",
						Name:       "myrc",
						APIVersion: "invalidAPIVersion/v1/v2",
					},
					Metric: autoscaling.MetricIdentifier{
						Name: "somemetric",
					},
					Target: autoscaling.MetricTarget{
						Type:  autoscaling.ValueMetricType,
						Value: resource.NewMilliQuantity(300, resource.DecimalSI),
					},
				},
			}},
		},
	}

	opts := HorizontalPodAutoscalerSpecValidationOptions{
		MinReplicasLowerBound: 0,
		ScaleTargetRefValidationOptions: CrossVersionObjectReferenceValidationOptions{
			AllowInvalidAPIVersion: true,
		},
		ObjectMetricsValidationOptions: CrossVersionObjectReferenceValidationOptions{
			AllowInvalidAPIVersion: true,
		},
	}
	modifiedHPAReplicasChanged := existingHPA.DeepCopy()
	modifiedHPAReplicasChanged.Spec.MinReplicas = ptr.To[int32](1)
	// 本次改动没有碰 apiVersion，因此沿用宽容选项继续放行旧对象的非法格式。
	// 本次改动没有碰 apiVersion，因此沿用宽容选项继续放行旧对象的非法格式。
	if errs := ValidateHorizontalPodAutoscalerUpdate(modifiedHPAReplicasChanged, &existingHPA, opts); len(errs) != 0 {
		t.Errorf("expected success: %v", errs)
	}

	// 这次改动碰到了对象指标的 apiVersion，宽容选项关闭后必须报错。
	modifiedHPAMetricsChanged := existingHPA.DeepCopy()
	modifiedHPAMetricsChanged.Spec.Metrics[0].Object.DescribedObject.APIVersion = "invalidAPIVersion/v1/v3"
	opts.ObjectMetricsValidationOptions.AllowInvalidAPIVersion = false
	if errs := ValidateHorizontalPodAutoscalerUpdate(modifiedHPAMetricsChanged, &existingHPA, opts); len(errs) == 0 {
		t.Error("expected error, APIVersion should be checked")
	}

	// 这次改动碰到了 scaleTargetRef 的 apiVersion，同样必须报错。
	modifiedHPAScaleRefChanged := existingHPA.DeepCopy()
	modifiedHPAScaleRefChanged.Spec.ScaleTargetRef.APIVersion = "invalidAPIVersion/v1/v4"
	opts.ScaleTargetRefValidationOptions.AllowInvalidAPIVersion = false
	if errs := ValidateHorizontalPodAutoscalerUpdate(modifiedHPAScaleRefChanged, &existingHPA, opts); len(errs) == 0 {
		t.Error("expected error, APIVersion should be checked")
	}
}

// TestValidateHorizontalPodAutoscalerStatusUpdate 验证 HPA 状态更新的校验：
// observedGeneration 为 nil、0、正数以及多个条件都非负时通过；
// 负数 observedGeneration（含非首条条件）以及负的 currentReplicas/desiredReplicas 被拒绝，
// 断言错误定位到具体字段路径，便于调用方修正。
func TestValidateHorizontalPodAutoscalerStatusUpdate(t *testing.T) {
	baseHPA := autoscaling.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "myautoscaler",
			Namespace:       metav1.NamespaceDefault,
			ResourceVersion: "1",
		},
	}

	// 表里的每条用例只翻转一个状态维度，错误路径要能精确定位到出问题的字段。
	testCases := []struct {
		name        string
		status      autoscaling.HorizontalPodAutoscalerStatus
		expectError bool
		errContains string
	}{
		{
			name: "valid: no conditions",
			status: autoscaling.HorizontalPodAutoscalerStatus{
				CurrentReplicas: 2,
				DesiredReplicas: 3,
			},
		}, {
			name: "valid: condition with nil observedGeneration",
			status: autoscaling.HorizontalPodAutoscalerStatus{
				CurrentReplicas: 1,
				DesiredReplicas: 1,
				Conditions: []autoscaling.HorizontalPodAutoscalerCondition{
					{Type: autoscaling.ScalingLimited, Status: autoscaling.ConditionFalse, ObservedGeneration: nil},
				},
			},
		}, {
			name: "valid: condition with zero observedGeneration",
			status: autoscaling.HorizontalPodAutoscalerStatus{
				CurrentReplicas: 1,
				DesiredReplicas: 1,
				Conditions: []autoscaling.HorizontalPodAutoscalerCondition{
					{Type: autoscaling.ScalingLimited, Status: autoscaling.ConditionFalse, ObservedGeneration: ptr.To[int64](0)},
				},
			},
		}, {
			name: "valid: condition with positive observedGeneration",
			status: autoscaling.HorizontalPodAutoscalerStatus{
				CurrentReplicas: 2,
				DesiredReplicas: 2,
				Conditions: []autoscaling.HorizontalPodAutoscalerCondition{
					{Type: autoscaling.ScalingLimited, Status: autoscaling.ConditionFalse, ObservedGeneration: ptr.To[int64](5)},
				},
			},
		}, {
			name: "valid: multiple conditions all with non-negative observedGeneration",
			status: autoscaling.HorizontalPodAutoscalerStatus{
				CurrentReplicas: 1,
				DesiredReplicas: 1,
				Conditions: []autoscaling.HorizontalPodAutoscalerCondition{
					{Type: autoscaling.ScalingActive, Status: autoscaling.ConditionTrue, ObservedGeneration: ptr.To[int64](1)},
					{Type: autoscaling.AbleToScale, Status: autoscaling.ConditionTrue, ObservedGeneration: ptr.To[int64](2)},
				},
			},
		}, {
			// generation 是递增的非负计数，负数永远是脏数据，必须拦下。
			name: "invalid: condition with negative observedGeneration",
			status: autoscaling.HorizontalPodAutoscalerStatus{
				CurrentReplicas: 1,
				DesiredReplicas: 1,
				Conditions: []autoscaling.HorizontalPodAutoscalerCondition{
					{
						Type:               autoscaling.ScalingActive,
						Status:             autoscaling.ConditionTrue,
						ObservedGeneration: ptr.To[int64](-1),
					},
				},
			},
			expectError: true,
			errContains: "status.conditions[0].observedGeneration",
		}, {
			name: "invalid: second condition has negative observedGeneration",
			status: autoscaling.HorizontalPodAutoscalerStatus{
				CurrentReplicas: 1,
				DesiredReplicas: 1,
				Conditions: []autoscaling.HorizontalPodAutoscalerCondition{
					{Type: autoscaling.ScalingActive, Status: autoscaling.ConditionTrue, ObservedGeneration: ptr.To[int64](1)},
					{Type: autoscaling.AbleToScale, Status: autoscaling.ConditionFalse, ObservedGeneration: ptr.To[int64](-3)},
				},
			},
			expectError: true,
			errContains: "status.conditions[1].observedGeneration",
		}, {
			name: "invalid: negative currentReplicas",
			status: autoscaling.HorizontalPodAutoscalerStatus{
				CurrentReplicas: -1,
				DesiredReplicas: 1,
			},
			expectError: true,
			errContains: "status.currentReplicas",
		}, {
			name: "invalid: negative desiredReplicas",
			status: autoscaling.HorizontalPodAutoscalerStatus{
				CurrentReplicas: 1,
				DesiredReplicas: -1,
			},
			expectError: true,
			errContains: "status.desiredReplicas",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			newHPA := baseHPA.DeepCopy()
			newHPA.Status = tc.status

			errs := ValidateHorizontalPodAutoscalerStatusUpdate(newHPA, &baseHPA)
			if tc.expectError {
				if len(errs) == 0 {
					t.Errorf("expected error containing %q, got none", tc.errContains)
					return
				}
				found := false
				for _, err := range errs {
					if strings.Contains(err.Field, tc.errContains) || strings.Contains(err.Error(), tc.errContains) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected error containing %q, got: %v", tc.errContains, errs)
				}
			} else if len(errs) != 0 {
				t.Errorf("expected no errors, got: %v", errs)
			}
		})
	}
}
