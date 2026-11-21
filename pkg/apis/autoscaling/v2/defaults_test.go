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

package v2_test

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/features"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
	_ "k8s.io/kubernetes/pkg/apis/autoscaling/install"
	. "k8s.io/kubernetes/pkg/apis/autoscaling/v2"
	_ "k8s.io/kubernetes/pkg/apis/core/install"
	"k8s.io/utils/ptr"
)

// TestGenerateScaleDownRules 用表驱动方式验证缩容规则默认值：
// 全默认、全部字段指定、仅百分比策略、仅 Pods 策略四类用例，
// 确认用户显式配置会覆盖默认值，稳定窗口保持 nil 以沿用命令行配置。
func TestGenerateScaleDownRules(t *testing.T) {
	// TestCase 描述一组缩容规则输入及其期望的合并结果。
	type TestCase struct {
		rateDownPods                 int32
		rateDownPodsPeriodSeconds    int32
		rateDownPercent              int32
		rateDownPercentPeriodSeconds int32
		stabilizationSeconds         *int32
		selectPolicy                 *autoscalingv2.ScalingPolicySelect

		expectedPolicies      []autoscalingv2.HPAScalingPolicy
		expectedStabilization *int32
		expectedSelectPolicy  string
		annotation            string
	}
	maxPolicy := autoscalingv2.MaxChangePolicySelect
	minPolicy := autoscalingv2.MinChangePolicySelect
	// 前两个用例分别覆盖全部默认和全部字段显式指定；后两个用例各只指定一种策略。
	tests := []TestCase{
		{
			annotation: "Default values",
			expectedPolicies: []autoscalingv2.HPAScalingPolicy{
				{Type: autoscalingv2.PercentScalingPolicy, Value: 100, PeriodSeconds: 15},
			},
			expectedStabilization: nil,
			expectedSelectPolicy:  string(autoscalingv2.MaxChangePolicySelect),
		},
		{
			annotation:                   "All parameters are specified",
			rateDownPods:                 1,
			rateDownPodsPeriodSeconds:    2,
			rateDownPercent:              3,
			rateDownPercentPeriodSeconds: 4,
			stabilizationSeconds:         ptr.To[int32](25),
			selectPolicy:                 &maxPolicy,
			expectedPolicies: []autoscalingv2.HPAScalingPolicy{
				{Type: autoscalingv2.PodsScalingPolicy, Value: 1, PeriodSeconds: 2},
				{Type: autoscalingv2.PercentScalingPolicy, Value: 3, PeriodSeconds: 4},
			},
			expectedStabilization: ptr.To[int32](25),
			expectedSelectPolicy:  string(autoscalingv2.MaxChangePolicySelect),
		},
		{
			annotation:                   "Percent policy is specified",
			rateDownPercent:              1,
			rateDownPercentPeriodSeconds: 2,
			selectPolicy:                 &minPolicy,
			expectedPolicies: []autoscalingv2.HPAScalingPolicy{
				{Type: autoscalingv2.PercentScalingPolicy, Value: 1, PeriodSeconds: 2},
			},
			expectedStabilization: nil,
			expectedSelectPolicy:  string(autoscalingv2.MinChangePolicySelect),
		},
		{
			annotation:                "Pods policy is specified",
			rateDownPods:              3,
			rateDownPodsPeriodSeconds: 4,
			expectedPolicies: []autoscalingv2.HPAScalingPolicy{
				{Type: autoscalingv2.PodsScalingPolicy, Value: 3, PeriodSeconds: 4},
			},
			expectedStabilization: nil,
			expectedSelectPolicy:  string(autoscalingv2.MaxChangePolicySelect),
		},
	}
	for _, tc := range tests {
		t.Run(tc.annotation, func(t *testing.T) {
			scaleDownRules := &autoscalingv2.HPAScalingRules{
				StabilizationWindowSeconds: tc.stabilizationSeconds,
				SelectPolicy:               tc.selectPolicy,
			}
			// 只有用例中显式给出非零值时才构造对应策略，模拟用户只填了部分字段。
			if tc.rateDownPods != 0 || tc.rateDownPodsPeriodSeconds != 0 {
				scaleDownRules.Policies = append(scaleDownRules.Policies, autoscalingv2.HPAScalingPolicy{
					Type: autoscalingv2.PodsScalingPolicy, Value: tc.rateDownPods, PeriodSeconds: tc.rateDownPodsPeriodSeconds,
				})
			}
			if tc.rateDownPercent != 0 || tc.rateDownPercentPeriodSeconds != 0 {
				scaleDownRules.Policies = append(scaleDownRules.Policies, autoscalingv2.HPAScalingPolicy{
					Type: autoscalingv2.PercentScalingPolicy, Value: tc.rateDownPercent, PeriodSeconds: tc.rateDownPercentPeriodSeconds,
				})
			}
			down := GenerateHPAScaleDownRules(scaleDownRules)
			assert.EqualValues(t, tc.expectedPolicies, down.Policies)
			// 稳定窗口为 nil 是合法结果（沿用命令行配置），不能无条件解引用。
			if tc.expectedStabilization != nil {
				assert.Equal(t, *tc.expectedStabilization, *down.StabilizationWindowSeconds)
			} else {
				assert.Equal(t, tc.expectedStabilization, down.StabilizationWindowSeconds)
			}
			assert.Equal(t, autoscalingv2.ScalingPolicySelect(tc.expectedSelectPolicy), *down.SelectPolicy)
		})
	}
}

// TestGenerateScaleUpRules 用表驱动方式验证扩容规则默认值：
// 覆盖全默认、全字段指定、仅某一种策略、指定稳定窗口以及指定容差等组合，
// 确认默认规则被完整补齐且显式配置优先。
func TestGenerateScaleUpRules(t *testing.T) {
	// TestCase 描述一组扩容规则输入及其期望的合并结果。
	type TestCase struct {
		rateUpPods                 int32
		rateUpPodsPeriodSeconds    int32
		rateUpPercent              int32
		rateUpPercentPeriodSeconds int32
		stabilizationSeconds       *int32
		selectPolicy               *autoscalingv2.ScalingPolicySelect
		tolerance                  *resource.Quantity

		expectedPolicies      []autoscalingv2.HPAScalingPolicy
		expectedStabilization *int32
		expectedSelectPolicy  string
		expectedTolerance     *resource.Quantity
		annotation            string
	}
	maxPolicy := autoscalingv2.MaxChangePolicySelect
	minPolicy := autoscalingv2.MinChangePolicySelect
	sampleTolerance := resource.MustParse("0.5")
	// 默认用例期望两条默认策略；其余用例分别验证单策略、稳定窗口和容差的覆盖行为。
	tests := []TestCase{
		{
			annotation: "Default values",
			expectedPolicies: []autoscalingv2.HPAScalingPolicy{
				{Type: autoscalingv2.PodsScalingPolicy, Value: 4, PeriodSeconds: 15},
				{Type: autoscalingv2.PercentScalingPolicy, Value: 100, PeriodSeconds: 15},
			},
			expectedStabilization: ptr.To[int32](0),
			expectedSelectPolicy:  string(autoscalingv2.MaxChangePolicySelect),
		},
		{
			annotation:                 "All parameters are specified",
			rateUpPods:                 1,
			rateUpPodsPeriodSeconds:    2,
			rateUpPercent:              3,
			rateUpPercentPeriodSeconds: 4,
			stabilizationSeconds:       ptr.To[int32](25),
			selectPolicy:               &maxPolicy,
			expectedPolicies: []autoscalingv2.HPAScalingPolicy{
				{Type: autoscalingv2.PodsScalingPolicy, Value: 1, PeriodSeconds: 2},
				{Type: autoscalingv2.PercentScalingPolicy, Value: 3, PeriodSeconds: 4},
			},
			expectedStabilization: ptr.To[int32](25),
			expectedSelectPolicy:  string(autoscalingv2.MaxChangePolicySelect),
		},
		{
			annotation:              "Pod policy is specified",
			rateUpPods:              1,
			rateUpPodsPeriodSeconds: 2,
			selectPolicy:            &minPolicy,
			expectedPolicies: []autoscalingv2.HPAScalingPolicy{
				{Type: autoscalingv2.PodsScalingPolicy, Value: 1, PeriodSeconds: 2},
			},
			expectedStabilization: ptr.To[int32](0),
			expectedSelectPolicy:  string(autoscalingv2.MinChangePolicySelect),
		},
		{
			annotation:                 "Percent policy is specified",
			rateUpPercent:              7,
			rateUpPercentPeriodSeconds: 10,
			expectedPolicies: []autoscalingv2.HPAScalingPolicy{
				{Type: autoscalingv2.PercentScalingPolicy, Value: 7, PeriodSeconds: 10},
			},
			expectedStabilization: ptr.To[int32](0),
			expectedSelectPolicy:  string(autoscalingv2.MaxChangePolicySelect),
		},
		{
			annotation:              "Pod policy and stabilization window are specified",
			rateUpPodsPeriodSeconds: 2,
			stabilizationSeconds:    ptr.To[int32](25),
			rateUpPods:              4,
			expectedPolicies: []autoscalingv2.HPAScalingPolicy{
				{Type: autoscalingv2.PodsScalingPolicy, Value: 4, PeriodSeconds: 2},
			},
			expectedStabilization: ptr.To[int32](25),
			expectedSelectPolicy:  string(autoscalingv2.MaxChangePolicySelect),
		},
		{
			annotation:                 "Percent policy and stabilization window are specified",
			rateUpPercent:              7,
			rateUpPercentPeriodSeconds: 60,
			stabilizationSeconds:       ptr.To[int32](25),
			expectedPolicies: []autoscalingv2.HPAScalingPolicy{
				{Type: autoscalingv2.PercentScalingPolicy, Value: 7, PeriodSeconds: 60},
			},
			expectedStabilization: ptr.To[int32](25),
			expectedSelectPolicy:  string(autoscalingv2.MaxChangePolicySelect),
		},
		{
			annotation:                 "Percent policy and tolerance is specified",
			rateUpPercent:              7,
			rateUpPercentPeriodSeconds: 10,
			tolerance:                  &sampleTolerance,
			expectedPolicies: []autoscalingv2.HPAScalingPolicy{
				{Type: autoscalingv2.PercentScalingPolicy, Value: 7, PeriodSeconds: 10},
			},
			expectedStabilization: ptr.To[int32](0),
			expectedSelectPolicy:  string(autoscalingv2.MaxChangePolicySelect),
			expectedTolerance:     &sampleTolerance,
		},
	}
	for _, tc := range tests {
		t.Run(tc.annotation, func(t *testing.T) {
			scaleUpRules := &autoscalingv2.HPAScalingRules{
				StabilizationWindowSeconds: tc.stabilizationSeconds,
				SelectPolicy:               tc.selectPolicy,
				Tolerance:                  tc.tolerance,
			}
			// 仅对显式给出的非零值构造策略，未给出的部分交给默认值补齐。
			if tc.rateUpPods != 0 || tc.rateUpPodsPeriodSeconds != 0 {
				scaleUpRules.Policies = append(scaleUpRules.Policies, autoscalingv2.HPAScalingPolicy{
					Type: autoscalingv2.PodsScalingPolicy, Value: tc.rateUpPods, PeriodSeconds: tc.rateUpPodsPeriodSeconds,
				})
			}
			if tc.rateUpPercent != 0 || tc.rateUpPercentPeriodSeconds != 0 {
				scaleUpRules.Policies = append(scaleUpRules.Policies, autoscalingv2.HPAScalingPolicy{
					Type: autoscalingv2.PercentScalingPolicy, Value: tc.rateUpPercent, PeriodSeconds: tc.rateUpPercentPeriodSeconds,
				})
			}
			up := GenerateHPAScaleUpRules(scaleUpRules)
			assert.Equal(t, tc.expectedPolicies, up.Policies)
			// 扩容默认稳定窗口为 0，但用例仍允许期望 nil，需区分处理避免误Panic。
			if tc.expectedStabilization != nil {
				assert.Equal(t, *tc.expectedStabilization, *up.StabilizationWindowSeconds)
			} else {
				assert.Equal(t, tc.expectedStabilization, up.StabilizationWindowSeconds)
			}

			assert.Equal(t, autoscalingv2.ScalingPolicySelect(tc.expectedSelectPolicy), *up.SelectPolicy)
			assert.Equal(t, tc.expectedTolerance, up.Tolerance)
		})
	}
}

// TestSetBehaviorDefaults 验证 SetDefaults_HorizontalPodAutoscalerBehavior：
// Behavior 为 nil 时不生成默认规则；非 nil 时为未指定的方向补齐默认规则，
// 同时保留用户设置的稳定窗口、策略和容差。
func TestSetBehaviorDefaults(t *testing.T) {
	sampleTolerance := resource.MustParse("0.5")
	maxPolicy := autoscalingv2.MaxChangePolicySelect
	policies := []autoscalingv2.HPAScalingPolicy{
		{Type: autoscalingv2.PercentScalingPolicy, Value: 7, PeriodSeconds: 10},
	}
	// TestCase 描述输入 Behavior 与期望得到的完整 Behavior。
	type TestCase struct {
		behavior         *autoscalingv2.HorizontalPodAutoscalerBehavior
		expectedBehavior *autoscalingv2.HorizontalPodAutoscalerBehavior
		annotation       string
	}

	// 三个用例分别覆盖 nil Behavior、仅设置扩容的稳定窗口和容差、仅设置缩容策略。
	tests := []TestCase{
		{
			annotation:       "Nil behavior",
			behavior:         nil,
			expectedBehavior: nil,
		},
		{
			annotation: "Behavior with stabilizationWindowSeconds and tolerance",
			behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{
				ScaleUp: &autoscalingv2.HPAScalingRules{
					StabilizationWindowSeconds: ptr.To[int32](100),
					Tolerance:                  &sampleTolerance,
				},
			},
			expectedBehavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{
				ScaleDown: &autoscalingv2.HPAScalingRules{
					SelectPolicy: &maxPolicy,
					Policies: []autoscalingv2.HPAScalingPolicy{
						{Type: autoscalingv2.PercentScalingPolicy, Value: 100, PeriodSeconds: 15},
					},
				},
				ScaleUp: &autoscalingv2.HPAScalingRules{
					StabilizationWindowSeconds: ptr.To[int32](100),
					SelectPolicy:               &maxPolicy,
					Policies: []autoscalingv2.HPAScalingPolicy{
						{Type: autoscalingv2.PodsScalingPolicy, Value: 4, PeriodSeconds: 15},
						{Type: autoscalingv2.PercentScalingPolicy, Value: 100, PeriodSeconds: 15},
					},
					Tolerance: &sampleTolerance,
				},
			},
		},
		{
			annotation: "Behavior with policy, without tolerance",
			behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{
				ScaleDown: &autoscalingv2.HPAScalingRules{
					Policies: policies,
				},
			},
			expectedBehavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{
				ScaleDown: &autoscalingv2.HPAScalingRules{
					SelectPolicy: &maxPolicy,
					Policies:     policies,
				},
				ScaleUp: &autoscalingv2.HPAScalingRules{
					StabilizationWindowSeconds: ptr.To[int32](0),
					SelectPolicy:               &maxPolicy,
					Policies: []autoscalingv2.HPAScalingPolicy{
						{Type: autoscalingv2.PodsScalingPolicy, Value: 4, PeriodSeconds: 15},
						{Type: autoscalingv2.PercentScalingPolicy, Value: 100, PeriodSeconds: 15},
					},
				},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.annotation, func(t *testing.T) {
			hpa := autoscalingv2.HorizontalPodAutoscaler{
				Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
					Behavior: tc.behavior,
				},
			}
			expectedHPA := autoscalingv2.HorizontalPodAutoscaler{
				Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
					Behavior: tc.expectedBehavior,
				},
			}
			SetDefaults_HorizontalPodAutoscalerBehavior(&hpa)
			assert.Equal(t, expectedHPA, hpa)
		})
	}
}

// TestSetBehaviorDefaultsConfigurableToleranceEnabled 验证启用 HPAConfigurableTolerance
// 特性门控后，用户未指定容差时默认规则不会擅自填充容差字段。
func TestSetBehaviorDefaultsConfigurableToleranceEnabled(t *testing.T) {
	// 打开 HPAConfigurableTolerance 特性门控。
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.HPAConfigurableTolerance, true)

	// 构造期望值时不设置 Tolerance，验证默认逻辑也不会填充它。
	maxPolicy := autoscalingv2.MaxChangePolicySelect

	hpa := autoscalingv2.HorizontalPodAutoscaler{
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			Behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{
				ScaleUp: &autoscalingv2.HPAScalingRules{
					StabilizationWindowSeconds: ptr.To[int32](100),
				},
			},
		},
	}

	expectedHPA := autoscalingv2.HorizontalPodAutoscaler{
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			Behavior: &autoscalingv2.HorizontalPodAutoscalerBehavior{
				ScaleDown: &autoscalingv2.HPAScalingRules{
					SelectPolicy: &maxPolicy,
					Policies: []autoscalingv2.HPAScalingPolicy{
						{Type: autoscalingv2.PercentScalingPolicy, Value: 100, PeriodSeconds: 15},
					},
				},
				ScaleUp: &autoscalingv2.HPAScalingRules{
					StabilizationWindowSeconds: ptr.To[int32](100),
					SelectPolicy:               &maxPolicy,
					Policies: []autoscalingv2.HPAScalingPolicy{
						{Type: autoscalingv2.PodsScalingPolicy, Value: 4, PeriodSeconds: 15},
						{Type: autoscalingv2.PercentScalingPolicy, Value: 100, PeriodSeconds: 15},
					},
				},
			},
		},
	}

	SetDefaults_HorizontalPodAutoscalerBehavior(&hpa)
	assert.Equal(t, expectedHPA, hpa)
}

// TestHorizontalPodAutoscalerAnnotations 验证 v2 <-> internal 往返转换后，
// 用于跨版本保存字段的注解都被清掉。用例分别使用空值、随机字符串和空数组作为注解值。
func TestHorizontalPodAutoscalerAnnotations(t *testing.T) {
	tests := []struct {
		hpa  autoscalingv2.HorizontalPodAutoscaler
		test string
	}{
		{
			hpa: autoscalingv2.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						autoscaling.HorizontalPodAutoscalerConditionsAnnotation: "",
						autoscaling.MetricSpecsAnnotation:                       "",
						autoscaling.BehaviorSpecsAnnotation:                     "",
						autoscaling.MetricStatusesAnnotation:                    "",
					},
				},
			},
			test: "test empty value for Annotations",
		},
		{
			hpa: autoscalingv2.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						autoscaling.HorizontalPodAutoscalerConditionsAnnotation: "abc",
						autoscaling.MetricSpecsAnnotation:                       "abc",
						autoscaling.BehaviorSpecsAnnotation:                     "abc",
						autoscaling.MetricStatusesAnnotation:                    "abc",
					},
				},
			},
			test: "test random value for Annotations",
		},
		{
			hpa: autoscalingv2.HorizontalPodAutoscaler{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						autoscaling.HorizontalPodAutoscalerConditionsAnnotation: "[]",
						autoscaling.MetricSpecsAnnotation:                       "[]",
						autoscaling.BehaviorSpecsAnnotation:                     "[]",
						autoscaling.MetricStatusesAnnotation:                    "[]",
					},
				},
			},
			test: "test empty array value for Annotations",
		},
	}

	for _, test := range tests {
		hpa := &test.hpa
		// 先拷贝一份，用于确认往返转换不会修改输入对象。
		hpaBeforeMuatate := *hpa.DeepCopy()
		obj := roundTrip(t, runtime.Object(hpa))
		final_obj, ok := obj.(*autoscalingv2.HorizontalPodAutoscaler)
		if !ok {
			t.Fatalf("unexpected object: %v", obj)
		}
		if !reflect.DeepEqual(*hpa, hpaBeforeMuatate) {
			t.Errorf("diff: %v", cmp.Diff(*hpa, hpaBeforeMuatate))
			t.Errorf("expected: %#v\n actual:   %#v", *hpa, hpaBeforeMuatate)
		}

		if len(final_obj.ObjectMeta.Annotations) != 0 {
			t.Fatalf("unexpected annotations: %v", final_obj.ObjectMeta.Annotations)
		}
	}
}

// roundTrip 把对象按 v2 编码，再用通用解码器解码并转换成同类型对象，
// 从而在测试里完整走一遍默认化和转换流程。
func roundTrip(t *testing.T, obj runtime.Object) runtime.Object {
	data, err := runtime.Encode(legacyscheme.Codecs.LegacyCodec(SchemeGroupVersion), obj)
	if err != nil {
		t.Errorf("%v\n %#v", err, obj)
		return nil
	}
	obj2, err := runtime.Decode(legacyscheme.Codecs.UniversalDecoder(), data)
	if err != nil {
		t.Errorf("%v\nData: %s\nSource: %#v", err, string(data), obj)
		return nil
	}
	// 新建一个同类型的空对象作为转换目标，避免复用输入对象掩盖问题。
	obj3 := reflect.New(reflect.TypeOf(obj).Elem()).Interface().(runtime.Object)
	err = legacyscheme.Scheme.Convert(obj2, obj3, nil)
	if err != nil {
		t.Errorf("%v\nSource: %#v", err, obj2)
		return nil
	}
	return obj3
}
