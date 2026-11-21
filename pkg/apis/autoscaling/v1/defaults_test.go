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

package v1_test

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
	_ "k8s.io/kubernetes/pkg/apis/autoscaling/install"
	. "k8s.io/kubernetes/pkg/apis/autoscaling/v1"
	_ "k8s.io/kubernetes/pkg/apis/core/install"
	"k8s.io/utils/ptr"
)

// TestSetDefaultHPA 验证 v1 HPA 的 MinReplicas 默认值：
// 未指定时补为 1，已指定时保留原值。通过编码-解码-转换的往返流程触发默认值逻辑。
func TestSetDefaultHPA(t *testing.T) {
	tests := []struct {
		hpa            autoscalingv1.HorizontalPodAutoscaler
		expectReplicas int32
		test           string
	}{
		{
			hpa:            autoscalingv1.HorizontalPodAutoscaler{},
			expectReplicas: 1,
			test:           "unspecified min replicas, use the default value",
		},
		{
			hpa: autoscalingv1.HorizontalPodAutoscaler{
				Spec: autoscalingv1.HorizontalPodAutoscalerSpec{
					MinReplicas: ptr.To[int32](3),
				},
			},
			expectReplicas: 3,
			test:           "set min replicas to 3",
		},
	}

	// 两个用例分别覆盖 MinReplicas 未指定和显式指定为 3 的场景。
	for _, test := range tests {
		hpa := &test.hpa
		obj2 := roundTrip(t, runtime.Object(hpa))
		hpa2, ok := obj2.(*autoscalingv1.HorizontalPodAutoscaler)
		if !ok {
			t.Fatalf("unexpected object: %v", obj2)
		}
		if hpa2.Spec.MinReplicas == nil {
			t.Errorf("unexpected nil MinReplicas")
		} else if test.expectReplicas != *hpa2.Spec.MinReplicas {
			t.Errorf("expected: %d MinReplicas, got: %d", test.expectReplicas, *hpa2.Spec.MinReplicas)
		}
	}
}

// TestHorizontalPodAutoscalerAnnotations 验证 v1 <-> internal 往返转换后，
// 用于跨版本保存字段的注解都被清掉。用例分别使用空值、随机字符串和空数组作为注解值。
func TestHorizontalPodAutoscalerAnnotations(t *testing.T) {
	tests := []struct {
		hpa  autoscalingv1.HorizontalPodAutoscaler
		test string
	}{
		{
			hpa: autoscalingv1.HorizontalPodAutoscaler{
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
			hpa: autoscalingv1.HorizontalPodAutoscaler{
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
			hpa: autoscalingv1.HorizontalPodAutoscaler{
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
		final_obj, ok := obj.(*autoscalingv1.HorizontalPodAutoscaler)
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

// roundTrip 把对象按 v1 编码，再用通用解码器解码并转换成同类型对象，
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
