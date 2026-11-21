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

package v2

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
	v1 "k8s.io/kubernetes/pkg/apis/autoscaling/v1"
	"k8s.io/kubernetes/pkg/apis/autoscaling/validation"
	"k8s.io/utils/ptr"
)

// 该测试确保带 averageValue 的 Object 指标 HPA 在 v2 -> v1 -> v2 往返转换后，
// averageValue 不会被误设回零导致校验失败。v1 的非指针 value 字段在 round-trip 中
// 会序列化成 value:"0"，转换时只保留非零值以避开这一问题。
// TestObjectMetricAverageValue_RoundTripV2V1 覆盖 v2 -> internal -> v1 -> JSON -> internal
// 的往返链路，并在每次转成 internal 后校验对象合法，确认 averageValue 在链路中不丢失。
func TestObjectMetricAverageValue_RoundTripV2V1(t *testing.T) {
	var err error

	// 构造一个 Object 指标目标为非零 averageValue 的 v2 HPA。
	hpaV2 := &autoscalingv2.HorizontalPodAutoscaler{
		TypeMeta: metav1.TypeMeta{
			Kind:       "HorizontalPodAutoscaler",
			APIVersion: "autoscaling/v2",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "my-hpa",
			Namespace: "default",
		},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{
				Kind:       "Deployment",
				Name:       "my-deployment",
				APIVersion: "apps/v1",
			},
			MinReplicas: ptr.To[int32](1),
			MaxReplicas: 3,
			Metrics: []autoscalingv2.MetricSpec{
				{
					Type: autoscalingv2.ObjectMetricSourceType,
					Object: &autoscalingv2.ObjectMetricSource{
						Target: autoscalingv2.MetricTarget{
							Type:         autoscalingv2.AverageValueMetricType,
							AverageValue: ptr.To(resource.MustParse("100")),
						},
						Metric: autoscalingv2.MetricIdentifier{
							Name: "requests-per-second",
						},
						DescribedObject: autoscalingv2.CrossVersionObjectReference{
							Kind:       "Deployment",
							Name:       "my-deployment",
							APIVersion: "apps/v1"},
					},
				},
			},
		},
	}

	// 转成 internal 后先校验一次，确认转换本身没有引入非法字段。
	hpaInternal := &autoscaling.HorizontalPodAutoscaler{}
	err = Convert_v2_HorizontalPodAutoscaler_To_autoscaling_HorizontalPodAutoscaler(hpaV2, hpaInternal, nil)
	assert.NoError(t, err, "Conversion to internal should not fail")

	validationErrors := validation.ValidateHorizontalPodAutoscaler(hpaInternal, validation.HorizontalPodAutoscalerSpecValidationOptions{})
	assert.Zero(t, len(validationErrors), "Validation should not produce errors")

	// 再转成 v1：v1 无法直接表达 averageValue 目标类型，只能靠注解往返保存。
	hpaV1 := &autoscalingv1.HorizontalPodAutoscaler{}
	err = v1.Convert_autoscaling_HorizontalPodAutoscaler_To_v1_HorizontalPodAutoscaler(hpaInternal, hpaV1, nil)
	assert.NoError(t, err, "Conversion to v1 should not fail")

	// 序列化到 JSON 再反序列化，模拟 API 请求/持久化的真实过程。
	jsonData, err := json.Marshal(hpaV1)
	assert.NoError(t, err, "JSON marshalling should not fail")

	// 反序列化到新的 v1 对象，避免复用原对象掩盖问题。
	var hpaV1FromJSON autoscalingv1.HorizontalPodAutoscaler
	err = json.Unmarshal(jsonData, &hpaV1FromJSON)
	assert.NoError(t, err, "JSON unmarshalling should not fail")

	// 从反序列化后的 v1 对象转回 internal，再次校验确认 averageValue 未被置零。
	hpaInternalFromV1 := &autoscaling.HorizontalPodAutoscaler{}
	err = v1.Convert_v1_HorizontalPodAutoscaler_To_autoscaling_HorizontalPodAutoscaler(&hpaV1FromJSON, hpaInternalFromV1, nil)
	assert.NoError(t, err, "Conversion from v1 should not fail")

	validationErrors = validation.ValidateHorizontalPodAutoscaler(hpaInternalFromV1, validation.HorizontalPodAutoscalerSpecValidationOptions{})
	assert.Zero(t, len(validationErrors), "Validation should not produce errors")
}
