/*
Copyright 2020 The Kubernetes Authors.

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

package kuberuntime

import (
	"testing"

	"github.com/stretchr/testify/assert"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubernetes/pkg/features"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

// TestConvertToKubeContainerImageSpec 验证 CRI Image 到 kubecontainer.ImageSpec 的转换：
// nil/空注解都应归一为空注解切片，非空注解按名称和值保留。
func TestConvertToKubeContainerImageSpec(t *testing.T) {
	testCases := []struct {
		input    *runtimeapi.Image
		expected kubecontainer.ImageSpec
	}{
		{
			input: &runtimeapi.Image{
				Id:   "test",
				Spec: nil,
			},
			expected: kubecontainer.ImageSpec{
				Image:       "test",
				Annotations: []kubecontainer.Annotation(nil),
			},
		},
		{
			input: &runtimeapi.Image{
				Id: "test",
				Spec: &runtimeapi.ImageSpec{
					Annotations: nil,
				},
			},
			expected: kubecontainer.ImageSpec{
				Image:       "test",
				Annotations: []kubecontainer.Annotation(nil),
			},
		},
		{
			input: &runtimeapi.Image{
				Id: "test",
				Spec: &runtimeapi.ImageSpec{
					Annotations: map[string]string{},
				},
			},
			expected: kubecontainer.ImageSpec{
				Image:       "test",
				Annotations: []kubecontainer.Annotation(nil),
			},
		},
		{
			input: &runtimeapi.Image{
				Id: "test",
				Spec: &runtimeapi.ImageSpec{
					Annotations: map[string]string{
						"kubernetes.io/os":             "linux",
						"kubernetes.io/runtimehandler": "handler",
					},
				},
			},
			expected: kubecontainer.ImageSpec{
				Image: "test",
				Annotations: []kubecontainer.Annotation{
					{
						Name:  "kubernetes.io/os",
						Value: "linux",
					},
					{
						Name:  "kubernetes.io/runtimehandler",
						Value: "handler",
					},
				},
			},
		},
	}

	for _, test := range testCases {
		actual := ToKubeContainerImageSpec(test.input)
		assert.Equal(t, test.expected, actual)
	}
}

// TestConvertToRuntimeAPIImageSpec 验证反向转换：注解切片转为 map，空注解转为空 map。
func TestConvertToRuntimeAPIImageSpec(t *testing.T) {
	testCases := []struct {
		input    kubecontainer.ImageSpec
		expected *runtimeapi.ImageSpec
	}{
		{
			input: kubecontainer.ImageSpec{
				Image:          "test",
				RuntimeHandler: "",
				Annotations:    nil,
			},
			expected: &runtimeapi.ImageSpec{
				Image:          "test",
				RuntimeHandler: "",
				Annotations:    map[string]string{},
			},
		},
		{
			input: kubecontainer.ImageSpec{
				Image:          "test",
				RuntimeHandler: "",
				Annotations:    []kubecontainer.Annotation{},
			},
			expected: &runtimeapi.ImageSpec{
				Image:          "test",
				RuntimeHandler: "",
				Annotations:    map[string]string{},
			},
		},
		{
			input: kubecontainer.ImageSpec{
				Image:          "test",
				RuntimeHandler: "",
				Annotations: []kubecontainer.Annotation{
					{
						Name:  "kubernetes.io/os",
						Value: "linux",
					},
					{
						Name:  "kubernetes.io/runtimehandler",
						Value: "handler",
					},
				},
			},
			expected: &runtimeapi.ImageSpec{
				Image:          "test",
				RuntimeHandler: "",
				Annotations: map[string]string{
					"kubernetes.io/os":             "linux",
					"kubernetes.io/runtimehandler": "handler",
				},
			},
		},
	}

	for _, test := range testCases {
		actual := ToRuntimeAPIImageSpec(test.input)
		assert.Equal(t, test.expected, actual)
	}
}

// TestConvertToKubeContainerImageSpecWithRuntimeHandlerInImageSpecCri 验证
// RuntimeClassInImageCriAPI 门控开启时，CRI ImageSpec 的 RuntimeHandler 会被保留。
func TestConvertToKubeContainerImageSpecWithRuntimeHandlerInImageSpecCri(t *testing.T) {
	testCases := []struct {
		input    *runtimeapi.Image
		expected kubecontainer.ImageSpec
	}{
		{
			input: &runtimeapi.Image{
				Id:   "test",
				Spec: nil,
			},
			expected: kubecontainer.ImageSpec{
				Image:          "test",
				RuntimeHandler: "",
				Annotations:    []kubecontainer.Annotation(nil),
			},
		},
		{
			input: &runtimeapi.Image{
				Id: "test",
				Spec: &runtimeapi.ImageSpec{
					Annotations: nil,
				},
			},
			expected: kubecontainer.ImageSpec{
				Image:          "test",
				RuntimeHandler: "",
				Annotations:    []kubecontainer.Annotation(nil),
			},
		},
		{
			input: &runtimeapi.Image{
				Id: "test",
				Spec: &runtimeapi.ImageSpec{
					Annotations: map[string]string{},
				},
			},
			expected: kubecontainer.ImageSpec{
				Image:          "test",
				RuntimeHandler: "",
				Annotations:    []kubecontainer.Annotation(nil),
			},
		},
		{
			input: &runtimeapi.Image{
				Id: "test",
				Spec: &runtimeapi.ImageSpec{
					RuntimeHandler: "test-runtimeHandler",
					Annotations: map[string]string{
						"kubernetes.io/os":             "linux",
						"kubernetes.io/runtimehandler": "handler",
					},
				},
			},
			expected: kubecontainer.ImageSpec{
				Image:          "test",
				RuntimeHandler: "test-runtimeHandler",
				Annotations: []kubecontainer.Annotation{
					{
						Name:  "kubernetes.io/os",
						Value: "linux",
					},
					{
						Name:  "kubernetes.io/runtimehandler",
						Value: "handler",
					},
				},
			},
		},
	}

	for _, test := range testCases {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.RuntimeClassInImageCriAPI, true)
		actual := ToKubeContainerImageSpec(test.input)
		assert.Equal(t, test.expected, actual)
	}
}

// TestConvertToRuntimeAPIImageSpecWithRuntimeHandlerInImageSpecCri 验证反向转换时
// RuntimeHandler 同样会写入 CRI ImageSpec。
func TestConvertToRuntimeAPIImageSpecWithRuntimeHandlerInImageSpecCri(t *testing.T) {
	testCases := []struct {
		input    kubecontainer.ImageSpec
		expected *runtimeapi.ImageSpec
	}{
		{
			input: kubecontainer.ImageSpec{
				Image:          "test",
				RuntimeHandler: "",
				Annotations:    nil,
			},
			expected: &runtimeapi.ImageSpec{
				Image:          "test",
				RuntimeHandler: "",
				Annotations:    map[string]string{},
			},
		},
		{
			input: kubecontainer.ImageSpec{
				Image:          "test",
				RuntimeHandler: "",
				Annotations:    []kubecontainer.Annotation{},
			},
			expected: &runtimeapi.ImageSpec{
				Image:          "test",
				RuntimeHandler: "",
				Annotations:    map[string]string{},
			},
		},
		{
			input: kubecontainer.ImageSpec{
				Image:          "test",
				RuntimeHandler: "test-runtimeHandler",
				Annotations: []kubecontainer.Annotation{
					{
						Name:  "kubernetes.io/os",
						Value: "linux",
					},
					{
						Name:  "kubernetes.io/runtimehandler",
						Value: "handler",
					},
				},
			},
			expected: &runtimeapi.ImageSpec{
				Image:          "test",
				RuntimeHandler: "test-runtimeHandler",
				Annotations: map[string]string{
					"kubernetes.io/os":             "linux",
					"kubernetes.io/runtimehandler": "handler",
				},
			},
		},
	}

	for _, test := range testCases {
		featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.RuntimeClassInImageCriAPI, true)
		actual := ToRuntimeAPIImageSpec(test.input)
		assert.Equal(t, test.expected, actual)
	}
}
