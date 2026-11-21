/*
Copyright 2015 The Kubernetes Authors.

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

package testing

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	podutil "k8s.io/kubernetes/pkg/api/pod"
	"k8s.io/kubernetes/pkg/api/testing/compat"
	api "k8s.io/kubernetes/pkg/apis/core"
	_ "k8s.io/kubernetes/pkg/apis/core/install"
	"k8s.io/kubernetes/pkg/apis/core/validation"
)

// TestCompatibility_v1_PodSecurityContext 验证 v1 Pod 的安全上下文字段与 1.0.0 版本的语义兼容。
// 通过 compat.TestCompatibility 对比新旧校验器产出的错误键，确保新增字段不会改变旧有行为。
func TestCompatibility_v1_PodSecurityContext(t *testing.T) {
	// 每个用例用不同字段的 true/false 取值，覆盖默认值为 false 时字段是否出现在校验错误中的差异。
	cases := []struct {
		name         string
		input        string
		expectedKeys map[string]string
		absentKeys   []string
	}{
		{
			name: "hostNetwork = true",
			input: `
{
	"kind":"Pod",
	"apiVersion":"v1",
	"metadata":{"name":"my-pod-name", "namespace":"my-pod-namespace"},
	"spec": {
		"hostNetwork": true,
		"containers":[{
			"name":"a",
			"image":"my-container-image"
		}]
	}
}
`,
			expectedKeys: map[string]string{
				"spec.hostNetwork": "true",
			},
		},
		{
			name: "hostNetwork = false",
			input: `
{
	"kind":"Pod",
	"apiVersion":"v1",
	"metadata":{"name":"my-pod-name", "namespace":"my-pod-namespace"},
	"spec": {
		"hostNetwork": false,
		"containers":[{
			"name":"a",
			"image":"my-container-image"
		}]
	}
}
`,
			absentKeys: []string{
				"spec.hostNetwork",
			},
		},
		{
			name: "hostIPC = true",
			input: `
{
	"kind":"Pod",
	"apiVersion":"v1",
	"metadata":{"name":"my-pod-name", "namespace":"my-pod-namespace"},
	"spec": {
		"hostIPC": true,
		"containers":[{
			"name":"a",
			"image":"my-container-image"
		}]
	}
}
`,
			expectedKeys: map[string]string{
				"spec.hostIPC": "true",
			},
		},
		{
			name: "hostIPC = false",
			input: `
{
	"kind":"Pod",
	"apiVersion":"v1",
	"metadata":{"name":"my-pod-name", "namespace":"my-pod-namespace"},
	"spec": {
		"hostIPC": false,
		"containers":[{
			"name":"a",
			"image":"my-container-image"
		}]
	}
}
`,
			absentKeys: []string{
				"spec.hostIPC",
			},
		},
		{
			name: "hostPID = true",
			input: `
{
	"kind":"Pod",
	"apiVersion":"v1",
	"metadata":{"name":"my-pod-name", "namespace":"my-pod-namespace"},
	"spec": {
		"hostPID": true,
		"containers":[{
			"name":"a",
			"image":"my-container-image"
		}]
	}
}
`,
			expectedKeys: map[string]string{
				"spec.hostPID": "true",
			},
		},
		{
			name: "hostPID = false",
			input: `
{
	"kind":"Pod",
	"apiVersion":"v1",
	"metadata":{"name":"my-pod-name", "namespace":"my-pod-namespace"},
	"spec": {
		"hostPID": false,
		"containers":[{
			"name":"a",
			"image":"my-container-image"
		}]
	}
}
`,
			absentKeys: []string{
				"spec.hostPID",
			},
		},
	}

	// validator 用当前 internal 类型的校验逻辑，供兼容测试与新版本做对照。
	validator := func(obj runtime.Object) field.ErrorList {
		opts := podutil.GetValidationOptionsFromPodSpecAndMeta(&(obj.(*api.Pod).Spec), nil, &(obj.(*api.Pod).ObjectMeta), nil)
		return validation.ValidatePodSpec(&(obj.(*api.Pod).Spec), &(obj.(*api.Pod).ObjectMeta), field.NewPath("spec"), opts)
	}

	// expectedKeys 必须出现，absentKeys 必须不出现，以此锁定字段默认值的兼容行为。
	for _, tc := range cases {
		t.Logf("Testing 1.0.0 backward compatibility for %v", tc.name)
		compat.TestCompatibility(t, v1.SchemeGroupVersion, []byte(tc.input), validator, tc.expectedKeys, tc.absentKeys)
	}
}
