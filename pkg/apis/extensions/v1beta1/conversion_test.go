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

package v1beta1

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/api/extensions/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/kubernetes/pkg/apis/networking"
)

// TestIngressBackendConversion 校验 extensions/v1beta1 与内部 Ingress 后端表示的双向转换。
func TestIngressBackendConversion(t *testing.T) {
	scheme := runtime.NewScheme()
	assert.NoError(t, RegisterConversions(scheme))

	// 用例覆盖数字端口、命名端口与空服务名等历史表示。
	tests := map[string]struct {
		external v1beta1.IngressSpec
		internal networking.IngressSpec
	}{
		// 数字端口应转换为 ServiceBackendPort.Number。
		"service-port-number": {
			external: v1beta1.IngressSpec{
				Backend: &v1beta1.IngressBackend{
					ServiceName: "test-backend",
					ServicePort: intstr.FromInt32(8080),
				},
			},
			internal: networking.IngressSpec{
				DefaultBackend: &networking.IngressBackend{
					Service: &networking.IngressServiceBackend{
						Name: "test-backend",
						Port: networking.ServiceBackendPort{
							Name:   "",
							Number: 8080,
						},
					},
				},
			},
		},
		// 命名端口应转换为 ServiceBackendPort.Name。
		"service-named-port": {
			external: v1beta1.IngressSpec{
				Backend: &v1beta1.IngressBackend{
					ServiceName: "test-backend",
					ServicePort: intstr.FromString("https"),
				},
			},
			internal: networking.IngressSpec{
				DefaultBackend: &networking.IngressBackend{
					Service: &networking.IngressServiceBackend{
						Name: "test-backend",
						Port: networking.ServiceBackendPort{
							Name: "https",
						},
					},
				},
			},
		},
		// 空服务名是合法输入，转换时不应额外补默认值。
		"empty-service-name": {
			external: v1beta1.IngressSpec{
				Backend: &v1beta1.IngressBackend{
					ServiceName: "",
					ServicePort: intstr.FromString("https"),
				},
			},
			internal: networking.IngressSpec{
				DefaultBackend: &networking.IngressBackend{
					Service: &networking.IngressServiceBackend{
						Name: "",
						Port: networking.ServiceBackendPort{
							Name: "https",
						},
					},
				},
			},
		},
	}

	// 每个用例都做双向转换，确保往返不丢失信息。
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			convertedInternal := networking.IngressSpec{}
			require.NoError(t,
				Convert_v1beta1_IngressSpec_To_networking_IngressSpec(&test.external, &convertedInternal, nil))
			assert.Equal(t, test.internal, convertedInternal, "v1beta1.IngressSpec -> networking.IngressSpec")

			// 反向转换结果应与原始外部对象完全一致。
			convertedV1beta1 := v1beta1.IngressSpec{}
			require.NoError(t,
				Convert_networking_IngressSpec_To_v1beta1_IngressSpec(&test.internal, &convertedV1beta1, nil))
			assert.Equal(t, test.external, convertedV1beta1, "networking.IngressSpec -> v1beta1.IngressSpec")
		})
	}
}
