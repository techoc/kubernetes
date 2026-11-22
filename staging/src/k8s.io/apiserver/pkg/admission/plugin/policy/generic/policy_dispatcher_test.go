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

package generic

import (
	"testing"

	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestConvertParamToInformerRepresentation verifies that params fetched with the
// dynamic client fallback are converted to the same representation the param
// informer returns, so that policy evaluation behaves identically regardless of
// which path resolved the param.
//
// 动态客户端回退路径拿到的是 unstructured，而 informer 路径缓存的是具体类型；若不做归一化，
// 同一条策略在两条路径上会看到不同的对象形态（例如 TypeMeta 是否为空），导致 CEL 求值结果不一致。
func TestConvertParamToInformerRepresentation(t *testing.T) {
	tests := []struct {
		name          string
		gvk           schema.GroupVersionKind
		param         *unstructured.Unstructured
		wantObject    runtime.Object
		wantUnchanged bool
		wantErr       bool
	}{
		{
			// 已知类型应被解码成具体类型，且 TypeMeta 清空，以对齐 client-go 缓存的形态。
			name: "typed kind converts to typed object with empty TypeMeta",
			gvk:  schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"},
			param: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata": map[string]interface{}{
						"name":      "test-param",
						"namespace": "default",
					},
					"data": map[string]interface{}{
						"maxReplicas": "3",
					},
				},
			},
			// Typed informers cache objects decoded by client-go, which have an empty TypeMeta.
			wantObject: &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-param",
					Namespace: "default",
				},
				Data: map[string]string{
					"maxReplicas": "3",
				},
			},
		},
		{
			// 未知类型（如 CRD 未注册）无法解码，应原样保留 unstructured。
			name: "unknown kind stays unstructured",
			gvk:  schema.GroupVersionKind{Group: "example.com", Version: "v1", Kind: "TestParam"},
			param: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "example.com/v1",
					"kind":       "TestParam",
					"metadata": map[string]interface{}{
						"name":      "test-param",
						"namespace": "default",
					},
					"spec": map[string]interface{}{
						"maxReplicas": int64(3),
					},
				},
			},
			wantUnchanged: true,
		},
		{
			// 声明为已知类型但字段结构不匹配时必须报错，不能静默降级成 unstructured。
			name: "typed kind with mismatched data errors",
			gvk:  schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"},
			param: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "ConfigMap",
					"metadata": map[string]interface{}{
						"name": "test-param",
					},
					"data": "not-a-map",
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			converted, err := convertParamToInformerRepresentation(tt.gvk, tt.param)
			if tt.wantErr {
				require.Error(t, err, "expected conversion error for malformed data")
				return
			}
			require.NoError(t, err, "unexpected error during conversion")
			if tt.wantUnchanged {
				// 用 Same 而非 Equal：必须是同一个指针，证明没有发生无谓的拷贝/重建。
				require.Same(t, tt.param, converted, "expected the unstructured param to be returned unchanged")
				return
			}
			require.Equal(t, tt.wantObject, converted)
		})
	}
}
