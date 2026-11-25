/*
Copyright 2024 The Kubernetes Authors.

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

package patch

import (
	"context"
	"github.com/google/go-cmp/cmp"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/openapi/openapitest"
)

// TestTypeConverter 验证 TypeConverter 的往返一致性：
// ObjectToTyped 再 TypedToObject 后，经 unstructured 比较应与原对象完全相等。
// 若转换器丢失字段（例如未处理某些嵌套结构），这里会以 diff 形式暴露。
func TestTypeConverter(t *testing.T) {
	deploymentGVK := schema.GroupVersionKind{Group: "apps", Version: "v1", Kind: "Deployment"}
	tests := []struct {
		name   string
		gvk    schema.GroupVersionKind
		object runtime.Object
	}{
		{
			// 简单往返：包含一个带端口的多容器 PodSpec，覆盖嵌套 slice/struct。
			name: "simple round trip",
			gvk:  deploymentGVK,
			object: &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "a"}, {Name: "x", Ports: []corev1.ContainerPort{{ContainerPort: 8080}}}},
			}}}},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	// 使用内嵌的 OpenAPI 文件客户端，测试不依赖真实 apiserver。
	tcManager := NewTypeConverterManager(nil, openapitest.NewEmbeddedFileClient())
	go tcManager.Run(ctx)

	// 等待类型转换器异步构建完成，最多 1 秒。
	err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, time.Second, true, func(context.Context) (done bool, err error) {
		converter := tcManager.GetTypeConverter(deploymentGVK)
		return converter != nil, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 显式补齐 TypeMeta，转换器需要 GVK 才能选对 schema。
			typeAccessor, err := meta.TypeAccessor(tc.object)
			if err != nil {
				t.Fatal(err)
			}
			typeAccessor.SetKind(tc.gvk.Kind)
			typeAccessor.SetAPIVersion(tc.gvk.GroupVersion().String())

			converter := tcManager.GetTypeConverter(tc.gvk)
			if converter == nil {
				t.Errorf("nil TypeConverter")
			}
			typedObject, err := converter.ObjectToTyped(tc.object)
			if err != nil {
				t.Fatal(err)
			}

			// 再转回通用对象，应能还原原始结构。
			roundTripped, err := converter.TypedToObject(typedObject)
			if err != nil {
				t.Fatal(err)
			}
			got, err := runtime.DefaultUnstructuredConverter.ToUnstructured(roundTripped)
			if err != nil {
				t.Fatal(err)
			}

			want, err := runtime.DefaultUnstructuredConverter.ToUnstructured(tc.object)
			if err != nil {
				t.Fatal(err)
			}
			if !equality.Semantic.DeepEqual(want, got) {
				t.Errorf("unexpected result, got diff:\n%s\n", cmp.Diff(want, got))
			}
		})
	}
}
