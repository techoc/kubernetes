/*
Copyright 2019 The Kubernetes Authors.

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

package internal_test

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields/internal"
	internaltesting "k8s.io/apimachinery/pkg/util/managedfields/internal/testing"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// TestFieldManagerUpdateNoErrors 验证 UpdateNoErrors 在底层 Manager 报错时仍保持幂等：
// managedFields 不应因一次失败的更新而变化。
func TestFieldManagerUpdateNoErrors(t *testing.T) {
	// fakeManager 记录底层 Manager，便于注入错误。
	fm := &fakeManager{}
	f := internaltesting.NewTestFieldManagerImpl(fakeTypeConverter, schema.FromAPIVersionAndKind("v1", "Pod"),
		"",
		func(m internal.Manager) internal.Manager {
			fm.Manager = m
			return fm
		})

	// podWithLabels 构造只带 labels 的 Pod。
	podWithLabels := func(labels ...string) runtime.Object {
		labelMap := map[string]interface{}{}
		for _, key := range labels {
			labelMap[key] = "true"
		}
		obj := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"metadata": map[string]interface{}{
					"labels": labelMap,
				},
			},
		}
		obj.SetKind("Pod")
		obj.SetAPIVersion("v1")
		return obj
	}

	f.UpdateNoErrors(podWithLabels("one"), "fieldmanager_test_update_1")
	if len(f.ManagedFields()) == 0 {
		t.Fatalf("expected managedFields to be set, but they are empty")
	}

	// 保存更新后的 managedFields 快照，用于比较后续失败更新是否造成变化。
	before := []metav1.ManagedFieldsEntry{}
	for _, m := range f.ManagedFields() {
		before = append(before, *m.DeepCopy())
	}

	// 注入错误，让 UpdateNoErrors 走错误处理路径。
	fm.Error = errors.New("test error")
	f.UpdateNoErrors(podWithLabels("one", "two"), "fieldmanager_test_update_1")

	if after := f.ManagedFields(); !apiequality.Semantic.DeepEqual(before, after) {
		t.Fatalf("expected idempotence, but managedFields changed:\nbefore: %v\n after: %v", mustMarshal(before), mustMarshal(after))
	}
}

// fakeTypeConverter 从仓库中的 swagger.json 构建真实的 TypeConverter，
// 使测试覆盖结构化 schema 解析；文件缺失或解析失败直接 panic。
var fakeTypeConverter = func() internal.TypeConverter {
	data, err := os.ReadFile(filepath.Join(
		strings.Repeat(".."+string(filepath.Separator), 8),
		"api", "openapi-spec", "swagger.json"))
	if err != nil {
		panic(err)
	}
	convertedDefs := map[string]*spec.Schema{}
	spec := spec.Swagger{}
	if err := json.Unmarshal(data, &spec); err != nil {
		panic(err)
	}

	for k, v := range spec.Definitions {
		vCopy := v
		convertedDefs[k] = &vCopy
	}

	typeConverter, err := internal.NewTypeConverter(convertedDefs, false)
	if err != nil {
		panic(err)
	}
	return typeConverter
}()
