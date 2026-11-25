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

package fieldmanager_test

import (
	"context"
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/endpoints/handlers/fieldmanager"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// TestAdmission 验证 managedFields 校验准入控制器会丢弃非法的 ManagedFieldsEntry。
//
// 服务端字段管理把 managedFields 视为客户端可写但不完全可信的数据；
// 该控制器在准入阶段做「读-校验-必要时重置」：一旦发现条目不符合规范
// （APIVersion/Operation/FieldsType/FieldsV1 非法），就整条移除，避免脏数据落库。
// 注意 manager 为空并不触发重置——空 manager 在历史数据中合法，这是兼容性约束。
func TestAdmission(t *testing.T) {
	wrap := &mockAdmissionController{}
	ac := fieldmanager.NewManagedFieldsValidatingAdmissionController(wrap)
	now := metav1.Now()

	// 构造一条完全合法的 FieldsV1：路径为 metadata.labels["test-label"]。
	validFieldsV1 := metav1.FieldsV1{}
	raw, err := fieldpath.NewSet(fieldpath.MakePathOrDie("metadata", "labels", "test-label")).ToJSON()
	if err != nil {
		t.Fatal(err)
	}
	validFieldsV1.SetRawBytes(raw)
	validManagedFieldsEntry := metav1.ManagedFieldsEntry{
		APIVersion: "v1",
		Operation:  metav1.ManagedFieldsOperationApply,
		Time:       &now,
		Manager:    "test",
		FieldsType: "FieldsV1",
		FieldsV1:   &validFieldsV1,
	}

	// 每个 mutator 注入一种非法形态；shouldReset 表示期望控制器清除该条目。
	managedFieldsMutators := map[string]func(in metav1.ManagedFieldsEntry) (out metav1.ManagedFieldsEntry, shouldReset bool){
		"invalid APIVersion": func(managedFields metav1.ManagedFieldsEntry) (metav1.ManagedFieldsEntry, bool) {
			managedFields.APIVersion = ""
			return managedFields, true
		},
		"invalid Operation": func(managedFields metav1.ManagedFieldsEntry) (metav1.ManagedFieldsEntry, bool) {
			managedFields.Operation = "invalid operation"
			return managedFields, true
		},
		"invalid fieldsType": func(managedFields metav1.ManagedFieldsEntry) (metav1.ManagedFieldsEntry, bool) {
			managedFields.FieldsType = "invalid fieldsType"
			return managedFields, true
		},
		"invalid fieldsV1": func(managedFields metav1.ManagedFieldsEntry) (metav1.ManagedFieldsEntry, bool) {
			managedFields.FieldsV1 = metav1.NewFieldsV1("{invalid}")
			return managedFields, true
		},
		"invalid manager": func(managedFields metav1.ManagedFieldsEntry) (metav1.ManagedFieldsEntry, bool) {
			managedFields.Manager = ""
			return managedFields, false
		},
	}

	for name, mutate := range managedFieldsMutators {
		t.Run(name, func(t *testing.T) {
			mutated, shouldReset := mutate(validManagedFieldsEntry)
			validEntries := []metav1.ManagedFieldsEntry{validManagedFieldsEntry}
			mutatedEntries := []metav1.ManagedFieldsEntry{mutated}

			obj := &v1.ConfigMap{}
			obj.SetManagedFields(validEntries)

			// 让被包装的控制器在准入时把 managedFields 替换为非法版本，
			// 模拟客户端提交了伪造数据。
			wrap.admit = replaceManagedFields(mutatedEntries)

			attrs := admission.NewAttributesRecord(obj, obj, schema.GroupVersionKind{}, "default", "", schema.GroupVersionResource{}, "", admission.Update, nil, false, nil)
			if err := ac.(admission.MutationInterface).Admit(context.TODO(), attrs, nil); err != nil {
				t.Fatal(err)
			}

			// 期望重置时对象应保留原始合法条目；期望保留时则应保留伪造条目。
			if shouldReset && !reflect.DeepEqual(obj.GetManagedFields(), validEntries) {
				t.Fatalf("expected: \n%v\ngot:\n%v", validEntries, obj.GetManagedFields())
			}
			if !shouldReset && reflect.DeepEqual(obj.GetManagedFields(), validEntries) {
				t.Fatalf("expected: \n%v\ngot:\n%v", mutatedEntries, obj.GetManagedFields())
			}
		})
	}
}

// replaceManagedFields 返回一个准入函数，把对象的 managedFields 整体替换为给定值。
// 用于模拟「客户端提交非法 managedFields」这一输入形态，
// 而不必真的构造完整的 API 请求。
func replaceManagedFields(with []metav1.ManagedFieldsEntry) func(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	return func(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
		objectMeta, err := meta.Accessor(a.GetObject())
		if err != nil {
			return err
		}
		objectMeta.SetManagedFields(with)
		return nil
	}
}

// mockAdmissionController 是一个最小准入控制器，行为由 admit 字段注入。
type mockAdmissionController struct {
	admit func(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error
}

// Handles 恒为 true：测试中需要该控制器对所有操作生效。
func (c *mockAdmissionController) Handles(operation admission.Operation) bool {
	return true
}

// Admit 委托给注入的函数，便于每个用例自定义行为。
func (c *mockAdmissionController) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	return c.admit(ctx, a, o)
}
