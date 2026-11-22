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

package managedfields

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/structured-merge-diff/v6/typed"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	runtimeschema "k8s.io/apimachinery/pkg/runtime/schema"
)

// TestExtractInto 验证 ExtractInto 能按字段管理者从对象的 managedFields 中
// 还原其拥有的字段：只取匹配 manager（及 subresource）的字段，忽略其他管理者的声明；
// 同时覆盖 unstructured 与结构化对象两种输入。
func TestExtractInto(t *testing.T) {
	one := int32(1)
	parser, err := typed.NewParser(schemaYAML)
	if err != nil {
		t.Fatalf("Failed to parse schema: %v", err)
	}
	cases := []struct {
		name          string
		obj           runtime.Object
		objType       typed.ParseableType
		managedFields []metav1.ManagedFieldsEntry // written to object before test is run
		fieldManager  string
		expectedOut   interface{}
		subresource   string
	}{
		{
			name:    "unstructured, no matching manager",
			obj:     &unstructured.Unstructured{Object: map[string]interface{}{"spec": map[string]interface{}{"replicas": 1}}},
			objType: parser.Type("io.k8s.api.apps.v1.Deployment"),
			managedFields: []metav1.ManagedFieldsEntry{
				applyFieldsEntry("mgr999", `{ "f:spec": { "f:replicas": {}}}`, ""),
			},
			fieldManager: "mgr1",
			expectedOut:  map[string]interface{}{},
		},
		{
			name:    "unstructured, one manager",
			obj:     &unstructured.Unstructured{Object: map[string]interface{}{"spec": map[string]interface{}{"replicas": 1}}},
			objType: parser.Type("io.k8s.api.apps.v1.Deployment"),
			managedFields: []metav1.ManagedFieldsEntry{
				applyFieldsEntry("mgr1", `{ "f:spec": { "f:replicas": {}}}`, ""),
			},
			fieldManager: "mgr1",
			expectedOut:  map[string]interface{}{"spec": map[string]interface{}{"replicas": 1}},
		},
		{
			name:    "unstructured, multiple manager",
			obj:     &unstructured.Unstructured{Object: map[string]interface{}{"spec": map[string]interface{}{"paused": true}}},
			objType: parser.Type("io.k8s.api.apps.v1.Deployment"),
			managedFields: []metav1.ManagedFieldsEntry{
				applyFieldsEntry("mgr1", `{ "f:spec": { "f:replicas": {}}}`, ""),
				applyFieldsEntry("mgr2", `{ "f:spec": { "f:paused": {}}}`, ""),
			},
			fieldManager: "mgr2",
			expectedOut:  map[string]interface{}{"spec": map[string]interface{}{"paused": true}},
		},
		{
			name:    "structured, no matching manager",
			obj:     &fakeDeployment{Spec: fakeDeploymentSpec{Replicas: &one}},
			objType: parser.Type("io.k8s.api.apps.v1.Deployment"),
			managedFields: []metav1.ManagedFieldsEntry{
				applyFieldsEntry("mgr999", `{ "f:spec": { "f:replicas": {}}}`, ""),
			},
			fieldManager: "mgr1",
			expectedOut:  map[string]interface{}{},
		},
		{
			name:    "structured, one manager",
			obj:     &fakeDeployment{Spec: fakeDeploymentSpec{Replicas: &one}},
			objType: parser.Type("io.k8s.api.apps.v1.Deployment"),
			managedFields: []metav1.ManagedFieldsEntry{
				applyFieldsEntry("mgr1", `{ "f:spec": { "f:replicas": {}}}`, ""),
			},
			fieldManager: "mgr1",
			expectedOut:  map[string]interface{}{"spec": map[string]interface{}{"replicas": int64(1)}},
		},
		{
			name:    "structured, multiple manager",
			obj:     &fakeDeployment{Spec: fakeDeploymentSpec{Replicas: &one, Paused: true}},
			objType: parser.Type("io.k8s.api.apps.v1.Deployment"),
			managedFields: []metav1.ManagedFieldsEntry{
				applyFieldsEntry("mgr1", `{ "f:spec": { "f:replicas": {}}}`, ""),
				applyFieldsEntry("mgr2", `{ "f:spec": { "f:paused": {}}}`, ""),
			},
			fieldManager: "mgr2",
			expectedOut:  map[string]interface{}{"spec": map[string]interface{}{"paused": true}},
		},
		{
			name:    "subresource",
			obj:     &fakeDeployment{Status: fakeDeploymentStatus{Replicas: &one}},
			objType: parser.Type("io.k8s.api.apps.v1.Deployment"),
			managedFields: []metav1.ManagedFieldsEntry{
				applyFieldsEntry("mgr1", `{ "f:status": { "f:replicas": {}}}`, "status"),
			},
			fieldManager: "mgr1",
			expectedOut:  map[string]interface{}{"status": map[string]interface{}{"replicas": int64(1)}},
			subresource:  "status",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out := map[string]interface{}{}
			accessor, err := meta.Accessor(tc.obj)
			if err != nil {
				t.Fatalf("Error accessing object: %v", err)
			}
			accessor.SetManagedFields(tc.managedFields)
			err = ExtractInto(tc.obj, tc.objType, tc.fieldManager, &out, tc.subresource)
			if err != nil {
				t.Fatalf("Unexpected extract error: %v", err)
			}
			if !equality.Semantic.DeepEqual(out, tc.expectedOut) {
				t.Fatalf("Expected output did not match actual output: %s", cmp.Diff(out, tc.expectedOut))
			}
		})
	}
}

// applyFieldsEntry 构造一条 apply 操作产生的 managedFields 记录；
// fieldsJSON 使用 FieldsV1 的 "f:" 路径语法描述该 manager 拥有的字段集。
func applyFieldsEntry(fieldManager string, fieldsJSON string, subresource string) metav1.ManagedFieldsEntry {
	return metav1.ManagedFieldsEntry{
		Manager:     fieldManager,
		Operation:   metav1.ManagedFieldsOperationApply,
		APIVersion:  "v1",
		FieldsType:  "FieldsV1",
		FieldsV1:    metav1.NewFieldsV1(fieldsJSON),
		Subresource: subresource,
	}
}

// fakeDeployment 是测试用的结构化对象，字段布局对应下方 schemaYAML 中的 Deployment 定义。
type fakeDeployment struct {
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              fakeDeploymentSpec   `json:"spec"`
	Status            fakeDeploymentStatus `json:"status"`
}

// fakeDeploymentSpec 只需包含测试涉及的两个字段（replicas、paused）。
type fakeDeploymentSpec struct {
	Replicas *int32 `json:"replicas"`
	Paused   bool   `json:"paused,omitempty"`
}

// fakeDeploymentStatus 对应 Deployment 的 status 子资源。
type fakeDeploymentStatus struct {
	Replicas *int32 `json:"replicas"`
}

// GetObjectMeta 返回对象元数据，使 fakeDeployment 满足 metav1.Object 接口。
func (o *fakeDeployment) GetObjectMeta() metav1.ObjectMeta {
	return o.ObjectMeta
}

// GetObjectKind 返回空的 ObjectKind：测试不关心 GVK。
func (o *fakeDeployment) GetObjectKind() runtimeschema.ObjectKind {
	return runtimeschema.EmptyObjectKind
}

// DeepCopyObject 直接返回自身，测试中对象不会被并发修改，无需真正深拷贝。
func (o *fakeDeployment) DeepCopyObject() runtime.Object {
	return o
}

// trimmed up schema for test purposes
// 中文补充：只保留测试路径上会用到的类型与字段，避免依赖完整 OpenAPI schema。
const schemaYAML = typed.YAMLObject(`types:
- name: io.k8s.api.apps.v1.Deployment
  map:
    fields:
    - name: apiVersion
      type:
        scalar: string
    - name: kind
      type:
        scalar: string
    - name: metadata
      type:
        namedType: io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
    - name: spec
      type:
        namedType: io.k8s.api.apps.v1.DeploymentSpec
    - name: status
      type:
        namedType: io.k8s.api.apps.v1.DeploymentStatus
- name: io.k8s.api.apps.v1.DeploymentSpec
  map:
    fields:
    - name: paused
      type:
        scalar: boolean
    - name: replicas
      type:
        scalar: numeric
- name: io.k8s.api.apps.v1.DeploymentStatus
  map:
    fields:
    - name: replicas
      type:
        scalar: numeric
- name: io.k8s.apimachinery.pkg.apis.meta.v1.ObjectMeta
  map:
    fields:
    - name: creationTimestamp
      type:
        namedType: io.k8s.apimachinery.pkg.apis.meta.v1.Time
    - name: managedFields
      type:
        list:
          elementType:
            namedType: io.k8s.apimachinery.pkg.apis.meta.v1.ManagedFieldsEntry
          elementRelationship: atomic
- name: io.k8s.apimachinery.pkg.apis.meta.v1.ManagedFieldsEntry
  map:
    fields:
    - name: apiVersion
      type:
        scalar: string
    - name: fieldsType
      type:
        scalar: string
    - name: fieldsV1
      type:
        namedType: io.k8s.apimachinery.pkg.apis.meta.v1.FieldsV1
    - name: manager
      type:
        scalar: string
    - name: operation
      type:
        scalar: string
    - name: time
      type:
        namedType: io.k8s.apimachinery.pkg.apis.meta.v1.Time
    - name: subresource
      type:
        scalar: string
- name: io.k8s.apimachinery.pkg.apis.meta.v1.FieldsV1
  map:
    elementType:
      scalar: untyped
      list:
        elementType:
          namedType: __untyped_atomic_
        elementRelationship: atomic
      map:
        elementType:
          namedType: __untyped_atomic_
        elementRelationship: atomic
- name: io.k8s.apimachinery.pkg.apis.meta.v1.Time
  scalar: untyped
- name: __untyped_atomic_
  scalar: untyped
  list:
    elementType:
      namedType: __untyped_atomic_
    elementRelationship: atomic
  map:
    elementType:
      namedType: __untyped_atomic_
    elementRelationship: atomic
`)
