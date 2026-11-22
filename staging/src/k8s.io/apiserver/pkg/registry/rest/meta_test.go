/*
Copyright 2017 The Kubernetes Authors.

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

package rest

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestWipeObjectMetaSystemFields validates that system populated fields are set on an object
//
// TestWipeObjectMetaSystemFields 验证清空系统字段后，创建时间、UID、
// 删除相关字段与 SelfLink 都回到零值。这些字段只能由服务端填写，
// 客户端传入时必须被抹掉。
func TestWipeObjectMetaSystemFields(t *testing.T) {
	resource := metav1.ObjectMeta{}
	WipeObjectMetaSystemFields(&resource)
	if !resource.CreationTimestamp.Time.IsZero() {
		t.Errorf("resource.CreationTimestamp is set")
	}
	if len(resource.UID) != 0 {
		t.Errorf("resource.UID is set")
	}
	if resource.DeletionTimestamp != nil {
		t.Errorf("resource.DeletionTimestamp is set")
	}
	if resource.DeletionGracePeriodSeconds != nil {
		t.Errorf("resource.DeletionGracePeriodSeconds is set")
	}
	if len(resource.SelfLink) != 0 {
		t.Errorf("resource.SelfLink is set")
	}
}

// TestFillObjectMetaSystemFields validates that system populated fields are set on an object
//
// TestFillObjectMetaSystemFields 验证新建对象时服务端会填充创建时间与
// 唯一 UID，这是对象持久化前必须满足的元数据不变量。
func TestFillObjectMetaSystemFields(t *testing.T) {
	resource := metav1.ObjectMeta{}
	FillObjectMetaSystemFields(&resource)
	if resource.CreationTimestamp.Time.IsZero() {
		t.Errorf("resource.CreationTimestamp is zero")
	}
	if len(resource.UID) == 0 {
		t.Errorf("resource.UID missing")
	}
}

// TestHasObjectMetaSystemFieldValues validates that true is returned if and only if all fields are populated
//
// TestHasObjectMetaSystemFieldValues 验证该判定只在全部系统字段就绪时
// 返回 true；存储层用它区分"新建对象"与"已持久化对象"，误判会导致
// 重复分配 UID。
func TestHasObjectMetaSystemFieldValues(t *testing.T) {
	resource := metav1.ObjectMeta{}
	objMeta, err := meta.Accessor(&resource)
	if err != nil {
		t.Fatal(err)
	}
	if metav1.HasObjectMetaSystemFieldValues(objMeta) {
		t.Errorf("the resource does not have all fields yet populated, but incorrectly reports it does")
	}
	FillObjectMetaSystemFields(&resource)
	if !metav1.HasObjectMetaSystemFieldValues(objMeta) {
		t.Errorf("the resource does have all fields populated, but incorrectly reports it does not")
	}
}

// TestEnsureObjectNamespaceMatchesRequestNamespace 验证请求命名空间与
// 对象命名空间的一致性规则：完全一致通过；集群级请求遇到带命名空间
// 的对象、或命名空间请求遇到无命名空间的对象时，为向后兼容而强制对齐
// 而非报错；两者都非空且不同才拒绝，防止通过 URL 命名空间越权写入。
func TestEnsureObjectNamespaceMatchesRequestNamespace(t *testing.T) {
	testcases := []struct {
		name        string
		reqNS       string
		objNS       string
		expectErr   bool
		expectObjNS string
	}{
		{
			name:        "cluster-scoped req, cluster-scoped obj",
			reqNS:       "",
			objNS:       "",
			expectErr:   false,
			expectObjNS: "",
		},
		{
			name:        "cluster-scoped req, namespaced obj",
			reqNS:       "",
			objNS:       "foo",
			expectErr:   false,
			expectObjNS: "", // no error, object is forced to cluster-scoped for backwards compatibility
		},
		{
			name:        "namespaced req, no-namespace obj",
			reqNS:       "foo",
			objNS:       "",
			expectErr:   false,
			expectObjNS: "foo", // no error, object is updated to match request for backwards compatibility
		},
		{
			name:        "namespaced req, matching obj",
			reqNS:       "foo",
			objNS:       "foo",
			expectErr:   false,
			expectObjNS: "foo",
		},
		{
			name:      "namespaced req, mis-matched obj",
			reqNS:     "foo",
			objNS:     "bar",
			expectErr: true,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			obj := metav1.ObjectMeta{Namespace: tc.objNS}
			err := EnsureObjectNamespaceMatchesRequestNamespace(tc.reqNS, &obj)
			if tc.expectErr {
				if err == nil {
					t.Fatal("expected err, got none")
				}
				return
			} else if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}

			// 即使无需报错，也要确认对象命名空间被对齐到期望值。
			if obj.Namespace != tc.expectObjNS {
				t.Fatalf("expected obj ns %q, got %q", tc.expectObjNS, obj.Namespace)
			}
		})
	}
}
