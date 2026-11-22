/*
Copyright 2018 The Kubernetes Authors.

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

package audit

import (
	"net/http"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

// TestMaybeTruncateUserAgent 验证 User-Agent 只在超长时截断，短值原样保留；
// 截断用于限制审计事件字段体积，防止客户端伪造超长 UA 撑大日志。
func TestMaybeTruncateUserAgent(t *testing.T) {
	req := &http.Request{}
	req.Header = http.Header{}

	ua := "short-agent"
	req.Header.Set("User-Agent", ua)
	assert.Equal(t, ua, maybeTruncateUserAgent(req))

	ua = ""
	for i := 0; i < maxUserAgentLength*2; i++ {
		ua = ua + "a"
	}
	req.Header.Set("User-Agent", ua)
	assert.NotEqual(t, ua, maybeTruncateUserAgent(req))
}

// TestCopyWithoutManagedFields 覆盖对象、列表与 Table 三种形态：函数必须返回
// 深拷贝并剥离 managedFields，同时保证入参对象不被修改。审计事件不应携带
// managedFields，否则体积大且泄露字段管理历史。
func TestCopyWithoutManagedFields(t *testing.T) {
	tests := []struct {
		name     string
		object   runtime.Object
		expected runtime.Object
		ok       bool
		err      error
	}{
		{
			// Status 既不是 Accessor 也不是列表/Table，应原样返回 nil 结果。
			name:   "object specified is not a meta.Accessor or a list or a table",
			object: &metav1.Status{},
		},
		{
			// 单个 Pod：预期副本中 only name/namespace 保留，managedFields 被清空。
			name: "object specified is a meta.Accessor and has managed fields",
			object: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "bar",
					ManagedFields: []metav1.ManagedFieldsEntry{
						{Manager: "a", Operation: metav1.ManagedFieldsOperationUpdate, Time: &metav1.Time{Time: time.Now()}},
						{Manager: "b", Operation: metav1.ManagedFieldsOperationUpdate, Time: &metav1.Time{Time: time.Now()}},
					},
				},
			},
			expected: &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "foo",
					Namespace: "bar",
				},
			},
			ok: true,
		},
		{
			// 列表需要逐项剥离，验证不会漏掉切片中的任一元素。
			name: "object specified is a list and its items have managed fields",
			object: &corev1.PodList{
				Items: []corev1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "foo",
							Namespace: "ns1",
							ManagedFields: []metav1.ManagedFieldsEntry{
								{Manager: "a", Operation: metav1.ManagedFieldsOperationUpdate, Time: &metav1.Time{Time: time.Now()}},
								{Manager: "b", Operation: metav1.ManagedFieldsOperationUpdate, Time: &metav1.Time{Time: time.Now()}},
							},
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "bar",
							Namespace: "ns2",
							ManagedFields: []metav1.ManagedFieldsEntry{
								{Manager: "c", Operation: metav1.ManagedFieldsOperationUpdate, Time: &metav1.Time{Time: time.Now()}},
								{Manager: "d", Operation: metav1.ManagedFieldsOperationUpdate, Time: &metav1.Time{Time: time.Now()}},
							},
						},
					},
				},
			},
			expected: &corev1.PodList{
				Items: []corev1.Pod{
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "foo",
							Namespace: "ns1",
						},
					},
					{
						ObjectMeta: metav1.ObjectMeta{
							Name:      "bar",
							Namespace: "ns2",
						},
					},
				},
			},
			ok: true,
		},
		{
			// Table 行的 Object 是 RawExtension，需要在解码后的对象上剥离；
			// 同时验证空行不会触发 panic。
			name: "object specified is a Table and objects in its rows have managed fields",
			object: &metav1.Table{
				Rows: []metav1.TableRow{
					{
						Object: runtime.RawExtension{
							Object: &corev1.Pod{
								ObjectMeta: metav1.ObjectMeta{
									Name:      "foo",
									Namespace: "ns1",
									ManagedFields: []metav1.ManagedFieldsEntry{
										{Manager: "a", Operation: metav1.ManagedFieldsOperationUpdate, Time: &metav1.Time{Time: time.Now()}},
										{Manager: "b", Operation: metav1.ManagedFieldsOperationUpdate, Time: &metav1.Time{Time: time.Now()}},
									},
								},
							},
						},
					},
					{
						Object: runtime.RawExtension{
							Object: &corev1.Pod{
								ObjectMeta: metav1.ObjectMeta{
									Name:      "bar",
									Namespace: "ns2",
									ManagedFields: []metav1.ManagedFieldsEntry{
										{Manager: "c", Operation: metav1.ManagedFieldsOperationUpdate, Time: &metav1.Time{Time: time.Now()}},
										{Manager: "d", Operation: metav1.ManagedFieldsOperationUpdate, Time: &metav1.Time{Time: time.Now()}},
									},
								},
							},
						},
					},
					// add an empty row to make sure we don't panic
					{
						Object: runtime.RawExtension{},
					},
				},
			},
			expected: &metav1.Table{
				Rows: []metav1.TableRow{
					{
						Object: runtime.RawExtension{
							Object: &corev1.Pod{
								ObjectMeta: metav1.ObjectMeta{
									Name:      "foo",
									Namespace: "ns1",
								},
							},
						},
					},
					{
						Object: runtime.RawExtension{
							Object: &corev1.Pod{
								ObjectMeta: metav1.ObjectMeta{
									Name:      "bar",
									Namespace: "ns2",
								},
							},
						},
					},
					{
						Object: runtime.RawExtension{},
					},
				},
			},
			ok: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 保留执行前快照，用于事后断言输入未被原地修改。
			original := test.object.DeepCopyObject()
			objectGot, ok, err := copyWithoutManagedFields(test.object)

			if test.err != err {
				t.Errorf("expected error: %v, but got: %v", test.err, err)
			}
			if test.ok != ok {
				t.Errorf("expected ok: %t, but got: %t", test.ok, ok)
			}

			switch {
			case test.expected == nil:
				if objectGot != nil {
					t.Errorf("expected the returned object to be nil, but got %#v", objectGot)
				}
			default:
				// verify that a deep copy of the specified object is made before mutating it.
				// 指针相同说明返回的是入参本身，后续剥离会污染调用方对象。
				if expected, actual := reflect.ValueOf(test.object), reflect.ValueOf(objectGot); expected.Pointer() == actual.Pointer() {
					t.Error("expected the returned object to be a deep copy of the input object")
				}

				if !cmp.Equal(test.expected, objectGot) {
					t.Errorf("expected and actual do not match, diff: %s", cmp.Diff(test.expected, objectGot))
				}
			}

			// we always expect the original object to be unchanged.
			if !cmp.Equal(original, test.object) {
				t.Errorf("the original object has mutated, diff: %s", cmp.Diff(original, test.object))
			}
		})
	}
}
