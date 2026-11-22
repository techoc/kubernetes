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

package unstructured

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUnstructuredList 验证 UnstructuredList 的 UnstructuredContent 会把 Items
// 展开为内容 map 中的 "items" 键，并且展开后的嵌套字段仍可被读取。
func TestUnstructuredList(t *testing.T) {
	list := &UnstructuredList{
		Object: map[string]interface{}{"kind": "List", "apiVersion": "v1"},
		Items: []Unstructured{
			{Object: map[string]interface{}{"kind": "Pod", "apiVersion": "v1", "metadata": map[string]interface{}{"name": "test"}}},
		},
	}
	content := list.UnstructuredContent()
	items := content["items"].([]interface{})
	require.Len(t, items, 1)
	val, found, err := NestedFieldCopy(items[0].(map[string]interface{}), "metadata", "name")
	require.True(t, found)
	require.NoError(t, err)
	assert.Equal(t, "test", val)
}

// TestNilDeletionTimestamp 验证零值 Unstructured 上读取/写入 nil 的删除时间戳
// 不会意外创建 metadata；设置真实时间戳后可读取，设为 nil 后字段被删除。
func TestNilDeletionTimestamp(t *testing.T) {
	var u Unstructured
	del := u.GetDeletionTimestamp()
	if del != nil {
		t.Errorf("unexpected non-nil deletion timestamp: %v", del)
	}
	u.SetDeletionTimestamp(u.GetDeletionTimestamp())
	del = u.GetDeletionTimestamp()
	if del != nil {
		t.Errorf("unexpected non-nil deletion timestamp: %v", del)
	}
	_, ok := u.Object["metadata"]
	assert.False(t, ok)

	now := metav1.Now()
	u.SetDeletionTimestamp(&now)
	assert.Equal(t, now.Unix(), u.GetDeletionTimestamp().Unix())
	u.SetDeletionTimestamp(nil)
	metadata := u.Object["metadata"].(map[string]interface{})
	_, ok = metadata["deletionTimestamp"]
	assert.False(t, ok)
}

// TestEmptyCreationTimestampIsOmitted 验证创建时间戳被设为零值时，对应字段会从对象中移除，
// 以保持与类型化对象一致的可选字段语义。
func TestEmptyCreationTimestampIsOmitted(t *testing.T) {
	var u Unstructured
	now := metav1.Now()

	// set an initial creationTimestamp and ensure the field exists
	// 先设置一个真实时间戳，确认字段存在。
	u.SetCreationTimestamp(now)
	metadata := u.Object["metadata"].(map[string]interface{})
	_, exists := metadata["creationTimestamp"]
	if !exists {
		t.Fatalf("unexpected missing creationTimestamp")
	}

	// set an empty timestamp and ensure the field no longer exists
	// 设为零值时间戳后，字段应被移除而不是写入零值。
	u.SetCreationTimestamp(metav1.Time{})
	metadata = u.Object["metadata"].(map[string]interface{})
	creationTimestamp, exists := metadata["creationTimestamp"]
	if exists {
		t.Errorf("unexpected creation timestamp field: %q", creationTimestamp)
	}
}
