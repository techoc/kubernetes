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

package registry

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/api/apitesting"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	"k8s.io/apiserver/pkg/storage/storagebackend/factory"
)

// NewDryRunnableTestStorage 启动嵌入式 etcd 并构造 DryRunnableStorage，
// 返回清理函数。测试需要真实存储后端来验证“dry-run 确实没有写盘”。
func NewDryRunnableTestStorage(t *testing.T) (DryRunnableStorage, func()) {
	server, sc := etcd3testing.NewUnsecuredEtcd3TestClientServer(t)
	sc.Codec = apitesting.TestStorageCodec(codecs, examplev1.SchemeGroupVersion)
	s, destroy, err := factory.Create(*sc.ForResource(schema.GroupResource{Resource: "pods"}), nil, nil, "/pods")
	if err != nil {
		t.Fatalf("Error creating storage: %v", err)
	}
	return DryRunnableStorage{Storage: s, Codec: sc.Codec}, func() {
		destroy()
		server.Terminate(t)
	}
}

// UnstructuredOrDie 从 JSON 构造 unstructured，解析失败直接 panic；
// 仅用于测试中写死的合法 JSON。
func UnstructuredOrDie(j string) *unstructured.Unstructured {
	m := map[string]interface{}{}
	err := json.Unmarshal([]byte(j), &m)
	if err != nil {
		panic(fmt.Errorf("Failed to unmarshal into Unstructured: %v", err))
	}
	return &unstructured.Unstructured{Object: m}
}

// TestDryRunCreateDoesntCreate 验证 dry-run create 不会在存储中留下对象。
func TestDryRunCreateDoesntCreate(t *testing.T) {
	s, destroy := NewDryRunnableTestStorage(t)
	defer destroy()

	obj := UnstructuredOrDie(`{"kind": "Pod"}`)
	out := UnstructuredOrDie(`{}`)

	err := s.Create(context.Background(), "/pods/key", obj, out, 0, true)
	if err != nil {
		t.Fatalf("Failed to create new dry-run object: %v", err)
	}

	err = s.Get(context.Background(), "/pods/key", storage.GetOptions{}, out)
	if e, ok := err.(*storage.StorageError); !ok || e.Code != storage.ErrCodeKeyNotFound {
		t.Errorf("Expected key to be not found, error: %v", err)
	}
}

// TestDryRunCreateReturnsObject 验证 dry-run create 会把输入对象写入 out，
// 使调用方拿到与真实创建一致的结果。
func TestDryRunCreateReturnsObject(t *testing.T) {
	s, destroy := NewDryRunnableTestStorage(t)
	defer destroy()

	obj := UnstructuredOrDie(`{"kind": "Pod"}`)
	out := UnstructuredOrDie(`{}`)

	err := s.Create(context.Background(), "/pods/key", obj, out, 0, true)
	if err != nil {
		t.Fatalf("Failed to create new dry-run object: %v", err)
	}

	if !reflect.DeepEqual(obj, out) {
		t.Errorf("Returned object different from input object:\nExpected: %v\nGot: %v", obj, out)
	}
}

// TestDryRunCreateExistingObjectFails 验证 dry-run create 对已存在的 key
// 仍返回 KeyExists，保持与真实创建一致的冲突语义。
func TestDryRunCreateExistingObjectFails(t *testing.T) {
	s, destroy := NewDryRunnableTestStorage(t)
	defer destroy()

	obj := UnstructuredOrDie(`{"kind": "Pod"}`)
	out := UnstructuredOrDie(`{}`)

	err := s.Create(context.Background(), "/pods/key", obj, out, 0, false)
	if err != nil {
		t.Fatalf("Failed to create new object: %v", err)
	}

	err = s.Create(context.Background(), "/pods/key", obj, out, 0, true)
	if e, ok := err.(*storage.StorageError); !ok || e.Code != storage.ErrCodeKeyExists {
		t.Errorf("Expected KeyExists error: %v", err)
	}

}

// TestDryRunUpdateMissingObjectFails 验证对象不存在时 dry-run update 返回 NotFound，
// 且不会调用 updateFunc——错误必须在执行业务更新之前暴露。
func TestDryRunUpdateMissingObjectFails(t *testing.T) {
	s, destroy := NewDryRunnableTestStorage(t)
	defer destroy()

	obj := UnstructuredOrDie(`{"kind": "Pod"}`)

	updateFunc := func(input runtime.Object, res storage.ResponseMeta) (output runtime.Object, ttl *uint64, err error) {
		return input, nil, errors.New("UpdateFunction shouldn't be called")
	}

	err := s.GuaranteedUpdate(context.Background(), "/pods/key", obj, false, nil, updateFunc, true, nil)
	if e, ok := err.(*storage.StorageError); !ok || e.Code != storage.ErrCodeKeyNotFound {
		t.Errorf("Expected key to be not found, error: %v", err)
	}
}

// TestDryRunUpdatePreconditions 验证 dry-run update 会执行前置条件检查：
// UID 不匹配返回 InvalidObj，匹配则正常完成。
func TestDryRunUpdatePreconditions(t *testing.T) {
	s, destroy := NewDryRunnableTestStorage(t)
	defer destroy()

	obj := UnstructuredOrDie(`{"kind": "Pod", "metadata": {"uid": "my-uid"}}`)
	out := UnstructuredOrDie(`{}`)
	err := s.Create(context.Background(), "/pods/key", obj, out, 0, false)
	if err != nil {
		t.Fatalf("Failed to create new object: %v", err)
	}

	updateFunc := func(input runtime.Object, res storage.ResponseMeta) (output runtime.Object, ttl *uint64, err error) {
		u, ok := input.(*unstructured.Unstructured)
		if !ok {
			return input, nil, errors.New("Input object is not unstructured")
		}
		unstructured.SetNestedField(u.Object, "value", "field")
		return u, nil, nil
	}
	wrongID := types.UID("wrong-uid")
	myID := types.UID("my-uid")
	err = s.GuaranteedUpdate(context.Background(), "/pods/key", obj, false, &storage.Preconditions{UID: &wrongID}, updateFunc, true, nil)
	if e, ok := err.(*storage.StorageError); !ok || e.Code != storage.ErrCodeInvalidObj {
		t.Errorf("Expected invalid object, error: %v", err)
	}

	err = s.GuaranteedUpdate(context.Background(), "/pods/key", obj, false, &storage.Preconditions{UID: &myID}, updateFunc, true, nil)
	if err != nil {
		t.Fatalf("Failed to update with valid precondition: %v", err)
	}
}

// TestDryRunUpdateDoesntUpdate 验证 dry-run update 不会修改存储中的对象。
func TestDryRunUpdateDoesntUpdate(t *testing.T) {
	s, destroy := NewDryRunnableTestStorage(t)
	defer destroy()

	obj := UnstructuredOrDie(`{"kind": "Pod"}`)
	created := UnstructuredOrDie(`{}`)

	err := s.Create(context.Background(), "/pods/key", obj, created, 0, false)
	if err != nil {
		t.Fatalf("Failed to create new object: %v", err)
	}

	updateFunc := func(input runtime.Object, res storage.ResponseMeta) (output runtime.Object, ttl *uint64, err error) {
		u, ok := input.(*unstructured.Unstructured)
		if !ok {
			return input, nil, errors.New("Input object is not unstructured")
		}
		unstructured.SetNestedField(u.Object, "value", "field")
		return u, nil, nil
	}

	err = s.GuaranteedUpdate(context.Background(), "/pods/key", obj, false, nil, updateFunc, true, nil)
	if err != nil {
		t.Fatalf("Failed to dry-run update: %v", err)
	}
	out := UnstructuredOrDie(`{}`)
	err = s.Get(context.Background(), "/pods/key", storage.GetOptions{}, out)
	if err != nil {
		t.Fatalf("Failed to get storage: %v", err)
	}
	if !reflect.DeepEqual(created, out) {
		t.Fatalf("Returned object %q different from expected %q", created, out)
	}
}

// TestDryRunUpdateReturnsObject 验证 dry-run update 会把更新结果（含新 ResourceVersion）
// 写回传入的对象，使客户端看到与真实更新相同的响应。
func TestDryRunUpdateReturnsObject(t *testing.T) {
	s, destroy := NewDryRunnableTestStorage(t)
	defer destroy()

	obj := UnstructuredOrDie(`{"kind": "Pod"}`)
	out := UnstructuredOrDie(`{}`)

	err := s.Create(context.Background(), "/pods/key", obj, out, 0, false)
	if err != nil {
		t.Fatalf("Failed to create new object: %v", err)
	}

	updateFunc := func(input runtime.Object, res storage.ResponseMeta) (output runtime.Object, ttl *uint64, err error) {
		u, ok := input.(*unstructured.Unstructured)
		if !ok {
			return input, nil, errors.New("Input object is not unstructured")
		}
		unstructured.SetNestedField(u.Object, "value", "field")
		return u, nil, nil
	}

	err = s.GuaranteedUpdate(context.Background(), "/pods/key", obj, false, nil, updateFunc, true, nil)
	if err != nil {
		t.Fatalf("Failed to dry-run update: %v", err)
	}
	out = UnstructuredOrDie(`{"field": "value", "kind": "Pod", "metadata": {"resourceVersion": "2"}}`)
	if !reflect.DeepEqual(obj, out) {
		t.Fatalf("Returned object %#v different from expected %#v", obj, out)
	}
}

// TestDryRunDeleteDoesntDelete 验证 dry-run delete 不会真正删除对象。
func TestDryRunDeleteDoesntDelete(t *testing.T) {
	s, destroy := NewDryRunnableTestStorage(t)
	defer destroy()

	obj := UnstructuredOrDie(`{"kind": "Pod"}`)
	out := UnstructuredOrDie(`{}`)

	err := s.Create(context.Background(), "/pods/key", obj, out, 0, false)
	if err != nil {
		t.Fatalf("Failed to create new object: %v", err)
	}

	err = s.Delete(context.Background(), "/pods/key", out, nil, rest.ValidateAllObjectFunc, true, nil, storage.DeleteOptions{})
	if err != nil {
		t.Fatalf("Failed to dry-run delete the object: %v", err)
	}

	err = s.Get(context.Background(), "/pods/key", storage.GetOptions{}, out)
	if err != nil {
		t.Fatalf("Failed to retrieve dry-run deleted object: %v", err)
	}
}

// TestDryRunDeleteMissingObjectFails 验证删除不存在的对象时 dry-run 同样返回 NotFound。
func TestDryRunDeleteMissingObjectFails(t *testing.T) {
	s, destroy := NewDryRunnableTestStorage(t)
	defer destroy()

	out := UnstructuredOrDie(`{}`)
	err := s.Delete(context.Background(), "/pods/key", out, nil, rest.ValidateAllObjectFunc, true, nil, storage.DeleteOptions{})
	if e, ok := err.(*storage.StorageError); !ok || e.Code != storage.ErrCodeKeyNotFound {
		t.Errorf("Expected key to be not found, error: %v", err)
	}
}

// TestDryRunDeleteReturnsObject 验证 dry-run delete 把将被删除的对象写入 out。
func TestDryRunDeleteReturnsObject(t *testing.T) {
	s, destroy := NewDryRunnableTestStorage(t)
	defer destroy()

	obj := UnstructuredOrDie(`{"kind": "Pod"}`)
	out := UnstructuredOrDie(`{}`)

	err := s.Create(context.Background(), "/pods/key", obj, out, 0, false)
	if err != nil {
		t.Fatalf("Failed to create new object: %v", err)
	}

	out = UnstructuredOrDie(`{}`)
	expected := UnstructuredOrDie(`{"kind": "Pod", "metadata": {"resourceVersion": "2"}}`)
	err = s.Delete(context.Background(), "/pods/key", out, nil, rest.ValidateAllObjectFunc, true, nil, storage.DeleteOptions{})
	if err != nil {
		t.Fatalf("Failed to delete with valid precondition: %v", err)
	}
	if !reflect.DeepEqual(expected, out) {
		t.Fatalf("Returned object %q doesn't match expected: %q", out, expected)
	}
}

// TestDryRunDeletePreconditions 验证 dry-run delete 会执行 UID 前置条件检查。
func TestDryRunDeletePreconditions(t *testing.T) {
	s, destroy := NewDryRunnableTestStorage(t)
	defer destroy()

	obj := UnstructuredOrDie(`{"kind": "Pod", "metadata": {"uid": "my-uid"}}`)
	out := UnstructuredOrDie(`{}`)

	err := s.Create(context.Background(), "/pods/key", obj, out, 0, false)
	if err != nil {
		t.Fatalf("Failed to create new object: %v", err)
	}

	wrongID := types.UID("wrong-uid")
	myID := types.UID("my-uid")
	err = s.Delete(context.Background(), "/pods/key", out, &storage.Preconditions{UID: &wrongID}, rest.ValidateAllObjectFunc, true, nil, storage.DeleteOptions{})
	if e, ok := err.(*storage.StorageError); !ok || e.Code != storage.ErrCodeInvalidObj {
		t.Errorf("Expected invalid object, error: %v", err)
	}

	err = s.Delete(context.Background(), "/pods/key", out, &storage.Preconditions{UID: &myID}, rest.ValidateAllObjectFunc, true, nil, storage.DeleteOptions{})
	if err != nil {
		t.Fatalf("Failed to delete with valid precondition: %v", err)
	}
}
