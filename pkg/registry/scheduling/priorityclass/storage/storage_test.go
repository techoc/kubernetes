/*
Copyright 2015 The Kubernetes Authors.

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

package storage

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistrytest "k8s.io/apiserver/pkg/registry/generic/testing"
	"k8s.io/apiserver/pkg/registry/rest"
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	schedulingapiv1 "k8s.io/kubernetes/pkg/apis/scheduling/v1"
	"k8s.io/kubernetes/pkg/registry/registrytest"
)

func newStorage(t *testing.T) (*REST, *etcd3testing.EtcdTestServer) {
	etcdStorage, server := registrytest.NewEtcdStorage(t, scheduling.GroupName)
	restOptions := generic.RESTOptions{
		StorageConfig:           etcdStorage,
		Decorator:               generic.UndecoratedStorage,
		DeleteCollectionWorkers: 1,
		ResourcePrefix:          "priorityclasses",
	}
	rest, err := NewREST(restOptions)
	if err != nil {
		t.Fatalf("unable to create REST %v", err)
	}
	return rest, server
}

func validNewPriorityClass() *scheduling.PriorityClass {
	return &scheduling.PriorityClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo",
		},
		Value:         100,
		GlobalDefault: false,
		Description:   "This is created only for testing.",
	}
}

func TestCreate(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope()
	test.TestCreate(
		validNewPriorityClass(),
		// 非法创建用例
		&scheduling.PriorityClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: "*badName",
			},
			Value:         100,
			GlobalDefault: true,
			Description:   "This is created only for testing.",
		},
	)
}

func TestUpdate(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope()
	test.TestUpdate(
		// 合法更新用例
		validNewPriorityClass(),
		// PriorityClass 的 Value 创建后不可修改，因此不存在合法的 Value 更新函数。
		func(obj runtime.Object) runtime.Object {
			pc := obj.(*scheduling.PriorityClass)
			pc.Value = 100
			pc.GlobalDefault = false
			return pc
		},
		// 非法更新用例
		// 修改 Value，更新应当被拒绝。
		func(obj runtime.Object) runtime.Object {
			pc := obj.(*scheduling.PriorityClass)
			pc.Value = 200
			pc.GlobalDefault = false
			return pc
		},
	)
}

func TestDelete(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope()
	test.TestDelete(validNewPriorityClass())
}

// TestDeleteSystemPriorityClass 验证内置系统 PriorityClass 不能被删除。
func TestDeleteSystemPriorityClass(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	key := "/priorityclasses/test/system-node-critical"
	ctx := genericregistrytest.NewClusterScopeContext(storage.Store)
	pc := schedulingapiv1.SystemPriorityClasses()[0]
	internalPc := &scheduling.PriorityClass{}
	if err := schedulingapiv1.Convert_v1_PriorityClass_To_scheduling_PriorityClass(pc, internalPc, nil); err != nil {
		t.Fatal(err)
	}
	if err := storage.Store.Storage.Create(ctx, key, internalPc, nil, 0, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, _, err := storage.Delete(ctx, pc.Name, rest.ValidateAllObjectFunc, nil); err == nil {
		t.Error("expected to receive an error")
	}
}

func TestGet(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope()
	test.TestGet(validNewPriorityClass())
}

func TestList(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope()
	test.TestList(validNewPriorityClass())
}

func TestWatch(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope()
	test.TestWatch(
		validNewPriorityClass(),
		// 标签选择器匹配
		[]labels.Set{},
		// 标签选择器不匹配
		[]labels.Set{
			{"foo": "bar"},
		},
		// 字段选择器匹配
		[]fields.Set{
			{"metadata.name": "foo"},
		},
		// 字段选择器不匹配
		[]fields.Set{
			{"metadata.name": "bar"},
		},
	)
}

func TestShortNames(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	expected := []string{"pc"}
	registrytest.AssertShortNames(t, storage, expected)
}
