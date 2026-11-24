/*
Copyright 2023 The Kubernetes Authors.

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

// 本测试文件验证 volumeattributesclass REST Store 的创建、读取、更新、删除及子资源行为。
package storage

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistrytest "k8s.io/apiserver/pkg/registry/generic/testing"
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	storageapi "k8s.io/kubernetes/pkg/apis/storage"
	"k8s.io/kubernetes/pkg/registry/registrytest"
)

func newStorage(t *testing.T) (*REST, *etcd3testing.EtcdTestServer) {
	etcdStorage, server := registrytest.NewEtcdStorageForResource(t, storageapi.SchemeGroupVersion.WithResource("volumeattributesclasses").GroupResource())
	restOptions := generic.RESTOptions{
		StorageConfig:           etcdStorage,
		Decorator:               generic.UndecoratedStorage,
		DeleteCollectionWorkers: 1,
		ResourcePrefix:          "volumeattributesclasses",
	}
	volumeAttributesClassStorage, err := NewREST(restOptions)
	if err != nil {
		t.Fatalf("unexpected error from REST storage: %v", err)
	}
	return volumeAttributesClassStorage, server
}

func validNewVolumeAttributesClass(name string) *storageapi.VolumeAttributesClass {
	return &storageapi.VolumeAttributesClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		DriverName: "fake",
		Parameters: map[string]string{
			"foo": "bar",
		},
	}
}

func TestCreate(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope()
	volumeAttributesClass := validNewVolumeAttributesClass("foo")
	volumeAttributesClass.ObjectMeta = metav1.ObjectMeta{GenerateName: "foo"}
	test.TestCreate(
		// 合法输入
		volumeAttributesClass,
		// 非法输入
		&storageapi.VolumeAttributesClass{
			ObjectMeta: metav1.ObjectMeta{Name: "*BadName!"},
			Parameters: map[string]string{"foo": "bar"},
		},
	)
}

func TestUpdate(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope()
	test.TestUpdate(
		// 合法输入
		validNewVolumeAttributesClass("foo"),
		// 更新函数
		func(obj runtime.Object) runtime.Object {
			object := obj.(*storageapi.VolumeAttributesClass)
			object.Parameters = map[string]string{"foo": "bar"}
			return object
		},
		// 非法更新函数
		func(obj runtime.Object) runtime.Object {
			object := obj.(*storageapi.VolumeAttributesClass)
			object.Parameters = map[string]string{"faz": "bar"}
			return object
		},
	)

}

func TestDelete(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope().ReturnDeletedObject()
	test.TestDelete(validNewVolumeAttributesClass("foo"))
}

func TestGet(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope()
	test.TestGet(validNewVolumeAttributesClass("foo"))
}

func TestList(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope()
	test.TestList(validNewVolumeAttributesClass("foo"))
}

func TestWatch(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope()
	test.TestWatch(
		validNewVolumeAttributesClass("foo"),
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
