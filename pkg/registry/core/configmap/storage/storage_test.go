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
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/registry/registrytest"
)

// newStorage 启动隔离 etcd 测试后端并构造真实 ConfigMap RESTStorage。
func newStorage(t *testing.T) (*REST, *etcd3testing.EtcdTestServer) {
	etcdStorage, server := registrytest.NewEtcdStorage(t, "")
	restOptions := generic.RESTOptions{
		StorageConfig:           etcdStorage,
		Decorator:               generic.UndecoratedStorage,
		DeleteCollectionWorkers: 1,
		ResourcePrefix:          "configmaps",
	}
	rest, err := NewREST(restOptions)
	if err != nil {
		t.Fatalf("unexpected error from REST storage: %v", err)
	}
	return rest, server
}

func validNewConfigMap() *api.ConfigMap {
	return &api.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			Labels: map[string]string{
				"label-1": "value-1",
				"label-2": "value-2",
			},
		},
		Data: map[string]string{
			"test": "data",
		},
	}
}

// TestCreate 验证 ConfigMap 创建路径会执行名称、命名空间和数据字段校验并持久化对象。
func TestCreate(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)

	validConfigMap := validNewConfigMap()
	validConfigMap.ObjectMeta = metav1.ObjectMeta{
		GenerateName: "foo-",
	}

	test.TestCreate(
		validConfigMap,
		&api.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "badName"},
			Data: map[string]string{
				"key": "value",
			},
		},
		&api.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "name-2"},
			Data: map[string]string{
				"..dotfile": "do: nothing\n",
			},
		},
	)
}

// TestUpdate 验证 ConfigMap 更新经过 Strategy 的字段处理与 ValidateConfigMapUpdate 校验。
func TestUpdate(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestUpdate(
		// 有效更新。
		validNewConfigMap(),
		// 更新函数。
		func(obj runtime.Object) runtime.Object {
			cfg := obj.(*api.ConfigMap)
			cfg.Data["update-test"] = "value"
			return cfg
		},
		// 无效更新函数。
		func(obj runtime.Object) runtime.Object {
			cfg := obj.(*api.ConfigMap)
			cfg.Data["bad*Key"] = "value"
			return cfg
		},
	)
}

func TestDelete(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestDelete(validNewConfigMap())
}

func TestGet(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestGet(validNewConfigMap())
}

func TestList(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestList(validNewConfigMap())
}

// TestWatch 验证 ConfigMap 创建/更新后产生的存储事件能够通过 Watch 接口正确返回。
func TestWatch(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestWatch(
		validNewConfigMap(),
		// 匹配标签。
		[]labels.Set{
			{"label-1": "value-1"},
			{"label-2": "value-2"},
		},
		// 不匹配标签。
		[]labels.Set{
			{"foo": "bar"},
		},
		// 匹配 fields
		[]fields.Set{
			{"metadata.namespace": "default"},
			{"metadata.name": "foo"},
		},
		// 不匹配 fields
		[]fields.Set{
			{"metadata.name": "bar"},
			{"name": "foo"},
		},
	)
}

func TestShortNames(t *testing.T) {
	storage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	expected := []string{"cm"}
	registrytest.AssertShortNames(t, storage, expected)
}
