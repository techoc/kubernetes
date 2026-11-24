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

package storage

import (
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistrytest "k8s.io/apiserver/pkg/registry/generic/testing"
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	"k8s.io/kubernetes/pkg/apis/apiserverinternal"
	"k8s.io/kubernetes/pkg/registry/registrytest"

	// 副作用导入会将 apiserverinternal API 类型注册到全局 Scheme，供 REST 测试使用。
	_ "k8s.io/kubernetes/pkg/apis/apiserverinternal/install"
)

func TestCreate(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope().Namer(name).GeneratesName()
	storageVersion := validStorageVersion()
	test.TestCreate(
		// 合法输入
		storageVersion,
		// 非法输入
		newStorageVersion(""),
	)
}

func TestUpdate(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope().Namer(name).GeneratesName()

	test.TestUpdate(
		// 合法输入
		validStorageVersion(),
		// 更新函数
		func(obj runtime.Object) runtime.Object {
			object := obj.(*apiserverinternal.StorageVersion)
			update := &apiserverinternal.StorageVersion{
				ObjectMeta: object.ObjectMeta,
			}
			return update
		},
		// 非法更新函数
		func(obj runtime.Object) runtime.Object {
			object := obj.(*apiserverinternal.StorageVersion)
			object.Name = ""
			return object
		},
	)
}

func TestGet(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope().Namer(name)
	test.TestGet(validStorageVersion())
}

func TestList(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope().Namer(name)
	test.TestList(validStorageVersion())
}

func TestDelete(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope().Namer(name)
	test.TestDelete(validStorageVersion())
}

func TestWatch(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope().Namer(name).GeneratesName()
	test.TestWatch(
		validStorageVersion(),
		[]labels.Set{},
		[]labels.Set{
			{"hoo": "bar"},
		},
		[]fields.Set{
			{"metadata.name": "core.pods"},
		},
		[]fields.Set{
			{"metadata.name": "nomatch"},
		},
	)
}

func name(i int) string {
	return fmt.Sprintf("core.pods%d", i)
}

func validStorageVersion() *apiserverinternal.StorageVersion {
	ssv1 := apiserverinternal.ServerStorageVersion{
		APIServerID:       "1",
		EncodingVersion:   "v1",
		DecodableVersions: []string{"v1", "v2"},
	}
	ssv2 := apiserverinternal.ServerStorageVersion{
		APIServerID:       "2",
		EncodingVersion:   "v1",
		DecodableVersions: []string{"v1", "v2"},
	}
	// ssv3 使用不同的存储编码版本，用于验证多个 API Server 的编码版本汇总结果。
	ssv3 := apiserverinternal.ServerStorageVersion{
		APIServerID:       "3",
		EncodingVersion:   "v2",
		DecodableVersions: []string{"v1", "v2"},
	}
	return &apiserverinternal.StorageVersion{
		ObjectMeta: metav1.ObjectMeta{
			Name: "core.pods",
		},
		Status: apiserverinternal.StorageVersionStatus{
			StorageVersions: []apiserverinternal.ServerStorageVersion{ssv1, ssv2, ssv3},
			Conditions:      commonVersionFalseCondition(),
		},
	}
}

func commonVersionFalseCondition() []apiserverinternal.StorageVersionCondition {
	return []apiserverinternal.StorageVersionCondition{{
		Type:    apiserverinternal.AllEncodingVersionsEqual,
		Status:  apiserverinternal.ConditionFalse,
		Reason:  "CommonEncodingVersionUnset",
		Message: "Common encoding version unset",
	}}
}
func newStorageVersion(name string) *apiserverinternal.StorageVersion {
	return &apiserverinternal.StorageVersion{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{"foo": "bar"},
		},
		Status: apiserverinternal.StorageVersionStatus{
			Conditions: commonVersionFalseCondition(),
		},
	}
}

func newStorage(t *testing.T) (*REST, *StatusREST, *etcd3testing.EtcdTestServer) {
	etcdStorage, server := registrytest.NewEtcdStorage(t, apiserverinternal.GroupName)
	restOptions := generic.RESTOptions{
		StorageConfig:           etcdStorage,
		Decorator:               generic.UndecoratedStorage,
		DeleteCollectionWorkers: 1,
		ResourcePrefix:          "storageversions",
	}
	storageVersionStorage, statusStorage, err := NewREST(restOptions)
	if err != nil {
		t.Fatalf("unexpected error from REST storage: %v", err)
	}
	return storageVersionStorage, statusStorage, server
}
