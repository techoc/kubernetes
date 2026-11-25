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
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistrytest "k8s.io/apiserver/pkg/registry/generic/testing"
	"k8s.io/apiserver/pkg/registry/rest"
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/registry/registrytest"
)

// newStorage 启动隔离 etcd，并构造 PVC 主资源与 Status Handler。
func newStorage(t *testing.T) (*REST, *StatusREST, *etcd3testing.EtcdTestServer) {
	etcdStorage, server := registrytest.NewEtcdStorage(t, "")
	restOptions := generic.RESTOptions{
		StorageConfig:           etcdStorage,
		Decorator:               generic.UndecoratedStorage,
		DeleteCollectionWorkers: 1,
		ResourcePrefix:          "persistentvolumeclaims",
	}
	persistentVolumeClaimStorage, statusStorage, err := NewREST(restOptions)
	if err != nil {
		t.Fatalf("unexpected error from REST storage: %v", err)
	}
	return persistentVolumeClaimStorage, statusStorage, server
}

func validNewPersistentVolumeClaim(name, ns string) *api.PersistentVolumeClaim {
	pv := &api.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Spec: api.PersistentVolumeClaimSpec{
			AccessModes: []api.PersistentVolumeAccessMode{api.ReadWriteOnce},
			Resources: api.VolumeResourceRequirements{
				Requests: api.ResourceList{
					api.ResourceName(api.ResourceStorage): resource.MustParse("10G"),
				},
			},
		},
		Status: api.PersistentVolumeClaimStatus{
			Phase: api.ClaimPending,
		},
	}
	return pv
}

func TestCreate(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	pv := validNewPersistentVolumeClaim("foo", metav1.NamespaceDefault)
	pv.ObjectMeta = metav1.ObjectMeta{}
	test.TestCreate(
		// 有效对象
		pv,
		// 无效对象
		&api.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: "*BadName!"},
		},
	)
}

func TestUpdate(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestUpdate(
		// 有效对象
		validNewPersistentVolumeClaim("foo", metav1.NamespaceDefault),
		// 更新函数
		func(obj runtime.Object) runtime.Object {
			object := obj.(*api.PersistentVolumeClaim)
			object.Spec.VolumeName = "onlyVolumeNameUpdateAllowed"
			return object
		},
	)
}

func TestDelete(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ReturnDeletedObject()
	test.TestDelete(validNewPersistentVolumeClaim("foo", metav1.NamespaceDefault))
}

func TestGet(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestGet(validNewPersistentVolumeClaim("foo", metav1.NamespaceDefault))
}

func TestList(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestList(validNewPersistentVolumeClaim("foo", metav1.NamespaceDefault))
}

func TestWatch(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestWatch(
		validNewPersistentVolumeClaim("foo", metav1.NamespaceDefault),
		// 匹配的标签
		[]labels.Set{},
		// 不匹配的标签
		[]labels.Set{
			{"foo": "bar"},
		},
		// 匹配的字段
		[]fields.Set{
			{"metadata.name": "foo"},
			{"name": "foo"},
		},
		// 不匹配的字段
		[]fields.Set{
			{"metadata.name": "bar"},
		},
	)
}

// TestUpdateStatus 验证 PVC 状态子资源只能更新 Status，不能更改 Spec 请求容量或数据源。
func TestUpdateStatus(t *testing.T) {
	storage, statusStorage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	ctx := genericregistrytest.NewNamespaceScopeContext(storage.Store, metav1.NamespaceDefault)
	statusCtx := genericregistrytest.NewNamespaceScopeContext(storage.Store, metav1.NamespaceDefault, "status")

	key, _ := storage.KeyFunc(ctx, "foo")
	pvcStart := validNewPersistentVolumeClaim("foo", metav1.NamespaceDefault)
	err := storage.Storage.Create(ctx, key, pvcStart, nil, 0, false)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	pvc := &api.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: metav1.NamespaceDefault,
		},
		Spec: api.PersistentVolumeClaimSpec{
			AccessModes: []api.PersistentVolumeAccessMode{api.ReadWriteOnce},
			Resources: api.VolumeResourceRequirements{
				Requests: api.ResourceList{
					api.ResourceName(api.ResourceStorage): resource.MustParse("3Gi"),
				},
			},
		},
		Status: api.PersistentVolumeClaimStatus{
			Phase: api.ClaimBound,
		},
	}

	_, _, err = statusStorage.Update(statusCtx, pvc.Name, rest.DefaultUpdatedObjectInfo(pvc), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	obj, err := storage.Get(ctx, "foo", &metav1.GetOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	pvcOut := obj.(*api.PersistentVolumeClaim)
	// 元数据存在差异，只比较相关状态变更。
	if !apiequality.Semantic.DeepEqual(pvc.Status, pvcOut.Status) {
		t.Errorf("unexpected object: %s", cmp.Diff(pvc.Status, pvcOut.Status))
	}
}

func TestShortNames(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	expected := []string{"pvc"}
	registrytest.AssertShortNames(t, storage, expected)
}

// TestDefaultOnReadPvc 验证 GET 时为历史 PVC 补齐 dataSourceRef，但不把只读默认值写回持久化对象。
func TestDefaultOnReadPvc(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	dataSource := api.TypedLocalObjectReference{
		Kind: "PersistentVolumeClaim",
		Name: "my-pvc",
	}
	dataSourceRef := api.TypedObjectReference{
		Kind: "PersistentVolumeClaim",
		Name: "my-pvc",
	}

	var tests = map[string]struct {
		dataSource    bool
		dataSourceRef bool
		want          bool
		wantRef       bool
	}{
		"empty ds": {},
		"volume ds": {
			dataSource: true,
			want:       true,
			wantRef:    true,
		},
		"volume ds ref": {
			dataSourceRef: true,
			want:          true,
			wantRef:       true,
		},
		"both data sources": {
			dataSource:    true,
			dataSourceRef: true,
			want:          true,
			wantRef:       true,
		},
	}

	for testName, test := range tests {
		t.Run(testName, func(t *testing.T) {
			pvc := new(api.PersistentVolumeClaim)
			if test.dataSource {
				pvc.Spec.DataSource = dataSource.DeepCopy()
			}
			if test.dataSourceRef {
				pvc.Spec.DataSourceRef = dataSourceRef.DeepCopy()
			}
			var expectDataSource *api.TypedLocalObjectReference
			if test.want {
				expectDataSource = &dataSource
			}
			var expectDataSourceRef *api.TypedObjectReference
			if test.wantRef {
				expectDataSourceRef = &dataSourceRef
			}

			// 被测方法
			storage.defaultOnReadPvc(pvc)

			if !reflect.DeepEqual(pvc.Spec.DataSource, expectDataSource) {
				t.Errorf("data source does not match, test: %s, dataSource: %v, expected: %v",
					testName, test.dataSource, test.want)
			}
			if !reflect.DeepEqual(pvc.Spec.DataSourceRef, expectDataSourceRef) {
				t.Errorf("data source ref does not match, test: %s, dataSourceRef: %v, expected: %v",
					testName, test.dataSourceRef, test.wantRef)
			}
		})
	}
}
