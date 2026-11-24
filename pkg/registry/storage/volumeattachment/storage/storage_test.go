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

// 本测试文件验证 volumeattachment REST Store 的创建、读取、更新、删除及子资源行为。
package storage

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistrytest "k8s.io/apiserver/pkg/registry/generic/testing"
	"k8s.io/apiserver/pkg/registry/rest"
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	storageapi "k8s.io/kubernetes/pkg/apis/storage"
	"k8s.io/kubernetes/pkg/registry/registrytest"
)

func newStorage(t *testing.T) (*REST, *StatusREST, *etcd3testing.EtcdTestServer) {
	etcdStorage, server := registrytest.NewEtcdStorage(t, storageapi.GroupName)
	restOptions := generic.RESTOptions{
		StorageConfig:           etcdStorage,
		Decorator:               generic.UndecoratedStorage,
		DeleteCollectionWorkers: 1,
		ResourcePrefix:          "volumeattachments",
	}
	volumeAttachmentStorage, err := NewStorage(restOptions)
	if err != nil {
		t.Fatalf("unexpected error from REST storage: %v", err)
	}
	return volumeAttachmentStorage.VolumeAttachment, volumeAttachmentStorage.Status, server
}

func validNewVolumeAttachment(name string) *storageapi.VolumeAttachment {
	pvName := "foo"
	return &storageapi.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: storageapi.VolumeAttachmentSpec{
			Attacher: "valid-attacher",
			Source: storageapi.VolumeAttachmentSource{
				PersistentVolumeName: &pvName,
			},
			NodeName: "valid-node",
		},
	}
}

func TestCreate(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope()
	volumeAttachment := validNewVolumeAttachment("foo")
	volumeAttachment.ObjectMeta = metav1.ObjectMeta{GenerateName: "foo"}
	pvName := "foo"
	test.TestCreate(
		// 合法输入
		volumeAttachment,
		// 非法输入
		&storageapi.VolumeAttachment{
			ObjectMeta: metav1.ObjectMeta{Name: "*BadName!"},
			Spec: storageapi.VolumeAttachmentSpec{
				Attacher: "invalid-attacher-!@#$%^&*()",
				Source: storageapi.VolumeAttachmentSource{
					PersistentVolumeName: &pvName,
				},
				NodeName: "invalid-node-!@#$%^&*()",
			},
		},
	)
}

func TestUpdate(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope()

	test.TestUpdate(
		// 合法输入
		validNewVolumeAttachment("foo"),
		// v1 和 v1beta1 都允许请求携带 Status 字段，
		// 但在 v1 中，客户端提交的新值不会生效。
		func(obj runtime.Object) runtime.Object {
			object := obj.(*storageapi.VolumeAttachment)
			object.Status.Attached = true
			return object
		},
		// 非法更新函数
		func(obj runtime.Object) runtime.Object {
			object := obj.(*storageapi.VolumeAttachment)
			object.Spec.Attacher = "invalid-attacher-!@#$%^&*()"
			return object
		},
	)
}

func TestDelete(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope().ReturnDeletedObject()
	test.TestDelete(validNewVolumeAttachment("foo"))
}

func TestGet(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope()
	test.TestGet(validNewVolumeAttachment("foo"))
}

func TestList(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope()
	test.TestList(validNewVolumeAttachment("foo"))
}

func TestWatch(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store).ClusterScope()
	test.TestWatch(
		validNewVolumeAttachment("foo"),
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

func TestEtcdStatusUpdate(t *testing.T) {
	storage, statusStorage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	ctx := genericregistrytest.NewClusterScopeContext(storage.Store)
	statusCtx := genericregistrytest.NewClusterScopeContext(storage.Store, "status")

	attachment := validNewVolumeAttachment("foo")
	if _, err := storage.Create(ctx, attachment, rest.ValidateAllObjectFunc, &metav1.CreateOptions{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	obj, err := storage.Get(ctx, attachment.ObjectMeta.Name, &metav1.GetOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// 更新 Status 子资源。
	attachmentIn := obj.(*storageapi.VolumeAttachment).DeepCopy()
	attachmentIn.Status.Attached = true

	_, _, err = statusStorage.Update(statusCtx, attachmentIn.Name, rest.DefaultUpdatedObjectInfo(attachmentIn), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Failed to update status: %v", err)
	}

	// 验证对象已按预期更新。
	obj, err = storage.Get(ctx, attachmentIn.ObjectMeta.Name, &metav1.GetOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	attachmentOut := obj.(*storageapi.VolumeAttachment)
	if !apiequality.Semantic.DeepEqual(attachmentIn.Spec, attachmentOut.Spec) {
		t.Errorf("objects differ: %v", cmp.Diff(attachmentOut.Spec, attachmentIn.Spec))
	}
	if !apiequality.Semantic.DeepEqual(attachmentIn.Status, attachmentOut.Status) {
		t.Errorf("objects differ: %v", cmp.Diff(attachmentOut.Status, attachmentIn.Status))
	}
}
