/*
Copyright 2016 The Kubernetes Authors.

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

// 本文件验证 PDB REST 存储的创建、读取、列表、删除、Watch 与 Status 子资源更新行为。
package storage

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistrytest "k8s.io/apiserver/pkg/registry/generic/testing"
	"k8s.io/apiserver/pkg/registry/rest"
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	"k8s.io/kubernetes/pkg/apis/policy"
	"k8s.io/kubernetes/pkg/registry/registrytest"
)

func newStorage(t *testing.T) (*REST, *StatusREST, *etcd3testing.EtcdTestServer) {
	etcdStorage, server := registrytest.NewEtcdStorage(t, policy.GroupName)
	restOptions := generic.RESTOptions{StorageConfig: etcdStorage, Decorator: generic.UndecoratedStorage, DeleteCollectionWorkers: 1, ResourcePrefix: "poddisruptionbudgets"}
	podDisruptionBudgetStorage, statusStorage, err := NewREST(restOptions)
	if err != nil {
		t.Fatalf("unexpected error from REST storage: %v", err)
	}
	return podDisruptionBudgetStorage, statusStorage, server
}

func validNewPodDisruptionBudget() *policy.PodDisruptionBudget {
	minAvailable := intstr.FromInt32(7)
	return &policy.PodDisruptionBudget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: metav1.NamespaceDefault,
			Labels:    map[string]string{"a": "b"},
		},
		Spec: policy.PodDisruptionBudgetSpec{
			Selector:     &metav1.LabelSelector{MatchLabels: map[string]string{"a": "b"}},
			MinAvailable: &minAvailable,
		},
		Status: policy.PodDisruptionBudgetStatus{},
	}
}

func TestCreate(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	pdb := validNewPodDisruptionBudget()
	pdb.ObjectMeta = metav1.ObjectMeta{}
	test.TestCreate(
		// 合法输入
		pdb,
		// TODO：待补充相应校验后，增加非法输入用例。
	)
}

// TODO：待允许相应 Spec 更新后，补充 Spec 更新场景测试。

func TestStatusUpdate(t *testing.T) {
	storage, statusStorage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	ctx := genericregistrytest.NewNamespaceScopeContext(storage.Store, metav1.NamespaceDefault)
	statusCtx := genericregistrytest.NewNamespaceScopeContext(storage.Store, metav1.NamespaceDefault, "status")
	key := "/poddisruptionbudgets/" + metav1.NamespaceDefault + "/foo"
	validPodDisruptionBudget := validNewPodDisruptionBudget()
	if err := storage.Storage.Create(ctx, key, validPodDisruptionBudget, nil, 0, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	obj, err := storage.Get(ctx, "foo", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get pdb: %v", err)
	}
	obtainedPdb := obj.(*policy.PodDisruptionBudget)

	minAvailable := intstr.FromInt32(8)
	update := policy.PodDisruptionBudget{
		ObjectMeta: obtainedPdb.ObjectMeta,
		Spec: policy.PodDisruptionBudgetSpec{
			MinAvailable: &minAvailable,
		},
		Status: policy.PodDisruptionBudgetStatus{
			ExpectedPods: 8,
		},
	}

	if _, _, err := statusStorage.Update(statusCtx, update.Name, rest.DefaultUpdatedObjectInfo(&update), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	obj, err = storage.Get(ctx, "foo", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	pdb := obj.(*policy.PodDisruptionBudget)
	if pdb.Spec.MinAvailable.IntValue() != 7 {
		t.Errorf("we expected .spec.replicas to not be updated but it was updated to %v", pdb.Spec.MinAvailable)
	}
	if pdb.Status.ExpectedPods != 8 {
		t.Errorf("we expected .status.replicas to be updated to %d but it was %v", 7, pdb.Status.ExpectedPods)
	}
}

func TestGet(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestGet(validNewPodDisruptionBudget())
}

func TestList(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestList(validNewPodDisruptionBudget())
}

func TestDelete(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestDelete(validNewPodDisruptionBudget())
}

func TestWatch(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestWatch(
		validNewPodDisruptionBudget(),
		// 标签选择器匹配
		[]labels.Set{
			{"a": "b"},
		},
		// 标签选择器不匹配
		[]labels.Set{
			{"a": "c"},
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

// TODO：补充 Generation 递增规则测试。
