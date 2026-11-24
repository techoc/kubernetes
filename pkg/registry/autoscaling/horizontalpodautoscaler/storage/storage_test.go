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

// 本文件负责装配 horizontalpodautoscaler API Group 的版本资源映射，并按资源启用配置注册 REST Storage。
package storage

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/apis/autoscaling"
	api "k8s.io/kubernetes/pkg/apis/core"

	// 显式导入 autoscaling/v1，确保该 API 版本的初始化逻辑执行。
	_ "k8s.io/api/autoscaling/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistrytest "k8s.io/apiserver/pkg/registry/generic/testing"
	"k8s.io/apiserver/pkg/registry/rest"
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	"k8s.io/kubernetes/pkg/registry/registrytest"
)

func newStorage(t *testing.T) (*REST, *StatusREST, *etcd3testing.EtcdTestServer) {
	etcdStorage, server := registrytest.NewEtcdStorage(t, autoscaling.GroupName)
	restOptions := generic.RESTOptions{
		StorageConfig:           etcdStorage,
		Decorator:               generic.UndecoratedStorage,
		DeleteCollectionWorkers: 1,
		ResourcePrefix:          "horizontalpodautoscalers",
	}
	horizontalPodAutoscalerStorage, statusStorage, err := NewREST(restOptions)
	if err != nil {
		t.Fatalf("unexpected error from REST storage: %v", err)
	}
	return horizontalPodAutoscalerStorage, statusStorage, server
}

func validNewHorizontalPodAutoscaler(name string) *autoscaling.HorizontalPodAutoscaler {
	cpu := int32(70)
	return &autoscaling.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceDefault,
		},
		Spec: autoscaling.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscaling.CrossVersionObjectReference{
				Kind: "ReplicationController",
				Name: "myrc",
			},
			MaxReplicas: 5,
			Metrics: []autoscaling.MetricSpec{
				{
					Type: autoscaling.ResourceMetricSourceType,
					Resource: &autoscaling.ResourceMetricSource{
						Name: api.ResourceCPU,
						Target: autoscaling.MetricTarget{
							Type:               autoscaling.UtilizationMetricType,
							AverageUtilization: &cpu,
						},
					},
				},
			},
		},
	}
}

func TestCreate(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	autoscaler := validNewHorizontalPodAutoscaler("foo")
	autoscaler.ObjectMeta = metav1.ObjectMeta{}
	test.TestCreate(
		// 有效对象
		autoscaler,
		// 无效对象
		&autoscaling.HorizontalPodAutoscaler{},
	)
}

func TestUpdate(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestUpdate(
		// 有效对象
		validNewHorizontalPodAutoscaler("foo"),
		// 更新函数
		func(obj runtime.Object) runtime.Object {
			object := obj.(*autoscaling.HorizontalPodAutoscaler)
			object.Spec.MaxReplicas = object.Spec.MaxReplicas + 1
			return object
		},
	)
}

func TestDelete(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestDelete(validNewHorizontalPodAutoscaler("foo"))
}

func TestGet(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestGet(validNewHorizontalPodAutoscaler("foo"))
}

func TestList(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestList(validNewHorizontalPodAutoscaler("foo"))
}

func TestWatch(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	test := genericregistrytest.New(t, storage.Store)
	test.TestWatch(
		validNewHorizontalPodAutoscaler("foo"),
		// 匹配的标签选择器
		[]labels.Set{},
		// 不匹配的标签选择器
		[]labels.Set{
			{"foo": "bar"},
		},
		// 匹配的字段选择器
		[]fields.Set{},
		// 不匹配的字段选择器
		[]fields.Set{
			{"metadata.name": "bar"},
			{"name": "foo"},
		},
	)
}

func TestShortNames(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	expected := []string{"hpa"}
	registrytest.AssertShortNames(t, storage, expected)
}

func TestCategories(t *testing.T) {
	storage, _, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	expected := []string{"all"}
	registrytest.AssertCategories(t, storage, expected)
}

func TestUpdateStatus(t *testing.T) {
	storage, statusStorage, server := newStorage(t)
	defer server.Terminate(t)
	defer storage.Store.DestroyFunc()
	ctx := genericregistrytest.NewNamespaceScopeContext(storage.Store, metav1.NamespaceDefault)
	statusCtx := genericregistrytest.NewNamespaceScopeContext(storage.Store, metav1.NamespaceDefault, "status")
	key, _ := storage.KeyFunc(ctx, "foo")
	autoscalerStart := validNewHorizontalPodAutoscaler("foo")
	err := storage.Storage.Create(ctx, key, autoscalerStart, nil, 0, false)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	autoscalerIn := &autoscaling.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: metav1.NamespaceDefault,
		},
		Status: autoscaling.HorizontalPodAutoscalerStatus{
			Conditions: []autoscaling.HorizontalPodAutoscalerCondition{
				{Status: "True"},
			},
		},
	}

	_, _, err = statusStorage.Update(statusCtx, autoscalerIn.Name, rest.DefaultUpdatedObjectInfo(autoscalerIn), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	obj, err := storage.Get(ctx, "foo", &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	autoscalerOut := obj.(*autoscaling.HorizontalPodAutoscaler)
	// 元数据会产生与本次状态更新无关的差异，因此只比较实际需要验证的 Status。
	if !apiequality.Semantic.DeepEqual(autoscalerIn.Status, autoscalerOut.Status) {
		t.Errorf("unexpected object: %s", cmp.Diff(autoscalerIn, autoscalerOut))
	}
}
