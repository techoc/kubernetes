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

// Package tester 为 genericregistry.Store 提供一套可复用的存储层测试，
// 覆盖 create/update/delete/get/list/watch 的标准行为约定。
// 各资源只需提供合法的示例对象即可获得一致的测试覆盖。
package tester

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authentication/user"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/registry/rest/resttest"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"
)

// Tester 包装 resttest.Tester，并补充对 genericregistry.Store 的直接访问，
// 用于构造测试数据和读取存储状态。
type Tester struct {
	tester  *resttest.Tester
	storage *genericregistry.Store
}

// UpdateFunc 接收对象并返回更新后的对象，供 TestUpdate 描述一次更新操作。
type UpdateFunc func(runtime.Object) runtime.Object

// New 为给定存储创建 Tester。
// 当存储的 scheme 中注册了至少一个外部版本，且 create/update 策略实现了
// runtime.ObjectTyper、VersionsForGroupKind 与 NewFunc 时，测试期间会自动设置 RequestInfo。
func New(t *testing.T, storage *genericregistry.Store, subresources ...string) *Tester {
	tester := resttest.New(t, storage)
	if info := priorityRequestInfo(storage); info != nil {
		if len(subresources) > 0 {
			info.Subresource = strings.Join(subresources, "/")
		}
		tester.SetRequestInfo(info)
	}
	return &Tester{
		tester:  tester,
		storage: storage,
	}
}

// priorityRequestInfoScheme 是构造 RequestInfo 所需的 scheme 能力。
type priorityRequestInfoScheme interface {
	runtime.ObjectTyper

	// VersionsForGroupKind 按优先级返回外部版本。
	// 参见 Scheme.VersionsForGroupKind。
	VersionsForGroupKind(schema.GroupKind) []schema.GroupVersion
}

// priorityRequestInfo 返回优先级最高的外部 API 版本；找不到外部版本时返回 nil。
// 要求 store 的 CreateStrategy 实现 runtime.ObjectTyper、VersionsForGroupKind()
// 并提供 NewFunc 实现；否则返回 nil。
func priorityRequestInfo(store *genericregistry.Store) *genericapirequest.RequestInfo {
	if store == nil || store.NewFunc == nil {
		return nil
	}
	// CreateStrategy 优先，其次 UpdateStrategy；两者都没有该能力时放弃。
	scheme, ok := store.CreateStrategy.(priorityRequestInfoScheme)
	if !ok {
		scheme, ok = store.UpdateStrategy.(priorityRequestInfoScheme)
		if !ok {
			return nil
		}
	}
	gvks, _, err := scheme.ObjectKinds(store.NewFunc())
	if err != nil || len(gvks) == 0 {
		return nil
	}
	gvs := scheme.VersionsForGroupKind(gvks[0].GroupKind())
	if len(gvs) == 0 {
		return nil
	}
	gr := store.DefaultQualifiedResource
	return &genericapirequest.RequestInfo{
		APIGroup:   gr.Group,
		APIVersion: gvs[0].Version,
		Resource:   gr.Resource,
	}
}

// NewClusterScopeContext 返回用于测试集群级请求的 context。
// 当存储具备外部版本及相应的 create/update 策略时，context 中会包含 RequestInfo。
func NewClusterScopeContext(store *genericregistry.Store, subresources ...string) context.Context {
	return newContext(store, "", subresources)
}

// NewNamespaceScopeContext 返回用于测试命名空间级请求的 context。
// 当存储具备外部版本及相应的 create/update 策略时，context 中会包含 RequestInfo。
func NewNamespaceScopeContext(store *genericregistry.Store, namespace string, subresources ...string) context.Context {
	return newContext(store, namespace, subresources)
}

// newContext 构造带命名空间（以及可用时的 RequestInfo）的 context。
func newContext(store *genericregistry.Store, namespace string, subresources []string) context.Context {
	ctx := genericapirequest.WithNamespace(genericapirequest.NewContext(), namespace)
	if info := priorityRequestInfo(store); info != nil {
		if len(subresources) > 0 {
			info.Subresource = strings.Join(subresources, "/")
		}
		ctx = genericapirequest.WithRequestInfo(ctx, info)
	}
	return ctx
}

// TestNamespace 返回测试使用的命名空间。
func (t *Tester) TestNamespace() string {
	return t.tester.TestNamespace()
}

// ClusterScope 把测试切换为集群级作用域，并返回自身以便链式调用。
func (t *Tester) ClusterScope() *Tester {
	t.tester = t.tester.ClusterScope()
	return t
}

// SetUserInfo 设置测试请求的认证用户。
func (t *Tester) SetUserInfo(userInfo user.Info) {
	t.tester.SetUserInfo(userInfo)
}

// Namer 设置对象命名函数，并返回自身以便链式调用。
func (t *Tester) Namer(namer func(int) string) *Tester {
	t.tester = t.tester.Namer(namer)
	return t
}

// AllowCreateOnUpdate 允许 update 请求在对象不存在时执行 create。
func (t *Tester) AllowCreateOnUpdate() *Tester {
	t.tester = t.tester.AllowCreateOnUpdate()
	return t
}

// GeneratesName 声明存储会为未命名的对象生成名称。
func (t *Tester) GeneratesName() *Tester {
	t.tester = t.tester.GeneratesName()
	return t
}

// ReturnDeletedObject 声明删除操作会返回被删除的对象。
func (t *Tester) ReturnDeletedObject() *Tester {
	t.tester = t.tester.ReturnDeletedObject()
	return t
}

// SetRequestInfo 覆盖自动推断的 RequestInfo。
func (t *Tester) SetRequestInfo(requestInfo *genericapirequest.RequestInfo) *Tester {
	t.tester.SetRequestInfo(requestInfo)
	return t
}

// TestCreate 运行创建相关测试：合法对象应成功，非法对象应被拒绝。
func (t *Tester) TestCreate(valid runtime.Object, invalid ...runtime.Object) {
	t.tester.TestCreate(
		valid,
		t.createObject,
		t.getObject,
		invalid...,
	)
}

// TestUpdate 运行更新相关测试：validUpdateFunc 应成功，invalidUpdateFunc 应被拒绝。
func (t *Tester) TestUpdate(valid runtime.Object, validUpdateFunc UpdateFunc, invalidUpdateFunc ...UpdateFunc) {
	var invalidFuncs []resttest.UpdateFunc
	for _, f := range invalidUpdateFunc {
		invalidFuncs = append(invalidFuncs, resttest.UpdateFunc(f))
	}
	t.tester.TestUpdate(
		valid,
		t.createObject,
		t.getObject,
		resttest.UpdateFunc(validUpdateFunc),
		invalidFuncs...,
	)
}

// TestDelete 运行删除测试，并校验删除后对象为 NotFound。
func (t *Tester) TestDelete(valid runtime.Object) {
	t.tester.TestDelete(
		valid,
		t.createObject,
		t.getObject,
		errors.IsNotFound,
	)
}

// TestDeleteGraceful 运行带优雅删除的测试，校验保留的 grace period。
func (t *Tester) TestDeleteGraceful(valid runtime.Object, expectedGrace int64) {
	t.tester.TestDeleteGraceful(
		valid,
		t.createObject,
		t.getObject,
		expectedGrace,
	)
}

// TestGet 运行读取测试。
func (t *Tester) TestGet(valid runtime.Object) {
	t.tester.TestGet(valid)
}

// TestList 运行列表测试，使用 setObjectsForList 准备数据。
func (t *Tester) TestList(valid runtime.Object) {
	t.tester.TestList(
		valid,
		t.setObjectsForList,
	)
}

// TestWatch 运行 watch 测试，分别验证标签与字段选择器的命中/不命中集合。
func (t *Tester) TestWatch(valid runtime.Object, labelsPass, labelsFail []labels.Set, fieldsPass, fieldsFail []fields.Set) {
	t.tester.TestWatch(
		valid,
		t.emitObject,
		labelsPass,
		labelsFail,
		fieldsPass,
		fieldsFail,
		// TODO: This should be filtered, the registry should not be aware of this level of detail
		[]string{"create", "delete"},
	)
}

// Helper functions

// getObject 按名称从 store 读取对象，供 resttest 校验存储状态。
func (t *Tester) getObject(ctx context.Context, obj runtime.Object) (runtime.Object, error) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}

	result, err := t.storage.Get(ctx, accessor.GetName(), &metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// createObject 绕过 Store 的准入逻辑，直接写入底层存储，用于准备测试数据。
func (t *Tester) createObject(ctx context.Context, obj runtime.Object) error {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	key, err := t.storage.KeyFunc(ctx, accessor.GetName())
	if err != nil {
		return err
	}
	return t.storage.Storage.Create(ctx, key, obj, nil, 0, false)
}

// setObjectsForList 清空集合后写入给定对象列表，保证列表测试从确定状态开始。
func (t *Tester) setObjectsForList(objects []runtime.Object) []runtime.Object {
	key := t.storage.KeyRootFunc(t.tester.TestContext())
	if _, err := t.storage.DeleteCollection(t.tester.TestContext(), rest.ValidateAllObjectFunc, nil, nil); err != nil {
		t.tester.Errorf("unable to clear collection: %v", err)
		return nil
	}
	if err := storagetesting.CreateObjList(key, t.storage.Storage.Storage, objects); err != nil {
		t.tester.Errorf("unexpected error: %v", err)
		return nil
	}
	return objects
}

// emitObject 按 watch 测试要求触发 create 或 delete 事件。
func (t *Tester) emitObject(obj runtime.Object, action string) error {
	ctx := t.tester.TestContext()
	var err error

	switch action {
	case "create":
		err = t.createObject(ctx, obj)
	case "delete":
		var accessor metav1.Object
		accessor, err = meta.Accessor(obj)
		if err != nil {
			return err
		}
		_, _, err = t.storage.Delete(ctx, accessor.GetName(), rest.ValidateAllObjectFunc, nil)
	default:
		err = fmt.Errorf("unexpected action: %v", action)
	}

	return err
}
