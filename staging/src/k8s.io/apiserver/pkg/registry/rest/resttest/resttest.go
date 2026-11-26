/*
Copyright 2014 The Kubernetes Authors.

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

package resttest

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/authentication/user"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
)

// TODO(apelisse): 本文件测试应始终删除自己创建的对象，以提高隔离性，
// 避免名字冲突。

// Tester 是 rest.Storage 的通用一致性测试驱动器：通过组合不同的 Test* 入口，
// 各资源的 registry 测试可以复用同一套行为断言。
//
// 标志位（clusterScope/createOnUpdate/generatesName/returnDeletedObject）用于
// 表达资源的策略差异，避免为每种策略复制测试。
type Tester struct {
	*testing.T
	storage             rest.Storage
	clusterScope        bool
	createOnUpdate      bool
	generatesName       bool
	returnDeletedObject bool
	namer               func(int) string
	userInfo            user.Info
	requestInfo         *genericapirequest.RequestInfo
}

// New 构造 Tester；默认使用 "foo%d" 命名，可通过 Namer 覆盖。
func New(t *testing.T, storage rest.Storage) *Tester {
	return &Tester{
		T:       t,
		storage: storage,
		namer:   defaultNamer,
	}
}

// defaultNamer 生成稳定且互不冲突的对象名。
func defaultNamer(i int) string {
	return fmt.Sprintf("foo%d", i)
}

// Namer 允许提供自定义命名器
// 默认使用 foo%d
func (t *Tester) Namer(namer func(int) string) *Tester {
	t.namer = namer
	return t
}

// ClusterScope 声明被测资源是集群作用域。
func (t *Tester) ClusterScope() *Tester {
	t.clusterScope = true
	return t
}

// AllowCreateOnUpdate 声明资源允许 update 时创建（create-on-update）。
func (t *Tester) AllowCreateOnUpdate() *Tester {
	t.createOnUpdate = true
	return t
}

// GeneratesName 声明资源会根据 generateName 生成名字。
func (t *Tester) GeneratesName() *Tester {
	t.generatesName = true
	return t
}

// ReturnDeletedObject 声明删除接口会返回被删除对象而非仅 Status。
func (t *Tester) ReturnDeletedObject() *Tester {
	t.returnDeletedObject = true
	return t
}

// SetRequestInfo 设置存储操作调用时 context 中应携带的 RequestInfo。
func (t *Tester) SetRequestInfo(requestInfo *genericapirequest.RequestInfo) {
	t.requestInfo = requestInfo
}

// TestNamespace 返回创建 context 时使用的命名空间。
// 集群作用域对象返回 NamespaceNone。
func (t *Tester) TestNamespace() string {
	if t.clusterScope {
		return metav1.NamespaceNone
	}
	return "test"
}

// SetUserInfo 设置存储操作调用时 context 中应携带的 UserInfo。
func (t *Tester) SetUserInfo(userInfo user.Info) {
	t.userInfo = userInfo
}

// TestContext 返回存储调用时使用的带命名空间的 context。
// 命名空间由 TestNamespace() 决定
func (t *Tester) TestContext() context.Context {
	ctx := genericapirequest.NewContext()
	ctx = genericapirequest.WithNamespace(ctx, t.TestNamespace())
	if t.userInfo != nil {
		ctx = genericapirequest.WithUser(ctx, t.userInfo)
	}
	if t.requestInfo != nil {
		ctx = genericapirequest.WithRequestInfo(ctx, t.requestInfo)
	}
	return ctx
}

// getObjectMetaOrFail 取 ObjectMeta；非元数据对象直接终止测试。
func (t *Tester) getObjectMetaOrFail(obj runtime.Object) metav1.Object {
	objMeta, err := meta.Accessor(obj)
	if err != nil {
		t.Fatalf("object does not have ObjectMeta: %v\n%#v", err, obj)
	}
	return objMeta
}

// setObjectMeta 为测试对象设置名称/命名空间并清理客户端可控的系统字段，
// 模拟 API server 在创建前的规范化行为。
func (t *Tester) setObjectMeta(obj runtime.Object, name string) {
	meta := t.getObjectMetaOrFail(obj)
	meta.SetName(name)
	if t.clusterScope {
		meta.SetNamespace(metav1.NamespaceNone)
	} else {
		meta.SetNamespace(genericapirequest.NamespaceValue(t.TestContext()))
	}
	meta.SetGenerateName("")
	meta.SetGeneration(1)
}

type AssignFunc func([]runtime.Object) []runtime.Object
type EmitFunc func(runtime.Object, string) error
type GetFunc func(context.Context, runtime.Object) (runtime.Object, error)
type InitWatchFunc func()
type InjectErrFunc func(err error)
type IsErrorFunc func(err error) bool
type CreateFunc func(context.Context, runtime.Object) error
type SetRVFunc func(uint64)
type UpdateFunc func(runtime.Object) runtime.Object

// 测试创建对象。
//
// 覆盖：元数据填充、名称生成、dry-run 不落盘且结果一致、重复创建、命名空间
// 处理（集群/命名空间作用域行为不同）、校验与非法名称。
func (t *Tester) TestCreate(valid runtime.Object, createFn CreateFunc, getFn GetFunc, invalid ...runtime.Object) {
	dryRunOpts := metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}}
	opts := metav1.CreateOptions{}
	t.testCreateHasMetadata(valid.DeepCopyObject())
	if !t.generatesName {
		t.testCreateGeneratesName(valid.DeepCopyObject())
	}
	t.testCreateDryRun(valid.DeepCopyObject(), getFn)
	t.testCreateDryRunEquals(valid.DeepCopyObject())
	t.testCreateEquals(valid.DeepCopyObject(), getFn)
	t.testCreateAlreadyExisting(valid.DeepCopyObject(), createFn, dryRunOpts)
	t.testCreateAlreadyExisting(valid.DeepCopyObject(), createFn, opts)
	if t.clusterScope {
		t.testCreateDiscardsObjectNamespace(valid.DeepCopyObject(), dryRunOpts)
		t.testCreateDiscardsObjectNamespace(valid.DeepCopyObject(), opts)
		t.testCreateIgnoresContextNamespace(valid.DeepCopyObject(), dryRunOpts)
		t.testCreateIgnoresContextNamespace(valid.DeepCopyObject(), opts)
		t.testCreateIgnoresMismatchedNamespace(valid.DeepCopyObject(), dryRunOpts)
		t.testCreateIgnoresMismatchedNamespace(valid.DeepCopyObject(), opts)
		t.testCreateResetsUserData(valid.DeepCopyObject(), dryRunOpts)
		t.testCreateResetsUserData(valid.DeepCopyObject(), opts)
	} else {
		t.testCreateRejectsMismatchedNamespace(valid.DeepCopyObject(), dryRunOpts)
		t.testCreateRejectsMismatchedNamespace(valid.DeepCopyObject(), opts)
	}
	t.testCreateInvokesValidation(dryRunOpts, invalid...)
	t.testCreateInvokesValidation(opts, invalid...)
	t.testCreateValidatesNames(valid.DeepCopyObject(), dryRunOpts)
	t.testCreateValidatesNames(valid.DeepCopyObject(), opts)
}

// 测试更新对象。
//
// 覆盖：更新结果一致性、资源版本过旧冲突、对象不存在、命名空间不匹配、
// 校验失败、UID 不匹配、旧对象传递、UpdatedObject 错误传播、generation 处理。
func (t *Tester) TestUpdate(valid runtime.Object, createFn CreateFunc, getFn GetFunc, updateFn UpdateFunc, invalidUpdateFn ...UpdateFunc) {
	dryRunOpts := metav1.UpdateOptions{DryRun: []string{metav1.DryRunAll}}
	opts := metav1.UpdateOptions{}
	t.testUpdateEquals(valid.DeepCopyObject(), createFn, getFn, updateFn)
	t.testUpdateFailsOnVersionTooOld(valid.DeepCopyObject(), createFn, getFn)
	t.testUpdateOnNotFound(valid.DeepCopyObject(), dryRunOpts)
	t.testUpdateOnNotFound(valid.DeepCopyObject(), opts)
	if !t.clusterScope {
		t.testUpdateRejectsMismatchedNamespace(valid.DeepCopyObject(), createFn, getFn)
	}
	t.testUpdateInvokesValidation(valid.DeepCopyObject(), createFn, invalidUpdateFn...)
	t.testUpdateWithWrongUID(valid.DeepCopyObject(), createFn, getFn, dryRunOpts)
	t.testUpdateWithWrongUID(valid.DeepCopyObject(), createFn, getFn, opts)
	t.testUpdateRetrievesOldObject(valid.DeepCopyObject(), createFn, getFn)
	t.testUpdatePropagatesUpdatedObjectError(valid.DeepCopyObject(), createFn, getFn, dryRunOpts)
	t.testUpdatePropagatesUpdatedObjectError(valid.DeepCopyObject(), createFn, getFn, opts)
	t.testUpdateIgnoreGenerationUpdates(valid.DeepCopyObject(), createFn, getFn)
}

// 测试删除对象。
//
// 覆盖：删除不存在对象、非优雅删除（含 dry-run）、UID/ResourceVersion 前置条件。
func (t *Tester) TestDelete(valid runtime.Object, createFn CreateFunc, getFn GetFunc, isNotFoundFn IsErrorFunc) {
	dryRunOpts := metav1.DeleteOptions{DryRun: []string{metav1.DryRunAll}}
	opts := metav1.DeleteOptions{}
	t.testDeleteNonExist(valid.DeepCopyObject(), dryRunOpts)
	t.testDeleteNonExist(valid.DeepCopyObject(), opts)
	t.testDeleteNoGraceful(valid.DeepCopyObject(), createFn, getFn, isNotFoundFn, true)
	t.testDeleteNoGraceful(valid.DeepCopyObject(), createFn, getFn, isNotFoundFn, false)
	t.testDeleteWithUID(valid.DeepCopyObject(), createFn, getFn, isNotFoundFn, dryRunOpts)
	t.testDeleteWithUID(valid.DeepCopyObject(), createFn, getFn, isNotFoundFn, opts)
	t.testDeleteWithResourceVersion(valid.DeepCopyObject(), createFn, getFn, isNotFoundFn, dryRunOpts)
	t.testDeleteWithResourceVersion(valid.DeepCopyObject(), createFn, getFn, isNotFoundFn, opts)
}

// 测试优雅删除对象。
//
// 覆盖宽限期默认值、显式值、nil 视为 0、延长/缩短宽限期、立即删除。
func (t *Tester) TestDeleteGraceful(valid runtime.Object, createFn CreateFunc, getFn GetFunc, expectedGrace int64) {
	t.testDeleteDryRunGracefulHasdefault(valid.DeepCopyObject(), createFn, expectedGrace)
	t.testDeleteGracefulHasDefault(valid.DeepCopyObject(), createFn, getFn, expectedGrace)
	t.testDeleteGracefulWithValue(valid.DeepCopyObject(), createFn, getFn, expectedGrace)
	t.testDeleteGracefulUsesZeroOnNil(valid.DeepCopyObject(), createFn, expectedGrace)
	t.testDeleteGracefulExtend(valid.DeepCopyObject(), createFn, getFn, expectedGrace)
	t.testDeleteGracefulShorten(valid.DeepCopyObject(), createFn, getFn, expectedGrace)
	t.testDeleteGracefulImmediate(valid.DeepCopyObject(), createFn, getFn, expectedGrace)
}

// 测试获取对象。
//
// 覆盖存在/不存在、命名空间不匹配，以及命名空间作用域资源的跨命名空间访问。
func (t *Tester) TestGet(valid runtime.Object) {
	t.testGetFound(valid.DeepCopyObject())
	t.testGetNotFound(valid.DeepCopyObject())
	t.testGetMimatchedNamespace(valid.DeepCopyObject())
	if !t.clusterScope {
		t.testGetDifferentNamespace(valid.DeepCopyObject())
	}
}

// 测试列举对象。
//
// 覆盖空列表、正常列表、标签选择器与 Table 转换。
func (t *Tester) TestList(valid runtime.Object, assignFn AssignFunc) {
	t.testListNotFound(assignFn)
	t.testListFound(valid.DeepCopyObject(), assignFn)
	t.testListMatchLabels(valid.DeepCopyObject(), assignFn)
	t.testListTableConversion(valid.DeepCopyObject(), assignFn)
}

// 测试 watch 对象。
//
// 分别用标签与字段选择器验证 watch 的过滤语义：pass 集合必须收到事件，
// fail 集合必须收不到。
func (t *Tester) TestWatch(
	valid runtime.Object, emitFn EmitFunc,
	labelsPass, labelsFail []labels.Set, fieldsPass, fieldsFail []fields.Set, actions []string) {
	t.testWatchLabels(valid.DeepCopyObject(), emitFn, labelsPass, labelsFail, actions)
	t.testWatchFields(valid.DeepCopyObject(), emitFn, fieldsPass, fieldsFail, actions)
}

// =============================================================================
// 创建测试。

func (t *Tester) delete(ctx context.Context, obj runtime.Object) error {
	objectMeta, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	deleter, ok := t.storage.(rest.GracefulDeleter)
	if !ok {
		return fmt.Errorf("Expected deleting storage, got %v", t.storage)
	}
	_, _, err = deleter.Delete(ctx, objectMeta.GetName(), rest.ValidateAllObjectFunc, nil)
	return err
}

func (t *Tester) testCreateAlreadyExisting(obj runtime.Object, createFn CreateFunc, opts metav1.CreateOptions) {
	ctx := t.TestContext()

	foo := obj.DeepCopyObject()
	t.setObjectMeta(foo, t.namer(1))
	if err := createFn(ctx, foo); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer t.delete(ctx, foo)

	_, err := t.storage.(rest.Creater).Create(ctx, foo, rest.ValidateAllObjectFunc, &opts)
	if !errors.IsAlreadyExists(err) {
		t.Errorf("expected already exists err, got %v", err)
	}
}

func (t *Tester) testCreateDryRun(obj runtime.Object, getFn GetFunc) {
	ctx := t.TestContext()

	foo := obj.DeepCopyObject()
	t.setObjectMeta(foo, t.namer(2))

	_, err := t.storage.(rest.Creater).Create(ctx, foo, rest.ValidateAllObjectFunc, &metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	_, err = getFn(ctx, foo)
	if !errors.IsNotFound(err) {
		t.Errorf("Expected NotFound error, got '%v'", err)
	}
}

func (t *Tester) testCreateDryRunEquals(obj runtime.Object) {
	ctx := t.TestContext()

	foo := obj.DeepCopyObject()
	t.setObjectMeta(foo, t.namer(2))

	createdFake, err := t.storage.(rest.Creater).Create(ctx, foo, rest.ValidateAllObjectFunc, &metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	created, err := t.storage.(rest.Creater).Create(ctx, foo, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer t.delete(ctx, created)

	// 设置创建结果中可能未设置的 resource version。
	createdMeta := t.getObjectMetaOrFail(created)
	createdFakeMeta := t.getObjectMetaOrFail(createdFake)
	createdMeta.SetCreationTimestamp(createdFakeMeta.GetCreationTimestamp())
	createdFakeMeta.SetResourceVersion("")
	createdMeta.SetResourceVersion("")
	createdMeta.SetUID(createdFakeMeta.GetUID())

	if e, a := created, createdFake; !apiequality.Semantic.DeepEqual(e, a) {
		t.Errorf("unexpected obj: %#v, expected %#v", e, a)
	}
}

func (t *Tester) testCreateEquals(obj runtime.Object, getFn GetFunc) {
	ctx := t.TestContext()

	foo := obj.DeepCopyObject()
	t.setObjectMeta(foo, t.namer(2))

	created, err := t.storage.(rest.Creater).Create(ctx, foo, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer t.delete(ctx, created)

	got, err := getFn(ctx, foo)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	// 设置创建结果中可能未设置的 resource version。
	createdMeta := t.getObjectMetaOrFail(created)
	gotMeta := t.getObjectMetaOrFail(got)
	createdMeta.SetResourceVersion(gotMeta.GetResourceVersion())

	if e, a := created, got; !apiequality.Semantic.DeepEqual(e, a) {
		t.Errorf("unexpected obj: %#v, expected %#v", e, a)
	}
}

func (t *Tester) testCreateDiscardsObjectNamespace(valid runtime.Object, opts metav1.CreateOptions) {
	objectMeta := t.getObjectMetaOrFail(valid)

	// 忽略对象元数据中非空的 namespace
	objectMeta.SetNamespace("not-default")

	// 理想情况应报错，但至少要验证 namespace 没被持久化
	created, err := t.storage.(rest.Creater).Create(t.TestContext(), valid.DeepCopyObject(), rest.ValidateAllObjectFunc, &opts)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer t.delete(t.TestContext(), created)
	createdObjectMeta := t.getObjectMetaOrFail(created)
	if createdObjectMeta.GetNamespace() != metav1.NamespaceNone {
		t.Errorf("Expected empty namespace on created object, got '%v'", createdObjectMeta.GetNamespace())
	}
}

func (t *Tester) testCreateGeneratesName(valid runtime.Object) {
	objectMeta := t.getObjectMetaOrFail(valid)
	objectMeta.SetName("")
	objectMeta.SetGenerateName("test-")

	created, err := t.storage.(rest.Creater).Create(t.TestContext(), valid, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer t.delete(t.TestContext(), created)
	createdMeta := t.getObjectMetaOrFail(created)
	if createdMeta.GetName() == "test-" || !strings.HasPrefix(createdMeta.GetName(), "test-") {
		t.Errorf("unexpected name: %#v", valid)
	}
}

func (t *Tester) testCreateHasMetadata(valid runtime.Object) {
	objectMeta := t.getObjectMetaOrFail(valid)
	objectMeta.SetName(t.namer(1))
	objectMeta.SetNamespace(t.TestNamespace())

	obj, err := t.storage.(rest.Creater).Create(t.TestContext(), valid, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if obj == nil {
		t.Fatalf("Unexpected object from result: %#v", obj)
	}
	defer t.delete(t.TestContext(), obj)
	createdMeta := t.getObjectMetaOrFail(obj)
	if !metav1.HasObjectMetaSystemFieldValues(createdMeta) {
		t.Errorf("storage did not populate object meta field values")
	}
}

func (t *Tester) testCreateIgnoresContextNamespace(valid runtime.Object, opts metav1.CreateOptions) {
	// 忽略 context 中非空的 namespace
	ctx := genericapirequest.WithNamespace(genericapirequest.NewContext(), "not-default2")
	if t.requestInfo != nil {
		ctx = genericapirequest.WithRequestInfo(ctx, t.requestInfo)
	}

	// 理想情况应报错，但至少要验证 namespace 没被持久化
	created, err := t.storage.(rest.Creater).Create(ctx, valid.DeepCopyObject(), rest.ValidateAllObjectFunc, &opts)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer t.delete(ctx, created)
	createdObjectMeta := t.getObjectMetaOrFail(created)
	if createdObjectMeta.GetNamespace() != metav1.NamespaceNone {
		t.Errorf("Expected empty namespace on created object, got '%v'", createdObjectMeta.GetNamespace())
	}
}

func (t *Tester) testCreateIgnoresMismatchedNamespace(valid runtime.Object, opts metav1.CreateOptions) {
	objectMeta := t.getObjectMetaOrFail(valid)

	// 忽略对象元数据中非空的 namespace
	objectMeta.SetNamespace("not-default")
	ctx := genericapirequest.WithNamespace(genericapirequest.NewContext(), "not-default2")
	if t.requestInfo != nil {
		ctx = genericapirequest.WithRequestInfo(ctx, t.requestInfo)
	}

	// 理想情况应报错，但至少要验证 namespace 没被持久化
	created, err := t.storage.(rest.Creater).Create(ctx, valid.DeepCopyObject(), rest.ValidateAllObjectFunc, &opts)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer t.delete(ctx, created)
	createdObjectMeta := t.getObjectMetaOrFail(created)
	if createdObjectMeta.GetNamespace() != metav1.NamespaceNone {
		t.Errorf("Expected empty namespace on created object, got '%v'", createdObjectMeta.GetNamespace())
	}
}

func (t *Tester) testCreateValidatesNames(valid runtime.Object, opts metav1.CreateOptions) {
	invalidPathElements := []string{".", ".."}
	for _, invalidName := range invalidPathElements {
		objCopy := valid.DeepCopyObject()
		objCopyMeta := t.getObjectMetaOrFail(objCopy)
		objCopyMeta.SetName(invalidName)

		ctx := t.TestContext()
		_, err := t.storage.(rest.Creater).Create(ctx, objCopy, rest.ValidateAllObjectFunc, &opts)
		if !errors.IsInvalid(err) {
			t.Errorf("%s: Expected to get an invalid resource error, got '%v'", invalidName, err)
		}
	}

	invalidInPath := []string{"/", "%"}
	for _, invalidSuffix := range invalidInPath {
		objCopy := valid.DeepCopyObject()
		objCopyMeta := t.getObjectMetaOrFail(objCopy)
		objCopyMeta.SetName(objCopyMeta.GetName() + invalidSuffix)

		ctx := t.TestContext()
		_, err := t.storage.(rest.Creater).Create(ctx, objCopy, rest.ValidateAllObjectFunc, &opts)
		if !errors.IsInvalid(err) {
			t.Errorf("%s: Expected to get an invalid resource error, got '%v'", invalidSuffix, err)
		}
	}
}

func (t *Tester) testCreateInvokesValidation(opts metav1.CreateOptions, invalid ...runtime.Object) {
	for i, obj := range invalid {
		ctx := t.TestContext()
		_, err := t.storage.(rest.Creater).Create(ctx, obj, rest.ValidateAllObjectFunc, &opts)
		if !errors.IsInvalid(err) {
			t.Errorf("%d: Expected to get an invalid resource error, got %v", i, err)
		}
	}
}

func (t *Tester) testCreateRejectsMismatchedNamespace(valid runtime.Object, opts metav1.CreateOptions) {
	objectMeta := t.getObjectMetaOrFail(valid)
	objectMeta.SetNamespace("not-default")

	_, err := t.storage.(rest.Creater).Create(t.TestContext(), valid, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
	if err == nil {
		t.Errorf("Expected an error, but we didn't get one")
	} else if !strings.Contains(err.Error(), "does not match the namespace sent on the request") {
		t.Errorf("Expected 'does not match the namespace sent on the request' error, got '%v'", err.Error())
	}
}

func (t *Tester) testCreateResetsUserData(valid runtime.Object, opts metav1.CreateOptions) {
	objectMeta := t.getObjectMetaOrFail(valid)
	now := metav1.Now()
	objectMeta.SetUID("bad-uid")
	objectMeta.SetCreationTimestamp(now)

	obj, err := t.storage.(rest.Creater).Create(t.TestContext(), valid, rest.ValidateAllObjectFunc, &opts)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if obj == nil {
		t.Fatalf("Unexpected object from result: %#v", obj)
	}
	defer t.delete(t.TestContext(), obj)
	createdMeta := t.getObjectMetaOrFail(obj)
	if createdMeta.GetUID() == "bad-uid" || createdMeta.GetCreationTimestamp() == now {
		t.Errorf("ObjectMeta did not reset basic fields: %#v", objectMeta)
	}
}

// =============================================================================
// 更新测试。

func (t *Tester) testUpdateEquals(obj runtime.Object, createFn CreateFunc, getFn GetFunc, updateFn UpdateFunc) {
	ctx := t.TestContext()

	foo := obj.DeepCopyObject()
	t.setObjectMeta(foo, t.namer(2))
	if err := createFn(ctx, foo); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	toUpdate, err := getFn(ctx, foo)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	toUpdate = updateFn(toUpdate)
	toUpdateMeta := t.getObjectMetaOrFail(toUpdate)
	updated, created, err := t.storage.(rest.Updater).Update(ctx, toUpdateMeta.GetName(), rest.DefaultUpdatedObjectInfo(toUpdate), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if created {
		t.Errorf("unexpected creation")
	}
	got, err := getFn(ctx, foo)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	// 设置创建结果中可能未设置的 resource version。
	updatedMeta := t.getObjectMetaOrFail(updated)
	gotMeta := t.getObjectMetaOrFail(got)
	updatedMeta.SetResourceVersion(gotMeta.GetResourceVersion())

	if e, a := updated, got; !apiequality.Semantic.DeepEqual(e, a) {
		t.Errorf("unexpected obj: %#v, expected %#v", e, a)
	}
}

func (t *Tester) testUpdateFailsOnVersionTooOld(obj runtime.Object, createFn CreateFunc, getFn GetFunc) {
	ctx := t.TestContext()

	foo := obj.DeepCopyObject()
	t.setObjectMeta(foo, t.namer(3))

	if err := createFn(ctx, foo); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	storedFoo, err := getFn(ctx, foo)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	older := storedFoo.DeepCopyObject()
	olderMeta := t.getObjectMetaOrFail(older)
	olderMeta.SetResourceVersion("1")

	_, _, err = t.storage.(rest.Updater).Update(t.TestContext(), olderMeta.GetName(), rest.DefaultUpdatedObjectInfo(older), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{})
	if err == nil {
		t.Errorf("Expected an error, but we didn't get one")
	} else if !errors.IsConflict(err) {
		t.Errorf("Expected Conflict error, got '%v'", err)
	}
}

func (t *Tester) testUpdateInvokesValidation(obj runtime.Object, createFn CreateFunc, invalidUpdateFn ...UpdateFunc) {
	ctx := t.TestContext()

	foo := obj.DeepCopyObject()
	t.setObjectMeta(foo, t.namer(4))
	if err := createFn(ctx, foo); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	for _, update := range invalidUpdateFn {
		toUpdate := update(foo.DeepCopyObject())
		toUpdateMeta := t.getObjectMetaOrFail(toUpdate)
		got, created, err := t.storage.(rest.Updater).Update(t.TestContext(), toUpdateMeta.GetName(), rest.DefaultUpdatedObjectInfo(toUpdate), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{})
		if got != nil || created {
			t.Errorf("expected nil object and no creation for object: %v", toUpdate)
		}
		if !errors.IsInvalid(err) && !errors.IsBadRequest(err) {
			t.Errorf("expected invalid or bad request error, got %v", err)
		}
	}
}

func (t *Tester) testUpdateWithWrongUID(obj runtime.Object, createFn CreateFunc, getFn GetFunc, opts metav1.UpdateOptions) {
	ctx := t.TestContext()
	foo := obj.DeepCopyObject()
	t.setObjectMeta(foo, t.namer(5))
	objectMeta := t.getObjectMetaOrFail(foo)
	objectMeta.SetUID(types.UID("UID0000"))
	if err := createFn(ctx, foo); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer t.delete(ctx, foo)
	objectMeta.SetUID(types.UID("UID1111"))

	obj, created, err := t.storage.(rest.Updater).Update(ctx, objectMeta.GetName(), rest.DefaultUpdatedObjectInfo(foo), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &opts)
	if created || obj != nil {
		t.Errorf("expected nil object and no creation for object: %v", foo)
	}
	if err == nil || !errors.IsConflict(err) {
		t.Errorf("unexpected error: %v", err)
	}
}

func (t *Tester) testUpdateRetrievesOldObject(obj runtime.Object, createFn CreateFunc, getFn GetFunc) {
	ctx := t.TestContext()
	foo := obj.DeepCopyObject()
	t.setObjectMeta(foo, t.namer(6))
	objectMeta := t.getObjectMetaOrFail(foo)
	objectMeta.SetAnnotations(map[string]string{"A": "1"})
	if err := createFn(ctx, foo); err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	storedFoo, err := getFn(ctx, foo)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	storedFooWithUpdates := storedFoo.DeepCopyObject()
	objectMeta = t.getObjectMetaOrFail(storedFooWithUpdates)
	objectMeta.SetAnnotations(map[string]string{"A": "2"})

	// 确保自定义 transform 被调用，且看到期望的 updatedObject 与 oldObject
	// 这测试的是把新旧对象传给准入的机制
	calledUpdatedObject := 0
	noopTransform := func(_ context.Context, updatedObject runtime.Object, oldObject runtime.Object) (runtime.Object, error) {
		if !reflect.DeepEqual(storedFoo, oldObject) {
			t.Errorf("Expected\n\t%#v\ngot\n\t%#v", storedFoo, oldObject)
		}
		if !reflect.DeepEqual(storedFooWithUpdates, updatedObject) {
			t.Errorf("Expected\n\t%#v\ngot\n\t%#v", storedFooWithUpdates, updatedObject)
		}
		calledUpdatedObject++
		return updatedObject, nil
	}

	updatedObj, created, err := t.storage.(rest.Updater).Update(ctx, objectMeta.GetName(), rest.DefaultUpdatedObjectInfo(storedFooWithUpdates, noopTransform), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if created {
		t.Errorf("expected no creation for object")
		return
	}
	if updatedObj == nil {
		t.Errorf("expected non-nil object from update")
		return
	}
	if calledUpdatedObject != 1 {
		t.Errorf("expected UpdatedObject() to be called 1 time, was called %d", calledUpdatedObject)
		return
	}
}

func (t *Tester) testUpdatePropagatesUpdatedObjectError(obj runtime.Object, createFn CreateFunc, getFn GetFunc, opts metav1.UpdateOptions) {
	ctx := t.TestContext()
	foo := obj.DeepCopyObject()
	name := t.namer(7)
	t.setObjectMeta(foo, name)
	if err := createFn(ctx, foo); err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	defer t.delete(ctx, foo)

	// Make sure our transform is called, and sees the expected updatedObject and oldObject
	propagateErr := fmt.Errorf("custom updated object error for %v", foo)
	noopTransform := func(_ context.Context, updatedObject runtime.Object, oldObject runtime.Object) (runtime.Object, error) {
		return nil, propagateErr
	}

	_, _, err := t.storage.(rest.Updater).Update(ctx, name, rest.DefaultUpdatedObjectInfo(foo, noopTransform), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &opts)
	if err != propagateErr {
		t.Errorf("expected propagated error, got %#v", err)
	}
}

func (t *Tester) testUpdateIgnoreGenerationUpdates(obj runtime.Object, createFn CreateFunc, getFn GetFunc) {
	ctx := t.TestContext()

	foo := obj.DeepCopyObject()
	name := t.namer(8)
	t.setObjectMeta(foo, name)

	if err := createFn(ctx, foo); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	storedFoo, err := getFn(ctx, foo)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	older := storedFoo.DeepCopyObject()
	olderMeta := t.getObjectMetaOrFail(older)
	olderMeta.SetGeneration(2)

	_, _, err = t.storage.(rest.Updater).Update(t.TestContext(), olderMeta.GetName(), rest.DefaultUpdatedObjectInfo(older), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	updatedFoo, err := getFn(ctx, older)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if exp, got := int64(1), t.getObjectMetaOrFail(updatedFoo).GetGeneration(); exp != got {
		t.Errorf("Unexpected generation update: expected %d, got %d", exp, got)
	}
}

func (t *Tester) testUpdateOnNotFound(obj runtime.Object, opts metav1.UpdateOptions) {
	t.setObjectMeta(obj, t.namer(0))
	_, created, err := t.storage.(rest.Updater).Update(t.TestContext(), t.namer(0), rest.DefaultUpdatedObjectInfo(obj), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &opts)
	if t.createOnUpdate {
		if err != nil {
			t.Errorf("creation allowed on updated, but got an error: %v", err)
		}
		if !created {
			t.Errorf("creation allowed on update, but object not created")
		}
	} else {
		if err == nil {
			t.Errorf("Expected an error, but we didn't get one")
		} else if !errors.IsNotFound(err) {
			t.Errorf("Expected NotFound error, got '%v'", err)
		}
	}
}

func (t *Tester) testUpdateRejectsMismatchedNamespace(obj runtime.Object, createFn CreateFunc, getFn GetFunc) {
	ctx := t.TestContext()

	foo := obj.DeepCopyObject()
	t.setObjectMeta(foo, t.namer(1))
	if err := createFn(ctx, foo); err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	storedFoo, err := getFn(ctx, foo)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	objectMeta := t.getObjectMetaOrFail(storedFoo)
	objectMeta.SetName(t.namer(1))
	objectMeta.SetNamespace("not-default")

	obj, updated, err := t.storage.(rest.Updater).Update(t.TestContext(), "foo1", rest.DefaultUpdatedObjectInfo(storedFoo), rest.ValidateAllObjectFunc, rest.ValidateAllObjectUpdateFunc, false, &metav1.UpdateOptions{})
	if obj != nil || updated {
		t.Errorf("expected nil object and not updated")
	}
	if err == nil {
		t.Errorf("expected an error, but didn't get one")
	} else if !strings.Contains(err.Error(), "does not match the namespace sent on the request") {
		t.Errorf("expected 'does not match the namespace sent on the request' error, got '%v'", err.Error())
	}
}

// =============================================================================
// 删除测试。

func (t *Tester) testDeleteNoGraceful(obj runtime.Object, createFn CreateFunc, getFn GetFunc, isNotFoundFn IsErrorFunc, dryRun bool) {
	ctx := t.TestContext()

	foo := obj.DeepCopyObject()
	t.setObjectMeta(foo, t.namer(1))
	if err := createFn(ctx, foo); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	defer t.delete(ctx, foo)
	objectMeta := t.getObjectMetaOrFail(foo)
	opts := metav1.NewDeleteOptions(10)
	if dryRun {
		opts.DryRun = []string{metav1.DryRunAll}
	}
	obj, wasDeleted, err := t.storage.(rest.GracefulDeleter).Delete(ctx, objectMeta.GetName(), rest.ValidateAllObjectFunc, opts)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !wasDeleted {
		t.Errorf("unexpected, object %s should have been deleted immediately", objectMeta.GetName())
	}
	if !t.returnDeletedObject {
		if status, ok := obj.(*metav1.Status); !ok {
			t.Errorf("expected status of delete, got %v", status)
		} else if status.Status != metav1.StatusSuccess {
			t.Errorf("expected success, got: %v", status.Status)
		}
	}

	_, err = getFn(ctx, foo)
	if !dryRun && (err == nil || !isNotFoundFn(err)) {
		t.Errorf("unexpected error: %v", err)
	} else if dryRun && isNotFoundFn(err) {
		t.Error("object should not have been removed in dry-run")
	}
}

func (t *Tester) testDeleteNonExist(obj runtime.Object, opts metav1.DeleteOptions) {
	objectMeta := t.getObjectMetaOrFail(obj)

	_, _, err := t.storage.(rest.GracefulDeleter).Delete(t.TestContext(), objectMeta.GetName(), rest.ValidateAllObjectFunc, &opts)
	if err == nil || !errors.IsNotFound(err) {
		t.Errorf("unexpected error: %v", err)
	}

}

// 这测试快速失败路径。前置条件在删除前再次校验的场景
// 在 pkg/storage/etcd 的测试中覆盖。
func (t *Tester) testDeleteWithUID(obj runtime.Object, createFn CreateFunc, getFn GetFunc, isNotFoundFn IsErrorFunc, opts metav1.DeleteOptions) {
	ctx := t.TestContext()

	foo := obj.DeepCopyObject()
	t.setObjectMeta(foo, t.namer(1))
	objectMeta := t.getObjectMetaOrFail(foo)
	objectMeta.SetUID(types.UID("UID0000"))
	if err := createFn(ctx, foo); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	opts.Preconditions = metav1.NewPreconditionDeleteOptions("UID1111").Preconditions
	_, _, err := t.storage.(rest.GracefulDeleter).Delete(ctx, objectMeta.GetName(), rest.ValidateAllObjectFunc, &opts)
	if err == nil || !errors.IsConflict(err) {
		t.Errorf("unexpected error: %v", err)
	}

	obj, _, err = t.storage.(rest.GracefulDeleter).Delete(ctx, objectMeta.GetName(), rest.ValidateAllObjectFunc, metav1.NewPreconditionDeleteOptions("UID0000"))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !t.returnDeletedObject {
		if status, ok := obj.(*metav1.Status); !ok {
			t.Errorf("expected status of delete, got %v", status)
		} else if status.Status != metav1.StatusSuccess {
			t.Errorf("expected success, got: %v", status.Status)
		}
	}

	_, err = getFn(ctx, foo)
	if err == nil || !isNotFoundFn(err) {
		t.Errorf("unexpected error: %v", err)
	}
}

// 这测试快速失败路径。前置条件在删除前再次校验的场景
// 在 pkg/storage/etcd 的测试中覆盖。
func (t *Tester) testDeleteWithResourceVersion(obj runtime.Object, createFn CreateFunc, getFn GetFunc, isNotFoundFn IsErrorFunc, opts metav1.DeleteOptions) {
	ctx := t.TestContext()

	foo := obj.DeepCopyObject()
	t.setObjectMeta(foo, t.namer(1))
	if err := createFn(ctx, foo); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	newObj, err := getFn(ctx, foo)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	objectMeta := t.getObjectMetaOrFail(newObj)

	opts.Preconditions = metav1.NewRVDeletionPrecondition("wrongVersion").Preconditions
	_, wasDeleted, err := t.storage.(rest.GracefulDeleter).Delete(ctx, objectMeta.GetName(), rest.ValidateAllObjectFunc, &opts)
	if err == nil || !errors.IsConflict(err) {
		t.Errorf("unexpected error: %v", err)
	}
	if wasDeleted {
		t.Errorf("unexpected, object %s should not have been deleted immediately", objectMeta.GetName())
	}
	obj, _, err = t.storage.(rest.GracefulDeleter).Delete(ctx, objectMeta.GetName(), rest.ValidateAllObjectFunc, metav1.NewRVDeletionPrecondition(objectMeta.GetResourceVersion()))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	if !t.returnDeletedObject {
		if status, ok := obj.(*metav1.Status); !ok {
			t.Errorf("expected status of delete, got %v", status)
		} else if status.Status != metav1.StatusSuccess {
			t.Errorf("expected success, got: %v", status.Status)
		}
	}

	_, err = getFn(ctx, foo)
	if err == nil || !isNotFoundFn(err) {
		t.Errorf("unexpected error: %v", err)
	}
}

// =============================================================================
// 优雅删除测试。

// testDeleteDryRunGracefulHasdefault 验证 dry-run 删除会返回带 deletionTimestamp
// 与默认宽限期的对象，但不会真正移除。
func (t *Tester) testDeleteDryRunGracefulHasdefault(obj runtime.Object, createFn CreateFunc, expectedGrace int64) {
	ctx := t.TestContext()

	foo := obj.DeepCopyObject()
	t.setObjectMeta(foo, t.namer(1))
	defer t.delete(ctx, foo)
	if err := createFn(ctx, foo); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	objectMeta := t.getObjectMetaOrFail(foo)
	object, wasDeleted, err := t.storage.(rest.GracefulDeleter).Delete(ctx, objectMeta.GetName(), rest.ValidateAllObjectFunc, &metav1.DeleteOptions{DryRun: []string{metav1.DryRunAll}})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if wasDeleted {
		t.Errorf("unexpected, object %s should not have been deleted immediately", objectMeta.GetName())
	}
	objectMeta = t.getObjectMetaOrFail(object)
	if objectMeta.GetDeletionTimestamp() == nil || objectMeta.GetDeletionGracePeriodSeconds() == nil || *objectMeta.GetDeletionGracePeriodSeconds() != expectedGrace {
		t.Errorf("unexpected deleted meta: %#v", objectMeta)
	}
	_, _, err = t.storage.(rest.GracefulDeleter).Delete(ctx, objectMeta.GetName(), rest.ValidateAllObjectFunc, &metav1.DeleteOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

// testDeleteGracefulHasDefault 验证未指定宽限期时使用资源默认值，且设置
// deletionTimestamp 会推进 generation。
func (t *Tester) testDeleteGracefulHasDefault(obj runtime.Object, createFn CreateFunc, getFn GetFunc, expectedGrace int64) {
	ctx := t.TestContext()

	foo := obj.DeepCopyObject()
	t.setObjectMeta(foo, t.namer(1))
	if err := createFn(ctx, foo); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	objectMeta := t.getObjectMetaOrFail(foo)
	generation := objectMeta.GetGeneration()
	_, wasDeleted, err := t.storage.(rest.GracefulDeleter).Delete(ctx, objectMeta.GetName(), rest.ValidateAllObjectFunc, &metav1.DeleteOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if wasDeleted {
		t.Errorf("unexpected, object %s should not have been deleted immediately", objectMeta.GetName())
	}
	if _, err := getFn(ctx, foo); err != nil {
		t.Fatalf("did not gracefully delete resource: %v", err)
	}

	object, err := t.storage.(rest.Getter).Get(ctx, objectMeta.GetName(), &metav1.GetOptions{})
	if err != nil {
		t.Fatalf("unexpected error, object should exist: %v", err)
	}
	objectMeta = t.getObjectMetaOrFail(object)
	if objectMeta.GetDeletionTimestamp() == nil || objectMeta.GetDeletionGracePeriodSeconds() == nil || *objectMeta.GetDeletionGracePeriodSeconds() != expectedGrace {
		t.Errorf("unexpected deleted meta: %#v", objectMeta)
	}
	if generation >= objectMeta.GetGeneration() {
		t.Error("Generation wasn't bumped when deletion timestamp was set")
	}
}

// testDeleteGracefulWithValue 验证显式宽限期被采纳。
func (t *Tester) testDeleteGracefulWithValue(obj runtime.Object, createFn CreateFunc, getFn GetFunc, expectedGrace int64) {
	ctx := t.TestContext()

	foo := obj.DeepCopyObject()
	t.setObjectMeta(foo, t.namer(2))
	if err := createFn(ctx, foo); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	objectMeta := t.getObjectMetaOrFail(foo)
	generation := objectMeta.GetGeneration()
	_, wasDeleted, err := t.storage.(rest.GracefulDeleter).Delete(ctx, objectMeta.GetName(), rest.ValidateAllObjectFunc, metav1.NewDeleteOptions(expectedGrace+2))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if wasDeleted {
		t.Errorf("unexpected, object %s should not have been deleted immediately", objectMeta.GetName())
	}
	if _, err := getFn(ctx, foo); err != nil {
		t.Fatalf("did not gracefully delete resource: %v", err)
	}

	object, err := t.storage.(rest.Getter).Get(ctx, objectMeta.GetName(), &metav1.GetOptions{})
	if err != nil {
		t.Errorf("unexpected error, object should exist: %v", err)
	}
	objectMeta = t.getObjectMetaOrFail(object)
	if objectMeta.GetDeletionTimestamp() == nil || objectMeta.GetDeletionGracePeriodSeconds() == nil || *objectMeta.GetDeletionGracePeriodSeconds() != expectedGrace+2 {
		t.Errorf("unexpected deleted meta: %#v", objectMeta)
	}
	if generation >= objectMeta.GetGeneration() {
		t.Error("Generation wasn't bumped when deletion timestamp was set")
	}
}

// testDeleteGracefulExtend 验证第二次删除请求的宽限期被忽略，已设置的宽限期保持不变。
func (t *Tester) testDeleteGracefulExtend(obj runtime.Object, createFn CreateFunc, getFn GetFunc, expectedGrace int64) {
	ctx := t.TestContext()

	foo := obj.DeepCopyObject()
	t.setObjectMeta(foo, t.namer(3))
	if err := createFn(ctx, foo); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	objectMeta := t.getObjectMetaOrFail(foo)
	generation := objectMeta.GetGeneration()
	_, wasDeleted, err := t.storage.(rest.GracefulDeleter).Delete(ctx, objectMeta.GetName(), rest.ValidateAllObjectFunc, metav1.NewDeleteOptions(expectedGrace))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if wasDeleted {
		t.Errorf("unexpected, object %s should not have been deleted immediately", objectMeta.GetName())
	}
	if _, err := getFn(ctx, foo); err != nil {
		t.Fatalf("did not gracefully delete resource: %v", err)
	}

	// 忽略第二次删除的时长
	_, wasDeleted, err = t.storage.(rest.GracefulDeleter).Delete(ctx, objectMeta.GetName(), rest.ValidateAllObjectFunc, metav1.NewDeleteOptions(expectedGrace+2))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if wasDeleted {
		t.Errorf("unexpected, object %s should not have been deleted immediately", objectMeta.GetName())
	}
	object, err := t.storage.(rest.Getter).Get(ctx, objectMeta.GetName(), &metav1.GetOptions{})
	if err != nil {
		t.Errorf("unexpected error, object should exist: %v", err)
	}
	objectMeta = t.getObjectMetaOrFail(object)
	if objectMeta.GetDeletionTimestamp() == nil || objectMeta.GetDeletionGracePeriodSeconds() == nil || *objectMeta.GetDeletionGracePeriodSeconds() != expectedGrace {
		t.Errorf("unexpected deleted meta: %#v", objectMeta)
	}
	if generation >= objectMeta.GetGeneration() {
		t.Error("Generation wasn't bumped when deletion timestamp was set")
	}
}

// testDeleteGracefulImmediate 验证在已有优雅删除的情况下，宽限期为 0 的
// 第二次删除会立即移除对象。
func (t *Tester) testDeleteGracefulImmediate(obj runtime.Object, createFn CreateFunc, getFn GetFunc, expectedGrace int64) {
	ctx := t.TestContext()

	foo := obj.DeepCopyObject()
	t.setObjectMeta(foo, "foo4")
	if err := createFn(ctx, foo); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	objectMeta := t.getObjectMetaOrFail(foo)
	generation := objectMeta.GetGeneration()
	_, wasDeleted, err := t.storage.(rest.GracefulDeleter).Delete(ctx, objectMeta.GetName(), rest.ValidateAllObjectFunc, metav1.NewDeleteOptions(expectedGrace))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if wasDeleted {
		t.Errorf("unexpected, object %s should not have been deleted immediately", objectMeta.GetName())
	}
	if _, err := getFn(ctx, foo); err != nil {
		t.Fatalf("did not gracefully delete resource: %v", err)
	}

	// 第二次删除立即完成，资源被删除
	out, wasDeleted, err := t.storage.(rest.GracefulDeleter).Delete(ctx, objectMeta.GetName(), rest.ValidateAllObjectFunc, metav1.NewDeleteOptions(0))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !wasDeleted {
		t.Errorf("unexpected, object %s should have been deleted immediately", objectMeta.GetName())
	}
	_, err = t.storage.(rest.Getter).Get(ctx, objectMeta.GetName(), &metav1.GetOptions{})
	if !errors.IsNotFound(err) {
		t.Errorf("unexpected error, object should be deleted immediately: %v", err)
	}
	objectMeta = t.getObjectMetaOrFail(out)
	// 第二次删除不应更新对象，因此 objectMeta.GetDeletionGracePeriodSeconds() 应等于第一次删除设置的值。
	if objectMeta.GetDeletionTimestamp() == nil || objectMeta.GetDeletionGracePeriodSeconds() == nil || *objectMeta.GetDeletionGracePeriodSeconds() != 0 {
		t.Errorf("unexpected deleted meta: %#v", objectMeta)
	}
	if generation >= objectMeta.GetGeneration() {
		t.Error("Generation wasn't bumped when deletion timestamp was set")
	}
}

// testDeleteGracefulUsesZeroOnNil 验证 nil DeleteOptions 等价于宽限期 0（立即删除）。
func (t *Tester) testDeleteGracefulUsesZeroOnNil(obj runtime.Object, createFn CreateFunc, expectedGrace int64) {
	ctx := t.TestContext()

	foo := obj.DeepCopyObject()
	t.setObjectMeta(foo, t.namer(5))
	if err := createFn(ctx, foo); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	objectMeta := t.getObjectMetaOrFail(foo)
	_, wasDeleted, err := t.storage.(rest.GracefulDeleter).Delete(ctx, objectMeta.GetName(), rest.ValidateAllObjectFunc, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !wasDeleted {
		t.Errorf("unexpected, object %s should have been deleted immediately", objectMeta.GetName())
	}
	if _, err := t.storage.(rest.Getter).Get(ctx, objectMeta.GetName(), &metav1.GetOptions{}); !errors.IsNotFound(err) {
		t.Errorf("unexpected error, object should not exist: %v", err)
	}
}

// 针对 #27539 讨论的 bug 的回归测试
// 第二次删除可以缩短宽限期，但 deletionTimestamp 必须相应提前，保证
// “到期时间 = 时间戳 + 宽限期” 始终成立。
func (t *Tester) testDeleteGracefulShorten(obj runtime.Object, createFn CreateFunc, getFn GetFunc, expectedGrace int64) {
	ctx := t.TestContext()

	foo := obj.DeepCopyObject()
	t.setObjectMeta(foo, t.namer(6))
	if err := createFn(ctx, foo); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	bigGrace := int64(time.Hour)
	if expectedGrace > bigGrace {
		bigGrace = 2 * expectedGrace
	}
	objectMeta := t.getObjectMetaOrFail(foo)
	_, wasDeleted, err := t.storage.(rest.GracefulDeleter).Delete(ctx, objectMeta.GetName(), rest.ValidateAllObjectFunc, metav1.NewDeleteOptions(bigGrace))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if wasDeleted {
		t.Errorf("unexpected, object %s should not have been deleted immediately", objectMeta.GetName())
	}
	object, err := getFn(ctx, foo)
	if err != nil {
		t.Fatalf("did not gracefully delete resource: %v", err)
	}
	objectMeta = t.getObjectMetaOrFail(object)
	deletionTimestamp := *objectMeta.GetDeletionTimestamp()

	// 忽略第二次删除的时长
	_, wasDeleted, err = t.storage.(rest.GracefulDeleter).Delete(ctx, objectMeta.GetName(), rest.ValidateAllObjectFunc, metav1.NewDeleteOptions(expectedGrace))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if wasDeleted {
		t.Errorf("unexpected, object %s should not have been deleted immediately", objectMeta.GetName())
	}
	object, err = t.storage.(rest.Getter).Get(ctx, objectMeta.GetName(), &metav1.GetOptions{})
	if err != nil {
		t.Errorf("unexpected error, object should exist: %v", err)
	}
	objectMeta = t.getObjectMetaOrFail(object)
	if objectMeta.GetDeletionTimestamp() == nil || objectMeta.GetDeletionGracePeriodSeconds() == nil ||
		*objectMeta.GetDeletionGracePeriodSeconds() != expectedGrace || !objectMeta.GetDeletionTimestamp().Before(&deletionTimestamp) {
		t.Errorf("unexpected deleted meta: %#v", objectMeta)
	}
}

// =============================================================================
// Get 测试。

// testGetDifferentNamespace 确保不同命名空间中同名对象不会冲突
func (t *Tester) testGetDifferentNamespace(obj runtime.Object) {
	if t.clusterScope {
		t.Fatalf("the test does not work in cluster-scope")
	}

	objMeta := t.getObjectMetaOrFail(obj)
	objMeta.SetName(t.namer(5))

	ctx1 := genericapirequest.WithNamespace(genericapirequest.NewContext(), "bar3")
	if t.requestInfo != nil {
		ctx1 = genericapirequest.WithRequestInfo(ctx1, t.requestInfo)
	}
	objMeta.SetNamespace(genericapirequest.NamespaceValue(ctx1))
	_, err := t.storage.(rest.Creater).Create(ctx1, obj, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	ctx2 := genericapirequest.WithNamespace(genericapirequest.NewContext(), "bar4")
	if t.requestInfo != nil {
		ctx2 = genericapirequest.WithRequestInfo(ctx2, t.requestInfo)
	}
	objMeta.SetNamespace(genericapirequest.NamespaceValue(ctx2))
	_, err = t.storage.(rest.Creater).Create(ctx2, obj, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	got1, err := t.storage.(rest.Getter).Get(ctx1, objMeta.GetName(), &metav1.GetOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	got1Meta := t.getObjectMetaOrFail(got1)
	if got1Meta.GetName() != objMeta.GetName() {
		t.Errorf("unexpected name of object: %#v, expected: %s", got1, objMeta.GetName())
	}
	if got1Meta.GetNamespace() != genericapirequest.NamespaceValue(ctx1) {
		t.Errorf("unexpected namespace of object: %#v, expected: %s", got1, genericapirequest.NamespaceValue(ctx1))
	}

	got2, err := t.storage.(rest.Getter).Get(ctx2, objMeta.GetName(), &metav1.GetOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	got2Meta := t.getObjectMetaOrFail(got2)
	if got2Meta.GetName() != objMeta.GetName() {
		t.Errorf("unexpected name of object: %#v, expected: %s", got2, objMeta.GetName())
	}
	if got2Meta.GetNamespace() != genericapirequest.NamespaceValue(ctx2) {
		t.Errorf("unexpected namespace of object: %#v, expected: %s", got2, genericapirequest.NamespaceValue(ctx2))
	}
}

func (t *Tester) testGetFound(obj runtime.Object) {
	// 创建后立即读取，二者除 resourceVersion 外应完全一致。
	ctx := t.TestContext()
	t.setObjectMeta(obj, t.namer(1))

	existing, err := t.storage.(rest.Creater).Create(ctx, obj, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	existingMeta := t.getObjectMetaOrFail(existing)

	got, err := t.storage.(rest.Getter).Get(ctx, t.namer(1), &metav1.GetOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	gotMeta := t.getObjectMetaOrFail(got)
	gotMeta.SetResourceVersion(existingMeta.GetResourceVersion())
	if e, a := existing, got; !apiequality.Semantic.DeepEqual(e, a) {
		t.Errorf("unexpected obj: %#v, expected %#v", e, a)
	}
}

// testGetMimatchedNamespace 验证命名空间作用域资源不能跨命名空间读取；
// 集群作用域资源则忽略命名空间。
func (t *Tester) testGetMimatchedNamespace(obj runtime.Object) {
	ctx1 := genericapirequest.WithNamespace(genericapirequest.NewContext(), "bar1")
	if t.requestInfo != nil {
		ctx1 = genericapirequest.WithRequestInfo(ctx1, t.requestInfo)
	}
	ctx2 := genericapirequest.WithNamespace(genericapirequest.NewContext(), "bar2")
	if t.requestInfo != nil {
		ctx2 = genericapirequest.WithRequestInfo(ctx2, t.requestInfo)
	}
	objMeta := t.getObjectMetaOrFail(obj)
	objMeta.SetName(t.namer(4))
	objMeta.SetNamespace(genericapirequest.NamespaceValue(ctx1))
	_, err := t.storage.(rest.Creater).Create(ctx1, obj, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	_, err = t.storage.(rest.Getter).Get(ctx2, t.namer(4), &metav1.GetOptions{})
	if t.clusterScope {
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
	} else {
		if !errors.IsNotFound(err) {
			t.Errorf("unexpected error returned: %#v", err)
		}
	}
}

func (t *Tester) testGetNotFound(obj runtime.Object) {
	ctx := t.TestContext()
	t.setObjectMeta(obj, t.namer(2))
	_, err := t.storage.(rest.Creater).Create(ctx, obj, rest.ValidateAllObjectFunc, &metav1.CreateOptions{})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	_, err = t.storage.(rest.Getter).Get(ctx, t.namer(3), &metav1.GetOptions{})
	if !errors.IsNotFound(err) {
		t.Errorf("unexpected error returned: %#v", err)
	}
}

// =============================================================================
// List 测试。

func listToItems(listObj runtime.Object) ([]runtime.Object, error) {
	v, err := conversion.EnforcePtr(listObj)
	if err != nil {
		return nil, fmt.Errorf("unexpected error: %v", err)
	}
	items := v.FieldByName("Items")
	if !items.IsValid() {
		return nil, fmt.Errorf("unexpected Items field in %v", listObj)
	}
	if items.Type().Kind() != reflect.Slice {
		return nil, fmt.Errorf("unexpected Items field type: %v", items.Type().Kind())
	}
	result := make([]runtime.Object, items.Len())
	for i := 0; i < items.Len(); i++ {
		result[i] = items.Index(i).Addr().Interface().(runtime.Object)
	}
	return result, nil
}

// testListFound 验证不带选择器时返回全部对象，顺序与 assignFn 写入顺序一致。
func (t *Tester) testListFound(obj runtime.Object, assignFn AssignFunc) {
	ctx := t.TestContext()

	foo1 := obj.DeepCopyObject()
	t.setObjectMeta(foo1, t.namer(1))
	foo2 := obj.DeepCopyObject()
	t.setObjectMeta(foo2, t.namer(2))

	existing := assignFn([]runtime.Object{foo1, foo2})

	listObj, err := t.storage.(rest.Lister).List(ctx, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	items, err := listToItems(listObj)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(items) != len(existing) {
		t.Errorf("unexpected number of items: %v", len(items))
	}
	if !apiequality.Semantic.DeepEqual(existing, items) {
		t.Errorf("expected: %#v, got: %#v", existing, items)
	}
}

// testListMatchLabels 验证标签选择器只返回匹配的对象。
func (t *Tester) testListMatchLabels(obj runtime.Object, assignFn AssignFunc) {
	ctx := t.TestContext()
	testLabels := map[string]string{"key": "value"}

	foo3 := obj.DeepCopyObject()
	t.setObjectMeta(foo3, "foo3")
	foo4 := obj.DeepCopyObject()
	foo4Meta := t.getObjectMetaOrFail(foo4)
	foo4Meta.SetName("foo4")
	foo4Meta.SetNamespace(genericapirequest.NamespaceValue(ctx))
	foo4Meta.SetLabels(testLabels)

	objs := ([]runtime.Object{foo3, foo4})

	assignFn(objs)
	filtered := []runtime.Object{objs[1]}

	selector := labels.SelectorFromSet(labels.Set(testLabels))
	options := &metainternalversion.ListOptions{LabelSelector: selector}
	listObj, err := t.storage.(rest.Lister).List(ctx, options)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	items, err := listToItems(listObj)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(items) != len(filtered) {
		t.Errorf("unexpected number of items: %v", len(items))
	}
	if !apiequality.Semantic.DeepEqual(filtered, items) {
		t.Errorf("expected: %#v, got: %#v", filtered, items)
	}
}

// testListNotFound 验证空存储返回空列表而非 nil/错误。
func (t *Tester) testListNotFound(assignFn AssignFunc) {
	ctx := t.TestContext()
	_ = assignFn([]runtime.Object{})

	listObj, err := t.storage.(rest.Lister).List(ctx, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	items, err := listToItems(listObj)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(items) != 0 {
		t.Errorf("unexpected items: %#v", items)
	}
}

// testListTableConversion 验证 TableList 返回值的一组已知边界与限制。
func (t *Tester) testListTableConversion(obj runtime.Object, assignFn AssignFunc) {
	ctx := t.TestContext()
	testLabels := map[string]string{"key": "value"}

	foo3 := obj.DeepCopyObject()
	t.setObjectMeta(foo3, "foo3")
	foo4 := obj.DeepCopyObject()
	foo4Meta := t.getObjectMetaOrFail(foo4)
	foo4Meta.SetName("foo4")
	foo4Meta.SetNamespace(genericapirequest.NamespaceValue(ctx))
	foo4Meta.SetLabels(testLabels)

	objs := ([]runtime.Object{foo3, foo4})

	assignFn(objs)

	options := &metainternalversion.ListOptions{}
	listObj, err := t.storage.(rest.Lister).List(ctx, options)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	items, err := listToItems(listObj)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(items) != len(objs) {
		t.Errorf("unexpected number of items: %v", len(items))
	}
	if !apiequality.Semantic.DeepEqual(objs, items) {
		t.Errorf("expected: %#v, got: %#v", objs, items)
	}

	m, err := meta.ListAccessor(listObj)
	if err != nil {
		t.Fatalf("list should support ListMeta %T: %v", listObj, err)
	}
	m.SetContinue("continuetoken")
	m.SetResourceVersion("11")

	table, err := t.storage.(rest.TableConvertor).ConvertToTable(ctx, listObj, nil)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if table.ResourceVersion != "11" || table.Continue != "continuetoken" {
		t.Errorf("printer lost list meta: %#v", table.ListMeta)
	}
	if len(table.Rows) != len(items) {
		t.Errorf("unexpected number of rows: %v", len(table.Rows))
	}
	columns := table.ColumnDefinitions
	if len(columns) == 0 {
		t.Fatalf("unexpected number of columns: %v\n%#v", len(columns), columns)
	}
	if !strings.EqualFold(columns[0].Name, "Name") || columns[0].Type != "string" || columns[0].Format != "name" {
		t.Errorf("expect column 0 to be the name column: %#v", columns[0])
	}
	for j, column := range columns {
		if len(column.Name) == 0 {
			t.Errorf("column %d has no name", j)
		}
		switch column.Type {
		case "string", "date", "integer", "number", "boolean":
		default:
			t.Errorf("column %d has unexpected type: %q", j, column.Type)
		}
		switch {
		case column.Format == "":
		case column.Format == "name" && column.Type == "string":
		default:
			t.Errorf("column %d has unexpected format: %q with type %q", j, column.Format, column.Type)
		}
		if column.Priority < 0 || column.Priority > 2 {
			t.Errorf("column %d has unexpected priority: %q", j, column.Priority)
		}
		if len(column.Description) == 0 {
			t.Errorf("column %d has no description", j)
		}
		if column.Name == "Created At" && column.Type != "date" && column.Format != "" {
			t.Errorf("column %d looks like a created at column, but has a different type and format: %#v", j, column)
		}
	}
	for i, row := range table.Rows {
		if len(row.Cells) != len(table.ColumnDefinitions) {
			t.Errorf("row %d did not have the correct number of cells: %d in %v, expected %d", i, len(row.Cells), row.Cells, len(table.ColumnDefinitions))
		}
		for j, cell := range row.Cells {
			// 未经讨论不要往此测试添加断言，可能破坏客户端
			switch cell.(type) {
			case float64, int64, int32, int, string, bool:
			case []interface{}:
			case nil:
			default:
				t.Errorf("row %d, cell %d has an unrecognized type, only JSON serialization safe types are allowed: %T ", i, j, cell)
			}
		}
		if len(row.Cells) != len(table.ColumnDefinitions) {
			t.Fatalf("unmatched row length on row %d: %#v", i, row.Cells)
		}
	}
	data, _ := json.MarshalIndent(table, "", "  ")
	t.Logf("%s", string(data))
}

// =============================================================================
// Watch 测试。

// testWatchFields 用字段选择器验证 watch 过滤：pass 集合必须收到事件，
// fail 集合在 500ms 内不得收到事件。
func (t *Tester) testWatchFields(obj runtime.Object, emitFn EmitFunc, fieldsPass, fieldsFail []fields.Set, actions []string) {
	ctx := t.TestContext()

	for _, field := range fieldsPass {
		for _, action := range actions {
			options := &metainternalversion.ListOptions{FieldSelector: field.AsSelector(), ResourceVersion: "1"}
			watcher, err := t.storage.(rest.Watcher).Watch(ctx, options)
			if err != nil {
				t.Errorf("unexpected error: %v, %v", err, action)
			}

			if err := emitFn(obj, action); err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			select {
			case _, ok := <-watcher.ResultChan():
				if !ok {
					t.Errorf("watch channel should be open")
				}
			case <-time.After(wait.ForeverTestTimeout):
				t.Errorf("unexpected timeout from result channel")
			}
			watcher.Stop()
		}
	}

	for _, field := range fieldsFail {
		for _, action := range actions {
			options := &metainternalversion.ListOptions{FieldSelector: field.AsSelector(), ResourceVersion: "1"}
			watcher, err := t.storage.(rest.Watcher).Watch(ctx, options)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if err := emitFn(obj, action); err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			select {
			case <-watcher.ResultChan():
				t.Errorf("unexpected result from result channel")
			case <-time.After(time.Millisecond * 500):
				// 预期场景
			}
			watcher.Stop()
		}
	}
}

// testWatchLabels 用标签选择器验证 watch 过滤，语义与 testWatchFields 相同。
func (t *Tester) testWatchLabels(obj runtime.Object, emitFn EmitFunc, labelsPass, labelsFail []labels.Set, actions []string) {
	ctx := t.TestContext()

	for _, label := range labelsPass {
		for _, action := range actions {
			options := &metainternalversion.ListOptions{LabelSelector: label.AsSelector(), ResourceVersion: "1"}
			watcher, err := t.storage.(rest.Watcher).Watch(ctx, options)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if err := emitFn(obj, action); err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			select {
			case _, ok := <-watcher.ResultChan():
				if !ok {
					t.Errorf("watch channel should be open")
				}
			case <-time.After(wait.ForeverTestTimeout):
				t.Errorf("unexpected timeout from result channel")
			}
			watcher.Stop()
		}
	}

	for _, label := range labelsFail {
		for _, action := range actions {
			options := &metainternalversion.ListOptions{LabelSelector: label.AsSelector(), ResourceVersion: "1"}
			watcher, err := t.storage.(rest.Watcher).Watch(ctx, options)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if err := emitFn(obj, action); err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			select {
			case <-watcher.ResultChan():
				t.Errorf("unexpected result from result channel")
			case <-time.After(time.Millisecond * 500):
				// 预期场景
			}
			watcher.Stop()
		}
	}
}
