/*
Copyright 2022 The Kubernetes Authors.

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

package testing

import (
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/apimachinery/pkg/util/managedfields/internal"
)

// FakeObjectCreater 实现 ObjectCreater，可创建指定 GVK 的空非结构化对象。
type FakeObjectCreater struct{}

// New 创建一个只带 apiVersion/kind 的空 unstructured 对象，
// 供 field manager 在创建路径上生成占位对象。
func (f *FakeObjectCreater) New(gvk schema.GroupVersionKind) (runtime.Object, error) {
	u := unstructured.Unstructured{Object: map[string]interface{}{}}
	u.SetAPIVersion(gvk.GroupVersion().String())
	u.SetKind(gvk.Kind)
	return &u, nil
}

// FakeObjectConvertor 实现 runtime.ObjectConvertor，但仅返回输入，不执行转换。
// 中文补充：测试里所有对象都是同一版本的 unstructured，因此转换可以退化为恒等操作。
type FakeObjectConvertor struct{}

// Convert 是空实现：仅给 out 赋一个会被丢弃的值以维持旧签名兼容。
//
//nolint:staticcheck,ineffassign // SA4009：保持向后兼容。
func (c *FakeObjectConvertor) Convert(in, out, context interface{}) error {
	out = in
	return nil
}

// ConvertToVersion 直接返回输入对象，不做任何版本转换。
func (c *FakeObjectConvertor) ConvertToVersion(in runtime.Object, _ runtime.GroupVersioner) (runtime.Object, error) {
	return in, nil
}

// ConvertFieldLabel 未实现：测试不涉及字段标签转换。
func (c *FakeObjectConvertor) ConvertFieldLabel(_ schema.GroupVersionKind, _, _ string) (string, string, error) {
	return "", "", errors.New("not implemented")
}

// FakeObjectDefaulter 实现 runtime.Defaulter，但不执行任何操作。
type FakeObjectDefaulter struct{}

// Default 空实现：测试对象不依赖默认值填充。
func (d *FakeObjectDefaulter) Default(in runtime.Object) {}

// TestFieldManagerImpl 是 internal.FieldManager 的测试包装：
// 持有一个“live”对象并在每次 Apply/Update 成功后用返回结果替换它，模拟 API 服务器状态。
type TestFieldManagerImpl struct {
	fieldManager *internal.FieldManager
	apiVersion   string
	emptyObj     runtime.Object
	liveObj      runtime.Object
}

// 所跟踪对象的 APIVersion。
func (f *TestFieldManagerImpl) APIVersion() string {
	return f.apiVersion
}

// Reset 将 liveObject 重置为空对象。
func (f *TestFieldManagerImpl) Reset() {
	f.liveObj = f.emptyObj.DeepCopyObject()
}

// Live 返回当前 liveObject 的副本。
// 中文补充：返回深拷贝，避免调用方修改内部状态。
func (f *TestFieldManagerImpl) Live() runtime.Object {
	return f.liveObj.DeepCopyObject()
}

// Apply 为指定管理器将给定对象应用到当前 liveObj，force 标志控制是否强制。
// 中文补充：只有成功时才提交新状态，失败时 liveObj 保持不变。
func (f *TestFieldManagerImpl) Apply(obj runtime.Object, manager string, force bool) error {
	out, err := f.fieldManager.Apply(f.liveObj, obj, manager, force)
	if err == nil {
		f.liveObj = out
	}
	return err
}

// Update 根据更新内容修改 liveObj 中的托管字段。
func (f *TestFieldManagerImpl) Update(obj runtime.Object, manager string) error {
	out, err := f.fieldManager.Update(f.liveObj, obj, manager)
	if err == nil {
		f.liveObj = out
	}
	return err
}

// UpdateNoErrors 与 Update 相同，但不返回错误。
// 中文补充：用于构造“即使存在冲突也接受”的测试前置状态。
func (f *TestFieldManagerImpl) UpdateNoErrors(obj runtime.Object, manager string) {
	f.liveObj = f.fieldManager.UpdateNoErrors(f.liveObj, obj, manager)
}

// ManagedFields 返回 liveObj 现有的托管字段列表。
func (f *TestFieldManagerImpl) ManagedFields() []metav1.ManagedFieldsEntry {
	accessor, err := meta.Accessor(f.liveObj)
	if err != nil {
		panic(fmt.Errorf("couldn't get accessor: %v", err))
	}

	return accessor.GetManagedFields()
}

// NewTestFieldManager 为指定 GVK 创建新管理器。
// 中文补充：手工组装与 internal.NewDefaultFieldManager 相同的 manager 链，
// 但省略 CapManager 与 FieldManager 外层，便于测试更细粒度的行为。
func NewTestFieldManagerImpl(typeConverter managedfields.TypeConverter, gvk schema.GroupVersionKind, subresource string, chainFieldManager func(internal.Manager) internal.Manager) *TestFieldManagerImpl {
	f, err := internal.NewStructuredMergeManager(
		typeConverter,
		&FakeObjectConvertor{},
		&FakeObjectDefaulter{},
		gvk.GroupVersion(),
		gvk.GroupVersion(),
		nil,
	)
	if err != nil {
		panic(err)
	}
	// live 是初始空对象，后续 Apply/Update 都基于它演进。
	live := &unstructured.Unstructured{}
	live.SetKind(gvk.Kind)
	live.SetAPIVersion(gvk.GroupVersion().String())
	//这与 `internal.NewDefaultFieldManager` 的不同之处在于：
	//1. 不创建 `internal.FieldManager`。
	//2. 不使用已单独测试过的 CapManager，而是使用比默认值更小的上限。
	f = internal.NewVersionCheckManager(
		internal.NewLastAppliedUpdater(
			internal.NewLastAppliedManager(
				internal.NewProbabilisticSkipNonAppliedManager(
					internal.NewBuildManagerInfoManager(
						internal.NewManagedFieldsUpdater(
							internal.NewStripMetaManager(f),
						), gvk.GroupVersion(), subresource,
					), &FakeObjectCreater{}, internal.DefaultTrackOnCreateProbability,
				), typeConverter, &FakeObjectConvertor{}, gvk.GroupVersion(),
			),
		), gvk,
	)
	// 允许调用方在最外层再包一层 manager，以测试额外的拦截逻辑。
	if chainFieldManager != nil {
		f = chainFieldManager(f)
	}
	return &TestFieldManagerImpl{
		fieldManager: internal.NewFieldManager(f, subresource),
		apiVersion:   gvk.GroupVersion().String(),
		emptyObj:     live,
		liveObj:      live.DeepCopyObject(),
	}
}
