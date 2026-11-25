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

// Package managedfieldstest 为测试提供服务端 apply / 字段跟踪的轻量模拟
// 实现，无需真实 apiserver。
package managedfieldstest

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/apimachinery/pkg/util/managedfields/internal/testing"
)

// TestFieldManager 是可在测试中模拟服务端应用和字段跟踪的 FieldManager，
// 并提供查询对象状态的方法。
//
// 此 TestFieldManager 使用一系列 "fake" 对象模拟部分行为；
// 由于没有版本转换逻辑，只能使用一个版本。
// 需要指定子资源或一组修改后的 Manager 进行针对性测试时，
// 可使用此类型代替 NewDefaultTestFieldManager。
//
// 接口刻意保持窄小，测试只需构造对象、执行 apply/update 并检查
// 最终的 managedFields，无需关心 manager 链内部结构。
type TestFieldManager interface {
	//所跟踪对象的 APIVersion。
	APIVersion() string
	//Reset 将 liveObject 重置为空对象。
	Reset()
	//Live 返回当前 liveObject 的副本。
	Live() runtime.Object
	//Apply 为指定管理器将给定对象应用到当前 liveObj，force 标志控制是否强制。
	Apply(obj runtime.Object, manager string, force bool) error
	//Update 根据更新内容修改 liveObj 中的托管字段。
	Update(obj runtime.Object, manager string) error
	//ManagedFields 返回 liveObj 现有的托管字段列表。
	ManagedFields() []metav1.ManagedFieldsEntry
}

// NewTestFieldManager 为指定 GVK 创建作用于主资源的 TestFieldManager。
func NewTestFieldManager(typeConverter managedfields.TypeConverter, gvk schema.GroupVersionKind) TestFieldManager {
	return testing.NewTestFieldManagerImpl(typeConverter, gvk, "", nil)
}

// NewTestFieldManagerSubresource 为指定 GVK 创建作用于给定子资源的 TestFieldManager。
func NewTestFieldManagerSubresource(typeConverter managedfields.TypeConverter, gvk schema.GroupVersionKind, subresource string) TestFieldManager {
	return testing.NewTestFieldManagerImpl(typeConverter, gvk, subresource, nil)

}

// NewFakeFieldManager 创建不执行转换的真实 FieldManager，
// 仅为测试提供便利，但使用方式非常受限。
//
// 它与 TestFieldManager 不同，不用于断言值或保存状态，
// 行为与普通 FieldManager 一致。
// 此外，它只作用于主资源，不能配置子资源。
func NewFakeFieldManager(typeConverter managedfields.TypeConverter, gvk schema.GroupVersionKind) *managedfields.FieldManager {
	ffm, err := managedfields.NewDefaultFieldManager(
		typeConverter,
		&testing.FakeObjectConvertor{},
		&testing.FakeObjectDefaulter{},
		&testing.FakeObjectCreater{},
		gvk,
		gvk.GroupVersion(),
		"",
		nil)
	if err != nil {
		// 参数全部来自调用方且均为测试用的 fake 实现，构造失败即代码错误，
		// 直接 panic 比返回 error 更适合测试辅助函数。
		panic(err)
	}
	return ffm
}
