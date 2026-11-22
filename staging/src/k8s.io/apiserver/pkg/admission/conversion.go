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

package admission

import (
	"reflect"

	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/cel/common"
)

// VersionedAttributes 在原始准入属性之外，额外携带按某个具体版本（GVK）转换后的
// 新对象与旧对象。策略类准入插件（如 ValidatingAdmissionPolicy、MutatingAdmissionPolicy）
// 需要以声明中请求的版本视图来求值 CEL，因此不能直接使用内部版本的属性对象。
type VersionedAttributes struct {
	// Attributes 保存原始的准入属性
	Attributes
	// VersionedOldObject 保存 Attributes.OldObject（若非 nil）转换到 VersionedKind 后的结果，
	// 并封装在 LazyObject 中。它绝不能被修改——旧对象只读是策略求值的基本约定。
	VersionedOldObject LazyObject
	// VersionedObject 保存 Attributes.Object（若非 nil）转换到 VersionedKind 后的结果，
	// 并封装在 LazyObject 中。若被修改，修改方必须把 Dirty 置为 true。
	VersionedObject LazyObject

	// VersionedKind 保存完全限定的 kind
	VersionedKind schema.GroupVersionKind
	// Dirty 表示 VersionedObject 内的对象自 Attributes.Object 转换以来已被修改
	Dirty bool
}

// UpdateObject 更新 VersionedObject 并清空缓存的 CEL 表示。
// 必须同时置位 Dirty：后续跨版本转换要靠它把变更回写到 Attributes.Object。
func (v *VersionedAttributes) UpdateObject(obj runtime.Object) {
	v.Dirty = true
	v.VersionedObject.Set(obj)
}

// GetObject 覆盖 Attributes.GetObject()：优先返回已转换/已修改的版本化对象，
// 这样后续插件看到的是前序 mutating 插件产生的对象。
func (v *VersionedAttributes) GetObject() runtime.Object {
	if v.VersionedObject.object != nil {
		return v.VersionedObject.object
	}
	return v.Attributes.GetObject()
}

// ConvertToGVK 将对象转换为目标 gvk。
func ConvertToGVK(obj runtime.Object, gvk schema.GroupVersionKind, o ObjectInterfaces) (runtime.Object, error) {
	// 与其他资源不同，自定义资源没有内部版本；若 obj 已是自定义资源且版本相同，
	// 则无需转换（也就避免了 object creater 中不存在对应内部类型的问题）。
	if obj.GetObjectKind().GroupVersionKind() == gvk {
		return obj, nil
	}
	out, err := o.GetObjectCreater().New(gvk)
	if err != nil {
		return nil, err
	}
	err = o.GetObjectConvertor().Convert(obj, out, nil)
	if err != nil {
		return nil, err
	}
	// 显式设置 GVK：转换器通常不负责填充 TypeMeta。
	out.GetObjectKind().SetGroupVersionKind(gvk)
	return out, nil
}

// NewVersionedAttributes 返回 versioned attributes，其中新、旧对象（若存在）已转换为请求的 kind。
func NewVersionedAttributes(attr Attributes, gvk schema.GroupVersionKind, o ObjectInterfaces) (*VersionedAttributes, error) {
	// 将旧对象和新对象都转换到请求的版本
	versionedAttr := &VersionedAttributes{
		Attributes:    attr,
		VersionedKind: gvk,
	}
	if oldObj := attr.GetOldObject(); oldObj != nil {
		out, err := ConvertToGVK(oldObj, gvk, o)
		if err != nil {
			return nil, err
		}
		versionedAttr.VersionedOldObject.Set(out)
	}
	if obj := attr.GetObject(); obj != nil {
		out, err := ConvertToGVK(obj, gvk, o)
		if err != nil {
			return nil, err
		}
		versionedAttr.VersionedObject.Set(out)
	}

	return versionedAttr, nil
}

// LazyObject 封装一个版本化的 runtime.Object 及其延迟求值的 Common Expression Language（CEL）表示。
// CEL 表示只在策略真正求值时才构造，避免为每个请求付出无谓的转换开销。
type LazyObject struct {
	object runtime.Object
	// UseSchemalessTypeRef 表示改用 SchemalessTypedToVal（基于反射的惰性 ref.Val），
	// 而不是通过 DefaultTypeAdapter 转成 unstructured 表示。
	//
	// Deprecated: This field is temporary and will be removed when schemaless becomes the default.
	// 已弃用：该字段是临时的，待 schemaless 成为默认实现后移除。
	UseSchemalessTypeRef bool
	celVal               ref.Val
}

// NewLazyObject 返回包装给定 runtime.Object 的新 LazyObject。
func NewLazyObject(obj runtime.Object) LazyObject {
	return LazyObject{object: obj}
}

// Object 返回底层的 runtime.Object。
func (l *LazyObject) Object() runtime.Object {
	return l.object
}

// Set 替换底层对象并使已缓存的 CEL 值失效——缓存与对象必须保持一致。
func (l *LazyObject) Set(obj runtime.Object) {
	l.object = obj
	l.celVal = nil
}

// CELValue 返回对象对应的 CEL 值，首次调用时计算并缓存。
// 返回 (nil, nil) 表示没有对象（例如 create 请求没有旧对象）。
func (l *LazyObject) CELValue() (ref.Val, error) {
	if l.celVal != nil {
		return l.celVal, nil
	}
	if l.object == nil {
		return nil, nil
	}
	// unstructured 已经是最适合 CEL 的类型适配输入，直接使用其 map。
	if u, ok := l.object.(*unstructured.Unstructured); ok {
		l.celVal = types.DefaultTypeAdapter.NativeToValue(u.Object)
		return l.celVal, nil
	}
	// TODO: use SchemalessTypeRef always.
	if l.UseSchemalessTypeRef {
		l.celVal = common.SchemalessTypedToVal(l.object)
		return l.celVal, nil
	}

	// 回退路径：先转为 unstructured，再由默认类型适配器转为 CEL 值。
	u, err := ConvertObjectToUnstructured(l.object)
	if err != nil {
		return nil, err
	}
	l.celVal = types.DefaultTypeAdapter.NativeToValue(u.Object)
	return l.celVal, nil
}

// ConvertVersionedAttributes 将 VersionedObject 和 VersionedOldObject 转换为指定的 kind（如需要）。
// 若 attr.VersionedKind 已与请求的 kind 一致，则不做任何转换。
// 需要转换时：
//   - Dirty=true 时，以 attr.VersionedObject.Object() 为源生成新对象（先经 attr.Attributes.Object
//     往返一次，并在过程中清除 Dirty），以保留前序插件所做的修改；
//   - Dirty=false 时，以 attr.Attributes.Object 为源生成新对象；
//   - 旧对象始终以 attr.Attributes.OldObject 为源。
func ConvertVersionedAttributes(attr *VersionedAttributes, gvk schema.GroupVersionKind, o ObjectInterfaces) error {
	// 已经是目标版本，无需转换
	if attr.VersionedKind == gvk {
		return nil
	}

	// 把原始旧对象转换到目标 GVK
	if oldObj := attr.Attributes.GetOldObject(); oldObj != nil {
		out, err := ConvertToGVK(oldObj, gvk, o)
		if err != nil {
			return err
		}
		attr.VersionedOldObject.Set(out)
	}

	if attr.VersionedObject.object != nil {
		// 先把现有的版本化对象转换回内部版本（仅当它被修改过）
		if attr.Dirty {
			err := o.GetObjectConvertor().Convert(attr.VersionedObject.object, attr.Attributes.GetObject(), nil)
			if err != nil {
				return err
			}
		}

		// 再转换到目标外部版本
		out, err := ConvertToGVK(attr.Attributes.GetObject(), gvk, o)
		if err != nil {
			return err
		}
		attr.VersionedObject.Set(out)
	}

	// 记录当前所处的版本
	attr.VersionedKind = gvk
	attr.Dirty = false

	return nil
}

// ConvertObjectToUnstructured 将对象转换为 unstructured 表示。
func ConvertObjectToUnstructured(obj interface{}) (*unstructured.Unstructured, error) {
	// 用反射判空，兼容 typed nil 指针：接口非 nil 但底层指针为 nil 时，
	// 直接交给 DefaultUnstructuredConverter 会 panic。
	if obj == nil || reflect.ValueOf(obj).IsNil() {
		return &unstructured.Unstructured{Object: nil}, nil
	}
	ret, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, err
	}
	return &unstructured.Unstructured{Object: ret}, nil
}
