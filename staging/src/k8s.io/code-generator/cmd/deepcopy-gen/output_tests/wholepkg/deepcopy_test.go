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

package wholepkg

import (
	"reflect"
	"testing"

	"sigs.k8s.io/randfill"
)

// TestDeepCopyPrimitives 验证内建类型字段的深拷贝：用 fuzzer 填充后拷贝，
// 断言源与目标在拷贝前后完全相等。
func TestDeepCopyPrimitives(t *testing.T) {
	x := StructPrimitives{}
	y := StructPrimitives{}

	// 起始值相等是后续判断失败的前提。
	if !reflect.DeepEqual(&x, &y) {
		t.Errorf("objects should be equal to start, but are not")
	}

	fuzzer := randfill.New()
	fuzzer.Fill(&x)
	fuzzer.Fill(&y)

	if reflect.DeepEqual(&x, &y) {
		t.Errorf("objects should not be equal, but are")
	}

	x.DeepCopyInto(&y)
	if !reflect.DeepEqual(&x, &y) {
		t.Errorf("objects should be equal, but are not")
	}
}

// TestDeepCopyInterfaceFields 验证接口字段（runtime.Object / Selector）的深拷贝。
// 生成代码应对接口中的具体对象递归调用 DeepCopyObject。
func TestDeepCopyInterfaceFields(t *testing.T) {
	x := StructInterfaces{}
	y := StructInterfaces{}

	if !reflect.DeepEqual(&x, &y) {
		t.Errorf("objects should be equal to start, but are not")
	}

	fuzzer := randfill.New()

	// ObjectField 与 SelectorField 分别指向不同的具体实现，确保两者都被拷贝。
	obj := StructExplicitObject{}
	fuzzer.Fill(&obj)
	x.ObjectField = &obj

	sel := StructExplicitSelectorExplicitObject{}
	fuzzer.Fill(&sel)
	x.SelectorField = &sel

	if reflect.DeepEqual(&x, &y) {
		t.Errorf("objects should not be equal, but are")
	}

	x.DeepCopyInto(&y)
	if !reflect.DeepEqual(&x, &y) {
		t.Errorf("objects should be equal, but are not")
	}
}

// TestNilCopy 验证 nil 接收者的 DeepCopy 仍返回 nil，而不是新建对象。
func TestNilCopy(t *testing.T) {
	var x *StructB
	y := x.DeepCopy()
	if y != nil {
		t.Errorf("Expected nil as deepcopy of nil, got %+v", y)
	}
}

// assertMethod 断言类型上存在指定方法。
func assertMethod(t *testing.T, typ reflect.Type, name string) {
	if _, found := typ.MethodByName(name); !found {
		t.Errorf("StructExplicitObject must have %v method", name)
	}
}

// assertNotMethod 断言类型上不存在指定方法。
func assertNotMethod(t *testing.T, typ reflect.Type, name string) {
	if _, found := typ.MethodByName(name); found {
		t.Errorf("%v must not have %v method", typ, name)
	}
}

// TestInterfaceTypes 验证哪些类型会因接口标记而生成 DeepCopyObject 等方法：
// 显式声明 runtime.Object / Selector 的类型才应生成对应方法。
func TestInterfaceTypes(t *testing.T) {
	explicitObject := reflect.TypeOf(&StructExplicitObject{})
	assertMethod(t, explicitObject, "DeepCopyObject")

	// 未标记 runtime.Object 的类型不应生成 DeepCopy。
	typeMeta := reflect.TypeOf(&StructTypeMeta{})
	assertNotMethod(t, typeMeta, "DeepCopy")

	objectAndList := reflect.TypeOf(&StructObjectAndList{})
	assertMethod(t, objectAndList, "DeepCopyObject")
	assertMethod(t, objectAndList, "DeepCopyList")

	objectAndObject := reflect.TypeOf(&StructObjectAndObject{})
	assertMethod(t, objectAndObject, "DeepCopyObject")

	explicitSelectorExplicitObject := reflect.TypeOf(&StructExplicitSelectorExplicitObject{})
	assertMethod(t, explicitSelectorExplicitObject, "DeepCopySelector")
	assertMethod(t, explicitSelectorExplicitObject, "DeepCopyObject")
}

// TestInterfaceDeepCopy 验证 DeepCopyObject 返回同类型的新对象且内容相等。
func TestInterfaceDeepCopy(t *testing.T) {
	x := StructExplicitObject{}

	fuzzer := randfill.New()
	fuzzer.Fill(&x)

	yObj := x.DeepCopyObject()
	y, ok := yObj.(*StructExplicitObject)
	if !ok {
		t.Fatalf("epxected StructExplicitObject from StructExplicitObject.DeepCopyObject, got: %t", yObj)
	}
	if !reflect.DeepEqual(y, &x) {
		t.Error("objects should be equal, but are not")
	}
}

// TestInterfaceNonPointerDeepCopy 验证非指针类型实现 runtime.Object 时的拷贝路径。
func TestInterfaceNonPointerDeepCopy(t *testing.T) {
	x := StructNonPointerExplicitObject{}

	fuzzer := randfill.New()
	fuzzer.Fill(&x)

	yObj := x.DeepCopyObject()
	y, ok := yObj.(StructNonPointerExplicitObject)
	if !ok {
		t.Fatalf("epxected StructNonPointerExplicitObject from StructNonPointerExplicitObject.DeepCopyObject, got: %t", yObj)
	}
	if !reflect.DeepEqual(y, x) {
		t.Error("objects should be equal, but are not")
	}
}
