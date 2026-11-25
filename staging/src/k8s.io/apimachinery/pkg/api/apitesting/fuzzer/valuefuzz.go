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

package fuzzer

import (
	"reflect"
)

// ValueFuzz 递归地修改对象中所有基本类型的值；切片、map、指针等引用本身
// 不会被替换，其地址保持不变。这样 fuzz 可以就地扰动已有对象，同时不改变
// 共享的底层引用结构，便于后续做语义比对。
func ValueFuzz(obj interface{}) {
	valueFuzz(reflect.ValueOf(obj))
}

// valueFuzz 是 ValueFuzz 的反射实现：按 kind 递归下降，只对可设置的叶子值
// （字符串、布尔、数值）做确定性扰动。nil 容器与 nil 指针被跳过，因为无法在
// 不破坏引用语义的前提下就地创建元素。
func valueFuzz(obj reflect.Value) {
	switch obj.Kind() {
	case reflect.Array:
		// 数组按值内联存储，逐元素递归即可就地修改。
		for i := 0; i < obj.Len(); i++ {
			valueFuzz(obj.Index(i))
		}
	case reflect.Slice:
		if obj.IsNil() {
			// TODO: 设置为非 nil 值。
			// nil 切片没有可写元素；保持 nil 以维持“nil 与空切片不同”的语义。
		} else {
			for i := 0; i < obj.Len(); i++ {
				valueFuzz(obj.Index(i))
			}
		}
	case reflect.Interface, reflect.Pointer:
		if obj.IsNil() {
			// TODO: 设置为非 nil 值。
			// nil 接口/指针没有可解引用的目标，直接跳过。
		} else {
			valueFuzz(obj.Elem())
		}
	case reflect.Struct:
		// 逐字段递归；匿名内联字段同样会走到这里。
		for i, n := 0, obj.NumField(); i < n; i++ {
			valueFuzz(obj.Field(i))
		}
	case reflect.Map:
		if obj.IsNil() {
			// TODO: 设置为非 nil 值。
			// nil map 无法写入；保留 nil 以避免改变序列化结果。
		} else {
			for _, k := range obj.MapKeys() {
				// map 值不可寻址，须复制到临时变量，修改后写回。
				v := obj.MapIndex(k)
				copy := reflect.New(v.Type())
				copy.Elem().Set(v)
				valueFuzz(copy.Elem())
				obj.SetMapIndex(k, copy.Elem())
			}
			// TODO: 添加新条目。
		}
	case reflect.Func: // API 无函数类型，直接忽略
		// API 中没有函数类型；函数值本身也无法被“修改”，直接忽略。
	default:
		if !obj.CanSet() {
			return
		}
		// 对基本类型施加确定性、可重复的扰动，既改变取值又便于定位差异。
		switch obj.Kind() {
		case reflect.String:
			obj.SetString(obj.String() + "x")
		case reflect.Bool:
			obj.SetBool(!obj.Bool())
		case reflect.Float32, reflect.Float64:
			obj.SetFloat(obj.Float()*2.0 + 1.0)
		case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Int:
			obj.SetInt(obj.Int() + 1)
		case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uint:
			obj.SetUint(obj.Uint() + 1)
		default:
		}
	}
}
