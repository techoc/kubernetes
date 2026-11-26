/*
Copyright 2018 The Kubernetes Authors.

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

package outputtests

import (
	"reflect"
)

// // ValueFuzz 递归改写对象中所有基本类型的值，但不碰引用本身：slice、map、指针的
// // 地址保持不变。测试借此检测深拷贝是否留下了别名。
// 这是测试辅助：用就地修改来检测深拷贝是否存在别名（副本若与原对象共享底层数组则会被一并改掉）。
func ValueFuzz(obj interface{}) {
	valueFuzz(reflect.ValueOf(obj))
}

// valueFuzz 递归遍历值并修改可设置的标量。
// 集合自身不重建（保持地址不变），只改其元素；nil 集合暂时跳过。
func valueFuzz(obj reflect.Value) {
	switch obj.Kind() {
	case reflect.Array:
		for i := 0; i < obj.Len(); i++ {
			valueFuzz(obj.Index(i))
		}
	case reflect.Slice:
		if obj.IsNil() {
			// // TODO: 设置为非 nil 值
		} else {
			for i := 0; i < obj.Len(); i++ {
				valueFuzz(obj.Index(i))
			}
		}
	case reflect.Interface, reflect.Pointer:
		if obj.IsNil() {
			// // TODO: 设置为非 nil 值
		} else {
			valueFuzz(obj.Elem())
		}
	case reflect.Struct:
		for i, n := 0, obj.NumField(); i < n; i++ {
			valueFuzz(obj.Field(i))
		}
	case reflect.Map:
		if obj.IsNil() {
			// // TODO: 设置为非 nil 值
		} else {
			for _, k := range obj.MapKeys() {
				// // map 的值不可寻址，需要先复制一份再改。
				v := obj.MapIndex(k)
				copy := reflect.New(v.Type())
				copy.Elem().Set(v)
				valueFuzz(copy.Elem())
				obj.SetMapIndex(k, copy.Elem())
			}
			// // TODO: 插入一些新值
		}
	case reflect.Func: // ignore, we don't have function types in our API
	default:
		// 不可设置的字段（如未导出字段、非寻址的 map 索引）无法修改，直接放弃。
		if !obj.CanSet() {
			return
		}
		// 用确定性的变换（+1、取反、*2+1）而非随机数，保证失败可复现。
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
