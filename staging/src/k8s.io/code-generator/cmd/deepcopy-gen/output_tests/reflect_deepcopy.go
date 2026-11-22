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
	"fmt"
	"reflect"
)

// ReflectDeepCopy deep copies the object using reflection.
// 返回 interface{} 便于在测试中与生成代码的结果做通用比较。
func ReflectDeepCopy(in interface{}) interface{} {
	return reflectDeepCopy(reflect.ValueOf(in)).Interface()
}

// reflectDeepCopy 是 ReflectDeepCopy 的递归实现，按 reflect.Kind 分派。
func reflectDeepCopy(src reflect.Value) reflect.Value {
	// nil 的 interface/pointer/map/slice 直接返回原值：复制 nil 仍应为 nil，
	// 不能变成零值对象，否则语义改变。
	switch src.Kind() {
	case reflect.Interface, reflect.Pointer, reflect.Map, reflect.Slice:
		if src.IsNil() {
			return src
		}
	}

	switch src.Kind() {
	case reflect.Chan, reflect.Func, reflect.UnsafePointer, reflect.Uintptr:
		// 这些类型不可安全复制，显式 panic 而不是产生浅拷贝。
		panic(fmt.Sprintf("cannot deep copy kind: %s", src.Kind()))
	case reflect.Array:
		dst := reflect.New(src.Type())
		for i := 0; i < src.Len(); i++ {
			dst.Elem().Index(i).Set(reflectDeepCopy(src.Index(i)))
		}
		return dst.Elem()
	case reflect.Interface:
		// 展开接口后递归复制其动态值；未导出字段由类型自身负责语义。
		return reflectDeepCopy(src.Elem())
	case reflect.Map:
		dst := reflect.MakeMap(src.Type())
		for _, k := range src.MapKeys() {
			dst.SetMapIndex(k, reflectDeepCopy(src.MapIndex(k)))
		}
		return dst
	case reflect.Pointer:
		dst := reflect.New(src.Type().Elem())
		dst.Elem().Set(reflectDeepCopy(src.Elem()))
		return dst
	case reflect.Slice:
		// 预留容量但按元素追加，保持与生成代码相同的 nil/空语义。
		dst := reflect.MakeSlice(src.Type(), 0, src.Len())
		for i := 0; i < src.Len(); i++ {
			dst = reflect.Append(dst, reflectDeepCopy(src.Index(i)))
		}
		return dst
	case reflect.Struct:
		dst := reflect.New(src.Type())
		for i := 0; i < src.NumField(); i++ {
			if !dst.Elem().Field(i).CanSet() {
				// Can't set private fields. At this point, the
				// best we can do is a shallow copy. For
				// example, time.Time is a value type with
				// private members that can be shallow copied.
				return src
			}
			dst.Elem().Field(i).Set(reflectDeepCopy(src.Field(i)))
		}
		return dst.Elem()
	default:
		// Value types like numbers, booleans, and strings.
		return src
	}
}
