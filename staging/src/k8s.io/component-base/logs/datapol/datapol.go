/*
Copyright 2020 The Kubernetes Authors.

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

// Package datapol 判断对象是否包含敏感数据（如密钥、token），
// 用于决定能否安全地将其写入日志。
// 判定依据是字段上的 datapolicy 标签以及值是否为零值。
package datapol

import (
	"reflect"
	"strings"

	"k8s.io/klog/v2"
)

// Verify 返回参数中可视为对日志敏感的数据类型列表。
//
// Verify 返回参数中命中敏感数据标签的类型名列表；nil 表示未发现敏感数据。
// 反射遍历任意对象可能 panic（例如未导出字段、非法值），因此这里统一 recover：
// 日志安全检查不应让业务进程崩溃。
func Verify(value interface{}) []string {
	defer func() {
		if r := recover(); r != nil {
			// TODO 可考虑导出指标。
			klog.Warningf("Error while inspecting arguments for sensitive data: %v", r)
		}
	}()
	t := reflect.ValueOf(value)
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return datatypes(t)
}

// datatypes 递归遍历值，先看当前类型是否有有效标签，再按 kind 深入。
// 只检查非零值：零值字段即使带标签也不构成泄露。
func datatypes(v reflect.Value) []string {
	if types := byType(v.Type()); len(types) > 0 {
		// 切片和映射可以为 nil 或空，只有 nil 才是零值。
		switch v.Kind() {
		case reflect.Slice, reflect.Map:
			if !v.IsZero() && v.Len() > 0 {
				return types
			}
		default:
			if !v.IsZero() {
				return types
			}
		}
	}
	switch v.Kind() {
	case reflect.Interface:
		// 接口需解引用后才能看到真实类型与字段。
		return datatypes(v.Elem())
	case reflect.Slice, reflect.Array:
		for i := 0; i < v.Len(); i++ {
			if types := datatypes(v.Index(i)); len(types) > 0 {
				return types
			}
		}
	case reflect.Map:
		// 键和值都需要检查；任一命中即视为敏感。
		mapIter := v.MapRange()
		for mapIter.Next() {
			k := mapIter.Key()
			v := mapIter.Value()
			if types := datatypes(k); len(types) > 0 {
				return types
			}
			if types := datatypes(v); len(types) > 0 {
				return types
			}
		}
	case reflect.Struct:
		t := v.Type()
		numField := t.NumField()

		for i := 0; i < numField; i++ {
			f := t.Field(i)
			// 指针字段的价值取决于其指向内容，交给普通递归处理会 panic；
			// 这里的语义是只对直接内嵌的结构体字段应用 datapolicy。
			if f.Type.Kind() == reflect.Pointer {
				continue
			}
			if reason, ok := f.Tag.Lookup("datapolicy"); ok {
				if !v.Field(i).IsZero() {
					return strings.Split(reason, ",")
				}
			}
			if types := datatypes(v.Field(i)); len(types) > 0 {
				return types
			}
		}
	}
	return nil
}
