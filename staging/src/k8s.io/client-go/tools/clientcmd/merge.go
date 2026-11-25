/*
Copyright 2024 The Kubernetes Authors.

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

package clientcmd

import (
	"fmt"
	"reflect"
	"strings"
)

// 递归地把 src 合并进 dst：
// - 含任意导出字段的非指针结构体字段递归合并
// - 只含非导出字段的非指针结构体字段，若 src 非零则整体采用 src
// - map 做浅合并，src 的键优先于 dst
// - 递归过程中遇到的既非 map 也非结构体的非零 src 字段直接覆盖，并停止递归
// merge 用反射实现 kubeconfig 合并。选择手写而非引入第三方依赖，是为了精确控制
// "map 先到先得、标量后到覆盖" 的兼容语义（见 ClientConfigLoadingRules.Load）。
func merge[T any](dst, src *T) error {
	if dst == nil {
		return fmt.Errorf("cannot merge into nil pointer")
	}
	if src == nil {
		// 没有源值可合并，视为成功而非错误。
		return nil
	}
	return mergeValues(nil, reflect.ValueOf(dst).Elem(), reflect.ValueOf(src).Elem())
}

// mergeValues 是递归核心；fieldNames 仅用于生成包含字段路径的错误信息。
func mergeValues(fieldNames []string, dst, src reflect.Value) error {
	dstType := dst.Type()
	// 无法读取 src 时什么也不做
	if !src.IsValid() {
		return nil
	}
	// 先检查类型一致
	if srcType := src.Type(); dstType != srcType {
		return fmt.Errorf("cannot merge mismatched types (%s, %s) at %s", dstType, srcType, strings.Join(fieldNames, "."))
	}

	switch dstType.Kind() {
	case reflect.Struct:
		if hasExportedField(dstType) {
			// 递归合并。
			// 含导出字段的结构体逐字段合并，而不是整体覆盖，以保留 dst 中未在 src 设置的字段。
			for i, n := 0, dstType.NumField(); i < n; i++ {
				if err := mergeValues(append(fieldNames, dstType.Field(i).Name), dst.Field(i), src.Field(i)); err != nil {
					return err
				}
			}
		} else if dst.CanSet() {
			// 若所有字段都未导出，则用 src 整体覆盖。
			// 用 src.IsZero() 判断更合理，但 mergo 不是这么做的。
			// 全未导出字段无法逐字段反射赋值，只能整体覆盖；保持与旧实现一致。
			dst.Set(src)
		}

	case reflect.Map:
		if dst.CanSet() && !src.IsZero() {
			// 必要时初始化 dst
			if dst.IsZero() {
				dst.Set(reflect.MakeMap(dstType))
			}
			// 浅合并：src 的键覆盖 dst 的键。
			// 值本身不递归合并，src 中同名 key 直接替换 dst 的整个 value。
			for _, mapKey := range src.MapKeys() {
				dst.SetMapIndex(mapKey, src.MapIndex(mapKey))
			}
		}

	case reflect.Slice:
		if dst.CanSet() && src.Len() > 0 {
			// 用非空的 src 切片覆盖 dst。
			// 空切片不覆盖，避免用“未设置”的值清空已有列表。
			dst.Set(src)
		}

	case reflect.Pointer:
		if dst.CanSet() && !src.IsZero() {
			// 指针非零时覆盖 dst
			if dstType.Elem().Kind() == reflect.Struct {
				// 结构体指针原样使用
				dst.Set(src)
			} else {
				// 对非结构体指针（接口、基本类型等）做浅拷贝
				// 复制指针目标而非共享指针，避免 dst/src 后续修改互相影响。
				dst.Set(reflect.New(dstType.Elem()))
				dst.Elem().Set(src.Elem())
			}
		}

	default:
		if dst.CanSet() && !src.IsZero() {
			// 其他类型用非零值覆盖 dst
			dst.Set(src)
		}
	}

	return nil
}

// hasExportedField 在给定类型拥有导出字段、或其匿名/嵌入结构体字段中含导出字段时
// 返回 true。
// 匿名嵌入结构体需要递归检查：嵌入本身不可导出，但内部仍可能有导出字段。
func hasExportedField(dstType reflect.Type) bool {
	for i, n := 0, dstType.NumField(); i < n; i++ {
		field := dstType.Field(i)
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			if hasExportedField(dstType.Field(i).Type) {
				return true
			}
		} else if len(field.PkgPath) == 0 {
			return true
		}
	}
	return false
}
