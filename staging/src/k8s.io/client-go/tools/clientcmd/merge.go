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

// recursively merges src into dst:
// - non-pointer struct fields with any exported fields are recursively merged
// - non-pointer struct fields with only unexported fields prefer src if the field is non-zero
// - maps are shallow merged with src keys taking priority over dst
// - non-zero src fields encountered during recursion that are not maps or structs overwrite and recursion stops
//
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
	// no-op if we can't read the src
	if !src.IsValid() {
		return nil
	}
	// sanity check types match
	if srcType := src.Type(); dstType != srcType {
		return fmt.Errorf("cannot merge mismatched types (%s, %s) at %s", dstType, srcType, strings.Join(fieldNames, "."))
	}

	switch dstType.Kind() {
	case reflect.Struct:
		if hasExportedField(dstType) {
			// recursively merge
			// 含导出字段的结构体逐字段合并，而不是整体覆盖，以保留 dst 中未在 src 设置的字段。
			for i, n := 0, dstType.NumField(); i < n; i++ {
				if err := mergeValues(append(fieldNames, dstType.Field(i).Name), dst.Field(i), src.Field(i)); err != nil {
					return err
				}
			}
		} else if dst.CanSet() {
			// If all fields are unexported, overwrite with src.
			// Using src.IsZero() would make more sense but that's not what mergo did.
			// 全未导出字段无法逐字段反射赋值，只能整体覆盖；保持与旧实现一致。
			dst.Set(src)
		}

	case reflect.Map:
		if dst.CanSet() && !src.IsZero() {
			// initialize dst if needed
			if dst.IsZero() {
				dst.Set(reflect.MakeMap(dstType))
			}
			// shallow-merge overwriting dst keys with src keys
			// 值本身不递归合并，src 中同名 key 直接替换 dst 的整个 value。
			for _, mapKey := range src.MapKeys() {
				dst.SetMapIndex(mapKey, src.MapIndex(mapKey))
			}
		}

	case reflect.Slice:
		if dst.CanSet() && src.Len() > 0 {
			// overwrite dst with non-empty src slice
			// 空切片不覆盖，避免用“未设置”的值清空已有列表。
			dst.Set(src)
		}

	case reflect.Pointer:
		if dst.CanSet() && !src.IsZero() {
			// overwrite dst with non-zero values for other types
			if dstType.Elem().Kind() == reflect.Struct {
				// use struct pointer as-is
				dst.Set(src)
			} else {
				// shallow-copy non-struct pointer (interfaces, primitives, etc)
				// 复制指针目标而非共享指针，避免 dst/src 后续修改互相影响。
				dst.Set(reflect.New(dstType.Elem()))
				dst.Elem().Set(src.Elem())
			}
		}

	default:
		if dst.CanSet() && !src.IsZero() {
			// overwrite dst with non-zero values for other types
			dst.Set(src)
		}
	}

	return nil
}

// hasExportedField returns true if the given type has any exported fields,
// or if it has any anonymous/embedded struct fields with exported fields
//
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
