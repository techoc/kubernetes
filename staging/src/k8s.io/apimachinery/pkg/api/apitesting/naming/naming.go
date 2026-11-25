/*
Copyright 2019 The Kubernetes Authors.

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

// Package naming 校验 API 类型是否遵循 Kubernetes 命名约定：API 组以
// .k8s.io 结尾，外部类型带 JSON tag，内部类型不带。
package naming

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
)

var (
	marshalerType   = reflect.TypeOf((*json.Marshaler)(nil)).Elem()
	unmarshalerType = reflect.TypeOf((*json.Unmarshaler)(nil)).Elem()
)

// VerifyGroupNames 确保 scheme 中所有 API 组以 k8s.io 结尾；
// legacyUnsuffixedGroups 可用于显式豁免遗留组。
// 该后缀避免第一方 API 与核心组或其他项目注册的组冲突。
func VerifyGroupNames(scheme *runtime.Scheme, legacyUnsuffixedGroups sets.String) error {
	errs := []error{}
	for _, gv := range scheme.PrioritizedVersionsAllGroups() {
		// 仅显式列入豁免名单的遗留组才允许缺少后缀。
		if !strings.HasSuffix(gv.Group, ".k8s.io") && !legacyUnsuffixedGroups.Has(gv.Group) {
			errs = append(errs, fmt.Errorf("group %s does not have the standard kubernetes API group suffix of .k8s.io", gv.Group))
		}
	}
	return errors.NewAggregate(errs)
}

// VerifyTagNaming 确保 scheme 中外部类型的字段带 JSON tag、内部类型不带。
// typesAllowedTags 与 allowedNonstandardJSONNames 可用于豁免特定类型。
func VerifyTagNaming(scheme *runtime.Scheme, typesAllowedTags map[reflect.Type]bool, allowedNonstandardJSONNames map[reflect.Type]string) error {
	errs := []error{}
	for gvk, knownType := range scheme.AllKnownTypes() {
		var err error
		// 内部类型从不直接序列化，tag 只会掩盖问题；外部类型必须完整标注以兼容 JSON。
		if gvk.Version == runtime.APIVersionInternal {
			err = errors.NewAggregate(ensureNoTags(gvk, knownType, nil, typesAllowedTags))
		} else {
			err = errors.NewAggregate(ensureTags(gvk, knownType, nil, allowedNonstandardJSONNames))
		}
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.NewAggregate(errs)
}

// ensureNoTags 遍历类型图，报告内部类型字段上的 json 或 protobuf tag。
// typesAllowedTags 可豁免自行实现编解码等特定类型。
func ensureNoTags(gvk schema.GroupVersionKind, tp reflect.Type, parents []reflect.Type, typesAllowedTags map[reflect.Type]bool) []error {
	errs := []error{}
	if _, ok := typesAllowedTags[tp]; ok {
		return errs
	}

	// 同一类型只检查一次，同时终止列表等自引用类型的递归遍历。
	if containsType(parents, tp) {
		return nil
	}
	parents = append(parents, tp)

	switch tp.Kind() {
	case reflect.Map, reflect.Slice, reflect.Pointer:
		errs = append(errs, ensureNoTags(gvk, tp.Elem(), parents, typesAllowedTags)...)

	case reflect.String, reflect.Bool, reflect.Float32, reflect.Float64, reflect.Int32, reflect.Int64, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr, reflect.Interface:
		// 标量不能携带 tag，无需检查。

	case reflect.Struct:
		for i := 0; i < tp.NumField(); i++ {
			f := tp.Field(i)
			if f.PkgPath != "" {
				continue // 忽略未导出字段
			}
			// 未导出字段不会编码，因此无需 tag。
			_, jsonTagExists := f.Tag.Lookup("json")
			protoTag := f.Tag.Get("protobuf")
			if jsonTagExists || len(protoTag) > 0 {
				errs = append(errs, fmt.Errorf("internal types should not have json or protobuf tags. %#v has tag on field %v: %v.\n%s", gvk, f.Name, f.Tag, fmtParentString(parents)))
			}

			errs = append(errs, ensureNoTags(gvk, f.Type, parents, typesAllowedTags)...)
		}

	default:
		errs = append(errs, fmt.Errorf("unexpected type %v in %#v.\n%s", tp.Kind(), gvk, fmtParentString(parents)))
	}
	return errs
}

// ensureTags 遍历类型图，报告缺少 JSON tag 或 JSON 名称不以小写字母开头的
// 外部类型字段；allowedNonstandardJSONNames 允许特定类型使用其他名称。
func ensureTags(gvk schema.GroupVersionKind, tp reflect.Type, parents []reflect.Type, allowedNonstandardJSONNames map[reflect.Type]string) []error {
	errs := []error{}
	// 该类型自行编解码，不需要 json tag。
	if tp.Implements(marshalerType) && (tp.Implements(unmarshalerType) || reflect.PointerTo(tp).Implements(unmarshalerType)) {
		return errs
	}

	// 同一类型只检查一次，同时终止列表等自引用类型的递归遍历。
	if containsType(parents, tp) {
		return nil
	}
	parents = append(parents, tp)

	switch tp.Kind() {
	case reflect.Map, reflect.Slice, reflect.Pointer:
		errs = append(errs, ensureTags(gvk, tp.Elem(), parents, allowedNonstandardJSONNames)...)

	case reflect.String, reflect.Bool, reflect.Float32, reflect.Float64, reflect.Int32, reflect.Int64, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr, reflect.Interface:
		// 标量不能携带 tag，无需检查。

	case reflect.Struct:
		for i := 0; i < tp.NumField(); i++ {
			f := tp.Field(i)
			jsonTag, jsonTagExists := f.Tag.Lookup("json")
			if len(jsonTag) == 0 {
				if f.Anonymous && jsonTagExists {
					// 允许嵌入字段使用 json:""
				} else {
					errs = append(errs, fmt.Errorf("external types should have json tags. %#v tags on field %v are: %s.\n%s", gvk, f.Name, f.Tag, fmtParentString(parents)))
				}
			}

			// Kubernetes JSON 名称使用 lowerCamelCase；"-" 表示跳过字段，可使用任意名称；
			// 显式豁免的类型不受此限制。
			jsonTagName := strings.Split(jsonTag, ",")[0]
			if len(jsonTagName) > 0 && (jsonTagName[0] < 'a' || jsonTagName[0] > 'z') && jsonTagName != "-" && allowedNonstandardJSONNames[tp] != jsonTagName {
				errs = append(errs, fmt.Errorf("external types should have json names starting with lowercase letter. %#v has json tag on field %v with name %s.\n%s", gvk, f.Name, jsonTagName, fmtParentString(parents)))
			}

			errs = append(errs, ensureTags(gvk, f.Type, parents, allowedNonstandardJSONNames)...)
		}

	default:
		errs = append(errs, fmt.Errorf("unexpected type %v in %#v.\n%s", tp.Kind(), gvk, fmtParentString(parents)))
	}
	return errs
}

// fmtParentString 生成遍历链供错误信息定位出错的嵌套字段。
func fmtParentString(parents []reflect.Type) string {
	str := "Type parents:\n"
	for i, tp := range parents {
		str += fmt.Sprintf("%s%v\n", strings.Repeat(" ", i), tp)
	}
	return str
}

// containsType 判断 s 是否包含 t。
func containsType(s []reflect.Type, t reflect.Type) bool {
	for _, u := range s {
		if t == u {
			return true
		}
	}
	return false
}
