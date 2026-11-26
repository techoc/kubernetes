//go:build go1.27

/*
Copyright The Kubernetes Authors.

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

package runtime

import (
	"reflect"
)

// isInlinedFromTag 判断字段是否通过 `json:",inline"`（Go 1.27 起的 "embed" 指令）
// 内联到父结构体中。
//
// 判断条件：字段必须是（可单层解引用的）结构体，且 tag 没有自定义名称、
// 只含唯一的 "embed" 指令。待办注释中提到的边界情况（非结构体内联、embed 与其他指令混用）
// 待切换到 json/v2 后再做严格报错。
func isInlinedFromTag(field reflect.StructField, tagName string, tagDirectives []string) bool {
	fieldType := field.Type
	if fieldType.Kind() == reflect.Pointer && fieldType.Name() == "" {
		// 可选地解开一层包装。
		// 只解一层指针：指向结构体的字段也可内联。
		fieldType = fieldType.Elem()
	}
	// TODO: 直接使用 json/v2 后，应对非结构体嵌入以及 embed 与其它指令同时出现的情况报错。
	return fieldType.Kind() == reflect.Struct && tagName == "" && len(tagDirectives) == 1 && tagDirectives[0] == "embed"
}
