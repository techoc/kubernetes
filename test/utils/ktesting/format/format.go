/*
Copyright 2022 The Kubernetes Authors.

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

// Package format 是对 Gomega format 包的扩展，改进了那些能很好地序列化为 YAML
// 的对象（例如 Kubernetes API 中的结构体）的打印效果。
//
// 只需导入该包即可在 Gomega 中启用这种特殊的 YAML 支持。
package format

import (
	"reflect"
	"strings"

	"github.com/onsi/gomega/format"

	"sigs.k8s.io/yaml"
)

func init() {
	format.RegisterCustomFormatter(handleYAML)
}

// Object 使得无需导入对应包即可使用 Gomega 的 [format.Object]。
func Object(object interface{}, indentation uint) string {
	return format.Object(object, indentation)
}

// handleYAML 将结果用 YAML 表示可能更美观的所有值格式化为 YAML：
//   - 指向结构体的指针，或所有字段都带有 `json` 标签的结构体
//   - 包含此类值的切片
//   - 键或值为此类值的 map
func handleYAML(object interface{}) (string, bool) {
	value := reflect.ValueOf(object)
	if !useYAML(value.Type()) {
		return "", false
	}
	y, err := yaml.Marshal(object)
	if err != nil {
		return "", false
	}
	return "\n" + strings.TrimSpace(string(y)), true
}

// unstructured 是 [k8s.io/apimachinery/pkg/apis/meta/v1/unstructured.Unstructured] 的副本。
// 我们不能直接使用该类型（会依赖 apimachinery！），但可以检测它，因为它可以
// 转换为此处的副本。
//
// 如果 Unstructured 日后被修改，test/utils/format 中的测试将会失败。
type unstructured struct {
	Object map[string]interface{}
}

var unstructuredObjectType = reflect.TypeFor[unstructured]()

func useYAML(t reflect.Type) bool {
	if t.ConvertibleTo(unstructuredObjectType) {
		// unstructured.Unstructured 用 YAML 表示更美观。
		//
		// unstructured.Unstructured 本质上是一个 map，但由于它被包裹在
		// 结构体中，下面的代码无法将其识别为 map，因此需要直接检查。
		//
		// 我们对其他看起来像 unstructured.Unstructured 的类型也这样做，
		// 一方面因为无法显式检查它，另一方面因为这么做可能本来就是正确的。
		return true
	}

	switch t.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array:
		return useYAML(t.Elem())
	case reflect.Map:
		return useYAML(t.Key()) || useYAML(t.Elem())
	case reflect.Struct:
		// 所有字段都必须带有 `json` 标签。
		for i := 0; i < t.NumField(); i++ {
			field := t.Field(i)
			if _, ok := field.Tag.Lookup("json"); !ok {
				return false
			}
		}
		return true
	default:
		return false
	}
}
