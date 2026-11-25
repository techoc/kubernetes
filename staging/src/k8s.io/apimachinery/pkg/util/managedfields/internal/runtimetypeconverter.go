/*
Copyright 2025 The Kubernetes Authors.

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

package internal

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/structured-merge-diff/v6/typed"
)

// schemeTypeConverter 借助 runtime.Scheme 把 GVK 解析成 OpenAPI 定义名，
// 再用 structured-merge-diff 的 typed.Parser 在 runtime.Object 与 TypedValue 之间转换。
type schemeTypeConverter struct {
	scheme *runtime.Scheme
	parser *typed.Parser
}

// 编译期确认实现了 TypeConverter 接口。
var _ TypeConverter = &schemeTypeConverter{}

// NewSchemeTypeConverter 创建使用给定 scheme 的 TypeConverter，
// 用于在 runtime.Object 与 TypedValue 之间转换。
// 中文补充：scheme 负责 GVK -> 类型名，parser 负责该类型对应的 schema。
func NewSchemeTypeConverter(scheme *runtime.Scheme, parser *typed.Parser) TypeConverter {
	return &schemeTypeConverter{scheme: scheme, parser: parser}
}

// ObjectToTyped 把对象转换为 typed 值：unstructured 对象走 FromUnstructured，
// 强类型对象走 FromStructured，前者能保留未知字段、后者会按 Go 类型做严格解析。
func (tc schemeTypeConverter) ObjectToTyped(obj runtime.Object, opts ...typed.ValidationOptions) (*typed.TypedValue, error) {
	gvk := obj.GetObjectKind().GroupVersionKind()
	name, err := tc.scheme.ToOpenAPIDefinitionName(gvk)
	if err != nil {
		return nil, err
	}
	t := tc.parser.Type(name)
	switch o := obj.(type) {
	case *unstructured.Unstructured:
		return t.FromUnstructured(o.UnstructuredContent(), opts...)
	default:
		return t.FromStructured(obj, opts...)
	}
}

// TypedToObject 把 typed 值还原为 unstructured 对象；顶层必须是 JSON 对象，
// 其它形态（数组、标量）无法表示成 Kubernetes 资源，因此返回错误。
func (tc schemeTypeConverter) TypedToObject(value *typed.TypedValue) (runtime.Object, error) {
	vu := value.AsValue().Unstructured()
	switch o := vu.(type) {
	case map[string]interface{}:
		return &unstructured.Unstructured{Object: o}, nil
	default:
		return nil, fmt.Errorf("failed to convert value to unstructured for type %T", vu)
	}
}
