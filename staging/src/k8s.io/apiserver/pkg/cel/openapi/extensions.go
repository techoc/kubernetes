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

package openapi

import (
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apiserver/pkg/cel/common"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

var intOrStringFormat = intstr.IntOrString{}.OpenAPISchemaFormat()

// isExtension 判断布尔型扩展是否显式为 true。
//
// 中文说明：必须同时检查存在性与值——扩展缺失时 GetBool 返回零值 false，
// 仅看值会把"未设置"和"设为 false"混为一谈。
func isExtension(schema *spec.Schema, key string) bool {
	v, ok := schema.Extensions.GetBool(key)
	return v && ok
}

// isXIntOrString 判断是否为 IntOrString 类型。
//
// 中文说明：内置类型用 format、CRD 用扩展，两条路径都要认，否则同一语义
// 在原生资源与自定义资源中表现不一致。
func isXIntOrString(schema *spec.Schema) bool {
	// built-in types have the Format while CRDs use extension
	// both are valid, checking both
	return schema.Format == intOrStringFormat || isExtension(schema, extIntOrString)
}

// isXEmbeddedResource 判断是否为嵌入资源（embedded resource）。
func isXEmbeddedResource(schema *spec.Schema) bool {
	return isExtension(schema, extEmbeddedResource)
}

// isXPreserveUnknownFields 判断是否保留未知字段（对应 CEL 中的 dyn 类型）。
func isXPreserveUnknownFields(schema *spec.Schema) bool {
	return isExtension(schema, extPreserveUnknownFields)
}

// getXListType 返回 x-kubernetes-list-type，未设置时为空串。
func getXListType(schema *spec.Schema) string {
	s, _ := schema.Extensions.GetString(extListType)
	return s
}

// getXMapType 返回 x-kubernetes-map-type，未设置时为空串。
func getXMapType(schema *spec.Schema) string {
	s, _ := schema.Extensions.GetString(extMapType)
	return s
}

// getXListMapKeys 返回 x-kubernetes-list-map-keys；缺失时返回 nil，
// 调用方据此区分"未声明"与"声明为空"。
func getXListMapKeys(schema *spec.Schema) []string {
	mapKeys, ok := schema.Extensions.GetStringSlice(extListMapKeys)
	if !ok {
		return nil
	}
	return mapKeys
}

// ValidationRule 是 x-kubernetes-validations 扩展条目的反序列化目标。
//
// 中文说明：字段名带 Field 后缀是为了与接口方法名区分；JSON 标签对应
// CRD 中校验规则的字段名，不能修改。
type ValidationRule struct {
	RuleField              string `json:"rule"`
	MessageField           string `json:"message"`
	MessageExpressionField string `json:"messageExpression"`
	PathField              string `json:"fieldPath"`
}

// Rule 返回 CEL 校验表达式。
func (v ValidationRule) Rule() string {
	return v.RuleField
}

// Message 返回静态错误信息。
func (v ValidationRule) Message() string {
	return v.MessageField
}

// FieldPath 返回错误信息中使用的字段路径。
func (v ValidationRule) FieldPath() string {
	return v.PathField
}

// MessageExpression 返回动态错误信息表达式。
func (v ValidationRule) MessageExpression() string {
	return v.MessageExpressionField
}

// TODO: simplify
//
// 中文说明：解析失败时返回 nil 而非错误，是因为扩展内容由 API 校验保证合法，
// 此处属于尽力而为的读取路径。
func getXValidations(schema *spec.Schema) []common.ValidationRule {
	var rules []ValidationRule
	err := schema.Extensions.GetObject(extValidations, &rules)
	if err != nil {
		return nil
	}
	// 逐个装箱为接口切片；ValidationRule 为值类型，接口方法也定义在值上。
	results := make([]common.ValidationRule, len(rules))
	for i, rule := range rules {
		results[i] = rule
	}
	return results
}

// 扩展键名与 CRD 的 OpenAPI 约定一一对应，属于对外契约，不可更改。
const extIntOrString = "x-kubernetes-int-or-string"
const extEmbeddedResource = "x-kubernetes-embedded-resource"
const extPreserveUnknownFields = "x-kubernetes-preserve-unknown-fields"
const extListType = "x-kubernetes-list-type"
const extMapType = "x-kubernetes-map-type"
const extListMapKeys = "x-kubernetes-list-map-keys"
const extValidations = "x-kubernetes-validations"
