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

package model

import (
	apimachineryvalidation "k8s.io/apimachinery/pkg/util/validation"
	apiservercel "k8s.io/apiserver/pkg/cel"
	"k8s.io/apiserver/pkg/cel/common"

	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
)

// SchemaDeclType 把结构化 schema 转换为 CEL 声明；若该 schema 不应暴露给 CEL 表达式，
// 则返回 nil。对于自定义资源或内嵌资源的根节点，isResourceRoot 置为 true。
//
// 带 XPreserveUnknownFields 的 schema 除非是 object 否则不暴露。数组和 map schema
// 若其 items 或 additionalProperties 未暴露，则自身也不暴露；object 的属性在其 schema
// 未暴露时同样不暴露。
//
// 对带 XPreserveUnknownFields 的 object，其 CEL 声明不会暴露未知字段。
func SchemaDeclType(s *schema.Structural, isResourceRoot bool) *apiservercel.DeclType {
	return common.SchemaDeclType(&Structural{Structural: s}, isResourceRoot)
}

// WithTypeAndObjectMeta 确保 kind、apiVersion、metadata.name 和 metadata.generateName
// 属性存在；必要时对传入 schema 做浅拷贝，避免修改调用方数据。
func WithTypeAndObjectMeta(s *schema.Structural) *schema.Structural {
	if s.Properties != nil &&
		s.Properties["kind"].Type == "string" &&
		s.Properties["apiVersion"].Type == "string" &&
		s.Properties["metadata"].Type == "object" &&
		s.Properties["metadata"].Properties != nil &&
		s.Properties["metadata"].Properties["name"].Type == "string" &&
		s.Properties["metadata"].Properties["generateName"].Type == "string" {
		return s
	}
	result := &schema.Structural{
		AdditionalProperties: s.AdditionalProperties,
		Generic:              s.Generic,
		Extensions:           s.Extensions,
		ValueValidation:      s.ValueValidation,
		ValidationExtensions: s.ValidationExtensions,
	}
	props := make(map[string]schema.Structural, len(s.Properties))
	for k, prop := range s.Properties {
		props[k] = prop
	}
	stringType := schema.Structural{Generic: schema.Generic{Type: "string"}}
	props["kind"] = stringType
	props["apiVersion"] = stringType
	metadataProps := s.Properties["metadata"].Properties
	props["metadata"] = schema.Structural{
		Generic: schema.Generic{Type: "object"},
		Properties: map[string]schema.Structural{
			"name":         cappedLengthStringType(metadataProps["name"], nameMaxLength),
			"generateName": cappedLengthStringType(metadataProps["generateName"], nameMaxLength-1), // 生成的后缀至少占 1 字节
		},
	}
	result.Properties = props

	return result
}

// nameMaxLength 是应用于 metadata.name 的默认 maxLength。
const nameMaxLength = int64(apimachineryvalidation.DNS1123SubdomainMaxLength)

// cappedLengthStringType 返回一个 maxLength 不超过 maxLength 的 string 类型；
// 若已有 schema 的 maxLength 更小，则沿用已有值。
func cappedLengthStringType(existing schema.Structural, maxLength int64) schema.Structural {
	if existing.ValueValidation != nil && existing.ValueValidation.MaxLength != nil {
		if *existing.ValueValidation.MaxLength < maxLength {
			maxLength = *existing.ValueValidation.MaxLength
		}
	}
	return schema.Structural{
		Generic:         schema.Generic{Type: "string"},
		ValueValidation: &schema.ValueValidation{MaxLength: &maxLength},
	}
}
