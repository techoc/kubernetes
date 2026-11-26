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

package common

import (
	"time"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"

	apiservercel "k8s.io/apiserver/pkg/cel"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// maxRequestSizeBytes 是单个请求的字节上限，用于估算无界字符串/数组/映射的 CEL 预算上限。
const maxRequestSizeBytes = apiservercel.DefaultMaxRequestSizeBytes

// SchemaDeclType 将结构化 schema 转换为 CEL 声明；若该 schema 不应暴露给 CEL 表达式则返回 nil。
//
// 自定义资源或内嵌资源的根节点要把 isResourceRoot 设为 true。
//
// XPreserveUnknownFields 的 schema 仅对象类型才暴露；数组与 "map" schema 若其 items 或 additionalProperties
// 未暴露则不暴露；对象属性若其 schema 未暴露也不暴露。
//
// 对带 XPreserveUnknownFields 的对象，CEL 声明不暴露未知字段。
//
// 返回 nil 是刻意的“不暴露”信号：调用方据此把该字段从声明中剔除，
// 从而让 CEL 表达式无法访问没有 schema 的数据。所有 MaxElements 都是估算值，
// 用于给 CEL 运行时预算做上界，而非精确校验。
func SchemaDeclType(s Schema, isResourceRoot bool) *apiservercel.DeclType {
	if s == nil {
		return nil
	}
	if s.IsXIntOrString() {
		// 使用 XIntOrString 的 schema 可以不指定 type。

		// intOrStringType 表示 CEL 表达式中的 x-kubernetes-int-or-string 联合类型。
		// 在 CEL 中它表示为动态值，可视为所有类型的联合。
		// XIntOrString 的类型检查全部推迟到运行时，因此所有取值都必须先用类型检查保护，例如：
		//
		// 要求字符串表示是百分比：
		//  `type(intOrStringField) == string && intOrStringField.matches(r'(\d+(\.\d+)?%)')`
		// 同时校验 int 与 string 两种表示：
		//  `type(intOrStringField) == int ? intOrStringField < 5 : double(intOrStringField.replace('%', '')) < 0.5
		//
		// 用 dyn 表示联合类型，所有类型检查推迟到运行时，因此表达式必须自行用 type() 守卫。
		dyn := apiservercel.NewSimpleTypeWithMinSize("dyn", cel.DynType, nil, 1) // x-kubernetes-int-or-string 序列化后的最小值为 0。

		// 若 schema 有 maxLength 约束，则据此限制最大元素数；
		// 否则回退到最大请求大小。
		if s.MaxLength() != nil {
			dyn.MaxElements = estimateMaxElementsFromMaxLength(s)
		} else {
			dyn.MaxElements = estimateMaxStringLengthPerRequest(s)
		}

		return dyn
	}

	// 这里忽略 XPreserveUnknownFields：对没有 schema 信息的数据不支持校验规则。
	//

	if isResourceRoot {
		// 资源根节点的校验规则始终可访问 'apiVersion'、'kind'、'metadata.name' 与 'metadata.generateName'，
		// 即使 schema 中没有声明。
		// 这包括自定义资源的根以及 XEmbeddedResource 对象的根。
		//
		// 这些字段是 apiserver 隐式保证存在的，即使 CRD schema 未声明也要暴露给校验规则。
		s = s.WithTypeAndObjectMeta()
	}

	switch s.Type() {
	case "array":
		if s.Items() != nil {
			itemsType := SchemaDeclType(s.Items(), s.Items().IsXEmbeddedResource())
			// 元素类型不可暴露时，整个数组也不暴露，避免出现“能取到数组却无法访问元素”的半吊子声明。
			if itemsType == nil {
				return nil
			}
			var maxItems int64
			if s.MaxItems() != nil {
				maxItems = zeroIfNegative(*s.MaxItems())
			} else {
				maxItems = estimateMaxArrayItemsFromMinSize(itemsType.MinSerializedSize)
			}
			return apiservercel.NewListType(itemsType, maxItems)
		}
		return nil
	case "object":
		if s.AdditionalProperties() != nil && s.AdditionalProperties().Schema() != nil {
			propsType := SchemaDeclType(s.AdditionalProperties().Schema(), s.AdditionalProperties().Schema().IsXEmbeddedResource())
			if propsType != nil {
				var maxProperties int64
				if s.MaxProperties() != nil {
					maxProperties = zeroIfNegative(*s.MaxProperties())
				} else {
					maxProperties = estimateMaxAdditionalPropertiesFromMinSize(propsType.MinSerializedSize)
				}
				return apiservercel.NewMapType(apiservercel.StringType, propsType, maxProperties)
			}
			return nil
		}
		fields := make(map[string]*apiservercel.DeclField, len(s.Properties()))

		required := map[string]bool{}
		if s.Required() != nil {
			for _, f := range s.Required() {
				required[f] = true
			}
		}
		// 对象至少会序列化为 {}，计算时需计入这一点。
		minSerializedSize := int64(2)
		for name, prop := range s.Properties() {
			var enumValues []interface{}
			if prop.Enum() != nil {
				for _, e := range prop.Enum() {
					enumValues = append(enumValues, e)
				}
			}
			if fieldType := SchemaDeclType(prop, prop.IsXEmbeddedResource()); fieldType != nil {
				// Escape 处理与 CEL 关键字/特殊字符冲突的字段名。
				if propName, ok := apiservercel.Escape(name); ok {
					fields[propName] = apiservercel.NewDeclField(propName, fieldType, required[name], enumValues, prop.Default())
				}
				// 对象的最小序列化大小为 2（{}）加上所有必需属性的最小大小之和；
				//
				// 只计入没有默认值的必需属性——默认值由服务端填充，
				// 不会出现在请求中。
				//
				// 有默认值的必填字段服务端会补全，不计入请求体积，否则会高估。
				if required[name] && prop.Default() == nil {
					minSerializedSize += int64(len(name)) + fieldType.MinSerializedSize + 4
				}
			}
		}
		objType := apiservercel.NewObjectType("object", fields)
		objType.MinSerializedSize = minSerializedSize
		return objType
	case "string":
		switch s.Format() {
		case "byte":
			byteWithMaxLength := apiservercel.NewSimpleTypeWithMinSize("bytes", cel.BytesType, types.Bytes([]byte{}), apiservercel.MinStringSize)
			if s.MaxLength() != nil {
				byteWithMaxLength.MaxElements = zeroIfNegative(*s.MaxLength())
			} else {
				byteWithMaxLength.MaxElements = estimateMaxStringLengthPerRequest(s)
			}
			return byteWithMaxLength
		case "duration":
			durationWithMaxLength := apiservercel.NewSimpleTypeWithMinSize("duration", cel.DurationType, types.Duration{Duration: time.Duration(0)}, int64(apiservercel.MinDurationSizeJSON))
			durationWithMaxLength.MaxElements = estimateMaxStringLengthPerRequest(s)
			return durationWithMaxLength
		case "date":
			timestampWithMaxLength := apiservercel.NewSimpleTypeWithMinSize("timestamp", cel.TimestampType, types.Timestamp{Time: time.Time{}}, int64(apiservercel.JSONDateSize))
			timestampWithMaxLength.MaxElements = estimateMaxStringLengthPerRequest(s)
			return timestampWithMaxLength
		case "date-time":
			timestampWithMaxLength := apiservercel.NewSimpleTypeWithMinSize("timestamp", cel.TimestampType, types.Timestamp{Time: time.Time{}}, int64(apiservercel.MinDatetimeSizeJSON))
			timestampWithMaxLength.MaxElements = estimateMaxStringLengthPerRequest(s)
			return timestampWithMaxLength
		}

		strWithMaxLength := apiservercel.NewSimpleTypeWithMinSize("string", cel.StringType, types.String(""), apiservercel.MinStringSize)
		if s.MaxLength() != nil {
			strWithMaxLength.MaxElements = estimateMaxElementsFromMaxLength(s)
		} else {
			// 枚举字符串的取值受限，用最长枚举值估算比按整个请求上限更贴近实际。
			if len(s.Enum()) > 0 {
				strWithMaxLength.MaxElements = estimateMaxStringEnumLength(s)
			} else {
				strWithMaxLength.MaxElements = estimateMaxStringLengthPerRequest(s)
			}
		}
		return strWithMaxLength
	case "boolean":
		return apiservercel.BoolType
	case "number":
		return apiservercel.DoubleType
	case "integer":
		return apiservercel.IntType
	}
	return nil
}

// zeroIfNegative 把负的 maxItems/maxLength 归零；OpenAPI 中负值无意义，但不应导致预算估算为负。
func zeroIfNegative(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}

// WithTypeAndObjectMeta 确保 kind、apiVersion、metadata.name 与 metadata.generateName 属性存在，
// 必要时对传入 schema 做浅拷贝。
//
// 只在缺失时补齐；已具备全部字段时直接返回原指针，避免无谓复制。
func WithTypeAndObjectMeta(s *spec.Schema) *spec.Schema {
	if s.Properties != nil &&
		s.Properties["kind"].Type.Contains("string") &&
		s.Properties["apiVersion"].Type.Contains("string") &&
		s.Properties["metadata"].Type.Contains("object") &&
		s.Properties["metadata"].Properties != nil &&
		s.Properties["metadata"].Properties["name"].Type.Contains("string") &&
		s.Properties["metadata"].Properties["generateName"].Type.Contains("string") {
		return s
	}
	result := *s
	props := make(map[string]spec.Schema, len(s.Properties))
	for k, prop := range s.Properties {
		props[k] = prop
	}
	stringType := spec.StringProperty()
	props["kind"] = *stringType
	props["apiVersion"] = *stringType
	props["metadata"] = spec.Schema{
		SchemaProps: spec.SchemaProps{
			Type: []string{"object"},
			Properties: map[string]spec.Schema{
				"name":         *stringType,
				"generateName": *stringType,
			},
		},
	}
	result.Properties = props

	return &result
}

// estimateMaxStringLengthPerRequest 估算在给定 schema 的格式约束下字符串的最大长度（字符数）。
//
// 只能对类型为 "string" 或 x-kubernetes-int-or-string: true 的 schema 调用。
func estimateMaxStringLengthPerRequest(s Schema) int64 {
	if s.IsXIntOrString() {
		// 对 x-kubernetes-int-or-string，按可能的最大字符串返回其最大长度与最小序列化大小。
		return maxRequestSizeBytes - 2
	}
	switch s.Format() {
	case "duration":
		return apiservercel.MaxDurationSizeJSON
	case "date":
		return apiservercel.JSONDateSize
	case "date-time":
		return apiservercel.MaxDatetimeSizeJSON
	default:
		// 减去 2 以计入两侧引号。
		return maxRequestSizeBytes - 2
	}
}

// estimateMaxStringLengthPerRequest 估算在给定 schema 的格式约束下字符串的最大长度（字符数）。
// 对具有枚举值的字符串，结果为最长枚举值的长度。
func estimateMaxStringEnumLength(s Schema) int64 {
	var maxLength int64
	for _, v := range s.Enum() {
		if s, ok := v.(string); ok && int64(len(s)) > maxLength {
			maxLength = int64(len(s))
		}
	}
	return maxLength
}

// estimateMaxArrayItemsPerRequest 估算在给定最小序列化大小下，单个请求中最多能容纳多少个数组元素。
//
// +1 用于补偿元素之间的逗号分隔符。
func estimateMaxArrayItemsFromMinSize(minSize int64) int64 {
	// 减去 2 以计入 [ 和 ]。
	return (maxRequestSizeBytes - 2) / (minSize + 1)
}

// estimateMaxAdditionalPropertiesPerRequest 估算在给定最小序列化大小下，单个请求中最多能容纳多少个额外属性。
func estimateMaxAdditionalPropertiesFromMinSize(minSize int64) int64 {
	// 每个键至少 2 字节，加上 ""、冒号、逗号与最小可能值；实际键长度通常各不相同，
	// 这里按最坏情况估算。
	keyValuePairSize := minSize + 6
	// 减去 2 以计入 { 和 }。
	return (maxRequestSizeBytes - 2) / keyValuePairSize
}

// estimateMaxElementsFromMaxLength 估算受 maxLength 约束的字符串 schema 的最大元素数。
//
// 乘 4 是因为 OpenAPI 的 maxLength 以码点计，而预算按字节计，单个码点最多 4 字节。
func estimateMaxElementsFromMaxLength(s Schema) int64 {
	// 对无类型的字符串，把用户提供的 max 长度乘以 4。
	// OpenAPIv3 规范中 maxLength 按 rune/码点计，但这里推算请求大小需要按字节，
	// 因为单个 Unicode 码点最多可占 4 字节。
	//
	return zeroIfNegative(*s.MaxLength()) * 4
}
