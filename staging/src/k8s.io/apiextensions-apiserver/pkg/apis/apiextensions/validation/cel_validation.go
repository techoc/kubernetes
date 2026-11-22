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

package validation

import (
	"fmt"
	"math"
	"sort"

	"k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/cel/model"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/cel"
	"k8s.io/utils/ptr"
)

// unbounded 用 nil 表示无界的基数取值。
var unbounded *uint64 = nil

// CELSchemaContext 记录特定 schema 节点的 x-kubernetes-validations 规则所使用的数据。
type CELSchemaContext struct {
	// withinValidationRuleScope 在当前位置或上层 schema 带有 x-kubernetes-validations 规则时为 true。
	// 只有在该值为 true 的 schema 节点上才应填充 typeInfo。
	withinValidationRuleScope bool

	// typeInfo 对处于 withinValidationRuleScope 的 schema 节点按需加载，可能通过以下两种方式之一填充：
	//   1. 使用 typeInfoAccessor 从父级类型信息中获取。该操作开销很小，
	//      当更高层级的 schema 已有类型信息时应优先使用。
	//   2. 使用 converter 从 jsonSchema 构造类型信息。该操作开销较大。
	typeInfo *CELTypeInfo
	// typeInfoErr 缓存按需加载 typeInfo 时产生的错误。
	typeInfoErr error

	// parent 是父 schema 节点的上下文；若当前是根 schema 节点的上下文则为 nil。
	parent *CELSchemaContext
	// typeInfoAccessor 提供从父级 CELSchemaContext 访问当前 schema 节点类型信息的方式。
	// 无法提取或父级为 nil 时为 nil。
	typeInfoAccessor typeInfoAccessor

	// jsonSchema 是当前 CELSchemaContext 节点的 schema，必须非 nil。
	jsonSchema *apiextensions.JSONSchemaProps
	// converter 把 JSONSchemaProps 转换为 CELTypeInfo。
	// 需要统计 CRD 校验过程中转换次数的测试会用带计数的 converter 包装 DefaultConverter。
	converter converter

	// MaxCardinality 表示基于父 schema 上的 MaxProperties 或 MaxItems 限制，
	// 当前 schema 可存在的数据元素数量上限。若所有父级 map 和 array schema
	// 都声明了 MaxProperties 或 MaxItems，MaxCardinality 是这些限制乘积的指针；
	// 若至少有一个父级 map 或 list schema 未设置 MaxProperties 或 MaxItems，
	// MaxCardinality 为 nil，表示父 schema 对当前 schema 元素数量不设上界。
	MaxCardinality *uint64
	// TotalCost 累计整个 CRD 的 x-kubernetes-validators 规则估算开销总和。
	// 每个 CustomResourceDefinition 分配一个 TotalCost，并在递归校验 OpenAPIv3 schema 时沿栈传递。
	TotalCost *TotalCost
}

// TypeInfo 返回当前 CELSchemaContext 节点的 CELTypeInfo。若当前上下文为 nil，
// 或当前层及以上没有 x-kubernetes-validations 规则，则返回 nil, nil。
// 返回的类型信息是共享的，调用方不应修改。
func (c *CELSchemaContext) TypeInfo() (*CELTypeInfo, error) {
	if c == nil || !c.withinValidationRuleScope {
		return nil, nil
	}
	if c.typeInfo != nil || c.typeInfoErr != nil {
		return c.typeInfo, c.typeInfoErr // 已有计算结果则直接返回
	}

	// 若能从父级类型信息获取，则优先采用，因为更高效。
	if c.parent != nil {
		parentTypeInfo, parentErr := c.parent.TypeInfo()
		if parentErr != nil {
			c.typeInfoErr = parentErr
			return nil, parentErr
		}
		if parentTypeInfo != nil && c.typeInfoAccessor != nil {
			c.typeInfo = c.typeInfoAccessor.accessTypeInfo(parentTypeInfo)
			if c.typeInfo != nil {
				return c.typeInfo, nil
			}
		}
	}
	// 无法从父级获取时，把 jsonSchema 转换为类型信息。对大型 schema 开销较大。
	c.typeInfo, c.typeInfoErr = c.converter(c.jsonSchema, c.parent == nil || c.jsonSchema.XEmbeddedResource)
	return c.typeInfo, c.typeInfoErr
}

// CELTypeInfo 表示 CEL 编译某个 schema 节点的 x-kubernetes-validations 规则所需的全部类型信息。
type CELTypeInfo struct {
	// Schema 是当前 CELSchemaContext 节点的结构化 schema，必须非 nil。
	Schema *structuralschema.Structural
	// DeclType 是 Schema 在当前 CELSchemaContext 节点上的 CEL 声明表示，必须非 nil。
	DeclType *cel.DeclType
}

// converter 把 JSON schema 转换为结构化 schema 和 CEL declType，失败时返回错误。
// 除需要统计转换次数的测试外，都应为 defaultConverter。
type converter func(schema *apiextensions.JSONSchemaProps, isRoot bool) (*CELTypeInfo, error)

// defaultConverter 是生产环境使用的转换器实现。
func defaultConverter(schema *apiextensions.JSONSchemaProps, isRoot bool) (*CELTypeInfo, error) {
	structural, err := structuralschema.NewStructural(schema)
	if err != nil {
		return nil, err
	}
	declType := model.SchemaDeclType(structural, isRoot)
	if declType == nil {
		return nil, fmt.Errorf("unable to convert structural schema to CEL declarations")
	}
	return &CELTypeInfo{structural, declType}, nil
}

// RootCELContext 为给定根 schema 构造 CELSchemaContext。
func RootCELContext(schema *apiextensions.JSONSchemaProps) *CELSchemaContext {
	rootCardinality := uint64(1)
	r := &CELSchemaContext{
		jsonSchema:                schema,
		withinValidationRuleScope: len(schema.XValidations) > 0,
		MaxCardinality:            &rootCardinality,
		TotalCost:                 &TotalCost{},
		converter:                 defaultConverter,
	}
	return r
}

// ChildPropertyContext 为属性 propertyName 构造并返回 CELSchemaContext；
// 若当前 CELSchemaContext 为 nil 则返回 nil。
func (c *CELSchemaContext) ChildPropertyContext(propSchema *apiextensions.JSONSchemaProps, propertyName string) *CELSchemaContext {
	if c == nil {
		return nil
	}
	return c.childContext(propSchema, propertyTypeInfoAccessor{propertyName: propertyName})
}

// ChildAdditionalPropertiesContext 在当前 CELSchemaContext 为对象时，
// 为其 additionalProperties 构造并返回 CELSchemaContext；若当前为 nil 则返回 nil。
// propsSchema 必须非 nil，且 schema.AdditionalProperties 必须非 nil。
func (c *CELSchemaContext) ChildAdditionalPropertiesContext(propsSchema *apiextensions.JSONSchemaProps) *CELSchemaContext {
	if c == nil {
		return nil
	}
	return c.childContext(propsSchema, additionalItemsTypeInfoAccessor{})
}

// ChildItemsContext 在当前 CELSchemaContext 为数组时，
// 为其 items 构造并返回 CELSchemaContext；若当前为 nil 则返回 nil。
func (c *CELSchemaContext) ChildItemsContext(itemsSchema *apiextensions.JSONSchemaProps) *CELSchemaContext {
	if c == nil {
		return nil
	}
	return c.childContext(itemsSchema, itemsTypeInfoAccessor{})
}

// childContext 为当前 schema 上下文的指定子 schema 构造新的 CELSchemaContext；
// 若当前 CELSchemaContext 为 nil 则返回 nil。
// accessor 可选地提供从当前 schema 上下文的 CELTypeInfo 访问子节点 CELTypeInfo 的方式。
// 返回的 CELSchemaContext 的 MaxCardinality 乘以该 schema 对子节点基数
// （可能的数据元素数量）的放大系数。若 CELSchemaContext 的 MaxCardinality 无界（nil）
// 或该放大系数无界，则结果的 MaxCardinality 同样无界。
func (c *CELSchemaContext) childContext(child *apiextensions.JSONSchemaProps, accessor typeInfoAccessor) *CELSchemaContext {
	result := &CELSchemaContext{
		parent:                    c,
		typeInfoAccessor:          accessor,
		withinValidationRuleScope: c.withinValidationRuleScope,
		TotalCost:                 c.TotalCost,
		MaxCardinality:            unbounded,
		converter:                 c.converter,
	}
	if child != nil {
		result.jsonSchema = child
		if len(child.XValidations) > 0 {
			result.withinValidationRuleScope = true
		}
	}
	if c.jsonSchema == nil {
		// 可能传入 nil schema：ChildSchemaContext 会在
		// ValidateCustomResourceDefinitionOpenAPISchema 执行 nil 检查之前被调用
		return result
	}
	if c.MaxCardinality == unbounded {
		return result
	}
	maxElements := extractMaxElements(c.jsonSchema)
	if maxElements == unbounded {
		return result
	}
	result.MaxCardinality = ptr.To[uint64](multiplyWithOverflowGuard(*c.MaxCardinality, *maxElements))
	return result
}

// typeInfoAccessor 提供从父级类型信息查找子 schema 类型信息的能力。
type typeInfoAccessor interface {
	// accessTypeInfo 从非 nil 的 parentTypeInfo 中查找并返回子 schema 的类型信息；
	// 若无法访问该子 schema 信息则返回 nil。例如属性名在 CEL 中无法转义时预期返回 nil。
	// 调用方必须保证传入的 parentTypeInfo 非 nil。
	accessTypeInfo(parentTypeInfo *CELTypeInfo) *CELTypeInfo
}

// propertyTypeInfoAccessor 按属性名访问子 schema 的类型信息。
type propertyTypeInfoAccessor struct {
	// propertyName 是当前 schema 在父 schema 中声明所用的属性名。
	propertyName string
}

// accessTypeInfo 从父级 DeclType 的字段中查找对应属性的类型。
func (c propertyTypeInfoAccessor) accessTypeInfo(parentTypeInfo *CELTypeInfo) *CELTypeInfo {
	if parentTypeInfo.Schema.Properties != nil {
		propSchema := parentTypeInfo.Schema.Properties[c.propertyName]
		if escapedPropName, ok := cel.Escape(c.propertyName); ok {
			if fieldDeclType, ok := parentTypeInfo.DeclType.Fields[escapedPropName]; ok {
				return &CELTypeInfo{Schema: &propSchema, DeclType: fieldDeclType.Type}
			} // 类型未知的字段会被完全排除在 CEL 校验之外
		} // 无法转义的字段名预期不会出现
	}
	return nil
}

// itemsTypeInfoAccessor 通过数组元素访问子 schema 的类型信息。
type itemsTypeInfoAccessor struct{}

// accessTypeInfo 从父级 DeclType 的元素类型中获取 items 的类型。
func (c itemsTypeInfoAccessor) accessTypeInfo(parentTypeInfo *CELTypeInfo) *CELTypeInfo {
	if parentTypeInfo.Schema.Items != nil {
		itemsSchema := parentTypeInfo.Schema.Items
		itemsDeclType := parentTypeInfo.DeclType.ElemType
		return &CELTypeInfo{Schema: itemsSchema, DeclType: itemsDeclType}
	}
	return nil
}

// additionalItemsTypeInfoAccessor 通过 additionalProperties 值访问子 schema 的类型信息。
type additionalItemsTypeInfoAccessor struct{}

// accessTypeInfo 从父级 DeclType 的元素类型中获取 additionalProperties 值的类型。
func (c additionalItemsTypeInfoAccessor) accessTypeInfo(parentTypeInfo *CELTypeInfo) *CELTypeInfo {
	if parentTypeInfo.Schema.AdditionalProperties != nil {
		propsSchema := parentTypeInfo.Schema.AdditionalProperties.Structural
		valuesDeclType := parentTypeInfo.DeclType.ElemType
		return &CELTypeInfo{Schema: propsSchema, DeclType: valuesDeclType}
	}
	return nil
}

// TotalCost 跟踪一个 CustomResourceDefinition 全部 x-kubernetes-validations 规则的求值总开销。
type TotalCost struct {
	// Total 累计 x-kubernetes-validations 的估算规则开销总和。
	Total uint64
	// MostExpensive 累计对总和贡献最大的 4 条规则。仅包含至少占总开销上限 1% 的规则。
	MostExpensive []RuleCost
}

// ObserveExpressionCost 累计一条 x-kubernetes-validations 规则的求值开销。
func (c *TotalCost) ObserveExpressionCost(path *field.Path, cost uint64) {
	if math.MaxUint64-c.Total < cost {
		c.Total = math.MaxUint64
	} else {
		c.Total += cost
	}

	if cost < StaticEstimatedCRDCostLimit/100 { // 忽略占总开销上限不到 1% 的规则
		return
	}
	c.MostExpensive = append(c.MostExpensive, RuleCost{Path: path, Cost: cost})
	sort.Slice(c.MostExpensive, func(i, j int) bool {
		// 降序排序，使开销最大的规则排在最前
		return c.MostExpensive[i].Cost > c.MostExpensive[j].Cost
	})
	if len(c.MostExpensive) > 4 {
		c.MostExpensive = c.MostExpensive[:4]
	}
}

// RuleCost 表示单条 x-kubernetes-validations 规则的求值开销。
type RuleCost struct {
	Path *field.Path
	Cost uint64
}

// extractMaxElements 返回 schema 对其子节点基数（可能的数据元素数量）的放大系数。
// 若 schema 是 map 且有 MaxProperties，或数组有 MaxItems，则返回最大值的指针。
// 若 schema 是 map 或数组但没有 MaxProperties 或 MaxItems，则返回无界（nil），
// 表示当前 schema 不对可能的数据元素数量施加限制。若 schema 是对象，返回 1，
// 表示其子节点的可能元素数量没有增加。基本类型没有子节点，为简单起见也返回 1。
func extractMaxElements(schema *apiextensions.JSONSchemaProps) *uint64 {
	switch schema.Type {
	case "object":
		if schema.AdditionalProperties != nil {
			if schema.MaxProperties != nil {
				maxProps := uint64(zeroIfNegative(*schema.MaxProperties))
				return &maxProps
			}
			return unbounded
		}
		// 返回 1 表示对象的每个字段至多随其所属对象的每次出现而存在一份
		return ptr.To[uint64](1)
	case "array":
		if schema.MaxItems != nil {
			maxItems := uint64(zeroIfNegative(*schema.MaxItems))
			return &maxItems
		}
		return unbounded
	default:
		return ptr.To[uint64](1)
	}
}

// zeroIfNegative 把负数归零，避免无符号转换产生巨大值。
func zeroIfNegative(v int64) int64 {
	if v < 0 {
		return 0
	}
	return v
}
