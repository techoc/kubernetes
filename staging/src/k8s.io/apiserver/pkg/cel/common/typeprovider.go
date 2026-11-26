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

package common

import (
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// TypeResolver 按给定名称解析类型；
// 用于把 CRD 等运行期才确定的 schema 注入 CEL 编译环境。
type TypeResolver interface {
	// 按名称解析类型；名称不指向已知对象类型时返回 false。
	Resolve(name string) (ResolvedType, bool)
}

// ResolvedType 表示可按字段查询的对象类型。
type ResolvedType interface {
	ref.Type

	Type() *types.Type

	// 按名称查找字段；未知字段返回 false。直接返回 CEL 可识别的 FieldType，以便更灵活定制。
	Field(name string) (*types.FieldType, bool)

	// 返回该类型的字段名；类型不存在时返回 false。
	FieldNames() ([]string, bool)

	// 根据给定的字段与值创建该类型的一个实例。
	Val(fields map[string]ref.Val) ref.Val
}

// ResolverTypeProvider 优先用 TypeResolver 解析类型，TypeResolver 未能解析的类型
// 再委托给底层 types.Provider，保证内建类型语义不变。
type ResolverTypeProvider struct {
	typeResolver           TypeResolver
	underlyingTypeProvider types.Provider
}

var _ types.Provider = (*ResolverTypeProvider)(nil)

// FindStructType 先用 DynamicTypeResolver 查找限定名并转换为 CEL 类型。
// 动态解析器不认识该名称时，回退到底层 ResolverTypeProvider。
func (p *ResolverTypeProvider) FindStructType(structType string) (*types.Type, bool) {
	t, ok := p.typeResolver.Resolve(structType)
	if ok {
		return types.NewTypeTypeWithParam(t.Type()), true
	}
	return p.underlyingTypeProvider.FindStructType(structType)
}

// FindStructFieldNames 返回类型的字段名，类型不存在时返回 false。
func (p *ResolverTypeProvider) FindStructFieldNames(structType string) ([]string, bool) {
	t, ok := p.typeResolver.Resolve(structType)
	if ok {
		return t.FieldNames()
	}
	return p.underlyingTypeProvider.FindStructFieldNames(structType)
}

// FindStructFieldType 返回已检查类型值的字段类型；找不到字段时返回 false。
func (p *ResolverTypeProvider) FindStructFieldType(structType, fieldName string) (*types.FieldType, bool) {
	t, ok := p.typeResolver.Resolve(structType)
	if ok {
		return t.Field(fieldName)
	}
	return p.underlyingTypeProvider.FindStructFieldType(structType, fieldName)
}

// NewValue 根据限定名和字段映射创建类型值。
func (p *ResolverTypeProvider) NewValue(structType string, fields map[string]ref.Val) ref.Val {
	t, ok := p.typeResolver.Resolve(structType)
	if ok {
		return t.Val(fields)
	}
	return p.underlyingTypeProvider.NewValue(structType, fields)
}

// EnumValue 枚举值不涉及 CRD schema，直接委托给底层 provider。
func (p *ResolverTypeProvider) EnumValue(enumName string) ref.Val {
	return p.underlyingTypeProvider.EnumValue(enumName)
}

// FindIdent 标识符查找同样直接委托给底层 provider。
func (p *ResolverTypeProvider) FindIdent(identName string) (ref.Val, bool) {
	return p.underlyingTypeProvider.FindIdent(identName)
}

// ResolverEnvOption 用给定的 DynamicTypeResolver 构造 ResolverTypeProvider，并返回可应用到 env 的 CEL 选项。
// 在应用选项时捕获当前 env 的内置 provider 作为回退，避免构造顺序耦合。
func ResolverEnvOption(resolver TypeResolver) cel.EnvOption {
	return func(e *cel.Env) (*cel.Env, error) {
		tp := &ResolverTypeProvider{typeResolver: resolver, underlyingTypeProvider: e.CELTypeProvider()}
		// 为该 env 替换类型 provider。
		typeProviderOption := cel.CustomTypeProvider(tp)
		return typeProviderOption(e)
	}
}
