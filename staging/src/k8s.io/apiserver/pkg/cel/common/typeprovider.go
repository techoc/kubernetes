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

// TypeResolver resolves a type by a given name.
// 用于把 CRD 等运行期才确定的 schema 注入 CEL 编译环境。
type TypeResolver interface {
	// Resolve resolves the type by its name.
	// This function returns false if the name does not refer to a known object type.
	Resolve(name string) (ResolvedType, bool)
}

// ResolvedType refers an object type that can be looked up for its fields.
type ResolvedType interface {
	ref.Type

	Type() *types.Type

	// Field finds the field by the field name, or false if the field is not known.
	// This function directly return a FieldType that is known to CEL to be more customizable.
	Field(name string) (*types.FieldType, bool)

	// FieldNames returns the field names associated with the type, if the type
	// is found.
	FieldNames() ([]string, bool)

	// Val creates an instance for the ResolvedType, given its fields and their values.
	Val(fields map[string]ref.Val) ref.Val
}

// ResolverTypeProvider delegates type resolution first to the TypeResolver and then
// to the underlying types.Provider for types not resolved by the TypeResolver.
// 先查自定义解析器，未命中再回退到 CEL 内置类型，保证内建类型语义不变。
type ResolverTypeProvider struct {
	typeResolver           TypeResolver
	underlyingTypeProvider types.Provider
}

var _ types.Provider = (*ResolverTypeProvider)(nil)

// FindStructType returns the Type give a qualified type name, by looking it up with
// the DynamicTypeResolver and translating it to CEL Type.
// If the type is not known to the DynamicTypeResolver, the lookup falls back to the underlying
// ResolverTypeProvider instead.
func (p *ResolverTypeProvider) FindStructType(structType string) (*types.Type, bool) {
	t, ok := p.typeResolver.Resolve(structType)
	if ok {
		return types.NewTypeTypeWithParam(t.Type()), true
	}
	return p.underlyingTypeProvider.FindStructType(structType)
}

// FindStructFieldNames returns the field names associated with the type, if the type
// is found.
func (p *ResolverTypeProvider) FindStructFieldNames(structType string) ([]string, bool) {
	t, ok := p.typeResolver.Resolve(structType)
	if ok {
		return t.FieldNames()
	}
	return p.underlyingTypeProvider.FindStructFieldNames(structType)
}

// FindStructFieldType returns the field type for a checked type value.
// Returns false if the field could not be found.
func (p *ResolverTypeProvider) FindStructFieldType(structType, fieldName string) (*types.FieldType, bool) {
	t, ok := p.typeResolver.Resolve(structType)
	if ok {
		return t.Field(fieldName)
	}
	return p.underlyingTypeProvider.FindStructFieldType(structType, fieldName)
}

// NewValue creates a new type value from a qualified name and map of fields.
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

// ResolverEnvOption creates the ResolverTypeProvider with a given DynamicTypeResolver
// and returns the CEL ResolverEnvOption to apply it to the env.
// 在应用选项时捕获当前 env 的内置 provider 作为回退，避免构造顺序耦合。
func ResolverEnvOption(resolver TypeResolver) cel.EnvOption {
	return func(e *cel.Env) (*cel.Env, error) {
		tp := &ResolverTypeProvider{typeResolver: resolver, underlyingTypeProvider: e.CELTypeProvider()}
		// set new type provider for the env.
		typeProviderOption := cel.CustomTypeProvider(tp)
		return typeProviderOption(e)
	}
}
