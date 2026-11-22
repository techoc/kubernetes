/*
Copyright 2021 The Kubernetes Authors.

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

package generators

import (
	"k8s.io/gengo/v2/types"

	"k8s.io/code-generator/cmd/client-gen/generators/util"
)

// refGraph 把已有类型映射到其对应 applyConfig 类型将要生成到的包，
// 以便正确生成 apply 配置之间的引用。
type refGraph map[types.Name]string

// refGraphForReachableTypes 返回一个 refGraph，包含从给定包中
// 根 clientgen 类型出发可达的所有类型。
func refGraphForReachableTypes(universe types.Universe, pkgTypes map[string]*types.Package, initialTypes map[types.Name]string) refGraph {
	var refs refGraph = initialTypes

	// 只收录从根 clientgen 类型可达的类型。
	// 若为不可达类型生成 apply 配置，生成的代码将没有调用方，纯属噪声。
	reachableTypes := map[types.Name]*types.Type{}
	for _, p := range pkgTypes {
		for _, t := range p.Types {
			tags := genclientTags(t)
			hasApply := tags.HasVerb("apply") || tags.HasVerb("applyStatus")
			// 只有带 genclient 且显式请求 apply 动词的类型才是“根”类型。
			if tags.GenerateClient && hasApply {
				findReachableTypes(t, reachableTypes)
			}
			// 若 apply 扩展声明了自定义输入类型，也要把该输入类型纳入可达集合，
			// 否则生成的 apply 配置会引用一个未生成的类型。
			for _, extension := range tags.Extensions {
				if extension.HasVerb("apply") {
					if len(extension.InputTypeOverride) > 0 {
						inputType := *t
						if name, pkg := extension.Input(); len(pkg) > 0 {
							inputType = *(universe.Type(types.Name{Package: pkg, Name: name}))
						} else {
							inputType.Name.Name = extension.InputTypeOverride
						}
						findReachableTypes(&inputType, reachableTypes)
					}
				}
			}
		}
	}
	// 第二遍：为可达且满足 apply 配置生成条件的类型登记其所在包。
	for pkg, p := range pkgTypes {
		for _, t := range p.Types {
			if _, ok := reachableTypes[t.Name]; !ok {
				continue
			}
			if requiresApplyConfiguration(t) {
				refs[t.Name] = pkg
			}
		}
	}

	return refs
}

// applyConfigForType 查找字段在生成的 apply 配置中应使用的类型。
// 结果要么是原类型，要么是另一个已生成的 applyConfig 类型。
func (t refGraph) applyConfigForType(field *types.Type) *types.Type {
	switch field.Kind {
	case types.Struct:
		// 结构体若在 refGraph 中登记过，说明它有自己的 applyConfig，
		// 必须把字段类型改写为带 ApplyConfiguration 后缀的引用类型。
		if pkg, ok := t[field.Name]; ok { // TODO(jpbetz): Refs to types defined in a separate system (e.g. TypeMeta if generating a 3rd party controller) end up referencing the go struct, not the apply configuration type
			return types.Ref(pkg, field.Name.Name+ApplyConfigurationTypeSuffix)
		}
		return field
	case types.Map:
		// 仅当值类型有 applyConfig 时才重建该 map；键类型走同一逻辑，
		// 非结构体键会原样返回。
		if _, ok := t[field.Elem.Name]; ok {
			return &types.Type{
				Kind: types.Map,
				Elem: t.applyConfigForType(field.Elem),
				Key:  t.applyConfigForType(field.Key),
			}
		}
		return field
	case types.Slice:
		// 元素需要 applyConfig 时，slice 本身也要重建以携带新的 Elem。
		if _, ok := t[field.Elem.Name]; ok {
			return &types.Type{
				Kind: types.Slice,
				Elem: t.applyConfigForType(field.Elem),
			}
		}
		return field
	case types.Pointer:
		// apply 配置不使用指针字段，直接下钻到被指向的类型。
		return t.applyConfigForType(field.Elem)
	default:
		return field
	}
}

// isApplyConfig 报告给定类型（或指针指向的类型）是否拥有生成的 applyConfig。
func (t refGraph) isApplyConfig(field *types.Type) bool {
	switch field.Kind {
	case types.Struct:
		_, ok := t[field.Name]
		return ok
	case types.Pointer:
		return t.isApplyConfig(field.Elem)
	}
	return false
}

// genclientTags 返回给定类型的 genclient 标签。
// SecondClosestCommentLines 先于 CommentLines 拼接，与 gengo 的标签解析顺序一致。
func genclientTags(t *types.Type) util.Tags {
	return util.MustParseClientGenTags(append(t.SecondClosestCommentLines, t.CommentLines...))
}

// findReachableTypes 找出从根类型出发传递可达的所有类型，包含根类型自身。
// 已访问过的类型直接返回，避免递归类型导致无限循环。
func findReachableTypes(t *types.Type, referencedTypes map[types.Name]*types.Type) {
	if _, ok := referencedTypes[t.Name]; ok {
		return
	}
	referencedTypes[t.Name] = t

	// 依次遍历类型图中所有可能的子类型槽位。
	if t.Elem != nil {
		findReachableTypes(t.Elem, referencedTypes)
	}
	if t.Underlying != nil {
		findReachableTypes(t.Underlying, referencedTypes)
	}
	if t.Key != nil {
		findReachableTypes(t.Key, referencedTypes)
	}
	for _, m := range t.Members {
		findReachableTypes(m.Type, referencedTypes)
	}
}

// excludeTypes 包含不为其实生成 apply 配置的已知类型。
// 之所以硬编码：类型体系中只有这两个具有特殊用途的类型需要排除。
var excludeTypes = map[types.Name]struct{}{
	rawExtension.Name: {},
	unknown.Name:      {},
	// DO NOT ADD TO THIS LIST. If we need to exclude other types, we should consider allowing the
	// go type declarations to be annotated as excluded from this generator.
}

// requiresApplyConfiguration 报告是否应为给定类型生成 applyConfig。
// 仅为含有带 json 标签字段的结构体类型生成：没有 json 标签的字段不会出现在
// API 序列化中，也就无需 apply 配置。
func requiresApplyConfiguration(t *types.Type) bool {
	// 别名没有自己的结构定义，需穿透到底层类型判断。
	for t.Kind == types.Alias {
		t = t.Underlying
	}
	if t.Kind != types.Struct {
		return false
	}
	if _, ok := excludeTypes[t.Name]; ok {
		return false
	}
	var hasJSONTaggedMembers bool
	for _, member := range t.Members {
		if _, ok := lookupJSONTags(member); ok {
			hasJSONTaggedMembers = true
		}
	}
	return hasJSONTaggedMembers
}
