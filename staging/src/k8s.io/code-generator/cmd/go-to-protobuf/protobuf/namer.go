/*
Copyright 2015 The Kubernetes Authors.

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

package protobuf

import (
	"fmt"
	"reflect"
	"strings"

	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/types"
)

// localNamer 在“本包上下文”内为类型生成 protobuf 类型名：
// 同包类型用短名，外部类型用全限定名，避免 IDL 中命名冲突。
type localNamer struct {
	localPackage types.Name
}

// Name 返回类型在 protobuf IDL 中的引用名。
func (n localNamer) Name(t *types.Type) string {
	// map 不是 protobuf 的一等类型，需展开成 map<key, value> 语法。
	if t.Key != nil && t.Elem != nil {
		return fmt.Sprintf("map<%s, %s>", n.Name(t.Key), n.Name(t.Elem))
	}
	// 本包类型直接用短名。
	if len(n.localPackage.Package) != 0 && n.localPackage.Package == t.Name.Package {
		return t.Name.Name
	}
	// 非本包且非内建类型使用绝对引用，在 Go 包路径前加 "."，符合 protobuf 规范：
	// see https://protobuf.com/docs/language-spec#type-references
	// 这里用 "是否含点" 粗略判断是否为内建类型（内建类型名不含点）。
	if strings.Contains(t.Name.Package, ".") {
		return fmt.Sprintf(".%s", t.Name)
	}
	return t.Name.String()
}

// protobufNamer 管理所有参与生成的 protobuf 包，并在 Go 名与 proto 名之间转换。
type protobufNamer struct {
	packages []*protobufPackage
	// key 是 Go import-path，保证同一路径只登记一次。
	// The key here is a Go import-path.
	packagesByPath map[string]*protobufPackage
}

// NewProtobufNamer 创建空的 protobufNamer。
func NewProtobufNamer() *protobufNamer {
	return &protobufNamer{
		packagesByPath: make(map[string]*protobufPackage),
	}
}

// Name 返回类型在 proto 中的名称；map 需展开为 map<k, v>。
func (n *protobufNamer) Name(t *types.Type) string {
	if t.Kind == types.Map {
		return fmt.Sprintf("map<%s, %s>", n.Name(t.Key), n.Name(t.Elem))
	}
	return t.Name.String()
}

// Add 登记一个 protobuf 包；按 import-path 去重，保持登记的先后顺序。
func (n *protobufNamer) Add(p *protobufPackage) {
	if _, ok := n.packagesByPath[p.Path()]; !ok {
		n.packagesByPath[p.Path()] = p
		n.packages = append(n.packages, p)
	}
}

// GoNameToProtoName 把 Go 类型名映射为 proto 类型名（含包名与 import path）。
// 先在按路径索引的表中查找；若未命中，再按各包登记的 FilterTypes 反查，
// 以兼容生成过程中才加入的类型。
func (n *protobufNamer) GoNameToProtoName(name types.Name) types.Name {
	if p, ok := n.packagesByPath[name.Package]; ok {
		return types.Name{
			Name:    name.Name,
			Package: p.Name(),
			Path:    p.ImportPath(),
		}
	}
	for _, p := range n.packages {
		if _, ok := p.FilterTypes[name]; ok {
			return types.Name{
				Name:    name.Name,
				Package: p.Name(),
				Path:    p.ImportPath(),
			}
		}
	}
	return types.Name{Name: name.Name}
}

// protoSafePackage 把 Go 风格的包名（含 "/" 与 "-"）转换为合法的 proto 包名：
// 路径分隔符替换为 "."，连字符替换为 "_"。
func protoSafePackage(name string) string {
	pkg := strings.ReplaceAll(name, "/", ".")
	return strings.ReplaceAll(pkg, "-", "_")
}

// typeNameSet 记录类型名到其所属 protobuf 包的映射。
type typeNameSet map[types.Name]*protobufPackage

// assignGoTypeToProtoPackage 沿 t 的引用关系递归查找其依赖的 Go 与 Protobuf 类型，
// 并登记到 p 的导入中；遇到已是 protobuf 的类型不再深入。
// assignGoTypeToProtoPackage looks for Go and Protobuf types that are referenced by a type in
// a package. It will not recurse into protobuf types.
func assignGoTypeToProtoPackage(p *protobufPackage, t *types.Type, local, global typeNameSet, optional map[types.Name]struct{}) {
	// 基本类型若本身对应 proto 基本类型，先换成 proto 表示。
	newT, isProto := isFundamentalProtoType(t)
	if isProto {
		t = newT
	}
	// 全局表命中说明该类型已在其他包登记，只需为本包补一个导入。
	if otherP, ok := global[t.Name]; ok {
		if _, ok := local[t.Name]; !ok {
			p.Imports.AddType(&types.Type{
				Kind: types.Protobuf,
				Name: otherP.ProtoTypeName(),
			})
		}
		return
	}
	if t.Name.Package == p.Path() {
		// 类型只归属于其所在包，避免跨包重复登记。
		// Associate types only to their own package
		global[t.Name] = p
	}
	if _, ok := local[t.Name]; ok {
		return
	}
	// 已是 proto 类型则直接加入导入，不再递归。
	// don't recurse into existing proto types
	if isProto {
		p.Imports.AddType(t)
		return
	}

	local[t.Name] = p
	for _, m := range t.Members {
		// 私有字段不会出现在 proto 中。
		if namer.IsPrivateGoName(m.Name) {
			continue
		}
		field := &protoField{}
		tag := reflect.StructTag(m.Tags).Get("protobuf")
		// protobuf:"-" 表示该字段不参与 proto。
		if tag == "-" {
			continue
		}
		// 字段可能有自定义 proto 类型（protobuf tag 指定），优先按 tag 解析。
		if err := protobufTagToField(tag, field, m, t, p.ProtoTypeName()); err == nil && field.Type != nil {
			assignGoTypeToProtoPackage(p, field.Type, local, global, optional)
			continue
		}
		assignGoTypeToProtoPackage(p, m.Type, local, global, optional)
	}
	// TODO: should methods be walked?
	// 沿着 map/slice 等复合类型的 key/elem 以及底层类型继续深入。
	if t.Elem != nil {
		assignGoTypeToProtoPackage(p, t.Elem, local, global, optional)
	}
	if t.Key != nil {
		assignGoTypeToProtoPackage(p, t.Key, local, global, optional)
	}
	if t.Underlying != nil {
		// 可选别名的信息需要单独记录，供生成 proto3 optional 时使用。
		if t.Kind == types.Alias && isOptionalAlias(t) {
			optional[t.Name] = struct{}{}
		}
		assignGoTypeToProtoPackage(p, t.Underlying, local, global, optional)
	}
}

// isTypeApplicableToProtobuf 判断类型是否需要参与 protobuf 处理。
// 目前过滤掉函数与私有类型。
// isTypeApplicableToProtobuf checks to see if a type is relevant for protobuf processing.
// Currently, it filters out functions and private types.
func isTypeApplicableToProtobuf(t *types.Type) bool {
	// 函数与“声明为函数”的类型与 protobuf 无关。
	// skip functions -- we don't care about them for protobuf
	if t.Kind == types.Func || (t.Kind == types.DeclarationOf && t.Underlying.Kind == types.Func) {
		return false
	}
	// 私有类型无法出现在 IDL 中。
	// skip private types
	if namer.IsPrivateGoName(t.Name.Name) {
		return false
	}

	return true
}

// AssignTypesToPackages 为每个 protobuf 包计算它需要导入的类型，
// 并填充 FilterTypes/LocalNames/OptionalTypeNames。
func (n *protobufNamer) AssignTypesToPackages(c *generator.Context) error {
	// global 跨包共享，保证一个类型只归属于它定义所在的包。
	global := make(typeNameSet)
	for _, p := range n.packages {
		local := make(typeNameSet)
		optional := make(map[types.Name]struct{})
		p.Imports = NewImportTracker(p.ProtoTypeName())
		for _, t := range c.Order {
			if t.Name.Package != p.Path() {
				continue
			}
			if !isTypeApplicableToProtobuf(t) {
				// 跳过不关心的类型，如函数。
				// skip types that we don't care about, like functions
				continue
			}
			assignGoTypeToProtoPackage(p, t, local, global, optional)
		}
		p.FilterTypes = make(map[types.Name]struct{})
		p.LocalNames = make(map[string]struct{})
		p.OptionalTypeNames = make(map[string]struct{})
		for k, v := range local {
			// 仅把真正属于本包的类型计入本包的过滤与本地名集合。
			if v == p {
				p.FilterTypes[k] = struct{}{}
				p.LocalNames[k.Name] = struct{}{}
				if _, ok := optional[k]; ok {
					p.OptionalTypeNames[k.Name] = struct{}{}
				}
			}
		}
	}
	return nil
}
