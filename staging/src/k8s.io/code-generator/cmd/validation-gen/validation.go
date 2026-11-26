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

package main

import (
	"bytes"
	"cmp"
	"fmt"
	"io"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"unicode"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/code-generator/cmd/validation-gen/util"
	"k8s.io/code-generator/cmd/validation-gen/validators"
	"k8s.io/gengo/v2/generator"
	"k8s.io/gengo/v2/namer"
	"k8s.io/gengo/v2/parser/tags"
	"k8s.io/gengo/v2/types"
	"k8s.io/klog/v2"
)

// mkPkgNames 将一组类型名批量绑定到同一包，简化生成器符号表声明。
func mkPkgNames(pkg string, names ...string) []types.Name {
	result := make([]types.Name, 0, len(names))
	for _, name := range names {
		result = append(result, types.Name{Package: pkg, Name: name})
	}
	return result
}

// 生成代码引用的运行时库符号。集中声明可避免模板里散落字符串，
// 也确保 import tracker 能正确收集依赖。
var (
	fieldPkg            = "k8s.io/apimachinery/pkg/util/validation/field"
	fieldPkgSymbols     = mkPkgNames(fieldPkg, "ErrorList", "InternalError", "Path")
	fmtPkgSymbols       = mkPkgNames("fmt", "Errorf", "Fprintln")
	safePkg             = "k8s.io/apimachinery/pkg/api/safe"
	safePkgSymbols      = mkPkgNames(safePkg, "Field", "Cast", "Value")
	operationPkg        = "k8s.io/apimachinery/pkg/api/operation"
	operationPkgSymbols = mkPkgNames(operationPkg, "Operation", "MatchesSubresource", "Update")
	contextPkg          = "context"
	contextPkgSymbols   = mkPkgNames(contextPkg, "Context")
	equalityPkg         = "k8s.io/apimachinery/pkg/api/equality"
	equalityPkgSymbols  = mkPkgNames(equalityPkg, "Semantic")
)

// genValidations 生成包含自动校验代码的文件。
// 每个输入包对应一个实例；inputToCanonicalPkg 用于把跨包引用解析到
// 最终生成位置。
type genValidations struct {
	generator.GoGenerator
	outputPackage       string
	inputPackage        string
	inputToCanonicalPkg map[string]string // input package -> the generated package cross-package references resolve to
	rootTypes           []*types.Type
	discovered          *typeDiscoverer
	imports             namer.ImportTracker
	schemeRegistry      types.Name
	emitRegisterFunc    bool
}

// NewGenValidations 为指定的包创建新的生成器。
// rootTypes 是本包显式选中的类型；discovered 提供全图信息以决定
// 哪些被引用类型也需要生成校验函数。
func NewGenValidations(outputFilename, outputPackage, inputPackage string, rootTypes []*types.Type, discovered *typeDiscoverer, inputToCanonicalPkg map[string]string, schemeRegistry types.Name, emitRegisterFunc bool) generator.Generator {
	return &genValidations{
		GoGenerator: generator.GoGenerator{
			OutputFilename: outputFilename,
		},
		outputPackage:       outputPackage,
		inputPackage:        inputPackage,
		inputToCanonicalPkg: inputToCanonicalPkg,
		rootTypes:           rootTypes,
		discovered:          discovered,
		imports:             generator.NewImportTrackerForPackage(outputPackage),
		schemeRegistry:      schemeRegistry,
		emitRegisterFunc:    emitRegisterFunc,
	}
}

// resolveFunc 把校验函数引用映射到实际调用它的包，按优先级套用第一条命中的规则：
//  1. 若本生成器校验该引用所属的输入包，指向本生成器的输出包（本地副本）；
//  2. 否则指向该输入的 canonical 包（若存在）；
//  3. 都不满足则原样返回。
func (g *genValidations) resolveFunc(name types.Name) types.Name {
	if name.Package == g.inputPackage {
		name.Package = g.outputPackage
	} else if pkg, ok := g.inputToCanonicalPkg[name.Package]; ok {
		name.Package = pkg
	}
	return name
}

// Namers 只使用 raw 命名系统，因为生成代码大量引用外部包类型。
func (g *genValidations) Namers(_ *generator.Context) namer.NameSystems {
	// 让 raw namer 负责登记本文件的 import。
	return namer.NameSystems{
		"raw": namer.NewRawNamer(g.outputPackage, g.imports),
	}
}

// Filter 决定是否为某个类型生成校验函数：root 类型总是生成；
// 其他类型仅当被 root 图引用且（传递地）含有可生成校验时才生成。
func (g *genValidations) Filter(_ *generator.Context, t *types.Type) bool {
	// 根类型一律生成。
	if slices.Contains(g.rootTypes, t) {
		return true
	}
	// 其他类型仅当被根类型图引用且含校验时才生成。仅作为别包的根被发现、
	// 未被任何类型引用的类型应跳过，避免产出不会被调用的校验函数。
	n := g.discovered.typeNodes[t]
	return n != nil && n.referenced && g.hasValidations(n)
}

// Imports 过滤目标包自身的 import，避免生成文件自引用。
func (g *genValidations) Imports(_ *generator.Context) (imports []string) {
	var importLines []string
	for _, singleImport := range g.imports.ImportLines() {
		if g.isOtherPackage(singleImport) {
			importLines = append(importLines, singleImport)
		}
	}
	return importLines
}

// isOtherPackage 判断 import 行是否来自目标包之外；import 行可能带引号，
// 因此同时比较裸路径与带引号形式。
func (g *genValidations) isOtherPackage(pkg string) bool {
	if pkg == g.outputPackage {
		return false
	}
	if strings.HasSuffix(pkg, `"`+g.outputPackage+`"`) {
		return false
	}
	return true
}

// Init 输出文件级代码：注册函数（若启用）与必要的初始化。
func (g *genValidations) Init(c *generator.Context, w io.Writer) error {
	klog.V(5).Infof("emitting registration code")
	sw := generator.NewSnippetWriter(w, c, "$", "$")
	if g.emitRegisterFunc {
		g.emitRegisterFunction(c, g.schemeRegistry, sw)
	}
	if err := sw.Error(); err != nil {
		return err
	}
	return nil
}

// GenerateType 为单个类型输出变量声明与校验函数。
func (g *genValidations) GenerateType(c *generator.Context, t *types.Type, w io.Writer) error {
	klog.V(5).Infof("emitting validation code for type %v", t)

	sw := generator.NewSnippetWriter(w, c, "$", "$")
	g.emitValidationVariables(c, t, sw)
	g.emitValidationFunction(c, t, sw)
	if err := sw.Error(); err != nil {
		return err
	}
	return nil
}

// typeDiscoverer 包含构建类型图所需的字段。
// 发现阶段只遍历一次类型图，后续生成阶段复用 typeNodes，避免重复解析
// 注释与递归类型。
type typeDiscoverer struct {
	initialized         bool
	validator           validators.ValidationExtractor
	inputToCanonicalPkg map[string]string

	// constantsByType 按类型收集对应的常量。
	constantsByType map[*types.Type][]*validators.Constant

	// typeNodes 记录发现过程中遇到的所有 gengo 类型及其 typeNode。
	typeNodes map[*types.Type]*typeNode
}

// NewTypeDiscoverer 创建类型发现器；调用 DiscoverType 前必须先调用 Init。
func NewTypeDiscoverer(validator validators.ValidationExtractor, inputToCanonicalPkg map[string]string) *typeDiscoverer {
	return &typeDiscoverer{
		validator:           validator,
		inputToCanonicalPkg: inputToCanonicalPkg,
		constantsByType:     map[*types.Type][]*validators.Constant{},
		typeNodes:           map[*types.Type]*typeNode{},
	}
}

// Init 借助生成器上下文为类型发现做准备。
// 预扫描参与生成的包中的常量，使 ref(...) 形式的默认值/校验参数可以在
// 类型发现阶段解析。
func (td *typeDiscoverer) Init(c *generator.Context) error {
	packages := c.Universe
	for _, pkg := range packages {
		// 只关心参与生成或只读的包。
		if _, ok := td.inputToCanonicalPkg[pkg.Path]; !ok {
			continue
		}
		for _, cnst := range pkg.Constants {
			context := validators.Context{
				Scope:          validators.ScopeConst,
				Type:           cnst.Underlying,
				Path:           nil, // NA when discovering a constant
				Member:         nil, // NA when discovering a constant
				ParentPath:     nil, // NA when discovering a constant
				ListSelector:   nil, // NA for constants
				ParentType:     nil, // NA for constants
				Constants:      nil, // NA for constants
				StabilityLevel: "",  // Default to stable unless overridden
			}
			tgs, err := td.validator.ExtractTags(context, cnst.CommentLines)
			if err != nil {
				return fmt.Errorf("constant %s: %w", cnst.Name, err)
			}
			if len(tgs) > 0 {
				// 同时校验标签解析出的校验规则是否合法。
				if _, err := td.validator.ExtractValidations(context, tgs...); err != nil {
					return fmt.Errorf("constant %s: %w", cnst.Name, err)
				}
			}
			td.constantsByType[cnst.Underlying] = append(td.constantsByType[cnst.Underlying], &validators.Constant{Constant: cnst, Tags: tgs})
		}
	}
	td.initialized = true
	return nil
}

// childNode 表示被其他类型使用（例如作为 struct 字段）的类型。
// 字段级校验与“迭代校验”分开保存：后者用于列表/映射元素，只有在确认
// 元素类型确实有校验时才输出，避免生成空函数。
type childNode struct {
	name      string      // the field name in the parent, populated when this node is a struct field
	jsonName  string      // always populated when name is populated
	childType *types.Type // the real type of the child (may be a pointer)
	node      *typeNode   // the node of the child's value type, or nil if it is in a foreign package

	fieldValidations validators.Validations // validations on the field

	// 迭代校验与字段校验不同，不计入 hasValidations。它让 list/map 类型
	// 生成逐元素校验代码，但发现阶段信息不足，无法判断是否可跳过。
	fieldValIterations validators.Validations // validations on each val
	fieldKeyIterations validators.Validations // validations on each key
}

// typeNode 表示类型图中的一个节点，并附带校验相关的信息。该类型里的全部内容
// （包括传递得到的部分）都归属于类型本身，而不是它的某个具体使用位置
// （例如被用作 struct 字段时）。
// valueType 永不为指针；指针信息由 childNode.childType 保留。
type typeNode struct {
	valueType  *types.Type // never a pointer, but may be a map, slice, struct, etc.
	funcName   types.Name  // populated when this type is has a validation function
	referenced bool        // true if used as a field/element/key/underlying of another type (set by discoverChild)

	fields     []*childNode // populated when this type is a struct
	key        *childNode   // populated when this type is a map
	elem       *childNode   // populated when this type is a map or slice
	underlying *childNode   // populated when this type is an alias

	typeValidations validators.Validations // validations on the type

	// 迭代校验与类型校验不同，不计入 hasValidations。它让 list/map 类型
	// 生成逐元素校验代码，但发现阶段信息不足，无法判断是否可跳过。
	typeValIterations validators.Validations // validations on each val
	typeKeyIterations validators.Validations // validations on each key
}

// resolveElemNode 穿透底层别名，找到切片/map 真正包含元素的节点。
func (n *typeNode) resolveElemNode() *typeNode {
	if n.elem != nil {
		return n.elem.node
	}
	if n.underlying != nil && n.underlying.node != nil && n.underlying.node.elem != nil {
		return n.underlying.node.elem.node
	}
	return nil
}

// resolveKeyNode 穿透底层别名，找到 map 真正包含键的节点。
func (n *typeNode) resolveKeyNode() *typeNode {
	if n.key != nil {
		return n.key.node
	}
	if n.underlying != nil && n.underlying.node != nil && n.underlying.node.key != nil {
		return n.underlying.node.key.node
	}
	return nil
}

// DiscoverType 递归遍历给定类型，在该 typeDiscoverer 中构建类型图。
// 对不同类型多次调用时，各张图会被合并。
// 指针根类型不被支持：根类型必须是可校验的值类型。
func (td *typeDiscoverer) DiscoverType(t *types.Type) error {
	if !td.initialized {
		return fmt.Errorf("typeDiscoverer not initialized")
	}
	if t.Kind == types.Pointer {
		return fmt.Errorf("type %v: pointer root-types are not supported", t)
	}
	fldPath := field.NewPath(t.Name.String())
	if node, err := td.discoverType(t, fldPath); err != nil {
		return err
	} else if node == nil {
		panic(fmt.Sprintf("discovered a nil node for type %v", t))
	}
	return nil
}

// discoverType 递归遍历类型并返回对应的 typeNode，不区分类型定义与结构体字段。
// 首次遇到类型时展开它；遇到已处理的类型则直接返回既有节点。
func (td *typeDiscoverer) discoverType(t *types.Type, fldPath *field.Path) (*typeNode, error) {
	// 除内建类型（gengo 放在空包名下）外，不能深入遍历本工具未处理的包。
	if t.Name.Package != "" {
		_, ok := td.inputToCanonicalPkg[t.Name.Package]
		if !ok {
			return nil, nil
		}
	}

	// 尽早拦截暂不支持的类型，让后续逻辑保持简单。
	if err := td.verifySupportedType(t); err != nil {
		return nil, fmt.Errorf("field %s (%s): %w", fldPath.String(), t, err)
	}

	// 发现过程针对值类型，指针先解引用。
	if t.Kind == types.Pointer {
		return td.discoverType(t.Elem, fldPath)
	}

	// 类型已处理过则直接返回，借此打断递归环。
	if node, found := td.typeNodes[t]; found {
		return node, nil
	}
	klog.V(4).InfoS("discoverType", "type", t, "kind", t.Kind, "path", fldPath.String())

	// 本函数剩余部分负责组装这个类型节点。
	thisNode := &typeNode{
		valueType: t,
	}
	td.typeNodes[t] = thisNode

	// 如果正在深入命名类型……
	switch t.Kind {
	case types.Alias, types.Struct:
		// 从类型名重启字段路径以便日志可读；沿用调用方路径会得到
		// <type1>.<field1>.<field2> 这类虽正确但冗余的前缀。
		fldPath = field.NewPath(t.Name.String())

		// 记录其校验函数，供后续使用。
		if fn, ok := td.getValidationFunctionName(t); ok {
			thisNode.funcName = fn
		}
	}

	// 先深入发现该类型，再提取类型级校验。
	switch t.Kind {
	case types.Builtin, types.Interface:
		// 无需进一步处理。
	case types.Alias:
		// 继续发现底层类型。
		//
		// 按 Go 语言定义，gengo 的 "Aliases"（其实是类型定义）底层类型来自类型字面量。
		// 例如 `type T1 string` 与 `type T2 T1` 中，T2 的底层类型是 string 而非 T1。
		// 因此：
		//   1) 两者会分别生成代码；若底层是多字段结构体，会得到两份相同函数；
		//   2) 校验 T2 字段时不会调用定义在 T1 上的校验；
		//   3) 若类型定义的 RHS 是带校验标签字段的结构体，这些字段的校验
		//      仍会在新类型的生成函数中被调用。
		if node, err := td.discoverChild(t.Underlying, fldPath); err != nil {
			return nil, err
		} else {
			thisNode.underlying = &childNode{
				childType: t.Underlying,
				node:      node,
			}
		}
	case types.Struct:
		// 递归发现结构体。
		if err := td.discoverStruct(thisNode, fldPath); err != nil {
			return nil, err
		}
	case types.Slice:
		// 发现元素类型。
		if node, err := td.discoverChild(t.Elem, fldPath.Key("vals")); err != nil {
			return nil, err
		} else {
			thisNode.elem = &childNode{
				childType: t.Elem,
				node:      node,
			}
		}
	case types.Map:
		// 发现键类型。
		if node, err := td.discoverChild(t.Key, fldPath.Key("keys")); err != nil {
			return nil, err
		} else {
			thisNode.key = &childNode{
				childType: t.Key,
				node:      node,
			}
		}

		// 发现元素类型。
		if node, err := td.discoverChild(t.Elem, fldPath.Key("vals")); err != nil {
			return nil, err
		} else {
			thisNode.elem = &childNode{
				childType: t.Elem,
				node:      node,
			}
		}
	}

	// 先深入发现类型，再提取类型级校验，这样校验器能拿到完整类型信息。
	// 例如结构体字段校验会先于类型校验被评估；这里只影响“决定生成什么”，
	// 不影响最终代码中的调用顺序。
	switch t.Kind {
	case types.Alias, types.Struct:
		if fldPath.String() != t.String() {
			panic(fmt.Sprintf("path for type != the type name: %s, %s", t.String(), fldPath.String()))
		}
		consts := td.constantsByType[t]
		context := validators.Context{
			Scope:          validators.ScopeType,
			Type:           t,
			Path:           fldPath,
			Member:         nil, // NA when discovering a type
			ParentPath:     nil, // NA when discovering a type
			Constants:      consts,
			ListSelector:   nil, // NA for type scope
			ParentType:     nil, // NA for type scope
			StabilityLevel: "",  // Default to stable unless overridden
		}
		extractedTags, err := td.validator.ExtractTags(context, t.CommentLines)
		if err != nil {
			return nil, fmt.Errorf("%v: %w", fldPath, err)
		}
		if validations, err := td.validator.ExtractValidations(context, extractedTags...); err != nil {
			return nil, fmt.Errorf("%v: %w", fldPath, err)
		} else if validations.Empty() {
			klog.V(6).InfoS("no type-attached validations", "type", t)
		} else {
			if util.NonPointer(util.NativeType(t)).Kind == types.Map && util.NonPointer(util.NativeType(t)).Elem.Kind == types.Slice {
				return nil, fmt.Errorf("field %s: validation for map of slices is not supported", fldPath)
			}
			klog.V(5).InfoS("found type-attached validations", "n", len(validations.Functions), "type", t)
			thisNode.typeValidations.Add(validations)
		}

		// 某些类型定义要等发现完成后才能决定输出：list/map 的别名虽需迭代，
		// 但键值类型若无校验就不该生成迭代代码；非纳入包的类型需要用户显式表态；
		// 递归类型也要等发现完毕才知道是否存在其他校验，以免生成空函数。
		if t.Kind == types.Alias {
			switch util.NonPointer(util.NativeType(t)).Kind {
			case types.Slice:
				// 逐个校验值。
				elemNode := thisNode.resolveElemNode()
				if elemNode == nil {
					if !thisNode.typeValidations.OpaqueValType {
						return nil, fmt.Errorf("%v: value type %v is in a non-included package; "+
							"either add this package to validation-gen's --readonly-pkg flag, "+
							"or add +k8s:eachVal=+k8s:opaqueType to the field to skip validation",
							fldPath, util.NativeType(t).Elem)
					}
				} else if thisNode.typeValidations.OpaqueValType {
					// 类型标记 opaque 时，按非纳入包的类型处理。
				} else {
					// 值类型是命名类型时，为每个元素调用其校验函数。
					if funcName := elemNode.funcName; funcName.Name != "" {
						// 信息此时最完整，先暂存迭代校验，之后按需决定是否输出。
						// Function() 的第一个参数只用于调试。
						v, err := validators.ForEachVal(fldPath, thisNode.valueType,
							validators.Function("iterateListValues", validators.DefaultFlags, funcName).
								WithComment("iterate the list and call the type's validation function"))
						if err != nil {
							return nil, fmt.Errorf("generating list iteration: %w", err)
						} else {
							thisNode.typeValIterations.Add(v)
						}
					}
				}
			case types.Map:
				// 逐个校验键。
				keyNode := thisNode.resolveKeyNode()
				if keyNode == nil {
					if !thisNode.typeValidations.OpaqueKeyType {
						return nil, fmt.Errorf("%v: key type %v is in a non-included package; "+
							"either add this package to validation-gen's --readonly-pkg flag, "+
							"or add +k8s:eachKey=+k8s:opaqueType to the field to skip validation",
							fldPath, util.NativeType(t).Key)
					}
				} else if thisNode.typeValidations.OpaqueKeyType {
					// 类型标记 opaque 时，按非纳入包的类型处理。
				} else {
					// 键类型是命名类型时，为每个键调用其校验函数。
					if funcName := keyNode.funcName; funcName.Name != "" {
						// 信息此时最完整，先暂存迭代校验，之后按需决定是否输出。
						// Function() 的第一个参数只用于调试。
						v, err := validators.ForEachKey(fldPath, thisNode.valueType,
							validators.Function("iterateMapKeys", validators.DefaultFlags, funcName).
								WithComment("iterate the map and call the key type's validation function"))
						if err != nil {
							return nil, fmt.Errorf("generating map key iteration: %w", err)
						} else {
							thisNode.typeKeyIterations.Add(v)
						}
					}
				}
				// 逐个校验值。
				elemNode := thisNode.resolveElemNode()
				if elemNode == nil {
					if !thisNode.typeValidations.OpaqueValType {
						return nil, fmt.Errorf("%v: value type %v is in a non-included package; "+
							"either add this package to validation-gen's --readonly-pkg flag, "+
							"or add +k8s:eachVal=+k8s:opaqueType to the field to skip validation",
							fldPath, util.NativeType(t).Elem)
					}
				} else if thisNode.typeValidations.OpaqueValType {
					// 类型标记 opaque 时，按非纳入包的类型处理。
				} else {
					// 值类型是命名类型时，为每个元素调用其校验函数。
					if funcName := elemNode.funcName; funcName.Name != "" {
						// 信息此时最完整，先暂存迭代校验，之后按需决定是否输出。
						// Function() 的第一个参数只用于调试。
						v, err := validators.ForEachVal(fldPath, thisNode.valueType,
							validators.Function("iterateMapValues", validators.DefaultFlags, funcName).
								WithComment("iterate the map and call the value type's validation function"))
						if err != nil {
							return nil, fmt.Errorf("generating map value iteration: %w", err)
						} else {
							thisNode.typeValIterations.Add(v)
						}
					}
				}
			}
		}
	}

	// 这些校验在标签解析阶段无法完全确定：可能要包裹内层校验，或依赖完整类型图。
	// 解析过程需要迭代，因为一个延迟校验还可能产生新的延迟校验。
	deferred := thisNode.typeValidations.Deferred
	thisNode.typeValidations.Deferred = nil
	depth := 0
	for len(deferred) > 0 {
		depth++
		if depth > 10 {
			return nil, fmt.Errorf("deferred validation recursion depth exceeded "+
				"10 for type %s at path %s", thisNode.valueType.String(), fldPath.String())
		}
		var nextDeferred []validators.DeferredGen
		for _, def := range deferred {
			res, err := def.Callback()
			if err != nil {
				return nil, err
			}
			if len(res.Deferred) > 0 {
				nextDeferred = append(nextDeferred, res.Deferred...)
				res.Deferred = nil
			}
			// 延迟校验可能来自 ParentContext 作用域的字段（如 UnionValidations），
			// 也可能来自类型定义上 ThisContext 作用域的校验（如切片类型上的 eachVal）。
			if def.Scope == validators.ThisContext || def.Scope == validators.ParentContext {
				thisNode.typeValidations.Add(res)
			} else {
				return nil, fmt.Errorf("unexpected scope %v", def.Scope)
			}
		}
		deferred = nextDeferred
	}

	return thisNode, nil
}

// discoverChild 是面向“子节点”的 discoverType：处理作为字段、元素、键或底层类型
// 被抵达的类型，并额外把节点标记为 referenced。见 typeNode.referenced。
func (td *typeDiscoverer) discoverChild(t *types.Type, fldPath *field.Path) (*typeNode, error) {
	node, err := td.discoverType(t, fldPath)
	if node != nil {
		node.referenced = true
	}
	return node, err
}

// verifySupportedType 检查给定类型是否受支持。
// 不支持的形态在发现阶段直接报错，避免后续生成出无法编译或语义错误的代码。
func (td *typeDiscoverer) verifySupportedType(t *types.Type) error {
	switch t.Kind {
	case types.Builtin, types.Struct:
		// 允许。
	case types.Interface:
		// 接口上做不了太多，但 RawExtension 等场景确实会出现。
	case types.Alias:
		if t.Underlying.Kind == types.Pointer {
			return fmt.Errorf("typedefs to pointers are not supported")
		}
	case types.Pointer:
		pointee := util.NativeType(t.Elem)
		switch pointee.Kind {
		case types.Pointer:
			return fmt.Errorf("pointers to pointers are not supported")
		case types.Slice, types.Array:
			return fmt.Errorf("pointers to lists are not supported")
		case types.Map:
			return fmt.Errorf("pointers to maps are not supported")
		}
	case types.Array:
		return fmt.Errorf("fixed-size arrays are not supported")
	case types.Slice:
		elem := util.NativeType(t.Elem)
		switch elem.Kind {
		case types.Slice:
			if util.NativeType(elem.Elem) != types.Byte {
				return fmt.Errorf("lists of lists are not supported")
			}
		case types.Map:
			return fmt.Errorf("lists of maps are not supported")
		}
	case types.Map:
		key := util.NativeType(t.Key)
		if key != types.String {
			return fmt.Errorf("maps with non-string keys are not supported")
		}
		elem := util.NativeType(t.Elem)
		switch elem.Kind {
		case types.Pointer:
			return fmt.Errorf("maps of pointers are not supported")
		case types.Map:
			return fmt.Errorf("maps of maps are not supported")
		}
	default:
		return fmt.Errorf("kind %v is not supported", t.Kind)
	}

	return nil
}

// resolveFieldElemNode 返回 slice/map 字段的元素节点；递归类型尚未构建完成时，
// 字段的 elem 子节点还没接上，此时回退到类型缓存。
func (td *typeDiscoverer) resolveFieldElemNode(child *childNode, elemType *types.Type) *typeNode {
	if child.node != nil && child.node.elem != nil {
		return child.node.elem.node
	}
	return td.typeNodes[elemType]
}

// resolveFieldKeyNode 是 resolveFieldElemNode 的 map 键版本。
func (td *typeDiscoverer) resolveFieldKeyNode(child *childNode, keyType *types.Type) *typeNode {
	if child.node != nil && child.node.key != nil {
		return child.node.key.node
	}
	return td.typeNodes[keyType]
}

// discoverStruct 递归遍历一个 struct 类型。
// 只处理导出字段：未导出字段无法从其他包访问，也无法生成校验调用。
func (td *typeDiscoverer) discoverStruct(thisNode *typeNode, fldPath *field.Path) error {
	var fields []*childNode

	klog.V(5).InfoS("discoverStruct", "type", thisNode.valueType)

	// 逐个发现该结构体的字段。
	for _, memb := range thisNode.valueType.Members {
		name := memb.Name

		// 只处理导出字段。
		if unicode.IsLower([]rune(name)[0]) {
			continue
		}

		// 生成代码需要 JSON 字段名；缺失时直接报错终止。
		jsonName := ""
		if commentTags, ok := tags.LookupJSON(memb); ok {
			jsonName = commentTags.Name
		}

		var childPath *field.Path
		if jsonName != "" {
			childPath = fldPath.Child(jsonName)
		} else {
			childPath = fldPath.Child(name)
		}

		// 发现字段类型。
		klog.V(5).InfoS("field", "name", name, "jsonName", jsonName, "type", memb.Type, "path", childPath)
		childType := memb.Type
		var child *childNode
		if node, err := td.discoverChild(childType, childPath); err != nil {
			return err
		} else {
			child = &childNode{
				name:      name,
				jsonName:  jsonName,
				childType: childType,
				node:      node,
			}
		}

		// 提取字段上附着的校验规则。
		context := validators.Context{
			Scope:          validators.ScopeField,
			Type:           childType,
			Path:           childPath,
			Member:         &memb,
			ParentPath:     fldPath,
			ParentType:     thisNode.valueType,
			ListSelector:   nil, // NA for fields
			Constants:      nil, // NA for fields
			StabilityLevel: "",  // Inherited or default
		}

		tags, err := td.validator.ExtractTags(context, memb.CommentLines)
		if err != nil {
			return fmt.Errorf("field %s: %w", childPath.String(), err)
		}
		if validations, err := td.validator.ExtractValidations(context, tags...); err != nil {
			return fmt.Errorf("field %s: %w", childPath.String(), err)
		} else if validations.Empty() {
			klog.V(6).InfoS("no field-attached validations", "field", childPath)
		} else {
			klog.V(5).InfoS("found field-attached validations", "n", len(validations.Functions), "field", childPath)
			if util.NonPointer(util.NativeType(childType)).Kind == types.Map && util.NonPointer(util.NativeType(childType)).Elem.Kind == types.Slice {
				return fmt.Errorf("field %s: validation for map of slices is not supported", childPath)
			}
			child.fieldValidations.Add(validations)
			// TODO: 目前有些字段校验确实需要生成变量，应重新审视对这类情形直接报错的做法。
		}

		// 处理未纳入生成的类型。
		switch util.NonPointer(childType).Kind {
		case types.Struct, types.Alias:
			if child.node == nil { // a non-included type
				if !child.fieldValidations.OpaqueType {
					return fmt.Errorf("%v: type %v is in a non-included package; "+
						"either add this package to validation-gen's --readonly-pkg flag, "+
						"or add +k8s:opaqueType to the field to skip validation",
						childPath, childType.String())
				}
			} else if child.fieldValidations.OpaqueType {
				// 字段标记 opaque 时，按未纳入包的类型处理。
				child.node = nil
			}
		}

		// 其余附着在字段上的“特殊”校验要等普通字段校验处理完再补：
		// 其中一些依赖其他校验是否已输出，以免生成空函数。
		// 这里需要的是字段级信息，因此放在本函数而非 discoverType() 中。
		switch childType.Kind {
		case types.Slice:
			// 逐个校验列表字段的值。
			if elemNode := td.resolveFieldElemNode(child, childType.Elem); elemNode == nil {
				if !child.fieldValidations.OpaqueValType {
					return fmt.Errorf("%v: value type %v is in a non-included package; "+
						"either add this package to validation-gen's --readonly-pkg flag, "+
						"or add +k8s:eachVal=+k8s:opaqueType to the field to skip validation",
						childPath, childType.Elem.String())
				}
			} else if child.fieldValidations.OpaqueValType {
				// 字段标记 opaque 时，按未纳入包的类型处理。
			} else {
				// 列表值类型是命名类型时，为每个元素调用其校验函数。
				if funcName := elemNode.funcName; funcName.Name != "" {
					// 信息此时最完整，先暂存迭代校验，之后按需决定是否输出。
					// Function() 的第一个参数只用于调试。
					v, err := validators.ForEachVal(childPath, childType,
						validators.Function("iterateListValues", validators.DefaultFlags, funcName).
							WithComment("iterate the list and call the type's validation function"))
					if err != nil {
						return fmt.Errorf("generating list iteration: %w", err)
					} else {
						child.fieldValIterations.Add(v)
					}
				}
			}
		case types.Map:
			// 逐个校验 map 字段的键。
			if keyNode := td.resolveFieldKeyNode(child, childType.Key); keyNode == nil {
				if !child.fieldValidations.OpaqueKeyType {
					return fmt.Errorf("%v: key type %v is in a non-included package; "+
						"either add this package to validation-gen's --readonly-pkg flag, "+
						"or add +k8s:eachKey=+k8s:opaqueType to the field to skip validation",
						childPath, childType.Key.String())
				}
			} else if child.fieldValidations.OpaqueKeyType {
				// 字段标记 opaque 时，按未纳入包的类型处理。
			} else {
				// map 的键类型是命名类型时，为每个键调用其校验函数。
				if funcName := keyNode.funcName; funcName.Name != "" {
					// 信息此时最完整，先暂存迭代校验，之后按需决定是否输出。
					// Function() 的第一个参数只用于调试。
					v, err := validators.ForEachKey(childPath, childType,
						validators.Function("iterateMapKeys", validators.DefaultFlags, funcName).
							WithComment("iterate the map and call the key type's validation function"))
					if err != nil {
						return fmt.Errorf("generating map key iteration: %w", err)
					} else {
						child.fieldKeyIterations.Add(v)
					}
				}
			}
			// 逐个校验 map 字段的值。
			if elemNode := td.resolveFieldElemNode(child, childType.Elem); elemNode == nil {
				if !child.fieldValidations.OpaqueValType {
					return fmt.Errorf("%v: value type %v is in a non-included package; "+
						"either add this package to validation-gen's --readonly-pkg flag, "+
						"or add +k8s:eachVal=+k8s:opaqueType to the field to skip validation",
						childPath, childType.Elem.String())
				}
			} else if child.fieldValidations.OpaqueValType {
				// 字段标记 opaque 时，按未纳入包的类型处理。
			} else {
				// map 的值类型是命名类型时，为每个元素调用其校验函数。
				if funcName := elemNode.funcName; funcName.Name != "" {
					// 信息此时最完整，先暂存迭代校验，之后按需决定是否输出。
					// Function() 的第一个参数只用于调试。
					v, err := validators.ForEachVal(childPath, childType,
						validators.Function("iterateMapValues", validators.DefaultFlags, funcName).
							WithComment("iterate the map and call the value type's validation function"))
					if err != nil {
						return fmt.Errorf("generating map value iteration: %w", err)
					} else {
						child.fieldValIterations.Add(v)
					}
				}
			}
		}

		fields = append(fields, child)
	}

	for _, child := range fields {
		// 字段级延迟校验与类型级类似，迭代解析直到不再产生新的延迟校验。
		deferred := child.fieldValidations.Deferred
		child.fieldValidations.Deferred = nil
		depth := 0
		for len(deferred) > 0 {
			depth++
			if depth > 10 {
				return fmt.Errorf("deferred validation recursion depth exceeded "+
					"10 for field %s of type %s", child.name, thisNode.valueType.String())
			}
			var nextDeferred []validators.DeferredGen
			for _, def := range deferred {
				res, err := def.Callback()
				if err != nil {
					return fmt.Errorf("deferred validation callback failed for field %s of type %s: %w",
						child.name, thisNode.valueType.String(), err)
				}
				if len(res.Deferred) > 0 {
					nextDeferred = append(nextDeferred, res.Deferred...)
					res.Deferred = nil
				}
				// 解析出的校验按作用域归位：
				// - ThisContext 归入字段校验；
				// - ParentContext 归入所属类型校验：字段上的某些校验实际约束整个结构体，
				//   例如跨字段生效的 union 校验。
				switch def.Scope {
				case validators.ThisContext:
					child.fieldValidations.Add(res)
				case validators.ParentContext:
					thisNode.typeValidations.Add(res)
				default:
					return fmt.Errorf("unexpected scope %v", def.Scope)
				}
			}
			deferred = nextDeferred
		}
	}

	thisNode.fields = fields
	return nil
}

// getValidationFunctionName 返回类型的校验函数标识：(输入包, Validate_T)。
// 调用实际指向哪个包取决于输出布局，因此在输出阶段由 resolveFunc 决定，不在此处写死。
//
// TODO: 目前只是“盲调用”：假定目标函数存在却不校验，而且只会向本生成器
// 正在处理的包发起调用。跨包调用需要验证目标，例如按命名约定加指纹，
// 或使用显式注释标签。
func (td *typeDiscoverer) getValidationFunctionName(t *types.Type) (types.Name, bool) {
	if _, ok := td.inputToCanonicalPkg[t.Name.Package]; !ok {
		return types.Name{}, false
	}
	return types.Name{Package: t.Name.Package, Name: "Validate_" + t.Name.Name}, true
}

// mkSymbolArgs 把一组 types.Name 转成模板参数，键为类型名，
// 值为 gengo universe 中的类型对象，供 raw 命名系统输出。
func mkSymbolArgs(c *generator.Context, names []types.Name) generator.Args {
	args := generator.Args{}
	for _, name := range names {
		args[name.Name] = c.Universe.Type(name)
	}
	return args
}

// hasValidations 检查给定 typeNode（含传递到的内部类型）是否存在任何校验。
// 使用 seen 集合防止递归类型导致无限循环。
func (g *genValidations) hasValidations(n *typeNode) bool {
	seen := map[*typeNode]bool{}
	return g.hasValidationsImpl(n, seen)
}

// hasValidationsImpl 实现 hasValidations，且不存在无限递归风险。
// opaqueType 会截断向下检查：其内部校验不会从父级被调用。
func (g *genValidations) hasValidationsImpl(n *typeNode, seen map[*typeNode]bool) bool {
	if n == nil {
		return false
	}

	if seen[n] {
		return false
	}
	seen[n] = true

	if n.typeValidations.HasEmitable() {
		return true
	}

	if n.typeValidations.OpaqueType {
		return false
	}

	if n.underlying != nil {
		if n.typeKeyIterations.HasEmitable() {
			if keyNode := n.resolveKeyNode(); keyNode != nil && g.hasValidationsImpl(keyNode, seen) {
				return true
			}
		}
		if n.typeValIterations.HasEmitable() {
			if elemNode := n.resolveElemNode(); elemNode != nil && g.hasValidationsImpl(elemNode, seen) {
				return true
			}
		}
		if g.hasValidationsImpl(n.underlying.node, seen) {
			return true
		}
	}

	for _, c := range n.fields {
		if c.fieldValidations.HasEmitable() {
			return true
		}
		if c.fieldKeyIterations.HasEmitable() {
			if keyNode := c.node.resolveKeyNode(); keyNode != nil && g.hasValidationsImpl(keyNode, seen) {
				return true
			}
		}
		if c.fieldValIterations.HasEmitable() {
			if elemNode := c.node.resolveElemNode(); elemNode != nil && g.hasValidationsImpl(elemNode, seen) {
				return true
			}
		}
		if g.hasValidationsImpl(c.node, seen) {
			return true
		}
	}

	return false
}

// emitRegisterFunction 输出校验函数的类型注册逻辑。
// 只为真正含校验的 root 类型注册，避免无意义的 switch 分支。
func (g *genValidations) emitRegisterFunction(c *generator.Context, schemeRegistry types.Name, sw *generator.SnippetWriter) {
	var targetTypes []*types.Type
	for _, rootType := range g.rootTypes {
		if g.hasValidations(g.discovered.typeNodes[rootType]) {
			targetTypes = append(targetTypes, rootType)
		}
	}
	if len(targetTypes) == 0 {
		return
	}

	scheme := c.Universe.Type(schemeRegistry)
	schemePtr := &types.Type{
		Kind: types.Pointer,
		Elem: scheme,
	}

	sw.Do("func init() { localSchemeBuilder.Register(RegisterValidations)}\n\n", nil)

	sw.Do("// RegisterValidations adds validation functions to the given scheme.\n", nil)
	sw.Do("// Public to allow building arbitrary schemes.\n", nil)
	sw.Do("func RegisterValidations(scheme $.|raw$) error {\n", schemePtr)
	for _, rootType := range targetTypes {
		node := g.discovered.typeNodes[rootType]
		if node == nil {
			panic(fmt.Sprintf("found nil node for root-type %v", rootType))
		}

		targs := generator.Args{
			"rootType":  rootType,
			"typePfx":   "",
			"field":     mkSymbolArgs(c, fieldPkgSymbols),
			"fmt":       mkSymbolArgs(c, fmtPkgSymbols),
			"operation": mkSymbolArgs(c, operationPkgSymbols),
			"safe":      mkSymbolArgs(c, safePkgSymbols),
			"context":   mkSymbolArgs(c, contextPkgSymbols),
		}
		if !util.IsNilableType(rootType) {
			targs["typePfx"] = "*"
		}

		// 这里用带类型的 nil 指针而非真实实例：只需要类型信息，不需要值。
		sw.Do("// type $.rootType|name$\n", targs)
		sw.Do("scheme.AddValidationFunc(\n", targs)
		sw.Do("    ($.typePfx$$.rootType|raw$)(nil),\n", targs)
		sw.Do("    func(ctx $.context.Context$, op $.operation.Operation|raw$, obj, oldObj interface{}) $.field.ErrorList|raw$ {\n", targs)

		sw.Do("switch op.Request.SubresourcePath() {\n", nil)
		sw.Do("case ", nil)
		for i, s := range g.toResourceList(rootType) {
			if i > 0 {
				sw.Do(", ", nil)
			}
			sw.Do("$.$", s)
		}
		sw.Do(":\n", nil)
		sw.Do("    return $.rootType|objectvalidationfn$(\n", targs)
		sw.Do("               ctx, ", targs)
		sw.Do("               op, ", targs)
		sw.Do("               nil /* fldPath */,\n", targs)
		sw.Do("               obj.($.typePfx$$.rootType|raw$),\n", targs)
		sw.Do("               $.safe.Cast|raw$[$.typePfx$$.rootType|raw$](oldObj))\n", targs)
		sw.Do("  }\n", targs)
		sw.Do("  return $.field.ErrorList|raw${\n", targs)
		sw.Do("      $.field.InternalError|raw$(", targs)
		sw.Do("          nil, ", targs)
		sw.Do("          $.fmt.Errorf|raw$(\"no validation found for %T, subresource: %v\", obj, op.Request.SubresourcePath())),\n", targs)
		sw.Do("  }\n", targs)
		sw.Do("})\n", targs)
	}
	sw.Do("return nil\n", nil)
	sw.Do("}\n\n", nil)
}

// toResourceList 返回某个 kind 所支持的资源列表。
// 结果会被排序并加引号，作为 switch 的 case 值；默认资源路径为 "/"。
func (g *genValidations) toResourceList(rootType *types.Type) []string {
	supportedSubresources := supportedSubresourceTags(rootType)

	if subresource, isSubresource := isSubresourceTag(rootType); isSubresource {
		supportedSubresources.Insert(subresource)
	} else {
		supportedSubresources.Insert("/")
	}
	supported := supportedSubresources.UnsortedList()
	slices.Sort(supported)
	for i, subresource := range supported {
		supported[i] = strconv.Quote(subresource)
	}
	return supported
}

// emitValidationFunction 为指定类型输出校验函数。
// 无任何可生成校验的类型不会产生函数，调用方通过 hasValidations 判断。
func (g *genValidations) emitValidationFunction(c *generator.Context, t *types.Type, sw *generator.SnippetWriter) {
	if !g.hasValidations(g.discovered.typeNodes[t]) {
		return
	}

	targs := generator.Args{
		"inType":     t,
		"field":      mkSymbolArgs(c, fieldPkgSymbols),
		"operation":  mkSymbolArgs(c, operationPkgSymbols),
		"context":    mkSymbolArgs(c, contextPkgSymbols),
		"objTypePfx": "*",
	}
	if util.IsNilableType(t) {
		targs["objTypePfx"] = ""
	}

	node := g.discovered.typeNodes[t]
	if node == nil {
		panic(fmt.Sprintf("found nil node for root-type %v", t))
	}
	sw.Do("// $.inType|objectvalidationfn$ validates an instance of $.inType|name$ according\n", targs)
	sw.Do("// to declarative validation rules in the API schema.\n", targs)
	sw.Do("func $.inType|objectvalidationfn$(\n", targs)
	sw.Do("    ctx $.context.Context|raw$, ", targs)
	sw.Do("    op $.operation.Operation|raw$, ", targs)
	sw.Do("    fldPath *$.field.Path|raw$,\n", targs)
	sw.Do("    obj, oldObj $.objTypePfx$$.inType|raw$) ", targs)
	sw.Do("(errs $.field.ErrorList|raw$) {\n\n", targs)
	fakeChild := &childNode{
		node:      node,
		childType: t,
	}
	g.emitValidationForChild(c, fakeChild, sw)
	sw.Do("return errs\n", nil)
	sw.Do("}\n\n", nil)
}

// emitValidationForChild 为 childNode 输出代码：先调用类型级校验，再深入类型
// （如结构体字段）。
//
// 生成代码假定待校验值总是一对可空变量 "obj"/"oldObj"，字段路径为 "fldPath"。
// 本函数假定 thisChild.node 非 nil。
func (g *genValidations) emitValidationForChild(c *generator.Context, thisChild *childNode, sw *generator.SnippetWriter) {
	thisNode := thisChild.node
	inType := thisNode.valueType

	targs := generator.Args{
		"inType": inType,
		"field":  mkSymbolArgs(c, fieldPkgSymbols),
		"safe":   mkSymbolArgs(c, safePkgSymbols),
	}

	didSome := false // for prettier output later

	// 输出类型级校验的调用代码。
	if validations := thisNode.typeValidations; !validations.Empty() {
		switch thisNode.valueType.Kind {
		case types.Struct, types.Alias: // OK
		default:
			panic(fmt.Sprintf("unexpected type-validations on type %v, kind %s", thisNode.valueType, thisNode.valueType.Kind))
		}
		emitComments(validations.Comments, sw)
		g.emitCallsToValidators(c, validations.Functions, sw)
		sw.Do("\n", nil)
		didSome = true
	}

	if validations := thisNode.typeKeyIterations; !validations.Empty() {
		keyNode := thisNode.resolveKeyNode()
		if keyNode != nil && g.hasValidations(keyNode) {
			emitComments(validations.Comments, sw)
			g.emitCallsToValidators(c, validations.Functions, sw)
			sw.Do("\n", nil)
			didSome = true
		}
	}

	if validations := thisNode.typeValIterations; !validations.Empty() {
		elemNode := thisNode.resolveElemNode()
		if elemNode != nil && g.hasValidations(elemNode) {
			emitComments(validations.Comments, sw)
			g.emitCallsToValidators(c, validations.Functions, sw)
			sw.Do("\n", nil)
			didSome = true
		}
	}

	if thisNode.typeValidations.OpaqueType {
		return
	}

	// 深入类型本身。
	switch inType.Kind {
	case types.Builtin:
		// 无需进一步处理。
	case types.Slice:
		// 无需进一步处理。
	case types.Map:
		// 无需进一步处理。
	case types.Alias:
		g.emitValidationForChild(c, thisNode.underlying, sw)
	case types.Struct:
		for _, fld := range thisNode.fields {
			if len(fld.name) == 0 {
				panic(fmt.Sprintf("missing field name in type %s (field-type %s)", thisNode.valueType, fld.childType))
			}
			// 只有确实要输出代码时才检查 JSON 名是否缺失。

			// 先写入缓冲区，避免输出空函数。
			buf := bytes.NewBuffer(nil)
			bufsw := sw.Dup(buf)

			// 关于 ratcheting 检查：
			//
			// 只在结构体字段上有校验可调用时才输出 ratcheting 检查。
			// 不在类型专用校验函数内输出：那会导致重复检查，几乎没有价值（详见下文）。
			//
			// 因此调用某个类型的校验函数时，默认调用方已经做过 ratcheting 检查
			// （生成代码都满足这一点，根类型除外）。结构体类型的函数本身仍会逐字段检查。
			//
			// 唯一例外是带类型级校验的类型被“未经预检”地调用：
			//   1. 外部调用方直接调用该类型的校验函数；
			//   2. 包的 register 函数调用根类型的校验函数。
			//
			// TODO: 暂不处理。若将来出现“根类型带类型级校验且该校验需要 ratcheting”，
			// 可考虑：
			//   1. 在 register 函数中做检查；
			//   2. 在类型校验函数内做检查（仅限带类型级校验的根类型）；
			//   3. 同时提供 "safe"（整体检查）与 "fast"（假定已检查）两种形式，
			//      生成代码调 "fast"，外部代码调 "safe"；
			//   4. 改为深度优先校验，每个函数额外返回“是否发生变化”，由调用方决定是否
			//      执行更高层校验（如结构体字段有变化才执行类型级校验）。
			//
			// 同理，指针列表不在类型专用函数内输出 nil 检查；重复 ratcheting 问题解决后，
			// nil 检查也应一并解决。

			if nt := util.NativeType(fld.childType); nt.Kind == types.Slice && nt.Elem.Kind == types.Pointer {
				hasFieldValidations := len(fld.fieldValidations.Functions) > 0
				hasPropagation := fld.node != nil && g.hasValidations(fld.node)
				hasKeyIterations := len(fld.fieldKeyIterations.Functions) > 0 && hasPropagation
				hasValIterations := len(fld.fieldValIterations.Functions) > 0 && hasPropagation

				if hasFieldValidations || hasPropagation || hasKeyIterations || hasValIterations {
					// nil 检查必须前置：后续校验可能假定切片中没有 nil 元素。
					nilCheck := validators.PtrSliceNoNils(nt.Elem.Elem.Name)
					fld.fieldValidations.Functions = append([]validators.FunctionGen{nilCheck}, fld.fieldValidations.Functions...)
				}
			}

			validations := fld.fieldValidations
			fldRatchetingChecked := false
			if !validations.Empty() {
				emitComments(validations.Comments, bufsw)
				if len(validations.Functions) > 0 {
					emitRatchetingCheck(c, fld.childType, bufsw)
					fldRatchetingChecked = true
					bufsw.Do("// call field-attached validations\n", nil)
					g.emitCallsToValidators(c, validations.Functions, bufsw)
				}
			}

			// 节点为 nil 说明类型来自未处理的包，对我们而言等价于 opaque。
			if fld.node != nil {
				// 取真实类型。
				switch fld.node.valueType.Kind {
				case types.Alias, types.Struct:
					// 字段是另一类型时可能需要调用其校验函数；若它（传递地）没有校验，则无需处理。
					if g.hasValidations(fld.node) {
						if !fldRatchetingChecked {
							emitRatchetingCheck(c, fld.childType, bufsw)
							fldRatchetingChecked = true
						}
						g.emitCallToOtherTypeFunc(c, fld.node, bufsw)
					}
				}

				emitIterations := func(iterations validators.Validations, node *typeNode) {
					if iterations.Empty() {
						return
					}
					if node != nil && g.hasValidations(node) {
						emitComments(iterations.Comments, bufsw)
						if len(iterations.Functions) > 0 {
							if !fldRatchetingChecked {
								emitRatchetingCheck(c, fld.childType, bufsw)
								fldRatchetingChecked = true
							}
							g.emitCallsToValidators(c, iterations.Functions, bufsw)
						}
					}
				}

				emitIterations(fld.fieldKeyIterations, fld.node.resolveKeyNode())
				emitIterations(fld.fieldValIterations, fld.node.resolveElemNode())

				if fld.node.valueType.Kind == types.Slice || fld.node.valueType.Kind == types.Map {
					// 深入该字段。
					g.emitValidationForChild(c, fld, bufsw)
				}
			}

			if buf.Len() > 0 {
				leafType, typePfx, exprPfx := getLeafTypeAndPrefixes(fld.childType)
				targs := targs.WithArgs(generator.Args{
					"fieldName":    fld.name,
					"fieldJSON":    fld.jsonName,
					"fieldType":    leafType,
					"fieldTypePfx": typePfx,
					"fieldExprPfx": exprPfx,
				})

				if didSome {
					sw.Do("\n", nil)
				}
				sw.Do("{ // field $.inType|raw$.$.fieldName$\n", targs)
				sw.Do("    fn := func(\n", targs)
				sw.Do("        fldPath *$.field.Path|raw$,\n", targs)
				sw.Do("        obj, oldObj $.fieldTypePfx$$.fieldType|raw$,\n", targs)
				sw.Do("        oldValueCorrelated bool) (errs $.field.ErrorList|raw$) {\n", targs)
				if err := sw.Merge(buf, bufsw); err != nil {
					panic(fmt.Sprintf("failed to merge buffer: %v", err))
				}
				sw.Do("            return\n", targs)
				sw.Do("    }\n", targs)
				// 对 map 等类型，旧对象中找不到可关联值时 safe.Field 返回 nil，
				// 这与“字段存在且为 nil”无法区分。ratcheting 需要区分二者：
				// 例如必填字段从 map 中删除时，旧值为 nil，新值也因不存在而为 nil，
				// ratcheting 会放行，但校验上这是必填字段缺失，必须报错。
				//
				// 解决办法是给校验函数多传一个布尔值，表示旧值是否成功关联。
				// 未关联意味着旧对象中不存在该字段，此时不应套用 ratcheting 逻辑；
				// `oldObj != nil` 就携带了这一位信息。
				//
				// 该标志目前不向下传递到更深层校验，但生成器只看一层，暂时够用。
				sw.Do("    oldVal := $.safe.Field|raw$(oldObj,\n", targs)
				sw.Do("        func(oldObj *$.inType|raw$) $.fieldTypePfx$$.fieldType|raw$ {\n", targs)
				sw.Do("            return $.fieldExprPfx$oldObj.$.fieldName$\n", targs)
				sw.Do("        })\n", targs)
				sw.Do("    errs = append(errs, fn(", targs)
				if len(fld.jsonName) > 0 {
					sw.Do("fldPath.Child(\"$.fieldJSON$\"), ", targs)
				} else {
					// 根类型中的内嵌字段可能没有 fldPath，这里必须给字段路径兜个底。
					sw.Do("$.safe.Value|raw$(fldPath, func() *$.field.Path|raw$ { return fldPath.Child(\"$.fieldType|raw$\") }), ", targs)
				}
				sw.Do("    $.fieldExprPfx$obj.$.fieldName$, oldVal, oldObj != nil)...)\n", targs)
				sw.Do("}\n", targs)
				sw.Do("\n", nil)
			} else {
				targs := targs.WithArgs(generator.Args{
					"fieldName": fld.name,
				})
				sw.Do("// field $.inType|raw$.$.fieldName$ has no validation\n", targs)
			}
			didSome = true
		}
	default:
		panic(fmt.Sprintf("unhandled type: %v (kind %s)", inType, inType.Kind))
	}
}

// emitCallToOtherTypeFunc 针对父级上下文中的某个字段，生成调用该节点已生成的校验函数。
//
// 生成的代码假定：被校验的值总是一对可空变量 "obj" 和 "oldObj"，
// 指向该值的字段路径名为 "fldPath"。
// 调用目标包由 resolveFunc 在输出阶段解析，支持跨包/本地副本。
func (g *genValidations) emitCallToOtherTypeFunc(c *generator.Context, node *typeNode, sw *generator.SnippetWriter) {
	targs := generator.Args{
		"funcName": c.Universe.Type(g.resolveFunc(node.funcName)),
	}
	sw.Do("// call the type's validation function\n", nil)
	sw.Do("errs = append(errs, $.funcName|raw$(ctx, op, fldPath, obj, oldObj)...)\n", targs)
}

// emitRatchetingCheck 输出默认 ratcheting 所用的等价性检查。
// 直接可比较类型用指针值比较，其余用 semantic.DeepEqual；两者都必须先
// 处理 nil，避免解引用 panic。
func emitRatchetingCheck(c *generator.Context, t *types.Type, sw *generator.SnippetWriter) {
	// 输出默认 ratcheting 所需的等价性检查。
	targs := generator.Args{
		"operation": mkSymbolArgs(c, operationPkgSymbols),
	}
	sw.Do("// don't revalidate unchanged data\n", nil)
	sw.Do("if oldValueCorrelated && op.Type == $.operation.Update|raw$ {\n", targs)
	// 内建类型在非 nil 时可用更简单的相等判断。
	if util.IsDirectComparable(util.NonPointer(util.NativeType(t))) {
		// 这里只会拿到指针，其他可空类型都不可比较。
		//
		// 条件看似繁琐，但每一项都不可少：
		// - obj == oldObj：新旧都为 nil 的指针；
		// - obj != nil：可选字段被更新为 nil；
		// - oldObj != nil：可选字段从 nil 更新而来；
		// - *obj == *oldObj：比较实际值。
		sw.Do("  if obj == oldObj || (obj != nil && oldObj != nil && *obj == *oldObj) {\n", targs)
	} else {
		targs["equality"] = mkSymbolArgs(c, equalityPkgSymbols)
		sw.Do("  if $.equality.Semantic|raw$.DeepEqual(obj, oldObj) {\n", targs)
	}
	sw.Do("    return nil\n", nil)
	sw.Do("  }\n", nil)
	sw.Do("}\n", nil)
}

// emitCallsToValidators 为单个字段或类型输出一串校验函数调用；
// validations 是要调用的函数及其参数列表。
//
// 调用注册的校验函数时一律传可空类型：字段是 string 就传 *string，
// 字段本身是 *string 也传 *string。这样更新语义更一致，
// 但校验器若要解引用，就必须自行做 nil 检查。
//
// 生成的代码假定：被校验的值总是一对可空变量 "obj" 和 "oldObj"，
// 指向该值的字段路径名为 "fldPath"。
//
// cohort 相同的调用被包进一个闭包：cohort 内可以共享提前返回状态，
// 但不会影响其他 cohort 的执行。
func (g *genValidations) emitCallsToValidators(c *generator.Context, validations []validators.FunctionGen, sw *generator.SnippetWriter) {
	// 按 cohort 分组并排序输入。
	cohorts := sortIntoCohorts(validations)

	for _, validations := range cohorts {
		cohortName := validations[0].Cohort
		if cohortName != "" {
			sw.Do("func() { // cohort = \"$.$\"\n", cohortName)
		}

		hasShortCircuits := false
		lastShortCircuitIdx := -1
		for i, v := range validations {
			if v.Flags.IsSet(validators.ShortCircuit) {
				hasShortCircuits = true
				lastShortCircuitIdx = i
			}
		}

		if hasShortCircuits {
			sw.Do("earlyReturn := false\n", nil)
		}

		for i, v := range validations {
			isShortCircuit := v.Flags.IsSet(validators.ShortCircuit)
			isNonError := v.Flags.IsSet(validators.NonError)

			targs := generator.Args{
				"funcName": c.Universe.Type(g.resolveFunc(v.Function)),
				"field":    mkSymbolArgs(c, fieldPkgSymbols),
				"fmt":      mkSymbolArgs(c, fmtPkgSymbols),
			}

			emitCall := func() {
				sw.Do("$.funcName|raw$", targs)
				if typeArgs := v.TypeArgs; len(typeArgs) > 0 {
					sw.Do("[", nil)
					for i, typeArg := range typeArgs {
						sw.Do("$.|raw$", c.Universe.Type(typeArg))
						if i < len(typeArgs)-1 {
							sw.Do(",", nil)
						}
					}
					sw.Do("]", nil)
				}
				sw.Do("(ctx, op, fldPath, obj, oldObj", targs)
				for _, arg := range v.Args {
					sw.Do(", ", nil)
					g.toGolangSourceDataLiteral(sw, c, arg, flNewlineOK)
				}
				sw.Do(")", targs)
				switch v.StabilityLevel {
				case validators.ValidationStabilityLevelAlpha:
					sw.Do(".MarkAlpha()", nil)
				case validators.ValidationStabilityLevelBeta:
					sw.Do(".MarkBeta()", nil)
				}
				if isShortCircuit {
					sw.Do(".MarkShortCircuit()", nil)
				}
			}

			// 条件校验要把函数调用包在条件判断里。
			if !v.Conditions.Empty() {
				emitBaseFunction := emitCall
				emitCall = func() {
					// emitOptionLookup 输出 "<var>, defined := op.HasOption(<option>)"，
					// 选项未定义时按内部错误处理。
					emitOptionLookup := func(varName, option string) {
						la := generator.Args{"field": targs["field"], "fmt": targs["fmt"], "opt": strconv.Quote(option)}
						sw.Do("  "+varName+", defined := op.HasOption($.opt$)\n", la)
						sw.Do("  if !defined {\n", nil)
						sw.Do("    return $.field.ErrorList|raw${$.field.InternalError|raw$(fldPath, $.fmt.Errorf|raw$(\"undefined validation option %q\", $.opt$))}\n", la)
						sw.Do("  }\n", nil)
					}
					sw.Do("func() $.field.ErrorList|raw$ {\n", targs)
					if len(v.Conditions.OptionEnabled) > 0 {
						emitOptionLookup("optionEnabled", v.Conditions.OptionEnabled)
					}
					if len(v.Conditions.OptionDisabled) > 0 {
						emitOptionLookup("optionDisabled", v.Conditions.OptionDisabled)
					}
					sw.Do("  if ", nil)
					firstCondition := true
					if len(v.Conditions.OptionEnabled) > 0 {
						sw.Do("optionEnabled", nil)
						firstCondition = false
					}
					if len(v.Conditions.OptionDisabled) > 0 {
						if !firstCondition {
							sw.Do(" && ", nil)
						}
						sw.Do("!optionDisabled", nil)
					}
					sw.Do(" {\n", nil)
					sw.Do("    return ", nil)
					emitBaseFunction()
					sw.Do("\n", nil)
					sw.Do("  }\n", nil)
					sw.Do("  return nil // skip validation\n", nil)
					sw.Do("}()", nil)
				}
			}

			for _, comment := range v.Comments {
				sw.Do("// $.$\n", comment)
			}
			if isShortCircuit {
				sw.Do("if e := ", nil)
				emitCall()
				sw.Do("; len(e) != 0 {\n", nil)
				if !isNonError {
					sw.Do("  errs = append(errs, e...)\n", nil)
				}
				sw.Do("  earlyReturn = true\n", nil)
				sw.Do("}\n", nil)

				// 只在最后一个短路函数之后检查提前返回。
				if hasShortCircuits && i == lastShortCircuitIdx {
					sw.Do("if earlyReturn {\n", nil)
					sw.Do("  return // do not proceed\n", nil)
					sw.Do("}\n", nil)
				}
			} else {
				if isNonError {
					emitCall()
				} else {
					sw.Do("if e := ", nil)
					emitCall()
					sw.Do("; len(e) != 0 {\n", nil)
					sw.Do("  errs = append(errs, e...)\n", nil)
					sw.Do("}\n", nil)
				}
			}
		}
		if cohortName != "" {
			sw.Do("}()\n", nil)
		}
	}
}

// sortIntoCohorts 把输入按 cohort 分组。同一 cohort 内，会短路（提前返回）的函数
// 调用排在其他调用之前。若存在默认 cohort（名为 ""），它总是排在第一个；
// 其余 cohort 按其在输入中的定义顺序返回。
//
// 短路的校验必须排在同 cohort 前面：它们的失败会决定是否继续执行
// 后续校验；cohort 之间不排序以保持声明顺序。
func sortIntoCohorts(in []validators.FunctionGen) [][]validators.FunctionGen {
	defaultCohort := make([]validators.FunctionGen, 0, len(in))
	namedCohorts := map[string][]validators.FunctionGen{}
	idx := make([]string, 0, len(in))
	for _, fg := range in {
		key := fg.Cohort
		if key == "" {
			defaultCohort = append(defaultCohort, fg)
		} else {
			if !slices.Contains(idx, key) {
				idx = append(idx, key)
			}
			namedCohorts[key] = append(namedCohorts[key], fg)
		}
	}
	if len(defaultCohort) > 0 {
		idx = append([]string{""}, idx...)
	}
	// 故意不按名字排序 cohort，以保留定义顺序。

	result := make([][]validators.FunctionGen, 0, len(in))
	for _, key := range idx {
		var cohort []validators.FunctionGen
		if key == "" {
			cohort = defaultCohort
		} else {
			cohort = namedCohorts[key]
		}

		sooner := make([]validators.FunctionGen, 0, len(cohort))
		later := make([]validators.FunctionGen, 0, len(cohort))

		for _, fg := range cohort {
			isShortCircuit := (fg.Flags.IsSet(validators.ShortCircuit))

			if isShortCircuit {
				sooner = append(sooner, fg)
			} else {
				later = append(later, fg)
			}
		}
		sorted := sooner
		sorted = append(sorted, later...)
		result = append(result, sorted)
	}
	return result
}

// emitComments 将校验来源注释原样写入生成代码，便于追溯。
func emitComments(comments []string, sw *generator.SnippetWriter) {
	for _, comment := range comments {
		sw.Do("// ", nil)
		sw.Do(comment, nil)
		sw.Do("\n", nil)
	}
}

// emitValidationVariables 输出一组变量声明。每个变量声明都有一个私有的（不导出的）变量名，
// 以及一个预期用于初始化该变量取值的函数调用。
// 变量按名字排序输出，保证生成结果稳定；私有命名避免污染包的导出符号。
func (g *genValidations) emitValidationVariables(c *generator.Context, t *types.Type, sw *generator.SnippetWriter) {
	tn := g.discovered.typeNodes[t]

	emit := func(variables []validators.VariableGen) {
		slices.SortFunc(variables, func(a, b validators.VariableGen) int {
			return cmp.Compare(a.Variable.Name, b.Variable.Name)
		})
		for _, variable := range variables {
			targs := generator.Args{
				"varName": c.Universe.Type(types.Name(variable.Variable)),
			}

			sw.Do("var $.varName|private$ = ", targs)
			g.toGolangSourceDataLiteral(sw, c, variable.Initializer, flNewlineOK)
			sw.Do("\n", nil)
		}
	}
	// TODO: 多个校验器生成同名变量时可能冲突，需处理。
	emit(tn.typeValidations.Variables)
	for _, field := range tn.fields {
		if len(field.fieldValidations.Variables) != 0 {
			emit(field.fieldValidations.Variables)
		}
	}
}

const (
	// flNewlineOK 表示渲染值时允许换行：复杂字面量换行更易读，
	// 简单字面量则不应换行。
	flNewlineOK uint64 = 1 << iota
)

// toGolangSourceDataLiteral 把值渲染为生成代码中的 Go 字面量。
// 只接受有限类型（基础类型、types.Type、validators.Identifier 等），
// 即校验函数常用的实参类型；flags 控制渲染细节。
func (g *genValidations) toGolangSourceDataLiteral(sw *generator.SnippetWriter, c *generator.Context, value any, flags uint64) {
	// 出于安全考虑，严格限制写入源码的值类型，并确保字符串带引号。

	switch v := value.(type) {
	case uint, uint8, uint16, uint32, uint64, int, int8, int16, int32, int64, float32, float64, bool:
		sw.Do(fmt.Sprintf("%v", value), nil)
	case string:
		// 即使传入字符串已带引号，也自行重新处理，以防万一。
		str := value.(string)
		if s, err := strconv.Unquote(str); err == nil {
			str = s
		}
		sw.Do(fmt.Sprintf("%q", str), nil)
	case *types.Type:
		sw.Do("$.|raw$", v)
	case types.Member:
		sw.Do("obj."+v.Name, nil)
	case *types.Member:
		sw.Do("obj."+v.Name, nil)
	case validators.Identifier:
		sw.Do("$.|raw$", c.Universe.Type(types.Name(v)))
	case *validators.Identifier:
		sw.Do("$.|raw$", c.Universe.Type(types.Name(*v)))
	case validators.PrivateVar:
		sw.Do("$.|private$", c.Universe.Type(types.Name(v)))
	case *validators.PrivateVar:
		sw.Do("$.|private$", c.Universe.Type(types.Name(*v)))
	case validators.WrapperFunction:
		if extraArgs := v.Function.Args; len(extraArgs) == 0 {
			// 被包装的函数没有额外参数时，直接引用即可。
			targs := generator.Args{
				"funcName": c.Universe.Type(g.resolveFunc(v.Function.Function)),
			}
			for _, comment := range v.Function.Comments {
				sw.Do("// $.$\n", comment)
			}
			sw.Do("$.funcName|raw$", targs)
		} else {
			// 被包装函数有额外参数时，需要包一层“标准签名”的校验函数。
			targs := generator.Args{
				"field":      mkSymbolArgs(c, fieldPkgSymbols),
				"operation":  mkSymbolArgs(c, operationPkgSymbols),
				"context":    mkSymbolArgs(c, contextPkgSymbols),
				"objType":    v.ObjType,
				"objTypePfx": "*",
			}
			if util.IsNilableType(v.ObjType) {
				targs["objTypePfx"] = ""
			}

			if flags&flNewlineOK != 0 {
				sw.Do("\n", nil)
			}
			sw.Do("func(", targs)
			sw.Do("    ctx $.context.Context|raw$, ", targs)
			sw.Do("    op $.operation.Operation|raw$, ", targs)
			sw.Do("    fldPath *$.field.Path|raw$, ", targs)
			sw.Do("    obj, oldObj $.objTypePfx$$.objType|raw$ ", targs)
			sw.Do(")    $.field.ErrorList|raw$ {\n", targs)
			sw.Do("return ", nil)
			g.emitFunctionCall(sw, c, v.Function, "ctx", "op", "fldPath", "obj", "oldObj")
			sw.Do("\n}", targs)
		}
	case validators.MultiWrapperFunction:
		// MultiWrapperFunction 生成闭包，按顺序执行多个校验函数。
		targs := generator.Args{
			"field":      mkSymbolArgs(c, fieldPkgSymbols),
			"operation":  mkSymbolArgs(c, operationPkgSymbols),
			"context":    mkSymbolArgs(c, contextPkgSymbols),
			"objType":    v.ObjType,
			"objTypePfx": "*",
		}
		// 标准校验函数接收可空形式，因此这里传指针。
		if util.IsNilableType(v.ObjType) {
			targs["objTypePfx"] = ""
		}

		if flags&flNewlineOK != 0 {
			sw.Do("\n", nil)
		}
		sw.Do("func(", targs)
		sw.Do("    ctx $.context.Context|raw$, ", targs)
		sw.Do("    op $.operation.Operation|raw$, ", targs)
		sw.Do("    fldPath *$.field.Path|raw$, ", targs)
		sw.Do("    obj, oldObj $.objTypePfx$$.objType|raw$ ", targs)
		sw.Do(")    $.field.ErrorList|raw$ {\n", targs)
		sw.Do("errs := $.field.ErrorList|raw${}\n", targs)

		// 判断被包装的函数中是否存在短路函数。
		hasShortCircuits := false
		lastShortCircuitIdx := -1
		for i, fg := range v.Functions {
			if fg.Flags.IsSet(validators.ShortCircuit) {
				hasShortCircuits = true
				lastShortCircuitIdx = i
			}
		}
		if hasShortCircuits {
			sw.Do("earlyReturn := false\n", nil)
		}

		for i, fg := range v.Functions {
			isNonError := fg.Flags.IsSet(validators.NonError)

			if fg.Flags.IsSet(validators.ShortCircuit) {
				// 短路函数一旦返回错误就停止后续执行。
				sw.Do("if e := ", nil)
				g.emitFunctionCall(sw, c, fg, "ctx", "op", "fldPath", "obj", "oldObj")
				sw.Do("; len(e) != 0 {\n", nil)
				if !isNonError {
					sw.Do("  errs = append(errs, e...)\n", nil)
				}
				sw.Do("  earlyReturn = true\n", nil)
				sw.Do("}\n", nil)

				// 短路失败时立即返回。
				if i == lastShortCircuitIdx {
					sw.Do("if earlyReturn {\n", nil)
					sw.Do("  return errs\n", nil)
					sw.Do("}\n", nil)
				}
			} else {
				// 普通函数把错误追加到列表。
				if isNonError {
					g.emitFunctionCall(sw, c, fg, "ctx", "op", "fldPath", "obj", "oldObj")
				} else {
					sw.Do("errs = append(errs, ", nil)
					g.emitFunctionCall(sw, c, fg, "ctx", "op", "fldPath", "obj", "oldObj")
					sw.Do("...)\n", nil)
				}
			}
		}
		sw.Do("return errs\n}", nil)
	case validators.Literal:
		sw.Do("$.$", v)
	case validators.FunctionGen:
		for _, comment := range v.Comments {
			sw.Do("// $.$\\n", comment)
		}
		g.emitFunctionCall(sw, c, v)
	case validators.FunctionLiteral:
		if flags&flNewlineOK != 0 {
			sw.Do("\n", nil)
		}
		sw.Do("func(", nil)
		for i, param := range v.Parameters {
			if i > 0 {
				sw.Do(", ", nil)
			}
			targs := generator.Args{
				"name": param.Name,
				"type": param.Type,
			}
			sw.Do("$.name$ $.type|raw$", targs)
		}
		sw.Do(")", nil)
		if len(v.Results) > 1 {
			sw.Do(" (", nil)
		}
		for i, ret := range v.Results {
			if i > 0 {
				sw.Do(", ", nil)
			}
			targs := generator.Args{
				"name": ret.Name,
				"type": ret.Type,
			}
			sw.Do("$.name$ $.type|raw$", targs)
		}
		if len(v.Results) > 1 {
			sw.Do(")", nil)
		}
		sw.Do(" { $.$ }", v.Body)
	case validators.StructLiteral:
		targs := generator.Args{
			"type": c.Universe.Type(v.Type),
		}
		if flags&flNewlineOK != 0 {
			sw.Do("\n", nil)
		}
		sw.Do("$.type|raw$", targs)
		if len(v.TypeArgs) > 0 {
			sw.Do("[", nil)
			for i, typeArg := range v.TypeArgs {
				if i > 0 {
					sw.Do(", ", nil)
				}
				sw.Do("$.|raw$", typeArg)
			}
			sw.Do("]", nil)
		}
		sw.Do("{", nil)
		if flags&flNewlineOK != 0 {
			sw.Do("\n", nil)
		}
		for _, f := range v.Fields {
			sw.Do(f.Name, nil)
			sw.Do(": ", nil)
			g.toGolangSourceDataLiteral(sw, c, f.Value, flags)
			sw.Do(",", nil)
			if flags&flNewlineOK != 0 {
				sw.Do("\n", nil)
			}
		}
		sw.Do("}", targs)
	case validators.SliceLiteral:
		if flags&flNewlineOK != 0 {
			sw.Do("\n", nil)
		}
		sw.Do("[]", nil)
		targs := generator.Args{
			"type": c.Universe.Type(v.ElementType),
		}
		sw.Do("$.type|raw$", targs)
		if len(v.ElementTypeArgs) > 0 {
			sw.Do("[", nil)
			for i, typeArg := range v.ElementTypeArgs {
				if i > 0 {
					sw.Do(", ", nil)
				}
				sw.Do("$.|raw$", typeArg)
			}
			sw.Do("]", nil)
		}
		sw.Do("{", nil)
		if flags&flNewlineOK != 0 {
			sw.Do("\n", nil)
		}
		for _, e := range v.Elements {
			// TODO: 结构体切片会多出空行：`{` 与 `,` 后各换行一次，结构体前又一次。
			// 应在 sw 中记录 "isNewLine"，改用 sw.NewLine() 仅在上一行未以换行结尾时换行。
			g.toGolangSourceDataLiteral(sw, c, e, flags)
			sw.Do(",", nil)
			if flags&flNewlineOK != 0 {
				sw.Do("\n", nil)
			}
		}
		sw.Do("}", nil)
	default:
		rv := reflect.ValueOf(value)
		switch rv.Kind() {
		case reflect.Array:
			arraySize := ""
			if rv.Kind() == reflect.Array {
				arraySize = strconv.Itoa(rv.Len())
			}
			var itemType string
			switch rv.Type().Elem().Kind() {
			case reflect.String: // For now, only support lists of strings.
				itemType = rv.Type().Elem().Name()
			default:
				panic(fmt.Sprintf("Unsupported extraArg type: %T", value))
			}
			rv.Type().Elem()
			sw.Do("[$.arraySize$]$.itemType${", map[string]string{"arraySize": arraySize, "itemType": itemType})
			for i := range rv.Len() {
				val := rv.Index(i)
				g.toGolangSourceDataLiteral(sw, c, val.Interface(), flags)
				if i < rv.Len()-1 {
					sw.Do(", ", nil)
				}
			}
			sw.Do("}", nil)
		default:
			// TODO: 应在发现阶段检查此情形，并给出更有用的错误信息。
			panic(fmt.Sprintf("Unsupported extraArg type: %T", value))
		}
	}
}

// emitFunctionCall 输出一次校验函数调用（含泛型类型参数、额外参数与
// 稳定性级别标记），供 Wrapper 与 MultiWrapper 复用。
func (g *genValidations) emitFunctionCall(sw *generator.SnippetWriter, c *generator.Context, v validators.FunctionGen, leadingArgs ...string) {
	targs := generator.Args{
		"funcName": c.Universe.Type(g.resolveFunc(v.Function)),
	}
	sw.Do("$.funcName|raw$", targs)
	if typeArgs := v.TypeArgs; len(typeArgs) > 0 {
		sw.Do("[", nil)
		for i, typeArg := range typeArgs {
			sw.Do("$.|raw$", c.Universe.Type(typeArg))
			if i < len(typeArgs)-1 {
				sw.Do(",", nil)
			}
		}
		sw.Do("]", nil)
	}
	sw.Do("(", nil)
	if len(leadingArgs) > 0 {
		sw.Do(strings.Join(leadingArgs, ", "), nil)
	}
	if len(leadingArgs) > 0 && len(v.Args) > 0 {
		sw.Do(", ", nil)
	}
	for i, arg := range v.Args {
		if i != 0 {
			sw.Do(", ", nil)
		}
		g.toGolangSourceDataLiteral(sw, c, arg, flNewlineOK)
	}
	sw.Do(")", nil)
	switch v.StabilityLevel {
	case validators.ValidationStabilityLevelAlpha:
		sw.Do(".MarkAlpha()", nil)
	case validators.ValidationStabilityLevelBeta:
		sw.Do(".MarkBeta()", nil)
	}
}

// getLeafTypeAndPrefixes 返回给定类型的“叶子值类型”，以及该输入类型对应的
// 类型前缀与表达式前缀：类型前缀加在类型名上得到可空形式，表达式前缀加在
// 变量上得到该值的可空形式。
//
// 例如输入 "string" 返回 (string, "*", "&")：叶子类型是 string，加类型前缀得
// *string；变量 x 加表达式前缀得 &x。
//
// 输入 "*string" 则返回 (string, "*", "")：叶子仍是 string，变量 x 保持 x。
func getLeafTypeAndPrefixes(inType *types.Type) (*types.Type, string, string) {
	leafType := inType
	typePfx := ""
	exprPfx := ""

	nPtrs := 0
	for leafType.Kind == types.Pointer {
		nPtrs++
		leafType = leafType.Elem
	}
	if !util.IsNilableType(leafType) {
		typePfx = "*"
		if nPtrs == 0 {
			exprPfx = "&"
		} else {
			exprPfx = strings.Repeat("*", nPtrs-1)
		}
	} else {
		exprPfx = strings.Repeat("*", nPtrs)
	}

	return leafType, typePfx, exprPfx
}

// FixtureTests 生成一个测试文件，检查所有 validateFalse 校验。
// 只有启用 validateFalse 测试夹具时才生成测试函数，避免无关包多出测试。
func FixtureTests(outputFilename string, testFixtureTags sets.Set[string]) generator.Generator {
	return &fixtureTestGen{
		GoGenerator: generator.GoGenerator{
			OutputFilename: outputFilename,
		},
		testFixtureTags: testFixtureTags,
	}
}

// fixtureTestGen 生成调用 testscheme.ValidateFixtures 的测试函数。
type fixtureTestGen struct {
	generator.GoGenerator
	testFixtureTags sets.Set[string]
}

// Imports 生成文件只依赖 testing 包。
func (g *fixtureTestGen) Imports(_ *generator.Context) (imports []string) {
	return []string{`"testing"`}
}

// Init 按夹具类型决定是否输出测试函数。
func (g *fixtureTestGen) Init(c *generator.Context, w io.Writer) error {
	if g.testFixtureTags.Has("validateFalse") {
		sw := generator.NewSnippetWriter(w, c, "$", "$")
		sw.Do("func TestValidation(t *testing.T) {\n", nil)
		sw.Do("  localSchemeBuilder.Test(t).ValidateFixtures()\n", nil)
		sw.Do("}\n", nil)
	}
	return nil
}
