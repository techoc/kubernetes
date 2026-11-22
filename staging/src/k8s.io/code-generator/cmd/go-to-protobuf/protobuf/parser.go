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
	"bytes"
	"errors"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"reflect"
	"strings"

	customreflect "k8s.io/code-generator/third_party/forked/golang/reflect"
)

// rewriteFile 解析 name 指向的 Go 文件，交给 rewriteFn 做 AST 级修改，
// 然后用 header 覆盖文件头并整体格式化写回。
//
// 之所以走“解析-改写-重新打印”而不是文本替换：生成代码的排版会随 gogo
// 版本变化，只有基于 AST 的改写才能在格式差异下保持幂等。
func rewriteFile(name string, header []byte, rewriteFn func(*token.FileSet, *ast.File) error) error {
	fset := token.NewFileSet()
	src, err := os.ReadFile(name)
	if err != nil {
		return err
	}
	// 需要 ParseComments，否则注释映射表为空，后续 cmap.Filter 会丢掉全部注释。
	file, err := parser.ParseFile(fset, name, src, parser.DeclarationErrors|parser.ParseComments)
	if err != nil {
		return err
	}

	if err := rewriteFn(fset, file); err != nil {
		return err
	}

	b := &bytes.Buffer{}
	// 先写入许可证头，再打印 AST：printer 不会保留原文件的头部注释。
	b.Write(header)
	if err := printer.Fprint(b, fset, file); err != nil {
		return err
	}

	body, err := format.Source(b.Bytes())
	if err != nil {
		return err
	}

	f, err := os.OpenFile(name, os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(body); err != nil {
		return err
	}
	return f.Close()
}

// ExtractFunc 从 TypeSpec 中提取信息，返回 true 表示该类型应从目标文件中移除。
type ExtractFunc func(*ast.TypeSpec) bool

// OptionalFunc 返回 true 表示该本地类型名带 protobuf.nullable=true，
// 其序列化函数需要去掉 'Items' 访问器。
type OptionalFunc func(name string) bool

// RewriteGeneratedGogoProtobufFile 对 gogo 生成的 .pb.go 文件做后处理：
// 适配 optional 类型、去掉 gogo 依赖、删除已存在的类型声明与空导入。
// 目的是让生成结果只保留 Kubernetes 自身需要的部分，避免引入 gogo 运行时依赖。
func RewriteGeneratedGogoProtobufFile(file string, extractFn ExtractFunc, optionalFn OptionalFunc, header []byte, dropGogo bool) error {
	return rewriteFile(file, header, func(fset *token.FileSet, file *ast.File) error {
		// 记录原始注释位置，改写后据此剔除已失效的注释。
		cmap := ast.NewCommentMap(fset, file, file.Comments)

		// 转换指向 optional map 或 slice 的方法。
		for _, d := range file.Decls {
			rewriteOptionalMethods(d, optionalFn)
		}
		if dropGogo {
			// 转换对 gogo sort 工具的引用。
			var oldSortImport string
			var usedSort bool
			for _, d := range file.Decls {
				oldSortImport, usedSort = rewriteGogoSortImport(d)
				if usedSort {
					break
				}
			}
			if usedSort {
				for _, d := range file.Decls {
					rewriteGogoSort(d, oldSortImport, "sort")
				}
			}
		}

		// 移除已声明的类型。
		decls := []ast.Decl{}
		for _, d := range file.Decls {
			if dropExistingTypeDeclarations(d, extractFn) {
				continue
			}
			if dropEmptyImportDeclarations(d) {
				continue
			}
			// 只保留必需的函数。
			if dropGogo && dropUnusedGo(d) {
				continue
			}
			decls = append(decls, d)
		}
		file.Decls = decls

		// 移除已无法映射到节点的注释。
		file.Comments = cmap.Filter(file).Comments()
		return nil
	})
}

// rewriteGogoSortImport 把 "github.com/gogo/protobuf/sortkeys" 的导入改写为 "sort"，
// 返回原包别名以及是否发生了改写。
// 若该 decl 不是 import decl，或不包含该导入，返回 "", false。
func rewriteGogoSortImport(decl ast.Decl) (string, bool) {
	t, ok := decl.(*ast.GenDecl)
	if !ok {
		return "", false
	}
	if t.Tok != token.IMPORT {
		return "", false
	}
	for _, s := range t.Specs {
		if spec, ok := s.(*ast.ImportSpec); ok {
			if spec.Path != nil && spec.Path.Value == `"github.com/gogo/protobuf/sortkeys"` {
				// 把 gogo sort 换成标准库 sort。
				spec.Path.Value = `"sort"`
				oldName := "sortkeys"
				if spec.Name != nil {
					oldName = spec.Name.Name
				}
				// 清除别名，使用标准库包名 sort。
				spec.Name = nil
				return oldName, true
			}
		}
	}
	return "", false
}

// rewriteGogoSort 遍历 AST，把 oldSortImport 包的使用替换为 newSortImport。
func rewriteGogoSort(decl ast.Decl, oldSortImport, newSortImport string) {
	t, ok := decl.(*ast.FuncDecl)
	if !ok {
		return
	}
	ast.Walk(replacePackageVisitor{oldPackage: oldSortImport, newPackage: newSortImport}, t.Body)
}

// keepFuncs 是应保留的顶层函数白名单。
// 白名单之外的函数（如 gogo 的 XXX_ 系列、Getter）在 dropGogo 模式下会被删除。
var keepFuncs = map[string]bool{
	// 生成的辅助函数
	"sovGenerated":           true,
	"sozGenerated":           true,
	"skipGenerated":          true,
	"encodeVarintGenerated":  true,
	"valueToStringGenerated": true,

	// 反序列化
	"Reset":     true,
	"Unmarshal": true,

	// 序列化
	"Size":                 true,
	"Marshal":              true,
	"MarshalTo":            true,
	"MarshalToSizedBuffer": true,

	// 其他被广泛使用的方法
	"String": true,
}

// keepVars 是应保留的顶层变量白名单（错误变量被生成代码引用）。
var keepVars = map[string]bool{
	"ErrInvalidLengthGenerated":        true,
	"ErrIntOverflowGenerated":          true,
	"ErrUnexpectedEndOfGroupGenerated": true,
}

// dropUnusedGo 返回该顶层 decl 是否应被丢弃。
// 各类 decl 的行为如下：
//   - import：改写以去掉 gogo 包导入。若 decl 中全部是 gogo 导入则返回 true，
//     若仍有非 gogo 导入则返回 false。
//   - var：改写以去掉不在 keepVars 白名单中的变量。若全部被移除返回 true，
//     若仍有白名单变量返回 false。
//   - const：返回 true
//   - type：返回 true
//   - func：若函数不在 keepFuncs 白名单中则应丢弃，返回 true。
//   - other：返回 false
func dropUnusedGo(decl ast.Decl) bool {
	switch t := decl.(type) {
	case *ast.GenDecl:
		switch t.Tok {
		case token.IMPORT:
			specs := []ast.Spec{}
			for _, s := range t.Specs {
				if spec, ok := s.(*ast.ImportSpec); ok {
					if spec.Path == nil || !strings.HasPrefix(spec.Path.Value, `"github.com/gogo/protobuf/`) {
						specs = append(specs, spec)
					}
				}
			}
			if len(specs) == 0 {
				return true
			}
			t.Specs = specs
			return false
		case token.CONST:
			// 丢弃全部 const 声明。
			return true
		case token.VAR:
			specs := []ast.Spec{}
			for _, s := range t.Specs {
				if spec, ok := s.(*ast.ValueSpec); ok {
					if keepVars[spec.Names[0].Name] {
						specs = append(specs, spec)
					}
				}
			}
			if len(specs) == 0 {
				return true
			}
			t.Specs = specs
			return false
		case token.TYPE:
			// 丢弃全部类型声明。
			return true
		}
	case *ast.FuncDecl:
		name := ""
		if t.Name != nil {
			name = t.Name.Name
		}
		return !keepFuncs[name]
	default:
		return false
	}
	return false
}

// rewriteOptionalMethods 对标记为 optional（在 wire 上可能为 nil）的类型的
// 序列化方法做特定改写。这样 protobuf 才能序列化 map 或 slice 并区分空值与 nil
// （protobuf 本身无法表达该区别）。
// TODO: move into upstream gogo-protobuf once https://github.com/gogo/protobuf/issues/181
// has agreement
func rewriteOptionalMethods(decl ast.Decl, isOptional OptionalFunc) {
	if t, ok := decl.(*ast.FuncDecl); ok {
		ident, ptr, ok := receiver(t)
		if !ok {
			return
		}

		// 把 `m.Field = &OptionalType{}` 形式的初始化改为
		// `m.Field = OptionalType{}`。
		if t.Name.Name == "Unmarshal" {
			ast.Walk(optionalAssignmentVisitor{fn: isOptional}, t.Body)
		}

		if !isOptional(ident.Name) {
			return
		}

		switch t.Name.Name {
		case "Unmarshal":
			ast.Walk(&optionalItemsVisitor{}, t.Body)
		case "MarshalTo", "Size", "String", "MarshalToSizedBuffer":
			ast.Walk(&optionalItemsVisitor{}, t.Body)
			fallthrough
		case "Marshal":
			// 若方法使用指针接收者，改回普通接收者，与 optional 字段展开后的类型一致。
			if ptr {
				t.Recv.List[0].Type = ident
			}
		}
	}
}

// optionalAssignmentVisitor 把 optional 类型的指针初始化改写为值初始化。
type optionalAssignmentVisitor struct {
	fn OptionalFunc
}

// Visit 遍历节点，把 m.Field = &OptionalType{} 改写为 m.Field = OptionalType{}。
func (v optionalAssignmentVisitor) Visit(n ast.Node) ast.Visitor {
	if t, ok := n.(*ast.AssignStmt); ok {
		if len(t.Lhs) == 1 && len(t.Rhs) == 1 {
			if !isFieldSelector(t.Lhs[0], "m", "") {
				return nil
			}
			unary, ok := t.Rhs[0].(*ast.UnaryExpr)
			if !ok || unary.Op != token.AND {
				return nil
			}
			// 只处理空的复合字面量：带元素的初始化语义不同，不能简单去地址。
			composite, ok := unary.X.(*ast.CompositeLit)
			if !ok || composite.Type == nil || len(composite.Elts) != 0 {
				return nil
			}
			if ident, ok := composite.Type.(*ast.Ident); ok && v.fn(ident.Name) {
				t.Rhs[0] = composite
			}
		}
		return nil
	}
	return v
}

// optionalItemsVisitor 把 optional 类型的 `m.Items` 访问改写为对 m 本身的访问。
type optionalItemsVisitor struct{}

// Visit 遍历节点，寻找与“把 struct{ map[x]y || []x } 变成 map[x]y 或 []x”
// 等价的模式并改写。
func (v *optionalItemsVisitor) Visit(n ast.Node) ast.Visitor {
	switch t := n.(type) {
	case *ast.RangeStmt:
		if isFieldSelector(t.X, "m", "Items") {
			t.X = &ast.Ident{Name: "m"}
		}
	case *ast.AssignStmt:
		if len(t.Lhs) == 1 && len(t.Rhs) == 1 {
			switch lhs := t.Lhs[0].(type) {
			case *ast.IndexExpr:
				if isFieldSelector(lhs.X, "m", "Items") {
					lhs.X = &ast.StarExpr{X: &ast.Ident{Name: "m"}}
				}
			default:
				if isFieldSelector(t.Lhs[0], "m", "Items") {
					t.Lhs[0] = &ast.StarExpr{X: &ast.Ident{Name: "m"}}
				}
			}
			if rhs, ok := t.Rhs[0].(*ast.CallExpr); ok {
				if ident, ok := rhs.Fun.(*ast.Ident); ok && ident.Name == "append" {
					ast.Walk(v, rhs)
					if len(rhs.Args) > 0 {
						if arg, ok := rhs.Args[0].(*ast.Ident); ok {
							if arg.Name == "m" {
								rhs.Args[0] = &ast.StarExpr{X: &ast.Ident{Name: "m"}}
							}
						}
					}
					return nil
				}
			}
		}
	case *ast.IfStmt:
		if cond, ok := t.Cond.(*ast.BinaryExpr); ok {
			if cond.Op == token.EQL {
				if isFieldSelector(cond.X, "m", "Items") && isIdent(cond.Y, "nil") {
					cond.X = &ast.StarExpr{X: &ast.Ident{Name: "m"}}
				}
			}
		}
		if t.Init != nil {
			// 匹配形式：
			// if err := m[len(m.Items)-1].Unmarshal(data[iNdEx:postIndex]); err != nil {
			// 	return err
			// }
			if s, ok := t.Init.(*ast.AssignStmt); ok {
				if call, ok := s.Rhs[0].(*ast.CallExpr); ok {
					if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
						if x, ok := sel.X.(*ast.IndexExpr); ok {
							// m[] -> (*m)[]
							if sel2, ok := x.X.(*ast.SelectorExpr); ok {
								if ident, ok := sel2.X.(*ast.Ident); ok && ident.Name == "m" {
									x.X = &ast.StarExpr{X: &ast.Ident{Name: "m"}}
								}
							}
							// len(m.Items) -> len(*m)
							if bin, ok := x.Index.(*ast.BinaryExpr); ok {
								if call2, ok := bin.X.(*ast.CallExpr); ok && len(call2.Args) == 1 {
									if isFieldSelector(call2.Args[0], "m", "Items") {
										call2.Args[0] = &ast.StarExpr{X: &ast.Ident{Name: "m"}}
									}
								}
							}
						}
					}
				}
			}
		}
	case *ast.IndexExpr:
		if isFieldSelector(t.X, "m", "Items") {
			t.X = &ast.Ident{Name: "m"}
			return nil
		}
	case *ast.CallExpr:
		changed := false
		for i := range t.Args {
			if isFieldSelector(t.Args[i], "m", "Items") {
				t.Args[i] = &ast.Ident{Name: "m"}
				changed = true
			}
		}
		if changed {
			return nil
		}
	}
	return v
}

// isFieldSelector 判断 n 是否为 `name.field` 形式的选择器表达式；
// field 为空时只检查接收者。
func isFieldSelector(n ast.Expr, name, field string) bool {
	s, ok := n.(*ast.SelectorExpr)
	if !ok || s.Sel == nil || (field != "" && s.Sel.Name != field) {
		return false
	}
	return isIdent(s.X, name)
}

// isIdent 判断 n 是否为名为 value 的标识符。
func isIdent(n ast.Expr, value string) bool {
	ident, ok := n.(*ast.Ident)
	return ok && ident.Name == value
}

// receiver 解析方法声明的接收者，返回类型标识符、是否为指针接收者以及是否成功。
func receiver(f *ast.FuncDecl) (ident *ast.Ident, pointer bool, ok bool) {
	if f.Recv == nil || len(f.Recv.List) != 1 {
		return nil, false, false
	}
	switch t := f.Recv.List[0].Type.(type) {
	case *ast.StarExpr:
		identity, ok := t.X.(*ast.Ident)
		if !ok {
			return nil, false, false
		}
		return identity, true, true
	case *ast.Ident:
		return t, false, true
	}
	return nil, false, false
}

// dropExistingTypeDeclarations 移除 extractFn 返回 true 的类型声明。
// 若整个声明都被移除，返回 true。
func dropExistingTypeDeclarations(decl ast.Decl, extractFn ExtractFunc) bool {
	if t, ok := decl.(*ast.GenDecl); ok {
		if t.Tok != token.TYPE {
			return false
		}
		specs := []ast.Spec{}
		for _, s := range t.Specs {
			if spec, ok := s.(*ast.TypeSpec); ok {
				if extractFn(spec) {
					continue
				}
				specs = append(specs, spec)
			}
		}
		if len(specs) == 0 {
			return true
		}
		t.Specs = specs
	}
	return false
}

// dropEmptyImportDeclarations 去掉生成代码中无实际作用的导入（如 `_ "pkg"`），
// 防止生成结果引入意外的副作用。若整个声明都被移除，返回 true。
func dropEmptyImportDeclarations(decl ast.Decl) bool {
	if t, ok := decl.(*ast.GenDecl); ok {
		if t.Tok != token.IMPORT {
			return false
		}
		specs := []ast.Spec{}
		for _, s := range t.Specs {
			if spec, ok := s.(*ast.ImportSpec); ok {
				if spec.Name != nil && spec.Name.Name == "_" {
					continue
				}
				specs = append(specs, spec)
			}
		}
		if len(specs) == 0 {
			return true
		}
		t.Specs = specs
	}
	return false
}

// RewriteTypesWithProtobufStructTags 把 structTags 中的 protobuf 标签写入
// name 文件对应的结构体字段。已存在的标签不会被覆盖，保证手工调整优先。
func RewriteTypesWithProtobufStructTags(name string, structTags map[string]map[string]string) error {
	return rewriteFile(name, []byte{}, func(fset *token.FileSet, file *ast.File) error {
		allErrs := []error{}

		// 设置所有新的 struct 标签。
		for _, d := range file.Decls {
			if errs := updateStructTags(d, structTags, []string{"protobuf"}); len(errs) > 0 {
				allErrs = append(allErrs, errs...)
			}
		}

		// 汇总全部错误后一次返回，便于用户一次修完所有标签问题。
		if len(allErrs) > 0 {
			var s string
			for _, err := range allErrs {
				s += err.Error() + "\n"
			}
			return errors.New(s)
		}
		return nil
	})
}

// getFieldName 从字段表达式中取出字段名，支持标识符、选择器和指针。
// 内嵌字段没有显式名字，只能从类型表达式推导。
func getFieldName(expr ast.Expr, structname string) (name string, err error) {
	for {
		switch t := expr.(type) {
		case *ast.Ident:
			return t.Name, nil
		case *ast.SelectorExpr:
			return t.Sel.Name, nil
		case *ast.StarExpr:
			expr = t.X
		default:
			return "", fmt.Errorf("unable to get name for tag from struct %q, field %#v", structname, t)
		}
	}
}

// updateStructTags 为声明中的结构体字段补充 toCopy 指定的标签。
func updateStructTags(decl ast.Decl, structTags map[string]map[string]string, toCopy []string) []error {
	var errs []error
	t, ok := decl.(*ast.GenDecl)
	if !ok {
		return nil
	}
	if t.Tok != token.TYPE {
		return nil
	}

	for _, s := range t.Specs {
		spec, ok := s.(*ast.TypeSpec)
		if !ok {
			continue
		}
		typeName := spec.Name.Name
		fieldTags, ok := structTags[typeName]
		if !ok {
			continue
		}
		st, ok := spec.Type.(*ast.StructType)
		if !ok {
			continue
		}

		for i := range st.Fields.List {
			f := st.Fields.List[i]
			var name string
			var err error
			if len(f.Names) == 0 {
				// 内嵌字段：从类型表达式取名字。
				name, err = getFieldName(f.Type, spec.Name.Name)
				if err != nil {
					errs = append(errs, err)
					continue
				}
			} else {
				name = f.Names[0].Name
			}
			value, ok := fieldTags[name]
			if !ok {
				continue
			}
			var tags customreflect.StructTags
			if f.Tag != nil {
				oldTags, err := customreflect.ParseStructTags(strings.Trim(f.Tag.Value, "`"))
				if err != nil {
					errs = append(errs, fmt.Errorf("unable to read struct tag from struct %q, field %q: %v", spec.Name.Name, name, err))
					continue
				}
				tags = oldTags
			}
			for _, name := range toCopy {
				// 不覆盖已有标签。
				if tags.Has(name) {
					continue
				}
				// 追加新标签。
				if v := reflect.StructTag(value).Get(name); len(v) > 0 {
					tags = append(tags, customreflect.StructTag{Name: name, Value: v})
				}
			}
			if len(tags) == 0 {
				continue
			}
			if f.Tag == nil {
				f.Tag = &ast.BasicLit{}
			}
			f.Tag.Value = tags.String()
		}
	}
	return errs
}

// replacePackageVisitor 把对某个包名的引用替换为另一个包名。
type replacePackageVisitor struct {
	oldPackage string
	newPackage string
}

// Visit 遍历节点，把对 oldPackage 的引用替换为 newPackage。
func (v replacePackageVisitor) Visit(n ast.Node) ast.Visitor {
	if e, ok := n.(*ast.SelectorExpr); ok {
		if i, ok := e.X.(*ast.Ident); ok && i.Name == v.oldPackage {
			i.Name = v.newPackage
		}
		return nil
	}
	return v
}
