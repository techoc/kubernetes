/*
Copyright 2016 The Kubernetes Authors.

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

// 列出所有将要运行的单元测试和 ginkgo 测试名称
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

var (
	dumpTree = flag.Bool("dump", false, "print AST")
	dumpJSON = flag.Bool("json", false, "output test list as JSON")
	warn     = flag.Bool("warn", false, "print warnings")
)

// Test 保存测试位置、包名和测试名称。
type Test struct {
	Loc      string
	Name     string
	TestName string
}

// collect 从文件中提取测试元数据。
// 若 src 为 nil，则从 filename 读取代码，否则使用 src
// （可以是 string、byte[] 或 io.Reader）。
func collect(filename string, src interface{}) []Test {
	// 通过解析 src 创建 AST。
	fset := token.NewFileSet() // 位置相对于 fset
	f, err := parser.ParseFile(fset, filename, src, parser.ParseComments)
	if err != nil {
		panic(err)
	}

	if *dumpTree {
		ast.Print(fset, f)
	}

	tests := make([]Test, 0)

	ast.Walk(makeWalker("[k8s.io]", fset, &tests), f)

	// 单元测试的枚举简单得多！
	if strings.HasSuffix(filename, "_test.go") {
		packageName := f.Name.Name
		dirName, _ := filepath.Split(filename)
		if filepath.Base(dirName) != packageName && *warn {
			log.Printf("Warning: strange path/package mismatch %s %s\n", filename, packageName)
		}
		testPath := "k8s.io/kubernetes/" + dirName[:len(dirName)-1]
		for _, decl := range f.Decls {
			funcdecl, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			name := funcdecl.Name.Name
			if strings.HasPrefix(name, "Test") {
				tests = append(tests, Test{fset.Position(funcdecl.Pos()).String(), testPath, name})
			}
		}
	}

	return tests
}

// funcName 将带有两个标识符的 selectorExpr 转换为字符串，
// 例如 x.y -> "x.y"
func funcName(n ast.Expr) string {
	if sel, ok := n.(*ast.SelectorExpr); ok {
		if x, ok := sel.X.(*ast.Ident); ok {
			return x.String() + "." + sel.Sel.String()
		}
	}
	return ""
}

// isSprintf 判断给定节点是否是对 fmt.Sprintf 的调用
func isSprintf(n ast.Expr) bool {
	call, ok := n.(*ast.CallExpr)
	return ok && funcName(call.Fun) == "fmt.Sprintf" && len(call.Args) != 0
}

type walker struct {
	path  string
	fset  *token.FileSet
	tests *[]Test
	vals  map[string]string
}

func makeWalker(path string, fset *token.FileSet, tests *[]Test) *walker {
	return &walker{path, fset, tests, make(map[string]string)}
}

// clone 创建一个新的 walker，用给定字符串扩展其路径。
func (w *walker) clone(ext string) *walker {
	return &walker{w.path + " " + ext, w.fset, w.tests, w.vals}
}

// firstArg 尝试静态确定第一个参数的值。
// 它只处理字符串，并将任何未知的值
// （fmt.Sprintf 插值）转换为 *。
func (w *walker) firstArg(n *ast.CallExpr) string {
	if len(n.Args) == 0 {
		return ""
	}
	var lit *ast.BasicLit
	if isSprintf(n.Args[0]) {
		return w.firstArg(n.Args[0].(*ast.CallExpr))
	}
	lit, ok := n.Args[0].(*ast.BasicLit)
	if ok && lit.Kind == token.STRING {
		v, err := strconv.Unquote(lit.Value)
		if err != nil {
			panic(err)
		}
		if strings.Contains(v, "%") {
			v = strings.Replace(v, "%d", "*", -1)
			v = strings.Replace(v, "%v", "*", -1)
			v = strings.Replace(v, "%s", "*", -1)
		}
		return v
	}
	if ident, ok := n.Args[0].(*ast.Ident); ok {
		if val, ok := w.vals[ident.String()]; ok {
			return val
		}
	}
	if *warn {
		log.Printf("Warning: dynamic arg value: %v\n", w.fset.Position(n.Args[0].Pos()))
	}
	return "*"
}

// describeName 若函数是 Ginkgo 相关的函数
// （Describe/SIGDescribe/Context），则返回其第一个参数，
// 否则返回空字符串。
func (w *walker) describeName(n *ast.CallExpr) string {
	switch x := n.Fun.(type) {
	case *ast.Ident:
		if x.Name != "SIGDescribe" && x.Name != "Describe" && x.Name != "Context" {
			return ""
		}
	default:
		return ""
	}
	return w.firstArg(n)
}

// itName 若是对 It() 的调用则返回第一个参数，否则返回 ""。
func (w *walker) itName(n *ast.CallExpr) string {
	if fun, ok := n.Fun.(*ast.Ident); ok && fun.Name == "It" {
		return w.firstArg(n)
	}
	return ""
}

// Visit 遍历 AST，跟踪 Ginkgo 上下文并收集测试。
// 更多细节参见 ast.Walk 的文档。
func (w *walker) Visit(n ast.Node) ast.Visitor {
	switch x := n.(type) {
	case *ast.CallExpr:
		name := w.describeName(x)
		if name != "" && len(x.Args) >= 2 {
			// 若调用 (Kube)Describe/Context，则创建一个新的
			// walker，带上描述信息继续递归。
			return w.clone(name)
		}
		name = w.itName(x)
		if name != "" {
			// 找到了 It() 调用，现在可以确定
			// 完整的测试名称。
			if w.path == "[k8s.io]" && *warn {
				log.Printf("It without matching Describe: %s\n", w.fset.Position(n.Pos()))
			}
			*w.tests = append(*w.tests, Test{w.fset.Position(n.Pos()).String(), w.path, name})
			return nil // 停止遍历
		}
	case *ast.AssignStmt:
		// 尝试跟踪可能用作参数的字面量。
		// 这种分析非常不严谨，既忽略作用域，
		// 也忽略程序流程，但足以满足
		// 我们这个小小的使用场景。
		ident, ok := x.Lhs[0].(*ast.Ident)
		if ok {
			if isSprintf(x.Rhs[0]) {
				// x := fmt.Sprintf("something", args)
				w.vals[ident.String()] = w.firstArg(x.Rhs[0].(*ast.CallExpr))
			}
			if lit, ok := x.Rhs[0].(*ast.BasicLit); ok && lit.Kind == token.STRING {
				// x := "a literal string"
				v, err := strconv.Unquote(lit.Value)
				if err != nil {
					panic(err)
				}
				w.vals[ident.String()] = v
			}
		}
	}
	return w // 继续遍历
}

type testList struct {
	tests []Test
}

// handlePath 递归遍历文件系统，从路径匹配
// *e2e*.go 和 *_test.go 的文件中收集测试，
// 并忽略 third_party 和 staging 目录。
func (t *testList) handlePath(path string, info os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	if strings.Contains(path, "third_party") ||
		strings.Contains(path, "staging") ||
		strings.Contains(path, "_output") {
		return filepath.SkipDir
	}
	if strings.HasSuffix(path, ".go") && strings.Contains(path, "e2e") ||
		strings.HasSuffix(path, "_test.go") {
		tests := collect(path, nil)
		t.tests = append(t.tests, tests...)
	}
	return nil
}

func main() {
	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		args = append(args, ".")
	}
	tests := testList{}
	for _, arg := range args {
		err := filepath.Walk(arg, tests.handlePath)
		if err != nil {
			log.Fatalf("Error walking: %v", err)
		}
	}
	if *dumpJSON {
		json, err := json.Marshal(tests.tests)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(json))
	} else {
		for _, t := range tests.tests {
			fmt.Println(t)
		}
	}
}
