/*
Copyright 2019 The Kubernetes Authors.

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

// 验证所有 import 都使用了我们偏好的别名。
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/build"
	"go/format"
	"go/parser"
	"go/token"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"golang.org/x/term"
)

var (
	importAliases = flag.String("import-aliases", "hack/.import-aliases", "json file with import aliases")
	confirm       = flag.Bool("confirm", false, "update file with the preferred aliases for imports")
	regex         = flag.String("include-path", "(test/e2e/|test/e2e_node)", "only files with paths matching this regex is touched")
	isTerminal    = term.IsTerminal(int(os.Stdout.Fd()))
	logPrefix     = ""
	aliases       = map[*regexp.Regexp]string{}
)

type analyzer struct {
	fset      *token.FileSet // 位置相对于 fset
	ctx       build.Context
	failed    bool
	donePaths map[string]interface{}
}

func newAnalyzer() *analyzer {
	ctx := build.Default
	ctx.CgoEnabled = true

	a := &analyzer{
		fset:      token.NewFileSet(),
		ctx:       ctx,
		donePaths: make(map[string]interface{}),
	}

	return a
}

// collect 从文件中提取测试元数据。
func (a *analyzer) collect(dir string) {
	if _, ok := a.donePaths[dir]; ok {
		return
	}
	a.donePaths[dir] = nil

	// 通过解析 src 创建 AST。
	fs, err := parser.ParseDir(a.fset, dir, nil, parser.AllErrors|parser.ParseComments)

	if err != nil {
		fmt.Fprintln(os.Stderr, "ERROR(syntax)", logPrefix, err)
		a.failed = true
		return
	}

	for _, p := range fs {
		// 返回第一个错误，但 a.handleError 会处理它
		files := a.filterFiles(p.Files)
		for _, file := range files {
			replacements := make(map[string]string)
			pathToFile := a.fset.File(file.Pos()).Name()
			for _, imp := range file.Imports {
				importPath := strings.Replace(imp.Path.Value, "\"", "", -1)
				pathSegments := strings.Split(importPath, "/")
				importName := pathSegments[len(pathSegments)-1]
				if imp.Name != nil {
					importName = imp.Name.Name
				}
				for re, template := range aliases {
					match := re.FindStringSubmatchIndex(importPath)
					if match == nil {
						// 没有匹配。
						continue
					}
					if match[0] > 0 || match[1] < len(importPath) {
						// 不是完全匹配。
						continue
					}
					alias := string(re.ExpandString(nil, template, importPath, match))
					if alias != importName {
						if !*confirm {
							fmt.Fprintf(os.Stderr, "%sERROR wrong alias for import \"%s\" should be %s in file %s\n", logPrefix, importPath, alias, pathToFile)
							a.failed = true
						}
						replacements[importName] = alias
						if imp.Name != nil {
							imp.Name.Name = alias
						} else {
							imp.Name = ast.NewIdent(alias)
						}
					}
					break
				}
			}

			if len(replacements) > 0 {
				if *confirm {
					fmt.Printf("%sReplacing imports with aliases in file %s\n", logPrefix, pathToFile)
					for key, value := range replacements {
						renameImportUsages(file, key, value)
					}
					ast.SortImports(a.fset, file)
					var buffer bytes.Buffer
					if err = format.Node(&buffer, a.fset, file); err != nil {
						panic(fmt.Sprintf("Error formatting ast node after rewriting import.\n%s\n", err.Error()))
					}

					fileInfo, err := os.Stat(pathToFile)
					if err != nil {
						panic(fmt.Sprintf("Error stat'ing file: %s\n%s\n", pathToFile, err.Error()))
					}

					err = os.WriteFile(pathToFile, buffer.Bytes(), fileInfo.Mode())
					if err != nil {
						panic(fmt.Sprintf("Error writing file: %s\n%s\n", pathToFile, err.Error()))
					}
				}
			}
		}
	}
}

func renameImportUsages(f *ast.File, old, new string) {
	// 用这个变量避免重命名包声明，例如：
	//   给定：package foo; import foo "bar"; foo.Baz，将 foo 重命名为 qux
	//   结果：package foo; import qux "bar"; qux.Baz
	var pkg *ast.Ident

	// 将顶层的 old 重命名为 new，既包括未解析的名字
	// （可能在其他文件中定义），也包括解析到
	// 我们已重命名声明的名字。
	ast.Inspect(f, func(node ast.Node) bool {
		if node == nil {
			return false
		}
		switch id := node.(type) {
		case *ast.File:
			pkg = id.Name
		case *ast.Ident:
			if pkg != nil && id == pkg {
				return false
			}
			if id.Name == old {
				id.Name = new
			}
		}
		return true
	})
}

func (a *analyzer) filterFiles(fs map[string]*ast.File) []*ast.File {
	var files []*ast.File
	for _, f := range fs {
		files = append(files, f)
	}
	return files
}

type collector struct {
	dirs  []string
	regex *regexp.Regexp
}

// handlePath 递归遍历文件系统，收集目录，
// 并忽略一些稍后会特殊处理的无用目录（隐藏目录/vendor 目录）。
func (c *collector) handlePath(path string, info os.FileInfo, err error) error {
	if err != nil {
		return err
	}
	if info.IsDir() {
		// 忽略隐藏目录（.git、.cache 等）
		if len(path) > 1 && path[0] == '.' ||
			// 特定 OS 的 vendor 代码通常由特定 OS 的包导入。
			// 我们对每个 OS 递归类型检查其导入的 vendored 包，
			// 而不是对每个 OS 检查所有内容。
			path == "vendor" ||
			path == "_output" ||
			// 这是个怪例：Go 会（大体上）忽略 /testdata/，
			// 这导致 kubernetes/vendor 无法工作。
			// 若 $GOSRC/$GOROOT 中没有 gopkg.in/yaml.v2，
			// edit/record.go 无法编译（类型检查和 shell 脚本都是如此）。
			path == "pkg/kubectl/cmd/testdata/edit" {
			return filepath.SkipDir
		}
		if c.regex.MatchString(path) {
			c.dirs = append(c.dirs, path)
		}
	}
	return nil
}

func main() {
	flag.Parse()
	args := flag.Args()

	if len(args) == 0 {
		args = append(args, ".")
	}

	regex, err := regexp.Compile(*regex)
	if err != nil {
		log.Fatalf("Error compiling regex: %v", err)
	}
	c := collector{regex: regex}
	for _, arg := range args {
		err := filepath.Walk(arg, c.handlePath)
		if err != nil {
			log.Fatalf("Error walking: %v", err)
		}
	}
	sort.Strings(c.dirs)

	if len(*importAliases) > 0 {
		bytes, err := os.ReadFile(*importAliases)
		if err != nil {
			log.Fatalf("Error reading import aliases: %v", err)
		}
		var stringAliases map[string]string
		err = json.Unmarshal(bytes, &stringAliases)
		if err != nil {
			log.Fatalf("Error loading aliases: %v", err)
		}
		for pattern, name := range stringAliases {
			re, err := regexp.Compile(pattern)
			if err != nil {
				log.Fatalf("Error parsing import path pattern %q as regular expression: %v", pattern, err)
			}
			aliases[re] = name
		}
	}
	if isTerminal {
		logPrefix = "\r" // 打印时清除状态栏
	}
	fmt.Println("checking-imports: ")

	a := newAnalyzer()
	for _, dir := range c.dirs {
		if isTerminal {
			fmt.Printf("\r\033[0m %-80s", dir)
		}
		a.collect(dir)
	}
	fmt.Println()
	if a.failed {
		os.Exit(1)
	}
}
