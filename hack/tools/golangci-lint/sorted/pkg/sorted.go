/*
Copyright 2025 The Kubernetes Authors.

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

// Package pkg 实现检查 feature gate 是否按字母序排列的 linter。
package pkg

import (
	"bufio"
	"fmt"
	"go/ast"
	"go/token"
	"sort"
	"strings"

	"github.com/pmezard/go-difflib/difflib"
	"golang.org/x/tools/go/analysis"
)

// Config 保存 sorted 分析器的配置。
type Config struct {
	// Files 指定待检查的文件；非空时只检查这些文件。
	Files []string
	// Debug 开启调试日志
	Debug bool
}

// NewAnalyzer 返回一个启用了调试日志的 sorted 分析器。
func NewAnalyzer() *analysis.Analyzer {
	return NewAnalyzerWithConfig(Config{Debug: true})
}

// NewAnalyzerWithConfig 按给定配置返回 sorted 分析器。
func NewAnalyzerWithConfig(config Config) *analysis.Analyzer {
	return &analysis.Analyzer{
		Name: "sorted",
		Doc:  "Checks if feature gates are sorted alphabetically in const and var blocks",
		Run: func(pass *analysis.Pass) (interface{}, error) {
			if config.Debug {
				fmt.Printf("Processing...\n")
			}
			return run(pass, config)
		},
	}
}

func isTargetFile(configFiles []string, filename string) bool {
	for _, item := range configFiles {
		if strings.HasSuffix(filename, item) {
			return true
		}
	}
	return false
}

func run(pass *analysis.Pass, config Config) (interface{}, error) {
	for _, file := range pass.Files {
		filename := pass.Fset.File(file.Pos()).Name()

		if !isTargetFile(config.Files, filename) {
			continue
		}

		// 检查文件中的所有声明
		for _, decl := range file.Decls {
			genDecl, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}

			// 处理普通 feature gate 所在的 var/const 块
			if (genDecl.Tok == token.VAR || genDecl.Tok == token.CONST) && len(genDecl.Specs) > 1 {
				// 提取 feature 及其注释
				features := extractFeatures(genDecl, file.Comments)

				// 不足两项时无需比较顺序
				if len(features) > 1 {
					// 按字母序排序
					sortedFeatures := sortFeatures(features)

					// 检查顺序是否变化
					orderChanged := hasOrderChanged(features, sortedFeatures)

					if orderChanged {
						// 生成 diff 以展示具体问题
						reportSortingIssue(pass, genDecl, features, sortedFeatures)
					}
				}
			}

			// 检查以 feature gate 为键的 map
			if genDecl.Tok == token.VAR {
				checkFeatureGateMaps(pass, genDecl)
			}
		}
	}
	return nil, nil
}

// checkFeatureGateMaps 检查以 feature gate 为键的 map 是否按字母序排列键
func checkFeatureGateMaps(pass *analysis.Pass, genDecl *ast.GenDecl) {
	for _, spec := range genDecl.Specs {
		valueSpec, ok := spec.(*ast.ValueSpec)
		if !ok || len(valueSpec.Names) == 0 || len(valueSpec.Values) == 0 {
			continue
		}

		// 逐个检查值是否为以 feature gate 为键的 map
		for _, value := range valueSpec.Values {
			compositeLit, ok := value.(*ast.CompositeLit)
			if !ok {
				continue
			}

			// 只处理 map 类型
			mapType, ok := compositeLit.Type.(*ast.MapType)
			if !ok {
				continue
			}

			// 判断键类型是否为 featuregate.Feature 或名称含 "Feature"
			isFeatureGateMap := false

			// 处理选择器表达式（如 featuregate.Feature）
			if selectorExpr, ok := mapType.Key.(*ast.SelectorExpr); ok {
				if selectorExpr.Sel.Name == "Feature" {
					isFeatureGateMap = true
				}
			}

			// 处理标识符（如 Feature）
			if ident, ok := mapType.Key.(*ast.Ident); ok {
				if ident.Name == "Feature" {
					isFeatureGateMap = true
				}
			}

			if !isFeatureGateMap {
				continue
			}

			// 确认是以 feature gate 为键的 map
			var features []Feature
			for _, elt := range compositeLit.Elts {
				keyValueExpr, ok := elt.(*ast.KeyValueExpr)
				if !ok {
					continue
				}

				// 提取作为键的 feature gate 标识
				var featureName string

				// 兼容不同形式的键
				switch key := keyValueExpr.Key.(type) {
				case *ast.Ident:
					featureName = key.Name
				case *ast.SelectorExpr:
					// 对 genericfeatures.APIServerIdentity 之类的选择器表达式
					if x, ok := key.X.(*ast.Ident); ok {
						featureName = x.Name + "." + key.Sel.Name
					} else {
						continue
					}
				default:
					continue
				}

				features = append(features, Feature{
					Name:     featureName,
					Comments: []string{}, // map 键没有注释
				})
			}

			if len(features) <= 1 {
				continue
			}

			// 按字母序排序
			sortedFeatures := sortFeatures(features)

			// 检查顺序是否变化
			orderChanged := hasOrderChanged(features, sortedFeatures)

			if orderChanged {
				// 生成 diff 以展示具体问题
				reportMapSortingIssue(pass, genDecl, valueSpec.Names[0].Name, features, sortedFeatures)
			}
		}
	}
}

// Feature 表示一个 feature 声明及其关联注释
type Feature struct {
	Name     string   // feature 名称
	Comments []string // 关联的注释
}

// extractFeatures 从 GenDecl 中提取 feature
func extractFeatures(decl *ast.GenDecl, comments []*ast.CommentGroup) []Feature {
	var features []Feature

	for _, spec := range decl.Specs {
		valueSpec, ok := spec.(*ast.ValueSpec)
		if !ok || len(valueSpec.Names) == 0 {
			continue
		}

		// 取 feature 名称
		name := valueSpec.Names[0].Name

		// 收集该 feature 的注释
		var featureComments []string

		// 优先取 ValueSpec 上的文档注释
		if valueSpec.Doc != nil {
			for _, comment := range valueSpec.Doc.List {
				featureComments = append(featureComments, comment.Text)
			}
		} else {
			// 否则查找紧邻该 spec 之前的注释
			for _, cg := range comments {
				if cg.End()+1 == valueSpec.Pos() {
					for _, comment := range cg.List {
						featureComments = append(featureComments, comment.Text)
					}
				}
			}
		}

		features = append(features, Feature{
			Name:     name,
			Comments: featureComments,
		})
	}

	return features
}

// sortFeatures 按名称字母序排序 feature
func sortFeatures(features []Feature) []Feature {
	sorted := make([]Feature, len(features))
	copy(sorted, features)

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Name < sorted[j].Name
	})

	return sorted
}

// hasOrderChanged 判断 feature 顺序是否发生变化
func hasOrderChanged(original, sorted []Feature) bool {
	if len(original) != len(sorted) {
		return true
	}

	for i := range original {
		if original[i].Name != sorted[i].Name {
			return true
		}
	}

	return false
}

// reportSortingIssue 报告顺序问题，并给出期望顺序的 diff
func reportSortingIssue(pass *analysis.Pass, decl *ast.GenDecl, current, sorted []Feature) {
	// 生成原始代码
	originalSource := generateSourceCode(decl.Tok, current)

	// 生成排序后的代码
	sortedSource := generateSourceCode(decl.Tok, sorted)

	// 生成两者的 unified diff
	diff := difflib.UnifiedDiff{
		A:        difflib.SplitLines(originalSource),
		B:        difflib.SplitLines(sortedSource),
		FromFile: "Current",
		ToFile:   "Expected",
		Context:  3,
	}

	diffText, err := difflib.GetUnifiedDiffString(diff)
	if err != nil {
		pass.Reportf(decl.Pos(), "not sorted alphabetically (error creating diff: %v)", err)
		return
	}

	// 报告问题并附上 diff
	pass.Reportf(decl.Pos(), "not sorted alphabetically (-got, +want):\n%s\n", stripHeader(diffText, 3))
}

func stripHeader(input string, n int) string {
	scanner := bufio.NewScanner(strings.NewReader(input))
	var result strings.Builder
	lineCount := 0

	for scanner.Scan() {
		lineCount++
		if lineCount > n {
			result.WriteString(scanner.Text() + "\n")
		}
	}

	return strings.TrimSuffix(result.String(), "\n")
}

// reportMapSortingIssue 报告 map 键未排序的问题
func reportMapSortingIssue(pass *analysis.Pass, decl *ast.GenDecl, mapName string, current, sorted []Feature) {
	// 生成原始代码
	originalSource := generateMapSourceCode(current)

	// 生成排序后的代码
	sortedSource := generateMapSourceCode(sorted)

	// 生成两者的 unified diff
	diff := difflib.UnifiedDiff{
		A:        difflib.SplitLines(originalSource),
		B:        difflib.SplitLines(sortedSource),
		FromFile: "Current Map Keys",
		ToFile:   "Expected Map Keys",
		Context:  3,
	}

	diffText, err := difflib.GetUnifiedDiffString(diff)
	if err != nil {
		pass.Reportf(decl.Pos(), "map '%s' keys not sorted alphabetically (error creating diff: %v)", mapName, err)
		return
	}

	// 报告问题并附上 diff
	pass.Reportf(decl.Pos(), "map '%s' keys not sorted alphabetically (-got, +want):\n%s\n", mapName, stripHeader(diffText, 3))
}

// generateMapSourceCode 重建 map 键的源代码片段，用于生成 diff
func generateMapSourceCode(features []Feature) string {
	var sb strings.Builder

	sb.WriteString("map[featuregate.Feature]featuregate.VersionedSpecs{\n")

	// 逐个写入 feature 键
	for _, feature := range features {
		sb.WriteString("\t")
		sb.WriteString(feature.Name)
		sb.WriteString(": ...,\n")
	}

	sb.WriteString("}")

	return sb.String()
}

// generateSourceCode 根据 feature 重建源代码片段，用于生成 diff
func generateSourceCode(tokenType token.Token, features []Feature) string {
	var sb strings.Builder

	// 用 var/const 开始声明块
	sb.WriteString(tokenType.String())
	sb.WriteString(" (\n")

	// 逐个写入 feature 及其注释
	for _, feature := range features {
		// 写入注释
		for _, comment := range feature.Comments {
			sb.WriteString("\t")
			sb.WriteString(comment)
			sb.WriteString("\n")
		}

		// 写入 feature 声明
		sb.WriteString("\t")
		sb.WriteString(feature.Name)
		sb.WriteString(" = ")
		// 没有真实值，用占位符代替
		sb.WriteString("value")
		sb.WriteString("\n\n")
	}

	// 闭合声明块
	sb.WriteString(")")

	return sb.String()
}
