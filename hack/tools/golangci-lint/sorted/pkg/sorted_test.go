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

package pkg

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/pmezard/go-difflib/difflib"
	"golang.org/x/tools/go/analysis"
)

func TestNewAnalyzer(t *testing.T) {
	analyzer := NewAnalyzer()
	if analyzer.Name != "sorted" {
		t.Errorf("Expected analyzer name to be 'sorted', got '%s'", analyzer.Name)
	}
}

func TestNewAnalyzerWithConfig(t *testing.T) {
	config := Config{
		Debug: true,
		Files: []string{"test.go"},
	}
	analyzer := NewAnalyzerWithConfig(config)
	if analyzer.Name != "sorted" {
		t.Errorf("Expected analyzer name to be 'sorted', got '%s'", analyzer.Name)
	}
}

func TestRunWithEmptyFileList(t *testing.T) {
	// 验证 analyzer 在文件列表为空时能正常处理
	pass := &analysis.Pass{
		Fset:     token.NewFileSet(),
		Files:    []*ast.File{},
		ResultOf: make(map[*analysis.Analyzer]interface{}),
		Report:   func(d analysis.Diagnostic) { t.Errorf("Report should not be called") },
	}

	// 直接调用 analyzer 的 run 函数
	result, err := run(pass, Config{})
	if err != nil {
		t.Errorf("run returned an error: %v", err)
	}
	if result != nil {
		t.Errorf("Expected nil result, got: %v", result)
	}
}

func TestRunWithDefaultFiles(t *testing.T) {
	// 验证 analyzer 使用默认文件列表时能正常工作
	// 构造模拟的 analysis.Pass
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "pkg/features/kube_features.go", `
package features

type Feature string

const (
	FeatureA Feature = "FeatureA"
	FeatureB Feature = "FeatureB"
)
`, parser.ParseComments)
	if err != nil {
		t.Fatalf("Failed to parse test file: %v", err)
	}

	pass := &analysis.Pass{
		Fset:     fset,
		Files:    []*ast.File{f},
		ResultOf: make(map[*analysis.Analyzer]interface{}),
		Report:   func(d analysis.Diagnostic) {},
	}

	// 直接调用 analyzer 的 run 函数
	_, err = run(pass, Config{})
	if err != nil {
		t.Errorf("run returned an error: %v", err)
	}
}

func TestRunWithNonTargetFile(t *testing.T) {
	// 验证 analyzer 会忽略非目标文件
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "some/random/file.go", `
package features

type Feature string

const (
	FeatureB Feature = "FeatureB"
	FeatureA Feature = "FeatureA"  // Unsorted, but should be ignored
)
`, parser.ParseComments)
	if err != nil {
		t.Fatalf("Failed to parse test file: %v", err)
	}

	var reportCalled bool
	pass := &analysis.Pass{
		Fset:     fset,
		Files:    []*ast.File{f},
		ResultOf: make(map[*analysis.Analyzer]interface{}),
		Report:   func(d analysis.Diagnostic) { reportCalled = true },
	}

	// 直接调用 analyzer 的 run 函数
	_, err = run(pass, Config{})
	if err != nil {
		t.Errorf("run returned an error: %v", err)
	}

	// 断言非目标文件未触发 Report
	if reportCalled {
		t.Errorf("Report should not be called for non-target files")
	}
}

func TestRunWithSpecifiedFiles(t *testing.T) {
	// 创建测试用的临时文件
	tmpFile, err := os.CreateTemp("", "custom_file_*.go")
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	// 写入测试源码
	testContent := `
package features

type Feature string

const (
	FeatureA Feature = "FeatureA"
	FeatureB Feature = "FeatureB"
)
`
	if _, err := tmpFile.Write([]byte(testContent)); err != nil {
		t.Fatalf("Failed to write to temporary file: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("Failed to close temporary file: %v", err)
	}

	// 验证 analyzer 使用指定文件列表时能正常工作
	config := Config{
		Files: []string{tmpFile.Name()},
	}

	// 构造模拟的 analysis.Pass
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, tmpFile.Name(), testContent, parser.ParseComments)
	if err != nil {
		t.Fatalf("Failed to parse test file: %v", err)
	}

	pass := &analysis.Pass{
		Fset:     fset,
		Files:    []*ast.File{f},
		ResultOf: make(map[*analysis.Analyzer]interface{}),
		Report:   func(d analysis.Diagnostic) {},
	}

	// 直接调用 analyzer 的 run 函数
	_, err = run(pass, config)
	if err != nil {
		t.Errorf("run returned an error: %v", err)
	}
}

func TestExtractFeatures(t *testing.T) {
	src := `
package test

type Feature string

const (
	// Comment for FeatureA
	FeatureA Feature = "FeatureA"
	
	// Comment for FeatureB
	FeatureB Feature = "FeatureB"
)
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "test.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("Failed to parse test file: %v", err)
	}

	var foundDecl *ast.GenDecl
	ast.Inspect(f, func(n ast.Node) bool {
		if decl, ok := n.(*ast.GenDecl); ok && decl.Tok == token.CONST {
			foundDecl = decl
			return false
		}
		return true
	})

	if foundDecl == nil {
		t.Fatalf("Failed to find const declaration")
	}

	features := extractFeatures(foundDecl, f.Comments)

	if len(features) != 2 {
		t.Errorf("Expected 2 features, got %d", len(features))
	}

	if features[0].Name != "FeatureA" {
		t.Errorf("Expected first feature to be FeatureA, got %s", features[0].Name)
	}

	if features[1].Name != "FeatureB" {
		t.Errorf("Expected second feature to be FeatureB, got %s", features[1].Name)
	}

	if len(features[0].Comments) == 0 {
		t.Errorf("Expected comments for FeatureA, got none")
	}
}

func TestSortFeatures(t *testing.T) {
	features := []Feature{
		{Name: "FeatureB"},
		{Name: "FeatureA"},
		{Name: "FeatureC"},
	}

	sorted := sortFeatures(features)

	if sorted[0].Name != "FeatureA" {
		t.Errorf("Expected first feature to be FeatureA, got %s", sorted[0].Name)
	}

	if sorted[1].Name != "FeatureB" {
		t.Errorf("Expected second feature to be FeatureB, got %s", sorted[1].Name)
	}

	if sorted[2].Name != "FeatureC" {
		t.Errorf("Expected third feature to be FeatureC, got %s", sorted[2].Name)
	}
}

func TestHasOrderChanged(t *testing.T) {
	original := []Feature{
		{Name: "FeatureB"},
		{Name: "FeatureA"},
		{Name: "FeatureC"},
	}

	sorted := []Feature{
		{Name: "FeatureA"},
		{Name: "FeatureB"},
		{Name: "FeatureC"},
	}

	if !hasOrderChanged(original, sorted) {
		t.Errorf("Expected hasOrderChanged to return true for different orders")
	}

	if hasOrderChanged(sorted, sorted) {
		t.Errorf("Expected hasOrderChanged to return false for same order")
	}
}

func TestReportSortingIssue(t *testing.T) {
	current := []Feature{
		{Name: "FeatureB", Comments: []string{"// Comment for FeatureB"}},
		{Name: "FeatureA", Comments: []string{"// Comment for FeatureA"}},
		{Name: "FeatureC", Comments: []string{"// Comment for FeatureC"}},
	}

	sorted := []Feature{
		{Name: "FeatureA", Comments: []string{"// Comment for FeatureA"}},
		{Name: "FeatureB", Comments: []string{"// Comment for FeatureB"}},
		{Name: "FeatureC", Comments: []string{"// Comment for FeatureC"}},
	}

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "test.go", `
package test

const (
	FeatureB = "FeatureB"
	FeatureA = "FeatureA"
	FeatureC = "FeatureC"
)
`, parser.ParseComments)
	if err != nil {
		t.Fatalf("Failed to parse test file: %v", err)
	}

	var foundDecl *ast.GenDecl
	ast.Inspect(f, func(n ast.Node) bool {
		if decl, ok := n.(*ast.GenDecl); ok && decl.Tok == token.CONST {
			foundDecl = decl
			return false
		}
		return true
	})

	if foundDecl == nil {
		t.Fatalf("Failed to find const declaration")
	}

	var reportMessage string
	pass := &analysis.Pass{
		Fset:     fset,
		Files:    []*ast.File{f},
		ResultOf: make(map[*analysis.Analyzer]interface{}),
		Report:   func(d analysis.Diagnostic) { reportMessage = d.Message },
	}

	reportSortingIssue(pass, foundDecl, current, sorted)

	if reportMessage == "" {
		t.Errorf("Expected Report to be called")
	}

	// 断言 diff 中 feature 的顺序正确
	if !strings.Contains(reportMessage, "FeatureB") || !strings.Contains(reportMessage, "FeatureA") || !strings.Contains(reportMessage, "FeatureC") {
		t.Errorf("Expected diff to contain all feature names, got: %s", reportMessage)
	}

	// 断言 diff 包含预期内容
	expectedContent := "const ("
	if !strings.Contains(reportMessage, expectedContent) {
		t.Errorf("Expected diff to contain %q, got: %s", expectedContent, reportMessage)
	}
}

func TestSortedFeatures(t *testing.T) {
	// 测试已正确排序的 feature
	src := `
package test

type Feature string

const (
	// These are properly sorted
	FeatureA Feature = "FeatureA"
	FeatureB Feature = "FeatureB"
	FeatureC Feature = "FeatureC"
)
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "test.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("Failed to parse test file: %v", err)
	}

	var foundDecl *ast.GenDecl
	ast.Inspect(f, func(n ast.Node) bool {
		if decl, ok := n.(*ast.GenDecl); ok && decl.Tok == token.CONST {
			foundDecl = decl
			return false
		}
		return true
	})

	if foundDecl == nil {
		t.Fatalf("Failed to find const declaration")
	}

	// 提取 feature
	features := extractFeatures(foundDecl, f.Comments)

	// 排序 feature
	sortedFeatures := sortFeatures(features)

	// 检查顺序是否变化
	if hasOrderChanged(features, sortedFeatures) {
		t.Errorf("Expected features to be already sorted")
	}
}

func TestUnsortedFeatures(t *testing.T) {
	// 测试未排序的 feature
	src := `
package test

type Feature string

const (
	// These are NOT properly sorted
	FeatureB Feature = "FeatureB"
	FeatureA Feature = "FeatureA"
	FeatureC Feature = "FeatureC"
)
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "test.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("Failed to parse test file: %v", err)
	}

	var foundDecl *ast.GenDecl
	ast.Inspect(f, func(n ast.Node) bool {
		if decl, ok := n.(*ast.GenDecl); ok && decl.Tok == token.CONST {
			foundDecl = decl
			return false
		}
		return true
	})

	if foundDecl == nil {
		t.Fatalf("Failed to find const declaration")
	}

	// 提取 feature
	features := extractFeatures(foundDecl, f.Comments)

	// 排序 feature
	sortedFeatures := sortFeatures(features)

	// 检查顺序是否变化
	if !hasOrderChanged(features, sortedFeatures) {
		t.Errorf("Expected features to be unsorted")
	}
}

func TestVarBlockFeatures(t *testing.T) {
	// 测试 var 块中的 feature（如 test/e2e/feature/feature.go）
	src := `
package test

import "k8s.io/kubernetes/test/e2e/framework"

var (
	// These are properly sorted
	FeatureA = framework.WithFeature(framework.ValidFeatures.Add("FeatureA"))
	FeatureB = framework.WithFeature(framework.ValidFeatures.Add("FeatureB"))
	FeatureC = framework.WithFeature(framework.ValidFeatures.Add("FeatureC"))
)
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "test.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("Failed to parse test file: %v", err)
	}

	var foundDecl *ast.GenDecl
	ast.Inspect(f, func(n ast.Node) bool {
		if decl, ok := n.(*ast.GenDecl); ok && decl.Tok == token.VAR {
			foundDecl = decl
			return false
		}
		return true
	})

	if foundDecl == nil {
		t.Fatalf("Failed to find var declaration")
	}

	// 提取 feature
	features := extractFeatures(foundDecl, f.Comments)

	// 排序 feature
	sortedFeatures := sortFeatures(features)

	// 检查顺序是否变化
	if hasOrderChanged(features, sortedFeatures) {
		t.Errorf("Expected features to be already sorted")
	}
}

func TestSingleItemBlock(t *testing.T) {
	// 测试声明块中只有一个条目的情况
	src := `
package test

type Feature string

const (
	// Single item, should be skipped
	FeatureA Feature = "FeatureA"
)
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "test.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("Failed to parse test file: %v", err)
	}

	var foundDecl *ast.GenDecl
	ast.Inspect(f, func(n ast.Node) bool {
		if decl, ok := n.(*ast.GenDecl); ok && decl.Tok == token.CONST {
			foundDecl = decl
			return false
		}
		return true
	})

	if foundDecl == nil {
		t.Fatalf("Failed to find const declaration")
	}

	// 提取 feature
	features := extractFeatures(foundDecl, f.Comments)

	if len(features) != 1 {
		t.Errorf("Expected 1 feature, got %d", len(features))
	}
}

func TestNonParenthesizedDeclarationsNotProcessed(t *testing.T) {
	// 测试未使用括号的声明
	src := `
package test

type Feature string

// First feature
const FeatureA Feature = "FeatureA"

// Second feature
const FeatureB Feature = "FeatureB"

// Third feature
const FeatureC Feature = "FeatureC"
`
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "test.go", src, parser.ParseComments)
	if err != nil {
		t.Fatalf("Failed to parse test file: %v", err)
	}

	// 构造模拟的 analysis.Pass
	var reportCalled bool
	pass := &analysis.Pass{
		Fset:     fset,
		Files:    []*ast.File{f},
		ResultOf: make(map[*analysis.Analyzer]interface{}),
		Report:   func(d analysis.Diagnostic) { reportCalled = true },
	}

	// Configure the analyzer to treat test.go as a target file
	config := Config{
		Files: []string{"test.go"},
	}

	// Run the analyzer
	_, err = run(pass, config)
	if err != nil {
		t.Errorf("run returned an error: %v", err)
	}

	// Check that Report was NOT called, since non-parenthesized declarations are skipped
	if reportCalled {
		t.Errorf("Expected Report not to be called for non-parenthesized declarations")
	}
}

// TestAnalyzerRunSimulatingGolangciLint 模拟 golangci-lint 运行 analyzer 的方式：
// 构造模拟的 analysis.Pass 并直接调用 Run。从仓库根目录运行时，
// 会检查 analyzer 配置中的所有默认目标文件，无需单独启动 golangci-lint；
// 从其他目录运行时，因找不到文件而跳过该测试。
func TestAnalyzerRunSimulatingGolangciLint(t *testing.T) {
	defaultTargetFiles := []string{
		"pkg/features/kube_features.go",
		"staging/src/k8s.io/apiserver/pkg/features/kube_features.go",
		"staging/src/k8s.io/client-go/features/known_features.go",
		"staging/src/k8s.io/controller-manager/pkg/features/kube_features.go",
		"staging/src/k8s.io/apiextensions-apiserver/pkg/features/kube_features.go",
		"test/e2e/feature/feature.go",
		"test/e2e/environment/environment.go",
	}
	for _, filename := range defaultTargetFiles {
		t.Run(filename, func(t *testing.T) {

			// 检查文件是否存在
			if _, err := os.Stat(filename); os.IsNotExist(err) {
				t.Skip("File not found; skipping test")
			}

			// 创建文件集并解析简单源码
			fset := token.NewFileSet()
			f, err := parser.ParseFile(fset, filename, nil, parser.ParseComments)
			if err != nil {
				t.Fatalf("Failed to parse test file: %v", err)
			}

			// 构造与 golangci-lint 所创建对象类似的 analysis.Pass
			var diagnostics []analysis.Diagnostic
			pass := &analysis.Pass{
				Fset:     fset,
				Files:    []*ast.File{f},
				ResultOf: make(map[*analysis.Analyzer]interface{}),
				Report: func(d analysis.Diagnostic) {
					diagnostics = append(diagnostics, d)
				},
			}

			// 获取 analyzer
			analyzer := NewAnalyzerWithConfig(Config{Debug: true, Files: defaultTargetFiles})

			// 像 golangci-lint 一样直接调用 Run
			result, err := analyzer.Run(pass)

			// 断言没有返回错误
			if err != nil {
				t.Errorf("Analyzer Run returned an error: %v", err)
			}

			// 断言结果为 nil（该 analyzer 不返回结果）
			if result != nil {
				t.Errorf("Expected nil result for %s, got: %v", filename, result)
			}

			// 断言正确排序的 feature 不产生诊断
			if len(diagnostics) > 0 {
				t.Errorf("Expected no diagnostics for %s, got: %v", filename, diagnostics)
			}
		})
	}
}

func TestDebugLogging(t *testing.T) {
	// 该测试不校验调试输出，只确保覆盖对应代码路径
	config := Config{
		Debug: true,
	}

	// 构造模拟的 analysis.Pass
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "pkg/features/kube_features.go", `
package features

type Feature string

const (
	FeatureA Feature = "FeatureA"
	FeatureB Feature = "FeatureB"
)
`, parser.ParseComments)
	if err != nil {
		t.Fatalf("Failed to parse test file: %v", err)
	}

	pass := &analysis.Pass{
		Fset:     fset,
		Files:    []*ast.File{f},
		ResultOf: make(map[*analysis.Analyzer]interface{}),
		Report:   func(d analysis.Diagnostic) {},
	}

	// 在启用调试的情况下直接调用 analyzer 的 run 函数
	_, err = run(pass, config)
	if err != nil {
		t.Errorf("run returned an error: %v", err)
	}
}

func TestGenerateSourceCode(t *testing.T) {
	features := []Feature{
		{Name: "FeatureB", Comments: []string{"// Comment for FeatureB"}},
		{Name: "FeatureA", Comments: []string{"// Comment for FeatureA"}},
	}

	// 测试 const 块代码生成
	constSource := generateSourceCode(token.CONST, features)
	expectedConstSource := `const (
	// Comment for FeatureB
	FeatureB = value

	// Comment for FeatureA
	FeatureA = value

)`

	if constSource != expectedConstSource {
		t.Errorf("Expected const source:\n%s\n\nGot:\n%s", expectedConstSource, constSource)
	}

	// 测试 var 块代码生成
	varSource := generateSourceCode(token.VAR, features)
	expectedVarSource := `var (
	// Comment for FeatureB
	FeatureB = value

	// Comment for FeatureA
	FeatureA = value

)`

	if varSource != expectedVarSource {
		t.Errorf("Expected var source:\n%s\n\nGot:\n%s", expectedVarSource, varSource)
	}
}

func TestDiffGeneration(t *testing.T) {
	// 构造未排序的 feature
	unsorted := []Feature{
		{Name: "FeatureC", Comments: []string{"// Comment for FeatureC"}},
		{Name: "FeatureA", Comments: []string{"// Comment for FeatureA"}},
		{Name: "FeatureB", Comments: []string{"// Comment for FeatureB"}},
	}

	// 构造已排序的 feature
	sorted := []Feature{
		{Name: "FeatureA", Comments: []string{"// Comment for FeatureA"}},
		{Name: "FeatureB", Comments: []string{"// Comment for FeatureB"}},
		{Name: "FeatureC", Comments: []string{"// Comment for FeatureC"}},
	}

	// 分别生成源代码
	originalSource := generateSourceCode(token.CONST, unsorted)
	sortedSource := generateSourceCode(token.CONST, sorted)

	// 创建 unified diff
	diff := difflib.UnifiedDiff{
		A:        difflib.SplitLines(originalSource),
		B:        difflib.SplitLines(sortedSource),
		FromFile: "Current",
		ToFile:   "Expected",
		Context:  3,
	}

	diffText, err := difflib.GetUnifiedDiffString(diff)
	if err != nil {
		t.Fatalf("Failed to generate diff: %v", err)
	}

	// 去掉头部，与实际实现保持一致
	diffText = stripHeader(diffText, 3)

	// 断言 diff 包含预期内容
	expectedLines := []string{
		"-\t// Comment for FeatureC",
		"-\tFeatureC = value",
	}

	for _, line := range expectedLines {
		if !strings.Contains(diffText, line) {
			t.Errorf("Expected diff to contain line: %q, but it was not found in:\n%s", line, diffText)
		}
	}

	// 断言 diff 正确反映了顺序变化
	if !strings.Contains(diffText, "FeatureA") || !strings.Contains(diffText, "FeatureB") || !strings.Contains(diffText, "FeatureC") {
		t.Errorf("Expected diff to contain all feature names, got: %s", diffText)
	}
}
