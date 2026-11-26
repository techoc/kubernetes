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

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	yaml "go.yaml.in/yaml/v2"

	"github.com/prometheus/client_golang/prometheus/testutil/promlint"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/kubernetes/hack/tools/instrumentation/internal/metric"
)

const (
	kubeMetricImportPath = `"k8s.io/component-base/metrics"`
	// 必须与 kubeMetricImportPath 的末级目录名一致
	kubeMetricsDefaultImportName = "metrics"
)

var (
	// 环境变量配置项
	GOOS                  string = findGOOS()
	ALL_STABILITY_CLASSES bool
	EndpointMappings      string
	LINT                  bool
)

func findGOOS() string {
	cmd := exec.Command("go", "env", "GOOS")
	out, err := cmd.CombinedOutput()
	if err != nil {
		panic(fmt.Sprintf("running `go env` failed: %v\n\n%s", err, string(out)))
	}
	if len(out) == 0 {
		panic("empty result from `go env GOOS`")
	}
	return string(out)
}

func main() {

	flag.BoolVar(&ALL_STABILITY_CLASSES, "allstabilityclasses", false, "use this flag to enable all stability classes")
	flag.StringVar(&EndpointMappings, "endpoint-mappings", "", "path to endpoint mappings configuration file")
	flag.BoolVar(&LINT, "lint", false, "run linter on extracted metrics")
	flag.Parse()
	if len(flag.Args()) < 1 {
		fmt.Fprintf(os.Stderr, "USAGE: %s <DIR or FILE or '-'> [...]\n", os.Args[0])
		os.Exit(64)
	}

	// 若指定则加载组件/端点映射配置
	var endpointConfig *endpointMappingConfig
	if EndpointMappings != "" {
		var err error
		endpointConfig, err = loadEndpointMappingConfig(EndpointMappings)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to load endpoint mappings: %s\n", err)
			os.Exit(1)
		}
	}

	stableMetricNames := map[string]struct{}{}
	stableMetrics := []metric.Metric{}
	errors := []error{}

	addStdin := false
	for _, arg := range flag.Args() {
		if arg == "-" {
			addStdin = true
			continue
		}
		ms, es := searchPathForStableMetrics(arg, endpointConfig)
		for _, m := range ms {
			fqName := m.BuildFQName()
			if _, ok := stableMetricNames[fqName]; !ok {
				stableMetrics = append(stableMetrics, m)
			}
			stableMetricNames[fqName] = struct{}{}
		}
		errors = append(errors, es...)
	}
	if addStdin {
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Split(bufio.ScanLines)
		for scanner.Scan() {
			arg := scanner.Text()
			ms, es := searchPathForStableMetrics(arg, endpointConfig)
			for _, m := range ms {
				fqName := m.BuildFQName()
				if _, ok := stableMetricNames[fqName]; !ok {
					stableMetrics = append(stableMetrics, m)
				}
				stableMetricNames[fqName] = struct{}{}
			}
			errors = append(errors, es...)
		}
	}

	for _, err := range errors {
		fmt.Fprintf(os.Stderr, "%s\n", err)
	}
	if len(errors) != 0 {
		os.Exit(1)
	}
	if len(stableMetrics) == 0 {
		os.Exit(0)
	}
	for i, m := range stableMetrics {
		if m.StabilityLevel == "" {
			m.StabilityLevel = "ALPHA"
		}
		stableMetrics[i] = m
	}
	sort.Sort(metric.ByFQName(stableMetrics))

	if LINT {
		var allProblems []promlint.Problem
		for _, m := range stableMetrics {
			fqName := m.BuildFQName()
			metricType := strings.ToLower(m.Type)
			if metricType == "timingratiohistogram" {
				metricType = "histogram"
			}
			text := fmt.Sprintf("# HELP %s %s\n# TYPE %s %s\n%s 0\n", fqName, m.Help, fqName, metricType, fqName)
			linter := promlint.New(strings.NewReader(text))
			problems, err := linter.Lint()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Error linting %s: %v\n", fqName, err)
				continue
			}
			allProblems = append(allProblems, problems...)
		}

		if err := testutil.GetLintError(allProblems); err != nil {
			fmt.Fprintf(os.Stderr, "Metric lint errors found:\n%v\n", err)
			os.Exit(1)
		}
		if err := testutil.CheckUnusedExceptions(allProblems); err != nil {
			fmt.Fprintf(os.Stderr, "Unused exceptions found:\n%v\nPlease remove them from the exception list in staging/src/k8s.io/component-base/metrics/testutil/promlint.go.\n", err)
			os.Exit(1)
		}
		fmt.Fprintln(os.Stderr, "All metrics passed linter (excluding exceptions and no unused exceptions).")
	}

	data, err := yaml.Marshal(stableMetrics)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err)
		os.Exit(1)
	}

	fmt.Print(string(data))
}

func searchPathForStableMetrics(path string, endpointConfig *endpointMappingConfig) ([]metric.Metric, []error) {
	metrics := []metric.Metric{}
	errors := []error{}
	err := filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if strings.HasPrefix(path, "vendor") {
			return filepath.SkipDir
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}
		ms, es := searchFileForStableMetrics(path, nil, endpointConfig)
		errors = append(errors, es...)
		metrics = append(metrics, ms...)
		return nil
	})
	if err != nil {
		errors = append(errors, err)
	}
	return metrics, errors
}

// searchFileForStableMetrics 接受已有文件名，或源码内容及其来源文件名。
func searchFileForStableMetrics(filename string, src interface{}, endpointConfig *endpointMappingConfig) ([]metric.Metric, []error) {
	fileset := token.NewFileSet()
	tree, err := parser.ParseFile(fileset, filename, src, parser.AllErrors)
	if err != nil {
		return []metric.Metric{}, []error{err}
	}
	metricsImportName, err := getLocalNameOfImportedPackage(tree, kubeMetricImportPath, kubeMetricsDefaultImportName)
	if err != nil {
		return []metric.Metric{}, addFileInformationToErrors([]error{err}, fileset)
	}
	if metricsImportName == "" {
		return []metric.Metric{}, []error{}
	}
	variables := globalVariableDeclarations(tree)

	variables, err = importedGlobalVariableDeclaration(variables, tree.Imports)
	if err != nil {
		return []metric.Metric{}, addFileInformationToErrors([]error{err}, fileset)
	}

	stableMetricsFunctionCalls, errors := findStableMetricDeclaration(tree, metricsImportName)
	metrics, es := decodeMetricCalls(stableMetricsFunctionCalls, metricsImportName, variables)
	errors = append(errors, es...)

	// 有配置时补充组件与端点信息
	if endpointConfig != nil {
		ces := endpointConfig.inferComponentEndpoints(filename)
		if len(ces) > 0 {
			for i := range metrics {
				metrics[i].ComponentEndpoints = ces
			}
		} else if len(metrics) > 0 {
			fmt.Fprintf(os.Stderr, "WARNING: found %d metric(s) in %q but could not infer component endpoints. "+
				"Consider updating endpoint-mappings.yaml.\n", len(metrics), filename)
		}
	}

	return metrics, addFileInformationToErrors(errors, fileset)
}

func getLocalNameOfImportedPackage(tree *ast.File, importPath, defaultImportName string) (string, error) {
	var importName string
	for _, im := range tree.Imports {
		if im.Path.Value == importPath {
			if im.Name == nil {
				importName = defaultImportName
			} else {
				if im.Name.Name == "." {
					return "", newDecodeErrorf(im, errImport)
				}
				importName = im.Name.Name
			}
		}
	}
	return importName, nil
}

func addFileInformationToErrors(es []error, fileset *token.FileSet) []error {
	for i := range es {
		if de, ok := es[i].(*decodeError); ok {
			es[i] = de.errorWithFileInformation(fileset)
		}
	}
	return es
}

func globalVariableDeclarations(tree *ast.File) map[string]ast.Expr {
	consts := make(map[string]ast.Expr)
	for _, d := range tree.Decls {
		if gd, ok := d.(*ast.GenDecl); ok && (gd.Tok == token.CONST || gd.Tok == token.VAR) {
			for _, spec := range gd.Specs {
				if vspec, ok := spec.(*ast.ValueSpec); ok {
					for _, name := range vspec.Names {
						for _, value := range vspec.Values {
							consts[name.Name] = value
						}
					}
				}
			}
		}
	}
	return consts
}

func findPkgDir(pkg string) (string, error) {
	// 借助 Go 的模块机制解析包路径。
	cmd := exec.Command("go", "list", "-find", "-json=Dir", pkg)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("running `go list` failed: %w\n\n%s", err, string(out))
	}
	result := struct {
		Dir string
	}{}
	if err := json.Unmarshal(out, &result); err != nil {
		return "", fmt.Errorf("json unmarshal of `go list` failed: %w", err)
	}
	if result.Dir != "" {
		return result.Dir, nil
	}

	return "", fmt.Errorf("empty respose from `go list`")
}

func importedGlobalVariableDeclaration(localVariables map[string]ast.Expr, imports []*ast.ImportSpec) (map[string]ast.Expr, error) {
	for _, im := range imports {
		// 确定别名
		var importAlias string
		if im.Name == nil {
			pathSegments := strings.Split(im.Path.Value, "/")
			importAlias = strings.Trim(pathSegments[len(pathSegments)-1], "\"")
		} else {
			importAlias = im.Name.String()
		}

		// 找到导入包在磁盘上的路径
		pkg, err := strconv.Unquote(im.Path.Value)
		if err != nil {
			return nil, fmt.Errorf("can't handle import '%s': %w", im.Path.Value, err)
		}
		importDirectory, err := findPkgDir(pkg)
		if err != nil {
			return nil, fmt.Errorf("can't find import '%s': %w", im.Path.Value, err)
		}

		files, err := os.ReadDir(importDirectory)
		if err != nil {
			return nil, fmt.Errorf("failed to read import directory %s: %w", importDirectory, err)
		}

		for _, file := range files {
			if file.IsDir() {
				// 不收集子包中的常量
				continue
			}

			if strings.Contains(file.Name(), "_test") {
				// 不解析测试文件
				continue
			}

			if !strings.HasSuffix(file.Name(), ".go") {
				// 非 Go 源文件，跳过
				continue
			}

			fileset := token.NewFileSet()
			tree, err := parser.ParseFile(fileset, strings.Join([]string{importDirectory, file.Name()}, string(os.PathSeparator)), nil, parser.AllErrors)
			if err != nil {
				return nil, fmt.Errorf("failed to parse path %s with error %w", im.Path.Value, err)
			}

			// 从解析后的文件中提取全局变量
			variables := globalVariableDeclarations(tree)

			// 合并到已有 map，并为键加上导入别名前缀
			for k, v := range variables {
				importK := strings.Join([]string{importAlias, k}, ".")
				if _, ok := localVariables[importK]; !ok {
					localVariables[importK] = v
				} else {
					// 通过构建标签按操作系统选择性编译的跨平台文件
					// 采用与当前 GOOS 匹配的版本

					if strings.Contains(file.Name(), GOOS) {
						// 假定最终能找到该文件当前 OS 对应的版本
						// 若当前 OS 没有专用文件，就可能额外引入本不应包含的常量
						// TODO: 是否应直接依据构建标签决定取舍？
						localVariables[importK] = v
					}

				}
			}
		}

	}

	return localVariables, nil
}
