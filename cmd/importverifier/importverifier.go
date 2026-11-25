/*
Copyright 2017 The Kubernetes Authors.

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
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	yaml "go.yaml.in/yaml/v2"
)

// Package 是 cmd/go.Package 的子集。
type Package struct {
	Dir          string   `yaml:",omitempty"` // 包含包源码的目录
	ImportPath   string   `yaml:",omitempty"` // 包的导入路径
	Imports      []string `yaml:",omitempty"` // 此包使用的导入路径
	TestImports  []string `yaml:",omitempty"` // 来自 TestGoFiles 的导入
	XTestImports []string `yaml:",omitempty"` // 来自 XTestGoFiles 的导入
}

// ImportRestriction 描述一棵源代码树所允许的
// 一组导入树
type ImportRestriction struct {
	// BaseDir 是受此配置约束的包树的根目录，
	// 以相对于仓库根目录的路径表示。
	// 这个目录可能不会被 `go list` 视为包（如果其中
	// 没有 .go 文件）
	BaseDir string `yaml:"baseImportPath"`
	// IgnoredSubTrees 是 BaseDir 下不施加任何导入限制的
	// 子树根，以相对于仓库根目录的路径
	// 表示。
	IgnoredSubTrees []string `yaml:"ignoredSubTrees,omitempty"`
	// AllowedImports 是允许从 BaseDir 导入的包树的根，
	// 以 Go 导入语句中使用的路径
	// 表示。
	AllowedImports []string `yaml:"allowedImports"`
	// ExcludeTests 表示跳过对测试依赖的检查。
	ExcludeTests bool `yaml:"excludeTests"`
}

// ForbiddenImportsFor 根据导入限制确定某个包的
// 所有禁用导入
func (i *ImportRestriction) ForbiddenImportsFor(pkg Package) ([]string, error) {
	if restricted, err := i.isRestrictedDir(pkg.Dir); err != nil {
		return []string{}, err
	} else if !restricted {
		return []string{}, nil
	}

	return i.forbiddenImportsFor(pkg), nil
}

// isRestrictedDir 判断源码目录是否受到
// 此配置的任何限制。
// 满足以下条件的路径将受到限制：
//   - 位于基础导入路径之下
//   - 不位于任何忽略的子树之下
func (i *ImportRestriction) isRestrictedDir(dir string) (bool, error) {
	if under, err := isPathUnder(i.BaseDir, dir); err != nil {
		return false, err
	} else if !under {
		return false, nil
	}

	for _, ignored := range i.IgnoredSubTrees {
		if under, err := isPathUnder(ignored, dir); err != nil {
			return false, err
		} else if under {
			return false, nil
		}
	}

	return true, nil
}

// isPathUnder 判断 path 是否位于 base 之下
func isPathUnder(base, path string) (bool, error) {
	absBase, err := filepath.Abs(base)
	if err != nil {
		return false, err
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return false, err
	}

	relPath, err := filepath.Rel(absBase, absPath)
	if err != nil {
		return false, err
	}

	// 如果 path 位于 base 之下，则从 base 到 path 的相对路径
	// 不会以 `../` 开头
	return !strings.HasPrefix(relPath, ".."), nil
}

// forbiddenImportsFor 根据导入限制确定某个包的
// 所有禁用导入，并返回去重后的
// 列表
func (i *ImportRestriction) forbiddenImportsFor(pkg Package) []string {
	forbiddenImportSet := map[string]struct{}{}
	imports := pkg.Imports
	if !i.ExcludeTests {
		imports = append(imports, append(pkg.TestImports, pkg.XTestImports...)...)
	}
	for _, imp := range imports {
		if i.isForbidden(imp) {
			forbiddenImportSet[imp] = struct{}{}
		}
	}

	var forbiddenImports []string
	for imp := range forbiddenImportSet {
		forbiddenImports = append(forbiddenImports, imp)
	}
	return forbiddenImports
}

// isForbidden 判断某个导入是否被禁用，
// 满足以下条件时返回 true：
//   - 导入的是 rootPackage 下的包
//   - 不属于基础导入路径或其子包
//   - 不属于允许的路径或其子包
func (i *ImportRestriction) isForbidden(imp string) bool {
	importsBelowRoot := strings.HasPrefix(imp, rootPackage)
	importsBelowBase := strings.HasPrefix(imp, i.BaseDir)
	importsAllowed := false
	for _, allowed := range i.AllowedImports {
		exactlyImportsAllowed := imp == allowed
		importsBelowAllowed := strings.HasPrefix(imp, fmt.Sprintf("%s/", allowed))
		importsAllowed = importsAllowed || (importsBelowAllowed || exactlyImportsAllowed)
	}

	return importsBelowRoot && !importsBelowBase && !importsAllowed
}

var rootPackage string

func main() {
	if len(os.Args) != 3 {
		log.Fatalf("Usage: %s <root> <restrictions.yaml>", os.Args[0])
	}

	rootPackage = os.Args[1]
	configFile := os.Args[2]
	importRestrictions, err := loadImportRestrictions(configFile)
	if err != nil {
		log.Fatalf("Failed to load import restrictions: %v", err)
	}

	foundForbiddenImports := false
	for _, restriction := range importRestrictions {
		baseDir := restriction.BaseDir
		if filepath.IsAbs(baseDir) {
			log.Fatalf("%q appears to be an absolute path", baseDir)
		}
		if !strings.HasPrefix(baseDir, "./") {
			baseDir = "./" + baseDir
		}
		baseDir = strings.TrimRight(baseDir, "/")
		log.Printf("Inspecting imports under %s/...\n", baseDir)

		packages, err := resolvePackageTree(baseDir)
		if err != nil {
			log.Fatalf("Failed to resolve package tree: %v", err)
		} else if len(packages) == 0 {
			log.Fatalf("Found no packages under tree %s", baseDir)
		}

		log.Printf("- validating imports for %d packages", len(packages))
		restrictionViolated := false
		for _, pkg := range packages {
			if forbidden, err := restriction.ForbiddenImportsFor(pkg); err != nil {
				log.Fatalf("-- failed to validate imports: %v", err)
			} else if len(forbidden) != 0 {
				logForbiddenPackages(pkg.ImportPath, forbidden)
				restrictionViolated = true
			}
		}
		if restrictionViolated {
			foundForbiddenImports = true
			log.Println("- FAIL")
		} else {
			log.Println("- OK")
		}
	}

	if foundForbiddenImports {
		os.Exit(1)
	}
}

func loadImportRestrictions(configFile string) ([]ImportRestriction, error) {
	config, err := os.ReadFile(configFile)
	if err != nil {
		return nil, fmt.Errorf("failed to load configuration from %s: %v", configFile, err)
	}

	var importRestrictions []ImportRestriction
	if err := yaml.Unmarshal(config, &importRestrictions); err != nil {
		return nil, fmt.Errorf("failed to unmarshal from %s: %v", configFile, err)
	}

	return importRestrictions, nil
}

func resolvePackageTree(treeBase string) ([]Package, error) {
	cmd := "go"
	args := []string{"list", "-json", fmt.Sprintf("%s/...", treeBase)}
	c := exec.Command(cmd, args...)
	stdout, err := c.Output()
	if err != nil {
		var message string
		if ee, ok := err.(*exec.ExitError); ok {
			message = fmt.Sprintf("%v\n%v", ee, string(ee.Stderr))
		} else {
			message = fmt.Sprintf("%v", err)
		}
		return nil, fmt.Errorf("failed to run `%s %s`: %v", cmd, strings.Join(args, " "), message)
	}

	packages, err := decodePackages(bytes.NewReader(stdout))
	if err != nil {
		return nil, fmt.Errorf("failed to decode packages: %v", err)
	}

	return packages, nil
}

func decodePackages(r io.Reader) ([]Package, error) {
	// `go list -json` 会拼接多个包定义，
	// 而不是输出单个合法 JSON，因此我们需要
	// 流式解析输出，从中解码出所需数据，
	// 而不能直接对标准输出使用简单的 JSON
	// 解码器
	var packages []Package
	decoder := json.NewDecoder(r)
	for decoder.More() {
		var pkg Package
		if err := decoder.Decode(&pkg); err != nil {
			return nil, fmt.Errorf("invalid package: %v", err)
		}
		packages = append(packages, pkg)
	}

	return packages, nil
}

func logForbiddenPackages(base string, forbidden []string) {
	log.Printf("-- found forbidden imports for %s:\n", base)
	for _, forbiddenPackage := range forbidden {
		log.Printf("--- %s\n", forbiddenPackage)
	}
}
