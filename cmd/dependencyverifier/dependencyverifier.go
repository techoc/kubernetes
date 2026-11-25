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

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/exec"
	"sort"
	"strings"

	"github.com/google/go-cmp/cmp" //nolint:depguard
)

type Unwanted struct {
	// 我们希望停止引用的内容
	Spec UnwantedSpec `json:"spec"`
	// 不期望依赖项的当前状态
	Status UnwantedStatus `json:"status"`
}

type UnwantedSpec struct {
	// 不希望依赖的模块名，映射到可选的原因说明
	UnwantedModules map[string]string `json:"unwantedModules"`
	// 版本不应被更新的模块名，映射到包含版本和原因的结构体
	PinnedModules map[string]PinnedModule `json:"pinnedModules"`
}

type PinnedModule struct {
	Version string `json:"Version"`
	Reason  string `json:"Reason"`
}

type UnwantedStatus struct {
	// 根据 `go mod graph` 的内容记录对 spec.unwantedModules 列表中模块的引用。
	// 减少该列表中的条目是好事，有时需要 upstream 配合才能做到。
	UnwantedReferences map[string][]string `json:"unwantedReferences"`
	// spec.unwantedModules 列表中已被 vendor 的模块
	UnwantedVendored []string `json:"unwantedVendored"`
}

// runCommand 执行 cmd 并返回合并后的 stdout 和 stderr，
// 命令失败时返回错误。
func runCommand(cmd ...string) (string, error) {
	return runCommandInDir("", cmd)
}

func runCommandInDir(dir string, cmd []string) (string, error) {
	c := exec.Command(cmd[0], cmd[1:]...)
	c.Dir = dir
	output, err := c.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("failed to run %q: %s (%s)", strings.Join(cmd, " "), err, output)
	}
	return string(output), nil
}

func readFile(path string) (string, error) {
	content, err := os.ReadFile(path)
	// 将 []byte 转换为字符串并返回（供后续打印）
	return string(content), err
}

func moduleInSlice(a module, list []module, matchVersion bool) bool {
	for _, b := range list {
		if b == a {
			return true
		}
		if !matchVersion && b.name == a.name {
			return true
		}
	}
	return false
}

// 将 `go mod graph` 的输出 modStr 转换为 from->[]to 引用映射和主模块列表
func convertToMap(modStr string) ([]module, map[module][]module) {
	var (
		mainModulesList = []module{}
		mainModules     = map[module]bool{}
	)
	modMap := make(map[module][]module)
	for _, line := range strings.Split(modStr, "\n") {
		if len(line) == 0 {
			continue
		}
		deps := strings.Split(line, " ")
		if len(deps) == 2 {
			first := parseModule(deps[0])
			second := parseModule(deps[1])
			if first.version == "" || first.version == "v0.0.0" {
				if !mainModules[first] {
					mainModules[first] = true
					mainModulesList = append(mainModulesList, first)
				}
			}
			modMap[first] = append(modMap[first], second)
		} else {
			// 跳过无效行
			log.Printf("!!!invalid line in mod.graph: %s", line)
			continue
		}
	}
	return mainModulesList, modMap
}

// difference 以排序后的列表形式返回 a-b 和 b-a
func difference(a, b []string) ([]string, []string) {
	aMinusB := map[string]bool{}
	bMinusA := map[string]bool{}
	for _, dependency := range a {
		aMinusB[dependency] = true
	}
	for _, dependency := range b {
		if _, found := aMinusB[dependency]; found {
			delete(aMinusB, dependency)
		} else {
			bMinusA[dependency] = true
		}
	}
	aMinusBList := []string{}
	bMinusAList := []string{}
	for dependency := range aMinusB {
		aMinusBList = append(aMinusBList, dependency)
	}
	for dependency := range bMinusA {
		bMinusAList = append(bMinusAList, dependency)
	}
	sort.Strings(aMinusBList)
	sort.Strings(bMinusAList)
	return aMinusBList, bMinusAList
}

type module struct {
	name    string
	version string
}

func (m module) String() string {
	if len(m.version) == 0 {
		return m.name
	}
	return m.name + "@" + m.version
}

func parseModule(s string) module {
	if !strings.Contains(s, "@") {
		return module{name: s}
	}
	parts := strings.SplitN(s, "@", 2)
	return module{name: parts[0], version: parts[1]}
}

// option1: dependencyverifier dependencies.json
// 它会运行 `go mod graph` 并进行检查。
func main() {
	var modeGraphStr string
	var err error
	if len(os.Args) == 2 {
		// 运行 `go mod graph`
		modeGraphStr, err = runCommand("go", "mod", "graph")
		if err != nil {
			log.Fatalf("Error running 'go mod graph': %s", err)
		}
	} else {
		log.Fatalf("Usage: %s dependencies.json", os.Args[0])
	}

	dependenciesJSONPath := string(os.Args[1])
	dependencies, err := readFile(dependenciesJSONPath)
	if err != nil {
		log.Fatalf("Error reading dependencies file %s: %s", dependencies, err)
	}

	// 从 JSON 文件加载 Unwanted 配置
	configFromFile := &Unwanted{}
	decoder := json.NewDecoder(bytes.NewBuffer([]byte(dependencies)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(configFromFile); err != nil {
		log.Fatalf("Error reading dependencies file %s: %s", dependenciesJSONPath, err)
	}

	// 把 `go mod graph` 的结果转换为主模块及 from->[]to 引用映射
	mainModules, moduleGraph := convertToMap(modeGraphStr)

	directDependencies := map[string]map[string]bool{}
	for _, mainModule := range mainModules {
		dir := ""
		if mainModule.name != "k8s.io/kubernetes" {
			dir = "staging/src/" + mainModule.name
		}
		listOutput, err := runCommandInDir(dir, []string{"go", "list", "-m", "-f", "{{if not .Indirect}}{{if not .Main}}{{.Path}}{{end}}{{end}}", "all"})
		if err != nil {
			log.Fatalf("Error running 'go list' for %s: %s", mainModule.name, err)
		}
		directDependencies[mainModule.name] = map[string]bool{}
		for _, directDependency := range strings.Split(listOutput, "\n") {
			directDependencies[mainModule.name][directDependency] = true
		}
	}

	// 通过主模块要求的版本收集实际生效的版本
	effectiveVersions := map[string]module{}
	for _, mainModule := range mainModules {
		for _, override := range moduleGraph[mainModule] {
			if _, ok := effectiveVersions[override.name]; !ok {
				effectiveVersions[override.name] = override
			}
		}
	}

	// 检查已被更新的固定版本模块
	pinnedModuleViolations := map[string][]string{}
	if len(configFromFile.Spec.PinnedModules) > 0 {
		// 获取固定版本模块的当前版本
		for pinnedModule, pinnedInfo := range configFromFile.Spec.PinnedModules {
			// 检查该模块是否出现在 effectiveVersions 中
			if effectiveModule, ok := effectiveVersions[pinnedModule]; ok {
				// 与 JSON 文件中固定的版本比较
				if effectiveModule.version != pinnedInfo.Version {
					pinnedModuleViolations[pinnedModule] = []string{
						fmt.Sprintf("Pinned version: %s", pinnedInfo.Version),
						fmt.Sprintf("Attempted update to: %s", effectiveModule.version),
						fmt.Sprintf("Reason for pinning: %s", pinnedInfo.Reason),
					}
				}
			}
		}
	}

	unwantedToReferencers := map[string][]module{}
	for _, mainModule := range mainModules {
		// 遍历依赖图，找出仍被主模块引用的不期望模块
		visit(func(m module, via []module) {
			if _, unwanted := configFromFile.Spec.UnwantedModules[m.name]; unwanted {
				// 这是不期望的依赖，记录引用它的模块
				referencer := via[len(via)-1]
				if !moduleInSlice(referencer, unwantedToReferencers[m.name], false) {
					// // uncomment to get a detailed tree of the path that referenced the unwanted dependency
					//
					// i := 0
					// for _, v := range via {
					// 	if v.version != "" && v.version != "v0.0.0" {
					// 		fmt.Println(strings.Repeat("  ", i), v)
					// 		i++
					// 	}
					// }
					// if i > 0 {
					// 	fmt.Println(strings.Repeat("  ", i+1), m)
					// 	fmt.Println()
					// }
					unwantedToReferencers[m.name] = append(unwantedToReferencers[m.name], referencer)
				}
			}
		}, mainModule, moduleGraph, effectiveVersions)
	}

	config := &Unwanted{}
	config.Spec.UnwantedModules = configFromFile.Spec.UnwantedModules
	for unwanted := range unwantedToReferencers {
		if config.Status.UnwantedReferences == nil {
			config.Status.UnwantedReferences = map[string][]string{}
		}
		sort.Slice(unwantedToReferencers[unwanted], func(i, j int) bool {
			ri := unwantedToReferencers[unwanted][i]
			rj := unwantedToReferencers[unwanted][j]
			if ri.name != rj.name {
				return ri.name < rj.name
			}
			return ri.version < rj.version
		})
		for _, referencer := range unwantedToReferencers[unwanted] {
			// 确保哪怕只有一条引用，也会体现在非 nil 的状态中
			if config.Status.UnwantedReferences == nil {
				config.Status.UnwantedReferences[unwanted] = []string{}
			}
			// 记录带版本号的引用方名称
			if referencer.version != "" && referencer.version != "v0.0.0" {
				config.Status.UnwantedReferences[unwanted] = append(config.Status.UnwantedReferences[unwanted], referencer.name)
			} else if directDependencies[referencer.name][unwanted] {
				config.Status.UnwantedReferences[unwanted] = append(config.Status.UnwantedReferences[unwanted], referencer.name)
			}
		}
	}

	vendorModulesTxt, err := os.ReadFile("vendor/modules.txt")
	if err != nil {
		log.Fatal(err)
	}
	vendoredModules := map[string]bool{}
	for _, l := range strings.Split(string(vendorModulesTxt), "\n") {
		parts := strings.Split(l, " ")
		if len(parts) == 3 && parts[0] == "#" && strings.HasPrefix(parts[2], "v") {
			vendoredModules[parts[1]] = true
		}
	}
	config.Status.UnwantedVendored = []string{}
	for unwanted := range configFromFile.Spec.UnwantedModules {
		if vendoredModules[unwanted] {
			config.Status.UnwantedVendored = append(config.Status.UnwantedVendored, unwanted)
		}
	}
	sort.Strings(config.Status.UnwantedVendored)

	needUpdate := false

	// 将 unwanted-dependencies.json 中的不期望列表与 `go mod graph` 得到的当前状态比较
	expected, err := json.MarshalIndent(configFromFile.Status, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	actual, err := json.MarshalIndent(config.Status, "", "  ")
	if err != nil {
		log.Fatal(err)
	}
	if !bytes.Equal(expected, actual) {
		log.Printf("Expected status of\n%s", string(expected))
		log.Printf("Got status of\n%s", string(actual))
		needUpdate = true
		log.Print("Status diff:\n", cmp.Diff(expected, actual))
	}
	for expectedRef, expectedFrom := range configFromFile.Status.UnwantedReferences {
		actualFrom, ok := config.Status.UnwantedReferences[expectedRef]
		if !ok {
			// 已完全消失
			log.Printf("Good news! Unwanted dependency %q is no longer referenced. Remove status.unwantedReferences[%q] in %s to ensure it doesn't get reintroduced.", expectedRef, expectedRef, dependenciesJSONPath)
			needUpdate = true
			continue
		}
		removedReferences, unwantedReferences := difference(expectedFrom, actualFrom)
		if len(removedReferences) > 0 {
			log.Printf("Good news! Unwanted module %q dropped the following dependants:", expectedRef)
			for _, reference := range removedReferences {
				log.Printf("   %s", reference)
			}
			log.Printf("!!! Remove those from status.unwantedReferences[%q] in %s to ensure they don't get reintroduced.", expectedRef, dependenciesJSONPath)
			needUpdate = true
		}
		if len(unwantedReferences) > 0 {
			log.Printf("Unwanted module %q marked in %s is referenced by new dependants:", expectedRef, dependenciesJSONPath)
			for _, reference := range unwantedReferences {
				log.Printf("   %s", reference)
			}
			log.Printf("!!! Avoid updating referencing modules to versions that reintroduce use of unwanted dependencies\n")
			needUpdate = true
		}
	}
	for actualRef, actualFrom := range config.Status.UnwantedReferences {
		if _, expected := configFromFile.Status.UnwantedReferences[actualRef]; expected {
			// 符合预期：上一轮循环已确认引用方一致
			continue
		}
		log.Printf("Unwanted module %q marked in %s is referenced", actualRef, dependenciesJSONPath)
		for _, reference := range actualFrom {
			log.Printf("   %s", reference)
		}
		log.Printf("!!! Avoid updating referencing modules to versions that reintroduce use of unwanted dependencies\n")
		needUpdate = true
	}

	removedVendored, addedVendored := difference(configFromFile.Status.UnwantedVendored, config.Status.UnwantedVendored)
	if len(removedVendored) > 0 {
		log.Printf("Good news! Unwanted modules are no longer vendered: %q", removedVendored)
		log.Printf("!!! Remove those from status.unwantedVendored in %s to ensure they don't get reintroduced.", dependenciesJSONPath)
		needUpdate = true
	}
	if len(addedVendored) > 0 {
		log.Printf("Unwanted modules are newly vendored: %q", addedVendored)
		log.Printf("!!! Avoid updates that increase vendoring of unwanted dependencies\n")
		needUpdate = true
	}

	if needUpdate {
		os.Exit(1)
	}

	// 检查是否存在固定版本模块被违反的情况
	if len(pinnedModuleViolations) > 0 {
		log.Printf("ERROR: The following pinned modules have been updated:")
		for module, details := range pinnedModuleViolations {
			log.Printf("Module: %s", module)
			for _, detail := range details {
				log.Printf("  %s", detail)
			}
		}
		log.Printf("Pinned modules must not be updated. Please revert these changes.")
		os.Exit(1)
	}
}

func visit(visitor func(m module, via []module), main module, references map[module][]module, effectiveVersions map[string]module) {
	doVisit(visitor, main, nil, map[module]bool{}, references, effectiveVersions)
}

func doVisit(visitor func(m module, via []module), from module, via []module, visited map[module]bool, references map[module][]module, effectiveVersions map[string]module) {
	visitor(from, via)
	via = append(via, from)
	if visited[from] {
		return
	}
	for _, to := range references[from] {
		// 切换到该依赖实际生效的版本
		if override, ok := effectiveVersions[to.name]; ok {
			to = override
		}
		// 若本次遍历中尚未访问过该模块，则继续递归
		if !moduleInSlice(to, via, false) {
			doVisit(visitor, to, via, visited, references, effectiveVersions)
		}
	}
	visited[from] = true
}
