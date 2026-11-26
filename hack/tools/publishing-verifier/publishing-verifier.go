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
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/mod/modfile"
	"k8s.io/publishing-bot/cmd/publishing-bot/config"
)

var (
	rulesFile           string
	componentsDirectory string
)

// getGoModDependencies 收集指定目录下各模块在 staging 中的依赖。
func getGoModDependencies(dir string) (map[string][]string, error) {
	allDependencies := make(map[string][]string)
	components, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	for _, component := range components {
		componentName := component.Name()
		if !component.IsDir() {
			// 目前没有强制校验 staging 目录下不能有其他文件
			continue
		}
		gomodFilePath := filepath.Join(dir, componentName, "go.mod")
		gomodFileContent, err := os.ReadFile(gomodFilePath)
		if err != nil {
			return nil, err
		}

		fmt.Printf("%s dependencies", componentName)

		allDependencies[componentName] = make([]string, 0)

		gomodFile, err := modfile.Parse(gomodFilePath, gomodFileContent, nil)
		if err != nil {
			return nil, err
		}
		// replace 段中的模块即 staging 内部依赖
		for _, module := range gomodFile.Replace {
			dep := strings.TrimPrefix(module.Old.Path, "k8s.io/")
			if dep == componentName {
				continue
			}
			allDependencies[componentName] = append(allDependencies[componentName], dep)
		}
	}
	return allDependencies, nil
}

// diffSlice 返回 s1 中不属于 s2 的元素。
func diffSlice(s1, s2 []string) []string {
	var diff []string
	set := make(map[string]struct{}, len(s2))
	for _, s := range s2 {
		set[s] = struct{}{}
	}
	for _, s := range s1 {
		if _, ok := set[s]; !ok {
			diff = append(diff, s)
		}
	}
	return diff
}

// getKeys 提取给定 map 的所有键。
func getKeys[K comparable, V any](m map[K]V) []K {
	var keys []K
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// checkValidSourceDirectory 校验规则中源目录字段的使用是否合规。
func checkValidSourceDirectory(rule config.RepositoryRule) error {
	for _, branch := range rule.Branches {
		if branch.Source.Dir != "" {
			return fmt.Errorf("use of deprecated `dir` field in rules for `%s`", rule.DestinationRepository)
		}
		if len(branch.Source.Dirs) > 1 {
			return fmt.Errorf("cannot have more than one directory (%s) per source branch `%s` of `%s`",
				branch.Source.Dirs,
				branch.Source.Branch,
				rule.DestinationRepository,
			)
		}
		if !strings.HasSuffix(branch.Source.Dirs[0], rule.DestinationRepository) {
			return fmt.Errorf("copy/paste error `%s` refers to `%s`", rule.DestinationRepository, branch.Source.Dirs[0])
		}
	}
	return nil
}

// checkMasterBranch 校验目标仓库的 master 分支是否对应源仓库的 master 分支。
func checkMasterBranch(rule config.RepositoryRule) error {
	branch := rule.Branches[0]
	if branch.Name != "master" {
		return fmt.Errorf("cannot find master branch for destination `%s`", rule.DestinationRepository)
	}

	if branch.Source.Branch != "master" {
		return fmt.Errorf("cannot find master source branch for destination `%s`", rule.DestinationRepository)
	}
	return nil
}

func checkDependencies(rule config.RepositoryRule, gomodDependencies map[string][]string) error {
	var processedDeps []string
	branch := rule.Branches[0]
	for _, dep := range gomodDependencies[rule.DestinationRepository] {
		found := false
		if len(branch.Dependencies) > 0 {
			for _, dep2 := range branch.Dependencies {
				processedDeps = append(processedDeps, dep2.Repository)
				if dep2.Branch != "master" {
					return fmt.Errorf("looking for master branch of %s and found : %s for destination", dep2.Repository, rule.DestinationRepository)
				}
				if dep2.Repository == dep {
					found = true
				}
			}
		} else {
			return fmt.Errorf("Please add %s as dependencies under destination %s", gomodDependencies[rule.DestinationRepository], rule.DestinationRepository)
		}
		if !found {
			return fmt.Errorf("Please add %s as a dependency under destination %s", dep, rule.DestinationRepository)
		} else {
			fmt.Printf("dependency %s found\n", dep)
		}
	}
	// 检查是否所有依赖都已处理。
	extraDeps := diffSlice(processedDeps, gomodDependencies[rule.DestinationRepository])
	if len(extraDeps) > 0 {
		return fmt.Errorf("extra dependencies in rules for %s: %s", rule.DestinationRepository, strings.Join(extraDeps, ","))
	}
	return nil
}

func verifyPublishingBotRules() error {
	rules, err := config.LoadRules(rulesFile)
	if err != nil {
		return fmt.Errorf("error loading rules: %v", err)
	}

	gomodDependencies, err := getGoModDependencies(componentsDirectory)

	var processedRepos []string
	for _, rule := range rules.Rules {
		branch := rule.Branches[0]

		// master 中已不存在该模块
		if _, ok := gomodDependencies[rule.DestinationRepository]; !ok {
			// 确保没有留下从 master 发布它的规则
			for _, branch := range rule.Branches {
				if branch.Name == "master" {
					err := fmt.Errorf("cannot find master branch for destination `%s`", rule.DestinationRepository)
					panic(err)
				}
			}
			// 并跳过其发布规则校验
			continue
		}

		if err := checkValidSourceDirectory(rule); err != nil {
			return fmt.Errorf("error validating source directory: %v", err)
		}

		if err := checkMasterBranch(rule); err != nil {
			return fmt.Errorf("error validating master branch: %v", err)
		}

		// 所有 master 分支的 Go 版本由 `default-go-version` 统一指定，
		// 因此规则里不得为 master 分支单独指定版本
		if branch.GoVersion != "" {
			err := fmt.Errorf("go version must not be specified for master branch for destination `%s`", rule.DestinationRepository)
			panic(err)
		}

		fmt.Printf("processing : %s", rule.DestinationRepository)
		if _, ok := gomodDependencies[rule.DestinationRepository]; !ok {
			err := fmt.Errorf("missing go.mod for `%s`", rule.DestinationRepository)
			panic(err)
		}
		processedRepos = append(processedRepos, rule.DestinationRepository)

		if err := checkDependencies(rule, gomodDependencies); err != nil {
			return fmt.Errorf("error validating dependencies: %v", err)
		}
	}

	// 检查是否所有仓库都已处理。
	items := diffSlice(getKeys(gomodDependencies), processedRepos)
	if len(items) > 0 {
		err := fmt.Errorf("missing rules for %s", strings.Join(items, ","))
		panic(err)
	}
	return nil
}

func main() {
	if len(os.Args) != 2 {
		panic("invalid number of arguments")
	}

	kubeRoot := os.Args[1]
	stagingDirectory := kubeRoot + "/staging/"
	rulesFile = stagingDirectory + "publishing/rules.yaml"
	componentsDirectory = stagingDirectory + "src/k8s.io/"

	if err := verifyPublishingBotRules(); err != nil {
		panic(err)
	}
}
