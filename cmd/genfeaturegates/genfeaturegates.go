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

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-base/featuregate"

	_ "k8s.io/apiextensions-apiserver/pkg/features"
	_ "k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	_ "k8s.io/kubernetes/pkg/features"
)

var (
	sortBy      = flag.String("sort", "name", "Sort by: name, stage, alpha, beta, ga, deprecated")
	reverse     = flag.Bool("reverse", false, "Reverse sort order")
	output      = flag.String("output", "", "Output file path (stdout if empty)")
	filterStage = flag.String("stage", "", "Filter by stage: alpha, beta, ga, deprecated")
	format      = flag.String("format", "markdown", "Output format: markdown, json")
)

// FeatureGateJSON 表示 JSON 输出格式中的特性门控
type FeatureGateJSON struct {
	Name         string      `json:"name"`
	Stages       []StageJSON `json:"stages"`
	Dependencies []string    `json:"dependencies,omitempty"`
}

// StageJSON 表示特性门控生命周期中的一个阶段
type StageJSON struct {
	Stage       string `json:"stage"`
	FromVersion string `json:"fromVersion"`
	ToVersion   string `json:"toVersion,omitempty"`
	Default     bool   `json:"defaultValue"`
	Locked      bool   `json:"locked,omitempty"`
}

// stageInfo 保存单个阶段条目的版本范围信息
type stageInfo struct {
	fromVersion string
	toVersion   string
}

// featureInfo 保存特性门控经过处理的信息（用于 markdown）
type featureInfo struct {
	name               string
	stage              string
	stageOrder         int // 用于排序：1=Alpha, 2=Beta, 3=GA, 4=Deprecated
	enabledFromVersion string
	lockedFromVersion  string
	alphaStages        []stageInfo
	alphaVersion       *version.Version
	betaStages         []stageInfo
	betaVersion        *version.Version
	gaStages           []stageInfo
	gaVersion          *version.Version
	depStages          []stageInfo
	depVersion         *version.Version
	deps               string
	linkCode           string
	linkKEPs           string
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags]\n\nFlags:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nExamples:\n")
		fmt.Fprintf(os.Stderr, "  %s                          # Markdown output (default)\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -format=json             # JSON output\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -sort=stage              # Sort by current stage\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -sort=alpha              # Sort by alpha version\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -sort=ga -reverse        # Sort by GA version, newest first\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -stage=alpha             # Show only alpha features\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  %s -output=features.json    # Write to file\n", os.Args[0])
	}
	flag.Parse()

	var result string
	if *format == "json" {
		result = generateJSON(*filterStage)
	} else {
		result = generateMarkdown(*sortBy, *reverse, *filterStage)
	}

	if *output == "" {
		fmt.Print(result)
	} else {
		dir := filepath.Dir(*output)
		if dir != "" && dir != "." {
			if err := os.MkdirAll(dir, 0755); err != nil {
				fmt.Fprintf(os.Stderr, "failed to create output directory: %v\n", err)
				os.Exit(1)
			}
		}
		if err := os.WriteFile(*output, []byte(result), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write output file: %v\n", err)
			os.Exit(1)
		}
	}
}

// skipFeatures 是不应出现在特性列表中的元特性
var skipFeatures = map[string]bool{
	"AllAlpha": true,
	"AllBeta":  true,
}

func generateJSON(filterStage string) string {
	// 通过公开方法获取所有带版本信息的特性规格及其依赖
	allFeatures := utilfeature.DefaultMutableFeatureGate.GetAllVersioned()
	allDependencies := utilfeature.DefaultMutableFeatureGate.Dependencies()

	// 构建特性列表
	features := make([]FeatureGateJSON, 0, len(allFeatures))

	for featureName, specs := range allFeatures {
		feature := string(featureName)

		// 跳过元特性
		if skipFeatures[feature] {
			continue
		}

		// 按版本排序规格，以便依次处理
		sort.Sort(featuregate.VersionedSpecs(specs))

		fg := FeatureGateJSON{
			Name:   feature,
			Stages: make([]StageJSON, 0),
		}

		// 记录每个阶段对应的版本，以便计算 toVersion
		type stageEntry struct {
			stage       string
			fromVersion string
			defaultVal  bool
			locked      bool
		}
		var entries []stageEntry

		for _, spec := range specs {
			stageName := ""
			switch spec.PreRelease {
			case featuregate.Alpha:
				stageName = "alpha"
			case featuregate.Beta:
				stageName = "beta"
			case featuregate.GA:
				stageName = "stable"
			case featuregate.Deprecated:
				stageName = "deprecated"
			}

			entries = append(entries, stageEntry{
				stage:       stageName,
				fromVersion: spec.Version.String(),
				defaultVal:  spec.Default,
				locked:      spec.LockToDefault,
			})
		}

		// 将条目转换为带 toVersion 的阶段
		for i, entry := range entries {
			stage := StageJSON{
				Stage:       entry.stage,
				FromVersion: entry.fromVersion,
				Default:     entry.defaultVal,
				Locked:      entry.locked,
			}

			// 查找 toVersion：寻找下一个发生变化的条目
			// （阶段、默认值或锁定状态不同）
			for j := i + 1; j < len(entries); j++ {
				if entries[j].stage != entry.stage ||
					entries[j].defaultVal != entry.defaultVal ||
					entries[j].locked != entry.locked {
					// toVersion 是下一条目 fromVersion 的前一个次版本
					stage.ToVersion = getPreviousMinorVersion(entries[j].fromVersion)
					break
				}
			}

			fg.Stages = append(fg.Stages, stage)
		}

		// 合并重复阶段：保留每个阶段首次出现的条目，并合并其版本范围
		fg.Stages = deduplicateStages(fg.Stages)

		// 应用阶段过滤
		if filterStage != "" {
			hasStage := false
			for _, s := range fg.Stages {
				if strings.EqualFold(s.Stage, filterStage) || (filterStage == "ga" && s.Stage == "stable") {
					hasStage = true
					break
				}
			}
			if !hasStage {
				continue
			}
		}

		// 获取依赖项
		deps := allDependencies[featuregate.Feature(feature)]
		if len(deps) > 0 {
			fg.Dependencies = make([]string, len(deps))
			for i, d := range deps {
				fg.Dependencies[i] = string(d)
			}
		}

		features = append(features, fg)
	}

	// 按名称排序
	sort.Slice(features, func(i, j int) bool {
		return features[i].Name < features[j].Name
	})

	jsonBytes, err := json.MarshalIndent(features, "", "  ")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to marshal JSON: %v\n", err)
		os.Exit(1)
	}

	return string(jsonBytes) + "\n"
}

// getPreviousMinorVersion 返回前一个次版本（例如 "1.30" -> "1.29"）
func getPreviousMinorVersion(ver string) string {
	v, err := version.Parse(ver)
	if err != nil {
		return ""
	}
	minor := v.Minor()
	if minor > 0 {
		return fmt.Sprintf("%d.%d", v.Major(), minor-1)
	}
	return ""
}

// deduplicateStages 合并阶段、默认值和锁定状态均相同的连续条目。
// 如果同一阶段内 defaultValue 或 locked 发生变化，则保留独立条目，
// 以完整体现生命周期历史（例如 1.23 中 beta 默认关闭，1.24 中 beta 默认开启）。
func deduplicateStages(stages []StageJSON) []StageJSON {
	if len(stages) == 0 {
		return stages
	}

	result := make([]StageJSON, 0)
	current := stages[0]

	for i := 1; i < len(stages); i++ {
		// 仅当阶段、默认值和锁定状态全部相同时才合并
		if stages[i].Stage == current.Stage &&
			stages[i].Default == current.Default &&
			stages[i].Locked == current.Locked {
			// 阶段与设置均相同，扩展版本范围
			current.ToVersion = stages[i].ToVersion
		} else {
			// 阶段不同或设置发生变化，保存当前条目并开始新条目
			result = append(result, current)
			current = stages[i]
		}
	}
	result = append(result, current)

	return result
}

func generateMarkdown(sortBy string, reverseSort bool, filterStage string) string {
	// 通过公开方法获取所有带版本信息的特性规格及其依赖
	allFeatures := utilfeature.DefaultMutableFeatureGate.GetAllVersioned()
	allDependencies := utilfeature.DefaultMutableFeatureGate.Dependencies()

	// 构建特性信息列表
	features := make([]featureInfo, 0, len(allFeatures))

	for featureName, specs := range allFeatures {
		feature := string(featureName)

		// 跳过元特性
		if skipFeatures[feature] {
			continue
		}

		info := featureInfo{
			name: feature,
		}

		// 按版本排序规格，以便依次处理
		sort.Sort(featuregate.VersionedSpecs(specs))

		// 第一轮：收集所有条目及其阶段信息
		type rawEntry struct {
			version      *version.Version
			verStr       string
			isAlpha      bool
			isBeta       bool
			isGA         bool
			isDeprecated bool
			defaultOn    bool
			locked       bool
		}
		var entries []rawEntry

		for _, spec := range specs {
			entry := rawEntry{
				version:   spec.Version,
				verStr:    spec.Version.String(),
				defaultOn: spec.Default,
				locked:    spec.LockToDefault,
			}
			switch spec.PreRelease {
			case featuregate.Alpha:
				entry.isAlpha = true
			case featuregate.Beta:
				entry.isBeta = true
			case featuregate.GA:
				entry.isGA = true
			case featuregate.Deprecated:
				entry.isDeprecated = true
			}
			entries = append(entries, entry)

			// 记录每个阶段的首个版本，以及首次启用/锁定的版本
			switch spec.PreRelease {
			case featuregate.Alpha:
				if info.alphaVersion == nil {
					info.alphaVersion = spec.Version
				}
				info.stage = "Alpha"
				info.stageOrder = 1
			case featuregate.Beta:
				if info.betaVersion == nil {
					info.betaVersion = spec.Version
				}
				info.stage = "Beta"
				info.stageOrder = 2
			case featuregate.GA:
				if info.gaVersion == nil {
					info.gaVersion = spec.Version
				}
				info.stage = "GA"
				info.stageOrder = 3
			case featuregate.Deprecated:
				if info.depVersion == nil {
					info.depVersion = spec.Version
				}
				info.stage = "Deprecated"
				info.stageOrder = 4
			}

			// 记录特性首次默认启用的时间
			if spec.Default && info.enabledFromVersion == "" {
				info.enabledFromVersion = spec.Version.String()
			}
			// 记录特性首次被锁定的时间
			if spec.LockToDefault && info.lockedFromVersion == "" {
				info.lockedFromVersion = spec.Version.String()
			}
		}

		// 第二轮：构建带 toVersion 的阶段范围
		// 这里只跟踪阶段变化（alpha->beta->ga->deprecated），不跟踪启用/锁定状态的变化。
		// Enabled 和 Locked 列会展示这些设置何时发生变化。
		for i := 0; i < len(entries); i++ {
			entry := entries[i]
			si := stageInfo{
				fromVersion: entry.verStr,
			}

			// 查找 toVersion：寻找下一个阶段不同的条目
			for j := i + 1; j < len(entries); j++ {
				sameStage := (entry.isAlpha == entries[j].isAlpha &&
					entry.isBeta == entries[j].isBeta &&
					entry.isGA == entries[j].isGA &&
					entry.isDeprecated == entries[j].isDeprecated)
				if !sameStage {
					si.toVersion = getPreviousMinorVersion(entries[j].verStr)
					break
				}
			}

			// 阶段相同则与上一个条目合并，否则新增条目
			if entry.isAlpha {
				if len(info.alphaStages) > 0 {
					last := &info.alphaStages[len(info.alphaStages)-1]
					last.toVersion = si.toVersion
					continue
				}
				info.alphaStages = append(info.alphaStages, si)
			} else if entry.isBeta {
				if len(info.betaStages) > 0 {
					last := &info.betaStages[len(info.betaStages)-1]
					last.toVersion = si.toVersion
					continue
				}
				info.betaStages = append(info.betaStages, si)
			} else if entry.isGA {
				if len(info.gaStages) > 0 {
					last := &info.gaStages[len(info.gaStages)-1]
					last.toVersion = si.toVersion
					continue
				}
				info.gaStages = append(info.gaStages, si)
			} else if entry.isDeprecated {
				if len(info.depStages) > 0 {
					last := &info.depStages[len(info.depStages)-1]
					last.toVersion = si.toVersion
					continue
				}
				info.depStages = append(info.depStages, si)
			}
		}

		// 应用阶段过滤
		if filterStage != "" && !strings.EqualFold(info.stage, filterStage) {
			continue
		}

		// 获取该特性的依赖项并转为列表（每行一个）
		deps := allDependencies[featuregate.Feature(feature)]
		if len(deps) > 0 {
			depItems := make([]string, len(deps))
			for i, d := range deps {
				depItems[i] = string(d)
			}
			info.deps = strings.Join(depItems, "<br>")
		}

		info.linkCode = fmt.Sprintf("[code](https://cs.k8s.io/?q=%%5Cb%s%%5Cb&i=nope&files=&excludeFiles=CHANGELOG&repos=kubernetes/kubernetes)", feature)
		info.linkKEPs = fmt.Sprintf("[KEPs](https://cs.k8s.io/?q=%%5Cb%s%%5Cb&i=nope&files=&excludeFiles=CHANGELOG&repos=kubernetes/enhancements)", feature)

		features = append(features, info)
	}

	// 根据 sortBy 参数对特性排序
	sortFeatures(features, sortBy, reverseSort)

	// 构建 markdown 输出
	var sb strings.Builder
	sb.WriteString("# Kubernetes Feature Gates\n\n")
	sb.WriteString("<!-- This file is auto-generated by hack/update-featuregates.sh. DO NOT EDIT. -->\n")
	sb.WriteString("<!-- To resolve merge conflicts, run: hack/update-featuregates.sh -->\n\n")

	if filterStage != "" {
		sb.WriteString(fmt.Sprintf("*Showing only %s features (%d)*\n\n", strings.ToUpper(filterStage), len(features)))
	}

	sb.WriteString("| Feature | Enabled | Locked | Alpha | Beta | GA | Deprecated | Dependencies | Links |\n")
	sb.WriteString("|---------|---------|--------|-------|------|----|------------|--------------|-------|\n")

	for _, info := range features {
		sb.WriteString(fmt.Sprintf("| %s | %s | %s | %s | %s | %s | %s | %s | %s %s |\n",
			info.name,
			formatEnabledColumn(info.enabledFromVersion),
			formatLockedColumn(info.lockedFromVersion),
			formatStageRanges(info.alphaStages),
			formatStageRanges(info.betaStages),
			formatStageRanges(info.gaStages),
			formatStageRanges(info.depStages),
			info.deps, info.linkCode, info.linkKEPs))
	}

	sb.WriteString("\n## Legend\n\n")
	sb.WriteString("| Symbol | Meaning |\n")
	sb.WriteString("|--------|--------|\n")
	sb.WriteString("| :ballot_box_with_check: | Enabled by default (in Enabled column: version when first enabled) |\n")
	sb.WriteString("| :closed_lock_with_key: | Locked to default, cannot be changed (in Locked column: version when first locked) |\n")
	sb.WriteString("| 1.X– | Feature is in this state from version 1.X (open-ended range) |\n")

	return sb.String()
}

// formatEnabledColumn 格式化 Enabled 列，显示特性首次默认启用的时间
func formatEnabledColumn(enabledFromVersion string) string {
	if enabledFromVersion == "" {
		return ""
	}
	return ":ballot_box_with_check:&nbsp;" + enabledFromVersion + "+"
}

// formatLockedColumn 格式化 Locked 列，显示特性首次被锁定的时间
func formatLockedColumn(lockedFromVersion string) string {
	if lockedFromVersion == "" {
		return ""
	}
	return ":closed_lock_with_key:&nbsp;" + lockedFromVersion + "+"
}

// formatStageRanges 格式化阶段范围列表以便用 markdown 展示
// 每个范围显示为 "fromVersion–toVersion"（使用 en-dash），开放范围显示为 "fromVersion–"
func formatStageRanges(stages []stageInfo) string {
	if len(stages) == 0 {
		return ""
	}

	var parts []string
	for i, s := range stages {
		var versionRange string
		if s.toVersion != "" && s.toVersion != s.fromVersion {
			versionRange = s.fromVersion + "–" + s.toVersion // en-dash 表示范围
		} else if s.toVersion == "" && i == len(stages)-1 {
			// 最后一个阶段没有结束版本，末尾的 en-dash 表示开放范围
			versionRange = s.fromVersion + "–"
		} else {
			versionRange = s.fromVersion
		}

		parts = append(parts, versionRange)
	}

	return strings.Join(parts, ", ")
}

func sortFeatures(features []featureInfo, sortBy string, reverseSort bool) {
	var less func(i, j int) bool

	switch sortBy {
	case "stage":
		less = func(i, j int) bool {
			if features[i].stageOrder != features[j].stageOrder {
				return features[i].stageOrder < features[j].stageOrder
			}
			return features[i].name < features[j].name
		}
	case "alpha":
		less = func(i, j int) bool {
			return compareVersions(features[i].alphaVersion, features[j].alphaVersion, features[i].name, features[j].name)
		}
	case "beta":
		less = func(i, j int) bool {
			return compareVersions(features[i].betaVersion, features[j].betaVersion, features[i].name, features[j].name)
		}
	case "ga":
		less = func(i, j int) bool {
			return compareVersions(features[i].gaVersion, features[j].gaVersion, features[i].name, features[j].name)
		}
	case "deprecated":
		less = func(i, j int) bool {
			return compareVersions(features[i].depVersion, features[j].depVersion, features[i].name, features[j].name)
		}
	default: // "name"
		less = func(i, j int) bool {
			return features[i].name < features[j].name
		}
	}

	if reverseSort {
		sort.Slice(features, func(i, j int) bool {
			return less(j, i)
		})
	} else {
		sort.Slice(features, less)
	}
}

// compareVersions 比较两个版本，nil 版本排在最后
func compareVersions(v1, v2 *version.Version, name1, name2 string) bool {
	if v1 == nil && v2 == nil {
		return name1 < name2
	}
	if v1 == nil {
		return false // nil 排在最后
	}
	if v2 == nil {
		return true // nil 排在最后
	}
	if v1.EqualTo(v2) {
		return name1 < name2
	}
	return v1.LessThan(v2)
}
