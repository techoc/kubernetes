/*
Copyright The Kubernetes Authors.

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

package apidefinitions

import (
	"fmt"
	"strings"

	"k8s.io/gengo/v2/types"
)

// Activation 表示从 API 包 doc.go 中提取的、用于控制某个生成器是否启用的数据。
// Activation 表示从 API 包 doc.go 中提取的生成器启用信息。
type Activation struct {
	// spec 是该 activation 对应的生成器规格。
	spec Spec
	// shouldGenerate 报告是否应为该包运行 spec 指定的生成器。
	shouldGenerate bool

	// typeFilters 仅在 spec 的 ValueMode 为 TypeFilterList 时设置。
	typeFilters []string

	// externalTypes 指向 external 类型的完整导入路径。
	externalTypes string

	// 以下字段仅用于 conversion-gen：
	// explicitOnly 表示 conversion 被配置为只为显式打标签的类型生成转换。
	explicitOnly bool
	// peerPackages 提供转换的 peer 包列表。
	peerPackages []string
}

// ShouldGenerate 报告是否应在此包上运行生成器。它统一了所有启用路径：
// 显式 ActivationTag 值、=false 退出、+<GroupActivationTag> 回退，以及
// ScanTypes 的类型级回退。ValueMode 为 ConversionPeerList 的生成器在
// ActivationTag 为 "false"（explicit-only 模式）时也会生成；
// 可用 IsExplicitOnly 区分。
// ShouldGenerate 报告是否应为该包运行生成器。它统一了所有启用路径：
// 显式 ActivationTag、=false 退出、+<GroupActivationTag> 回退和 ScanTypes 类型级回退。
// ValueMode 为 ConversionPeerList 的生成器在 ActivationTag="false" 时仍会生成
// （explicit-only 模式），可用 IsExplicitOnly 区分。
func (a *Activation) ShouldGenerate() bool { return a.shouldGenerate }

// TypeFilters 返回生成器应处理的类型名模式。
// TypeFilters 返回生成器应处理的类型名模式。
func (a *Activation) TypeFilters() []string { return a.typeFilters }

// ExternalTypes 返回 external 类型的完整导入路径。
// ExternalTypes 返回 external 类型的完整 import path。
func (a *Activation) ExternalTypes() string { return a.externalTypes }

// PeerPackages 返回转换的 peer 包列表。
// PeerPackages 返回转换的 peer 包列表。
func (a *Activation) PeerPackages() []string { return a.peerPackages }

// IsExplicitOnly 报告 conversion-gen 是否应运行在 explicit-only 模式
// （只为显式打标签的类型生成转换）。该值由 +k8s:conversion-gen=false 设置，
// 且仅对使用 ConversionPeerList 的生成器有意义。
// IsExplicitOnly 报告 conversion-gen 是否运行在 explicit-only 模式，
// 即只为显式打标签的类型生成转换。该值由 +k8s:conversion-gen=false 设置，
// 且仅对使用 ConversionPeerList 的生成器有意义。
func (a *Activation) IsExplicitOnly() bool { return a.explicitOnly }

// Identify 从 pkg 的 doc.go 中为 spec 描述的生成器提取激活信息。
// opts 可启用 lint 规则；pkg 为 nil 时返回零值 Activation（视为未启用）。
// Identify 从 pkg 的 doc.go 中提取 spec 指定生成器的启用信息。
func Identify(pkg *types.Package, spec Spec, opts ...Option) (*Activation, error) {
	var st identifyState
	for _, opt := range opts {
		opt(&st)
	}
	// lint 先于激活判定执行，以便尽早暴露 doc.go 中的配置错误。
	if err := applyLintRules(&st, pkg); err != nil {
		return nil, err
	}
	a := &Activation{spec: spec}
	if pkg == nil {
		return a, nil
	}
	values, optedOut, err := extractGeneratorTag(pkg.Comments, spec.ActivationTag)
	if err != nil {
		return nil, err
	}
	enabled := !optedOut && len(values) > 0
	switch spec.ValueMode {
	case ConversionPeerList:
		// peer 包必须是完整导入路径，相对路径会让生成结果不可复现。
		for _, v := range values {
			if err := checkRelativeImportPath(spec.ActivationTag, v); err != nil {
				return nil, err
			}
		}
		a.peerPackages = values
		a.explicitOnly = optedOut
	case TypeFilterList:
		a.typeFilters = values
	}
	a.externalTypes, err = resolveExternalTypes(pkg, spec)
	if err != nil {
		return nil, err
	}

	// 按优先级依次尝试各种启用条件，先命中者生效。
	switch {
	case enabled:
		a.shouldGenerate = true
	case optedOut:
		// 只有 ConversionPeerList 模式在 =false 时仍需生成，
		// 用于支持 conversion-gen 的 explicit-only 模式。
		a.shouldGenerate = spec.ValueMode == ConversionPeerList
	case spec.DefaultEnabled:
		a.shouldGenerate = true
	case isGroupActivated(pkg, spec):
		a.shouldGenerate = true
	case spec.ScanTypes:
		a.shouldGenerate = scanTypesForActivation(pkg, spec.ActivationTag)
	}
	return a, nil
}

// isGroupActivated 报告包是否带有组级激活标签（任一值即视为启用）。
func isGroupActivated(pkg *types.Package, spec Spec) bool {
	if len(spec.GroupActivationTag) > 0 {
		if vals, _ := tagValues(pkg.Comments, spec.GroupActivationTag); len(vals) > 0 {
			return true
		}
	}
	return false
}

// scanTypesForActivation 报告 pkg 中是否存在带类型级
// 生成指令说明：+<tagName>=<非 false> 标签的类型。这实现了 ScanTypes 激活检查。
// scanTypesForActivation 报告 pkg 中是否存在带类型级非 false 标签的类型，
// 实现 ScanTypes 激活检查。
func scanTypesForActivation(pkg *types.Package, tagName string) bool {
	if tagName == "" {
		return false
	}
	for _, t := range pkg.Types {
		// 两处注释来源都要看：标签可能紧贴类型，也可能在其上方的注释块中。
		lines := append(append([]string{}, t.CommentLines...), t.SecondClosestCommentLines...)
		vals, err := tagValues(lines, tagName)
		if err != nil {
			// 单个类型的标签格式错误不应让整个扫描失败。
			continue
		}
		for _, v := range vals {
			if v != "false" {
				return true
			}
		}
	}
	return false
}

// resolveExternalTypes 返回 pkg 的 +<spec.InputTag> 值（若存在），
// 否则返回 pkg.Path。输入标签出现多个值是错误。
// resolveExternalTypes 返回 pkg 的 +<spec.InputTag> 值，不存在时返回 pkg.Path。
// 输入标签出现多个值视为错误。
func resolveExternalTypes(pkg *types.Package, spec Spec) (string, error) {
	if spec.InputTag == "" {
		return pkg.Path, nil
	}
	values, err := tagValues(pkg.Comments, spec.InputTag)
	if err != nil {
		return "", err
	}
	switch len(values) {
	case 0:
		// 未声明时 external 类型与包本身同址（同一包内版本）。
		return pkg.Path, nil
	case 1:
		if err := checkRelativeImportPath(spec.InputTag, values[0]); err != nil {
			return "", err
		}
		return values[0], nil
	default:
		return "", fmt.Errorf("+%s: expected at most one value, got %v", spec.InputTag, values)
	}
}

// checkRelativeImportPath 检查路径是否为相对路径，是则返回错误。
// 相对路径在不同工作目录下解析结果不同，无法保证生成结果可复现。
func checkRelativeImportPath(tagName, path string) error {
	if strings.HasPrefix(path, "./") || strings.HasPrefix(path, "../") {
		return fmt.Errorf("+%s: relative path %q is not supported; use the full package import path", tagName, path)
	}
	return nil
}
