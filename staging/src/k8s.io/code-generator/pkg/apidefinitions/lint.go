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
	"errors"
	"fmt"
	"path"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/spf13/pflag"

	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/types"
)

const (
	// LintRuleKnownTagsOnly 拒绝包中任何未被识别的 +k8s: 标签。
	LintRuleKnownTagsOnly = "known-tags-only"

	// LintRuleExplicitDisablement 要求通常需要生成器的 API 定义包显式启用它，
	// 或用标签显式禁用，避免在需要生成的包里漏配。
	LintRuleExplicitDisablement = "require-explicit-disablement"
)

// Option 为包识别过程提供配置选项。
type Option func(*identifyState)

// identifyState 保存识别过程中由 Option 累积的状态。
type identifyState struct {
	lintRules []string
}

// WithLintRules 启用一个或多个标签检查 lint 规则。
func WithLintRules(names ...string) Option {
	return func(s *identifyState) {
		if s.lintRules == nil {
			s.lintRules = append(s.lintRules, names...)
		}
	}
}

// LintArgs 是各生成器共享的 lint 命令行参数。
type LintArgs struct {
	// LintRules 指定要启用的标签检查 lint 规则名，见 apipkg.LintRule* 常量。
	// 默认关闭，以保持对第三方生成器标签的向后兼容。
	LintRules []string
}

// allLintRules 是支持的全部 lint 规则，供参数校验与帮助信息使用。
var allLintRules = []string{LintRuleKnownTagsOnly, LintRuleExplicitDisablement}

// AddFlags 把 lint 相关参数注册到 flag set。
func AddFlags(args *LintArgs, fs *pflag.FlagSet) {
	supportedLinters := allLintRules
	fs.StringSliceVar(&args.LintRules, "lint-rules", args.LintRules,
		fmt.Sprintf("Comma-separated list of tag-checking lint rules to enable. Supports: %s",
			strings.Join(supportedLinters, ",")))
}

// ValidateFlags 校验每个规则名是否受支持。
func ValidateFlags(lintRules []string) error {
	for _, rule := range lintRules {
		if !slices.Contains(allLintRules, rule) {
			return fmt.Errorf("unrecognized rule in --lint-rules: %s", rule)
		}
	}
	return nil
}

// applyLintRules 按顺序执行已启用的 lint 规则，首个错误即返回。
func applyLintRules(st *identifyState, pkg *types.Package) error {
	for _, rule := range st.lintRules {
		switch rule {
		case LintRuleKnownTagsOnly:
			if err := checkKnownTags(pkg); err != nil {
				return err
			}
		case LintRuleExplicitDisablement:
			if err := checkExplicitDisablement(pkg); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unrecognized lint-rule: %s", rule)
		}
	}
	return nil
}

// checkKnownTags 执行 LintRuleKnownTagsOnly。它只检查 +k8s:*-gen[...] 家族中的
// 标签；非生成器类的 +k8s: 标签原样放行。
// checkKnownTags 执行 LintRuleKnownTagsOnly，只检查 +k8s:*-gen[...] 家族的标签；
// 非生成器类的 +k8s: 标签原样放行。
func checkKnownTags(pkg *types.Package) error {
	if pkg == nil {
		return nil
	}
	var unknown []string
	for name := range codetags.Extract("+", pkg.Comments) {
		// 仅关注形如 k8s:xxx-gen 的生成器标签，其余标签不在此规则职责内。
		if !strings.HasPrefix(name, "k8s:") || !strings.Contains(name, "-gen") {
			continue
		}
		// 去掉 k8s: 之后的 ":" 子标签（如 deepcopy-gen:interfaces），只比对基础标签名。
		base := name
		if i := strings.Index(name[len("k8s:"):], ":"); i >= 0 {
			base = name[:len("k8s:")+i]
		}
		if !allKnownTags.Has(base) {
			unknown = append(unknown, "+"+name)
		}
	}
	if len(unknown) == 0 {
		return nil
	}

	// 排序未知标签与已知标签，保证错误信息稳定可读。
	sort.Strings(unknown)
	allowed := allKnownTags.UnsortedList()
	slices.Sort(allowed)
	return fmt.Errorf("%s/doc.go: unrecognized +k8s: tag(s): %s\nrecognized generator tags: %s",
		pkg.Dir, strings.Join(unknown, ", "), strings.Join(allowed, ", "))
}

// checkExplicitDisablement 执行 LintRuleExplicitDisablement：
// 对识别出的 API 包角色，要求每个通常需要的生成器都有显式配置或禁用标签。
func checkExplicitDisablement(pkg *types.Package) error {
	if pkg == nil {
		return nil
	}
	// 先按启发式规则判断包角色，再推导该角色通常需要哪些生成器。
	roles := classifyPackage(pkg)
	expected := expectedGenerators(roles)
	if len(expected) == 0 {
		return nil
	}
	tagged := codetags.Extract("+", pkg.Comments)
	var missing []Spec
	for _, spec := range expected {
		if _, ok := tagged[spec.ActivationTag]; !ok {
			missing = append(missing, spec)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	// 按标签名排序，保证多次运行的错误信息一致。
	sort.Slice(missing, func(i, j int) bool { return missing[i].ActivationTag < missing[j].ActivationTag })

	var sb strings.Builder
	fmt.Fprintf(&sb, "%s/doc.go: %s.\n", pkg.Dir, roles.describePackage(pkg.Dir))
	sb.WriteString("Set explicit configuration or disablement for these generators in doc.go:")
	for _, spec := range missing {
		fmt.Fprintf(&sb, "\n  +%s=<%s>", spec.ActivationTag, valueHint(spec))
	}
	return errors.New(sb.String())
}

// valueHint 描述 ActivationTag 可接受的取值形状，用于面向用户的错误信息。
// valueHint 描述 ActivationTag 可接受的取值形状，用于面向用户的错误信息。
func valueHint(spec Spec) string {
	switch spec.ValueMode {
	case ConversionPeerList:
		return "peer-package or false"
	case TypeFilterList:
		return "TypeMeta or false"
	case Package:
		return "package or false"
	case Boolean:
		return "true or false"
	default:
		return "value or false"
	}
}

// expectedGenerators 根据包角色推导通常需要的生成器集合：
// external 版本包需要 deepcopy；internal 版本包需要 conversion/defaulter/validation。
func expectedGenerators(roles packageRoles) []Spec {
	var expected []Spec
	if roles.isExternalVersion {
		expected = append(expected, Deepcopy)
	}
	if roles.isInternalVersion {
		expected = append(expected, Conversion, Defaulter, Validation)
	}
	return expected
}

// packageRoles 描述包在 API 布局中可能扮演的角色（可同时为多个）。
type packageRoles struct {
	// isExternalGroup 标识包含版本化 API 类型的 API 组包
	// （例如 staging/src/k8s.io/api/apps）。
	isExternalGroup bool

	// isInternalGroup 标识包含 API 类型内部表示的组包
	// （例如 pkg/apis/apps）。"internal type"（即 Hub 类型）通常位于此处。
	isInternalGroup bool

	// isExternalVersion 标识包含版本化 API 类型的包
	// （例如 staging/src/k8s.io/api/apps/v1）。
	isExternalVersion bool

	// isInternalVersion 标识包含父目录 Hub 类型转换/默认值/校验的逐版本内部包
	// （例如 pkg/apis/apps/v1）。
	isInternalVersion bool
}

// describePackage 生成人类可读的包角色描述，用于 lint 错误信息。
func (r packageRoles) describePackage(pkgDir string) string {
	var parts []string
	if r.isExternalGroup {
		parts = append(parts, "API group package with versioned API types in subpackages")
	}
	if r.isInternalGroup {
		parts = append(parts, "API group package containing hub API types")
	}
	if r.isExternalVersion {
		parts = append(parts, "package containing versioned API types")
	}
	if r.isInternalVersion {
		desc := "package containing per-version internals (conversion, defaulting, validation)"
		// 附上父目录，方便用户确认“hub 类型在哪”。
		if parent := path.Dir(pkgDir); parent != "" && parent != "." && parent != "/" {
			desc += " for hub types at " + parent
		}
		parts = append(parts, desc)
	}
	if len(parts) == 0 {
		return "package has no recognized API role"
	}
	return "this package looks like: " + strings.Join(parts, "; ")
}

// classifyPackage 根据目录路径、标签与类型特征推断包角色。
//
// 可能产生假阴性：信息不足时无法判断所有包的意图。
// 例如一个没有标签也没有类型的目录，可能本应包含 API 类型，但这里检测不到。
//
// 只能用于 lint，不得用于决定生成行为。
// classifyPackage 根据已有证据推断包角色。
//
// 信息不足时可能漏判：例如没有标签也没有类型的目录本应包含 API 类型，
// 但这里检测不到。
//
// 只能用于 lint，不得用于决定生成行为。
func classifyPackage(pkg *types.Package) packageRoles {
	// TODO: 将来可在包内引入 API 声明文件，从而精确分类而不依赖这里的启发式规则。
	if pkg == nil || !looksLikeAPIPackage(pkg) {
		return packageRoles{}
	}

	roles := packageRoles{}
	// 版本路径 + external 特征 => external 版本包；否则按 internal 判断。
	isVersion := isVersionPath(pkg.Path)
	external := looksLikeExternalVersionPackage(pkg)
	if isVersion {
		if external {
			roles.isExternalVersion = true
		} else if looksLikeInternalVersionPackage(pkg) {
			roles.isInternalVersion = true
		}
	} else {
		if external {
			roles.isExternalGroup = true
		} else {
			roles.isInternalGroup = true
		}
	}
	return roles
}

// looksLikeAPIPackage 判断包是否像 API 包：声明了 groupName 或带有任一已知生成器标签。
func looksLikeAPIPackage(pkg *types.Package) bool {
	if _, ok, _ := GroupNameForPackage(pkg.Comments); ok {
		return true
	}
	for name := range codetags.Extract("+", pkg.Comments) {
		if allKnownTags.Has(name) {
			return true
		}
	}
	return false
}

// looksLikeInternalVersionPackage 判断是否为 internal 版本包：
// 声明了 defaulter/validation 的激活标签或输入标签。
func looksLikeInternalVersionPackage(pkg *types.Package) bool {
	raw := codetags.Extract("+", pkg.Comments)
	for _, s := range []Spec{Defaulter, Validation} {
		if _, ok := raw[s.ActivationTag]; ok {
			return true
		}
		if s.InputTag != "" {
			if _, ok := raw[s.InputTag]; ok {
				return true
			}
		}
	}
	return false
}

// looksLikeExternalVersionPackage 判断是否为 external 版本包：
// 存在内嵌且 json 名为空的 TypeMeta 字段（即 `json:""`），这是 versioned API 类型
// 的标志性写法。
func looksLikeExternalVersionPackage(pkg *types.Package) bool {
	for _, t := range pkg.Types {
		for _, m := range t.Members {
			if m.Name == "TypeMeta" && m.Embedded {
				tag, exists := reflect.StructTag(m.Tags).Lookup("json")
				name := strings.Split(tag, ",")[0]
				if exists && name == "" {
					return true
				}
			}
		}
	}
	return false
}

// versionPathRE 匹配 API 版本路径段，如 v1、v1beta2、v2alpha1。
var versionPathRE = regexp.MustCompile(`^v\d+([a-z]+\d+)?$`)

// isVersionPath 报告路径末段是否形如 API 版本（v1、v1beta2、v2alpha1 等）。
// isVersionPath 报告路径末段是否形如 API 版本（v1、v1beta2、v2alpha1 等）。
func isVersionPath(p string) bool {
	if p == "" {
		return false
	}
	return versionPathRE.MatchString(path.Base(p))
}
