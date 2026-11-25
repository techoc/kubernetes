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

package field

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

// NormalizationRule 保存一条预编译的正则及其替换串，用于归一化字段路径。
// 预编译是为了避免在每次匹配时重复编译（测试中可能匹配成百上千条错误）。
type NormalizationRule struct {
	Regexp      *regexp.Regexp
	Replacement string
}

// ErrorMatcher 是 Error 对象的比较辅助器。
// 各 matchXxx 字段由链式 ByXxx 方法逐步开启，未开启的维度不参与比较。
type ErrorMatcher struct {
	//TODO(thockin): 考虑 type 是否一定必需，或许可以直接假定必需。
	matchType bool
	//TODO(thockin): 考虑是否可默认匹配 field；如果 "want" 错误的 field 为 nil，
	//则不按 field 匹配。
	matchField bool
	//TODO(thockin): 考虑是否可默认匹配 value；如果 "want" 错误的 value 为 nil，
	//则不按 value 匹配。
	matchValue                    bool
	matchOrigin                   bool
	matchDetail                   func(want, got string) bool
	requireOriginWhenInvalid      bool
	matchValidationStabilityLevel bool
	matchSource                   bool
	matchAncestorShortCircuit     bool
	matchShortCircuit             bool
	//normalizationRules 保存路径规范化所用的预编译正则表达式。
	normalizationRules []NormalizationRule
}

// isChildPath 判断 child 是否为 parent 的后代路径。
// 通过显式检查 '.' 或 '[' 分隔符，避免 "spec.containers" 被误判为 "spec.container" 的子路径。
func isChildPath(parent, child string) bool {
	//不支持将 "" 作为父路径，否则可能意外匹配任意路径；
	//理论上系统错误可以匹配任意路径错误。
	if len(parent) == 0 {
		return false
	}
	if len(child) <= len(parent) {
		return false
	}
	if child[:len(parent)] != parent {
		return false
	}
	sep := child[len(parent)]
	return sep == '.' || sep == '['
}

// Matches 按已配置的维度判断两个 Error 是否匹配。
// 若配置了字段归一化，只对 "got" 的字段路径做归一化（把旧 API 版本路径提升为内部/最新格式），
// "want" 则假定已经是规范格式。
func (m ErrorMatcher) Matches(want, got *Error) bool {
	gotField := got.Field
	// 仅在路径不同时才归一化，省去无谓的正则匹配。
	if want.Field != gotField {
		gotField = m.normalizePath(gotField)
	}
	// 短路匹配：got 在祖先路径上短路，且该祖先与 want 的路径存在父子关系时视为匹配。
	if m.matchAncestorShortCircuit {
		if got.ShortCircuit && (isChildPath(gotField, want.Field) || isChildPath(got.Field, want.Field)) {
			return true
		}
	}

	if m.matchType && want.Type != got.Type {
		return false
	}
	if m.matchField && want.Field != gotField {
		return false
	}
	if m.matchValue && !reflect.DeepEqual(want.BadValue, got.BadValue) {
		return false
	}

	if m.matchOrigin {
		if want.Origin != got.Origin {
			return false
		}
		// 当要求 Invalid 错误必须带 origin 时，空 origin 视为不匹配，防止测试漏写来源。
		if m.requireOriginWhenInvalid && want.Type == ErrorTypeInvalid {
			if want.Origin == "" || got.Origin == "" {
				return false
			}
		}
	}
	if m.matchDetail != nil && !m.matchDetail(want.Detail, got.Detail) {
		return false
	}
	if m.matchValidationStabilityLevel && want.ValidationStabilityLevel != got.ValidationStabilityLevel {
		return false
	}
	if m.matchSource && want.FromImperative != got.FromImperative {
		return false
	}
	if m.matchShortCircuit && want.ShortCircuit != got.ShortCircuit {
		return false
	}

	return true
}

// normalizePath 依次应用归一化规则，只采用第一条真正改变了路径的规则，
// 避免多条规则叠加导致路径被重复改写。
func (m ErrorMatcher) normalizePath(path string) string {
	for _, rule := range m.normalizationRules {
		normalized := rule.Regexp.ReplaceAllString(path, rule.Replacement)
		if normalized != path {
			//仅应用第一条匹配的规则。
			return normalized
		}
	}
	return path
}

// Render 按 ErrorMatcher 已启用的维度生成 Error 的字符串表示，供测试失败信息使用。
func (m ErrorMatcher) Render(e *Error) string {
	buf := strings.Builder{}

	comma := func() {
		if buf.Len() > 0 {
			buf.WriteString(", ")
		}
	}

	if m.matchType {
		comma()
		fmt.Fprintf(&buf, "Type=%q", e.Type)
	}
	if m.matchField {
		comma()
		if normalized := m.normalizePath(e.Field); normalized != e.Field {
			fmt.Fprintf(&buf, "Field=%q (aka %q)", normalized, e.Field)
		} else {
			fmt.Fprintf(&buf, "Field=%q", e.Field)
		}
	}
	if m.matchValue {
		comma()
		if s, ok := e.BadValue.(string); ok {
			fmt.Fprintf(&buf, "Value=%q", s)
		} else {
			rv := reflect.ValueOf(e.BadValue)
			if rv.Kind() == reflect.Pointer && !rv.IsNil() {
				rv = rv.Elem()
			}
			if rv.IsValid() && rv.CanInterface() {
				fmt.Fprintf(&buf, "Value=%v", rv.Interface())
			} else {
				fmt.Fprintf(&buf, "Value=%v", e.BadValue)
			}
		}
	}
	if m.matchOrigin || m.requireOriginWhenInvalid && e.Type == ErrorTypeInvalid {
		comma()
		fmt.Fprintf(&buf, "Origin=%q", e.Origin)
	}
	if m.matchDetail != nil {
		comma()
		fmt.Fprintf(&buf, "Detail=%q", e.Detail)
	}
	if m.matchValidationStabilityLevel {
		comma()
		fmt.Fprintf(&buf, "ValidationStabilityLevel=%s", e.ValidationStabilityLevel)
	}
	if m.matchSource {
		comma()
		fmt.Fprintf(&buf, "FromImperative=%t", e.FromImperative)
	}
	if m.matchShortCircuit {
		comma()
		fmt.Fprintf(&buf, "ShortCircuit=%t", e.ShortCircuit)
	}
	return "{" + buf.String() + "}"
}

// Exactly 返回一个比较所有字段都精确相等的派生匹配器。
func (m ErrorMatcher) Exactly() ErrorMatcher {
	return m.ByType().ByField().ByValue().ByOrigin().ByDetailExact()
}

// ByType 返回额外按错误类型匹配的派生匹配器。
func (m ErrorMatcher) ByType() ErrorMatcher {
	m.matchType = true
	return m
}

// ByField 返回额外按字段路径匹配的派生匹配器。
// 若需要改写字段路径（例如跨版本归一化），请使用 ByFieldNormalized。
func (m ErrorMatcher) ByField() ErrorMatcher {
	m.matchField = true
	return m
}

// ByFieldNormalized 返回额外按字段路径匹配的派生匹配器，但在比较前会对实际（got）错误的
// 字段路径应用归一化规则。
// 这样可以用内部 API 格式的期望去匹配旧 API 版本产生的错误路径。
//
// 归一化规则只作用于 "got"；"want" 始终假定为内部（最新）API 格式。
//
// rules 中的正则应预先编译好。
//
// 示例：
//
//	rules := []NormalizationRule{
//	  {
//	    Regexp:      regexp.MustCompile(`spec\.devices\.requests\[(\d+)\]\.allocationMode`),
//	    Replacement: "spec.devices.requests[$1].exactly.allocationMode",
//	  },
//	}
//	matcher := ErrorMatcher{}.ByFieldNormalized(rules)
func (m ErrorMatcher) ByFieldNormalized(rules []NormalizationRule) ErrorMatcher {
	m.matchField = true
	m.normalizationRules = rules
	return m
}

// ByValue 返回额外按错误值匹配的派生匹配器。
func (m ErrorMatcher) ByValue() ErrorMatcher {
	m.matchValue = true
	return m
}

// ByOrigin 返回额外按错误来源匹配的派生匹配器。
// 启用后，若期望错误的 origin 非空，则所有同 origin 的实际错误都视为匹配（可一对多）；
// 唯一例外是完全相同的错误重复出现——那几乎一定是 bug，仍会报错。
// 这使测试只需表达“我把 X 设为非法值，预期来自 origin Y 的错误”，
// 而不必关心错误的确切数量、顺序或措辞。
func (m ErrorMatcher) ByOrigin() ErrorMatcher {
	m.matchOrigin = true
	return m
}

// RequireOriginWhenInvalid 返回一个派生匹配器：在按 origin 匹配时，
// 若错误类型为 Invalid，则要求 origin 必须非空。
func (m ErrorMatcher) RequireOriginWhenInvalid() ErrorMatcher {
	m.requireOriginWhenInvalid = true
	return m
}

// BySource 返回额外按字段错误的来源（命令式/声明式）匹配的派生匹配器。
func (m ErrorMatcher) BySource() ErrorMatcher {
	m.matchSource = true
	return m
}

// MatchAncestorShortCircuit 返回一个派生匹配器：当 got 在 want 字段路径的祖先处短路时也视为匹配。
func (m ErrorMatcher) MatchAncestorShortCircuit() ErrorMatcher {
	m.matchAncestorShortCircuit = true
	return m
}

// MatchShortCircuit 返回额外按 ShortCircuit 值匹配的派生匹配器。
func (m ErrorMatcher) MatchShortCircuit() ErrorMatcher {
	m.matchShortCircuit = true
	return m
}

// ByValidationStabilityLevel 返回额外按字段错误的校验稳定性级别匹配的派生匹配器。
func (m ErrorMatcher) ByValidationStabilityLevel() ErrorMatcher {
	m.matchValidationStabilityLevel = true
	return m
}

// ByDetailExact 返回额外按 detail 字符串精确匹配的派生匹配器。
func (m ErrorMatcher) ByDetailExact() ErrorMatcher {
	m.matchDetail = func(want, got string) bool {
		return got == want
	}
	return m
}

// ByDetailSubstring 返回额外按 detail 字符串包含关系匹配的派生匹配器。
func (m ErrorMatcher) ByDetailSubstring() ErrorMatcher {
	m.matchDetail = func(want, got string) bool {
		return strings.Contains(got, want)
	}
	return m
}

// ByDetailRegexp 返回额外按 detail 字符串正则匹配的派生匹配器，
// 其中 want 被当作合法正则表达式。
func (m ErrorMatcher) ByDetailRegexp() ErrorMatcher {
	m.matchDetail = func(want, got string) bool {
		return regexp.MustCompile(want).MatchString(got)
	}
	return m
}

// TestIntf 允许调用方传入 testing.T，同时避免本包直接依赖 Go 的 testing 包。
type TestIntf interface {
	Helper()
	Errorf(format string, args ...any)
}

// Test 按匹配器配置比较两个 ErrorList，不匹配时调用 tb.Errorf 报告。
// "want" 错误应为内部（最新）API 格式；"got" 可来自任意版本，若配置了归一化规则会被改写。
//
// 若启用了 origin 匹配且错误的 origin 非空，一个 "want" 可以匹配多个 "got" 并全部消费；
// 但若出现完全相同的重复错误，则视为疑似 bug 而报错。
// 该方法不支持 matchAncestorShortCircuit（测试场景不需要）。
func (m ErrorMatcher) Test(tb TestIntf, want, got ErrorList) {
	tb.Helper()

	if m.matchAncestorShortCircuit {
		tb.Errorf("matchAncestorShortCircuit is not supported for test")
	}
	// 复制一份“全字段精确”匹配器，仅用于生成重复检测的 key 和最终未匹配错误的渲染。
	exactly := m.Exactly() //创建副本。

	//如果发现完全重复的错误，几乎可以确定是缺陷，值得报告。
	//如果未来存在合理用例，
	//可重新审视此假设。
	seen := map[string]bool{}
	for _, g := range got {
		key := exactly.Render(g)
		if seen[key] {
			tb.Errorf("exact duplicate error:\n%s", key)
		}
		seen[key] = true
	}

	// 逐个消费 want：每个 want 至多匹配一个 got（origin 匹配时除外，可一对多）。
	remaining := got
	for _, w := range want {
		tmp := make(ErrorList, 0, len(remaining))
		matched := false
		for i, g := range remaining {
			if m.Matches(w, g) {
				matched = true
				if m.matchOrigin && w.Origin != "" {
					//匹配包含 origin 时，允许同一个 "want" 错误匹配多个结果，
					//使测试不受可能返回多个错误时的具体数量、顺序和措辞影响。
					continue
				} else {
					//单次匹配时保存剩余 "got" 错误，
					//继续处理下一个 "want" 错误。
					tmp = append(tmp, remaining[i+1:]...)
					break
				}
			} else {
				tmp = append(tmp, g)
			}
		}
		if !matched {
			tb.Errorf("expected an error matching:\n%s", m.Render(w))
		}
		remaining = tmp
	}
	// 所有 want 消费完后仍有剩余 got，说明实际返回了多余的错误。
	if len(remaining) > 0 {
		for _, e := range remaining {
			tb.Errorf("unmatched error:\n%s", exactly.Render(e))
		}
	}
}
