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

package cel

import (
	"fmt"
	"regexp"
	"strings"
	"testing"

	"sigs.k8s.io/randfill"
)

// TestEscaping 验证属性名转义规则，并检查转义结果的合法性与往返一致性。
func TestEscaping(t *testing.T) {
	cases := []struct {
		unescaped   string
		escaped     string
		unescapable bool
	}{
		// '.'、'-'、'/' 与 '__' 必须转义，因为 CEL 标识符只允许 [a-zA-Z_][a-zA-Z0-9_]*。
		{unescaped: "a.a", escaped: "a__dot__a"},
		{unescaped: "a-a", escaped: "a__dash__a"},
		{unescaped: "a__a", escaped: "a__underscores__a"},
		{unescaped: "a.-/__a", escaped: "a__dot____dash____slash____underscores__a"},
		{unescaped: "a._a", escaped: "a__dot___a"},
		{unescaped: "a__.__a", escaped: "a__underscores____dot____underscores__a"},
		{unescaped: "a___a", escaped: "a__underscores___a"},
		{unescaped: "a____a", escaped: "a__underscores____underscores__a"},
		{unescaped: "a__dot__a", escaped: "a__underscores__dot__underscores__a"},
		{unescaped: "a__underscores__a", escaped: "a__underscores__underscores__underscores__a"},
		// CEL 词法保留字必须转义，否则会被解析为关键字而不是字段名。
		{unescaped: "true", escaped: "__true__"},
		{unescaped: "false", escaped: "__false__"},
		{unescaped: "null", escaped: "__null__"},
		{unescaped: "in", escaped: "__in__"},
		{unescaped: "as", escaped: "__as__"},
		{unescaped: "break", escaped: "__break__"},
		{unescaped: "const", escaped: "__const__"},
		{unescaped: "continue", escaped: "__continue__"},
		{unescaped: "else", escaped: "__else__"},
		{unescaped: "for", escaped: "__for__"},
		{unescaped: "function", escaped: "__function__"},
		{unescaped: "if", escaped: "__if__"},
		{unescaped: "import", escaped: "__import__"},
		{unescaped: "let", escaped: "__let__"},
		{unescaped: "loop", escaped: "__loop__"},
		{unescaped: "package", escaped: "__package__"},
		{unescaped: "namespace", escaped: "__namespace__"},
		{unescaped: "return", escaped: "__return__"},
		{unescaped: "var", escaped: "__var__"},
		{unescaped: "void", escaped: "__void__"},
		{unescaped: "while", escaped: "__while__"},
		// 并非所有属性名都可转义：以数字或非法字符开头的名称无法映射为合法标识符。
		{unescaped: "@", unescapable: true},
		{unescaped: "1up", unescapable: true},
		{unescaped: "👑", unescapable: true},
		// CEL 宏与函数名和标识符不在同一命名空间，因此无需转义。
		{unescaped: "has", escaped: "has"},
		{unescaped: "all", escaped: "all"},
		{unescaped: "exists", escaped: "exists"},
		{unescaped: "exists_one", escaped: "exists_one"},
		{unescaped: "filter", escaped: "filter"},
		{unescaped: "size", escaped: "size"},
		{unescaped: "contains", escaped: "contains"},
		{unescaped: "startsWith", escaped: "startsWith"},
		{unescaped: "endsWith", escaped: "endsWith"},
		{unescaped: "matches", escaped: "matches"},
		{unescaped: "duration", escaped: "duration"},
		{unescaped: "timestamp", escaped: "timestamp"},
		{unescaped: "getDate", escaped: "getDate"},
		{unescaped: "getDayOfMonth", escaped: "getDayOfMonth"},
		{unescaped: "getDayOfWeek", escaped: "getDayOfWeek"},
		{unescaped: "getFullYear", escaped: "getFullYear"},
		{unescaped: "getHours", escaped: "getHours"},
		{unescaped: "getMilliseconds", escaped: "getMilliseconds"},
		{unescaped: "getMinutes", escaped: "getMinutes"},
		{unescaped: "getMonth", escaped: "getMonth"},
		{unescaped: "getSeconds", escaped: "getSeconds"},
		// 单个下划线是合法标识符，不触发转义。
		{unescaped: "_if", escaped: "_if"},
		{unescaped: "_has", escaped: "_has"},
		{unescaped: "_int", escaped: "_int"},
		{unescaped: "_anything", escaped: "_anything"},
	}

	for _, tc := range cases {
		t.Run(tc.unescaped, func(t *testing.T) {
			e, escapable := Escape(tc.unescaped)
			if tc.unescapable {
				// 期望不可转义时，只要 Escape 报告无解即可。
				if escapable {
					t.Errorf("Expected escapable=false, but got %t", escapable)
				}
				return
			}
			if !escapable {
				// 可转义的用例必须得到结果。
				t.Fatalf("Expected escapable=true, but got %t", escapable)
			}
			if tc.escaped != e {
				t.Errorf("Expected %s to escape to %s, but got %s", tc.unescaped, tc.escaped, e)
			}

			if !validCelIdent.MatchString(e) {
				// 转义结果必须是合法 CEL 标识符。
				t.Errorf("Expected %s to escape to a valid CEL identifier, but got %s", tc.unescaped, e)
			}

			u, ok := Unescape(tc.escaped)
			if !ok {
				// 转义结果必须能被 Unescape 识别。
				t.Fatalf("Expected %s to be escapable, but it was not", tc.escaped)
			}
			if tc.unescaped != u {
				// 往返一致：Escape 后再 Unescape 应还原原始名称。
				t.Errorf("Expected %s to unescape to %s, but got %s", tc.escaped, tc.unescaped, u)
			}
		})
	}
}

// TestUnescapeMalformed 验证非法或带尾部垃圾的转义序列不会被误接受。
func TestUnescapeMalformed(t *testing.T) {
	for _, s := range []string{"__int__extra", "__illegal__"} {
		t.Run(s, func(t *testing.T) {
			e, ok := Unescape(s)
			if ok {
				// ok=true 意味着非法输入被错误地当成了合法转义。
				t.Fatalf("Expected %s to be unescapable, but it escaped to: %s", s, e)
			}
		})
	}
}

// TestEscapingFuzz 用随机字符串验证转义器的往返性质：可转义的输入转义后必须是合法
// CEL 标识符，并且能原样还原。
func TestEscapingFuzz(t *testing.T) {
	fuzzer := randfill.New()
	for i := 0; i < 1000; i++ {
		var unescaped string
		fuzzer.Fill(&unescaped)
		t.Run(fmt.Sprintf("%d - '%s'", i, unescaped), func(t *testing.T) {
			if len(unescaped) == 0 {
				// 空串没有转义意义，跳过。
				return
			}
			escaped, ok := Escape(unescaped)
			if !ok {
				// 不可转义的输入不在本测试的约束范围内。
				return
			}

			if !validCelIdent.MatchString(escaped) {
				t.Errorf("Expected %s to escape to a valid CEL identifier, but got %s", unescaped, escaped)
			}
			u, ok := Unescape(escaped)
			if !ok {
				t.Fatalf("Expected %s to be unescapable, but it was not", escaped)
			}
			if unescaped != u {
				t.Errorf("Expected %s to unescape to %s, but got %s", escaped, unescaped, u)
			}
		})
	}
}

// validCelIdent 是 CEL 标识符的合法形式，用于校验转义结果。
var validCelIdent = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// TestCanSkipRegex 验证“可跳过转义”的快速判定与非法字符检测：
// 简单标识符无需正则即可通过，而非法字符必须被识别出来。
func TestCanSkipRegex(t *testing.T) {
	cases := []struct {
		unescaped        string
		canSkip          bool
		invalidCharFound bool
	}{
		// 含转义字符的名称不能走快速路径。
		{unescaped: "a.a", canSkip: false},
		{unescaped: "a-a", canSkip: false},
		{unescaped: "a__a", canSkip: false},
		{unescaped: "a.-/__a", canSkip: false},
		// 纯字母/下划线名称是合法标识符，可跳过正则检查。
		{unescaped: "a_a", canSkip: true},
		{unescaped: "a_a_a", canSkip: true},
		// 非法字符必须被标记出来，而不是落入快速路径。
		{unescaped: "@", invalidCharFound: true},
		{unescaped: "👑", invalidCharFound: true},
		// 若先遇到转义关键字再遇到非法字符，invalidCharFound 不会被置位。
		{unescaped: "/👑", canSkip: false},
	}

	for _, tc := range cases {
		t.Run(tc.unescaped, func(t *testing.T) {
			escapeCheck := skipRegexCheck(tc.unescaped)
			if escapeCheck.invalidCharFound {
				// 发现非法字符时只需比较该标记，canSkip 无意义。
				if escapeCheck.invalidCharFound != tc.invalidCharFound {
					t.Errorf("Expected input validation to be %v, but got %t", tc.invalidCharFound, escapeCheck.invalidCharFound)
				}
			} else {
				// 无非法字符时，比较快速路径的判定与实际转义需求是否一致。
				if escapeCheck.canSkipRegex != tc.canSkip {
					t.Errorf("Expected %v, but got %t", tc.canSkip, escapeCheck.canSkipRegex)
				}
			}
		})
	}
}

// TestCELReservedSymbolsNoDoubleUnderscore 验证保留字表中不含 "__"：
// 否则转义方案（以 __ 作为分隔/标记）会与保留字本身产生歧义。
func TestCELReservedSymbolsNoDoubleUnderscore(t *testing.T) {
	for symbol := range celReservedSymbols {
		if strings.Contains(symbol, "__") {
			// 保留字一旦包含 __，Escape/Unescape 的编码将无法无歧义地还原。
			t.Errorf("CEL reserved symbol '%s' contains '__', which is not allowed as it would interfere with escaping", symbol)
		}
	}
}
