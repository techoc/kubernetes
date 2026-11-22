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
	"regexp"

	"k8s.io/apimachinery/pkg/util/sets"
)

// celReservedSymbols 是 CEL 词法分析器中定义的一批 RESERVED 符号。
// 任何标识符都不能与这些符号冲突。
// https://github.com/google/cel-spec/blob/master/doc/langdef.md#syntax
var celReservedSymbols = sets.NewString(
	"true", "false", "null", "in",
	"as", "break", "const", "continue", "else",
	"for", "function", "if", "import", "let",
	"loop", "package", "namespace", "return", // !! Kubernetes 中大量使用 'namespace'
	"var", "void", "while",
)

// expandMatcher 匹配转义序列、需要转义的字符，以及不受支持的字符
var expandMatcher = regexp.MustCompile(`(__|[-./]|[^a-zA-Z0-9-./_])`)

// newCharacterFilter 返回一个布尔数组，标记哪些字符被允许。
// 用字符码作为下标做 O(1) 查表，避免在热路径上逐字符比较字符串。
func newCharacterFilter(characters string) []bool {
	maxChar := 0
	for _, c := range characters {
		if maxChar < int(c) {
			maxChar = int(c)
		}
	}
	filter := make([]bool, maxChar+1)

	for _, c := range characters {
		filter[int(c)] = true
	}

	return filter
}

// escapeCheck 记录快速预检的结论。
type escapeCheck struct {
	// canSkipRegex 为 true 表示标识符无需正则展开，可直接使用。
	canSkipRegex bool
	// invalidCharFound 为 true 表示存在不受支持的字符。
	invalidCharFound bool
}

// skipRegexCheck 判断 Escape 是否可跳过正则处理。
// invalidCharFound 为 true 时一定含非法字符；为 false 时不能确定是否含非法字符。
func skipRegexCheck(ident string) escapeCheck {
	escapeCheck := escapeCheck{canSkipRegex: true, invalidCharFound: false}
	// 尽可能跳过转义
	previous_underscore := false
	for _, c := range ident {
		// 出现需要转义的字符，必须走正则路径
		if c == '/' || c == '-' || c == '.' {
			escapeCheck.canSkipRegex = false
			return escapeCheck
		}
		intc := int(c)
		if intc < 0 || intc >= len(validCharacterFilter) || !validCharacterFilter[intc] {
			escapeCheck.invalidCharFound = true
			return escapeCheck
		}
		// 连续两个下划线需要转义为 __underscores__
		if c == '_' && previous_underscore {
			escapeCheck.canSkipRegex = false
			return escapeCheck
		}

		previous_underscore = c == '_'
	}
	return escapeCheck
}

// validCharacterFilter 标记允许的字符。
var validCharacterFilter = newCharacterFilter("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_")

// Escape 转义 ident 并返回 CEL 标识符（形如 '[a-zA-Z_][a-zA-Z0-9_]*'）；
// 若 ident 不匹配支持的输入格式 `[a-zA-Z_.-/][a-zA-Z0-9_.-/]*` 则返回 false。
// 转义规则：
//   - '__' 转义为 '__underscores__'
//   - '.' 转义为 '__dot__'
//   - '-' 转义为 '__dash__'
//   - '/' 转义为 '__slash__'
//   - 与 CEL RESERVED 关键字完全相同的标识符转义为 '__{keyword}__'。关键字包括："true"、"false"、
//     "null"、"in"、"as"、"break"、"const"、"continue"、"else"、"for"、"function"、"if"、"import"、"let"、"loop"、"package"、
//     "namespace"、"return"。
func Escape(ident string) (string, bool) {
	// 空标识符或数字开头的标识符不是合法 CEL 标识符
	if len(ident) == 0 || ('0' <= ident[0] && ident[0] <= '9') {
		return "", false
	}
	// 关键字必须加前后双下划线包裹，避免与字面量混淆
	if celReservedSymbols.Has(ident) {
		return "__" + ident + "__", true
	}

	escapeCheck := skipRegexCheck(ident)
	if escapeCheck.invalidCharFound {
		return "", false
	}
	// 无需转义时直接返回，省去正则替换开销
	if escapeCheck.canSkipRegex {
		return ident, true
	}

	ok := true
	ident = expandMatcher.ReplaceAllStringFunc(ident, func(s string) string {
		switch s {
		case "__":
			return "__underscores__"
		case ".":
			return "__dot__"
		case "-":
			return "__dash__"
		case "/":
			return "__slash__"
		default: // 匹配到不受支持的字符
			ok = false
			return ""
		}
	})
	if !ok {
		return "", false
	}
	return ident, true
}

// unexpandMatcher 匹配 Escape 产生的 __name__ 形式转义序列。
var unexpandMatcher = regexp.MustCompile(`(_{2}[^_]+_{2})`)

// Unescape 还原包含 Escape 所述转义序列的 CEL 标识符；
// 若字符串包含非法转义序列则返回 false。
// 输入被假定为合法 CEL 标识符，但本函数不校验这一点。
func Unescape(escaped string) (string, bool) {
	ok := true
	escaped = unexpandMatcher.ReplaceAllStringFunc(escaped, func(s string) string {
		contents := s[2 : len(s)-2]
		switch contents {
		case "underscores":
			return "__"
		case "dot":
			return "."
		case "dash":
			return "-"
		case "slash":
			return "/"
		}
		// 关键字转义必须覆盖整个标识符；若前后还有内容则说明该序列并非关键字转义。
		if celReservedSymbols.Has(contents) {
			if len(s) != len(escaped) {
				ok = false
			}
			return contents
		}
		ok = false
		return ""
	})
	if !ok {
		return "", false
	}
	return escaped, true
}
