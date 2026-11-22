/*
Copyright 2015 The Kubernetes Authors.

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

package fields

import (
	"reflect"
	"testing"
)

// TestSplitTerms 覆盖逗号分隔的字段选择器切分，重点验证转义逗号/反斜杠
// 不会被误当作分隔符，以及空项与多字节字符的处理。
func TestSplitTerms(t *testing.T) {
	testcases := map[string][]string{
		// 简单选择器
		`a`:                            {`a`},
		`a=avalue`:                     {`a=avalue`},
		`a=avalue,b=bvalue`:            {`a=avalue`, `b=bvalue`},
		`a=avalue,b==bvalue,c!=cvalue`: {`a=avalue`, `b==bvalue`, `c!=cvalue`},

		// 空项：保留空字符串以维持位置。
		``:     nil,
		`a=a,`: {`a=a`, ``},
		`,a=a`: {``, `a=a`},

		// 转义值
		`k=\,,k2=v2`:   {`k=\,`, `k2=v2`},   // 值中的转义逗号
		`k=\\,k2=v2`:   {`k=\\`, `k2=v2`},   // 转义反斜杠，逗号未转义
		`k=\\\,,k2=v2`: {`k=\\\,`, `k2=v2`}, // 转义反斜杠和逗号
		`k=\a\b\`:      {`k=\a\b\`},         // 非转义序列
		`k=\`:          {`k=\`},             // 孤立反斜杠

		// 多字节字符
		`함=수,목=록`: {`함=수`, `목=록`},
	}

	for selector, expectedTerms := range testcases {
		if terms := splitTerms(selector); !reflect.DeepEqual(terms, expectedTerms) {
			t.Errorf("splitSelectors(`%s`): Expected\n%#v\ngot\n%#v", selector, expectedTerms, terms)
		}
	}
}

// TestSplitTerm 覆盖单个 term 的 key/operator/value 拆分，
// 包括 ==/!= 等操作符、转义字符与非法输入。
func TestSplitTerm(t *testing.T) {
	testcases := map[string]struct {
		lhs string
		op  string
		rhs string
		ok  bool
	}{
		// 简单 term
		`a=value`:  {lhs: `a`, op: `=`, rhs: `value`, ok: true},
		`b==value`: {lhs: `b`, op: `==`, rhs: `value`, ok: true},
		`c!=value`: {lhs: `c`, op: `!=`, rhs: `value`, ok: true},

		// 空或非法 term
		``:  {lhs: ``, op: ``, rhs: ``, ok: false},
		`a`: {lhs: ``, op: ``, rhs: ``, ok: false},

		// 转义值
		`k=\,`:          {lhs: `k`, op: `=`, rhs: `\,`, ok: true},
		`k=\=`:          {lhs: `k`, op: `=`, rhs: `\=`, ok: true},
		`k=\\\a\b\=\,\`: {lhs: `k`, op: `=`, rhs: `\\\a\b\=\,\`, ok: true},

		// 多字节字符
		`함=수`: {lhs: `함`, op: `=`, rhs: `수`, ok: true},
	}

	for term, expected := range testcases {
		lhs, op, rhs, ok := splitTerm(term)
		if lhs != expected.lhs || op != expected.op || rhs != expected.rhs || ok != expected.ok {
			t.Errorf(
				"splitTerm(`%s`): Expected\n%s,%s,%s,%v\nGot\n%s,%s,%s,%v",
				term,
				expected.lhs, expected.op, expected.rhs, expected.ok,
				lhs, op, rhs, ok,
			)
		}
	}
}

// TestEscapeValue 验证 EscapeValue/UnescapeValue 互逆，
// 且非法转义序列返回 InvalidEscapeSequence 类型错误。
func TestEscapeValue(t *testing.T) {
	// map 的值是规范化后的转义形式。
	testcases := map[string]string{
		``:      ``,
		`a`:     `a`,
		`=`:     `\=`,
		`,`:     `\,`,
		`\`:     `\\`,
		`\=\,\`: `\\\=\\\,\\`,
	}

	for unescapedValue, escapedValue := range testcases {
		actualEscaped := EscapeValue(unescapedValue)
		if actualEscaped != escapedValue {
			t.Errorf("EscapeValue(%s): expected %s, got %s", unescapedValue, escapedValue, actualEscaped)
		}

		actualUnescaped, err := UnescapeValue(escapedValue)
		if err != nil {
			t.Errorf("UnescapeValue(%s): unexpected error %v", escapedValue, err)
		}
		if actualUnescaped != unescapedValue {
			t.Errorf("UnescapeValue(%s): expected %s, got %s", escapedValue, unescapedValue, actualUnescaped)
		}
	}

	// 测试非法转义序列
	invalidTestcases := []string{
		`\`,   // 孤立斜杠非法
		`\\\`, // 孤立斜杠非法
		`\a`,  // 未识别的转义序列非法
	}
	for _, invalidValue := range invalidTestcases {
		_, err := UnescapeValue(invalidValue)
		if _, ok := err.(InvalidEscapeSequence); !ok || err == nil {
			t.Errorf("UnescapeValue(%s): expected invalid escape sequence error, got %#v", invalidValue, err)
		}
	}
}

// TestSelectorParse 覆盖 ParseSelector 的合法/非法输入，
// 并验证合法输入重新序列化后与原文一致。
func TestSelectorParse(t *testing.T) {
	testGoodStrings := []string{
		"x=a,y=b,z=c",
		"",
		"x!=a,y=b",
		`x=a||y\=b`,
		`x=a\=\=b`,
	}
	testBadStrings := []string{
		"x=a||y=b",
		"x==a==b",
		"x=a,b",
		"x in (a)",
		"x in (a,b,c)",
		"x",
	}
	for _, test := range testGoodStrings {
		lq, err := ParseSelector(test)
		if err != nil {
			t.Errorf("%v: error %v (%#v)\n", test, err, err)
		}
		if test != lq.String() {
			t.Errorf("%v restring gave: %v\n", test, lq.String())
		}
	}
	for _, test := range testBadStrings {
		_, err := ParseSelector(test)
		if err == nil {
			t.Errorf("%v: did not get expected error\n", test)
		}
	}
}

// TestDeterministicParse 验证相同条件以不同顺序输入时，解析结果序列化一致。
func TestDeterministicParse(t *testing.T) {
	s1, err := ParseSelector("x=a,a=x")
	s2, err2 := ParseSelector("a=x,x=a")
	if err != nil || err2 != nil {
		t.Errorf("Unexpected parse error")
	}
	if s1.String() != s2.String() {
		t.Errorf("Non-deterministic parse")
	}
}

// expectMatch 断言选择器能匹配给定字段集。
func expectMatch(t *testing.T, selector string, ls Set) {
	lq, err := ParseSelector(selector)
	if err != nil {
		t.Errorf("Unable to parse %v as a selector\n", selector)
		return
	}
	if !lq.Matches(ls) {
		t.Errorf("Wanted %s to match '%s', but it did not.\n", selector, ls)
	}
}

// expectNoMatch 断言选择器不能匹配给定字段集。
func expectNoMatch(t *testing.T, selector string, ls Set) {
	lq, err := ParseSelector(selector)
	if err != nil {
		t.Errorf("Unable to parse %v as a selector\n", selector)
		return
	}
	if lq.Matches(ls) {
		t.Errorf("Wanted '%s' to not match '%s', but it did.", selector, ls)
	}
}

// TestEverything 验证 Everything() 匹配任意字段集且被视为空选择器。
func TestEverything(t *testing.T) {
	if !Everything().Matches(Set{"x": "y"}) {
		t.Errorf("Nil selector didn't match")
	}
	if !Everything().Empty() {
		t.Errorf("Everything was not empty")
	}
}

// TestSelectorMatches 覆盖解析后的选择器匹配，包括相等/不等、多个条件，
// 以及含转义字符的值。
func TestSelectorMatches(t *testing.T) {
	expectMatch(t, "", Set{"x": "y"})
	expectMatch(t, "x=y", Set{"x": "y"})
	expectMatch(t, "x=y,z=w", Set{"x": "y", "z": "w"})
	expectMatch(t, "x!=y,z!=w", Set{"x": "z", "z": "a"})
	expectMatch(t, "notin=in", Set{"notin": "in"}) // in 和 notin 在 exactMatch 中
	expectNoMatch(t, "x=y", Set{"x": "z"})
	expectNoMatch(t, "x=y,z=w", Set{"x": "w", "z": "w"})
	expectNoMatch(t, "x!=y,z!=w", Set{"x": "z", "z": "w"})

	fieldset := Set{
		"foo":     "bar",
		"baz":     "blah",
		"complex": `=value\,\`,
	}
	expectMatch(t, "foo=bar", fieldset)
	expectMatch(t, "baz=blah", fieldset)
	expectMatch(t, "foo=bar,baz=blah", fieldset)
	expectMatch(t, `foo=bar,baz=blah,complex=\=value\\\,\\`, fieldset)
	expectNoMatch(t, "foo=blah", fieldset)
	expectNoMatch(t, "baz=bar", fieldset)
	expectNoMatch(t, "foo=bar,foobar=bar,baz=blah", fieldset)
}

// TestOneTermEqualSelector 验证 OneTermEqualSelector 的匹配与不匹配。
func TestOneTermEqualSelector(t *testing.T) {
	if !OneTermEqualSelector("x", "y").Matches(Set{"x": "y"}) {
		t.Errorf("No match when match expected.")
	}
	if OneTermEqualSelector("x", "y").Matches(Set{"x": "z"}) {
		t.Errorf("Match when none expected.")
	}
}

// expectMatchDirect 断言由 Set 直接构造的选择器能匹配给定字段集。
func expectMatchDirect(t *testing.T, selector, ls Set) {
	if !SelectorFromSet(selector).Matches(ls) {
		t.Errorf("Wanted %s to match '%s', but it did not.\n", selector, ls)
	}
}

// expectNoMatchDirect 断言由 Set 直接构造的选择器不能匹配给定字段集。
func expectNoMatchDirect(t *testing.T, selector, ls Set) {
	if SelectorFromSet(selector).Matches(ls) {
		t.Errorf("Wanted '%s' to not match '%s', but it did.", selector, ls)
	}
}

// TestSetMatches 覆盖 SelectorFromSet 的匹配语义：子集匹配、空选择器匹配全部。
func TestSetMatches(t *testing.T) {
	labelset := Set{
		"foo": "bar",
		"baz": "blah",
	}
	expectMatchDirect(t, Set{}, labelset)
	expectMatchDirect(t, Set{"foo": "bar"}, labelset)
	expectMatchDirect(t, Set{"baz": "blah"}, labelset)
	expectMatchDirect(t, Set{"foo": "bar", "baz": "blah"}, labelset)
	expectNoMatchDirect(t, Set{"foo": "=blah"}, labelset)
	expectNoMatchDirect(t, Set{"baz": "=bar"}, labelset)
	expectNoMatchDirect(t, Set{"foo": "=bar", "foobar": "bar", "baz": "blah"}, labelset)
}

// TestNilMapIsValid 验证 nil Set 生成的选择器等价于 Everything 且 Empty 为 true。
func TestNilMapIsValid(t *testing.T) {
	selector := Set(nil).AsSelector()
	if selector == nil {
		t.Errorf("Selector for nil set should be Everything")
	}
	if !selector.Empty() {
		t.Errorf("Selector for nil set should be Empty")
	}
}

// TestSetIsEmpty 覆盖空判断在空 Set、nil/嵌套 andTerm、hasTerm/notHasTerm 上的行为。
func TestSetIsEmpty(t *testing.T) {
	if !(Set{}).AsSelector().Empty() {
		t.Errorf("Empty set should be empty")
	}
	if !(andTerm(nil)).Empty() {
		t.Errorf("Nil andTerm should be empty")
	}
	if (&hasTerm{}).Empty() {
		t.Errorf("hasTerm should not be empty")
	}
	if (&notHasTerm{}).Empty() {
		t.Errorf("notHasTerm should not be empty")
	}
	if !(andTerm{andTerm{}}).Empty() {
		t.Errorf("Nested andTerm should be empty")
	}
	if (andTerm{&hasTerm{"a", "b"}}).Empty() {
		t.Errorf("Nested andTerm should not be empty")
	}
}

// TestRequiresExactMatch 覆盖从选择器中提取精确匹配值的行为：
// 只有单值的 Equals/DoubleEquals/In 才会返回值，notHasTerm 不返回。
func TestRequiresExactMatch(t *testing.T) {
	testCases := map[string]struct {
		S     Selector
		Label string
		Value string
		Found bool
	}{
		"empty set":                 {Set{}.AsSelector(), "test", "", false},
		"empty hasTerm":             {&hasTerm{}, "test", "", false},
		"skipped hasTerm":           {&hasTerm{"a", "b"}, "test", "", false},
		"valid hasTerm":             {&hasTerm{"test", "b"}, "test", "b", true},
		"valid hasTerm no value":    {&hasTerm{"test", ""}, "test", "", true},
		"valid notHasTerm":          {&notHasTerm{"test", "b"}, "test", "", false},
		"valid notHasTerm no value": {&notHasTerm{"test", ""}, "test", "", false},
		"nil andTerm":               {andTerm(nil), "test", "", false},
		"empty andTerm":             {andTerm{}, "test", "", false},
		"nested andTerm":            {andTerm{andTerm{}}, "test", "", false},
		"nested andTerm matches":    {andTerm{&hasTerm{"test", "b"}}, "test", "b", true},
		"andTerm with non-match":    {andTerm{&hasTerm{}, &hasTerm{"test", "b"}}, "test", "b", true},
	}
	for k, v := range testCases {
		value, found := v.S.RequiresExactMatch(v.Label)
		if value != v.Value {
			t.Errorf("%s: expected value %s, got %s", k, v.Value, value)
		}
		if found != v.Found {
			t.Errorf("%s: expected found %t, got %t", k, v.Found, found)
		}
	}
}

// TestTransform 覆盖 ParseAndTransformSelector 的字段重写与删除：
// 返回空字段名表示删除该条件，全部删除后选择器变为空。
func TestTransform(t *testing.T) {
	testCases := []struct {
		name      string
		selector  string
		transform func(field, value string) (string, string, error)
		result    string
		isEmpty   bool
	}{
		{
			name:      "empty selector",
			selector:  "",
			transform: func(field, value string) (string, string, error) { return field, value, nil },
			result:    "",
			isEmpty:   true,
		},
		{
			name:      "no-op transform",
			selector:  "a=b,c=d",
			transform: func(field, value string) (string, string, error) { return field, value, nil },
			result:    "a=b,c=d",
			isEmpty:   false,
		},
		{
			name:     "transform one field",
			selector: "a=b,c=d",
			transform: func(field, value string) (string, string, error) {
				if field == "a" {
					return "e", "f", nil
				}
				return field, value, nil
			},
			result:  "e=f,c=d",
			isEmpty: false,
		},
		{
			name:      "remove field to make empty",
			selector:  "a=b",
			transform: func(field, value string) (string, string, error) { return "", "", nil },
			result:    "",
			isEmpty:   true,
		},
		{
			name:     "remove only one field",
			selector: "a=b,c=d,e=f",
			transform: func(field, value string) (string, string, error) {
				if field == "c" {
					return "", "", nil
				}
				return field, value, nil
			},
			result:  "a=b,e=f",
			isEmpty: false,
		},
	}

	for i, tc := range testCases {
		result, err := ParseAndTransformSelector(tc.selector, tc.transform)
		if err != nil {
			t.Errorf("[%d] unexpected error during Transform: %v", i, err)
		}
		if result.Empty() != tc.isEmpty {
			t.Errorf("[%d] expected empty: %t, got: %t", i, tc.isEmpty, result.Empty())
		}
		if result.String() != tc.result {
			t.Errorf("[%d] unexpected result: %s", i, result.String())
		}
	}

}
