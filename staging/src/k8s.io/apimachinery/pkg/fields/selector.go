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
	"bytes"
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/selection"
)

// Selector 表示一个字段选择器。
//
// 字段选择器在服务端用于缩小列表结果（如 metadata.name=x），由若干
// 原子条件以 AND 组合；不支持 OR。所有实现都必须可安全并发使用。
type Selector interface {
	// Matches 判断当前选择器是否匹配给定字段集合。
	Matches(Fields) bool

	// Empty 判断当前选择器是否未对可选对象范围施加限制。
	Empty() bool

	// RequiresExactMatch 检查选择器是否要求某个字段精确等于指定值；若是则返回该值。
	RequiresExactMatch(field string) (value string, found bool)

	// Transform 对选择器中的每个条件应用转换函数并返回新副本；转换函数报错时返回错误。
	// 如果某个条件的字段名和值都被转换为空字符串，则跳过该条件。
	Transform(fn TransformFunc) (Selector, error)

	// Requirements 将选择器展开为 Requirements，向调用方暴露更详细的条件信息。
	Requirements() Requirements

	// String 返回表示当前选择器的可读字符串。
	String() string

	// DeepCopySelector 返回当前选择器的深拷贝。
	DeepCopySelector() Selector
}

// nothingSelector 是“什么都不匹配”的哨兵实现：Empty() 返回 false，因为
// 它确实限制了选择空间（虽然结果为空集）。
type nothingSelector struct{}

func (n nothingSelector) Matches(_ Fields) bool      { return false }
func (n nothingSelector) Empty() bool                { return false }
func (n nothingSelector) String() string             { return "" }
func (n nothingSelector) Requirements() Requirements { return nil }
func (n nothingSelector) DeepCopySelector() Selector { return n }
func (n nothingSelector) RequiresExactMatch(field string) (value string, found bool) {
	return "", false
}
func (n nothingSelector) Transform(fn TransformFunc) (Selector, error) { return n, nil }

// Nothing 返回一个永远无法匹配任何字段集合的选择器。
func Nothing() Selector {
	return nothingSelector{}
}

// Everything 返回一个匹配所有字段集合的选择器。
//
// 空 andTerm 恒真，因此直接复用它表示“无条件”，无需额外类型。
func Everything() Selector {
	return andTerm{}
}

// hasTerm 表示 field=value 的等值条件。value 在 String() 中会被转义，
// 但内部始终保存未转义的字面量。
type hasTerm struct {
	field, value string
}

func (t *hasTerm) Matches(ls Fields) bool {
	return ls.Get(t.field) == t.value
}

func (t *hasTerm) Empty() bool {
	return false
}

func (t *hasTerm) RequiresExactMatch(field string) (value string, found bool) {
	if t.field == field {
		return t.value, true
	}
	return "", false
}

func (t *hasTerm) Transform(fn TransformFunc) (Selector, error) {
	// Transform 应用转换函数；若转换后 field 与 value 都为空，视为该条件被
	// 移除，返回 Everything() 以便上层在 andTerm 中把它过滤掉。
	field, value, err := fn(t.field, t.value)
	if err != nil {
		return nil, err
	}
	if len(field) == 0 && len(value) == 0 {
		return Everything(), nil
	}
	return &hasTerm{field, value}, nil
}

func (t *hasTerm) Requirements() Requirements {
	return []Requirement{{
		Field:    t.field,
		Operator: selection.Equals,
		Value:    t.value,
	}}
}

func (t *hasTerm) String() string {
	return fmt.Sprintf("%v=%v", t.field, EscapeValue(t.value))
}

func (t *hasTerm) DeepCopySelector() Selector {
	if t == nil {
		return nil
	}
	out := new(hasTerm)
	*out = *t
	return out
}

// notHasTerm 表示 field!=value 条件。注意语义：字段缺失时 Get 返回 ""，
// 因此“缺失”与“值不等于目标”一样会被匹配。
type notHasTerm struct {
	field, value string
}

func (t *notHasTerm) Matches(ls Fields) bool {
	return ls.Get(t.field) != t.value
}

func (t *notHasTerm) Empty() bool {
	return false
}

func (t *notHasTerm) RequiresExactMatch(field string) (value string, found bool) {
	return "", false
}

func (t *notHasTerm) Transform(fn TransformFunc) (Selector, error) {
	field, value, err := fn(t.field, t.value)
	if err != nil {
		return nil, err
	}
	if len(field) == 0 && len(value) == 0 {
		return Everything(), nil
	}
	return &notHasTerm{field, value}, nil
}

func (t *notHasTerm) Requirements() Requirements {
	return []Requirement{{
		Field:    t.field,
		Operator: selection.NotEquals,
		Value:    t.value,
	}}
}

func (t *notHasTerm) String() string {
	return fmt.Sprintf("%v!=%v", t.field, EscapeValue(t.value))
}

func (t *notHasTerm) DeepCopySelector() Selector {
	if t == nil {
		return nil
	}
	out := new(notHasTerm)
	*out = *t
	return out
}

// andTerm 是所有子选择器的合取。nil 与空切片都表示恒真。
type andTerm []Selector

// Matches 短路求值：任一子选择器不匹配即返回 false。
func (t andTerm) Matches(ls Fields) bool {
	for _, q := range t {
		if !q.Matches(ls) {
			return false
		}
	}
	return true
}

// Empty 的判定是递归的：只有当所有子项都为空时整体才为空；
// nil 与空切片都视为无限制。
func (t andTerm) Empty() bool {
	if t == nil {
		return true
	}
	if len([]Selector(t)) == 0 {
		return true
	}
	for i := range t {
		if !t[i].Empty() {
			return false
		}
	}
	return true
}

// RequiresExactMatch 只要任一子项要求该字段精确匹配就返回，用于让调用方
// 把选择器下推为更高效的单键查询（如 API server 的字段索引）。
func (t andTerm) RequiresExactMatch(field string) (string, bool) {
	if t == nil || len([]Selector(t)) == 0 {
		return "", false
	}
	for i := range t {
		if value, found := t[i].RequiresExactMatch(field); found {
			return value, found
		}
	}
	return "", false
}

// Transform 逐项转换，并丢弃转换后变为空的项，避免生成无意义的空条件。
func (t andTerm) Transform(fn TransformFunc) (Selector, error) {
	next := make([]Selector, 0, len([]Selector(t)))
	for _, s := range []Selector(t) {
		n, err := s.Transform(fn)
		if err != nil {
			return nil, err
		}
		if !n.Empty() {
			next = append(next, n)
		}
	}
	return andTerm(next), nil
}

// Requirements 把嵌套选择器拍平成条件列表，供索引层使用。
func (t andTerm) Requirements() Requirements {
	reqs := make([]Requirement, 0, len(t))
	for _, s := range []Selector(t) {
		rs := s.Requirements()
		reqs = append(reqs, rs...)
	}
	return reqs
}

// String 用逗号连接各条件；这是客户端可解析的规范形式。
func (t andTerm) String() string {
	var terms []string
	for _, q := range t {
		terms = append(terms, q.String())
	}
	return strings.Join(terms, ",")
}

func (t andTerm) DeepCopySelector() Selector {
	if t == nil {
		return nil
	}
	out := make([]Selector, len(t))
	for i := range t {
		out[i] = t[i].DeepCopySelector()
	}
	return andTerm(out)
}

// SelectorFromSet 创建一个精确匹配给定 Set 的选择器；nil Set 与 Everything() 等价。
//
// 单个条件返回原子类型而不是 andTerm，避免不必要的间接层；注意 map 迭代
// 顺序随机，但 String() 输出顺序因此不保证稳定。
func SelectorFromSet(ls Set) Selector {
	if ls == nil {
		return Everything()
	}
	items := make([]Selector, 0, len(ls))
	for field, value := range ls {
		items = append(items, &hasTerm{field: field, value: value})
	}
	if len(items) == 1 {
		return items[0]
	}
	return andTerm(items)
}

// valueEscaper 在反斜杠、逗号和等号前添加反斜杠进行转义。
var valueEscaper = strings.NewReplacer(
	// 先转义反斜杠字符。
	`\`, `\\`,
	// 再转义逗号和等号，确保字段选择器中的值能够被无歧义地解析。
	`,`, `\,`,
	`=`, `\=`,
)

// EscapeValue 将任意字面字符串转义为可安全用作字段选择器值的形式。
func EscapeValue(s string) string {
	return valueEscaper.Replace(s)
}

// InvalidEscapeSequence 表示反解字段选择器时遇到了非法转义序列。
//
// sequence 保存原始的两字符转义（如 `\x`），便于错误信息定位。
type InvalidEscapeSequence struct {
	sequence string
}

func (i InvalidEscapeSequence) Error() string {
	return fmt.Sprintf("invalid field selector: invalid escape sequence: %s", i.sequence)
}

// UnescapedRune 表示反解字段选择器时遇到了未转义的特殊字符。
//
// 出现在值的字面量中的 `,` 或 `=` 未转义时触发。
type UnescapedRune struct {
	r rune
}

func (i UnescapedRune) Error() string {
	return fmt.Sprintf("invalid field selector: unescaped character in value: %v", i.r)
}

// UnescapeValue 反解字段选择器中的值并返回原始字面字符串。
// 如果字符串不包含转义或特殊字符，可能直接返回原字符串而不分配新内存。
//
// 只反解 `\\`、`\,`、`\=` 三种转义；其余转义序列报错而不是原样保留，
// 避免同一输入存在多种解释。快速路径避免无特殊字符时的内存分配。
func UnescapeValue(s string) (string, error) {
	// 没有转义或特殊字符时直接返回，避免不必要的内存分配。
	if !strings.ContainsAny(s, `\,=`) {
		return s, nil
	}

	v := bytes.NewBuffer(make([]byte, 0, len(s)))
	inSlash := false
	for _, c := range s {
		if inSlash {
			switch c {
			case '\\', ',', '=':
				// 对于合法转义序列，去掉转义用的反斜杠。
				v.WriteRune(c)
			default:
				// 遇到不支持的转义序列时返回错误。
				return "", InvalidEscapeSequence{sequence: string([]rune{'\\', c})}
			}
			inSlash = false
			continue
		}

		switch c {
		case '\\':
			inSlash = true
		case ',', '=':
			// 字段选择器的值中不允许出现未转义的逗号或等号。
			return "", UnescapedRune{r: c}
		default:
			v.WriteRune(c)
		}
	}

	// 以单个反斜杠结尾表示转义不完整，属于非法序列。
	if inSlash {
		return "", InvalidEscapeSequence{sequence: "\\"}
	}

	return v.String(), nil
}

// ParseSelectorOrDie 将选择器字符串解析为可用于匹配的对象；解析失败时直接 panic。
func ParseSelectorOrDie(s string) Selector {
	selector, err := ParseSelector(s)
	if err != nil {
		panic(err)
	}
	return selector
}

// ParseSelector 将选择器字符串解析为可用于匹配的对象；解析失败时返回错误。
//
// 使用恒等变换解析，语法为 `field<op>value`，多个条件以逗号分隔且默认
// 按 AND 组合。
func ParseSelector(selector string) (Selector, error) {
	return parseSelector(selector,
		func(lhs, rhs string) (newLhs, newRhs string, err error) {
			return lhs, rhs, nil
		})
}

// ParseAndTransformSelector 解析选择器，并对解析出的每个条件应用指定的 TransformFunc。
func ParseAndTransformSelector(selector string, fn TransformFunc) (Selector, error) {
	return parseSelector(selector, fn)
}

// TransformFunc 定义字段选择器条件的转换函数。
//
// 允许把字段名/值映射为内部表示（例如把标签键做别名转换），返回空串表示
// 该条件应被删除。
type TransformFunc func(field, value string) (newField, newValue string, err error)

// splitTerms 将字段选择器按逗号拆分为多个条件。
// 由反斜杠转义的逗号属于值本身，不作为分隔符；返回的片段会保留该反斜杠。
//
// 注意：函数只识别 `\,`，不校验其他转义；非法转义留给后续 UnescapeValue
// 报错，以便错误信息更精确。
func splitTerms(fieldSelector string) []string {
	if len(fieldSelector) == 0 {
		return nil
	}

	terms := make([]string, 0, 1)
	startIndex := 0
	inSlash := false
	for i, c := range fieldSelector {
		switch {
		case inSlash:
			inSlash = false
		case c == '\\':
			inSlash = true
		case c == ',':
			terms = append(terms, fieldSelector[startIndex:i])
			startIndex = i + 1
		}
	}

	terms = append(terms, fieldSelector[startIndex:])

	return terms
}

const (
	notEqualOperator    = "!="
	doubleEqualOperator = "=="
	equalOperator       = "="
)

// termOperators 保存字段选择器支持的比较操作符。
// == 与 = 语义相同，但必须先检查 ==，否则右侧值会残留一个前导等号。
var termOperators = []string{notEqualOperator, doubleEqualOperator, equalOperator}

// splitTerm 从单个条件中解析左操作数、操作符和右操作数，并返回解析是否成功。
// 左操作数不支持特殊字符转义，因此以遇到的第一个合法操作符作为切分位置；
// 右操作数按原样返回，由调用方决定是否执行反转义。
//
// 逐字符扫描以正确处理 `x!=y` 与 `x==y`：`=` 必须在 `!=`/`==` 之后
// 检查，否则会在 rhs 上留下前导 `=`。
func splitTerm(term string) (lhs, op, rhs string, ok bool) {
	for i := range term {
		remaining := term[i:]
		for _, op := range termOperators {
			if strings.HasPrefix(remaining, op) {
				return term[0:i], op, term[i+len(op):], true
			}
		}
	}
	return "", "", "", false
}

func parseSelector(selector string, fn TransformFunc) (Selector, error) {
	// 排序让相同选择器集合的解析结果在 String() 中稳定输出，便于比较与缓存。
	parts := splitTerms(selector)
	sort.StringSlice(parts).Sort()
	var items []Selector
	for _, part := range parts {
		// 容忍尾随逗号等产生的空项，而不是报错。
		if part == "" {
			continue
		}
		lhs, op, rhs, ok := splitTerm(part)
		if !ok {
			return nil, fmt.Errorf("invalid selector: '%s'; can't understand '%s'", selector, part)
		}
		// 解析阶段统一对 rhs 反解转义，lhs 不支持转义。
		unescapedRHS, err := UnescapeValue(rhs)
		if err != nil {
			return nil, err
		}
		switch op {
		case notEqualOperator:
			items = append(items, &notHasTerm{field: lhs, value: unescapedRHS})
		case doubleEqualOperator:
			items = append(items, &hasTerm{field: lhs, value: unescapedRHS})
		case equalOperator:
			items = append(items, &hasTerm{field: lhs, value: unescapedRHS})
		default:
			return nil, fmt.Errorf("invalid selector: '%s'; can't understand '%s'", selector, part)
		}
	}
	if len(items) == 1 {
		return items[0].Transform(fn)
	}
	return andTerm(items).Transform(fn)
}

// OneTermEqualSelector 创建一个选择器，用于匹配指定字段等于给定值的对象。该构造过程不会返回错误。
func OneTermEqualSelector(k, v string) Selector {
	return &hasTerm{field: k, value: v}
}

// OneTermNotEqualSelector 创建一个选择器，用于匹配指定字段不等于给定值的对象。该构造过程不会返回错误。
func OneTermNotEqualSelector(k, v string) Selector {
	return &notHasTerm{field: k, value: v}
}

// AndSelectors 创建一个选择器，其匹配结果是所有输入选择器的逻辑与。
//
// 不做任何归一化：传入空列表会得到恒真的 Everything()。
func AndSelectors(selectors ...Selector) Selector {
	return andTerm(selectors)
}
