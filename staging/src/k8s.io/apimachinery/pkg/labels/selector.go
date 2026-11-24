/*
Copyright 2014 The Kubernetes Authors.

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

package labels

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/api/validate/content"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

var (
	// unaryOperators 是只需键、不需要值的操作符。
	unaryOperators = []string{
		string(selection.Exists), string(selection.DoesNotExist),
	}
	// binaryOperators 是需要键和值的操作符。
	binaryOperators = []string{
		string(selection.In), string(selection.NotIn),
		string(selection.Equals), string(selection.DoubleEquals), string(selection.NotEquals),
		string(selection.GreaterThan), string(selection.LessThan),
	}
	// validRequirementOperators 汇总所有合法操作符，用于错误提示。
	validRequirementOperators = append(binaryOperators, unaryOperators...)
)

// Requirements 是各 Requirement 的逻辑与（AND）。
type Requirements []Requirement

// String 以逗号分隔的形式返回所有 Requirement。
func (r Requirements) String() string {
	var sb strings.Builder

	for i, requirement := range r {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(requirement.String())
	}

	return sb.String()
}

// Selector 表示标签选择器。
type Selector interface {
	// Matches 返回该选择器是否匹配给定的标签集合。
	Matches(Labels) bool

	// Empty 返回该选择器是否不限制选择范围。
	Empty() bool

	// String 返回该选择器的人类可读字符串表示。
	String() string

	// Add 向选择器添加条件。
	Add(r ...Requirement) Selector

	// Requirements 把接口转换为 Requirements，以暴露更详细的选择信息。
	// 若存在查询参数，会返回转换后的条件且 selectable=true。
	// 若该选择器不选择任何对象，则返回 selectable=false。
	Requirements() (requirements Requirements, selectable bool)

	// DeepCopySelector 对选择器做深拷贝。
	DeepCopySelector() Selector

	// RequiresExactMatch 允许调用方探查选择器是否要求某个标签必须被设置为
	// 特定值，若是则返回该值。
	RequiresExactMatch(label string) (value string, found bool)
}

// 共享该实例每次使用可省一次分配；它是不可变的，因此安全。
var sharedEverythingSelector Selector = internalSelector{}

// Everything 返回匹配所有标签的选择器。
func Everything() Selector {
	return sharedEverythingSelector
}

type nothingSelector struct{}

func (n nothingSelector) Matches(_ Labels) bool              { return false }
func (n nothingSelector) Empty() bool                        { return false }
func (n nothingSelector) String() string                     { return "" }
func (n nothingSelector) Add(_ ...Requirement) Selector      { return n }
func (n nothingSelector) Requirements() (Requirements, bool) { return nil, false }
func (n nothingSelector) DeepCopySelector() Selector         { return n }
func (n nothingSelector) RequiresExactMatch(label string) (value string, found bool) {
	return "", false
}

// 共享该实例每次使用可省一次分配；它是不可变的，因此安全。
var sharedNothingSelector Selector = nothingSelector{}

// Nothing 返回不匹配任何标签的选择器。
func Nothing() Selector {
	return sharedNothingSelector
}

// MatchesNothing 仅对确定不匹配任何对象的选择器返回 true。
// 目前只识别 `labels.Nothing()`，未来可能扩展识别更多选择器。
//
// 注意：当前实现不检测选择器内部冲突（例如 a=a,a!=a）。
// 未来可加入此类检测。
func MatchesNothing(selector Selector) bool {
	return selector == sharedNothingSelector
}

// NewSelector 返回一个 nil 选择器。
func NewSelector() Selector {
	return internalSelector(nil)
}

type internalSelector []Requirement

// DeepCopy 返回 internalSelector 的深拷贝；nil 保持为 nil。
func (s internalSelector) DeepCopy() internalSelector {
	if s == nil {
		return nil
	}
	result := make([]Requirement, len(s))
	for i := range s {
		s[i].DeepCopyInto(&result[i])
	}
	return result
}

// DeepCopySelector 实现 Selector 接口。
func (s internalSelector) DeepCopySelector() Selector {
	return s.DeepCopy()
}

// ByKey 按 key 排序 Requirement，以获得确定性的解析结果。
type ByKey []Requirement

func (a ByKey) Len() int { return len(a) }

func (a ByKey) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

func (a ByKey) Less(i, j int) bool { return a[i].key < a[j].key }

// Requirement 包含值、键以及连接键值的操作符。
// Requirement 的零值无效。
// Requirement 同时实现基于集合的匹配与精确匹配。
// 应通过 NewRequirement 构造函数创建有效的 Requirement。
// +k8s:deepcopy-gen=true
type Requirement struct {
	key      string
	operator selection.Operator
	// 绝大多数情况下这里最多只有一个值。
	// 对单元素切片操作通常比对单元素 map 更快，因此使用切片。
	strValues []string
}

// NewRequirement 是 Requirement 的构造函数。
// 违反以下任一规则都会返回错误：
//  1. 操作符只能是 In、NotIn、Equals、DoubleEquals、Gt、Lt、NotEquals、Exists 或 DoesNotExist。
//  2. 若操作符为 In 或 NotIn，值集合必须非空。
//  3. 若操作符为 Equals、DoubleEquals 或 NotEquals，值集合必须恰好包含一个值。
//  4. 若操作符为 Exists 或 DoesNotExist，值集合必须为空。
//  5. 若操作符为 Gt 或 Lt，值集合必须只包含一个值，且该值会被解释为整数。
//  6. key 因长度或字符序列非法。详见 validateLabelKey。
//
// 输入值集合中空字符串是合法值。
// 若返回的 error 非 nil，保证是聚合后的 field.ErrorList。
func NewRequirement(key string, op selection.Operator, vals []string, opts ...field.PathOption) (*Requirement, error) {
	var allErrs field.ErrorList
	path := field.ToPath(opts...)
	if err := validateLabelKey(key, path.Child("key")); err != nil {
		allErrs = append(allErrs, err)
	}

	valuePath := path.Child("values")
	switch op {
	case selection.In, selection.NotIn:
		if len(vals) == 0 {
			allErrs = append(allErrs, field.Invalid(valuePath, vals, "for 'in', 'notin' operators, values set can't be empty"))
		}
	case selection.Equals, selection.DoubleEquals, selection.NotEquals:
		if len(vals) != 1 {
			allErrs = append(allErrs, field.Invalid(valuePath, vals, "exact-match compatibility requires one single value"))
		}
	case selection.Exists, selection.DoesNotExist:
		if len(vals) != 0 {
			allErrs = append(allErrs, field.Invalid(valuePath, vals, "values set must be empty for exists and does not exist"))
		}
	case selection.GreaterThan, selection.LessThan:
		if len(vals) != 1 {
			allErrs = append(allErrs, field.Invalid(valuePath, vals, "for 'Gt', 'Lt' operators, exactly one value is required"))
		}
		for i := range vals {
			if _, err := strconv.ParseInt(vals[i], 10, 64); err != nil {
				allErrs = append(allErrs, field.Invalid(valuePath.Index(i), vals[i], "for 'Gt', 'Lt' operators, the value must be an integer"))
			}
		}
	default:
		allErrs = append(allErrs, field.NotSupported(path.Child("operator"), op, validRequirementOperators))
	}

	for i := range vals {
		if err := validateLabelValue(key, vals[i], valuePath.Index(i)); err != nil {
			allErrs = append(allErrs, err)
		}
	}
	return &Requirement{key: key, operator: op, strValues: vals}, allErrs.ToAggregate()
}

// hasValue 判断值是否在 strValues 中。
func (r *Requirement) hasValue(value string) bool {
	for i := range r.strValues {
		if r.strValues[i] == value {
			return true
		}
	}
	return false
}

// Matches 返回 Requirement 是否匹配输入 Labels。
// 以下情况视为匹配：
//  1. 操作符为 Exists 且 Labels 含有 Requirement 的 key。
//  2. 操作符为 In，Labels 含有该 key，且该 key 的值在 Requirement 的值集合中。
//  3. 操作符为 NotIn，Labels 含有该 key，且该 key 的值不在 Requirement 的值集合中。
//  4. 操作符为 DoesNotExist 或 NotIn，且 Labels 不含该 key。
//  5. 操作符为 GreaterThan 或 LessThan，Labels 含有该 key，且对应值满足数学不等式。
func (r *Requirement) Matches(ls Labels) bool {
	switch r.operator {
	case selection.In, selection.Equals, selection.DoubleEquals:
		val, exists := ls.Lookup(r.key)
		if !exists {
			return false
		}
		return r.hasValue(val)
	case selection.NotIn, selection.NotEquals:
		val, exists := ls.Lookup(r.key)
		if !exists {
			return true
		}
		return !r.hasValue(val)
	case selection.Exists:
		return ls.Has(r.key)
	case selection.DoesNotExist:
		return !ls.Has(r.key)
	case selection.GreaterThan, selection.LessThan:
		val, exists := ls.Lookup(r.key)
		if !exists {
			return false
		}
		lsValue, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			//nolint:logcheck // 为此扩展 API 以支持带上下文的结构化日志，收益不足以抵消复杂度。
			klog.V(10).InfoS("ParseInt failed", "value", val, "label", ls, "err", err)
			return false
		}

		// r.strValues 应只有一个值，且可转换为整数。
		if len(r.strValues) != 1 {
			//nolint:logcheck // 为此扩展 API 以支持带上下文的结构化日志，收益不足以抵消复杂度。
			klog.V(10).InfoS("Invalid values count: for 'Gt', 'Lt' operators, exactly one value is required", "count", len(r.strValues), "requirement", r)
			return false
		}

		var rValue int64
		for i := range r.strValues {
			rValue, err = strconv.ParseInt(r.strValues[i], 10, 64)
			if err != nil {
				//nolint:logcheck // 为此扩展 API 以支持带上下文的结构化日志，收益不足以抵消复杂度。
				klog.V(10).InfoS("ParseInt failed: for 'Gt', 'Lt' operators, the value must be an integer", "value", r.strValues[i], "requirement", r, "err", err)
				return false
			}
		}
		return (r.operator == selection.GreaterThan && lsValue > rValue) || (r.operator == selection.LessThan && lsValue < rValue)
	default:
		return false
	}
}

// Key 返回 requirement 的键。
func (r *Requirement) Key() string {
	return r.key
}

// Operator 返回 requirement 的操作符。
func (r *Requirement) Operator() selection.Operator {
	return r.operator
}

// Values 以集合形式返回 requirement 的值。
func (r *Requirement) Values() sets.String {
	ret := sets.String{}
	for i := range r.strValues {
		ret.Insert(r.strValues[i])
	}
	return ret
}

// ValuesUnsorted 返回传给 NewRequirement 的值副本，保持原始顺序、不排序。
func (r *Requirement) ValuesUnsorted() []string {
	ret := make([]string, 0, len(r.strValues))
	ret = append(ret, r.strValues...)
	return ret
}

// Equal 判断两个 requirement 是否相等。
func (r Requirement) Equal(x Requirement) bool {
	if r.key != x.key {
		return false
	}
	if r.operator != x.operator {
		return false
	}
	return slices.Equal(r.strValues, x.strValues)
}

// Empty 返回 internalSelector 是否不限制选择范围。
func (s internalSelector) Empty() bool {
	if s == nil {
		return true
	}
	return len(s) == 0
}

// String 返回该 Requirement 的人类可读字符串。
// 对无效 Requirement 调用会得到错误形式；请使用 NewRequirement 创建有效值。
func (r *Requirement) String() string {
	var sb strings.Builder
	sb.Grow(
		// r.key 的长度
		len(r.key) +
			// 'r.operator' 的长度 + 最坏情况下（'in' 和 'notin'）2 个空格
			len(r.operator) + 2 +
			// r.strValues 切片长度乘以启发式的每词 5 个字符
			+5*len(r.strValues))
	if r.operator == selection.DoesNotExist {
		sb.WriteString("!")
	}
	sb.WriteString(r.key)

	switch r.operator {
	case selection.Equals:
		sb.WriteString("=")
	case selection.DoubleEquals:
		sb.WriteString("==")
	case selection.NotEquals:
		sb.WriteString("!=")
	case selection.In:
		sb.WriteString(" in ")
	case selection.NotIn:
		sb.WriteString(" notin ")
	case selection.GreaterThan:
		sb.WriteString(">")
	case selection.LessThan:
		sb.WriteString("<")
	case selection.Exists, selection.DoesNotExist:
		return sb.String()
	}

	switch r.operator {
	case selection.In, selection.NotIn:
		sb.WriteString("(")
	}
	if len(r.strValues) == 1 {
		sb.WriteString(r.strValues[0])
	} else { // 只可能是 > 1，因为 NewRequirement 禁止 == 0
		// 输出时规范化值顺序，但不修改内存中的选择器表示，
		// 既避免不必要的归一化，也确保不修改共享数据。
		sb.WriteString(strings.Join(safeSort(r.strValues), ","))
	}

	switch r.operator {
	case selection.In, selection.NotIn:
		sb.WriteString(")")
	}
	return sb.String()
}

// safeSort 在不修改输入的前提下排序字符串。
func safeSort(in []string) []string {
	if sort.StringsAreSorted(in) {
		return in
	}
	out := make([]string, len(in))
	copy(out, in)
	sort.Strings(out)
	return out
}

// Add 向选择器添加条件。它复制当前选择器并返回新实例。
func (s internalSelector) Add(reqs ...Requirement) Selector {
	ret := make(internalSelector, 0, len(s)+len(reqs))
	ret = append(ret, s...)
	ret = append(ret, reqs...)
	sort.Sort(ByKey(ret))
	return ret
}

// Matches 对 internalSelector 返回 true 表示其所有 Requirement 都匹配输入 Labels。
// 任一 Requirement 不匹配则返回 false。
func (s internalSelector) Matches(l Labels) bool {
	for ix := range s {
		if matches := s[ix].Matches(l); !matches {
			return false
		}
	}
	return true
}

func (s internalSelector) Requirements() (Requirements, bool) { return Requirements(s), true }

// String 返回所有 internalSelector Requirement 可读字符串的逗号分隔形式。
func (s internalSelector) String() string {
	var reqs []string
	for ix := range s {
		reqs = append(reqs, s[ix].String())
	}
	return strings.Join(reqs, ",")
}

// RequiresExactMatch 探查选择器是否要求某个特定字段必须被设置，若是则返回所需的值。
func (s internalSelector) RequiresExactMatch(label string) (value string, found bool) {
	for ix := range s {
		if s[ix].key == label {
			switch s[ix].operator {
			case selection.Equals, selection.DoubleEquals, selection.In:
				if len(s[ix].strValues) == 1 {
					return s[ix].strValues[0], true
				}
			}
			return "", false
		}
	}
	return "", false
}

// Token 表示词法分析器（lexer）的 token 常量。
type Token int

const (
	// ErrorToken 表示词法扫描过程中发生错误。
	ErrorToken Token = iota
	// EndOfStringToken 表示输入字符串结束。
	EndOfStringToken
	// ClosedParToken 表示右括号。
	ClosedParToken
	// CommaToken 表示逗号。
	CommaToken
	// DoesNotExistToken 表示逻辑非（!）。
	DoesNotExistToken
	// DoubleEqualsToken 表示双等号（==）。
	DoubleEqualsToken
	// EqualsToken 表示等号（=）。
	EqualsToken
	// GreaterThanToken 表示大于号（>）。
	GreaterThanToken
	// IdentifierToken 表示标识符，例如标签键和值。
	IdentifierToken
	// InToken 表示 in 操作符。
	InToken
	// LessThanToken 表示小于号（<）。
	LessThanToken
	// NotEqualsToken 表示不等号（!=）。
	NotEqualsToken
	// NotInToken 表示 notin 操作符。
	NotInToken
	// OpenParToken 表示左括号。
	OpenParToken
)

// string2token 保存词法 Token 与 token 字面量之间的映射
// （不含 IdentifierToken、EndOfStringToken 与 ErrorToken，因为它们没有意义）。
var string2token = map[string]Token{
	")":     ClosedParToken,
	",":     CommaToken,
	"!":     DoesNotExistToken,
	"==":    DoubleEqualsToken,
	"=":     EqualsToken,
	">":     GreaterThanToken,
	"in":    InToken,
	"<":     LessThanToken,
	"!=":    NotEqualsToken,
	"notin": NotInToken,
	"(":     OpenParToken,
}

// ScannedItem 包含词法分析器产生的 Token 与字面量。
type ScannedItem struct {
	tok     Token
	literal string
}

// isWhitespace 判断字符是否为空格、制表符或换行。
func isWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n'
}

// isSpecialSymbol 判断字符 ch 是否可作为操作符的一部分。
func isSpecialSymbol(ch byte) bool {
	switch ch {
	case '=', '!', '(', ')', ',', '>', '<':
		return true
	}
	return false
}

// Lexer 是标签选择器的词法分析器。
// 它包含对输入字符串进行分词所需的信息。
type Lexer struct {
	// s 保存待分词的字符串。
	s string
	// pos 是当前分词位置。
	pos int
}

// read 返回当前读取的字符，同时递增位置并检查缓冲区越界。
func (l *Lexer) read() (b byte) {
	b = 0
	if l.pos < len(l.s) {
		b = l.s[l.pos]
		l.pos++
	}
	return b
}

// unread 撤销上一次 read 读取的字符。
func (l *Lexer) unread() {
	l.pos--
}

// scanIDOrKeyword 扫描字符串，识别字面量 token（例如 'in'）或标识符。
func (l *Lexer) scanIDOrKeyword() (tok Token, lit string) {
	var buffer []byte
IdentifierLoop:
	for {
		switch ch := l.read(); {
		case ch == 0:
			break IdentifierLoop
		case isSpecialSymbol(ch) || isWhitespace(ch):
			l.unread()
			break IdentifierLoop
		default:
			buffer = append(buffer, ch)
		}
	}
	s := string(buffer)
	if val, ok := string2token[s]; ok { // 是否为字面量 token？
		return val, s
	}
	return IdentifierToken, s // 否则视为标识符
}

// scanSpecialSymbol 扫描以特殊符号开头的字符串。
// 特殊符号用于识别非字面量操作符："!="、"=="、"="。
func (l *Lexer) scanSpecialSymbol() (Token, string) {
	lastScannedItem := ScannedItem{}
	var buffer []byte
SpecialSymbolLoop:
	for {
		switch ch := l.read(); {
		case ch == 0:
			break SpecialSymbolLoop
		case isSpecialSymbol(ch):
			buffer = append(buffer, ch)
			if token, ok := string2token[string(buffer)]; ok {
				lastScannedItem = ScannedItem{tok: token, literal: string(buffer)}
			} else if lastScannedItem.tok != 0 {
				// 已匹配到最长合法操作符，多读的符号需回退。
				l.unread()
				break SpecialSymbolLoop
			}
		default:
			l.unread()
			break SpecialSymbolLoop
		}
	}
	if lastScannedItem.tok == 0 {
		return ErrorToken, fmt.Sprintf("error expected: keyword found '%s'", buffer)
	}
	return lastScannedItem.tok, lastScannedItem.literal
}

// skipWhiteSpaces 消费所有空白字符，返回第一个非空白字符。
func (l *Lexer) skipWhiteSpaces(ch byte) byte {
	for {
		if !isWhitespace(ch) {
			return ch
		}
		ch = l.read()
	}
}

// Lex 返回一个 Token 及其字面量。
// 字面量仅对 IdentifierToken 有意义。
func (l *Lexer) Lex() (tok Token, lit string) {
	switch ch := l.skipWhiteSpaces(l.read()); {
	case ch == 0:
		return EndOfStringToken, ""
	case isSpecialSymbol(ch):
		l.unread()
		return l.scanSpecialSymbol()
	default:
		l.unread()
		return l.scanIDOrKeyword()
	}
}

// Parser 包含标签选择器解析器的数据结构。
type Parser struct {
	l            *Lexer
	scannedItems []ScannedItem
	position     int
	path         *field.Path
}

// ParserContext 表示解析期间的上下文：
// 某些字面量（例如 'in' 和 'notin'）既可被识别为操作符（如 'x in (a)'），
// 也可被识别为值（如 'value in (in)'）。
type ParserContext int

const (
	// KeyAndOperator 表示解析 key 与操作符的上下文。
	KeyAndOperator ParserContext = iota
	// Values 表示解析值的上下文。
	Values
)

// lookahead 返回当前 token 与字符串，不递增当前位置。
func (p *Parser) lookahead(context ParserContext) (Token, string) {
	tok, lit := p.scannedItems[p.position].tok, p.scannedItems[p.position].literal
	if context == Values {
		// 在值位置，in/notin 应作为普通标识符处理。
		switch tok {
		case InToken, NotInToken:
			tok = IdentifierToken
		}
	}
	return tok, lit
}

// consume 返回当前 token 与字符串，并递增位置。
func (p *Parser) consume(context ParserContext) (Token, string) {
	p.position++
	tok, lit := p.scannedItems[p.position-1].tok, p.scannedItems[p.position-1].literal
	if context == Values {
		switch tok {
		case InToken, NotInToken:
			tok = IdentifierToken
		}
	}
	return tok, lit
}

// scan 遍历输入字符串并把 ScannedItem 存入数组，
// 之后 Parser 即可进行 lookahead 与 consume。
func (p *Parser) scan() {
	for {
		token, literal := p.l.Lex()
		p.scannedItems = append(p.scannedItems, ScannedItem{token, literal})
		if token == EndOfStringToken {
			break
		}
	}
}

// parse 对输入字符串运行左递归下降算法，返回 Requirement 列表。
func (p *Parser) parse() (internalSelector, error) {
	p.scan() // 初始化 scannedItems

	var requirements internalSelector
	for {
		tok, lit := p.lookahead(Values)
		switch tok {
		case IdentifierToken, DoesNotExistToken:
			r, err := p.parseRequirement()
			if err != nil {
				return nil, fmt.Errorf("unable to parse requirement: %v", err)
			}
			requirements = append(requirements, *r)
			t, l := p.consume(Values)
			switch t {
			case EndOfStringToken:
				return requirements, nil
			case CommaToken:
				t2, l2 := p.lookahead(Values)
				if t2 != IdentifierToken && t2 != DoesNotExistToken {
					return nil, fmt.Errorf("found '%s', expected: identifier after ','", l2)
				}
			default:
				return nil, fmt.Errorf("found '%s', expected: ',' or 'end of string'", l)
			}
		case EndOfStringToken:
			return requirements, nil
		default:
			return nil, fmt.Errorf("found '%s', expected: !, identifier, or 'end of string'", lit)
		}
	}
}

// parseRequirement 解析单个 requirement：key、可选操作符与值。
func (p *Parser) parseRequirement() (*Requirement, error) {
	key, operator, err := p.parseKeyAndInferOperator()
	if err != nil {
		return nil, err
	}
	if operator == selection.Exists || operator == selection.DoesNotExist { // 已通过 lookahead 确定操作符
		return NewRequirement(key, operator, []string{}, field.WithPath(p.path))
	}
	operator, err = p.parseOperator()
	if err != nil {
		return nil, err
	}
	var values sets.String
	switch operator {
	case selection.In, selection.NotIn:
		values, err = p.parseValues()
	case selection.Equals, selection.DoubleEquals, selection.NotEquals, selection.GreaterThan, selection.LessThan:
		values, err = p.parseExactValue()
	}
	if err != nil {
		return nil, err
	}
	return NewRequirement(key, operator, values.List(), field.WithPath(p.path))

}

// parseKeyAndInferOperator 解析字面量。
// 若未发现 '!, in, notin, ==, =, !=' 等操作符，则推断为 'exists' 操作符。
func (p *Parser) parseKeyAndInferOperator() (string, selection.Operator, error) {
	var operator selection.Operator
	tok, literal := p.consume(Values)
	if tok == DoesNotExistToken {
		operator = selection.DoesNotExist
		tok, literal = p.consume(Values)
	}
	if tok != IdentifierToken {
		err := fmt.Errorf("found '%s', expected: identifier", literal)
		return "", "", err
	}
	if err := validateLabelKey(literal, p.path); err != nil {
		return "", "", err
	}
	// 若 key 之后直接结束或遇到逗号，则这是只有 key 的 requirement。
	if t, _ := p.lookahead(Values); t == EndOfStringToken || t == CommaToken {
		if operator != selection.DoesNotExist {
			operator = selection.Exists
		}
	}
	return literal, operator, nil
}

// parseOperator 返回操作符。
// 这里的 matchType 可以是 exact。
func (p *Parser) parseOperator() (op selection.Operator, err error) {
	tok, lit := p.consume(KeyAndOperator)
	switch tok {
	// DoesNotExistToken 不应出现在这里，因为它是单目操作符而非双目操作符。
	case InToken:
		op = selection.In
	case EqualsToken:
		op = selection.Equals
	case DoubleEqualsToken:
		op = selection.DoubleEquals
	case GreaterThanToken:
		op = selection.GreaterThan
	case LessThanToken:
		op = selection.LessThan
	case NotInToken:
		op = selection.NotIn
	case NotEqualsToken:
		op = selection.NotEquals
	default:
		return "", fmt.Errorf("found '%s', expected: %v", lit, strings.Join(binaryOperators, ", "))
	}
	return op, nil
}

// parseValues 解析基于集合匹配的值列表 (x,y,z)。
func (p *Parser) parseValues() (sets.String, error) {
	tok, lit := p.consume(Values)
	if tok != OpenParToken {
		return nil, fmt.Errorf("found '%s' expected: '('", lit)
	}
	tok, lit = p.lookahead(Values)
	switch tok {
	case IdentifierToken, CommaToken:
		s, err := p.parseIdentifiersList() // 处理一般情况
		if err != nil {
			return s, err
		}
		if tok, _ = p.consume(Values); tok != ClosedParToken {
			return nil, fmt.Errorf("found '%s', expected: ')'", lit)
		}
		return s, nil
	case ClosedParToken: // handles "()"
		p.consume(Values)
		return sets.NewString(""), nil
	default:
		return nil, fmt.Errorf("found '%s', expected: ',', ')' or identifier", lit)
	}
}

// parseIdentifiersList 解析（可能为空的）逗号分隔标识符列表。
func (p *Parser) parseIdentifiersList() (sets.String, error) {
	s := sets.NewString()
	for {
		tok, lit := p.consume(Values)
		switch tok {
		case IdentifierToken:
			s.Insert(lit)
			tok2, lit2 := p.lookahead(Values)
			switch tok2 {
			case CommaToken:
				continue
			case ClosedParToken:
				return s, nil
			default:
				return nil, fmt.Errorf("found '%s', expected: ',' or ')'", lit2)
			}
		case CommaToken: // 在此处理，因为可能出现 "(,"
			if s.Len() == 0 {
				s.Insert("") // 处理 (,
			}
			tok2, _ := p.lookahead(Values)
			if tok2 == ClosedParToken {
				s.Insert("") // 处理 ,)，重复的 "" 会被 StringSet 去重
				return s, nil
			}
			if tok2 == CommaToken {
				s.Insert("") // 处理 ,,，重复的 "" 会被 StringSet 去重
			}
		default: // 也可能是操作符
			return s, fmt.Errorf("found '%s', expected: ',', or identifier", lit)
		}
	}
}

// parseExactValue 解析精确匹配风格所需的唯一值。
func (p *Parser) parseExactValue() (sets.String, error) {
	s := sets.NewString()
	tok, _ := p.lookahead(Values)
	// key= 后直接结束或逗号，表示空字符串值。
	if tok == EndOfStringToken || tok == CommaToken {
		s.Insert("")
		return s, nil
	}
	tok, lit := p.consume(Values)
	if tok == IdentifierToken {
		s.Insert(lit)
		return s, nil
	}
	return nil, fmt.Errorf("found '%s', expected: identifier", lit)
}

// Parse 接收选择器字符串并返回选择器对象，或返回错误。
// 该解析函数与 ParseSelector 不同，二者解析的语法不同。
// 若输入不符合以下形式则报错：
//
//	<selector-syntax>         ::= <requirement> | <requirement> "," <selector-syntax>
//	<requirement>             ::= [!] KEY [ <set-based-restriction> | <exact-match-restriction> ]
//	<set-based-restriction>   ::= "" | <inclusion-exclusion> <value-set>
//	<inclusion-exclusion>     ::= <inclusion> | <exclusion>
//	<exclusion>               ::= "notin"
//	<inclusion>               ::= "in"
//	<value-set>               ::= "(" <values> ")"
//	<values>                  ::= VALUE | VALUE "," <values>
//	<exact-match-restriction> ::= ["="|"=="|"!="] VALUE
//
// KEY 是遵循 [ DNS_SUBDOMAIN "/" ] DNS_LABEL 的一个或多个字符，最大长度 63。
// VALUE 是零个或多个 "([A-Za-z0-9_-\.])" 字符，最大长度 63。
// 分隔符为空白字符：(' ', '\t')
// 合法语法示例：
//
//	"x in (foo,,baz),y,z notin ()"
//
// 注意：
//  1. 包含 - " in " - 表示 KEY 存在且等于其 requirement 中任一 VALUE。
//  2. 排除 - " notin " - 表示 KEY 不等于其 requirement 中任一 VALUE，或不存在。
//  3. 空字符串是合法的 VALUE。
//  4. 只有 KEY 的 requirement（如上面的 "y"）表示 KEY 存在且可为任意 VALUE。
//  5. 只有 !KEY 的 requirement 要求 KEY 不存在。
func Parse(selector string, opts ...field.PathOption) (Selector, error) {
	parsedSelector, err := parse(selector, field.ToPath(opts...))
	if err == nil {
		return parsedSelector, nil
	}
	return nil, err
}

// parse 解析选择器字符串并返回 internalSelector。
// 调用方可决定如何把 internalSelector 返回给自己的调用方。
// 目前有两个调用方：一个返回 Selector 接口，另一个返回 requirement 列表。
func parse(selector string, path *field.Path) (internalSelector, error) {
	p := &Parser{l: &Lexer{s: selector, pos: 0}, path: path}
	items, err := p.parse()
	if err != nil {
		return nil, err
	}
	sort.Sort(ByKey(items)) // 排序以保证解析结果确定
	return internalSelector(items), err
}

// validateLabelKey 校验标签键是否合法。
func validateLabelKey(k string, path *field.Path) *field.Error {
	if errs := content.IsLabelKey(k); len(errs) != 0 {
		return field.Invalid(path, k, strings.Join(errs, "; "))
	}
	return nil
}

// validateLabelValue 校验标签值是否合法；错误路径会带上键名以便定位。
func validateLabelValue(k, v string, path *field.Path) *field.Error {
	if errs := validation.IsValidLabelValue(v); len(errs) != 0 {
		return field.Invalid(path.Key(k), v, strings.Join(errs, "; "))
	}
	return nil
}

// SelectorFromSet 返回恰好匹配给定 Set 的选择器。
// nil 与空 Set 等价于 Everything()。
// 它不做任何校验，因此若 Set 含非法值，服务端会拒绝该请求。
func SelectorFromSet(ls Set) Selector {
	return SelectorFromValidatedSet(ls)
}

// ValidatedSelectorFromSet 返回恰好匹配给定 Set 的选择器。
// nil 与空 Set 等价于 Everything()。
// Set 会在客户端校验，从而尽早发现错误。
func ValidatedSelectorFromSet(ls Set) (Selector, error) {
	if ls == nil || len(ls) == 0 {
		return internalSelector{}, nil
	}
	requirements := make([]Requirement, 0, len(ls))
	for label, value := range ls {
		r, err := NewRequirement(label, selection.Equals, []string{value})
		if err != nil {
			return nil, err
		}
		requirements = append(requirements, *r)
	}
	// 排序以获得确定的字符串表示
	sort.Sort(ByKey(requirements))
	return internalSelector(requirements), nil
}

// SelectorFromValidatedSet 返回恰好匹配给定 Set 的选择器。
// nil 与空 Set 等价于 Everything()。
// 它假定 Set 已校验，不做任何校验。
// 注意：该方法会复制 Set；若 Set 不可变，可考虑用 ValidatedSetSelector 包装，
// 后者不复制。
func SelectorFromValidatedSet(ls Set) Selector {
	if ls == nil || len(ls) == 0 {
		return internalSelector{}
	}
	requirements := make([]Requirement, 0, len(ls))
	for label, value := range ls {
		requirements = append(requirements, Requirement{key: label, operator: selection.Equals, strValues: []string{value}})
	}
	// 排序以获得确定的字符串表示
	sort.Sort(ByKey(requirements))
	return internalSelector(requirements)
}

// ParseToRequirements 接收选择器字符串并返回 requirement 列表。
// 适合需要对选择器条件做额外处理的调用方。
// 详见 Parse() 的文档。
// TODO：评估是否应直接导出 internalSelector 类型。
func ParseToRequirements(selector string, opts ...field.PathOption) ([]Requirement, error) {
	return parse(selector, field.ToPath(opts...))
}

// ValidatedSetSelector 包装一个 Set，使其实现 Selector 接口。
// 与 Set.AsSelectorPreValidated（会复制输入 Set）不同，该类型只是包装底层 Set，
// 因此效率高得多。nil 与空 Set 等价于 Everything()。
//
// 调用方必须确保底层 Set 不会被修改，且已经过校验。若不满足这些约束，
// 应优先使用 Set.AsValidatedSelector。
//
// Selector 的方法都不会修改底层 Set，但 Add() 与 Requirements() 会转换为
// 优化程度较低的版本。
type ValidatedSetSelector Set

// Matches 判断包装的 Set 是否完全包含于给定标签中（键值均相等）。
func (s ValidatedSetSelector) Matches(labels Labels) bool {
	for k, v := range s {
		val, exists := labels.Lookup(k)
		if !exists || v != val {
			return false
		}
	}
	return true
}

// Empty 返回包装的 Set 是否为空。
func (s ValidatedSetSelector) Empty() bool {
	return len(s) == 0
}

// String 返回排序后的 key=value 逗号分隔字符串，保证输出确定。
func (s ValidatedSetSelector) String() string {
	keys := make([]string, 0, len(s))
	for k := range s {
		keys = append(keys, k)
	}
	// 保证输出确定
	sort.Strings(keys)
	b := strings.Builder{}
	for i, key := range keys {
		v := s[key]
		b.Grow(len(key) + 2 + len(v))
		if i != 0 {
			b.WriteString(",")
		}
		b.WriteString(key)
		b.WriteString("=")
		b.WriteString(v)
	}
	return b.String()
}

// Add 转换为完整选择器后添加条件（因为 Set 形式无法直接表达任意操作符）。
func (s ValidatedSetSelector) Add(r ...Requirement) Selector {
	return s.toFullSelector().Add(r...)
}

// Requirements 转换为完整选择器后返回条件列表。
func (s ValidatedSetSelector) Requirements() (requirements Requirements, selectable bool) {
	return s.toFullSelector().Requirements()
}

// DeepCopySelector 复制包装的 Set，返回新的选择器。
func (s ValidatedSetSelector) DeepCopySelector() Selector {
	res := make(ValidatedSetSelector, len(s))
	for k, v := range s {
		res[k] = v
	}
	return res
}

// RequiresExactMatch 返回指定 key 的值（若存在），因为 Set 中的每项都是精确匹配。
func (s ValidatedSetSelector) RequiresExactMatch(label string) (value string, found bool) {
	v, f := s[label]
	return v, f
}

// toFullSelector 把 Set 形式转换为通用的 internalSelector。
func (s ValidatedSetSelector) toFullSelector() Selector {
	return SelectorFromValidatedSet(Set(s))
}

var _ Selector = ValidatedSetSelector{}
