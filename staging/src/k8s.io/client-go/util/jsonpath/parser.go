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

package jsonpath

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// eof 是输入结束的哨兵值，避免与任何合法 rune 冲突。
const eof = -1

const (
	// 模板中动作表达式被包裹在 { } 之间。
	leftDelim  = "{"
	rightDelim = "}"
)

// Parser 是 JSONPath 表达式的递归下降解析器，持有输入文本与扫描位置。
//
// 解析结果是以 Root 为根的 AST；解析器本身不是并发安全的，每个表达式应独立解析。
type Parser struct {
	Name  string
	Root  *ListNode
	input string
	pos   int
	start int
	width int
}

var (
	// ErrSyntax 表示表达式语法错误。
	ErrSyntax        = errors.New("invalid syntax")
	dictKeyRex       = regexp.MustCompile(`^'([^']*)'$`)
	sliceOperatorRex = regexp.MustCompile(`^(-?[\d]*)(:-?[\d]*)?(:-?[\d]*)?$`)
)

// Parse parsed the given text and return a node Parser.
// If an error is encountered, parsing stops and an empty
// Parser is returned with the error
//
// Parse 解析文本并返回 Parser；出错时返回 nil，便于调用方直接判空。
func Parse(name, text string) (*Parser, error) {
	p := NewParser(name)
	err := p.Parse(text)
	if err != nil {
		p = nil
	}
	return p, err
}

// NewParser 创建指定名字的空解析器。
func NewParser(name string) *Parser {
	return &Parser{
		Name: name,
	}
}

// parseAction parsed the expression inside delimiter
//
// parseAction 解析两个分隔符之间的动作表达式，并返回以该动作根节点为 Root 的 Parser。
func parseAction(name, text string) (*Parser, error) {
	p, err := Parse(name, fmt.Sprintf("%s%s%s", leftDelim, text, rightDelim))
	// when error happens, p will be nil, so we need to return here
	if err != nil {
		return p, err
	}
	p.Root = p.Root.Nodes[0].(*ListNode)
	return p, nil
}

// Parse 重置解析状态并解析 text。
func (p *Parser) Parse(text string) error {
	p.input = text
	p.Root = newList()
	p.pos = 0
	return p.parseText(p.Root)
}

// consumeText return the parsed text since last cosumeText
//
// consumeText 返回自上次消费以来的文本，并把起点移动到当前位置。
func (p *Parser) consumeText() string {
	value := p.input[p.start:p.pos]
	p.start = p.pos
	return value
}

// next returns the next rune in the input.
func (p *Parser) next() rune {
	if p.pos >= len(p.input) {
		p.width = 0
		return eof
	}
	r, w := utf8.DecodeRuneInString(p.input[p.pos:])
	p.width = w
	p.pos += p.width
	return r
}

// peek returns but does not consume the next rune in the input.
func (p *Parser) peek() rune {
	r := p.next()
	p.backup()
	return r
}

// backup steps back one rune. Can only be called once per call of next.
func (p *Parser) backup() {
	p.pos -= p.width
}

// parseText 扫描普通文本，直到遇到左分隔符或输入结束；
// 文本片段作为 TextNode 追加到当前列表，左分隔符交给 parseLeftDelim 处理。
func (p *Parser) parseText(cur *ListNode) error {
	for {
		if strings.HasPrefix(p.input[p.pos:], leftDelim) {
			if p.pos > p.start {
				cur.append(newText(p.consumeText()))
			}
			return p.parseLeftDelim(cur)
		}
		if p.next() == eof {
			break
		}
	}
	// Correctly reached EOF.
	if p.pos > p.start {
		cur.append(newText(p.consumeText()))
	}
	return nil
}

// parseLeftDelim scans the left delimiter, which is known to be present.
//
// parseLeftDelim 消费左分隔符，新建一个列表节点进入动作内部继续解析。
func (p *Parser) parseLeftDelim(cur *ListNode) error {
	p.pos += len(leftDelim)
	p.consumeText()
	newNode := newList()
	cur.append(newNode)
	cur = newNode
	return p.parseInsideAction(cur)
}

// parseInsideAction 解析动作内部的语法，按前缀分派到过滤器、递归下降，
// 或根据当前字符分派到数组、字符串、字段、数字、标识符等解析函数。
func (p *Parser) parseInsideAction(cur *ListNode) error {
	prefixMap := map[string]func(*ListNode) error{
		rightDelim: p.parseRightDelim,
		"[?(":      p.parseFilter,
		"..":       p.parseRecursive,
	}
	for prefix, parseFunc := range prefixMap {
		if strings.HasPrefix(p.input[p.pos:], prefix) {
			return parseFunc(cur)
		}
	}

	switch r := p.next(); {
	case r == eof || isEndOfLine(r):
		return fmt.Errorf("unclosed action")
	case r == ' ':
		p.consumeText()
	case r == '@' || r == '$': //the current object, just pass it
		p.consumeText()
	case r == '[':
		return p.parseArray(cur)
	case r == '"' || r == '\'':
		return p.parseQuote(cur, r)
	case r == '.':
		return p.parseField(cur)
	case r == '+' || r == '-' || unicode.IsDigit(r):
		p.backup()
		return p.parseNumber(cur)
	case isAlphaNumeric(r):
		p.backup()
		return p.parseIdentifier(cur)
	default:
		return fmt.Errorf("unrecognized character in action: %#U", r)
	}
	return p.parseInsideAction(cur)
}

// parseRightDelim scans the right delimiter, which is known to be present.
//
// parseRightDelim 消费右分隔符，回到文本模式继续解析。
func (p *Parser) parseRightDelim(cur *ListNode) error {
	p.pos += len(rightDelim)
	p.consumeText()
	return p.parseText(p.Root)
}

// parseIdentifier scans build-in keywords, like "range" "end"
//
// parseIdentifier 扫描标识符；true/false 会被解析为布尔字面量，其余作为 identifier 节点。
func (p *Parser) parseIdentifier(cur *ListNode) error {
	var r rune
	for {
		r = p.next()
		if isTerminator(r) {
			p.backup()
			break
		}
	}
	value := p.consumeText()

	if isBool(value) {
		v, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("can not parse bool '%s': %s", value, err.Error())
		}

		cur.append(newBool(v))
	} else {
		cur.append(newIdentifier(value))
	}

	return p.parseInsideAction(cur)
}

// parseRecursive scans the recursive descent operator ..
//
// parseRecursive 解析递归下降运算符 ".."；不允许连续出现两个 ".."。
func (p *Parser) parseRecursive(cur *ListNode) error {
	if lastIndex := len(cur.Nodes) - 1; lastIndex >= 0 && cur.Nodes[lastIndex].Type() == NodeRecursive {
		return fmt.Errorf("invalid multiple recursive descent")
	}
	p.pos += len("..")
	p.consumeText()
	cur.append(newRecursive())
	if r := p.peek(); isAlphaNumeric(r) {
		return p.parseField(cur)
	}
	return p.parseInsideAction(cur)
}

// parseNumber scans number
//
// parseNumber 解析整数或浮点数（支持前导正负号）。
func (p *Parser) parseNumber(cur *ListNode) error {
	r := p.peek()
	if r == '+' || r == '-' {
		p.next()
	}
	for {
		r = p.next()
		if r != '.' && !unicode.IsDigit(r) {
			p.backup()
			break
		}
	}
	value := p.consumeText()
	// 先尝试整数，失败再尝试浮点，保持对整数值的最精确表示。
	i, err := strconv.Atoi(value)
	if err == nil {
		cur.append(newInt(i))
		return p.parseInsideAction(cur)
	}
	d, err := strconv.ParseFloat(value, 64)
	if err == nil {
		cur.append(newFloat(d))
		return p.parseInsideAction(cur)
	}
	return fmt.Errorf("cannot parse number %s", value)
}

// parseArray scans array index selection
//
// parseArray 解析方括号内的索引、通配、并集、字典键或切片表达式。
func (p *Parser) parseArray(cur *ListNode) error {
Loop:
	for {
		switch p.next() {
		case eof, '\n':
			return fmt.Errorf("unterminated array")
		case ']':
			break Loop
		}
	}
	text := p.consumeText()
	text = text[1 : len(text)-1]
	// "*" 等价于完整切片 ":"。
	if text == "*" {
		text = ":"
	}

	//union operator
	strs := strings.Split(text, ",")
	if len(strs) > 1 {
		union := []*ListNode{}
		for _, str := range strs {
			parser, err := parseAction("union", fmt.Sprintf("[%s]", strings.Trim(str, " ")))
			if err != nil {
				return err
			}
			union = append(union, parser.Root)
		}
		cur.append(newUnion(union))
		return p.parseInsideAction(cur)
	}

	// dict key
	// 'key' 形式的字典下标，转换为字段访问。
	value := dictKeyRex.FindStringSubmatch(text)
	if value != nil {
		parser, err := parseAction("arraydict", fmt.Sprintf(".%s", value[1]))
		if err != nil {
			return err
		}
		for _, node := range parser.Root.Nodes {
			cur.append(node)
		}
		return p.parseInsideAction(cur)
	}

	//slice operator
	value = sliceOperatorRex.FindStringSubmatch(text)
	if value == nil {
		return fmt.Errorf("invalid array index %s", text)
	}
	value = value[1:]
	params := [3]ParamsEntry{}
	for i := 0; i < 3; i++ {
		if value[i] != "" {
			// 后两个参数带 ":" 前缀，需要去掉再解析数字。
			if i > 0 {
				value[i] = value[i][1:]
			}
			if i > 0 && value[i] == "" {
				params[i].Known = false
			} else {
				var err error
				params[i].Known = true
				params[i].Value, err = strconv.Atoi(value[i])
				if err != nil {
					return fmt.Errorf("array index %s is not a number", value[i])
				}
			}
		} else {
			if i == 1 {
				// 省略结束位置时，默认到起始位置之后一位（单个元素）。
				params[i].Known = true
				params[i].Value = params[0].Value + 1
				params[i].Derived = true
			} else {
				params[i].Known = false
				params[i].Value = 0
			}
		}
	}
	cur.append(newArray(params))
	return p.parseInsideAction(cur)
}

// parseFilter scans filter inside array selection
//
// parseFilter 解析 "[?(...)]" 过滤器；无法识别比较运算符时按“字段存在”处理。
func (p *Parser) parseFilter(cur *ListNode) error {
	p.pos += len("[?(")
	p.consumeText()
	begin := false
	end := false
	var pair rune

Loop:
	for {
		r := p.next()
		switch r {
		case eof, '\n':
			return fmt.Errorf("unterminated filter")
		case '"', '\'':
			if begin == false {
				//save the paired rune
				begin = true
				pair = r
				continue
			}
			//only add when met paired rune
			if p.input[p.pos-2] != '\\' && r == pair {
				end = true
			}
		case ')':
			//in rightParser below quotes only appear zero or once
			//and must be paired at the beginning and end
			if begin == end {
				break Loop
			}
		}
	}
	if p.next() != ']' {
		return fmt.Errorf("unclosed array expect ]")
	}
	reg := regexp.MustCompile(`^([^!<>=]+)([!<>=]+)(.+?)$`)
	text := p.consumeText()
	text = text[:len(text)-2]
	value := reg.FindStringSubmatch(text)
	if value == nil {
		// 没有比较运算符，视为“存在性”过滤。
		parser, err := parseAction("text", text)
		if err != nil {
			return err
		}
		cur.append(newFilter(parser.Root, newList(), "exists"))
	} else {
		leftParser, err := parseAction("left", value[1])
		if err != nil {
			return err
		}
		rightParser, err := parseAction("right", value[3])
		if err != nil {
			return err
		}
		cur.append(newFilter(leftParser.Root, rightParser.Root, value[2]))
	}
	return p.parseInsideAction(cur)
}

// parseQuote unquotes string inside double or single quote
//
// parseQuote 解析单/双引号字符串，支持转义引号。
func (p *Parser) parseQuote(cur *ListNode, end rune) error {
Loop:
	for {
		switch p.next() {
		case eof, '\n':
			return fmt.Errorf("unterminated quoted string")
		case end:
			//if it's not escape break the Loop
			if p.input[p.pos-2] != '\\' {
				break Loop
			}
		}
	}
	value := p.consumeText()
	s, err := UnquoteExtend(value)
	if err != nil {
		return fmt.Errorf("unquote string %s error %v", value, err)
	}
	cur.append(newText(s))
	return p.parseInsideAction(cur)
}

// parseField scans a field until a terminator
//
// parseField 解析字段名直到终止符；"*" 会被解析为通配符节点。
func (p *Parser) parseField(cur *ListNode) error {
	p.consumeText()
	for p.advance() {
	}
	value := p.consumeText()
	if value == "*" {
		cur.append(newWildcard())
	} else {
		// 去掉转义用的反斜杠，得到真实字段名。
		cur.append(newField(strings.Replace(value, "\\", "", -1)))
	}
	return p.parseInsideAction(cur)
}

// advance scans until next non-escaped terminator
//
// advance 前进一个 rune；遇到转义符时跳过其后一个字符，遇到未转义的终止符则回退并返回 false。
func (p *Parser) advance() bool {
	r := p.next()
	if r == '\\' {
		p.next()
	} else if isTerminator(r) {
		p.backup()
		return false
	}
	return true
}

// isTerminator reports whether the input is at valid termination character to appear after an identifier.
func isTerminator(r rune) bool {
	if isSpace(r) || isEndOfLine(r) {
		return true
	}
	switch r {
	case eof, '.', ',', '[', ']', '$', '@', '{', '}':
		return true
	}
	return false
}

// isSpace reports whether r is a space character.
func isSpace(r rune) bool {
	return r == ' ' || r == '\t'
}

// isEndOfLine reports whether r is an end-of-line character.
func isEndOfLine(r rune) bool {
	return r == '\r' || r == '\n'
}

// isAlphaNumeric reports whether r is an alphabetic, digit, or underscore.
func isAlphaNumeric(r rune) bool {
	return r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r)
}

// isBool reports whether s is a boolean value.
func isBool(s string) bool {
	return s == "true" || s == "false"
}

// UnquoteExtend is almost same as strconv.Unquote(), but it support parse single quotes as a string
//
// UnquoteExtend 与 strconv.Unquote 基本相同，但额外支持单引号字符串。
func UnquoteExtend(s string) (string, error) {
	n := len(s)
	if n < 2 {
		return "", ErrSyntax
	}
	quote := s[0]
	if quote != s[n-1] {
		return "", ErrSyntax
	}
	s = s[1 : n-1]

	if quote != '"' && quote != '\'' {
		return "", ErrSyntax
	}

	// Is it trivial?  Avoid allocation.
	if !contains(s, '\\') && !contains(s, quote) {
		return s, nil
	}

	var runeTmp [utf8.UTFMax]byte
	buf := make([]byte, 0, 3*len(s)/2) // Try to avoid more allocations.
	for len(s) > 0 {
		c, multibyte, ss, err := strconv.UnquoteChar(s, quote)
		if err != nil {
			return "", err
		}
		s = ss
		if c < utf8.RuneSelf || !multibyte {
			buf = append(buf, byte(c))
		} else {
			n := utf8.EncodeRune(runeTmp[:], c)
			buf = append(buf, runeTmp[:n]...)
		}
	}
	return string(buf), nil
}

// contains 判断字节 c 是否出现在 s 中。
func contains(s string, c byte) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return true
		}
	}
	return false
}
