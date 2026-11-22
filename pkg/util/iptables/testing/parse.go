//go:build linux

/*
Copyright 2022 The Kubernetes Authors.

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

package testing

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"k8s.io/kubernetes/pkg/util/iptables"
)

// IPTablesDump 表示解析后的 iptables 规则转储，
// 即 "iptables-save" 的输出或 "iptables-restore" 的输入。
type IPTablesDump struct {
	Tables []Table
}

// Table 表示 iptables 的一个表。
type Table struct {
	Name   iptables.Table
	Chains []Chain
}

// Chain 表示 iptables 的一个链及其包/字节计数和规则列表。
type Chain struct {
	Name    iptables.Chain
	Packets uint64
	Bytes   uint64
	Rules   []*Rule

	// 输入中包含 "-X Name" 行时置位。iptables-save 不会输出这种行，
	// 但 iptables-restore 的输入中可以出现。
	Deleted bool
}

var declareTableRegex = regexp.MustCompile(`^\*(.*)$`)
var declareChainRegex = regexp.MustCompile(`^:([^ ]*) - \[([0-9]*):([0-9]*)\]$`)
var addRuleRegex = regexp.MustCompile(`^-A ([^ ]*) (.*)$`)
var deleteChainRegex = regexp.MustCompile(`^-X (.*)$`)

// parseState 标记当前解析到转储的哪个阶段。
type parseState int

const (
	parseTableDeclaration parseState = iota
	parseChainDeclarations
	parseChains
)

// ParseIPTablesDump 解析 iptables 规则转储。注意：可能会忽略部分非法数据。
func ParseIPTablesDump(data string) (*IPTablesDump, error) {
	dump := &IPTablesDump{}
	state := parseTableDeclaration
	lines := strings.Split(strings.Trim(data, "\n"), "\n")
	var t *Table

	for _, line := range lines {
	retry:
		line = strings.TrimSpace(line)
		if line == "" || line[0] == '#' {
			continue
		}

		switch state {
		case parseTableDeclaration:
			// 解析表声明行，如 "*filter"。
			match := declareTableRegex.FindStringSubmatch(line)
			if match == nil {
				return nil, fmt.Errorf("could not parse iptables data (table %d starts with %q not a table name)", len(dump.Tables)+1, line)
			}
			dump.Tables = append(dump.Tables, Table{Name: iptables.Table(match[1])})
			t = &dump.Tables[len(dump.Tables)-1]
			state = parseChainDeclarations

		case parseChainDeclarations:
			match := declareChainRegex.FindStringSubmatch(line)
			if match == nil {
				state = parseChains
				goto retry
			}

			chain := iptables.Chain(match[1])
			packets, _ := strconv.ParseUint(match[2], 10, 64)
			bytes, _ := strconv.ParseUint(match[3], 10, 64)

			t.Chains = append(t.Chains,
				Chain{
					Name:    chain,
					Packets: packets,
					Bytes:   bytes,
				},
			)

		case parseChains:
			if match := addRuleRegex.FindStringSubmatch(line); match != nil {
				chain := iptables.Chain(match[1])

				c, err := dump.GetChain(t.Name, chain)
				if err != nil {
					return nil, fmt.Errorf("error parsing rule %q: %v", line, err)
				}
				if c.Deleted {
					return nil, fmt.Errorf("cannot add rules to deleted chain %q", chain)
				}

				rule, err := ParseRule(line, false)
				if err != nil {
					return nil, err
				}
				c.Rules = append(c.Rules, rule)
			} else if match := deleteChainRegex.FindStringSubmatch(line); match != nil {
				chain := iptables.Chain(match[1])

				c, err := dump.GetChain(t.Name, chain)
				if err != nil {
					return nil, fmt.Errorf("error parsing rule %q: %v", line, err)
				}
				if len(c.Rules) != 0 {
					return nil, fmt.Errorf("cannot delete chain %q after adding rules", chain)
				}
				c.Deleted = true
			} else if line == "COMMIT" {
				state = parseTableDeclaration
			} else {
				return nil, fmt.Errorf("error parsing rule %q", line)
			}
		}
	}

	if state != parseTableDeclaration {
		return nil, fmt.Errorf("could not parse iptables data (no COMMIT line?)")
	}

	return dump, nil
}

// String 将解析结果重新序列化为 iptables-save 风格的文本。
func (dump *IPTablesDump) String() string {
	buffer := &strings.Builder{}
	for _, t := range dump.Tables {
		fmt.Fprintf(buffer, "*%s\n", t.Name)
		for _, c := range t.Chains {
			fmt.Fprintf(buffer, ":%s - [%d:%d]\n", c.Name, c.Packets, c.Bytes)
		}
		for _, c := range t.Chains {
			for _, r := range c.Rules {
				fmt.Fprintf(buffer, "%s\n", r.Raw)
			}
		}
		for _, c := range t.Chains {
			if c.Deleted {
				fmt.Fprintf(buffer, "-X %s\n", c.Name)
			}
		}
		fmt.Fprintf(buffer, "COMMIT\n")
	}
	return buffer.String()
}

// GetTable 按表名查找表；不存在时返回错误。
func (dump *IPTablesDump) GetTable(table iptables.Table) (*Table, error) {
	for i := range dump.Tables {
		if dump.Tables[i].Name == table {
			return &dump.Tables[i], nil
		}
	}
	return nil, fmt.Errorf("no such table %q", table)
}

// GetChain 按表名和链名查找链；任一级不存在时返回错误。
func (dump *IPTablesDump) GetChain(table iptables.Table, chain iptables.Chain) (*Chain, error) {
	t, err := dump.GetTable(table)
	if err != nil {
		return nil, err
	}
	for i := range t.Chains {
		if t.Chains[i].Name == chain {
			return &t.Chains[i], nil
		}
	}
	return nil, fmt.Errorf("no such chain %q", chain)
}

// Rule 表示一条解析后的 iptables 规则（目前覆盖 pkg/proxy/iptables 与
// pkg/proxy/ipvs 实际使用的全部规则类型）。
//
// 解析主要靠类型反射完成：字段上的 `param` 标签指明哪个参数的值放入该字段。
// 代码假定同一参数不会同时使用短名和长名（如 "-s" 与 "--source"），目前确实如此，
// 需要时可扩展。`negatable` 标签表示该参数是否允许前置 "!"。
//
// 带值的参数存为 `*IPTablesValue`，其中封装了字符串值及是否被取反
// （即规则要求*匹配*还是*不匹配*该值）。不可取反的字符串参数也用 IPTablesValue
// 而非 string，纯粹为了 API 一致。
//
// 不带值的参数存为 `*bool`：未出现为 nil，出现为 &true，出现且取反为 &false。
//
// 解析会跳过 "-m MODULE"，因为多数参数名本身已足够唯一；少数重名情况
// （如 "-m tcp --sport" 与 "-m udp --sport"）彼此互斥，放在同一字段反而更方便。
type Rule struct {
	// Raw 保留原始规则文本，便于日志与回写。
	Raw string

	Chain   iptables.Chain `param:"-A"`
	Comment *IPTablesValue `param:"--comment"`

	Protocol *IPTablesValue `param:"-p" negatable:"true"`

	SourceAddress *IPTablesValue `param:"-s" negatable:"true"`
	SourceType    *IPTablesValue `param:"--src-type" negatable:"true"`
	SourcePort    *IPTablesValue `param:"--sport" negatable:"true"`

	DestinationAddress *IPTablesValue `param:"-d" negatable:"true"`
	DestinationType    *IPTablesValue `param:"--dst-type" negatable:"true"`
	DestinationPort    *IPTablesValue `param:"--dport" negatable:"true"`

	MatchSet *IPTablesValue `param:"--match-set" negatable:"true"`

	Jump            *IPTablesValue `param:"-j"`
	RandomFully     *bool          `param:"--random-fully"`
	Probability     *IPTablesValue `param:"--probability"`
	DNATDestination *IPTablesValue `param:"--to-destination"`

	// 这些参数的值并不使用，只关心它们是否出现。
	AffinityCheck *bool          `param:"--rcheck" negatable:"true"`
	MarkCheck     *IPTablesValue `param:"--mark" negatable:"true"`
	CTStateCheck  *IPTablesValue `param:"--ctstate" negatable:"true"`

	// 单测目前不关心这些值，但被解析的规则里确实会出现，
	// 因此仍需定义其解析方式，避免误判为未知参数。
	AffinityName    *IPTablesValue `param:"--name"`
	AffinitySeconds *IPTablesValue `param:"--seconds"`
	AffinitySet     *bool          `param:"--set" negatable:"true"`
	AffinityReap    *bool          `param:"--reap"`
	StatisticMode   *IPTablesValue `param:"--mode"`
}

// IPTablesValue 表示规则中一个参数的值，并记录该参数是否被取反。
type IPTablesValue struct {
	Negated bool
	Value   string
}

// String 便于调试输出；否则 %v 只会打印指针地址。
func (v *IPTablesValue) String() string {
	if v.Negated {
		return fmt.Sprintf("NOT %q", v.Value)
	} else {
		return fmt.Sprintf("%q", v.Value)
	}
}

// Matches 按 v.Negated 判断 cmp 是否等于 / 不等于 v.Value。
func (v *IPTablesValue) Matches(cmp string) bool {
	if v.Negated {
		return v.Value != cmp
	} else {
		return v.Value == cmp
	}
}

// findParamField 在 value 中查找带有 `param:"${param}"` 标签的字段，
// 返回该字段值的指针及其 "negatable" 标签值。
func findParamField(value reflect.Value, param string) (*reflect.Value, bool) {
	typ := value.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		if field.Tag.Get("param") == param {
			fValue := value.Field(i)
			return &fValue, field.Tag.Get("negatable") == "true"
		}
	}
	return nil, false
}

// wordRegex 匹配单个词或带引号的字符串，要求其位于串首或前面是空白。
var wordRegex = regexp.MustCompile(`(?:^|\s)("[^"]*"|[^"]\S*)`)

// ParseRule 用来区分两类参数字段的类型。
var boolPtrType = reflect.PointerTo(reflect.TypeOf(true))
var ipTablesValuePtrType = reflect.TypeOf((*IPTablesValue)(nil))

// ParseRule 解析一条规则。strict 为 false 时只解析已识别参数、忽略未知参数；
// 为 true 时遇到未知参数即报错。
func ParseRule(rule string, strict bool) (*Rule, error) {
	parsed := &Rule{Raw: rule}

	// 按“词”切分规则，带引号的字符串视为一个词。
	var words []string
	for _, match := range wordRegex.FindAllStringSubmatch(rule, -1) {
		words = append(words, strings.Trim(match[1], `"`))
	}

	// 链名必须出现在最前面，且不能是唯一内容。
	if len(words) < 2 || words[0] != "-A" {
		return nil, fmt.Errorf(`bad iptables rule (does not start with "-A CHAIN")`)
	} else if len(words) < 3 {
		return nil, fmt.Errorf("bad iptables rule (no match rules)")
	}

	// 逐词根据 Rule 的字段标签识别 iptables 参数。非严格模式下隐含假设：
	// 未知参数不会携带一个看起来像其他参数的参数值。
	v := reflect.ValueOf(parsed).Elem()
	negated := false
	for w := 0; w < len(words); {
		if words[w] == "-m" && w < len(words)-1 {
			// 跳过 "-m MODULE"：参数名本身已足够唯一，无需关心模块名。
			w += 2
			continue
		}

		if words[w] == "!" {
			negated = true
			w++
			continue
		}

		// 其余词尝试匹配 Rule 的字段。
		if field, negatable := findParamField(v, words[w]); field != nil {
			if negated && !negatable {
				return nil, fmt.Errorf("cannot negate parameter %q", words[w])
			}
			if field.Type() != boolPtrType && w == len(words)-1 {
				return nil, fmt.Errorf("parameter %q requires an argument", words[w])
			}
			switch field.Type() {
			case boolPtrType:
				boolVal := !negated
				field.Set(reflect.ValueOf(&boolVal))
				w++
			case ipTablesValuePtrType:
				field.Set(reflect.ValueOf(&IPTablesValue{Negated: negated, Value: words[w+1]}))
				w += 2
			default:
				field.SetString(words[w+1])
				w += 2
			}
		} else if strict {
			return nil, fmt.Errorf("unrecognized parameter %q", words[w])
		} else {
			// 非严格模式下忽略未知参数。
			w++
		}

		negated = false
	}

	return parsed, nil
}
