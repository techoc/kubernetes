/*
Copyright 2020 The Kubernetes Authors.

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

package netpol

import (
	"strings"

	"k8s.io/kubernetes/test/e2e/framework"
)

// TruthTable 接收 n 个 item，为每个有序对维护一张 n x n 的布尔值表
type TruthTable struct {
	Froms  []string
	Tos    []string
	toSet  map[string]bool
	Values map[string]map[string]bool
}

// NewTruthTableFromItems 用 items 创建新的 truth table
func NewTruthTableFromItems(items []string, defaultValue *bool) *TruthTable {
	return NewTruthTable(items, items, defaultValue)
}

// NewTruthTable 用 froms 和 tos 创建新的 truth table
func NewTruthTable(froms []string, tos []string, defaultValue *bool) *TruthTable {
	values := map[string]map[string]bool{}
	for _, from := range froms {
		values[from] = map[string]bool{}
		for _, to := range tos {
			if defaultValue != nil {
				values[from][to] = *defaultValue
			}
		}
	}
	toSet := map[string]bool{}
	for _, to := range tos {
		toSet[to] = true
	}
	return &TruthTable{
		Froms:  froms,
		Tos:    tos,
		toSet:  toSet,
		Values: values,
	}
}

// 每一对 item 都有值时 IsComplete 返回 true，否则返回 false。
func (tt *TruthTable) IsComplete() bool {
	for _, from := range tt.Froms {
		for _, to := range tt.Tos {
			if _, ok := tt.Values[from][to]; !ok {
				return false
			}
		}
	}
	return true
}

// Set 设置 from->to 的值
func (tt *TruthTable) Set(from string, to string, value bool) {
	dict, ok := tt.Values[from]
	if !ok {
		framework.Failf("from-key %s not found", from)
	}
	if _, ok := tt.toSet[to]; !ok {
		framework.Failf("to-key %s not allowed", to)
	}
	dict[to] = value
}

// SetAllFrom 设置 from = 'from' 的所有值
func (tt *TruthTable) SetAllFrom(from string, value bool) {
	dict, ok := tt.Values[from]
	if !ok {
		framework.Failf("from-key %s not found", from)
	}
	for _, to := range tt.Tos {
		dict[to] = value
	}
}

// SetAllTo 设置 to = 'to' 的所有值
func (tt *TruthTable) SetAllTo(to string, value bool) {
	if _, ok := tt.toSet[to]; !ok {
		framework.Failf("to-key %s not found", to)
	}
	for _, from := range tt.Froms {
		tt.Values[from][to] = value
	}
}

// Get 获取指定值
func (tt *TruthTable) Get(from string, to string) bool {
	dict, ok := tt.Values[from]
	if !ok {
		framework.Failf("from-key %s not found", from)
	}
	val, ok := dict[to]
	if !ok {
		framework.Failf("to-key %s not found in map (%+v)", to, dict)
	}
	return val
}

// Compare 检查两张 truth table 是否相等，并
// 用第三张 truth table 表示结果。两张表预期
// 拥有完全相同的 item。
func (tt *TruthTable) Compare(other *TruthTable) *TruthTable {
	if len(tt.Froms) != len(other.Froms) || len(tt.Tos) != len(other.Tos) {
		framework.Failf("cannot compare tables of different dimensions")
	}
	for i, fr := range tt.Froms {
		if other.Froms[i] != fr {
			framework.Failf("cannot compare: from keys at index %d do not match (%s vs %s)", i, other.Froms[i], fr)
		}
	}
	for i, to := range tt.Tos {
		if other.Tos[i] != to {
			framework.Failf("cannot compare: to keys at index %d do not match (%s vs %s)", i, other.Tos[i], to)
		}
	}

	values := map[string]map[string]bool{}
	for from, dict := range tt.Values {
		values[from] = map[string]bool{}
		for to, val := range dict {
			values[from][to] = val == other.Values[from][to]
		}
	}
	return &TruthTable{
		Froms:  tt.Froms,
		Tos:    tt.Tos,
		toSet:  tt.toSet,
		Values: values,
	}
}

// PrettyPrint 生成便于查看的可视化表示。
func (tt *TruthTable) PrettyPrint(indent string) string {
	header := indent + strings.Join(append([]string{"-\t"}, tt.Tos...), "\t")
	lines := []string{header}
	for _, from := range tt.Froms {
		line := []string{from}
		for _, to := range tt.Tos {
			mark := "X"
			val, ok := tt.Values[from][to]
			if !ok {
				mark = "?"
			} else if val {
				mark = "."
			}
			line = append(line, mark+"\t")
		}
		lines = append(lines, indent+strings.Join(line, "\t"))
	}
	return strings.Join(lines, "\n")
}
