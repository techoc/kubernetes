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
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"

	"k8s.io/client-go/third_party/forked/golang/template"
)

// JSONPath 保存一次解析后的模板执行状态。
// 注意它包含 range 嵌套深度等可变字段，因此同一个实例不可被多个 goroutine 并发复用。
type JSONPath struct {
	name       string
	parser     *Parser
	beginRange int
	inRange    int
	endRange   int

	lastEndNode *Node

	allowMissingKeys bool
	outputJSON       bool
}

// New 创建给定名称的 JSONPath。
func New(name string) *JSONPath {
	return &JSONPath{
		name:       name,
		beginRange: 0,
		inRange:    0,
		endRange:   0,
	}
}

// AllowMissingKeys 控制字段或 map key 找不到时是报错还是返回空结果；返回接收者
// 以便链式调用。
func (j *JSONPath) AllowMissingKeys(allow bool) *JSONPath {
	j.allowMissingKeys = allow
	return j
}

// Parse 解析给定模板，失败时返回错误。
func (j *JSONPath) Parse(text string) error {
	var err error
	j.parser, err = Parse(j.name, text)
	return err
}

// Execute 把数据绑定到模板并把结果写出。
//
// 所有顶层结果按出现顺序写入同一个 writer，彼此以空格分隔（由 PrintResults 负责）。
func (j *JSONPath) Execute(wr io.Writer, data interface{}) error {
	fullResults, err := j.FindResults(data)
	if err != nil {
		return err
	}
	for ix := range fullResults {
		if err := j.PrintResults(wr, fullResults[ix]); err != nil {
			return err
		}
	}
	return nil
}

// FindResults 按节点顺序求值，返回每个顶层节点产生的一组结果。
//
// range 节点通过递归调用自身实现循环：递归前临时替换 parser.Root.Nodes，
// 使循环体只处理 range 与 end 之间的节点，返回后再恢复到结束节点之后。
func (j *JSONPath) FindResults(data interface{}) ([][]reflect.Value, error) {
	if j.parser == nil {
		return nil, fmt.Errorf("%s is an incomplete jsonpath template", j.name)
	}

	cur := []reflect.Value{reflect.ValueOf(data)}
	nodes := j.parser.Root.Nodes
	fullResult := [][]reflect.Value{}
	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		results, err := j.walk(cur, node)
		if err != nil {
			return nil, err
		}

		// 遇到 end 节点则结束当前块。
		if j.endRange > 0 && j.endRange <= j.inRange {
			j.endRange--
			j.lastEndNode = &nodes[i]
			break
		}
		// 遇到 range 节点则开始循环。
		if j.beginRange > 0 {
			j.beginRange--
			j.inRange++
			if len(results) > 0 {
				for _, value := range results {
					j.parser.Root.Nodes = nodes[i+1:]
					nextResults, err := j.FindResults(value.Interface())
					if err != nil {
						return nil, err
					}
					fullResult = append(fullResult, nextResults...)
				}
			} else {
				// 即使 range 没有结果，也要处理其内部节点，使位置推进到 end 节点。
				j.parser.Root.Nodes = nodes[i+1:]
				_, err := j.FindResults(nil)
				if err != nil {
					return nil, err
				}
			}
			j.inRange--

			// 快进到最近遇到的 end 节点之后继续处理。
			for k := i + 1; k < len(nodes); k++ {
				if &nodes[k] == j.lastEndNode {
					i = k
					break
				}
			}
			continue
		}
		fullResult = append(fullResult, results)
	}
	return fullResult, nil
}

// EnableJSONOutput 让 PrintResults 返回结果的 JSON 数组。
func (j *JSONPath) EnableJSONOutput(v bool) {
	j.outputJSON = v
}

// PrintResults 把结果写入 writer。
//
// 复合类型（map/slice/struct 等）默认输出 JSON；标量走 evalToText，
// 以免给字符串加上多余的引号。多个结果之间以单空格分隔。
func (j *JSONPath) PrintResults(wr io.Writer, results []reflect.Value) error {
	if j.outputJSON {
		// 把 []reflect.Value 转换为 json 可marshal 的形式。
		r := make([]interface{}, 0, len(results))
		for i := range results {
			r = append(r, results[i].Interface())
		}
		results = []reflect.Value{reflect.ValueOf(r)}
	}
	for i, r := range results {
		var text []byte
		var err error
		outputJSON := true
		kind := r.Kind()
		if kind == reflect.Interface {
			kind = r.Elem().Kind()
		}
		switch kind {
		case reflect.Map:
		case reflect.Array:
		case reflect.Slice:
		case reflect.Struct:
		default:
			outputJSON = false
		}
		switch {
		case outputJSON || j.outputJSON:
			if j.outputJSON {
				text, err = json.MarshalIndent(r.Interface(), "", "    ")
				text = append(text, '\n')
			} else {
				text, err = json.Marshal(r.Interface())
			}
		default:
			text, err = j.evalToText(r)
		}
		if err != nil {
			return err
		}
		if i != len(results)-1 {
			text = append(text, ' ')
		}
		if _, err = wr.Write(text); err != nil {
			return err
		}
	}

	return nil

}

// walk 按 DFS 顺序访问以给定节点为根的树。
func (j *JSONPath) walk(value []reflect.Value, node Node) ([]reflect.Value, error) {
	// 按节点类型分派到对应求值器；未知节点类型说明解析阶段出现了不一致。
	switch node := node.(type) {
	case *ListNode:
		return j.evalList(value, node)
	case *TextNode:
		return []reflect.Value{reflect.ValueOf(node.Text)}, nil
	case *FieldNode:
		return j.evalField(value, node)
	case *ArrayNode:
		return j.evalArray(value, node)
	case *FilterNode:
		return j.evalFilter(value, node)
	case *IntNode:
		return j.evalInt(value, node)
	case *BoolNode:
		return j.evalBool(value, node)
	case *FloatNode:
		return j.evalFloat(value, node)
	case *WildcardNode:
		return j.evalWildcard(value, node)
	case *RecursiveNode:
		return j.evalRecursive(value, node)
	case *UnionNode:
		return j.evalUnion(value, node)
	case *IdentifierNode:
		return j.evalIdentifier(value, node)
	default:
		return value, fmt.Errorf("unexpected Node %v", node)
	}
}

// evalInt 求值 IntNode。
func (j *JSONPath) evalInt(input []reflect.Value, node *IntNode) ([]reflect.Value, error) {
	result := make([]reflect.Value, len(input))
	for i := range input {
		result[i] = reflect.ValueOf(node.Value)
	}
	return result, nil
}

// evalFloat 求值 FloatNode。
func (j *JSONPath) evalFloat(input []reflect.Value, node *FloatNode) ([]reflect.Value, error) {
	result := make([]reflect.Value, len(input))
	for i := range input {
		result[i] = reflect.ValueOf(node.Value)
	}
	return result, nil
}

// evalBool 求值 BoolNode。
func (j *JSONPath) evalBool(input []reflect.Value, node *BoolNode) ([]reflect.Value, error) {
	result := make([]reflect.Value, len(input))
	for i := range input {
		result[i] = reflect.ValueOf(node.Value)
	}
	return result, nil
}

// evalList 求值 ListNode。
func (j *JSONPath) evalList(value []reflect.Value, node *ListNode) ([]reflect.Value, error) {
	var err error
	curValue := value
	for _, node := range node.Nodes {
		curValue, err = j.walk(curValue, node)
		if err != nil {
			return curValue, err
		}
	}
	return curValue, nil
}

// evalIdentifier 求值 IdentifierNode。
func (j *JSONPath) evalIdentifier(input []reflect.Value, node *IdentifierNode) ([]reflect.Value, error) {
	results := []reflect.Value{}
	switch node.Name {
	case "range":
		j.beginRange++
		results = input
	case "end":
		if j.inRange > 0 {
			j.endRange++
		} else {
			return results, fmt.Errorf("not in range, nothing to end")
		}
	default:
		return input, fmt.Errorf("unrecognized identifier %v", node.Name)
	}
	return results, nil
}

// evalArray 求值 ArrayNode。
//
// 支持 [start:end:step] 切片，负索引从末尾计数；params 未给出时分别默认 0/len。
func (j *JSONPath) evalArray(input []reflect.Value, node *ArrayNode) ([]reflect.Value, error) {
	result := []reflect.Value{}
	for _, value := range input {

		value, isNil := template.Indirect(value)
		if isNil {
			continue
		}
		if value.Kind() != reflect.Array && value.Kind() != reflect.Slice {
			return input, fmt.Errorf("%v is not array or slice", value.Type())
		}
		params := node.Params
		if !params[0].Known {
			params[0].Value = 0
		}
		// 负起始索引转换为相对末尾的位置。
		if params[0].Value < 0 {
			params[0].Value += value.Len()
		}
		if !params[1].Known {
			params[1].Value = value.Len()
		}

		// Derived 表示结束索引来自 [-1] 这类单元素写法，0 需要解释为末尾。
		if params[1].Value < 0 || (params[1].Value == 0 && params[1].Derived) {
			params[1].Value += value.Len()
		}
		sliceLength := value.Len()
		if params[1].Value != params[0].Value { // 请求 0 个元素时也放行。
			if params[0].Value >= sliceLength || params[0].Value < 0 {
				return input, fmt.Errorf("array index out of bounds: index %d, length %d", params[0].Value, sliceLength)
			}
			if params[1].Value > sliceLength || params[1].Value < 0 {
				return input, fmt.Errorf("array index out of bounds: index %d, length %d", params[1].Value-1, sliceLength)
			}
			if params[0].Value > params[1].Value {
				return input, fmt.Errorf("starting index %d is greater than ending index %d", params[0].Value, params[1].Value)
			}
		} else {
			return result, nil
		}

		value = value.Slice(params[0].Value, params[1].Value)

		step := 1
		if params[2].Known {
			if params[2].Value <= 0 {
				return input, fmt.Errorf("step must be > 0")
			}
			step = params[2].Value
		}
		for i := 0; i < value.Len(); i += step {
			result = append(result, value.Index(i))
		}
	}
	return result, nil
}

// evalUnion 求值 UnionNode。
func (j *JSONPath) evalUnion(input []reflect.Value, node *UnionNode) ([]reflect.Value, error) {
	result := []reflect.Value{}
	for _, listNode := range node.Nodes {
		temp, err := j.evalList(input, listNode)
		if err != nil {
			return input, err
		}
		result = append(result, temp...)
	}
	return result, nil
}

// findFieldInValue 在结构体中按 json tag 查找字段；
// 匿名且 tag 为空的字段视为 inline，会递归进入其内部查找。
// 找不到时回退到 Go 字段名，保持与 encoding/json 类似的宽松行为。
func (j *JSONPath) findFieldInValue(value *reflect.Value, node *FieldNode) (reflect.Value, error) {
	t := value.Type()
	var inlineValue *reflect.Value
	for ix := 0; ix < t.NumField(); ix++ {
		f := t.Field(ix)
		jsonTag, _ := f.Tag.Lookup("json")
		parts := strings.Split(jsonTag, ",")
		if len(parts) == 0 {
			continue
		}
		if parts[0] == node.Value {
			return value.Field(ix), nil
		}
		if len(parts[0]) == 0 && f.Anonymous {
			val := value.Field(ix)
			inlineValue = &val
		}
	}
	if inlineValue != nil {
		if inlineValue.Kind() == reflect.Struct {
			// handle 'inline'
			match, err := j.findFieldInValue(inlineValue, node)
			if err != nil {
				return reflect.Value{}, err
			}
			if match.IsValid() {
				return match, nil
			}
		}
	}
	return value.FieldByName(node.Value), nil
}

// evalField 求值结构体字段或 map key。
func (j *JSONPath) evalField(input []reflect.Value, node *FieldNode) ([]reflect.Value, error) {
	results := []reflect.Value{}
	// 没有输入就没有输出。
	if len(input) == 0 {
		return results, nil
	}
	for _, value := range input {
		var result reflect.Value
		value, isNil := template.Indirect(value)
		if isNil {
			continue
		}

		if value.Kind() == reflect.Struct {
			var err error
			if result, err = j.findFieldInValue(&value, node); err != nil {
				return nil, err
			}
		} else if value.Kind() == reflect.Map {
			mapKeyType := value.Type().Key()
			nodeValue := reflect.ValueOf(node.Value)
			// 节点值类型必须可转换为 map key 类型。
			if !nodeValue.Type().ConvertibleTo(mapKeyType) {
				return results, fmt.Errorf("%s is not convertible to %s", nodeValue, mapKeyType)
			}
			result = value.MapIndex(nodeValue.Convert(mapKeyType))
		}
		if result.IsValid() {
			results = append(results, result)
		}
	}
	if len(results) == 0 {
		// 默认把“字段不存在”视为错误；allowMissingKeys 打开时返回空结果，
		// 便于 kubectl 对异构对象做批量查询。
		if j.allowMissingKeys {
			return results, nil
		}
		return results, fmt.Errorf("%s is not found", node.Value)
	}
	return results, nil
}

// evalWildcard 提取给定值的全部内容。
func (j *JSONPath) evalWildcard(input []reflect.Value, node *WildcardNode) ([]reflect.Value, error) {
	results := []reflect.Value{}
	for _, value := range input {
		value, isNil := template.Indirect(value)
		if isNil {
			continue
		}

		kind := value.Kind()
		if kind == reflect.Struct {
			for i := 0; i < value.NumField(); i++ {
				results = append(results, value.Field(i))
			}
		} else if kind == reflect.Map {
			for _, key := range value.MapKeys() {
				results = append(results, value.MapIndex(key))
			}
		} else if kind == reflect.Array || kind == reflect.Slice || kind == reflect.String {
			for i := 0; i < value.Len(); i++ {
				results = append(results, value.Index(i))
			}
		}
	}
	return results, nil
}

// evalRecursive 递归访问给定值并把所有值压入结果。
//
// 先序输出：只有包含子元素的节点才会被收集，叶子节点本身不进入结果，
// 这与 {..field} 语义一致（后续 FieldNode 再从中筛选）。
func (j *JSONPath) evalRecursive(input []reflect.Value, node *RecursiveNode) ([]reflect.Value, error) {
	result := []reflect.Value{}
	for _, value := range input {
		results := []reflect.Value{}
		value, isNil := template.Indirect(value)
		if isNil {
			continue
		}

		kind := value.Kind()
		if kind == reflect.Struct {
			for i := 0; i < value.NumField(); i++ {
				results = append(results, value.Field(i))
			}
		} else if kind == reflect.Map {
			for _, key := range value.MapKeys() {
				results = append(results, value.MapIndex(key))
			}
		} else if kind == reflect.Array || kind == reflect.Slice || kind == reflect.String {
			for i := 0; i < value.Len(); i++ {
				results = append(results, value.Index(i))
			}
		}
		if len(results) != 0 {
			result = append(result, value)
			output, err := j.evalRecursive(results, node)
			if err != nil {
				return result, err
			}
			result = append(result, output...)
		}
	}
	return result, nil
}

// evalFilter 按 FilterNode 过滤数组。
//
// 对数组每个元素求 left/right 表达式并应用比较运算符；
// "exists" 是特殊运算符，只判断左侧是否有结果。
func (j *JSONPath) evalFilter(input []reflect.Value, node *FilterNode) ([]reflect.Value, error) {
	results := []reflect.Value{}
	for _, value := range input {
		value, _ = template.Indirect(value)

		if value.Kind() != reflect.Array && value.Kind() != reflect.Slice {
			return input, fmt.Errorf("%v is not array or slice and cannot be filtered", value)
		}
		for i := 0; i < value.Len(); i++ {
			temp := []reflect.Value{value.Index(i)}
			lefts, err := j.evalList(temp, node.Left)

			//case exists
			if node.Operator == "exists" {
				if len(lefts) > 0 {
					results = append(results, value.Index(i))
				}
				continue
			}

			if err != nil {
				return input, err
			}

			var left, right interface{}
			switch {
			case len(lefts) == 0:
				continue
			case len(lefts) > 1:
				return input, fmt.Errorf("can only compare one element at a time")
			}
			left = lefts[0].Interface()

			rights, err := j.evalList(temp, node.Right)
			if err != nil {
				return input, err
			}
			switch {
			case len(rights) == 0:
				continue
			case len(rights) > 1:
				return input, fmt.Errorf("can only compare one element at a time")
			}
			right = rights[0].Interface()

			pass := false
			switch node.Operator {
			case "<":
				pass, err = template.Less(left, right)
			case ">":
				pass, err = template.Greater(left, right)
			case "==":
				pass, err = template.Equal(left, right)
			case "!=":
				pass, err = template.NotEqual(left, right)
			case "<=":
				pass, err = template.LessEqual(left, right)
			case ">=":
				pass, err = template.GreaterEqual(left, right)
			default:
				return results, fmt.Errorf("unrecognized filter operator %s", node.Operator)
			}
			if err != nil {
				return results, err
			}
			if pass {
				results = append(results, value.Index(i))
			}
		}
	}
	return results, nil
}

// evalToText 把 reflect 值转换为对应文本。
//
// 优先使用 template.PrintableValue 取得可直接打印的接口值；
// nil 输出 "null"，其余按 Go 默认格式打印。
func (j *JSONPath) evalToText(v reflect.Value) ([]byte, error) {
	iface, ok := template.PrintableValue(v)
	if !ok {
		return nil, fmt.Errorf("can't print type %s", v.Type())
	}
	if iface == nil {
		return []byte("null"), nil
	}
	var buffer bytes.Buffer
	fmt.Fprint(&buffer, iface)
	return buffer.Bytes(), nil
}
