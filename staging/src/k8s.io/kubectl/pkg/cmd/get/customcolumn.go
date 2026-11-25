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

package get

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"strings"

	"github.com/liggitt/tabwriter"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/client-go/util/jsonpath"
)

var jsonRegexp = regexp.MustCompile(`^\{\.?([^{}]+)\}$|^\.?([^{}]+)$`)

// RelaxedJSONPathExpression 尽量灵活处理 JSONPath 表达式，它接受：
//   - metadata.name（没有前导 '.' 或花括号 '{...}'）
//   - {metadata.name}（没有前导 '.'）
//   - .metadata.name（没有花括号 '{...}'）
//   - {.metadata.name}（完整表达式）
//
// 并将它们全部转换为合法的 jsonpath 表达式：
//
//	{.metadata.name}
func RelaxedJSONPathExpression(pathExpression string) (string, error) {
	if len(pathExpression) == 0 {
		return pathExpression, nil
	}
	submatches := jsonRegexp.FindStringSubmatch(pathExpression)
	if submatches == nil {
		return "", fmt.Errorf("unexpected path string, expected a 'name1.name2' or '.name1.name2' or '{name1.name2}' or '{.name1.name2}'")
	}
	if len(submatches) != 3 {
		return "", fmt.Errorf("unexpected submatch list: %v", submatches)
	}
	var fieldSpec string
	if len(submatches[1]) != 0 {
		fieldSpec = submatches[1]
	} else {
		fieldSpec = submatches[2]
	}
	return fmt.Sprintf("{.%s}", fieldSpec), nil
}

// NewCustomColumnsPrinterFromSpec 根据以逗号分隔的 <header>:<jsonpath-field-spec> 对创建自定义列打印器。
// 例如，NAME:metadata.name,API_VERSION:apiVersion 会创建一个打印如下内容的打印器：
//
//	NAME               API_VERSION
//	foo                bar
func NewCustomColumnsPrinterFromSpec(spec string, decoder runtime.Decoder, noHeaders bool) (*CustomColumnsPrinter, error) {
	if len(spec) == 0 {
		return nil, fmt.Errorf("custom-columns format specified but no custom columns given")
	}
	parts := strings.Split(spec, ",")
	columns := make([]Column, len(parts))
	for ix := range parts {
		colSpec := strings.SplitN(parts[ix], ":", 2)
		if len(colSpec) != 2 {
			return nil, fmt.Errorf("unexpected custom-columns spec: %s, expected <header>:<json-path-expr>", parts[ix])
		}
		spec, err := RelaxedJSONPathExpression(colSpec[1])
		if err != nil {
			return nil, err
		}
		columns[ix] = Column{Header: colSpec[0], FieldSpec: spec}
	}
	return &CustomColumnsPrinter{Columns: columns, Decoder: decoder, NoHeaders: noHeaders}, nil
}

func splitOnWhitespace(line string) []string {
	lineScanner := bufio.NewScanner(bytes.NewBufferString(line))
	lineScanner.Split(bufio.ScanWords)
	result := []string{}
	for lineScanner.Scan() {
		result = append(result, lineScanner.Text())
	}
	return result
}

// NewCustomColumnsPrinterFromTemplate 根据模板流创建自定义列打印器。模板应
// 由两行组成，以空白分隔。第一行是表头行，第二行是 jsonpath 字段规格
// 例如，下面的模板：
// NAME               API_VERSION
// {metadata.name}    {apiVersion}
func NewCustomColumnsPrinterFromTemplate(templateReader io.Reader, decoder runtime.Decoder) (*CustomColumnsPrinter, error) {
	scanner := bufio.NewScanner(templateReader)
	if !scanner.Scan() {
		return nil, fmt.Errorf("invalid template, missing header line. Expected format is one line of space separated headers, one line of space separated column specs.")
	}
	headers := splitOnWhitespace(scanner.Text())

	if !scanner.Scan() {
		return nil, fmt.Errorf("invalid template, missing spec line. Expected format is one line of space separated headers, one line of space separated column specs.")
	}
	specs := splitOnWhitespace(scanner.Text())

	if len(headers) != len(specs) {
		return nil, fmt.Errorf("number of headers (%d) and field specifications (%d) don't match", len(headers), len(specs))
	}

	columns := make([]Column, len(headers))
	for ix := range headers {
		spec, err := RelaxedJSONPathExpression(specs[ix])
		if err != nil {
			return nil, err
		}
		columns[ix] = Column{
			Header:    headers[ix],
			FieldSpec: spec,
		}
	}
	return &CustomColumnsPrinter{Columns: columns, Decoder: decoder, NoHeaders: false}, nil
}

// Column 表示用户指定的列
type Column struct {
	// 打印在列上方的表头，通常风格为全大写
	Header string
	// 以 JSONPath 形式指向对象中要打印字段的指针
	// 例如 {.ObjectMeta.Name}，详见 pkg/util/jsonpath。
	FieldSpec string
}

// CustomColumnPrinter 是一个打印器，知道如何打印 `Columns` 数组中
// 模板所指定的任意数据列
type CustomColumnsPrinter struct {
	Columns   []Column
	Decoder   runtime.Decoder
	NoHeaders bool
	// lastType 记录上次打印的资源类型，以免在打印
	// 相同类型的资源时重复打印表头。
	lastType reflect.Type
}

func (s *CustomColumnsPrinter) PrintObj(obj runtime.Object, out io.Writer) error {
	// 这里使用 reflect.Indirect 是为了从指针中获取实际值。
	// 我们需要实际值才能获取对象所属的包路径。
	// 此处不加区分地使用 reflect.Indirect 是安全的，因为所有 runtime.Object 都应为指针。
	if printers.InternalObjectPreventer.IsForbidden(reflect.Indirect(reflect.ValueOf(obj)).Type().PkgPath()) {
		return errors.New(printers.InternalObjectPrinterErr)
	}

	if _, found := out.(*tabwriter.Writer); !found {
		w := printers.GetNewTabWriter(out)
		out = w
		defer w.Flush()
	}

	t := reflect.TypeOf(obj)
	if !s.NoHeaders && t != s.lastType {
		headers := make([]string, len(s.Columns))
		for ix := range s.Columns {
			headers[ix] = s.Columns[ix].Header
		}
		fmt.Fprintln(out, strings.Join(headers, "\t"))
		s.lastType = t
	}
	parsers := make([]*jsonpath.JSONPath, len(s.Columns))
	for ix := range s.Columns {
		parsers[ix] = jsonpath.New(fmt.Sprintf("column%d", ix)).AllowMissingKeys(true)
		if err := parsers[ix].Parse(s.Columns[ix].FieldSpec); err != nil {
			return err
		}
	}

	if meta.IsListType(obj) {
		objs, err := meta.ExtractList(obj)
		if err != nil {
			return err
		}
		for ix := range objs {
			if err := s.printOneObject(objs[ix], parsers, out); err != nil {
				return err
			}
		}
	} else {
		if err := s.printOneObject(obj, parsers, out); err != nil {
			return err
		}
	}
	return nil
}

func (s *CustomColumnsPrinter) printOneObject(obj runtime.Object, parsers []*jsonpath.JSONPath, out io.Writer) error {
	columns := make([]string, len(parsers))
	switch u := obj.(type) {
	case *metav1.WatchEvent:
		if printers.InternalObjectPreventer.IsForbidden(reflect.Indirect(reflect.ValueOf(u.Object.Object)).Type().PkgPath()) {
			return errors.New(printers.InternalObjectPrinterErr)
		}
		unstructuredObject, err := runtime.DefaultUnstructuredConverter.ToUnstructured(u.Object.Object)
		if err != nil {
			return err
		}
		obj = &unstructured.Unstructured{
			Object: map[string]interface{}{
				"type":   u.Type,
				"object": unstructuredObject,
			},
		}

	case *runtime.Unknown:
		if len(u.Raw) > 0 {
			var err error
			if obj, err = runtime.Decode(s.Decoder, u.Raw); err != nil {
				return fmt.Errorf("can't decode object for printing: %v (%s)", err, u.Raw)
			}
		}
	}

	for ix := range parsers {
		parser := parsers[ix]

		var values [][]reflect.Value
		var err error
		if unstructured, ok := obj.(runtime.Unstructured); ok {
			values, err = parser.FindResults(unstructured.UnstructuredContent())
		} else {
			values, err = parser.FindResults(reflect.ValueOf(obj).Elem().Interface())
		}

		if err != nil {
			return err
		}
		valueStrings := []string{}
		if len(values) == 0 || len(values[0]) == 0 {
			valueStrings = append(valueStrings, "<none>")
		}
		for arrIx := range values {
			for valIx := range values[arrIx] {
				valueStrings = append(valueStrings, printers.EscapeTerminal(fmt.Sprint(values[arrIx][valIx].Interface())))
			}
		}
		columns[ix] = strings.Join(valueStrings, ",")
	}
	fmt.Fprintln(out, strings.Join(columns, "\t"))
	return nil
}
