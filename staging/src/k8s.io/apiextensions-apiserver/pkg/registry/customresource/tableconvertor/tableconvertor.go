/*
Copyright 2018 The Kubernetes Authors.

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

// Package tableconvertor 将自定义资源转换为 kubectl 表格输出。
package tableconvertor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metatable "k8s.io/apimachinery/pkg/api/meta/table"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/client-go/util/jsonpath"
)

// swaggerMetadataDescriptions 提供 metav1 的 swagger 描述，供 Name 列使用。
var swaggerMetadataDescriptions = metav1.ObjectMeta{}.SwaggerDoc()

// New 根据给定的 CRD 列定义创建表格转换器。若打印器定义无法解析，
// 会同时返回错误与一个默认的表格转换器。
func New(crdColumns []apiextensionsv1.CustomResourceColumnDefinition) (rest.TableConvertor, error) {
	headers := []metav1.TableColumnDefinition{
		{Name: "Name", Type: "string", Format: "name", Description: swaggerMetadataDescriptions["name"]},
	}
	c := &convertor{
		headers: headers,
	}

	for _, col := range crdColumns {
		path := jsonpath.New(col.Name)
		if err := path.Parse(fmt.Sprintf("{%s}", col.JSONPath)); err != nil {
			return c, fmt.Errorf("unrecognized column definition %q", col.JSONPath)
		}
		path.AllowMissingKeys(true)

		desc := fmt.Sprintf("Custom resource definition column (in JSONPath format): %s", col.JSONPath)
		if len(col.Description) > 0 {
			desc = col.Description
		}

		c.additionalColumns = append(c.additionalColumns, path)
		c.headers = append(c.headers, metav1.TableColumnDefinition{
			Name:        col.Name,
			Type:        col.Type,
			Format:      col.Format,
			Description: desc,
			Priority:    col.Priority,
		})
	}

	return c, nil
}

// columnPrinter 抽象一列 JSONPath 的取值与渲染逻辑，便于测试替换。
type columnPrinter interface {
	FindResults(data interface{}) ([][]reflect.Value, error)
	PrintResults(w io.Writer, results []reflect.Value) error
}

// convertor 实现 rest.TableConvertor，负责把 CRD 对象转换为表格。
type convertor struct {
	headers           []metav1.TableColumnDefinition
	additionalColumns []columnPrinter
}

// ConvertToTable 将对象或列表转换为 metav1.Table。
func (c *convertor) ConvertToTable(ctx context.Context, obj runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	table := &metav1.Table{}
	opt, ok := tableOptions.(*metav1.TableOptions)
	// 调用方可以要求不返回列定义
	noHeaders := ok && opt != nil && opt.NoHeaders
	if !noHeaders {
		table.ColumnDefinitions = c.headers
	}

	if m, err := meta.ListAccessor(obj); err == nil {
		table.ResourceVersion = m.GetResourceVersion()
		table.Continue = m.GetContinue()
		table.RemainingItemCount = m.GetRemainingItemCount()
	} else {
		if m, err := meta.CommonAccessor(obj); err == nil {
			table.ResourceVersion = m.GetResourceVersion()
		}
	}

	var err error
	buf := &bytes.Buffer{}
	table.Rows, err = metatable.MetaToTableRow(obj, func(obj runtime.Object, m metav1.Object, name, age string) ([]interface{}, error) {
		cells := make([]interface{}, 1, 1+len(c.additionalColumns))
		cells[0] = name
		customHeaders := c.headers[1:]
		us, ok := obj.(runtime.Unstructured)
		if !ok {
			// 非结构化转换失败时无法计算额外列，直接返回错误
			m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
			if err != nil {
				return nil, err
			}
			us = &unstructured.Unstructured{Object: m}
		}
		for i, column := range c.additionalColumns {
			results, err := column.FindResults(us.UnstructuredContent())
			if err != nil || len(results) == 0 || len(results[0]) == 0 {
				cells = append(cells, nil)
				continue
			}

			// 仅支持简单 JSONPath，因此可以假定至多一个结果（无结果的情况上面已过滤）
			value := results[0][0].Interface()
			if customHeaders[i].Type == "string" {
				if err := column.PrintResults(buf, []reflect.Value{reflect.ValueOf(value)}); err == nil {
					cells = append(cells, buf.String())
					buf.Reset()
				} else {
					cells = append(cells, nil)
				}
			} else {
				cells = append(cells, cellForJSONValue(customHeaders[i].Type, value))
			}
		}
		return cells, nil
	})
	return table, err
}

// cellForJSONValue 按列类型将 JSON 值转换为表格单元格值；类型不匹配时返回 nil。
func cellForJSONValue(headerType string, value interface{}) interface{} {
	if value == nil {
		return nil
	}

	switch headerType {
	case "integer":
		switch typed := value.(type) {
		case int64:
			return typed
		case float64:
			return int64(typed)
		case json.Number:
			if i64, err := typed.Int64(); err == nil {
				return i64
			}
		}
	case "number":
		switch typed := value.(type) {
		case int64:
			return float64(typed)
		case float64:
			return typed
		case json.Number:
			if f, err := typed.Float64(); err == nil {
				return f
			}
		}
	case "boolean":
		if b, ok := value.(bool); ok {
			return b
		}
	case "string":
		if s, ok := value.(string); ok {
			return s
		}
	case "date":
		if typed, ok := value.(string); ok {
			var timestamp metav1.Time
			err := timestamp.UnmarshalQueryParameter(typed)
			if err != nil {
				return "<invalid>"
			}
			return metatable.ConvertToHumanReadableDateType(timestamp)
		}
	}

	return nil
}
