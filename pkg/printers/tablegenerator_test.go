/*
Copyright 2019 The Kubernetes Authors.

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

package printers

import (
	"fmt"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1beta1 "k8s.io/apimachinery/pkg/apis/meta/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestPrintType 是测试用的自定义类型，实现 runtime.Object 接口，
// 用于验证 TableGenerator 支持自定义类型的 print handler 注册。
type TestPrintType struct {
	Data string
}

func (obj *TestPrintType) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }
func (obj *TestPrintType) DeepCopyObject() runtime.Object {
	if obj == nil {
		return nil
	}
	clone := *obj
	return &clone
}

// PrintCustomType 是成功的自定义 print handler，将 TestPrintType 的 Data 字段作为唯一单元格返回。
func PrintCustomType(obj *TestPrintType, options GenerateOptions) ([]metav1beta1.TableRow, error) {
	return []metav1beta1.TableRow{{Cells: []interface{}{obj.Data}}}, nil
}

// ErrorPrintHandler 始终返回固定错误，供测试核对 GenerateTable 是否原样向上传播处理器错误。
func ErrorPrintHandler(obj *TestPrintType, options GenerateOptions) ([]metav1beta1.TableRow, error) {
	return nil, fmt.Errorf("ErrorPrintHandler error")
}

// TestCustomTypePrinting 验证 TableGenerator 可以注册自定义类型的 print handler，
// 并正确生成包含自定义数据的 Table 对象。
func TestCustomTypePrinting(t *testing.T) {
	columns := []metav1beta1.TableColumnDefinition{{Name: "Data"}}
	generator := NewTableGenerator()
	err := generator.TableHandler(columns, PrintCustomType)
	if err != nil {
		t.Fatalf("An error occurred when adds a print handler with a given set of columns: %#v", err)
	}

	obj := TestPrintType{"test object"}
	table, err := generator.GenerateTable(&obj, GenerateOptions{})
	if err != nil {
		t.Fatalf("An error occurred generating the table for custom type: %#v", err)
	}

	expectedTable := &metav1.Table{
		ColumnDefinitions: []metav1.TableColumnDefinition{{Name: "Data"}},
		Rows:              []metav1.TableRow{{Cells: []interface{}{"test object"}}},
	}
	if !reflect.DeepEqual(expectedTable, table) {
		t.Errorf("Error generating table from custom type. Expected (%#v), got (%#v)", expectedTable, table)
	}
}

// TestPrintHandlerError 验证当自定义 print handler 返回错误时，
// GenerateTable 正确传播该错误，而非静默失败。
func TestPrintHandlerError(t *testing.T) {
	columns := []metav1beta1.TableColumnDefinition{{Name: "Data"}}
	generator := NewTableGenerator()
	err := generator.TableHandler(columns, ErrorPrintHandler)
	if err != nil {
		t.Fatalf("An error occurred when adds a print handler with a given set of columns: %#v", err)
	}

	obj := TestPrintType{"test object"}
	_, err = generator.GenerateTable(&obj, GenerateOptions{})
	if err == nil || err.Error() != "ErrorPrintHandler error" {
		t.Errorf("Did not get the expected error: %#v", err)
	}
}
