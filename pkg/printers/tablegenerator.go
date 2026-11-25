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

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

// GenerateOptions 控制表格生成的行为，如是否显示表头、是否显示宽模式列。
type GenerateOptions struct {
	// NoHeaders 为 true 时不输出表格列定义（表头）
	NoHeaders bool
	// Wide 为 true 时显示所有列，包括 Priority != 0 的“宽模式”列
	Wide bool
}

// TableGenerator 将 runtime.Object 转换为 metav1.Table 的接口。
// 实现此接口可以为不同类型的资源提供自定义的表格输出逻辑。
type TableGenerator interface {
	// GenerateTable 按 options 指定的表头和列范围生成表格；实现必须保留对象的分页元数据。
	GenerateTable(obj runtime.Object, options GenerateOptions) (*metav1.Table, error)
}

// PrintHandler 用于注册打印处理器的接口。
// 通过 TableHandler 方法为特定类型注册列定义和对应的打印函数。
type PrintHandler interface {
	// TableHandler 注册列定义及生成表格行的函数；同一对象类型只能注册一次。
	TableHandler(columns []metav1.TableColumnDefinition, printFunc interface{}) error
}

// handlerEntry 存储单个类型的打印处理器注册信息，包括列定义和打印函数。
type handlerEntry struct {
	// columnDefinitions 是该类型表格的列定义
	columnDefinitions []metav1.TableColumnDefinition
	// printFunc 是生成表格行的函数，通过反射调用
	printFunc reflect.Value
}

// HumanReadableGenerator 是 TableGenerator 的具体实现，为特定资源类型生成表格。
// 通过 handlerMap 按类型注册打印处理器，调用 GenerateTable 时根据对象类型查找对应的处理器。
// 生成的表格通过 TablePrinter 的 PrintObj 方法输出。
type HumanReadableGenerator struct {
	// handlerMap 按类型映射到对应的打印处理器，键为资源类型的 reflect.Type
	handlerMap map[reflect.Type]*handlerEntry
}

var _ TableGenerator = &HumanReadableGenerator{}
var _ PrintHandler = &HumanReadableGenerator{}

// NewTableGenerator 创建一个新的 HumanReadableGenerator 实例，可用于调用 GenerateTable。
func NewTableGenerator() *HumanReadableGenerator {
	return &HumanReadableGenerator{
		handlerMap: make(map[reflect.Type]*handlerEntry),
	}
}

// With 接受一组构建函数，按顺序修改 HumanReadableGenerator。
// 返回 receiver 自身以支持链式调用。
func (h *HumanReadableGenerator) With(fns ...func(PrintHandler)) *HumanReadableGenerator {
	for _, fn := range fns {
		fn(h)
	}
	return h
}

// GenerateTable 使用注册的打印处理器为给定对象生成表格。
// 返回的表格包含 options 请求的所有信息，但不会移除行或列——调用方负责过滤。
// 非 Wide 模式下会过滤掉 Priority > 0 的列。
func (h *HumanReadableGenerator) GenerateTable(obj runtime.Object, options GenerateOptions) (*metav1.Table, error) {
	// 根据对象类型查找注册的打印处理器
	t := reflect.TypeOf(obj)
	handler, ok := h.handlerMap[t]
	if !ok {
		return nil, fmt.Errorf("no table handler registered for this type %v", t)
	}

	// 通过反射调用注册的打印函数，传入对象和选项
	args := []reflect.Value{reflect.ValueOf(obj), reflect.ValueOf(options)}
	results := handler.printFunc.Call(args)
	if !results[1].IsNil() {
		return nil, results[1].Interface().(error)
	}

	// 处理列定义：NoHeaders 时不输出列定义；非 Wide 模式时过滤掉高优先级列
	var columns []metav1.TableColumnDefinition
	if !options.NoHeaders {
		columns = handler.columnDefinitions
		if !options.Wide {
			columns = make([]metav1.TableColumnDefinition, 0, len(handler.columnDefinitions))
			for i := range handler.columnDefinitions {
				// Priority != 0 的列仅在 Wide 模式下显示
				if handler.columnDefinitions[i].Priority != 0 {
					continue
				}
				columns = append(columns, handler.columnDefinitions[i])
			}
		}
	}
	// 构建表格对象，从打印函数返回结果中提取行数据
	table := &metav1.Table{
		ListMeta: metav1.ListMeta{
			ResourceVersion: "",
		},
		ColumnDefinitions: columns,
		Rows:              results[0].Interface().([]metav1.TableRow),
	}
	// 从对象中复制分页相关字段：List 类型取 ListAccessor，单资源取 CommonAccessor
	if m, err := meta.ListAccessor(obj); err == nil {
		table.ResourceVersion = m.GetResourceVersion()
		table.Continue = m.GetContinue()
		table.RemainingItemCount = m.GetRemainingItemCount()
	} else {
		if m, err := meta.CommonAccessor(obj); err == nil {
			table.ResourceVersion = m.GetResourceVersion()
		}
	}
	return table, nil
}

// TableHandler 为 HumanReadableGenerator 注册一个打印处理器，包含列定义和打印函数。
// 打印函数的签名必须符合 ValidateRowPrintHandlerFunc 的要求。
func (h *HumanReadableGenerator) TableHandler(columnDefinitions []metav1.TableColumnDefinition, printFunc interface{}) error {
	printFuncValue := reflect.ValueOf(printFunc)
	if err := ValidateRowPrintHandlerFunc(printFuncValue); err != nil {
		utilruntime.HandleError(fmt.Errorf("unable to register print function: %v", err))
		return err
	}
	entry := &handlerEntry{
		columnDefinitions: columnDefinitions,
		printFunc:         printFuncValue,
	}

	// 从打印函数的第一个参数类型作为键，注册到 handlerMap
	objType := printFuncValue.Type().In(0)
	if _, ok := h.handlerMap[objType]; ok {
		err := fmt.Errorf("registered duplicate printer for %v", objType)
		utilruntime.HandleError(err)
		return err
	}
	h.handlerMap[objType] = entry
	return nil
}

// ValidateRowPrintHandlerFunc 验证打印处理器的函数签名是否正确。
// printFunc 是用于打印对象的函数，签名必须为：
//
//	func printFunc(object ObjectType, options GenerateOptions) ([]metav1.TableRow, error)
//
// 其中 ObjectType 是要打印的对象类型，第一个返回值是行切片，
// 每行的单元格数与该打印函数定义的列数匹配。
func ValidateRowPrintHandlerFunc(printFunc reflect.Value) error {
	if printFunc.Kind() != reflect.Func {
		return fmt.Errorf("invalid print handler. %#v is not a function", printFunc)
	}
	funcType := printFunc.Type()
	if funcType.NumIn() != 2 || funcType.NumOut() != 2 {
		return fmt.Errorf("invalid print handler." +
			"Must accept 2 parameters and return 2 value")
	}
	if funcType.In(1) != reflect.TypeOf((*GenerateOptions)(nil)).Elem() ||
		funcType.Out(0) != reflect.TypeOf((*[]metav1.TableRow)(nil)).Elem() ||
		funcType.Out(1) != reflect.TypeOf((*error)(nil)).Elem() {
		return fmt.Errorf("invalid print handler. The expected signature is: "+
			"func handler(obj %v, options GenerateOptions) ([]metav1.TableRow, error)", funcType.In(0))
	}
	return nil
}
