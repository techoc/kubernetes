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
	"fmt"
	"io"
	"reflect"
	"sort"

	"k8s.io/klog/v2"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/client-go/util/jsonpath"
)

// SortingPrinter 在委托给另一个打印器之前，先对列表类型进行排序。
// 非列表类型则直接透传
type SortingPrinter struct {
	SortField string
	Delegate  printers.ResourcePrinter
	Decoder   runtime.Decoder
}

func (s *SortingPrinter) PrintObj(obj runtime.Object, out io.Writer) error {
	if table, isTable := obj.(*metav1.Table); isTable && len(table.Rows) > 1 {
		parsedField, err := RelaxedJSONPathExpression(s.SortField)
		if err != nil {
			parsedField = s.SortField
		}

		if sorter, err := NewTableSorter(table, parsedField); err != nil {
			return err
		} else if err := sorter.Sort(); err != nil {
			return err
		}
		return s.Delegate.PrintObj(table, out)
	}

	if meta.IsListType(obj) {
		if err := s.sortObj(obj); err != nil {
			return err
		}
		return s.Delegate.PrintObj(obj, out)
	}

	return s.Delegate.PrintObj(obj, out)
}

func (s *SortingPrinter) sortObj(obj runtime.Object) error {
	objs, err := meta.ExtractList(obj)
	if err != nil {
		return err
	}
	if len(objs) == 0 {
		return nil
	}

	sorter, err := SortObjects(s.Decoder, objs, s.SortField)
	if err != nil {
		return err
	}

	switch list := obj.(type) {
	case *corev1.List:
		outputList := make([]runtime.RawExtension, len(objs))
		for ix := range objs {
			outputList[ix] = list.Items[sorter.OriginalPosition(ix)]
		}
		list.Items = outputList
		return nil
	}
	return meta.SetList(obj, objs)
}

// SortObjects 根据 fieldInput 对 runtime.Object 排序，并返回实现了
// golang sort 接口的 RuntimeSort
func SortObjects(decoder runtime.Decoder, objs []runtime.Object, fieldInput string) (*RuntimeSort, error) {
	for ix := range objs {
		item := objs[ix]
		switch u := item.(type) {
		case *runtime.Unknown:
			var err error
			// 将 runtime.Unknown 解码为 runtime.Unstructured 以便排序。
			// 我们并不想要已知类型的内部版本。
			if objs[ix], _, err = decoder.Decode(u.Raw, nil, &unstructured.Unstructured{}); err != nil {
				return nil, err
			}
		}
	}

	field, err := RelaxedJSONPathExpression(fieldInput)
	if err != nil {
		return nil, err
	}

	parser := jsonpath.New("sorting").AllowMissingKeys(true)
	if err := parser.Parse(field); err != nil {
		return nil, err
	}

	// 这里不做任何模型校验，因此会遍历所有待排序对象，
	// 如果该字段对其中至少一个对象有效，就认为是有效字段；
	// 否则报错。
	// 注意，这要求排序时稍后再考虑空字段。
	var fieldFoundOnce bool
	for _, obj := range objs {
		values, err := findJSONPathResults(parser, obj)
		if err != nil {
			return nil, err
		}
		if len(values) > 0 && len(values[0]) > 0 {
			fieldFoundOnce = true
			break
		}
	}
	if !fieldFoundOnce {
		return nil, fmt.Errorf("couldn't find any field with path %q in the list of objects", field)
	}

	sorter := NewRuntimeSort(field, objs)
	sort.Sort(sorter)
	return sorter, nil
}

// RuntimeSort 是 golang sort 接口的实现，知道如何对
// runtime.Object 列表进行排序
type RuntimeSort struct {
	field        string
	objs         []runtime.Object
	origPosition []int
}

// NewRuntimeSort 创建一个新的 RuntimeSort 结构体，它实现了 golang sort 接口
func NewRuntimeSort(field string, objs []runtime.Object) *RuntimeSort {
	sorter := &RuntimeSort{field: field, objs: objs, origPosition: make([]int, len(objs))}
	for ix := range objs {
		sorter.origPosition[ix] = ix
	}
	return sorter
}

func (r *RuntimeSort) Len() int {
	return len(r.objs)
}

func (r *RuntimeSort) Swap(i, j int) {
	r.objs[i], r.objs[j] = r.objs[j], r.objs[i]
	r.origPosition[i], r.origPosition[j] = r.origPosition[j], r.origPosition[i]
}

func isLess(i, j reflect.Value) (bool, error) {
	switch i.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return i.Int() < j.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return i.Uint() < j.Uint(), nil
	case reflect.Float32, reflect.Float64:
		return i.Float() < j.Float(), nil
	case reflect.String:
		return i.String() < j.String(), nil
	case reflect.Pointer:
		return isLess(i.Elem(), j.Elem())
	case reflect.Struct:
		// 对 metav1.Time 排序
		in := i.Interface()
		if t, ok := in.(metav1.Time); ok {
			time := j.Interface().(metav1.Time)
			return t.Before(&time), nil
		}
		// 对 resource.Quantity 排序
		if iQuantity, ok := in.(resource.Quantity); ok {
			jQuantity := j.Interface().(resource.Quantity)
			return iQuantity.Cmp(jQuantity) < 0, nil
		}
		// 回退到字段比较
		for idx := 0; idx < i.NumField(); idx++ {
			less, err := isLess(i.Field(idx), j.Field(idx))
			if err != nil || !less {
				return less, err
			}
		}
		return true, nil
	case reflect.Array, reflect.Slice:
		// 注意：i 和 j 的长度可能不同
		for idx := 0; idx < min(i.Len(), j.Len()); idx++ {
			less, err := isLess(i.Index(idx), j.Index(idx))
			if err != nil || !less {
				return less, err
			}
		}
		return true, nil
	case reflect.Interface:
		if i.IsNil() && j.IsNil() {
			return false, nil
		} else if i.IsNil() {
			return true, nil
		} else if j.IsNil() {
			return false, nil
		}
		switch itype := i.Interface().(type) {
		case uint8:
			if jtype, ok := j.Interface().(uint8); ok {
				return itype < jtype, nil
			}
		case uint16:
			if jtype, ok := j.Interface().(uint16); ok {
				return itype < jtype, nil
			}
		case uint32:
			if jtype, ok := j.Interface().(uint32); ok {
				return itype < jtype, nil
			}
		case uint64:
			if jtype, ok := j.Interface().(uint64); ok {
				return itype < jtype, nil
			}
		case int8:
			if jtype, ok := j.Interface().(int8); ok {
				return itype < jtype, nil
			}
		case int16:
			if jtype, ok := j.Interface().(int16); ok {
				return itype < jtype, nil
			}
		case int32:
			if jtype, ok := j.Interface().(int32); ok {
				return itype < jtype, nil
			}
		case int64:
			if jtype, ok := j.Interface().(int64); ok {
				return itype < jtype, nil
			}
		case uint:
			if jtype, ok := j.Interface().(uint); ok {
				return itype < jtype, nil
			}
		case int:
			if jtype, ok := j.Interface().(int); ok {
				return itype < jtype, nil
			}
		case float32:
			if jtype, ok := j.Interface().(float32); ok {
				return itype < jtype, nil
			}
		case float64:
			if jtype, ok := j.Interface().(float64); ok {
				return itype < jtype, nil
			}
		case string:
			if jtype, ok := j.Interface().(string); ok {
				// 检查是否为 Quantity
				itypeQuantity, err := resource.ParseQuantity(itype)
				if err != nil {
					return itype < jtype, nil
				}
				jtypeQuantity, err := resource.ParseQuantity(jtype)
				if err != nil {
					return itype < jtype, nil
				}
				// 两个字符串都是数量值
				return itypeQuantity.Cmp(jtypeQuantity) < 0, nil
			}
		default:
			return false, fmt.Errorf("unsortable type: %T", itype)
		}
		return false, fmt.Errorf("unsortable interface: %v", i.Kind())

	default:
		return false, fmt.Errorf("unsortable type: %v", i.Kind())
	}
}

func (r *RuntimeSort) Less(i, j int) bool {
	iObj := r.objs[i]
	jObj := r.objs[j]

	var iValues [][]reflect.Value
	var jValues [][]reflect.Value
	var err error

	parser := jsonpath.New("sorting").AllowMissingKeys(true)
	err = parser.Parse(r.field)
	if err != nil {
		panic(err)
	}

	iValues, err = findJSONPathResults(parser, iObj)
	if err != nil {
		klog.Fatalf("Failed to get i values for %#v using %s (%#v)", iObj, r.field, err)
	}

	jValues, err = findJSONPathResults(parser, jObj)
	if err != nil {
		klog.Fatalf("Failed to get j values for %#v using %s (%v)", jObj, r.field, err)
	}

	if len(iValues) == 0 || len(iValues[0]) == 0 {
		return true
	}
	if len(jValues) == 0 || len(jValues[0]) == 0 {
		return false
	}
	iField := iValues[0][0]
	jField := jValues[0][0]

	less, err := isLess(iField, jField)
	if err != nil {
		klog.Exitf("Field %s in %T is an unsortable type: %s, err: %v", r.field, iObj, iField.Kind().String(), err)
	}
	return less
}

// OriginalPosition 返回某个索引的起始（原始）位置。
// 例如，如果 OriginalPosition(0) 返回 5，
// 则当前位于位置 0 的项在原始未排序数组中位于位置 5。
func (r *RuntimeSort) OriginalPosition(ix int) int {
	if ix < 0 || ix > len(r.origPosition) {
		return -1
	}
	return r.origPosition[ix]
}

type TableSorter struct {
	field      string
	obj        *metav1.Table
	parsedRows [][][]reflect.Value
}

func (t *TableSorter) Len() int {
	return len(t.obj.Rows)
}

func (t *TableSorter) Swap(i, j int) {
	t.obj.Rows[i], t.obj.Rows[j] = t.obj.Rows[j], t.obj.Rows[i]
	t.parsedRows[i], t.parsedRows[j] = t.parsedRows[j], t.parsedRows[i]
}

func (t *TableSorter) Less(i, j int) bool {
	iValues := t.parsedRows[i]
	jValues := t.parsedRows[j]

	if len(iValues) == 0 || len(iValues[0]) == 0 {
		return true
	}
	if len(jValues) == 0 || len(jValues[0]) == 0 {
		return false
	}

	iField := iValues[0][0]
	jField := jValues[0][0]

	less, err := isLess(iField, jField)
	if err != nil {
		klog.Exitf("Field %s in %T is an unsortable type: %s, err: %v", t.field, t.parsedRows, iField.Kind().String(), err)
	}
	return less
}

func (t *TableSorter) Sort() error {
	sort.Sort(t)
	return nil
}

func NewTableSorter(table *metav1.Table, field string) (*TableSorter, error) {
	var parsedRows [][][]reflect.Value

	parser := jsonpath.New("sorting").AllowMissingKeys(true)
	err := parser.Parse(field)
	if err != nil {
		return nil, fmt.Errorf("sorting error: %v", err)
	}

	fieldFoundOnce := false
	for i := range table.Rows {
		parsedRow, err := findJSONPathResults(parser, table.Rows[i].Object.Object)
		if err != nil {
			return nil, fmt.Errorf("Failed to get values for %#v using %s (%#v)", parsedRow, field, err)
		}
		parsedRows = append(parsedRows, parsedRow)
		if len(parsedRow) > 0 && len(parsedRow[0]) > 0 {
			fieldFoundOnce = true
		}
	}

	if len(table.Rows) > 0 && !fieldFoundOnce {
		return nil, fmt.Errorf("couldn't find any field with path %q in the list of objects", field)
	}

	return &TableSorter{
		obj:        table,
		field:      field,
		parsedRows: parsedRows,
	}, nil
}
func findJSONPathResults(parser *jsonpath.JSONPath, from runtime.Object) ([][]reflect.Value, error) {
	if unstructuredObj, ok := from.(*unstructured.Unstructured); ok {
		return parser.FindResults(unstructuredObj.Object)
	}
	return parser.FindResults(reflect.ValueOf(from).Elem().Interface())
}
