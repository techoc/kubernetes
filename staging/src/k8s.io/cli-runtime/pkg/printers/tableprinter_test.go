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
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// 下面两个全局样本供多个测试复用：带标签的 Pod 用于验证命名空间/标签列，
// Status 用于验证非表格对象的专用三列输出。
var testPodName = "test-pod-name"
var testPodNamespace = "test-namespace"

var testPod = &corev1.Pod{
	TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
	ObjectMeta: metav1.ObjectMeta{
		Name:      testPodName,
		Namespace: testPodNamespace,
		Labels: map[string]string{
			"first-label":  "12",
			"second-label": "label-value",
		},
	},
}

var testStatus = &metav1.Status{
	TypeMeta: metav1.TypeMeta{APIVersion: "metav1", Kind: "Status"},
	Status:   "Failure",
	Message:  "test-status-message",
	Reason:   "test-status-reason",
}

// TestPrintTable_MissingColumnsRows 确认没有列或没有行时输出为空——
// 空表连表头都不应出现，否则脚本会误以为存在资源。
func TestPrintTable_MissingColumnsRows(t *testing.T) {
	tests := []struct {
		columns  []metav1.TableColumnDefinition
		rows     []metav1.TableRow
		options  PrintOptions
		expected string
	}{
		// 无列无行时 Table 输出为空字符串。
		{
			columns:  []metav1.TableColumnDefinition{},
			rows:     []metav1.TableRow{},
			options:  PrintOptions{},
			expected: "",
		},
		// 没有行时输出为空（不打印表头）。
		{
			columns: []metav1.TableColumnDefinition{
				{Name: "Name", Type: "string"},
				{Name: "Ready", Type: "string"},
				{Name: "Status", Type: "string"},
				{Name: "Retries", Type: "integer"},
				{Name: "Age", Type: "string"},
			},
			rows:     []metav1.TableRow{},
			options:  PrintOptions{},
			expected: "",
		},
	}

	for _, test := range tests {
		// 用列定义和行数据构造 Table。
		table := &metav1.Table{
			ColumnDefinitions: test.columns,
			Rows:              test.rows,
		}
		// 输出 Table。
		out := bytes.NewBuffer([]byte{})
		printer := NewTablePrinter(test.options)
		printer.PrintObj(table, out)

		// 验证输出为空。
		if len(out.String()) > 0 {
			t.Errorf("Error Printing Table. Should be empty; got (%s)", out.String())
		}
	}
}

// TestPrintTable_ColumnPriority 验证 Priority > 0 的列在默认输出中隐藏，
// 仅在 -o wide（options.Wide）时展示；覆盖单行与多行两种对齐场景。
func TestPrintTable_ColumnPriority(t *testing.T) {
	tests := []struct {
		columns  []metav1.TableColumnDefinition
		rows     []metav1.TableRow
		options  PrintOptions
		expected string
	}{
		// 基本单行 Table；Priority > 0 的列不应输出。
		{
			columns: []metav1.TableColumnDefinition{
				{Name: "Name", Type: "string"},
				{Name: "Ready", Type: "string"},
				{Name: "Status", Type: "string"},
				{Name: "Retries", Type: "integer", Priority: 1}, // Priority > 0
				{Name: "Age", Type: "string", Priority: 1},      // Priority > 0
			},
			rows: []metav1.TableRow{
				{Cells: []interface{}{"test1", "1/1", "podPhase", int64(5), "20h"}},
			},
			options:  PrintOptions{},
			expected: "NAME    READY   STATUS\ntest1   1/1     podPhase\n",
		},
		// 基本多行 Table；Priority > 0 的列不应输出。
		{
			columns: []metav1.TableColumnDefinition{
				{Name: "Name", Type: "string"},
				{Name: "Ready", Type: "string"},
				{Name: "Status", Type: "string"},
				{Name: "Retries", Type: "integer", Priority: 1}, // Priority > 0
				{Name: "Age", Type: "string", Priority: 1},      // Priority > 0
			},
			rows: []metav1.TableRow{
				{Cells: []interface{}{"test1", "1/1", "podPhase", int64(5), "20h"}},
				{Cells: []interface{}{"test2", "1/2", "podPhase", int64(30), "21h"}},
				{Cells: []interface{}{"test3", "4/4", "podPhase", int64(1), "22h"}},
			},
			options: PrintOptions{},
			expected: `NAME    READY   STATUS
test1   1/1     podPhase
test2   1/2     podPhase
test3   4/4     podPhase
`,
		},
		// 单行 Table 开启 "wide"；Priority > 0 的列应输出。
		{
			columns: []metav1.TableColumnDefinition{
				{Name: "Name", Type: "string"},
				{Name: "Ready", Type: "string"},
				{Name: "Status", Type: "string"},
				{Name: "Retries", Type: "integer", Priority: 1},
				{Name: "Age", Type: "string", Priority: 1},
			},
			rows: []metav1.TableRow{
				{Cells: []interface{}{"test1", "1/1", "podPhase", int64(5), "20h"}},
			},
			// 不输出表头。
			options:  PrintOptions{Wide: true},
			expected: "NAME    READY   STATUS     RETRIES   AGE\ntest1   1/1     podPhase   5         20h\n",
		},
		// 多行 Table 开启 "wide"；Priority > 0 的列应输出。
		{
			columns: []metav1.TableColumnDefinition{
				{Name: "Name", Type: "string"},
				{Name: "Ready", Type: "string"},
				{Name: "Status", Type: "string"},
				{Name: "Retries", Type: "integer", Priority: 1},
				{Name: "Age", Type: "string", Priority: 1},
			},
			rows: []metav1.TableRow{
				{Cells: []interface{}{"test1", "1/1", "podPhase", int64(5), "20h"}},
				{Cells: []interface{}{"test2", "1/2", "podPhase", int64(30), "21h"}},
				{Cells: []interface{}{"test3", "4/4", "podPhase", int64(1), "22h"}},
			},
			options: PrintOptions{Wide: true},
			expected: `NAME    READY   STATUS     RETRIES   AGE
test1   1/1     podPhase   5         20h
test2   1/2     podPhase   30        21h
test3   4/4     podPhase   1         22h
`,
		},
	}
	for _, test := range tests {
		// 用列定义和行数据构造 Table。
		table := &metav1.Table{
			ColumnDefinitions: test.columns,
			Rows:              test.rows,
		}
		// 输出 Table。
		out := bytes.NewBuffer([]byte{})
		printer := NewTablePrinter(test.options)
		printer.PrintObj(table, out)
		// 验证实际输出与期望一致。
		if test.expected != out.String() {
			t.Errorf("Table printing error: expected (%s), got (%s)", test.expected, out.String())
		}
	}
}

// TestPrintTable_ColumnHeaders 验证默认输出表头，而 NoHeaders 时完全省略表头，
// 同时检查无表头模式下首行仍按最大列宽对齐。
func TestPrintTable_ColumnHeaders(t *testing.T) {
	tests := []struct {
		columns  []metav1.TableColumnDefinition
		rows     []metav1.TableRow
		options  PrintOptions
		expected string
	}{
		// 基本单行 Table，输出表头。
		{
			columns: []metav1.TableColumnDefinition{
				{Name: "Name", Type: "string"},
				{Name: "Ready", Type: "string"},
				{Name: "Status", Type: "string"},
				{Name: "Retries", Type: "integer"},
				{Name: "Age", Type: "string"},
			},
			rows: []metav1.TableRow{
				{Cells: []interface{}{"test1", "1/1", "podPhase", int64(5), "20h"}},
			},
			options:  PrintOptions{},
			expected: "NAME    READY   STATUS     RETRIES   AGE\ntest1   1/1     podPhase   5         20h\n",
		},
		// 基本多行 Table，输出表头。
		{
			columns: []metav1.TableColumnDefinition{
				{Name: "Name", Type: "string"},
				{Name: "Ready", Type: "string"},
				{Name: "Status", Type: "string"},
				{Name: "Retries", Type: "integer"},
				{Name: "Age", Type: "string"},
			},
			rows: []metav1.TableRow{
				{Cells: []interface{}{"test1", "1/1", "podPhase", int64(5), "20h"}},
				{Cells: []interface{}{"test2", "1/2", "podPhase", int64(30), "21h"}},
				{Cells: []interface{}{"test3", "4/4", "podPhase", int64(1), "22h"}},
			},
			options: PrintOptions{},
			expected: `NAME    READY   STATUS     RETRIES   AGE
test1   1/1     podPhase   5         20h
test2   1/2     podPhase   30        21h
test3   4/4     podPhase   1         22h
`,
		},
		// 单行 Table 开启 "NoHeaders"；不输出表头。
		{
			columns: []metav1.TableColumnDefinition{
				{Name: "Name", Type: "string"},
				{Name: "Ready", Type: "string"},
				{Name: "Status", Type: "string"},
				{Name: "Retries", Type: "integer"},
				{Name: "Age", Type: "string"},
			},
			rows: []metav1.TableRow{
				{Cells: []interface{}{"test1", "1/1", "podPhase", int64(5), "20h"}},
			},
			// 不输出表头。
			options:  PrintOptions{NoHeaders: true},
			expected: "test1   1/1   podPhase   5     20h\n",
		},
		// 多行 Table 开启 "NoHeaders"；不输出表头。
		{
			columns: []metav1.TableColumnDefinition{
				{Name: "Name", Type: "string"},
				{Name: "Ready", Type: "string"},
				{Name: "Status", Type: "string"},
				{Name: "Retries", Type: "integer"},
				{Name: "Age", Type: "string"},
			},
			rows: []metav1.TableRow{
				{Cells: []interface{}{"test1", "1/1", "podPhase", int64(5), "20h"}},
				{Cells: []interface{}{"test2", "1/2", "podPhase", int64(30), "21h"}},
				{Cells: []interface{}{"test3", "4/4", "podPhase", int64(1), "22h"}},
			},
			options: PrintOptions{NoHeaders: true},
			expected: `test1   1/1   podPhase   5     20h
test2   1/2   podPhase   30    21h
test3   4/4   podPhase   1     22h
`,
		},
	}
	for _, test := range tests {
		// 用列定义和行数据构造 Table。
		table := &metav1.Table{
			ColumnDefinitions: test.columns,
			Rows:              test.rows,
		}
		// 输出 Table。
		out := bytes.NewBuffer([]byte{})
		printer := NewTablePrinter(test.options)
		printer.PrintObj(table, out)
		// 验证实际输出与期望一致。
		if test.expected != out.String() {
			t.Errorf("Table printing error: expected (%s), got (%s)", test.expected, out.String())
		}
	}
}

// TestPrintTable_WithNamespace 验证 WithNamespace 会在最左侧插入 NAMESPACE 列，
// 其值从 row.Object 的 metadata 中读取（-A 场景）。
func TestPrintTable_WithNamespace(t *testing.T) {
	tests := []struct {
		columns  []metav1.TableColumnDefinition
		rows     []metav1.TableRow
		options  PrintOptions
		expected string
	}{
		// 单行 Table 开启 "WithNamespace"；前插 NAMESPACE 列。
		{
			columns: []metav1.TableColumnDefinition{
				{Name: "Name", Type: "string"},
				{Name: "Ready", Type: "string"},
				{Name: "Status", Type: "string"},
				{Name: "Retries", Type: "integer"},
				{Name: "Age", Type: "string"},
			},
			rows: []metav1.TableRow{
				{
					Cells:  []interface{}{"test1", "1/1", "podPhase", int64(5), "20h"},
					Object: runtime.RawExtension{Object: testPod},
				},
			},
			// 输出时前插 namespace 列。
			options: PrintOptions{WithNamespace: true},
			expected: `NAMESPACE        NAME    READY   STATUS     RETRIES   AGE
test-namespace   test1   1/1     podPhase   5         20h
`,
		},
	}
	for _, test := range tests {
		// 用列定义和行数据构造 Table。
		table := &metav1.Table{
			ColumnDefinitions: test.columns,
			Rows:              test.rows,
		}
		// 输出 Table。
		out := bytes.NewBuffer([]byte{})
		printer := NewTablePrinter(test.options)
		printer.PrintObj(table, out)
		// 验证实际输出与期望一致。
		if test.expected != out.String() {
			t.Errorf("Table printing error: expected (%s), got (%s)", test.expected, out.String())
		}
	}
}

// TestPrintTable_WithKind 验证 --show-kind 只在 format=name 的列前拼接小写 kind，
// 而不是新加一列。
func TestPrintTable_WithKind(t *testing.T) {
	tests := []struct {
		columns  []metav1.TableColumnDefinition
		rows     []metav1.TableRow
		options  PrintOptions
		expected string
	}{
		// 单行 Table 开启 "WithKind"；名称前加 "pod"。
		{
			columns: []metav1.TableColumnDefinition{
				{Name: "Name", Type: "string", Format: "name"},
				{Name: "Ready", Type: "string"},
				{Name: "Status", Type: "string"},
				{Name: "Retries", Type: "integer"},
				{Name: "Age", Type: "string"},
			},
			rows: []metav1.TableRow{
				{
					Cells: []interface{}{"test1", "1/1", "podPhase", int64(5), "20h"},
				},
			},
			// 输出时在名称前加 Kind "pod"。
			options: PrintOptions{WithKind: true, Kind: schema.GroupKind{Kind: "Pod"}},
			expected: `NAME        READY   STATUS     RETRIES   AGE
pod/test1   1/1     podPhase   5         20h
`,
		},
	}
	for _, test := range tests {
		// 用列定义和行数据构造 Table。
		table := &metav1.Table{
			ColumnDefinitions: test.columns,
			Rows:              test.rows,
		}
		// 输出 Table。
		out := bytes.NewBuffer([]byte{})
		printer := NewTablePrinter(test.options)
		printer.PrintObj(table, out)
		// 验证实际输出与期望一致。
		if test.expected != out.String() {
			t.Errorf("Table printing error: expected (%s), got (%s)", test.expected, out.String())
		}
	}
}

// TestPrintTable_WithLabels 覆盖 --show-labels 与 -L/--label-columns：
// 前者把全部标签拼成一列，后者按指定键各生成一列，并沿用标签键的末段作为表头。
func TestPrintTable_WithLabels(t *testing.T) {
	tests := []struct {
		columns  []metav1.TableColumnDefinition
		rows     []metav1.TableRow
		options  PrintOptions
		expected string
	}{
		// Table 开启 "ShowLabels"；把标签追加为列。
		{
			columns: []metav1.TableColumnDefinition{
				{Name: "Name", Type: "string"},
				{Name: "Ready", Type: "string"},
				{Name: "Status", Type: "string"},
				{Name: "Retries", Type: "integer"},
				{Name: "Age", Type: "string"},
			},
			rows: []metav1.TableRow{
				{
					Cells:  []interface{}{"test1", "1/1", "podPhase", int64(5), "20h"},
					Object: runtime.RawExtension{Object: testPod},
				},
			},
			options: PrintOptions{ShowLabels: true},
			expected: `NAME    READY   STATUS     RETRIES   AGE   LABELS
test1   1/1     podPhase   5         20h   first-label=12,second-label=label-value
`,
		},
		// Table 开启 "ColumnLabels"；把标签追加为列。
		{
			columns: []metav1.TableColumnDefinition{
				{Name: "Name", Type: "string"},
				{Name: "Ready", Type: "string"},
				{Name: "Status", Type: "string"},
				{Name: "Retries", Type: "integer"},
				{Name: "Age", Type: "string"},
			},
			rows: []metav1.TableRow{
				{
					Cells:  []interface{}{"test1", "1/1", "podPhase", int64(5), "20h"},
					Object: runtime.RawExtension{Object: testPod},
				},
			},
			// 以 "second-label" 为列名输出标签值。
			options: PrintOptions{ColumnLabels: []string{"second-label"}},
			expected: `NAME    READY   STATUS     RETRIES   AGE   SECOND-LABEL
test1   1/1     podPhase   5         20h   label-value
`,
		},
		// Table 开启 "ColumnLabels"；把标签追加为列。
		{
			columns: []metav1.TableColumnDefinition{
				{Name: "Name", Type: "string"},
				{Name: "Ready", Type: "string"},
				{Name: "Status", Type: "string"},
				{Name: "Retries", Type: "integer"},
				{Name: "Age", Type: "string"},
			},
			rows: []metav1.TableRow{
				{
					Cells:  []interface{}{"test1", "1/1", "podPhase", int64(5), "20h"},
					Object: runtime.RawExtension{Object: testPod},
				},
			},
			// 把多个标签及取值输出为列。
			options: PrintOptions{ColumnLabels: []string{"first-label", "second-label"}},
			expected: `NAME    READY   STATUS     RETRIES   AGE   FIRST-LABEL   SECOND-LABEL
test1   1/1     podPhase   5         20h   12            label-value
`,
		},
	}
	for _, test := range tests {
		// 用列定义和行数据构造 Table。
		table := &metav1.Table{
			ColumnDefinitions: test.columns,
			Rows:              test.rows,
		}
		// 输出 Table。
		out := bytes.NewBuffer([]byte{})
		printer := NewTablePrinter(test.options)
		printer.PrintObj(table, out)
		// 验证实际输出与期望一致。
		if test.expected != out.String() {
			t.Errorf("Table printing error: expected (%s), got (%s)", test.expected, out.String())
		}
	}
}

// TestPrintTable_NonTable 验证非 Table 对象走兜底路径：
// Pod 输出 NAME/AGE（Age 因 creationTimestamp 为零显示 <unknown>），
// Status 输出 STATUS/REASON/MESSAGE。
func TestPrintTable_NonTable(t *testing.T) {
	tests := []struct {
		object   runtime.Object
		options  PrintOptions
		expected string
	}{
		// 测试 Pod 的非 Table 默认输出。
		{
			object:   testPod,
			options:  PrintOptions{},
			expected: "NAME            AGE\ntest-pod-name   <unknown>\n",
		},
		// 测试 Pod 开启 "NoHeaders" 的非 Table 默认输出。
		{
			object:   testPod,
			options:  PrintOptions{NoHeaders: true},
			expected: "test-pod-name   <unknown>\n",
		},
		// 测试 Pod 开启 "WithNamespace" 的非 Table 默认输出。
		{
			object:   testPod,
			options:  PrintOptions{WithNamespace: true},
			expected: "NAMESPACE        NAME            AGE\ntest-namespace   test-pod-name   <unknown>\n",
		},
		// 测试 Pod 开启 "ShowLabels" 的非 Table 默认输出。
		{
			object:  testPod,
			options: PrintOptions{ShowLabels: true},
			expected: `NAME            AGE         LABELS
test-pod-name   <unknown>   first-label=12,second-label=label-value
`,
		},
		// 测试 Status 资源的非 Table 默认输出。
		{
			object:  testStatus,
			options: PrintOptions{},
			expected: `STATUS    REASON               MESSAGE
Failure   test-status-reason   test-status-message
`,
		},
		// 测试 Status 资源开启 NoHeaders 的非 Table 默认输出。
		{
			object:   testStatus,
			options:  PrintOptions{NoHeaders: true},
			expected: "Failure   test-status-reason   test-status-message\n",
		},
	}
	for _, test := range tests {
		// 输出 Table。
		out := bytes.NewBuffer([]byte{})
		printer := NewTablePrinter(test.options)
		printer.PrintObj(test.object, out)
		// 验证实际输出与期望一致。
		if test.expected != out.String() {
			t.Errorf("Table printing error: expected (%s), got (%s)", test.expected, out.String())
		}
	}
}

// TestPrintTable_WatchEvents 验证 WatchEvent 中的 Table 会被拆包输出，
// 并把事件类型作为 EVENT 首列；NoHeaders 时依然保留事件类型列。
func TestPrintTable_WatchEvents(t *testing.T) {
	tests := []struct {
		columns  []metav1.TableColumnDefinition
		rows     []metav1.TableRow
		options  PrintOptions
		expected string
	}{
		// 输出包含 Table 的 WatchEvent。
		{
			columns: []metav1.TableColumnDefinition{
				{Name: "Name", Type: "string"},
				{Name: "Ready", Type: "string"},
				{Name: "Status", Type: "string"},
				{Name: "Retries", Type: "integer"},
				{Name: "Age", Type: "string"},
			},
			rows: []metav1.TableRow{
				{
					Cells:  []interface{}{"test1", "1/1", "podPhase", int64(5), "20h"},
					Object: runtime.RawExtension{Object: testPod},
				},
			},
			options: PrintOptions{},
			expected: `EVENT   NAME    READY   STATUS     RETRIES   AGE
Added   test1   1/1     podPhase   5         20h
`,
		},
		// 输出包含 Table 的 WatchEvent，并开启 "NoHeaders"、"WithNamespace" 和 "ShowLabels"。
		{
			columns: []metav1.TableColumnDefinition{
				{Name: "Name", Type: "string"},
				{Name: "Ready", Type: "string"},
				{Name: "Status", Type: "string"},
				{Name: "Retries", Type: "integer"},
				{Name: "Age", Type: "string"},
			},
			rows: []metav1.TableRow{
				{
					Cells:  []interface{}{"test1", "1/1", "podPhase", int64(5), "20h"},
					Object: runtime.RawExtension{Object: testPod},
				},
			},
			options:  PrintOptions{NoHeaders: true, WithNamespace: true, ShowLabels: true},
			expected: "Added   test-namespace   test1   1/1   podPhase   5     20h   first-label=12,second-label=label-value\n",
		},
	}
	for _, test := range tests {
		// 用列定义和行数据构造 Table。
		table := &metav1.Table{
			ColumnDefinitions: test.columns,
			Rows:              test.rows,
		}
		// 把 Table 放入 WatchEvent。
		event := &metav1.WatchEvent{
			Type:   "Added",
			Object: runtime.RawExtension{Object: table},
		}
		// 输出事件。
		out := bytes.NewBuffer([]byte{})
		printer := NewTablePrinter(test.options)
		printer.PrintObj(event, out)
		// 验证实际输出与期望一致。
		if test.expected != out.String() {
			t.Errorf("Event/Table printing error: expected (%s), got (%s)", test.expected, out.String())
		}
	}
}

// TestPrintUnstructuredObject 用正则而非精确字符串断言：
// Unstructured 的 Age 依赖当前时间，只能匹配数字/单位；重点覆盖
// 单对象、WithNamespace、ShowLabels 以及 items 列表的展开。
func TestPrintUnstructuredObject(t *testing.T) {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Test",
			"dummy1":     "present",
			"dummy2":     "present",
			"metadata": map[string]interface{}{
				"name":              "MyName",
				"namespace":         "MyNamespace",
				"creationTimestamp": "2017-04-01T00:00:00Z",
				"resourceVersion":   123,
				"uid":               "00000000-0000-0000-0000-000000000001",
				"dummy3":            "present",
				"labels":            map[string]interface{}{"test": "other"},
			},
			/*"items": []interface{}{
				map[string]interface{}{
					"itemBool": true,
					"itemInt":  42,
				},
			},*/
			"url":    "http://localhost",
			"status": "ok",
		},
	}

	tests := []struct {
		expected string
		options  PrintOptions
		object   runtime.Object
	}{
		{
			expected: "NAME\\s+AGE\nMyName\\s+\\d+",
			object:   obj,
		},
		{
			options: PrintOptions{
				WithNamespace: true,
			},
			expected: "NAMESPACE\\s+NAME\\s+AGE\nMyNamespace\\s+MyName\\s+\\d+",
			object:   obj,
		},
		{
			options: PrintOptions{
				ShowLabels:    true,
				WithNamespace: true,
			},
			expected: "NAMESPACE\\s+NAME\\s+AGE\\s+LABELS\nMyNamespace\\s+MyName\\s+\\d+\\w+\\s+test\\=other",
			object:   obj,
		},
		{
			expected: "NAME\\s+AGE\nMyName\\s+\\d+\\w+\nMyName2\\s+\\d+",
			object: &unstructured.Unstructured{
				Object: map[string]interface{}{
					"apiVersion": "v1",
					"kind":       "Test",
					"dummy1":     "present",
					"dummy2":     "present",
					"items": []interface{}{
						map[string]interface{}{
							"metadata": map[string]interface{}{
								"name":              "MyName",
								"namespace":         "MyNamespace",
								"creationTimestamp": "2017-04-01T00:00:00Z",
								"resourceVersion":   123,
								"uid":               "00000000-0000-0000-0000-000000000001",
								"dummy3":            "present",
								"labels":            map[string]interface{}{"test": "other"},
							},
						},
						map[string]interface{}{
							"metadata": map[string]interface{}{
								"name":              "MyName2",
								"namespace":         "MyNamespace",
								"creationTimestamp": "2017-04-01T00:00:00Z",
								"resourceVersion":   123,
								"uid":               "00000000-0000-0000-0000-000000000001",
								"dummy3":            "present",
								"labels":            "badlabel",
							},
						},
					},
					"url":    "http://localhost",
					"status": "ok",
				},
			},
		},
	}
	out := bytes.NewBuffer([]byte{})

	for _, test := range tests {
		out.Reset()
		printer := NewTablePrinter(test.options)
		printer.PrintObj(test.object, out)

		matches, err := regexp.MatchString(test.expected, out.String())
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !matches {
			t.Errorf("wanted:\n%s\ngot:\n%s", test.expected, out)
		}
	}
}

// 验证即使最宽的单元格出现在首次 tabwriter 刷新之后，输出的所有行仍按相同列宽对齐。
// 这是针对一次回归的测试：printTable 每 100 行刷新一次，
// 而 RememberWidths 只能在后续刷新中增宽，无法回填已刷新的行，
// 导致早先的行比后面的行窄。
//
// 因此 printTable 必须先预扫描最大列宽；本测试把最宽的单元格放在最后，
// 从而确保一旦回归为“边写边算宽度”，跨 flush 边界的列就会错位。
func TestPrintTable_ConsistentAlignmentAcrossFlushes(t *testing.T) {
	// 构造超过两个刷新间隔的数据，跨过多个刷新边界；
	// 把最宽的 Name 单元格放在最后一组，使首次刷新锁定的宽度小于全局最大宽度。
	const rowCount = flushInterval*2 + flushInterval/2
	const shortName = "a"
	const longName = "a-name-that-is-significantly-wider"

	buildTable := func() *metav1.Table {
		columns := []metav1.TableColumnDefinition{
			{Name: "Name", Type: "string"},
			{Name: "Status", Type: "string"},
			{Name: "Age", Type: "string"},
		}
		rows := make([]metav1.TableRow, rowCount)
		for i := range rows {
			name := shortName
			if i == rowCount-1 {
				name = longName
			}
			rows[i] = metav1.TableRow{Cells: []interface{}{name, "Running", "5d"}}
		}
		return &metav1.Table{ColumnDefinitions: columns, Rows: rows}
	}

	assertAligned := func(t *testing.T, output string, expectedLines int) {
		t.Helper()
		lines := strings.Split(strings.TrimRight(output, "\n"), "\n")
		if len(lines) != expectedLines {
			t.Fatalf("expected %d lines, got %d", expectedLines, len(lines))
		}
		// 比较各行非首列的起始字节偏移。末列尾部内容忽略
		// （表头 "AGE" 本就比数据行的 "5d" 宽），但之前各列的边界必须逐行一致。
		wantStarts := columnStartOffsets(lines[0])
		for i, line := range lines {
			gotStarts := columnStartOffsets(line)
			if len(gotStarts) != len(wantStarts) {
				t.Errorf("line %d: found %d column boundaries, want %d\nline: %q\nref:  %q", i, len(gotStarts), len(wantStarts), line, lines[0])
				continue
			}
			for j := range gotStarts {
				if gotStarts[j] != wantStarts[j] {
					t.Errorf("line %d: column %d starts at offset %d, want %d (misaligned across flush boundary)\nline: %q\nref:  %q", i, j+2, gotStarts[j], wantStarts[j], line, lines[0])
					break
				}
			}
		}
	}

	t.Run("WithHeaders", func(t *testing.T) {
		out := &bytes.Buffer{}
		printer := NewTablePrinter(PrintOptions{})
		if err := printer.PrintObj(buildTable(), out); err != nil {
			t.Fatalf("PrintObj error: %v", err)
		}
		// 表头 + rowCount 行数据。
		assertAligned(t, out.String(), rowCount+1)
	})

	t.Run("NoHeaders", func(t *testing.T) {
		out := &bytes.Buffer{}
		printer := NewTablePrinter(PrintOptions{NoHeaders: true})
		if err := printer.PrintObj(buildTable(), out); err != nil {
			t.Fatalf("PrintObj error: %v", err)
		}
		assertAligned(t, out.String(), rowCount)
	})
}

// columnStartOffsets 返回 tabwriter 格式化行中每个非首列的起始字节偏移；
// 列起始于连续至少两个空格后的第一个非空格字节。这与 tabwriter 的列间补白规则一致，
// 也避开了末列内容长度天然可变的问题。
//
// 用“连续两个空格后首个非空格字符”作为列起始判定，比直接按 tab 分割更稳健，
// 因为 tabwriter 输出的是空格填充，不含制表符。
func columnStartOffsets(line string) []int {
	var offsets []int
	for i := 2; i < len(line); i++ {
		if line[i] != ' ' && line[i-1] == ' ' && line[i-2] == ' ' {
			offsets = append(offsets, i)
		}
	}
	return offsets
}

// 验证优化后的 printTable 在大表上仍输出正确；大表正是这次优化的目标。
//
// 500 行会跨越多个 flush 周期；这里只校验总行数和首尾行存在，
// 具体对齐由上一个测试负责。
func TestPrintTable_LargeRowCount(t *testing.T) {
	columns := []metav1.TableColumnDefinition{
		{Name: "Name", Type: "string"},
		{Name: "Status", Type: "string"},
		{Name: "Age", Type: "string"},
	}
	rowCount := 500
	rows := make([]metav1.TableRow, rowCount)
	for i := range rows {
		rows[i] = metav1.TableRow{Cells: []interface{}{
			fmt.Sprintf("pod-%d", i), "Running", "5d",
		}}
	}
	table := &metav1.Table{
		ColumnDefinitions: columns,
		Rows:              rows,
	}
	out := &bytes.Buffer{}
	printer := NewTablePrinter(PrintOptions{})
	if err := printer.PrintObj(table, out); err != nil {
		t.Fatalf("PrintObj error: %v", err)
	}
	lines := bytes.Count(out.Bytes(), []byte("\n"))
	if lines != rowCount+1 { // +1 for header
		t.Errorf("expected %d lines, got %d", rowCount+1, lines)
	}
	// 验证首尾数据行内容符合预期。
	got := out.String()
	if !strings.Contains(got, "pod-0") {
		t.Error("output missing first row")
	}
	if !strings.Contains(got, fmt.Sprintf("pod-%d", rowCount-1)) {
		t.Error("output missing last row")
	}
}

// TestUnknownType 是没有 metadata 且没有已知 kind 的对象，
// 用于验证 printer 遇到无法提取对象信息的输入时返回错误而非静默输出。
type TestUnknownType struct{}

func (obj *TestUnknownType) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }
func (obj *TestUnknownType) DeepCopyObject() runtime.Object {
	if obj == nil {
		return nil
	}
	clone := *obj
	return &clone
}

// TestUnknownTypePrinting 确保未知类型走兜底 printer 时得到明确错误，
// 因为 meta.Accessor 无法从它取到 name。
func TestUnknownTypePrinting(t *testing.T) {
	printer := NewTablePrinter(PrintOptions{})
	buffer := &bytes.Buffer{}
	err := printer.PrintObj(&TestUnknownType{}, buffer)
	if err == nil {
		t.Errorf("An error was expected from printing unknown type")
	}
}

// TestStringPrinting 验证单元格内容的终端安全处理：
// 多行文本在首个换行处截断并追加 "..."；长单行不截断；
// 带换行的长文本同样截断；ESC 控制字符转义为 [[^[] 形式，防止篡改终端显示。
func TestStringPrinting(t *testing.T) {
	tests := []struct {
		columns  []metav1.TableColumnDefinition
		rows     []metav1.TableRow
		expected string
	}{
		// 多行字符串。
		{
			columns: []metav1.TableColumnDefinition{
				{Name: "Name", Type: "string"},
				{Name: "Age", Type: "string"},
				{Name: "Description", Type: "string"},
			},
			rows: []metav1.TableRow{
				{Cells: []interface{}{"test1", "20h", "This is first line\nThis is second line\nThis is third line\nand another one\n"}},
			},
			expected: `NAME    AGE   DESCRIPTION
test1   20h   This is first line...
`,
		},
		// 长字符串。
		{
			columns: []metav1.TableColumnDefinition{
				{Name: "Name", Type: "string"},
				{Name: "Age", Type: "string"},
				{Name: "Description", Type: "string"},
			},
			rows: []metav1.TableRow{
				{Cells: []interface{}{"test1", "20h", "This is first line which is long and goes for on and on and on an on and on and on and on and on and on and on and on and on and on and on"}},
			},
			expected: `NAME    AGE   DESCRIPTION
test1   20h   This is first line which is long and goes for on and on and on an on and on and on and on and on and on and on and on and on and on and on
`,
		},
		// 长字符串。 + newline
		{
			columns: []metav1.TableColumnDefinition{
				{Name: "Name", Type: "string"},
				{Name: "Age", Type: "string"},
				{Name: "Description", Type: "string"},
			},
			rows: []metav1.TableRow{
				{Cells: []interface{}{"test1", "20h", "This is first\n line which is long and goes for on and on and on an on and on and on and on and on and on and on and on and on and on and on"}},
			},
			expected: `NAME    AGE   DESCRIPTION
test1   20h   This is first...
`,
		},
		// 终端特殊字符，应被转义。
		{
			columns: []metav1.TableColumnDefinition{
				{Name: "Name", Type: "string"},
			},
			rows: []metav1.TableRow{
				{Cells: []interface{}{"test1\x1b"}},
			},
			expected: `NAME
test1^[
`,
		},
	}

	for _, test := range tests {
		// 用列定义和行数据构造 Table。
		table := &metav1.Table{
			ColumnDefinitions: test.columns,
			Rows:              test.rows,
		}
		// 输出 Table。
		out := bytes.NewBuffer([]byte{})
		printer := NewTablePrinter(PrintOptions{})
		printer.PrintObj(table, out)

		if test.expected != out.String() {
			t.Errorf("Table printing error: expected \n(%s), got \n(%s)", test.expected, out.String())
		}
	}
}
