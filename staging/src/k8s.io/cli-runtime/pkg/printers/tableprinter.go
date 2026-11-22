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
	"io"
	"reflect"
	"strings"
	"time"

	"github.com/liggitt/tabwriter"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/duration"
	"k8s.io/apimachinery/pkg/watch"
)

// cellBreakChars are characters that cause cell value truncation.
// Formfeed is included because tabwriter treats it as a newline.
//
// 单元格中出现这些字符就截断并追加 "..."，避免多行内容破坏表格对齐；
// 换页符 \f 也在此列，因为 tabwriter 会把它当作换行处理。
const cellBreakChars = "\f\n\r"

// flushInterval is the number of rows between tabwriter flushes when
// streaming a large table. Flushing bounds memory while preserving
// alignment via tabwriter.RememberWidths after the first flush.
//
// 每 100 行 flush 一次，既限制流式输出时的缓冲大小，又借助
// RememberWidths 保证后续 flush 的列宽一致。
const flushInterval = 100

var _ ResourcePrinter = &HumanReadablePrinter{}

// printHandler 把一个对象类型绑定到“列定义 + 打印函数”。
// printFunc 用反射调用，是因为历史 printer 注册表需要按类型动态分发。
type printHandler struct {
	columnDefinitions []metav1.TableColumnDefinition
	printFunc         reflect.Value
}

var (
	// 非 Table 类型对象的兜底路径：Status 有专用三列，其余走 ObjectMeta 两列。
	statusHandlerEntry = &printHandler{
		columnDefinitions: statusColumnDefinitions,
		printFunc:         reflect.ValueOf(printStatus),
	}

	statusColumnDefinitions = []metav1.TableColumnDefinition{
		{Name: "Status", Type: "string"},
		{Name: "Reason", Type: "string"},
		{Name: "Message", Type: "string"},
	}

	defaultHandlerEntry = &printHandler{
		columnDefinitions: objectMetaColumnDefinitions,
		printFunc:         reflect.ValueOf(printObjectMeta),
	}

	objectMetaColumnDefinitions = []metav1.TableColumnDefinition{
		{Name: "Name", Type: "string", Format: "name", Description: metav1.ObjectMeta{}.SwaggerDoc()["name"]},
		{Name: "Age", Type: "string", Description: metav1.ObjectMeta{}.SwaggerDoc()["creationTimestamp"]},
	}

	// watch 事件与命名空间是前置列，单独存放便于在表头阶段 prepend。
	withEventTypePrefixColumns = []string{"EVENT"}
	withNamespacePrefixColumns = []string{"NAMESPACE"} // TODO(erictune): print cluster name too.
)

// HumanReadablePrinter is an implementation of ResourcePrinter which attempts to provide
// more elegant output. It is not threadsafe, but you may call PrintObj repeatedly; headers
// will only be printed if the object type changes. This makes it useful for printing items
// received from watches.
//
// 状态（lastType/lastColumns/printedHeaders）跨调用保留，用于实现
// “同类型对象只打印一次表头”；因此该 printer 不是并发安全的，
// 但非常适合 watch 流式输出。
type HumanReadablePrinter struct {
	options        PrintOptions
	lastType       interface{}
	lastColumns    []metav1.TableColumnDefinition
	printedHeaders bool
}

// NewTablePrinter creates a printer suitable for calling PrintObj().
func NewTablePrinter(options PrintOptions) ResourcePrinter {
	printer := &HumanReadablePrinter{
		options: options,
	}
	return printer
}

// printHeader 用制表符拼接列名并在末尾补换行，交给 tabwriter 统一对齐。
func printHeader(columnNames []string, w io.Writer) error {
	if _, err := fmt.Fprintf(w, "%s\n", strings.Join(columnNames, "\t")); err != nil {
		return err
	}
	return nil
}

// PrintObj prints the obj in a human-friendly format according to the type of the obj.
func (h *HumanReadablePrinter) PrintObj(obj runtime.Object, output io.Writer) error {

	// 调用方可能已经传入 tabwriter（大表流式输出场景），此时不能重复包装，
	// 否则对齐语义与 flush 节奏都会被打乱。
	if _, found := output.(*tabwriter.Writer); !found {
		w := GetNewTabWriter(output)
		output = w
		defer w.Flush()
	}

	// WatchEvent 只携带事件类型和真实对象；先拆包，事件类型稍后作为首列插入。
	var eventType string
	if event, isEvent := obj.(*metav1.WatchEvent); isEvent {
		eventType = event.Type
		obj = event.Object.Object
	}

	// Parameter "obj" is a table from server; print it.
	// display tables following the rules of options
	if table, ok := obj.(*metav1.Table); ok {
		// Do not print headers if this table has no column definitions, or they are the same as the last ones we printed
		localOptions := h.options
		if h.printedHeaders && (len(table.ColumnDefinitions) == 0 || reflect.DeepEqual(table.ColumnDefinitions, h.lastColumns)) {
			localOptions.NoHeaders = true
		}

		if len(table.ColumnDefinitions) == 0 {
			// If this table has no column definitions, use the columns from the last table we printed for decoration and layout.
			// This is done when receiving tables in watch events to save bandwidth.
			table.ColumnDefinitions = h.lastColumns
		} else if !reflect.DeepEqual(table.ColumnDefinitions, h.lastColumns) {
			// If this table has column definitions, remember them for future use.
			// 列定义变化说明服务端切换了资源类型或版本，需要重置表头状态。
			h.lastColumns = table.ColumnDefinitions
			h.printedHeaders = false
		}

		if len(table.Rows) > 0 {
			h.printedHeaders = true
		}

		// 服务端返回的 Table 只含原始行；命名空间、自定义标签等列由客户端补充。
		if err := decorateTable(table, localOptions); err != nil {
			return err
		}
		if len(eventType) > 0 {
			if err := addColumns(beginning, table,
				[]metav1.TableColumnDefinition{{Name: "Event", Type: "string"}},
				[]cellValueFunc{func(metav1.TableRow) (interface{}, error) { return formatEventType(eventType), nil }},
			); err != nil {
				return err
			}
		}
		return printTable(table, output, localOptions)
	}

	// Could not find print handler for "obj"; use the default or status print handler.
	// Print with the default or status handler, and use the columns from the last time
	var handler *printHandler
	if _, isStatus := obj.(*metav1.Status); isStatus {
		handler = statusHandlerEntry
	} else {
		handler = defaultHandlerEntry
	}

	includeHeaders := h.lastType != handler && !h.options.NoHeaders

	// 类型切换时插入空行，避免多个资源的表格连在一起难以区分。
	if h.lastType != nil && h.lastType != handler && !h.options.NoHeaders {
		fmt.Fprintln(output)
	}

	if err := printRowsForHandlerEntry(output, handler, eventType, obj, h.options, includeHeaders); err != nil {
		return err
	}
	h.lastType = handler

	return nil
}

// printTable prints a table to the provided output respecting the filtering rules for options
// for wide columns and filtered rows. It filters out rows that are Completed. You should call
// decorateTable if you receive a table from a remote server before calling printTable.
//
// 这里同时负责三件事：按 -o wide 过滤 Priority 列、按 NoHeaders 控制表头、
// 以及为 tabwriter 预扫描列宽以保证流式 flush 后仍能对齐。
func printTable(table *metav1.Table, output io.Writer, options PrintOptions) error {
	numColDefs := len(table.ColumnDefinitions)

	// When writing to a tabwriter we flush periodically (see below) to bound
	// memory. Because tabwriter's RememberWidths can only grow column widths
	// on future flushes — not retroactively re-pad rows already written — we
	// pre-scan all rows to learn the ultimate max cell width per column. We
	// then pad either the header (with headers) or the first data row
	// (NoHeaders) so the first flush sets widths matching the ultimate max.
	// Without this, a wider cell appearing after row flushInterval causes
	// misalignment. Width comparisons use byte length because tabwriter
	// measures columns in bytes, not runes.
	var maxCellWidths []int
	tw, isTabwriter := output.(*tabwriter.Writer)
	if isTabwriter && len(table.Rows) > 0 {
		maxCellWidths = make([]int, numColDefs)
		for _, row := range table.Rows {
			for i, cell := range row.Cells {
				// 服务端可能返回比列定义更多的单元格，超出部分忽略（见下方 issue）。
				if i >= numColDefs {
					break
				}
				// 非 wide 模式不显示 Priority != 0 的列，宽度也无需计算。
				if !options.Wide && table.ColumnDefinitions[i].Priority != 0 {
					continue
				}
				if cell == nil {
					continue
				}
				var l int
				if s, ok := cell.(string); ok {
					l = len(s)
				} else {
					l = len(fmt.Sprint(cell))
				}
				if l > maxCellWidths[i] {
					maxCellWidths[i] = l
				}
			}
		}
	}

	// Find the last visible column so we can skip padding it — padding the
	// final column would leave trailing whitespace without any alignment
	// benefit (tabwriter doesn't right-pad the last column for data rows).
	lastVisibleCol := -1
	for i, column := range table.ColumnDefinitions {
		if !options.Wide && column.Priority != 0 {
			continue
		}
		lastVisibleCol = i
	}

	if !options.NoHeaders {
		// avoid printing headers if we have no rows to display
		// 空表连表头都不输出，避免脚本把只有表头的输出误判为有资源。
		if len(table.Rows) == 0 {
			return nil
		}

		first := true
		for i, column := range table.ColumnDefinitions {
			if !options.Wide && column.Priority != 0 {
				continue
			}
			if first {
				first = false
			} else {
				output.Write([]byte{'\t'}) //nolint:errcheck
			}
			// 表头统一大写，是 kubectl 表格输出的惯例。
			name := strings.ToUpper(column.Name)
			io.WriteString(output, name) //nolint:errcheck
			// 用预扫描的最大宽度补齐表头，使第一次 flush 就锁定列宽。
			if i != lastVisibleCol && i < len(maxCellWidths) {
				if pad := maxCellWidths[i] - len(name); pad > 0 {
					output.Write(bytes.Repeat([]byte{' '}, pad)) //nolint:errcheck
				}
			}
		}
		output.Write([]byte{'\n'}) //nolint:errcheck
	}

	// rowBuf accumulates tab-separated cell values for each row,
	// reducing per-cell Write calls from ~3 (tab + value + truncation)
	// down to 1 per row in the common case (no special characters).
	// 40 bytes per column is a heuristic based on typical kubectl output:
	// cell values range from ~5 bytes ("Ready") to ~45 bytes (FQDNs).
	rowBuf := make([]byte, 0, numColDefs*40)

	// When writing to a tabwriter with RememberWidths, flush periodically
	// (flushInterval) to bound memory usage. After the first flush the
	// tabwriter remembers column widths, so subsequent flushes produce
	// consistently aligned output. For small tables or non-tabwriter
	// outputs, no flushing happens here.
	//
	// NoHeaders mode: the header path above primes column widths by
	// padding header cells to maxCellWidths. Without a header to prime
	// widths, we instead pad the first data row's non-final cells using
	// the same pre-scanned widths.
	padFirstRow := isTabwriter && options.NoHeaders && len(maxCellWidths) > 0

	for ri, row := range table.Rows {
		rowBuf = rowBuf[:0]
		first := true
		for i, cell := range row.Cells {
			if i >= numColDefs {
				// https://issue.k8s.io/66379
				// don't panic in case of bad output from the server, with more cells than column definitions
				break
			}
			if !options.Wide && table.ColumnDefinitions[i].Priority != 0 {
				continue
			}
			if first {
				first = false
			} else {
				rowBuf = append(rowBuf, '\t')
			}
			// Track byte length of the cell string we're about to
			// write so we can pad to maxCellWidths without measuring
			// through rowBuf (appendCellValue's slow path flushes
			// rowBuf directly, making post-write measurement unsafe).
			cellLen := 0
			if cell != nil {
				var cellStr string
				switch val := cell.(type) {
				case string:
					cellStr = val
				default:
					cellStr = fmt.Sprint(val)
				}
				cellLen = len(cellStr)
				rowBuf = appendCellValue(output, rowBuf, cellStr)
			}
			if padFirstRow && ri == 0 && i != lastVisibleCol && i < len(maxCellWidths) {
				if pad := maxCellWidths[i] - cellLen; pad > 0 {
					rowBuf = append(rowBuf, bytes.Repeat([]byte{' '}, pad)...)
				}
			}
		}
		rowBuf = append(rowBuf, '\n')
		output.Write(rowBuf) //nolint:errcheck

		// 周期性 flush：写错误交给 tabwriter 自身处理，这里刻意忽略返回值。
		if isTabwriter && (ri+1)%flushInterval == 0 {
			tw.Flush() //nolint:errcheck
		}
	}
	return nil
}

// appendCellValue appends a cell's display text to rowBuf. For the common
// case (no special characters), this is a simple append with zero allocations.
// When the value contains control characters or escape sequences, it flushes
// the buffer, writes the sanitized value directly, and returns an empty buffer.
func appendCellValue(w io.Writer, buf []byte, val string) []byte {
	// Fast path: scan for characters that need special handling.
	// cellBreakChars cause truncation; \x1b needs escaping.
	// The vast majority of cell values contain neither.
	idx := strings.IndexAny(val, cellSpecialChars)
	if idx < 0 {
		return append(buf, val...)
	}

	// Slow path: flush accumulated buffer, then handle the special value.
	// 之所以先 flush 再直接写，是为了让转义/截断只作用于当前单元格，
	// 同时保持之前的行内内容顺序不变。
	if len(buf) > 0 {
		w.Write(buf) //nolint:errcheck
		buf = buf[:0]
	}

	// Truncate at the first break character (newline, carriage return,
	// or formfeed — which tabwriter treats as a newline).
	breakchar := strings.IndexAny(val, cellBreakChars)
	if breakchar >= 0 {
		WriteEscaped(w, val[:breakchar]) //nolint:errcheck
		io.WriteString(w, "...")         //nolint:errcheck
	} else {
		// No break chars, but contains terminal-unsafe chars that need escaping.
		WriteEscaped(w, val) //nolint:errcheck
	}
	return buf
}

// cellValueFunc 根据表格行计算新增列的值；供 addColumns 注入 EVENT 等列。
type cellValueFunc func(metav1.TableRow) (interface{}, error)

// columnAddPosition 指定新列插入到表头/行的开头还是末尾。
type columnAddPosition int

const (
	beginning columnAddPosition = 1
	end       columnAddPosition = 2
)

// addColumns 在表格开头或末尾插入若干列，并为每个数据行计算新单元格。
// 先完整计算所有行再统一替换，保证任一取值函数出错时不会留下半成品表格。
func addColumns(pos columnAddPosition, table *metav1.Table, columns []metav1.TableColumnDefinition, valueFuncs []cellValueFunc) error {
	if len(columns) != len(valueFuncs) {
		return fmt.Errorf("cannot prepend columns, unmatched value functions")
	}
	if len(columns) == 0 {
		return nil
	}

	// Compute the new rows
	newRows := make([][]interface{}, len(table.Rows))
	for i := range table.Rows {
		newCells := make([]interface{}, 0, len(columns)+len(table.Rows[i].Cells))

		if pos == end {
			// If we're appending, start with the existing cells,
			// then add nil cells to match the number of columns
			// 先用 nil 补齐列定义数量，避免服务端返回的短行在追加后错位。
			newCells = append(newCells, table.Rows[i].Cells...)
			for len(newCells) < len(table.ColumnDefinitions) {
				newCells = append(newCells, nil)
			}
		}

		// Compute cells for new columns
		for _, f := range valueFuncs {
			newCell, err := f(table.Rows[i])
			if err != nil {
				return err
			}
			newCells = append(newCells, newCell)
		}

		if pos == beginning {
			// If we're prepending, add existing cells
			newCells = append(newCells, table.Rows[i].Cells...)
		}

		// Remember the new cells for this row
		newRows[i] = newCells
	}

	// All cells successfully computed, now replace columns and rows
	newColumns := make([]metav1.TableColumnDefinition, 0, len(columns)+len(table.ColumnDefinitions))
	switch pos {
	case beginning:
		newColumns = append(newColumns, columns...)
		newColumns = append(newColumns, table.ColumnDefinitions...)
	case end:
		newColumns = append(newColumns, table.ColumnDefinitions...)
		newColumns = append(newColumns, columns...)
	default:
		return fmt.Errorf("invalid column add position: %v", pos)
	}
	table.ColumnDefinitions = newColumns
	for i := range table.Rows {
		table.Rows[i].Cells = newRows[i]
	}

	return nil
}

// decorateTable takes a table and attempts to add label columns and the
// namespace column. It will fill empty columns with nil (if the object
// does not expose metadata). It returns an error if the table cannot
// be decorated.
//
// 注意 rows 是表头行拷贝，修改 row.Cells 后必须回写 rows[i]，
// 否则对切片的追加只会体现在副本上。
func decorateTable(table *metav1.Table, options PrintOptions) error {
	width := len(table.ColumnDefinitions) + len(options.ColumnLabels)
	if options.WithNamespace {
		width++
	}
	if options.ShowLabels {
		width++
	}

	columns := table.ColumnDefinitions

	// --show-kind 时把 kind 前缀拼到 format=name 的列（通常是 NAME）上。
	nameColumn := -1
	if options.WithKind && !options.Kind.Empty() {
		for i := range columns {
			if columns[i].Format == "name" && columns[i].Type == "string" {
				nameColumn = i
				break
			}
		}
	}

	if width != len(table.ColumnDefinitions) {
		// 只有需要额外列时才重建列定义，否则直接复用服务端返回的切片。
		columns = make([]metav1.TableColumnDefinition, 0, width)
		if options.WithNamespace {
			columns = append(columns, metav1.TableColumnDefinition{
				Name: "Namespace",
				Type: "string",
			})
		}
		columns = append(columns, table.ColumnDefinitions...)
		for _, label := range formatLabelHeaders(options.ColumnLabels) {
			columns = append(columns, metav1.TableColumnDefinition{
				Name: label,
				Type: "string",
			})
		}
		if options.ShowLabels {
			columns = append(columns, metav1.TableColumnDefinition{
				Name: "Labels",
				Type: "string",
			})
		}
	}

	rows := table.Rows

	includeLabels := len(options.ColumnLabels) > 0 || options.ShowLabels
	if includeLabels || options.WithNamespace || nameColumn != -1 {
		for i := range rows {
			row := rows[i]

			if nameColumn != -1 {
				row.Cells[nameColumn] = fmt.Sprintf("%s/%s", strings.ToLower(options.Kind.String()), row.Cells[nameColumn])
			}

			var m metav1.Object
			if obj := row.Object.Object; obj != nil {
				if acc, err := meta.Accessor(obj); err == nil {
					m = acc
				}
			}
			// if we can't get an accessor, fill out the appropriate columns with empty spaces
			// 取不到 metadata 时仍要补齐单元格，否则表格列数不齐会破坏 tabwriter 对齐。
			if m == nil {
				if options.WithNamespace {
					r := make([]interface{}, 1, width)
					row.Cells = append(r, row.Cells...)
				}
				for j := 0; j < width-len(row.Cells); j++ {
					row.Cells = append(row.Cells, nil)
				}
				rows[i] = row
				continue
			}

			if options.WithNamespace {
				r := make([]interface{}, 1, width)
				r[0] = m.GetNamespace()
				row.Cells = append(r, row.Cells...)
			}
			if includeLabels {
				row.Cells = appendLabelCells(row.Cells, m.GetLabels(), options)
			}
			rows[i] = row
		}
	}

	table.ColumnDefinitions = columns
	table.Rows = rows
	return nil
}

// printRowsForHandlerEntry prints the incremental table output (headers if the current type is
// different from lastType) including all the rows in the object. It returns the current type
// or an error, if any.
func printRowsForHandlerEntry(output io.Writer, handler *printHandler, eventType string, obj runtime.Object, options PrintOptions, includeHeaders bool) error {
	var results []reflect.Value

	// printFunc 的签名固定为 func(runtime.Object, PrintOptions) ([]metav1.TableRow, error)。
	args := []reflect.Value{reflect.ValueOf(obj), reflect.ValueOf(options)}
	results = handler.printFunc.Call(args)
	if !results[1].IsNil() {
		return results[1].Interface().(error)
	}

	if includeHeaders {
		var headers []string
		for _, column := range handler.columnDefinitions {
			if column.Priority != 0 && !options.Wide {
				continue
			}
			headers = append(headers, strings.ToUpper(column.Name))
		}
		headers = append(headers, formatLabelHeaders(options.ColumnLabels)...)
		// LABELS is always the last column.
		headers = append(headers, formatShowLabelsHeader(options.ShowLabels)...)
		// prepend namespace header
		if options.WithNamespace {
			headers = append(withNamespacePrefixColumns, headers...)
		}
		// prepend event type header
		if len(eventType) > 0 {
			headers = append(withEventTypePrefixColumns, headers...)
		}
		printHeader(headers, output)
	}

	if results[1].IsNil() {
		rows := results[0].Interface().([]metav1.TableRow)
		printRows(output, eventType, rows, options)
		return nil
	}
	return results[1].Interface().(error)
}

// formattedEventType 把 watch 事件类型映射为固定宽度的展示文本，
// 补空格是为了在没有列对齐的情况下让后续列起始位置一致。
var formattedEventType = map[string]string{
	string(watch.Added):    "ADDED   ",
	string(watch.Modified): "MODIFIED",
	string(watch.Deleted):  "DELETED ",
	string(watch.Error):    "ERROR   ",
}

// formatEventType 未知事件类型原样返回，避免丢失服务端可能新增的取值。
func formatEventType(eventType string) string {
	if formatted, ok := formattedEventType[eventType]; ok {
		return formatted
	}
	return eventType
}

// printRows writes the provided rows to output.
func printRows(output io.Writer, eventType string, rows []metav1.TableRow, options PrintOptions) {
	for _, row := range rows {
		if len(eventType) > 0 {
			fmt.Fprint(output, formatEventType(eventType))
			fmt.Fprint(output, "\t")
		}
		if options.WithNamespace {
			// 命名空间从 row.Object 中取；取不到时留空列而不是报错。
			if obj := row.Object.Object; obj != nil {
				if m, err := meta.Accessor(obj); err == nil {
					fmt.Fprint(output, m.GetNamespace())
				}
			}
			fmt.Fprint(output, "\t")
		}

		for i, cell := range row.Cells {
			if i != 0 {
				fmt.Fprint(output, "\t")
			} else {
				// TODO: remove this once we drop the legacy printers
				if options.WithKind && !options.Kind.Empty() {
					fmt.Fprintf(output, "%s/%s", strings.ToLower(options.Kind.String()), cell)
					continue
				}
			}
			fmt.Fprint(output, cell)
		}

		hasLabels := len(options.ColumnLabels) > 0
		if obj := row.Object.Object; obj != nil && (hasLabels || options.ShowLabels) {
			if m, err := meta.Accessor(obj); err == nil {
				for _, value := range labelValues(m.GetLabels(), options) {
					output.Write([]byte("\t"))
					output.Write([]byte(value))
				}
			}
		}

		output.Write([]byte("\n"))
	}
}

// formatLabelHeaders 把 "app.kubernetes.io/name" 这类标签键转换为
// 表头只保留最后一段（NAME），与 kubectl -L 的展示习惯一致。
func formatLabelHeaders(columnLabels []string) []string {
	formHead := make([]string, len(columnLabels))
	for i, l := range columnLabels {
		p := strings.Split(l, "/")
		formHead[i] = strings.ToUpper(p[len(p)-1])
	}
	return formHead
}

// headers for --show-labels=true
func formatShowLabelsHeader(showLabels bool) []string {
	if showLabels {
		return []string{"LABELS"}
	}
	return nil
}

// labelValues returns a slice of value columns matching the requested print options.
func labelValues(itemLabels map[string]string, opts PrintOptions) []string {
	var values []string
	for _, key := range opts.ColumnLabels {
		values = append(values, itemLabels[key])
	}
	if opts.ShowLabels {
		values = append(values, labels.FormatLabels(itemLabels))
	}
	return values
}

// appendLabelCells returns a slice of value columns matching the requested print options.
// Intended for use with tables.
func appendLabelCells(values []interface{}, itemLabels map[string]string, opts PrintOptions) []interface{} {
	for _, key := range opts.ColumnLabels {
		values = append(values, itemLabels[key])
	}
	if opts.ShowLabels {
		values = append(values, labels.FormatLabels(itemLabels))
	}
	return values
}

// printStatus 为 *metav1.Status 生成一行三列（Status/Reason/Message）。
func printStatus(obj runtime.Object, options PrintOptions) ([]metav1.TableRow, error) {
	status, ok := obj.(*metav1.Status)
	if !ok {
		return nil, fmt.Errorf("expected *v1.Status, got %T", obj)
	}
	return []metav1.TableRow{{
		Object: runtime.RawExtension{Object: obj},
		Cells:  []interface{}{status.Status, status.Reason, status.Message},
	}}, nil
}

// printObjectMeta 是通用兜底：对 List 递归展开，对单对象只输出 Name/Age。
func printObjectMeta(obj runtime.Object, options PrintOptions) ([]metav1.TableRow, error) {
	if meta.IsListType(obj) {
		rows := make([]metav1.TableRow, 0, 16)
		err := meta.EachListItem(obj, func(obj runtime.Object) error {
			nestedRows, err := printObjectMeta(obj, options)
			if err != nil {
				return err
			}
			rows = append(rows, nestedRows...)
			return nil
		})
		if err != nil {
			return nil, err
		}
		return rows, nil
	}

	rows := make([]metav1.TableRow, 0, 1)
	m, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	row := metav1.TableRow{
		Object: runtime.RawExtension{Object: obj},
	}
	row.Cells = append(row.Cells, m.GetName(), translateTimestampSince(m.GetCreationTimestamp()))
	rows = append(rows, row)
	return rows, nil
}

// translateTimestampSince returns the elapsed time since timestamp in
// human-readable approximation.
//
// 零值时间无法计算时长，显示 <unknown> 而不是一个极易误解的巨大年龄。
func translateTimestampSince(timestamp metav1.Time) string {
	if timestamp.IsZero() {
		return "<unknown>"
	}

	return duration.HumanDuration(time.Since(timestamp.Time))
}
