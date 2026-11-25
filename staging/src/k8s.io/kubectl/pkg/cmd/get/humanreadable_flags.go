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

package get

import (
	"github.com/spf13/cobra"
	"k8s.io/cli-runtime/pkg/genericclioptions"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/printers"
)

// HumanPrintFlags 提供打印所需的默认标志。
// 给定以下标志值后，可以请求一个能够
// 根据这些值处理打印的打印器。
type HumanPrintFlags struct {
	ShowKind     *bool
	ShowLabels   *bool
	SortBy       *string
	ColumnLabels *[]string

	// get.go 专用值
	NoHeaders bool

	Kind          schema.GroupKind
	WithNamespace bool
}

// SetKind 设置 Kind 选项
func (f *HumanPrintFlags) SetKind(kind schema.GroupKind) {
	f.Kind = kind
}

// EnsureWithKind 将 "Showkind" 可读选项设为 true。
func (f *HumanPrintFlags) EnsureWithKind() error {
	showKind := true
	f.ShowKind = &showKind
	return nil
}

// EnsureWithNamespace 将 "WithNamespace" 可读选项设为 true。
func (f *HumanPrintFlags) EnsureWithNamespace() error {
	f.WithNamespace = true
	return nil
}

// AllowedFormats 返回更多定制化的格式选项
func (f *HumanPrintFlags) AllowedFormats() []string {
	return []string{"wide"}
}

// ToPrinter 接收一个 outputFormat，并返回能够
// 处理人类可读输出的打印器。
func (f *HumanPrintFlags) ToPrinter(outputFormat string) (printers.ResourcePrinter, error) {
	if len(outputFormat) > 0 && outputFormat != "wide" {
		return nil, genericclioptions.NoCompatiblePrinterError{Options: f, AllowedFormats: f.AllowedFormats()}
	}

	showKind := false
	if f.ShowKind != nil {
		showKind = *f.ShowKind
	}

	showLabels := false
	if f.ShowLabels != nil {
		showLabels = *f.ShowLabels
	}

	columnLabels := []string{}
	if f.ColumnLabels != nil {
		columnLabels = *f.ColumnLabels
	}

	p := printers.NewTablePrinter(printers.PrintOptions{
		Kind:          f.Kind,
		WithKind:      showKind,
		NoHeaders:     f.NoHeaders,
		Wide:          outputFormat == "wide",
		WithNamespace: f.WithNamespace,
		ColumnLabels:  columnLabels,
		ShowLabels:    showLabels,
	})

	// TODO(juanvallejo): 在此处处理排序

	return p, nil
}

// AddFlags 接收一个 *cobra.Command 引用，并绑定
// 与人类可读打印相关的标志
func (f *HumanPrintFlags) AddFlags(c *cobra.Command) {
	if f.ShowLabels != nil {
		c.Flags().BoolVar(f.ShowLabels, "show-labels", *f.ShowLabels, "When printing, show all labels as the last column (default hide labels column)")
	}
	if f.SortBy != nil {
		c.Flags().StringVar(f.SortBy, "sort-by", *f.SortBy, "If non-empty, sort list types using this field specification.  The field specification is expressed as a JSONPath expression (e.g. '{.metadata.name}'). The field in the API resource specified by this JSONPath expression must be an integer or a string.")
	}
	if f.ColumnLabels != nil {
		c.Flags().StringSliceVarP(f.ColumnLabels, "label-columns", "L", *f.ColumnLabels, "Accepts a comma separated list of labels that are going to be presented as columns. Names are case-sensitive. You can also use multiple flag options like -L label1 -L label2...")
	}
	if f.ShowKind != nil {
		c.Flags().BoolVar(f.ShowKind, "show-kind", *f.ShowKind, "If present, list the resource type for the requested object(s).")
	}
}

// NewHumanPrintFlags 返回与人类可读打印相关的标志，
// 并已设置默认值。
func NewHumanPrintFlags() *HumanPrintFlags {
	showLabels := false
	sortBy := ""
	showKind := false
	columnLabels := []string{}

	return &HumanPrintFlags{
		NoHeaders:     false,
		WithNamespace: false,
		ColumnLabels:  &columnLabels,

		Kind:       schema.GroupKind{},
		ShowLabels: &showLabels,
		SortBy:     &sortBy,
		ShowKind:   &showKind,
	}
}
