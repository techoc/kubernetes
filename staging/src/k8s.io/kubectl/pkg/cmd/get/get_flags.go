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
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/kubectl/pkg/cmd/util"
)

// PrintFlags 组合了 Get 命令中使用的
// 通用打印器标志结构体。
type PrintFlags struct {
	JSONYamlPrintFlags *genericclioptions.JSONYamlPrintFlags
	NamePrintFlags     *genericclioptions.NamePrintFlags
	CustomColumnsFlags *CustomColumnsPrintFlags
	HumanReadableFlags *HumanPrintFlags
	TemplateFlags      *genericclioptions.KubeTemplatePrintFlags

	NoHeaders    *bool
	OutputFormat *string
}

// SetKind 设置可读标志中的 Kind 选项
func (f *PrintFlags) SetKind(kind schema.GroupKind) {
	f.HumanReadableFlags.SetKind(kind)
}

// EnsureWithNamespace 确保可读标志返回
// 能够打印带 "namespace" 列的打印器。
func (f *PrintFlags) EnsureWithNamespace() error {
	return f.HumanReadableFlags.EnsureWithNamespace()
}

// EnsureWithKind 确保可读标志返回
// 能够包含资源 Kind 的打印器。
func (f *PrintFlags) EnsureWithKind() error {
	return f.HumanReadableFlags.EnsureWithKind()
}

// Copy 返回一份可用于修改的 PrintFlags 副本
func (f *PrintFlags) Copy() PrintFlags {
	printFlags := *f
	return printFlags
}

// AllowedFormats 是数据可显示格式的列表
func (f *PrintFlags) AllowedFormats() []string {
	formats := f.JSONYamlPrintFlags.AllowedFormats()
	formats = append(formats, f.NamePrintFlags.AllowedFormats()...)
	formats = append(formats, f.TemplateFlags.AllowedFormats()...)
	formats = append(formats, f.CustomColumnsFlags.AllowedFormats()...)
	formats = append(formats, f.HumanReadableFlags.AllowedFormats()...)
	return formats
}

// ToPrinter 尝试找到一组组合的 PrintFlags，以便
// 根据当前标志值返回合适的打印器。
func (f *PrintFlags) ToPrinter() (printers.ResourcePrinter, error) {
	outputFormat := ""
	if f.OutputFormat != nil {
		outputFormat = *f.OutputFormat
	}

	noHeaders := false
	if f.NoHeaders != nil {
		noHeaders = *f.NoHeaders
	}
	f.HumanReadableFlags.NoHeaders = noHeaders
	f.CustomColumnsFlags.NoHeaders = noHeaders

	// 对 "get.go" 而言，即使没有提供 --output 格式，我们也希望支持传入 --template 参数
	if f.TemplateFlags.TemplateArgument != nil && len(*f.TemplateFlags.TemplateArgument) > 0 && len(outputFormat) == 0 {
		outputFormat = "go-template"
	}

	if p, err := f.TemplateFlags.ToPrinter(outputFormat); !genericclioptions.IsNoCompatiblePrinterError(err) {
		return p, err
	}

	if f.TemplateFlags.TemplateArgument != nil {
		f.CustomColumnsFlags.TemplateArgument = *f.TemplateFlags.TemplateArgument
	}

	if p, err := f.JSONYamlPrintFlags.ToPrinter(outputFormat); !genericclioptions.IsNoCompatiblePrinterError(err) {
		return p, err
	}

	if p, err := f.HumanReadableFlags.ToPrinter(outputFormat); !genericclioptions.IsNoCompatiblePrinterError(err) {
		return p, err
	}

	if p, err := f.CustomColumnsFlags.ToPrinter(outputFormat); !genericclioptions.IsNoCompatiblePrinterError(err) {
		return p, err
	}

	if p, err := f.NamePrintFlags.ToPrinter(outputFormat); !genericclioptions.IsNoCompatiblePrinterError(err) {
		return p, err
	}

	return nil, genericclioptions.NoCompatiblePrinterError{OutputFormat: &outputFormat, AllowedFormats: f.AllowedFormats()}
}

// AddFlags 接收一个 *cobra.Command 引用，并绑定
// 与可读格式和模板打印相关的标志。
func (f *PrintFlags) AddFlags(cmd *cobra.Command) {
	f.JSONYamlPrintFlags.AddFlags(cmd)
	f.NamePrintFlags.AddFlags(cmd)
	f.TemplateFlags.AddFlags(cmd)
	f.HumanReadableFlags.AddFlags(cmd)
	f.CustomColumnsFlags.AddFlags(cmd)

	if f.OutputFormat != nil {
		cmd.Flags().StringVarP(f.OutputFormat, "output", "o", *f.OutputFormat, fmt.Sprintf(`Output format. One of: (%s). See custom columns [https://kubernetes.io/docs/reference/kubectl/#custom-columns], golang template [http://golang.org/pkg/text/template/#pkg-overview] and jsonpath template [https://kubernetes.io/docs/reference/kubectl/jsonpath/].`, strings.Join(f.AllowedFormats(), ", ")))
		util.CheckErr(cmd.RegisterFlagCompletionFunc(
			"output",
			func(cmd *cobra.Command, args []string, toComplete string) ([]string, cobra.ShellCompDirective) {
				var comps []string
				for _, format := range f.AllowedFormats() {
					if strings.HasPrefix(format, toComplete) {
						comps = append(comps, format)
					}
				}
				return comps, cobra.ShellCompDirectiveNoFileComp
			},
		))
	}
	if f.NoHeaders != nil {
		cmd.Flags().BoolVar(f.NoHeaders, "no-headers", *f.NoHeaders, "When using the default or custom-column output format, don't print headers (default print headers).")
	}
}

// NewGetPrintFlags 返回与可读格式、模板及 "name" 打印相关的标志，
// 并已设置默认值。
func NewGetPrintFlags() *PrintFlags {
	outputFormat := ""
	noHeaders := false

	return &PrintFlags{
		OutputFormat: &outputFormat,
		NoHeaders:    &noHeaders,

		JSONYamlPrintFlags: genericclioptions.NewJSONYamlPrintFlags(),
		NamePrintFlags:     genericclioptions.NewNamePrintFlags(""),
		TemplateFlags:      genericclioptions.NewKubeTemplatePrintFlags(),

		HumanReadableFlags: NewHumanPrintFlags(),
		CustomColumnsFlags: NewCustomColumnsPrintFlags(),
	}
}
