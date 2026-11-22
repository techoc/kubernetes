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

package genericclioptions

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/printers"
)

// NoCompatiblePrinterError is a struct that contains error information.
// It will be constructed when a invalid printing format is provided
//
// NoCompatiblePrinterError 表达“这个 printer 不认得该输出格式”，而不是真正的失败；
// 调用方据此继续尝试其他 printer，全部尝试失败后才会对外报错。
type NoCompatiblePrinterError struct {
	// OutputFormat 是用户请求的格式；nil 表示未提供。
	OutputFormat *string
	// AllowedFormats 用于拼出“合法取值有哪些”的提示。
	AllowedFormats []string
	// Options 保留原始 flag 结构，便于上层调试或扩展错误信息。
	Options interface{}
}

// Error 实现 error 接口；排序后拼接合法格式，保证同一输入下提示稳定。
func (e NoCompatiblePrinterError) Error() string {
	output := ""
	if e.OutputFormat != nil {
		output = *e.OutputFormat
	}

	sort.Strings(e.AllowedFormats)
	return fmt.Sprintf("unable to match a printer suitable for the output format %q, allowed formats are: %s", output, strings.Join(e.AllowedFormats, ","))
}

// IsNoCompatiblePrinterError returns true if it is a not a compatible printer
// otherwise it will return false
//
// 用类型断言而非 errors.As：该错误只在本包内产生，且通常不走 errors.Wrap。
func IsNoCompatiblePrinterError(err error) bool {
	if err == nil {
		return false
	}

	_, ok := err.(NoCompatiblePrinterError)
	return ok
}

// PrintFlags composes common printer flag structs
// used across all commands, and provides a method
// of retrieving a known printer based on flag values provided.
//
// PrintFlags 把多种输出格式的 flag 聚合在一起，按优先级依次尝试构造 printer。
type PrintFlags struct {
	JSONYamlPrintFlags   *JSONYamlPrintFlags
	NamePrintFlags       *NamePrintFlags
	TemplatePrinterFlags *KubeTemplatePrintFlags

	// TypeSetterPrinter 可选；设置后会为缺少类型信息的对象补齐 GVK 再打印。
	TypeSetterPrinter *printers.TypeSetterPrinter

	// OutputFormat 对应 --output 的值；指针便于调用方选择是否绑定该 flag。
	OutputFormat *string

	// OutputFlagSpecified indicates whether the user specifically requested a certain kind of output.
	// Using this function allows a sophisticated caller to change the flag binding logic if they so desire.
	OutputFlagSpecified func() bool
}

// Complete sets NamePrintFlags operation flag from successTemplate
//
// 仅影响 name 输出时的操作动词（如 created/configured）。
func (f *PrintFlags) Complete(successTemplate string) error {
	return f.NamePrintFlags.Complete(successTemplate)
}

// AllowedFormats returns slice of string of allowed JSONYaml/Name/Template printing format
//
// 汇总三类 printer 的格式并保持各自顺序，供帮助文本和错误信息使用。
func (f *PrintFlags) AllowedFormats() []string {
	ret := []string{}
	ret = append(ret, f.JSONYamlPrintFlags.AllowedFormats()...)
	ret = append(ret, f.NamePrintFlags.AllowedFormats()...)
	ret = append(ret, f.TemplatePrinterFlags.AllowedFormats()...)
	return ret
}

// ToPrinter returns a printer capable of
// handling --output or --template printing.
// Returns false if the specified outputFormat does not match a supported format.
// Supported format types can be found in pkg/printers/printers.go
//
// ToPrinter 按 JSON/YAML、name、template 的顺序尝试；每个子 printer 不匹配时
// 返回 NoCompatiblePrinterError，最终都失败才返回汇总错误。
func (f *PrintFlags) ToPrinter() (printers.ResourcePrinter, error) {
	outputFormat := ""
	if f.OutputFormat != nil {
		outputFormat = *f.OutputFormat
	}
	// For backwards compatibility we want to support a --template argument given, even when no --output format is provided.
	// If no explicit output format has been provided via the --output flag, fallback
	// to honoring the --template argument.
	// 历史行为：只写 --template 不写 --output 时，等价于 --output=go-template。
	templateFlagSpecified := f.TemplatePrinterFlags != nil &&
		f.TemplatePrinterFlags.TemplateArgument != nil &&
		len(*f.TemplatePrinterFlags.TemplateArgument) > 0
	outputFlagSpecified := f.OutputFlagSpecified != nil && f.OutputFlagSpecified()
	if templateFlagSpecified && !outputFlagSpecified {
		outputFormat = "go-template"
	}

	// 依次尝试，IsNoCompatiblePrinterError 表示“换下一个”，其他错误则原样返回。
	if f.JSONYamlPrintFlags != nil {
		if p, err := f.JSONYamlPrintFlags.ToPrinter(outputFormat); !IsNoCompatiblePrinterError(err) {
			return f.TypeSetterPrinter.WrapToPrinter(p, err)
		}
	}

	if f.NamePrintFlags != nil {
		if p, err := f.NamePrintFlags.ToPrinter(outputFormat); !IsNoCompatiblePrinterError(err) {
			return f.TypeSetterPrinter.WrapToPrinter(p, err)
		}
	}

	if f.TemplatePrinterFlags != nil {
		if p, err := f.TemplatePrinterFlags.ToPrinter(outputFormat); !IsNoCompatiblePrinterError(err) {
			return f.TypeSetterPrinter.WrapToPrinter(p, err)
		}
	}

	return nil, NoCompatiblePrinterError{OutputFormat: f.OutputFormat, AllowedFormats: f.AllowedFormats()}
}

// AddFlags receives a *cobra.Command reference and binds
// flags related to JSON/Yaml/Name/Template printing to it
func (f *PrintFlags) AddFlags(cmd *cobra.Command) {
	f.JSONYamlPrintFlags.AddFlags(cmd)
	f.NamePrintFlags.AddFlags(cmd)
	f.TemplatePrinterFlags.AddFlags(cmd)

	if f.OutputFormat != nil {
		// 帮助文本里动态列出合法格式，新增格式时无需手工同步。
		cmd.Flags().StringVarP(f.OutputFormat, "output", "o", *f.OutputFormat, fmt.Sprintf(`Output format. One of: (%s).`, strings.Join(f.AllowedFormats(), ", ")))
		// 用 cobra 的 Changed 判断是否显式传了 -o，而不是和默认值比较。
		if f.OutputFlagSpecified == nil {
			f.OutputFlagSpecified = func() bool {
				return cmd.Flag("output").Changed
			}
		}
	}
}

// WithDefaultOutput sets a default output format if one is not provided through a flag value
//
// 链式设置默认输出格式，返回自身以支持连续调用。
func (f *PrintFlags) WithDefaultOutput(output string) *PrintFlags {
	f.OutputFormat = &output
	return f
}

// WithTypeSetter sets a wrapper than will surround the returned printer with a printer to type resources
//
// 某些对象（尤其从文件读取的）缺少 apiVersion/kind，TypeSetter 在打印前补齐。
func (f *PrintFlags) WithTypeSetter(scheme *runtime.Scheme) *PrintFlags {
	f.TypeSetterPrinter = printers.NewTypeSetter(scheme)
	return f
}

// NewPrintFlags returns a default *PrintFlags
//
// operation 决定 name 输出时的成功动词（如 created），其余子 flag 使用默认值。
func NewPrintFlags(operation string) *PrintFlags {
	outputFormat := ""

	return &PrintFlags{
		OutputFormat: &outputFormat,

		JSONYamlPrintFlags:   NewJSONYamlPrintFlags(),
		NamePrintFlags:       NewNamePrintFlags(operation),
		TemplatePrinterFlags: NewKubeTemplatePrintFlags(),
	}
}
