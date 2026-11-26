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

// NoCompatiblePrinterError 携带输出格式不兼容的错误信息，
// 在用户提供无效输出格式时构造。
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

// IsNoCompatiblePrinterError 判断错误是否表示输出格式不兼容。
//
// 用类型断言而非 errors.As：该错误只在本包内产生，且通常不走 errors.Wrap。
func IsNoCompatiblePrinterError(err error) bool {
	if err == nil {
		return false
	}

	_, ok := err.(NoCompatiblePrinterError)
	return ok
}

// PrintFlags 组合各命令共用的输出标志，
// 并提供根据标志值取得对应 printer 的方法。
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

	// OutputFlagSpecified 判断用户是否显式指定了输出格式；
	// 高级调用方可据此改写标志绑定逻辑。
	OutputFlagSpecified func() bool
}

// Complete 根据 successTemplate 设置 NamePrintFlags 的 operation 标志。
//
// 仅影响 name 输出时的操作动词（如 created/configured）。
func (f *PrintFlags) Complete(successTemplate string) error {
	return f.NamePrintFlags.Complete(successTemplate)
}

// AllowedFormats 返回 JSON/YAML、Name、Template 输出支持的格式名列表。
//
// 汇总三类 printer 的格式并保持各自顺序，供帮助文本和错误信息使用。
func (f *PrintFlags) AllowedFormats() []string {
	ret := []string{}
	ret = append(ret, f.JSONYamlPrintFlags.AllowedFormats()...)
	ret = append(ret, f.NamePrintFlags.AllowedFormats()...)
	ret = append(ret, f.TemplatePrinterFlags.AllowedFormats()...)
	return ret
}

// ToPrinter 返回可处理 --output 或 --template 的 printer；
// 格式不匹配时返回 false。支持的格式见 pkg/printers/printers.go。
//
// ToPrinter 按 JSON/YAML、name、template 的顺序尝试；每个子 printer 不匹配时
// 返回 NoCompatiblePrinterError，最终都失败才返回汇总错误。
func (f *PrintFlags) ToPrinter() (printers.ResourcePrinter, error) {
	outputFormat := ""
	if f.OutputFormat != nil {
		outputFormat = *f.OutputFormat
	}
	// 为兼容旧行为，即使没有 --output，也要支持 --template。
	// 未通过 --output 显式指定格式时，回退到 --template。
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

// AddFlags 把 JSON/YAML、Name、Template 输出相关标志绑定到给定的 *cobra.Command。
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

// WithDefaultOutput 在未通过标志指定输出格式时设置默认值。
//
// 链式设置默认输出格式，返回自身以支持连续调用。
func (f *PrintFlags) WithDefaultOutput(output string) *PrintFlags {
	f.OutputFormat = &output
	return f
}

// WithTypeSetter 给返回的 printer 套一层包装，用于为资源补全类型信息。
//
// 某些对象（尤其从文件读取的）缺少 apiVersion/kind，TypeSetter 在打印前补齐。
func (f *PrintFlags) WithTypeSetter(scheme *runtime.Scheme) *PrintFlags {
	f.TypeSetterPrinter = printers.NewTypeSetter(scheme)
	return f
}

// NewPrintFlags 返回带默认值的 *PrintFlags。
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
