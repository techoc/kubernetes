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

package output

import (
	"fmt"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/printers"
)

const (
	// TextOutput 表示纯文本输出。
	TextOutput = "text"

	// JSONOutput 表示 JSON 输出。
	JSONOutput = "json"

	// YAMLOutput 表示 YAML 输出。
	YAMLOutput = "yaml"
)

// TextPrintFlags 是处理自定义文本输出的接口。
type TextPrintFlags interface {
	ToPrinter(outputFormat string) (Printer, error)
}

// PrintFlags 组合 kubeadm 各命令共用的打印参数结构，
// 并根据参数值返回对应的打印器。
type PrintFlags struct {
	// JSONYamlPrintFlags 提供 JSON/YAML 输出所需的默认参数。
	JSONYamlPrintFlags *genericclioptions.JSONYamlPrintFlags
	// KubeTemplatePrintFlags 组合同时支持 JSONPath 和 go-template 打印器的参数。
	KubeTemplatePrintFlags *genericclioptions.KubeTemplatePrintFlags
	// TextPrintFlags 提供 kubeadm 文本输出所需的默认参数。
	TextPrintFlags TextPrintFlags
	// TypeSetterPrinter 是 ResourcePrinter 的实现，包装另一个打印器并在对象上设置类型。
	TypeSetterPrinter *printers.TypeSetterPrinter
	// OutputFormat 保存当前输出格式。
	OutputFormat *string
}

// AllowedFormats 返回允许的输出格式列表。
func (pf *PrintFlags) AllowedFormats() []string {
	ret := []string{TextOutput}
	ret = append(ret, pf.JSONYamlPrintFlags.AllowedFormats()...)
	ret = append(ret, pf.KubeTemplatePrintFlags.AllowedFormats()...)

	return ret
}

// ToPrinter 接收 outputFormat，返回能按该格式打印的打印器；格式不受支持时返回错误。
func (pf *PrintFlags) ToPrinter() (Printer, error) {
	outputFormat := ""
	if pf.OutputFormat != nil {
		outputFormat = *pf.OutputFormat
	}

	if pf.TextPrintFlags != nil {
		if p, err := pf.TextPrintFlags.ToPrinter(outputFormat); !genericclioptions.IsNoCompatiblePrinterError(err) {
			return p, err
		}
	}

	if pf.JSONYamlPrintFlags != nil {
		if p, err := pf.JSONYamlPrintFlags.ToPrinter(outputFormat); !genericclioptions.IsNoCompatiblePrinterError(err) {
			return NewResourcePrinterWrapper(pf.TypeSetterPrinter.WrapToPrinter(p, err))
		}
	}

	if pf.KubeTemplatePrintFlags != nil {
		if p, err := pf.KubeTemplatePrintFlags.ToPrinter(outputFormat); !genericclioptions.IsNoCompatiblePrinterError(err) {
			return NewResourcePrinterWrapper(pf.TypeSetterPrinter.WrapToPrinter(p, err))
		}
	}

	return nil, genericclioptions.NoCompatiblePrinterError{OutputFormat: pf.OutputFormat, AllowedFormats: pf.AllowedFormats()}
}

// AddFlags 接收 *cobra.Command，并绑定 kubeadm 打印相关参数。
func (pf *PrintFlags) AddFlags(cmd *cobra.Command) {
	pf.JSONYamlPrintFlags.AddFlags(cmd)
	pf.KubeTemplatePrintFlags.AddFlags(cmd)
	cmd.Flags().StringVarP(pf.OutputFormat, "output", "o", *pf.OutputFormat, fmt.Sprintf("Output format. One of: %s.", strings.Join(pf.AllowedFormats(), "|")))
}

// WithDefaultOutput 在参数未指定输出格式时设置默认格式。
func (pf *PrintFlags) WithDefaultOutput(outputFormat string) *PrintFlags {
	pf.OutputFormat = &outputFormat
	return pf
}

// WithTypeSetter 设置包装器，用类型设置打印器包围返回的打印器。
func (pf *PrintFlags) WithTypeSetter(scheme *runtime.Scheme) *PrintFlags {
	pf.TypeSetterPrinter = printers.NewTypeSetter(scheme)
	return pf
}

// NewOutputFlags 创建 KubeadmOutputFlags。
func NewOutputFlags(textPrintFlags TextPrintFlags) *PrintFlags {
	outputFormat := ""

	pf := &PrintFlags{
		OutputFormat: &outputFormat,

		JSONYamlPrintFlags:     genericclioptions.NewJSONYamlPrintFlags(),
		KubeTemplatePrintFlags: genericclioptions.NewKubeTemplatePrintFlags(),
		TextPrintFlags:         textPrintFlags,
	}

	// 禁用已弃用的 --template 选项。
	pf.KubeTemplatePrintFlags.TemplateArgument = nil

	return pf
}

// Printer 是 kubeadm 的通用打印接口。
type Printer interface {
	PrintObj(obj runtime.Object, writer io.Writer) error
	Fprintf(writer io.Writer, format string, args ...interface{}) (n int, err error)
	Fprintln(writer io.Writer, args ...interface{}) (n int, err error)
	Printf(format string, args ...interface{}) (n int, err error)
	Println(args ...interface{}) (n int, err error)
}

// TextPrinter 实现 Printer，用于通用文本输出。
type TextPrinter struct {
}

// PrintObj 实现 ResourcePrinter.PrintObj，打印对象。
func (tp *TextPrinter) PrintObj(obj runtime.Object, writer io.Writer) error {
	_, err := fmt.Fprintf(writer, "%+v\n", obj)
	return err
}

// Fprintf 包装 fmt.Fprintf。
func (tp *TextPrinter) Fprintf(writer io.Writer, format string, args ...interface{}) (n int, err error) {
	return fmt.Fprintf(writer, format, args...)
}

// Fprintln 包装 fmt.Fprintln。
func (tp *TextPrinter) Fprintln(writer io.Writer, args ...interface{}) (n int, err error) {
	return fmt.Fprintln(writer, args...)
}

// Printf 包装 fmt.Printf。
func (tp *TextPrinter) Printf(format string, args ...interface{}) (n int, err error) {
	return fmt.Printf(format, args...)
}

// Println 包装 fmt.Printf。
func (tp *TextPrinter) Println(args ...interface{}) (n int, err error) {
	return fmt.Println(args...)
}

// ResourcePrinterWrapper 包装 ResourcePrinter 并实现 Printer。
type ResourcePrinterWrapper struct {
	Printer printers.ResourcePrinter
}

// NewResourcePrinterWrapper 创建 ResourcePrinter。
func NewResourcePrinterWrapper(resourcePrinter printers.ResourcePrinter, err error) (Printer, error) {
	if err != nil {
		return nil, err
	}
	return &ResourcePrinterWrapper{Printer: resourcePrinter}, nil
}

// PrintObj 实现 ResourcePrinter.PrintObj，调用底层打印器 API。
func (rpw *ResourcePrinterWrapper) PrintObj(obj runtime.Object, writer io.Writer) error {
	return rpw.Printer.PrintObj(obj, writer)
}

// Fprintf 为空实现，用于满足 Printer 接口并抑制结构化输出的信息打印；文本输出通常会重写此方法。
func (rpw *ResourcePrinterWrapper) Fprintf(writer io.Writer, format string, args ...interface{}) (n int, err error) {
	return 0, nil
}

// Fprintln 为空实现，用于满足 Printer 接口并抑制结构化输出的信息打印；文本输出通常会重写此方法。
func (rpw *ResourcePrinterWrapper) Fprintln(writer io.Writer, args ...interface{}) (n int, err error) {
	return 0, nil
}

// Printf 为空实现，用于满足 Printer 接口并抑制结构化输出的信息打印；文本输出通常会重写此方法。
func (rpw *ResourcePrinterWrapper) Printf(format string, args ...interface{}) (n int, err error) {
	return 0, nil
}

// Println 为空实现，用于满足 Printer 接口并抑制结构化输出的信息打印；文本输出通常会重写此方法。
func (rpw *ResourcePrinterWrapper) Println(args ...interface{}) (n int, err error) {
	return 0, nil
}
