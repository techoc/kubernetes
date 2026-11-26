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
	"github.com/spf13/cobra"

	"k8s.io/cli-runtime/pkg/printers"
)

// KubeTemplatePrintFlags 把 JSONPath 和 go-template 两种 printer 的标志组合在一起。
// 两者标志有重叠，需要同时支持时只能通过这种方式统一处理。
//
// 由于 go-template 与 jsonpath 两套 flag（--template、--allow-missing-template-keys）
// 名称相同，不能各自注册；因此这里持有一份共享的 flag 变量，
// 再分别注入两套 PrintFlags，保证两种输出格式共用同一组 flag 值。
type KubeTemplatePrintFlags struct {
	GoTemplatePrintFlags *GoTemplatePrintFlags
	JSONPathPrintFlags   *JSONPathPrintFlags

	AllowMissingKeys *bool
	TemplateArgument *string
}

// AllowedFormats 返回 GoTemplate 和 JSONPath 输出支持的格式名列表。
func (f *KubeTemplatePrintFlags) AllowedFormats() []string {
	if f == nil {
		return []string{}
	}
	return append(f.GoTemplatePrintFlags.AllowedFormats(), f.JSONPathPrintFlags.AllowedFormats()...)
}

// ToPrinter 根据 outputFormat 返回可处理 --template 输出的 printer；
// 格式不匹配时返回 false。支持的格式见 pkg/printers/printers.go。
//
// 先尝试 JSONPath；只有“无兼容 printer”这一种错误才回退到 Go template，
// 其余真实错误原样返回，避免掩盖模板解析问题。
func (f *KubeTemplatePrintFlags) ToPrinter(outputFormat string) (printers.ResourcePrinter, error) {
	if f == nil {
		return nil, NoCompatiblePrinterError{}
	}

	if p, err := f.JSONPathPrintFlags.ToPrinter(outputFormat); !IsNoCompatiblePrinterError(err) {
		return p, err
	}
	return f.GoTemplatePrintFlags.ToPrinter(outputFormat)
}

// AddFlags 把模板输出相关标志绑定到给定的 *cobra.Command。
func (f *KubeTemplatePrintFlags) AddFlags(c *cobra.Command) {
	if f == nil {
		return
	}

	if f.TemplateArgument != nil {
		c.Flags().StringVar(f.TemplateArgument, "template", *f.TemplateArgument, "Template string or path to template file to use when -o=go-template, -o=go-template-file. The template format is golang templates [http://golang.org/pkg/text/template/#pkg-overview].")
		// 启用文件名补全：--template 也可指向模板文件。
		c.MarkFlagFilename("template")
	}
	if f.AllowMissingKeys != nil {
		c.Flags().BoolVar(f.AllowMissingKeys, "allow-missing-template-keys", *f.AllowMissingKeys, "If true, ignore any errors in templates when a field or map key is missing in the template. Only applies to golang and jsonpath output formats.")
	}
}

// NewKubeTemplatePrintFlags 返回带默认值的 --template 输出标志。
//
// allowMissingKeys 默认为 true，是 kubectl 长期以来对模板中缺失字段
// 保持宽松的约定；改为 false 会破坏既有脚本的输出行为。
func NewKubeTemplatePrintFlags() *KubeTemplatePrintFlags {
	allowMissingKeysPtr := true
	templateArgPtr := ""

	return &KubeTemplatePrintFlags{
		GoTemplatePrintFlags: &GoTemplatePrintFlags{
			TemplateArgument: &templateArgPtr,
			AllowMissingKeys: &allowMissingKeysPtr,
		},
		JSONPathPrintFlags: &JSONPathPrintFlags{
			TemplateArgument: &templateArgPtr,
			AllowMissingKeys: &allowMissingKeysPtr,
		},

		TemplateArgument: &templateArgPtr,
		AllowMissingKeys: &allowMissingKeysPtr,
	}
}
