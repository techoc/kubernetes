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
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"k8s.io/cli-runtime/pkg/printers"
)

// 模板在语义上是可选的格式说明，
// 因此用户可以写成 --output=jsonpath= 来指定模板。
//
// 这三个键是 --output 的合法取值。值本身可以省略模板，模板通过
// --output=jsonpath='{...}' 或 --template 单独给出。
var jsonFormats = map[string]bool{
	"jsonpath":         true,
	"jsonpath-file":    true,
	"jsonpath-as-json": true,
}

// JSONPathPrintFlags 提供模板输出所需的默认标志，
// 根据这些标志值可以取得能按对应格式输出的 printer。
//
// JSONPathPrintFlags 保存 jsonpath 输出所需的 flag 绑定，负责把用户输入
// 转成对应的 ResourcePrinter。
type JSONPathPrintFlags struct {
	// 指示渲染输出模板时是否允许忽略缺失的字段。
	AllowMissingKeys *bool
	// TemplateArgument 是 jsonpath 表达式；为指针以便调用方可选择不绑定该 flag。
	TemplateArgument *string
}

// AllowedFormats 返回 JSONPath 输出支持的格式名列表。
//
// 返回排序后的格式列表，便于错误信息和帮助文本保持稳定顺序。
func (f *JSONPathPrintFlags) AllowedFormats() []string {
	formats := make([]string, 0, len(jsonFormats))
	for format := range jsonFormats {
		formats = append(formats, format)
	}
	sort.Strings(formats)
	return formats
}

// ToPrinter 根据 templateFormat 返回可处理 --template 输出的 printer；
// 格式不匹配时返回 false。
//
// ToPrinter 支持两种模板来源：--output=jsonpath=<expr> 内嵌表达式，或 --template
// 指定的表达式/文件。不匹配时返回 NoCompatiblePrinterError，让上层继续尝试其他 printer。
func (f *JSONPathPrintFlags) ToPrinter(templateFormat string) (printers.ResourcePrinter, error) {
	// 既没有格式也没有模板时无法构造 printer；返回可兼容性错误而非普通错误。
	if (f.TemplateArgument == nil || len(*f.TemplateArgument) == 0) && len(templateFormat) == 0 {
		return nil, NoCompatiblePrinterError{Options: f, OutputFormat: &templateFormat}
	}

	templateValue := ""

	if f.TemplateArgument == nil || len(*f.TemplateArgument) == 0 {
		// 解析 --output=jsonpath=<expr> 形式：把前缀剥离，剩余部分作为模板。
		for format := range jsonFormats {
			format = format + "="
			if strings.HasPrefix(templateFormat, format) {
				templateValue = templateFormat[len(format):]
				templateFormat = format[:len(format)-1]
				break
			}
		}
	} else {
		// --template 显式给出的模板优先级更高。
		templateValue = *f.TemplateArgument
	}

	// 格式不在白名单中，交由调用方尝试其他 printer。
	if _, supportedFormat := jsonFormats[templateFormat]; !supportedFormat {
		return nil, NoCompatiblePrinterError{OutputFormat: &templateFormat, AllowedFormats: f.AllowedFormats()}
	}

	if len(templateValue) == 0 {
		return nil, fmt.Errorf("template format specified but no template given")
	}

	// jsonpath-file 表示模板来自文件，其余格式则直接使用内联表达式。
	if templateFormat == "jsonpath-file" {
		data, err := os.ReadFile(templateValue)
		if err != nil {
			return nil, fmt.Errorf("error reading --template %s, %v", templateValue, err)
		}

		templateValue = string(data)
	}

	p, err := printers.NewJSONPathPrinter(templateValue)
	if err != nil {
		return nil, fmt.Errorf("error parsing jsonpath %s, %v", templateValue, err)
	}

	// 默认允许缺失键，只有调用方显式设置 AllowMissingKeys 时才改变。
	allowMissingKeys := true
	if f.AllowMissingKeys != nil {
		allowMissingKeys = *f.AllowMissingKeys
	}

	p.AllowMissingKeys(allowMissingKeys)

	// jsonpath-as-json 在逐项求值后把结果序列化为 JSON，便于脚本消费。
	if templateFormat == "jsonpath-as-json" {
		p.EnableJSONOutput(true)
	}

	return p, nil
}

// AddFlags 把模板输出相关标志绑定到给定的 *cobra.Command。
//
// 只在对应指针非 nil 时绑定，允许调用方裁剪不需要的 flag。
func (f *JSONPathPrintFlags) AddFlags(c *cobra.Command) {
	if f.TemplateArgument != nil {
		c.Flags().StringVar(f.TemplateArgument, "template", *f.TemplateArgument, "Template string or path to template file to use when --output=jsonpath, --output=jsonpath-file.")
		c.MarkFlagFilename("template")
	}
	if f.AllowMissingKeys != nil {
		c.Flags().BoolVar(f.AllowMissingKeys, "allow-missing-template-keys", *f.AllowMissingKeys, "If true, ignore any errors in templates when a field or map key is missing in the template. Only applies to golang and jsonpath output formats.")
	}
}

// NewJSONPathPrintFlags 返回带默认值的 --template 输出标志。
//
// 返回时即把两个指针字段指向局部变量副本，便于后续 AddFlags 直接绑定。
func NewJSONPathPrintFlags(templateValue string, allowMissingKeys bool) *JSONPathPrintFlags {
	return &JSONPathPrintFlags{
		TemplateArgument: &templateValue,
		AllowMissingKeys: &allowMissingKeys,
	}
}
