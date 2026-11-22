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

// templates are logically optional for specifying a format.
// this allows a user to specify a template format value
// as --output=go-template=
// 中文：模板可内联在 -o go-template=... 里，也可省略等号后的内容而由 --template 提供；
// 这些别名是为兼容历史 CLI 用法而保留的。
var templateFormats = map[string]bool{
	"template":         true,
	"go-template":      true,
	"go-template-file": true,
	"templatefile":     true,
}

// GoTemplatePrintFlags provides default flags necessary for template printing.
// Given the following flag values, a printer can be requested that knows
// how to handle printing based on these values.
type GoTemplatePrintFlags struct {
	// indicates if it is OK to ignore missing keys for rendering
	// an output template.
	// 中文：默认 true，缺失字段渲染为空串；设为 false 时缺失字段直接报错，便于发现拼写错误。
	AllowMissingKeys *bool
	// --template 的值，既可以是模板文本，也可以是模板文件路径（取决于格式）。
	TemplateArgument *string
}

// AllowedFormats returns slice of string of allowed GoTemplatePrint printing format
func (f *GoTemplatePrintFlags) AllowedFormats() []string {
	// map 遍历无序，排序后返回，保证帮助信息与错误提示稳定可读。
	formats := make([]string, 0, len(templateFormats))
	for format := range templateFormats {
		formats = append(formats, format)
	}
	sort.Strings(formats)
	return formats
}

// ToPrinter receives an templateFormat and returns a printer capable of
// handling --template format printing.
// Returns false if the specified templateFormat does not match a template format.
func (f *GoTemplatePrintFlags) ToPrinter(templateFormat string) (printers.ResourcePrinter, error) {
	// 既没有 --template 也没给格式，说明用户意图不明确，按不匹配处理。
	if (f.TemplateArgument == nil || len(*f.TemplateArgument) == 0) && len(templateFormat) == 0 {
		return nil, NoCompatiblePrinterError{Options: f, OutputFormat: &templateFormat}
	}

	templateValue := ""

	if f.TemplateArgument == nil || len(*f.TemplateArgument) == 0 {
		// 未提供 --template 时，从 -o go-template=XXX 中切出等号后的模板文本。
		for format := range templateFormats {
			format = format + "="
			if strings.HasPrefix(templateFormat, format) {
				templateValue = templateFormat[len(format):]
				templateFormat = format[:len(format)-1]
				break
			}
		}
	} else {
		templateValue = *f.TemplateArgument
	}

	// 此处 templateFormat 已被规约成不带等号的形式。
	if _, supportedFormat := templateFormats[templateFormat]; !supportedFormat {
		return nil, NoCompatiblePrinterError{OutputFormat: &templateFormat, AllowedFormats: f.AllowedFormats()}
	}

	if len(templateValue) == 0 {
		return nil, fmt.Errorf("template format specified but no template given")
	}

	// *-file 形式把参数当作文件路径读取，其余形式直接当模板文本。
	if templateFormat == "templatefile" || templateFormat == "go-template-file" {
		data, err := os.ReadFile(templateValue)
		if err != nil {
			return nil, fmt.Errorf("error reading --template %s, %v", templateValue, err)
		}

		templateValue = string(data)
	}

	p, err := printers.NewGoTemplatePrinter([]byte(templateValue))
	if err != nil {
		return nil, fmt.Errorf("error parsing template %s, %v", templateValue, err)
	}

	allowMissingKeys := true
	// 未显式配置时默认容忍缺失键，这是 kubectl 长期沿用的行为。
	if f.AllowMissingKeys != nil {
		allowMissingKeys = *f.AllowMissingKeys
	}

	p.AllowMissingKeys(allowMissingKeys)
	return p, nil
}

// AddFlags receives a *cobra.Command reference and binds
// flags related to template printing to it
func (f *GoTemplatePrintFlags) AddFlags(c *cobra.Command) {
	if f.TemplateArgument != nil {
		// 标记为文件名，便于 shell 补全识别为路径候选。
		c.Flags().StringVar(f.TemplateArgument, "template", *f.TemplateArgument, "Template string or path to template file to use when -o=go-template, -o=go-template-file. The template format is golang templates [http://golang.org/pkg/text/template/#pkg-overview].")
		c.MarkFlagFilename("template")
	}
	if f.AllowMissingKeys != nil {
		c.Flags().BoolVar(f.AllowMissingKeys, "allow-missing-template-keys", *f.AllowMissingKeys, "If true, ignore any errors in templates when a field or map key is missing in the template. Only applies to golang and jsonpath output formats.")
	}
}

// NewGoTemplatePrintFlags returns flags associated with
// --template printing, with default values set.
func NewGoTemplatePrintFlags() *GoTemplatePrintFlags {
	allowMissingKeysPtr := true
	templateValuePtr := ""

	return &GoTemplatePrintFlags{
		TemplateArgument: &templateValuePtr,
		AllowMissingKeys: &allowMissingKeysPtr,
	}
}
