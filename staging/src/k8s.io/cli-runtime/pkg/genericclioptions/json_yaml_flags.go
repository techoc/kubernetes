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
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"k8s.io/cli-runtime/pkg/printers"
)

// AllowedFormats returns slice of string of allowed JSONYaml printing format
// 中文：kyaml 是 YAML 的严格子集，也归入这一组输出风格。
func (f *JSONYamlPrintFlags) AllowedFormats() []string {
	// 允许 nil 接收者调用，方便调用方在未初始化时安全探测可用格式。
	if f == nil {
		return []string{}
	}
	return []string{"json", "yaml", "kyaml"}
}

// JSONYamlPrintFlags provides default flags necessary for json/yaml printing.
// Given the following flag values, a printer can be requested that knows
// how to handle printing based on these values.
type JSONYamlPrintFlags struct {
	// 默认隐藏 managedFields（用户通常不关心），只有显式打开时才保留，避免输出噪声。
	ShowManagedFields bool
}

// ToPrinter receives an outputFormat and returns a printer capable of
// handling --output=(yaml|json) printing.
// Returns false if the specified outputFormat does not match a supported format.
// Supported Format types can be found in pkg/printers/printers.go
func (f *JSONYamlPrintFlags) ToPrinter(outputFormat string) (printers.ResourcePrinter, error) {
	var printer printers.ResourcePrinter

	// 输出格式大小写不敏感（-o JSON 与 -o json 等价）。
	outputFormat = strings.ToLower(outputFormat)

	valid := f.AllowedFormats()
	// 不匹配时返回带候选列表的错误，帮助用户发现拼写错误。
	if !slices.Contains(valid, outputFormat) {
		return nil, NoCompatiblePrinterError{OutputFormat: &outputFormat, AllowedFormats: valid}
	}

	switch outputFormat {
	case "json":
		printer = &printers.JSONPrinter{}
	case "yaml":
		printer = &printers.YAMLPrinter{}
	case "kyaml":
		printer = &printers.KYAMLPrinter{}
	default:
		return nil, NoCompatiblePrinterError{OutputFormat: &outputFormat, AllowedFormats: f.AllowedFormats()}
	}

	// 默认用装饰器剥离 managedFields，保持与历史输出一致。
	if !f.ShowManagedFields {
		printer = &printers.OmitManagedFieldsPrinter{Delegate: printer}
	}
	return printer, nil
}

// AddFlags receives a *cobra.Command reference and binds
// flags related to JSON or Yaml printing to it
func (f *JSONYamlPrintFlags) AddFlags(c *cobra.Command) {
	// nil 接收者表示该命令不支持 JSON/YAML 输出，静默跳过注册。
	if f == nil {
		return
	}

	c.Flags().BoolVar(&f.ShowManagedFields, "show-managed-fields", f.ShowManagedFields, "If true, keep the managedFields when printing objects in JSON or YAML format.")
}

// NewJSONYamlPrintFlags returns flags associated with
// yaml or json printing, with default values set.
func NewJSONYamlPrintFlags() *JSONYamlPrintFlags {
	return &JSONYamlPrintFlags{}
}
