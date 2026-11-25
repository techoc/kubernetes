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
	"os"
	"sort"
	"strings"

	"github.com/spf13/cobra"

	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/printers"
	"k8s.io/kubectl/pkg/scheme"
)

var columnsFormats = map[string]bool{
	"custom-columns-file": true,
	"custom-columns":      true,
}

// CustomColumnsPrintFlags 提供打印所需的默认标志，
// 用于从内联模板或文件打印自定义资源列。
type CustomColumnsPrintFlags struct {
	NoHeaders        bool
	TemplateArgument string
}

func (f *CustomColumnsPrintFlags) AllowedFormats() []string {
	formats := make([]string, 0, len(columnsFormats))
	for format := range columnsFormats {
		formats = append(formats, format)
	}
	sort.Strings(formats)
	return formats
}

// ToPrinter 接收一个 templateFormat，并返回能够
// 处理自定义列打印的打印器。
// 如果指定的 templateFormat 与支持的格式不匹配，则返回 false。
// 支持的格式类型见 pkg/printers/printers.go
func (f *CustomColumnsPrintFlags) ToPrinter(templateFormat string) (printers.ResourcePrinter, error) {
	if len(templateFormat) == 0 {
		return nil, genericclioptions.NoCompatiblePrinterError{}
	}

	templateValue := ""

	if len(f.TemplateArgument) == 0 {
		for format := range columnsFormats {
			format = format + "="
			if strings.HasPrefix(templateFormat, format) {
				templateValue = templateFormat[len(format):]
				templateFormat = format[:len(format)-1]
				break
			}
		}
	} else {
		templateValue = f.TemplateArgument
	}

	if _, supportedFormat := columnsFormats[templateFormat]; !supportedFormat {
		return nil, genericclioptions.NoCompatiblePrinterError{OutputFormat: &templateFormat, AllowedFormats: f.AllowedFormats()}
	}

	if len(templateValue) == 0 {
		return nil, fmt.Errorf("custom-columns format specified but no custom columns given")
	}

	// 调用 UniversalDecoder 时必须指定参数版本，否则会解码为内部版本。
	decoder := scheme.Codecs.UniversalDecoder(scheme.Scheme.PrioritizedVersionsAllGroups()...)

	var printer *CustomColumnsPrinter
	if templateFormat == "custom-columns-file" {
		file, err := os.Open(templateValue)
		if err != nil {
			return nil, fmt.Errorf("error reading template %s, %v\n", templateValue, err)
		}
		defer file.Close()
		printer, err = NewCustomColumnsPrinterFromTemplate(file, decoder)
		return printer, err
	}

	return NewCustomColumnsPrinterFromSpec(templateValue, decoder, f.NoHeaders)
}

// AddFlags 接收一个 *cobra.Command 引用，并绑定
// 与自定义列打印相关的标志
func (f *CustomColumnsPrintFlags) AddFlags(c *cobra.Command) {}

// NewCustomColumnsPrintFlags 返回与自定义列打印相关的标志，
// 并已设置默认值。
// NoHeaders 和 TemplateArgument 应由调用方设置。
func NewCustomColumnsPrintFlags() *CustomColumnsPrintFlags {
	return &CustomColumnsPrintFlags{
		NoHeaders:        false,
		TemplateArgument: "",
	}
}
