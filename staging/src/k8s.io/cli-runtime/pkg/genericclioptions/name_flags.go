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
	"strings"

	"github.com/spf13/cobra"

	"k8s.io/cli-runtime/pkg/printers"
)

// NamePrintFlags provides default flags necessary for printing
// a resource's fully-qualified Kind.group/name, or a successful
// message about that resource if an Operation is provided.
type NamePrintFlags struct {
	// Operation describes the name of the action that
	// took place on an object, to be included in the
	// finalized "successful" message.
	// 中文：如 "created"、"patched"；为空时只打印资源名，非空时拼成成功提示。
	Operation string
}

// Complete sets NamePrintFlags operation flag from successTemplate
// 中文：把操作动词套进模板（通常是 "%%s <verb>" 一类）后存入 Operation，供最终消息使用。
func (f *NamePrintFlags) Complete(successTemplate string) error {
	f.Operation = fmt.Sprintf(successTemplate, f.Operation)
	return nil
}

// AllowedFormats returns slice of string of allowed Name printing format
func (f *NamePrintFlags) AllowedFormats() []string {
	// 允许 nil 接收者安全调用。
	if f == nil {
		return []string{}
	}
	return []string{"name"}
}

// ToPrinter receives an outputFormat and returns a printer capable of
// handling --output=name printing.
// Returns false if the specified outputFormat does not match a supported format.
// Supported format types can be found in pkg/printers/printers.go
func (f *NamePrintFlags) ToPrinter(outputFormat string) (printers.ResourcePrinter, error) {
	namePrinter := &printers.NamePrinter{
		Operation: f.Operation,
	}

	outputFormat = strings.ToLower(outputFormat)
	switch outputFormat {
	case "name":
		// 显式 -o name 时强制只输出 "kind/name"，即使带了操作动词。
		namePrinter.ShortOutput = true
		fallthrough
	case "":
		// 未指定输出格式时沿用默认行为（可能输出操作成功消息）。
		return namePrinter, nil
	default:
		return nil, NoCompatiblePrinterError{OutputFormat: &outputFormat, AllowedFormats: f.AllowedFormats()}
	}
}

// AddFlags receives a *cobra.Command reference and binds
// flags related to name printing to it
// 中文：name 输出无需额外 flag，保留空实现以满足 PrintFlags 的统一接口。
func (f *NamePrintFlags) AddFlags(c *cobra.Command) {}

// NewNamePrintFlags returns flags associated with
// --name printing, with default values set.
func NewNamePrintFlags(operation string) *NamePrintFlags {
	return &NamePrintFlags{
		Operation: operation,
	}
}
