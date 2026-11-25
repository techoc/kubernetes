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

package get

import (
	"io"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/printers"
)

// skipPrinter 允许通过 output 字段有条件地抑制对象输出。
// 对 table 对象，将其 Rows 设为 nil 来抑制输出（这样列定义仍可传递给委托的打印器）。
// 对非 table 对象，则完全不调用委托的打印器来抑制输出。
type skipPrinter struct {
	delegate printers.ResourcePrinter
	output   *bool
}

func (p *skipPrinter) PrintObj(obj runtime.Object, writer io.Writer) error {
	if *p.output {
		return p.delegate.PrintObj(obj, writer)
	}

	table, isTable := obj.(*metav1.Table)
	if !isTable {
		return nil
	}

	table = table.DeepCopy()
	table.Rows = nil
	return p.delegate.PrintObj(table, writer)
}
