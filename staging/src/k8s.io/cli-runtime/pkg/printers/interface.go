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

package printers

import (
	"io"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// ResourcePrinterFunc 是可打印对象的函数类型。
//
// 让普通函数可以直接满足 ResourcePrinter 接口，省去定义包装类型。
type ResourcePrinterFunc func(runtime.Object, io.Writer) error

// PrintObj 实现 ResourcePrinter 接口。
func (fn ResourcePrinterFunc) PrintObj(obj runtime.Object, w io.Writer) error {
	return fn(obj, w)
}

// ResourcePrinter 定义打印 runtime 对象的能力。
type ResourcePrinter interface {
	// PrintObj 接收对象、格式化后写入 writer。
	PrintObj(runtime.Object, io.Writer) error
}

// PrintOptions 汇总各类输出选项。
//
// PrintOptions 是表格式输出（如 kubectl get）的通用配置，各 flag 由调用方填充。
type PrintOptions struct {
	// NoHeaders 关闭表头，便于脚本消费。
	NoHeaders bool
	// WithNamespace 在表格中额外输出 NAMESPACE 列。
	WithNamespace bool
	// WithKind 在每行输出中附带资源 Kind。
	WithKind bool
	// Wide 输出额外列（如 kubectl get -o wide）。
	Wide bool
	// ShowLabels 输出标签列。
	ShowLabels bool
	// Kind 指定当前打印对象的 GroupKind。
	Kind schema.GroupKind
	// ColumnLabels 是服务端返回的自定义列名。
	ColumnLabels []string

	// SortBy 指定表格排序字段，空表示保持服务端顺序。
	SortBy string

	// 指示渲染输出模板时是否允许忽略缺失的字段。
	AllowMissingKeys bool
}
