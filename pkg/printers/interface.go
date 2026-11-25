/*
Copyright 2017 The Kubernetes Authors.

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

// Package printers 提供 Kubernetes 资源对象的打印和表格生成功能。
// 主要用于 kubectl get/describe 等命令将资源对象格式化为人类可读的输出。
//
// 核心概念：
//   - ResourcePrinter: 资源打印器接口，定义如何将 runtime.Object 输出到 io.Writer
//   - TableGenerator: 表格生成器接口，将资源对象转换为 metav1.Table 结构
//   - HumanReadableGenerator: 表格生成器的具体实现，通过注册类型的打印处理器来生成表格
package printers

import (
	"io"

	"k8s.io/apimachinery/pkg/runtime"
)

// ResourcePrinter 是资源打印器接口，知道如何打印 runtime 对象。
// 用于 kubectl 等工具将 Kubernetes 资源格式化为人类可读的输出。
type ResourcePrinter interface {
	// PrintObj 将 runtime 对象格式化后写入 writer；格式化或写入失败时返回错误。
	PrintObj(runtime.Object, io.Writer) error
}

// ResourcePrinterFunc 是可直接作为 ResourcePrinter 使用的函数类型，
// 允许以函数形式实现打印逻辑，无需定义完整结构体。
type ResourcePrinterFunc func(runtime.Object, io.Writer) error

// PrintObj 调用底层函数，实现 ResourcePrinter 接口。
func (fn ResourcePrinterFunc) PrintObj(obj runtime.Object, w io.Writer) error {
	return fn(obj, w)
}
