/*
Copyright 2020 The Kubernetes Authors.

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

package v1

// This file contains all constants defined in CRI.
// 本文件集中定义 CRI（容器运行时接口）中使用的常量，包括运行时状态条件类型、
// 容器日志的流类型与日志行标签等。它们被 kubelet 和容器运行时共同遵守。

// Required runtime condition type.
// 运行时必须上报的状态条件类型。kubelet 通过 Status RPC 获取这些条件，
// 用来判断运行时是否已经就绪、是否可以正常创建容器。
const (
	// RuntimeReady means the runtime is up and ready to accept basic containers.
	// RuntimeReady 表示容器运行时进程已经启动并就绪，能够创建和运行"基础"容器，
	// 即不依赖网络功能的容器。
	RuntimeReady = "RuntimeReady"
	// NetworkReady means the runtime network is up and ready to accept containers which require network.
	// NetworkReady 表示运行时的网络组件已经就绪，能够创建需要网络能力的容器。
	// 只有 RuntimeReady 和 NetworkReady 同时为 true 时，kubelet 才认为节点整体就绪。
	NetworkReady = "NetworkReady"
)

// LogStreamType is the type of the stream in CRI container log.
// LogStreamType 表示 CRI 容器日志中日志内容的输出流类型，用于区分标准输出和标准错误。
type LogStreamType string

const (
	// Stdout is the stream type for stdout.
	// Stdout 表示日志来自容器的标准输出（stdout）。
	Stdout LogStreamType = "stdout"
	// Stderr is the stream type for stderr.
	// Stderr 表示日志来自容器的标准错误输出（stderr）。
	Stderr LogStreamType = "stderr"
)

// LogTag is the tag of a log line in CRI container log.
// Currently defined log tags:
// * First tag: Partial/Full - P/F.
// The field in the container log format can be extended to include multiple
// tags by using a delimiter, but changes should be rare. If it becomes clear
// that better extensibility is desired, a more extensible format (e.g., json)
// should be adopted as a replacement and/or addition.
//
// LogTag 表示 CRI 容器日志中每一行日志的标签类型。
// 当前已定义的标签：
//   - 第一个标签：Partial/Full，取值 P 或 F。
//
// 容器日志格式中的标签字段后续可以通过分隔符扩展为多个标签，但这种改动应当非常谨慎。
// 如果确实需要更强的扩展性，应考虑采用 JSON 等可扩展格式来替代或补充现有格式。
type LogTag string

const (
	// LogTagPartial means the line is part of multiple lines.
	// LogTagPartial 表示当前日志行是多行日志的"一部分"，即该行还没有以换行结尾，
	// 需要与后续日志行拼接起来才是完整的一行。
	LogTagPartial LogTag = "P"
	// LogTagFull means the line is a single full line or the end of multiple lines.
	// LogTagFull 表示当前日志行是一个完整的单行日志，或者是多行日志的最后一行。
	LogTagFull LogTag = "F"
	// LogTagDelimiter is the delimiter for different log tags.
	// LogTagDelimiter 是不同日志标签之间的分隔符，用于在扩展多个标签时把它们分隔开。
	LogTagDelimiter = ":"
)
