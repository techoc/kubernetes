/*
Copyright 2021 The Kubernetes Authors.

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

import (
	"encoding/json"
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// 支持的输出格式。
const (
	// DefaultLogFormat 是传统的 klog 输出格式。
	DefaultLogFormat = "text"

	// JSONLogFormat 将每条日志消息输出为 JSON 结构体。
	JSONLogFormat = "json"
)

// 结构体的 alpha/beta 级别取其内部字段中最不稳定的级别，
// LoggingConfiguration.ValidateAndApply 会检查相应特性门控。
//
// 结构体的 alpha/beta 级别取其内部字段的最高不稳定级别；
// LoggingConfiguration.ValidateAndApply 会据此检查特性门控是否允许启用。

// LoggingConfiguration 包含日志选项。
//
// LoggingConfiguration 描述组件的日志行为，通常由 --config 或命令行标志填充。
type LoggingConfiguration struct {
	// Format 指定日志消息的结构。
	// 默认值为 `text`。
	Format string `json:"format,omitempty"`
	// 日志刷新之间的最大时间间隔。
	// 字符串按时长解析（例如 "1s"），
	// 整数表示最大纳秒数（例如 1s = 1000000000）。
	// 若所选日志后端无缓冲地写入消息，则忽略此字段。
	FlushFrequency TimeOrMetaDuration `json:"flushFrequency"`
	// Verbosity 是决定记录哪些日志消息的阈值。
	// 默认值为 0，只记录最重要的消息；值越大，启用的消息越多。
	// 错误消息始终记录。
	Verbosity VerbosityLevel `json:"verbosity"`
	// VModule 覆盖单个文件的详细程度阈值。
	// 仅支持 "text" 日志格式。
	VModule VModuleConfiguration `json:"vmodule,omitempty"`
	// [Alpha] Options 保存各日志格式特有的额外参数。
	// 仅使用所选格式的选项，但会校验全部选项。
	// 仅在启用 LoggingAlphaOptions 特性门控时可用。
	Options FormatOptions `json:"options,omitempty"`
}

// TimeOrMetaDuration 仅为 flushFrequency 字段的向后兼容而保留，
// 新字段应使用 metav1.Duration。
//
// TimeOrMetaDuration 仅为 flushFrequency 字段的向后兼容而保留：
// 旧版本接受整数纳秒，新版本同时接受时长字符串。
type TimeOrMetaDuration struct {
	// Duration 保存时长。
	Duration metav1.Duration
	// SerializeAsString 控制值序列化为字符串还是整数。
	SerializeAsString bool `json:"-"`
}

// MarshalJSON 按解析时的形态回写：字符串输入继续输出字符串，
// 整数输入继续输出整数纳秒，避免配置文件往返后格式改变。
func (t TimeOrMetaDuration) MarshalJSON() ([]byte, error) {
	if t.SerializeAsString {
		return t.Duration.MarshalJSON()
	} else {
		// 为向后兼容而序列化为整数。
		return json.Marshal(t.Duration.Duration)
	}
}

// UnmarshalJSON 依据首字符是否为引号区分新旧两种编码。
func (t *TimeOrMetaDuration) UnmarshalJSON(b []byte) error {
	if len(b) > 0 && b[0] == '"' {
		// 字符串值按 metav1.Duration 解析。
		t.SerializeAsString = true
		return json.Unmarshal(b, &t.Duration)
	}
	t.SerializeAsString = false
	if err := json.Unmarshal(b, &t.Duration.Duration); err != nil {
		return fmt.Errorf("invalid duration %q: %w", string(b), err)
	}
	return nil
}

// FormatOptions 包含各日志格式的选项。
//
// 所有格式的选项都放在同一结构体里并全部校验，但只有被选中格式的选项会生效。
type FormatOptions struct {
	// [Alpha] Text 包含 "text" 日志格式的选项。
	// 仅在启用 LoggingAlphaOptions 特性门控时可用。
	Text TextOptions `json:"text,omitempty"`
	// [Alpha] JSON 包含 "json" 日志格式的选项。
	// 仅在启用 LoggingAlphaOptions 特性门控时可用。
	JSON JSONOptions `json:"json,omitempty"`
}

// TextOptions 包含 "text" 日志格式的选项。
type TextOptions struct {
	// 内嵌输出路由选项，JSON 中直接展开为 splitStream 等字段。
	OutputRoutingOptions `json:""`
}

// JSONOptions 包含 "json" 日志格式的选项。
type JSONOptions struct {
	// 与 text 共用输出路由选项，保证两种格式的流分离语义一致。
	OutputRoutingOptions `json:""`
}

// OutputRoutingOptions 包含 "text" 和 "json" 都支持的选项。
type OutputRoutingOptions struct {
	// [Alpha] SplitStream 将错误消息重定向到 stderr，将 info 消息缓冲后写入 stdout。
	// 默认将两者无缓冲地写入 stdout。
	// 仅在启用 LoggingAlphaOptions 特性门控时可用。
	SplitStream bool `json:"splitStream,omitempty"`
	// [Alpha] InfoBufferSize 设置使用分离流时 info 流的大小。
	// 默认值为 0，表示禁用缓冲。
	// 仅在启用 LoggingAlphaOptions 特性门控时可用。
	InfoBufferSize resource.QuantityValue `json:"infoBufferSize,omitempty"`
}

// VModuleConfiguration 是单个文件名或模式及其对应详细程度阈值的集合。
//
// 列表按顺序生效；同名模式重复出现时由 klog 的 vmodule 解析决定优先级。
type VModuleConfiguration []VModuleItem

// VModuleItem 定义匹配某个 glob 模式的一个或多个文件的详细程度。
type VModuleItem struct {
	// FilePattern 是基础文件名（即去掉 ".go" 后缀和目录），
	// 或针对该名称的 "glob" 模式。不能包含逗号和等号，
	// 因为它们是相应 klog 命令行参数的分隔符。
	FilePattern string `json:"filePattern"`
	// Verbosity 是匹配该模式的文件中日志消息的阈值。
	Verbosity VerbosityLevel `json:"verbosity"`
}

// VerbosityLevel 表示 klog 或 logr 的详细程度阈值。
//
// 数值越大日志越详细；0 只输出最重要的消息，错误消息不受该阈值影响。
type VerbosityLevel uint32
