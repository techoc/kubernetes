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

// Supported output formats.
const (
	// DefaultLogFormat is the traditional klog output format.
	DefaultLogFormat = "text"

	// JSONLogFormat emits each log message as a JSON struct.
	JSONLogFormat = "json"
)

// The alpha or beta level of structs is the highest stability level of any field
// inside it. Feature gates will get checked during LoggingConfiguration.ValidateAndApply.
//
// 结构体的 alpha/beta 级别取其内部字段的最高不稳定级别；
// LoggingConfiguration.ValidateAndApply 会据此检查特性门控是否允许启用。

// LoggingConfiguration contains logging options.
//
// LoggingConfiguration 描述组件的日志行为，通常由 --config 或命令行标志填充。
type LoggingConfiguration struct {
	// Format Flag specifies the structure of log messages.
	// default value of format is `text`
	Format string `json:"format,omitempty"`
	// Maximum time between log flushes.
	// If a string, parsed as a duration (i.e. "1s")
	// If an int, the maximum number of nanoseconds (i.e. 1s = 1000000000).
	// Ignored if the selected logging backend writes log messages without buffering.
	FlushFrequency TimeOrMetaDuration `json:"flushFrequency"`
	// Verbosity is the threshold that determines which log messages are
	// logged. Default is zero which logs only the most important
	// messages. Higher values enable additional messages. Error messages
	// are always logged.
	Verbosity VerbosityLevel `json:"verbosity"`
	// VModule overrides the verbosity threshold for individual files.
	// Only supported for "text" log format.
	VModule VModuleConfiguration `json:"vmodule,omitempty"`
	// [Alpha] Options holds additional parameters that are specific
	// to the different logging formats. Only the options for the selected
	// format get used, but all of them get validated.
	// Only available when the LoggingAlphaOptions feature gate is enabled.
	Options FormatOptions `json:"options,omitempty"`
}

// TimeOrMetaDuration is present only for backwards compatibility for the
// flushFrequency field, and new fields should use metav1.Duration.
//
// TimeOrMetaDuration 仅为 flushFrequency 字段的向后兼容而保留：
// 旧版本接受整数纳秒，新版本同时接受时长字符串。
type TimeOrMetaDuration struct {
	// Duration holds the duration
	Duration metav1.Duration
	// SerializeAsString controls whether the value is serialized as a string or an integer
	SerializeAsString bool `json:"-"`
}

// MarshalJSON 按解析时的形态回写：字符串输入继续输出字符串，
// 整数输入继续输出整数纳秒，避免配置文件往返后格式改变。
func (t TimeOrMetaDuration) MarshalJSON() ([]byte, error) {
	if t.SerializeAsString {
		return t.Duration.MarshalJSON()
	} else {
		// Marshal as integer for backwards compatibility
		return json.Marshal(t.Duration.Duration)
	}
}

// UnmarshalJSON 依据首字符是否为引号区分新旧两种编码。
func (t *TimeOrMetaDuration) UnmarshalJSON(b []byte) error {
	if len(b) > 0 && b[0] == '"' {
		// string values unmarshal as metav1.Duration
		t.SerializeAsString = true
		return json.Unmarshal(b, &t.Duration)
	}
	t.SerializeAsString = false
	if err := json.Unmarshal(b, &t.Duration.Duration); err != nil {
		return fmt.Errorf("invalid duration %q: %w", string(b), err)
	}
	return nil
}

// FormatOptions contains options for the different logging formats.
//
// 所有格式的选项都放在同一结构体里并全部校验，但只有被选中格式的选项会生效。
type FormatOptions struct {
	// [Alpha] Text contains options for logging format "text".
	// Only available when the LoggingAlphaOptions feature gate is enabled.
	Text TextOptions `json:"text,omitempty"`
	// [Alpha] JSON contains options for logging format "json".
	// Only available when the LoggingAlphaOptions feature gate is enabled.
	JSON JSONOptions `json:"json,omitempty"`
}

// TextOptions contains options for logging format "text".
type TextOptions struct {
	// 内嵌输出路由选项，JSON 中直接展开为 splitStream 等字段。
	OutputRoutingOptions `json:""`
}

// JSONOptions contains options for logging format "json".
type JSONOptions struct {
	// 与 text 共用输出路由选项，保证两种格式的流分离语义一致。
	OutputRoutingOptions `json:""`
}

// OutputRoutingOptions contains options that are supported by both "text" and "json".
type OutputRoutingOptions struct {
	// [Alpha] SplitStream redirects error messages to stderr while
	// info messages go to stdout, with buffering. The default is to write
	// both to stdout, without buffering. Only available when
	// the LoggingAlphaOptions feature gate is enabled.
	SplitStream bool `json:"splitStream,omitempty"`
	// [Alpha] InfoBufferSize sets the size of the info stream when
	// using split streams. The default is zero, which disables buffering.
	// Only available when the LoggingAlphaOptions feature gate is enabled.
	InfoBufferSize resource.QuantityValue `json:"infoBufferSize,omitempty"`
}

// VModuleConfiguration is a collection of individual file names or patterns
// and the corresponding verbosity threshold.
//
// 列表按顺序生效；同名模式重复出现时由 klog 的 vmodule 解析决定优先级。
type VModuleConfiguration []VModuleItem

// VModuleItem defines verbosity for one or more files which match a certain
// glob pattern.
type VModuleItem struct {
	// FilePattern is a base file name (i.e. minus the ".go" suffix and
	// directory) or a "glob" pattern for such a name. It must not contain
	// comma and equal signs because those are separators for the
	// corresponding klog command line argument.
	FilePattern string `json:"filePattern"`
	// Verbosity is the threshold for log messages emitted inside files
	// that match the pattern.
	Verbosity VerbosityLevel `json:"verbosity"`
}

// VerbosityLevel represents a klog or logr verbosity threshold.
//
// 数值越大日志越详细；0 只输出最重要的消息，错误消息不受该阈值影响。
type VerbosityLevel uint32
