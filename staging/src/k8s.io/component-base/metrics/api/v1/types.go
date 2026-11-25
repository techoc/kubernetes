/*
Copyright 2025 The Kubernetes Authors.

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

// MetricsConfiguration 包含全部指标选项。
// 这些字段对应组件配置文件中的 metrics 段，在启动时转换为内部选项。
type MetricsConfiguration struct {
	// ShowHiddenMetricsForVersion 指定展示隐藏指标的版本，格式为 <major>.<minor>，例如 '1.16'。
	// 只有上一个次版本有意义，其他值均不允许；该格式让使用者有机会注意到下一个版本将隐藏更多指标，
	// 而不是在后续版本永久移除时才感到意外。
	//
	// 隐藏指标只保留一个版本的展示窗口，迫使运维在永久移除前及时更新监控。
	ShowHiddenMetricsForVersion string `json:"showHiddenMetricsForVersion,omitempty"`
	// DisabledMetrics 是需要禁用的完整限定指标名列表；禁用指标的优先级高于展示隐藏指标。
	DisabledMetrics []string `json:"disabledMetrics,omitempty"`
	// AllowListMapping 是“指标-标签”到该标签允许值列表的映射。
	// key 格式为 <MetricName>,<LabelName>，value 是该指标标签的允许值列表，即 <allowed_value>,<allowed_value>,...。
	// 例如：metric1,label1='v1,v2,v3'，metric1,label2='v1,v2,v3'，metric2,label1='v1,v2,v3'。
	//
	// 把高基数标签值限制在已知集合内，避免指标基数爆炸；未知值会被替换为占位符。
	AllowListMapping map[string]string `json:"allowListMapping,omitempty"`
	// AllowListMappingManifest 是包含允许值映射的清单文件路径，作为 AllowListMapping 的便捷替代项。
	// 文件内容必须是字符串键值映射，例如：
	//  "metric1,label1": "value11,value12"
	//  "metric2,label2": ""
	//
	// 与 AllowListMapping 同时设置时的合并规则由启动代码决定；通常清单文件更易维护。
	AllowListMappingManifest string `json:"allowListMappingManifest,omitempty"`
}
