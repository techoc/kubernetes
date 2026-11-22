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

// MetricsConfiguration contains all metrics options.
//
// 这些字段对应组件配置文件中的 metrics 段，在启动时转换为内部选项。
type MetricsConfiguration struct {
	// ShowHiddenMetricsForVersion is the previous version for which you want to show hidden metrics.
	// Only the previous minor version is meaningful, other values will not be allowed.
	// The format is <major>.<minor>, e.g.: '1.16'.
	// The purpose of this format is make sure you have the opportunity to notice if the next release hides additional metrics,
	// rather than being surprised when they are permanently removed in the release after that.
	//
	// 隐藏指标只保留一个版本的展示窗口，迫使运维在永久移除前及时更新监控。
	ShowHiddenMetricsForVersion string `json:"showHiddenMetricsForVersion,omitempty"`
	// DisabledMetrics is a list of fully qualified metric names that should be disabled.
	// Disabling metrics is higher in precedence than showing hidden metrics.
	DisabledMetrics []string `json:"disabledMetrics,omitempty"`
	// AllowListMapping is the map from metric-label to value allow-list of this label.
	// The key's format is <MetricName>,<LabelName>, while its value is a list of allowed values for that label of that metric, i.e., <allowed_value>,<allowed_value>,...
	// For e.g., metric1,label1='v1,v2,v3', metric1,label2='v1,v2,v3' metric2,label1='v1,v2,v3'."
	//
	// 把高基数标签值限制在已知集合内，避免指标基数爆炸；未知值会被替换为占位符。
	AllowListMapping map[string]string `json:"allowListMapping,omitempty"`
	// The path to the manifest file that contains the allow-list mapping. Provided for convenience over AllowListMapping.
	// The file contents must represent a map of string keys and values, i.e.,
	//  "metric1,label1": "value11,value12"
	//  "metric2,label2": ""
	//
	// 与 AllowListMapping 同时设置时的合并规则由启动代码决定；通常清单文件更易维护。
	AllowListMappingManifest string `json:"allowListMappingManifest,omitempty"`
}
