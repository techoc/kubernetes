/*
Copyright 2016 The Kubernetes Authors.

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

package perftype

// TODO(random-liu): Replace this with prometheus' data model.

// 以下性能数据结构是通用且格式良好的。
// 它们可以漂亮地打印为 json 格式，并被其他性能
// 分析工具分析，例如 Perfdash（k8s.io/contrib/perfdash）。

// DataItem 是数据点。
type DataItem struct {
	// Data 是从 bucket 到实际数据点的 map（例如 "Perc90" -> 23.5）。注意
	// 所有标签组合相同的数据项应具有相同的 bucket。
	Data map[string]float64 `json:"data"`
	// Unit 是数据单位。注意，所有标签组合相同的数据项
	// 应具有相同的单位。
	Unit string `json:"unit"`
	// Labels 是数据项的标签。
	Labels map[string]string `json:"labels,omitempty"`
}

// PerfData 包含当前测试生成的所有数据项。
type PerfData struct {
	// Version 是 metrics 的版本。metrics 使用者可用它
	// 检测 metrics 版本变化并决定支持哪个版本。
	Version   string     `json:"version"`
	DataItems []DataItem `json:"dataItems"`
	// Labels 是数据集的标签。
	Labels map[string]string `json:"labels,omitempty"`
}

// PerfResultTag 是生成的 perfdata 的前缀。分析工具可通过该标签找到性能
// 结果。
const PerfResultTag = "[Result:Performance]"

// PerfResultEnd 是生成的 perfdata 的结尾。分析工具可通过该标签找到性能
// 结果的结束位置。
const PerfResultEnd = "[Finish:Performance]"
