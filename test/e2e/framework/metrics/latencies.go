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

package metrics

import (
	"time"
)

// PodLatencyData 封装 Pod 启动延迟信息。
type PodLatencyData struct {
	// Pod 的名称
	Name string
	// 该 Pod 运行的节点
	Node string
	// 与 Pod 启动时间相关的延迟信息
	Latency time.Duration
}

// LatencySlice 是 PodLatencyData 的数组，封装 Pod 启动延迟信息。
type LatencySlice []PodLatencyData

func (a LatencySlice) Len() int           { return len(a) }
func (a LatencySlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a LatencySlice) Less(i, j int) bool { return a[i].Latency < a[j].Latency }
