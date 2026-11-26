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

package perf

import (
	"fmt"

	e2ekubelet "k8s.io/kubernetes/test/e2e/framework/kubelet"
	"k8s.io/kubernetes/test/e2e/perftype"
)

// CurrentKubeletPerfMetricsVersion 是当前 kubelet 性能指标
// 版本。多个性能相关数据结构都使用它。每次对指标做
// 不兼容更改时都应提升版本。
const CurrentKubeletPerfMetricsVersion = "v2"

// ResourceUsageToPerfData 把 ResourceUsagePerNode 转换为 PerfData。注意该函数
// 只关心内存用量，因为 cpu 用量信息会从 NodesCPUSummary 中提取。
func ResourceUsageToPerfData(usagePerNode e2ekubelet.ResourceUsagePerNode) *perftype.PerfData {
	return ResourceUsageToPerfDataWithLabels(usagePerNode, nil)
}

// CPUUsageToPerfData 把 NodesCPUSummary 转换为 PerfData。
func CPUUsageToPerfData(usagePerNode e2ekubelet.NodesCPUSummary) *perftype.PerfData {
	return CPUUsageToPerfDataWithLabels(usagePerNode, nil)
}

// ResourceUsageToPerfDataWithLabels 把 ResourceUsagePerNode 转换为带附加标签的 PerfData。
// 注意该函数只关心内存用量，因为 cpu 用量信息会从 NodesCPUSummary 中提取。
func ResourceUsageToPerfDataWithLabels(usagePerNode e2ekubelet.ResourceUsagePerNode, labels map[string]string) *perftype.PerfData {
	items := []perftype.DataItem{}
	for node, usages := range usagePerNode {
		for c, usage := range usages {
			item := perftype.DataItem{
				Data: map[string]float64{
					"memory":     float64(usage.MemoryUsageInBytes) / (1024 * 1024),
					"workingset": float64(usage.MemoryWorkingSetInBytes) / (1024 * 1024),
					"rss":        float64(usage.MemoryRSSInBytes) / (1024 * 1024),
				},
				Unit: "MB",
				Labels: map[string]string{
					"node":      node,
					"container": c,
					"datatype":  "resource",
					"resource":  "memory",
				},
			}
			items = append(items, item)
		}
	}
	return &perftype.PerfData{
		Version:   CurrentKubeletPerfMetricsVersion,
		DataItems: items,
		Labels:    labels,
	}
}

// CPUUsageToPerfDataWithLabels 把 NodesCPUSummary 转换为带附加标签的 PerfData。
func CPUUsageToPerfDataWithLabels(usagePerNode e2ekubelet.NodesCPUSummary, labels map[string]string) *perftype.PerfData {
	items := []perftype.DataItem{}
	for node, usages := range usagePerNode {
		for c, usage := range usages {
			data := map[string]float64{}
			for perc, value := range usage {
				data[fmt.Sprintf("Perc%02.0f", perc*100)] = value * 1000
			}

			item := perftype.DataItem{
				Data: data,
				Unit: "mCPU",
				Labels: map[string]string{
					"node":      node,
					"container": c,
					"datatype":  "resource",
					"resource":  "cpu",
				},
			}
			items = append(items, item)
		}
	}
	return &perftype.PerfData{
		Version:   CurrentKubeletPerfMetricsVersion,
		DataItems: items,
		Labels:    labels,
	}
}
