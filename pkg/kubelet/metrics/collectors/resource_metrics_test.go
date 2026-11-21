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

package collectors

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-base/metrics/testutil"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	summaryprovidertest "k8s.io/kubernetes/pkg/kubelet/server/stats/testing"
	"k8s.io/utils/ptr"
)

// TestCollectResourceMetrics 验证资源指标采集器在各种 Summary 输入下产出的指标文本。
// 覆盖节点、容器、Pod 三类指标，以及采集失败、时间戳不一致、字段为空值等边界场景。
func TestCollectResourceMetrics(t *testing.T) {
	// 固定的采集时间戳：2021-06-23 05:11:18.302091597 +0800。
	// 用固定值而非随机时间，保证各用例的时间戳断言具备确定性。
	staticTimestamp := time.Unix(0, 1624396278302091597)
	swapTimestamp := staticTimestamp.Add(10 * time.Second)
	testTime := metav1.NewTime(staticTimestamp)
	swapTestTime := metav1.NewTime(swapTimestamp)
	// 只校验这些指标，避免其他指标的增删导致本测试无意义地失败。
	interestedMetrics := []string{
		"scrape_error",
		"resource_scrape_error",
		"node_cpu_usage_seconds_total",
		"node_memory_working_set_bytes",
		"node_swap_usage_bytes",
		"container_cpu_usage_seconds_total",
		"container_memory_working_set_bytes",
		"container_swap_usage_bytes",
		"container_swap_limit_bytes",
		"container_start_time_seconds",
		"pod_cpu_usage_seconds_total",
		"pod_memory_working_set_bytes",
		"pod_swap_usage_bytes",
	}

	tests := []struct {
		name            string
		summary         *statsapi.Summary
		summaryErr      error
		expectedMetrics string
	}{
		{
			name:       "error getting summary",
			summary:    nil,
			summaryErr: fmt.Errorf("failed to get summary"),
			expectedMetrics: `
				# HELP scrape_error [ALPHA] 1 if there was an error while getting container metrics, 0 otherwise
				# TYPE scrape_error gauge
				scrape_error 1
				# HELP resource_scrape_error [STABLE] 1 if there was an error while getting container metrics, 0 otherwise
				# TYPE resource_scrape_error gauge
				resource_scrape_error 1
			`,
		},
		{
			name: "arbitrary node metrics",
			summary: &statsapi.Summary{
				Node: statsapi.NodeStats{
					CPU: &statsapi.CPUStats{
						Time:                 testTime,
						UsageCoreNanoSeconds: ptr.To[uint64](10000000000),
					},
					Memory: &statsapi.MemoryStats{
						Time:            testTime,
						WorkingSetBytes: ptr.To[uint64](1000),
					},
					Swap: &statsapi.SwapStats{
						Time:           testTime,
						SwapUsageBytes: ptr.To[uint64](500),
					},
				},
			},
			summaryErr: nil,
			expectedMetrics: `
				# HELP node_cpu_usage_seconds_total [STABLE] Cumulative cpu time consumed by the node in core-seconds
				# TYPE node_cpu_usage_seconds_total counter
				node_cpu_usage_seconds_total 10 1624396278302
				# HELP node_memory_working_set_bytes [STABLE] Current working set of the node in bytes
				# TYPE node_memory_working_set_bytes gauge
				node_memory_working_set_bytes 1000 1624396278302
				# HELP node_swap_usage_bytes [ALPHA] Current swap usage of the node in bytes. Reported only on non-windows systems
				# TYPE node_swap_usage_bytes gauge
				node_swap_usage_bytes 500 1624396278302
				# HELP scrape_error [ALPHA] 1 if there was an error while getting container metrics, 0 otherwise
				# TYPE scrape_error gauge
				scrape_error 0
				# HELP resource_scrape_error [STABLE] 1 if there was an error while getting container metrics, 0 otherwise
				# TYPE resource_scrape_error gauge
				resource_scrape_error 0
			`,
		},
		{
			name: "node metrics with different timestamps",
			summary: &statsapi.Summary{
				Node: statsapi.NodeStats{
					CPU: &statsapi.CPUStats{
						Time:                 testTime,
						UsageCoreNanoSeconds: ptr.To[uint64](10000000000),
					},
					Memory: &statsapi.MemoryStats{
						Time:            testTime,
						WorkingSetBytes: ptr.To[uint64](1000),
					},
					Swap: &statsapi.SwapStats{
						Time:           swapTestTime,
						SwapUsageBytes: ptr.To[uint64](500),
					},
				},
			},
			summaryErr: nil,
			expectedMetrics: `
				# HELP node_cpu_usage_seconds_total [STABLE] Cumulative cpu time consumed by the node in core-seconds
				# TYPE node_cpu_usage_seconds_total counter
				node_cpu_usage_seconds_total 10 1624396278302
				# HELP node_memory_working_set_bytes [STABLE] Current working set of the node in bytes
				# TYPE node_memory_working_set_bytes gauge
				node_memory_working_set_bytes 1000 1624396278302
				# HELP node_swap_usage_bytes [ALPHA] Current swap usage of the node in bytes. Reported only on non-windows systems
				# TYPE node_swap_usage_bytes gauge
				node_swap_usage_bytes 500 1624396288302
				# HELP scrape_error [ALPHA] 1 if there was an error while getting container metrics, 0 otherwise
				# TYPE scrape_error gauge
				scrape_error 0
				# HELP resource_scrape_error [STABLE] 1 if there was an error while getting container metrics, 0 otherwise
				# TYPE resource_scrape_error gauge
				resource_scrape_error 0
			`,
		},
		{
			name: "nil node metrics",
			summary: &statsapi.Summary{
				Node: statsapi.NodeStats{
					CPU: &statsapi.CPUStats{
						Time:                 testTime,
						UsageCoreNanoSeconds: nil,
					},
					Memory: &statsapi.MemoryStats{
						Time:            testTime,
						WorkingSetBytes: nil,
					},
				},
			},
			summaryErr: nil,
			expectedMetrics: `
				# HELP scrape_error [ALPHA] 1 if there was an error while getting container metrics, 0 otherwise
				# TYPE scrape_error gauge
				scrape_error 0
				# HELP resource_scrape_error [STABLE] 1 if there was an error while getting container metrics, 0 otherwise
				# TYPE resource_scrape_error gauge
				resource_scrape_error 0
			`,
		},
		{
			name: "arbitrary container metrics for different container, pods and namespaces",
			summary: &statsapi.Summary{
				Pods: []statsapi.PodStats{
					{
						PodRef: statsapi.PodReference{
							Name:      "pod_a",
							Namespace: "namespace_a",
						},
						Containers: []statsapi.ContainerStats{
							{
								Name:      "container_a",
								StartTime: metav1.NewTime(staticTimestamp.Add(-30 * time.Second)),
								CPU: &statsapi.CPUStats{
									Time:                 testTime,
									UsageCoreNanoSeconds: ptr.To[uint64](10000000000),
								},
								Memory: &statsapi.MemoryStats{
									Time:            testTime,
									WorkingSetBytes: ptr.To[uint64](1000),
								},
								Swap: &statsapi.SwapStats{
									Time:               testTime,
									SwapUsageBytes:     ptr.To[uint64](1000),
									SwapAvailableBytes: ptr.To[uint64](9000),
								},
							},
							{
								Name:      "container_b",
								StartTime: metav1.NewTime(staticTimestamp.Add(-2 * time.Minute)),
								CPU: &statsapi.CPUStats{
									Time:                 testTime,
									UsageCoreNanoSeconds: ptr.To[uint64](10000000000),
								},
								Memory: &statsapi.MemoryStats{
									Time:            testTime,
									WorkingSetBytes: ptr.To[uint64](1000),
								},
							},
						},
					},
					{
						PodRef: statsapi.PodReference{
							Name:      "pod_b",
							Namespace: "namespace_b",
						},
						Containers: []statsapi.ContainerStats{
							{
								Name:      "container_a",
								StartTime: metav1.NewTime(staticTimestamp.Add(-10 * time.Minute)),
								CPU: &statsapi.CPUStats{
									Time:                 testTime,
									UsageCoreNanoSeconds: ptr.To[uint64](10000000000),
								},
								Memory: &statsapi.MemoryStats{
									Time:            testTime,
									WorkingSetBytes: ptr.To[uint64](1000),
								},
							},
						},
					},
				},
			},
			summaryErr: nil,
			expectedMetrics: `
				# HELP scrape_error [ALPHA] 1 if there was an error while getting container metrics, 0 otherwise
				# TYPE scrape_error gauge
				scrape_error 0
				# HELP resource_scrape_error [STABLE] 1 if there was an error while getting container metrics, 0 otherwise
				# TYPE resource_scrape_error gauge
				resource_scrape_error 0
				# HELP container_cpu_usage_seconds_total [STABLE] Cumulative cpu time consumed by the container in core-seconds
				# TYPE container_cpu_usage_seconds_total counter
				container_cpu_usage_seconds_total{container="container_a",namespace="namespace_a",pod="pod_a"} 10 1624396278302
				container_cpu_usage_seconds_total{container="container_a",namespace="namespace_b",pod="pod_b"} 10 1624396278302
				container_cpu_usage_seconds_total{container="container_b",namespace="namespace_a",pod="pod_a"} 10 1624396278302
				# HELP container_memory_working_set_bytes [STABLE] Current working set of the container in bytes
				# TYPE container_memory_working_set_bytes gauge
				container_memory_working_set_bytes{container="container_a",namespace="namespace_a",pod="pod_a"} 1000 1624396278302
				container_memory_working_set_bytes{container="container_a",namespace="namespace_b",pod="pod_b"} 1000 1624396278302
				container_memory_working_set_bytes{container="container_b",namespace="namespace_a",pod="pod_a"} 1000 1624396278302
				# HELP container_start_time_seconds [STABLE] Start time of the container since unix epoch in seconds
				# TYPE container_start_time_seconds gauge
				container_start_time_seconds{container="container_a",namespace="namespace_a",pod="pod_a"} 1.6243962483020916e+09
				container_start_time_seconds{container="container_a",namespace="namespace_b",pod="pod_b"} 1.6243956783020916e+09
				container_start_time_seconds{container="container_b",namespace="namespace_a",pod="pod_a"} 1.6243961583020916e+09
				# HELP container_swap_limit_bytes [ALPHA] Current amount of the container swap limit in bytes. Reported only on non-windows systems
				# TYPE container_swap_limit_bytes gauge
				container_swap_limit_bytes{container="container_a",namespace="namespace_a",pod="pod_a"} 10000 1624396278302
        		# HELP container_swap_usage_bytes [ALPHA] Current amount of the container swap usage in bytes. Reported only on non-windows systems
        		# TYPE container_swap_usage_bytes gauge
        		container_swap_usage_bytes{container="container_a",namespace="namespace_a",pod="pod_a"} 1000 1624396278302
			`,
		},
		{
			name: "arbitrary container metrics for negative StartTime",
			summary: &statsapi.Summary{
				Pods: []statsapi.PodStats{
					{
						PodRef: statsapi.PodReference{
							Name:      "pod_a",
							Namespace: "namespace_a",
						},
						Containers: []statsapi.ContainerStats{
							{
								Name:      "container_a",
								StartTime: metav1.NewTime(time.Unix(0, -1624396278302091597)),
								CPU: &statsapi.CPUStats{
									Time:                 testTime,
									UsageCoreNanoSeconds: ptr.To[uint64](10000000000),
								},
								Memory: &statsapi.MemoryStats{
									Time:            testTime,
									WorkingSetBytes: ptr.To[uint64](1000),
								},
							},
						},
					},
				},
			},
			summaryErr: nil,
			expectedMetrics: `
				# HELP scrape_error [ALPHA] 1 if there was an error while getting container metrics, 0 otherwise
				# TYPE scrape_error gauge
				scrape_error 0
				# HELP resource_scrape_error [STABLE] 1 if there was an error while getting container metrics, 0 otherwise
				# TYPE resource_scrape_error gauge
				resource_scrape_error 0
				# HELP container_cpu_usage_seconds_total [STABLE] Cumulative cpu time consumed by the container in core-seconds
				# TYPE container_cpu_usage_seconds_total counter
				container_cpu_usage_seconds_total{container="container_a",namespace="namespace_a",pod="pod_a"} 10 1624396278302
				# HELP container_memory_working_set_bytes [STABLE] Current working set of the container in bytes
				# TYPE container_memory_working_set_bytes gauge
				container_memory_working_set_bytes{container="container_a",namespace="namespace_a",pod="pod_a"} 1000 1624396278302
			`,
		},
		{
			name: "nil container metrics",
			summary: &statsapi.Summary{
				Pods: []statsapi.PodStats{
					{
						PodRef: statsapi.PodReference{
							Name:      "pod_a",
							Namespace: "namespace_a",
						},
						Containers: []statsapi.ContainerStats{
							{
								Name:      "container_a",
								StartTime: metav1.NewTime(staticTimestamp.Add(-30 * time.Second)),
								CPU: &statsapi.CPUStats{
									Time:                 testTime,
									UsageCoreNanoSeconds: nil,
								},
								Memory: &statsapi.MemoryStats{
									Time:            testTime,
									WorkingSetBytes: nil,
								},
							},
						},
					},
					{
						PodRef: statsapi.PodReference{
							Name:      "pod_b",
							Namespace: "namespace_b",
						},
						Containers: []statsapi.ContainerStats{
							{
								Name:      "container_a",
								StartTime: metav1.NewTime(staticTimestamp.Add(-10 * time.Minute)),
								CPU: &statsapi.CPUStats{
									Time:                 testTime,
									UsageCoreNanoSeconds: ptr.To[uint64](10000000000),
								},
								Memory: &statsapi.MemoryStats{
									Time:            testTime,
									WorkingSetBytes: ptr.To[uint64](1000),
								},
							},
						},
					},
				},
			},
			summaryErr: nil,
			expectedMetrics: `
				# HELP container_cpu_usage_seconds_total [STABLE] Cumulative cpu time consumed by the container in core-seconds
				# TYPE container_cpu_usage_seconds_total counter
				container_cpu_usage_seconds_total{container="container_a",namespace="namespace_b",pod="pod_b"} 10 1624396278302
				# HELP container_memory_working_set_bytes [STABLE] Current working set of the container in bytes
				# TYPE container_memory_working_set_bytes gauge
				container_memory_working_set_bytes{container="container_a",namespace="namespace_b",pod="pod_b"} 1000 1624396278302
				# HELP container_start_time_seconds [STABLE] Start time of the container since unix epoch in seconds
				# TYPE container_start_time_seconds gauge
				container_start_time_seconds{container="container_a",namespace="namespace_a",pod="pod_a"} 1.6243962483020916e+09
				container_start_time_seconds{container="container_a",namespace="namespace_b",pod="pod_b"} 1.6243956783020916e+09
				# HELP scrape_error [ALPHA] 1 if there was an error while getting container metrics, 0 otherwise
				# TYPE scrape_error gauge
				scrape_error 0
				# HELP resource_scrape_error [STABLE] 1 if there was an error while getting container metrics, 0 otherwise
				# TYPE resource_scrape_error gauge
				resource_scrape_error 0
			`,
		},
		{
			name: "arbitrary pod metrics",
			summary: &statsapi.Summary{
				Pods: []statsapi.PodStats{
					{
						PodRef: statsapi.PodReference{
							Name:      "pod_a",
							Namespace: "namespace_a",
						},
						CPU: &statsapi.CPUStats{
							Time:                 testTime,
							UsageCoreNanoSeconds: ptr.To[uint64](10000000000),
						},
						Memory: &statsapi.MemoryStats{
							Time:            testTime,
							WorkingSetBytes: ptr.To[uint64](1000),
						},
						Swap: &statsapi.SwapStats{
							Time:           testTime,
							SwapUsageBytes: ptr.To[uint64](5000),
						},
					},
				},
			},
			summaryErr: nil,
			expectedMetrics: `
				# HELP scrape_error [ALPHA] 1 if there was an error while getting container metrics, 0 otherwise
				# TYPE scrape_error gauge
				scrape_error 0
				# HELP resource_scrape_error [STABLE] 1 if there was an error while getting container metrics, 0 otherwise
				# TYPE resource_scrape_error gauge
				resource_scrape_error 0
				# HELP pod_cpu_usage_seconds_total [STABLE] Cumulative cpu time consumed by the pod in core-seconds
				# TYPE pod_cpu_usage_seconds_total counter
				pod_cpu_usage_seconds_total{namespace="namespace_a",pod="pod_a"} 10 1624396278302
				# HELP pod_memory_working_set_bytes [STABLE] Current working set of the pod in bytes
				# TYPE pod_memory_working_set_bytes gauge
				pod_memory_working_set_bytes{namespace="namespace_a",pod="pod_a"} 1000 1624396278302
				# HELP pod_swap_usage_bytes [ALPHA] Current amount of the pod swap usage in bytes. Reported only on non-windows systems
				# TYPE pod_swap_usage_bytes gauge
				pod_swap_usage_bytes{namespace="namespace_a",pod="pod_a"} 5000 1624396278302
			`,
		},
		{
			name: "nil pod metrics",
			summary: &statsapi.Summary{
				Pods: []statsapi.PodStats{
					{
						PodRef: statsapi.PodReference{
							Name:      "pod_a",
							Namespace: "namespace_a",
						},
						CPU: &statsapi.CPUStats{
							Time:                 testTime,
							UsageCoreNanoSeconds: nil,
						},
						Memory: &statsapi.MemoryStats{
							Time:            testTime,
							WorkingSetBytes: nil,
						},
					},
				},
			},
			summaryErr: nil,
			expectedMetrics: `
				# HELP scrape_error [ALPHA] 1 if there was an error while getting container metrics, 0 otherwise
				# TYPE scrape_error gauge
				scrape_error 0
				# HELP resource_scrape_error [STABLE] 1 if there was an error while getting container metrics, 0 otherwise
				# TYPE resource_scrape_error gauge
				resource_scrape_error 0
			`,
		},
	}

	// 逐条执行用例，每个用例作为独立子测试运行，便于精确定位失败项。
	for _, test := range tests {
		tc := test
		t.Run(tc.name, func(t *testing.T) {
			// 测试不依赖真实的上下文取消逻辑，使用 TODO 上下文即可。
			ctx := context.TODO()
			// 使用 mock 的 SummaryProvider 隔离被测采集器，按用例注入固定输入。
			provider := summaryprovidertest.NewMockSummaryProvider(t)
			provider.EXPECT().GetCPUAndMemoryStats(ctx).Return(tc.summary, tc.summaryErr).Maybe()
			// 构造采集器，本测试只关心其采集逻辑，不涉及真实节点数据。
			collector := NewResourceMetricsCollector(provider)

			// 采集后与期望的指标文本逐项比对，指标值或时间戳不一致即判定失败。
			if err := testutil.CustomCollectAndCompare(collector, strings.NewReader(tc.expectedMetrics), interestedMetrics...); err != nil {
				t.Fatal(err)
			}
		})
	}
}
