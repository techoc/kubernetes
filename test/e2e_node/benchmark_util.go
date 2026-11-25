//go:build linux

/*
Copyright 2015 The Kubernetes Authors.

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

package e2enode

import (
	"context"
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/test/e2e/framework"
	e2emetrics "k8s.io/kubernetes/test/e2e/framework/metrics"
	e2eperf "k8s.io/kubernetes/test/e2e/framework/perf"
	"k8s.io/kubernetes/test/e2e/perftype"
	nodeperftype "k8s.io/kubernetes/test/e2e_node/perftype"
)

const (
	// TimeSeriesTag 是时间序列的起始标记。
	TimeSeriesTag = "[Result:TimeSeries]"
	// TimeSeriesEnd 是时间序列的结束标记。
	TimeSeriesEnd = "[Finish:TimeSeries]"
)

// dumpDataToFile 在 labels 中插入当前时间戳，并将测试数据写入以指定前缀命名的文件
func dumpDataToFile(data interface{}, labels map[string]string, prefix string) {
	testName := labels["test"]
	fileName := path.Join(framework.TestContext.ReportDir, fmt.Sprintf("%s-%s-%s.json", prefix, framework.TestContext.ReportPrefix, testName))
	labels["timestamp"] = strconv.FormatInt(time.Now().UTC().Unix(), 10)
	framework.Logf("Dumping perf data for test %q to %q.", testName, fileName)
	if err := os.WriteFile(fileName, []byte(framework.PrettyPrintJSON(data)), 0644); err != nil {
		framework.Logf("Failed to write perf data for test %q to %q: %v", testName, fileName, err)
	}
}

// logPerfData 在 framework.TestContext.ReportDir 非空时将性能数据写入独立的 json 文件，
// 否则写入常规构建日志。perfType 标识性能数据的类型，例如 "cpu" 和 "memory"。
// 发生错误时不会记录任何性能数据。
func logPerfData(p *perftype.PerfData, perfType string) {
	if framework.TestContext.ReportDir == "" {
		printPerfData(p)
		return
	}
	dumpDataToFile(p, p.Labels, "performance-"+perfType)
}

// logDensityTimeSeries 在 framework.TestContext.ReportDir 非空时将操作与资源使用量的
// 时间序列写入独立的 json 文件，否则写入常规构建日志。
// 发生错误时不会记录任何性能数据。
func logDensityTimeSeries(rc *ResourceCollector, create, watch map[string]metav1.Time, testInfo map[string]string) {
	timeSeries := &nodeperftype.NodeTimeSeries{
		Labels:  testInfo,
		Version: e2eperf.CurrentKubeletPerfMetricsVersion,
	}
	// 附上操作时间序列
	timeSeries.OperationData = map[string][]int64{
		"create":  getCumulatedPodTimeSeries(create),
		"running": getCumulatedPodTimeSeries(watch),
	}
	// 附上资源时间序列
	timeSeries.ResourceData = rc.GetResourceTimeSeries()

	if framework.TestContext.ReportDir == "" {
		framework.Logf("%s %s\n%s", TimeSeriesTag, framework.PrettyPrintJSON(timeSeries), TimeSeriesEnd)
		return
	}
	dumpDataToFile(timeSeries, timeSeries.Labels, "time_series")
}

type int64arr []int64

func (a int64arr) Len() int           { return len(a) }
func (a int64arr) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a int64arr) Less(i, j int) bool { return a[i] < a[j] }

// getCumulatedPodTimeSeries 返回累计 pod 数量的时间序列
func getCumulatedPodTimeSeries(timePerPod map[string]metav1.Time) []int64 {
	timeSeries := make(int64arr, 0)
	for _, ts := range timePerPod {
		timeSeries = append(timeSeries, ts.Time.UnixNano())
	}
	// 对所有时间戳排序
	sort.Sort(timeSeries)
	return timeSeries
}

// getLatencyPerfData 返回 pod 启动延迟的性能数据
func getLatencyPerfData(latency e2emetrics.LatencyMetric, testInfo map[string]string) *perftype.PerfData {
	return &perftype.PerfData{
		Version: e2eperf.CurrentKubeletPerfMetricsVersion,
		DataItems: []perftype.DataItem{
			{
				Data: map[string]float64{
					"Perc50":  float64(latency.Perc50) / 1000000,
					"Perc90":  float64(latency.Perc90) / 1000000,
					"Perc99":  float64(latency.Perc99) / 1000000,
					"Perc100": float64(latency.Perc100) / 1000000,
				},
				Unit: "ms",
				Labels: map[string]string{
					"datatype":    "latency",
					"latencytype": "create-pod",
				},
			},
		},
		Labels: testInfo,
	}
}

// getThroughputPerfData 返回 pod 创建启动吞吐量的性能数据
func getThroughputPerfData(batchLag time.Duration, e2eLags []e2emetrics.PodLatencyData, podsNr int, testInfo map[string]string) *perftype.PerfData {
	return &perftype.PerfData{
		Version: e2eperf.CurrentKubeletPerfMetricsVersion,
		DataItems: []perftype.DataItem{
			{
				Data: map[string]float64{
					"batch":        float64(podsNr) / batchLag.Minutes(),
					"single-worst": 1.0 / e2eLags[len(e2eLags)-1].Latency.Minutes(),
				},
				Unit: "pods/min",
				Labels: map[string]string{
					"datatype":    "throughput",
					"latencytype": "create-pod",
				},
			},
		},
		Labels: testInfo,
	}
}

// getTestNodeInfo 返回一个 label map，包含测试名称和描述、运行测试的节点名称、
// 节点镜像名称以及节点容量
func getTestNodeInfo(f *framework.Framework, testName, testDesc string) map[string]string {
	nodeName := framework.TestContext.NodeName
	node, err := f.ClientSet.CoreV1().Nodes().Get(context.TODO(), nodeName, metav1.GetOptions{})
	framework.ExpectNoError(err)

	cpu, ok := node.Status.Capacity[v1.ResourceCPU]
	if !ok {
		framework.Failf("Fail to fetch CPU capacity value of test node.")
	}

	memory, ok := node.Status.Capacity[v1.ResourceMemory]
	if !ok {
		framework.Failf("Fail to fetch Memory capacity value of test node.")
	}

	cpuValue, ok := cpu.AsInt64()
	if !ok {
		framework.Failf("Fail to fetch CPU capacity value as Int64.")
	}

	memoryValue, ok := memory.AsInt64()
	if !ok {
		framework.Failf("Fail to fetch Memory capacity value as Int64.")
	}

	image := node.Status.NodeInfo.OSImage
	if framework.TestContext.ImageDescription != "" {
		image = fmt.Sprintf("%s (%s)", image, framework.TestContext.ImageDescription)
	}
	return map[string]string{
		"node":    nodeName,
		"test":    testName,
		"image":   image,
		"machine": fmt.Sprintf("cpu:%dcore,memory:%.1fGB", cpuValue, float32(memoryValue)/(1024*1024*1024)),
		"desc":    testDesc,
	}
}

// printPerfData 以 json 格式输出性能数据，并加上 PerfResultTag 前缀。
// 发生错误时不输出任何内容。
func printPerfData(p *perftype.PerfData) {
	// 必须确保 perftype.PerfResultEnd 位于新的一行
	if str := framework.PrettyPrintJSON(p); str != "" {
		framework.Logf("%s %s\n%s", perftype.PerfResultTag, str, perftype.PerfResultEnd)
	}
}
