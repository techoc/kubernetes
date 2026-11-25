/*
Copyright 2018 The Kubernetes Authors.

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

package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"

	benchparse "golang.org/x/tools/benchmark/parse"
)

// TODO(random-liu): 将其替换为 prometheus 的数据模型。

// 以下性能数据结构是通用且格式规范的。
// 它们可以漂亮地打印为 json 格式，并被其他性能分析工具（如
// Perfdash (k8s.io/contrib/perfdash)）分析。

// DataItem 表示数据点。
type DataItem struct {
	// Data 是从 bucket 到实际数据点的映射（例如 "Perc90" -> 23.5）。注意，
	// 所有具有相同标签组合的数据项都应具有相同的 bucket。
	Data map[string]float64 `json:"data"`
	// Unit 是数据的单位。注意，所有具有相同标签组合的数据项
	// 都应具有相同的单位。
	Unit string `json:"unit"`
	// Labels 是数据项的标签。
	Labels map[string]string `json:"labels,omitempty"`
}

// PerfData 包含当前测试中生成的所有数据项。
type PerfData struct {
	// Version 是指标的版本。指标消费者可以使用该版本
	// 检测指标版本变化，并决定支持哪个版本。
	Version   string     `json:"version"`
	DataItems []DataItem `json:"dataItems"`
	// Labels 是数据集的标签。
	Labels map[string]string `json:"labels,omitempty"`
}

func main() {
	err := run()
	if err != nil {
		panic(err)
	}
}

func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("output filename is a required argument")
	}
	benchmarkSet, err := benchparse.ParseSet(os.Stdin)
	if err != nil {
		return err
	}
	data := PerfData{Version: "v1"}
	for _, benchMarks := range benchmarkSet {
		for _, benchMark := range benchMarks {
			data.DataItems = appendIfMeasured(data.DataItems, benchMark, benchparse.NsPerOp, "time", "μs", benchMark.NsPerOp/1000.0)
			data.DataItems = appendIfMeasured(data.DataItems, benchMark, benchparse.MBPerS, "throughput", "MBps", benchMark.MBPerS)
			data.DataItems = appendIfMeasured(data.DataItems, benchMark, benchparse.AllocedBytesPerOp, "allocated", "bytes", float64(benchMark.AllocedBytesPerOp))
			data.DataItems = appendIfMeasured(data.DataItems, benchMark, benchparse.AllocsPerOp, "allocations", "1", float64(benchMark.AllocsPerOp))
			data.DataItems = appendIfMeasured(data.DataItems, benchMark, 0, "iterations", "1", float64(benchMark.N))
		}
	}
	output := &bytes.Buffer{}
	if err := json.NewEncoder(output).Encode(data); err != nil {
		return err
	}
	formatted := &bytes.Buffer{}
	if err := json.Indent(formatted, output.Bytes(), "", "  "); err != nil {
		return err
	}
	return os.WriteFile(os.Args[1], formatted.Bytes(), 0664)
}

func appendIfMeasured(items []DataItem, benchmark *benchparse.Benchmark, metricType int, metricName string, unit string, value float64) []DataItem {
	if metricType != 0 && (benchmark.Measured&metricType) == 0 {
		return items
	}
	return append(items, DataItem{
		Unit: unit,
		Labels: map[string]string{
			"benchmark":  benchmark.Name,
			"metricName": metricName},
		Data: map[string]float64{
			"value": value}})
}
