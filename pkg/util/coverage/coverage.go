//go:build coverage

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

// Package coverage 为启用了覆盖率检测的二进制提供收集与刷写覆盖率信息的工具。
package coverage

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
)

var coverageFile string

// tempCoveragePath 返回写入覆盖率信息的临时文件路径。
// 临时文件与目标文件位于同一目录，确保 os.Rename 能正常工作。
func tempCoveragePath() string {
	return coverageFile + ".tmp"
}

// InitCoverage 由占位单元测试调用，用于初始化 Go 的覆盖率框架。
// 普通调用方不应直接使用它。
func InitCoverage(name string) {
	// 目标路径来自 KUBE_COVERAGE_FILE；未设置时回退到 /tmp 下的默认路径。
	coverageFile = os.Getenv("KUBE_COVERAGE_FILE")
	if coverageFile == "" {
		coverageFile = "/tmp/k8s-" + name + ".cov"
	}
	fmt.Println("Dumping coverage information to " + coverageFile)

	flushInterval := 5 * time.Second
	requestedInterval := os.Getenv("KUBE_COVERAGE_FLUSH_INTERVAL")
	if requestedInterval != "" {
		if duration, err := time.ParseDuration(requestedInterval); err == nil {
			flushInterval = duration
		} else {
			panic("Invalid KUBE_COVERAGE_FLUSH_INTERVAL value; try something like '30s'.")
		}
	}

	// 伪装成带 -test.coverprofile 参数启动，激活测试框架的覆盖率输出。
	flag.CommandLine.Parse([]string{"-test.coverprofile", tempCoveragePath()})

	// 启动周期性刷写
	go wait.Forever(FlushCoverage, flushInterval)
}

// FlushCoverage 将收集到的覆盖率信息刷写到磁盘。
// 目标文件在启动时确定，之后不可更改。
// 调用该函数还会向标准输出打印类似 "coverage: 5% of statements" 的一行。
func FlushCoverage() {
	// 实际上没有测试可跑，但必须让 Go 以为跑过测试才会落盘覆盖率数据，
	// 因此构造一批空的测试套件并“运行”它们。
	tests := []testing.InternalTest{}
	benchmarks := []testing.InternalBenchmark{}
	examples := []testing.InternalExample{}
	fuzztargets := []testing.InternalFuzzTarget{}

	var deps fakeTestDeps

	dummyRun := testing.MainStart(deps, tests, benchmarks, fuzztargets, examples)
	dummyRun.Run()

	// 写入临时文件后再移动到目标路径，使并发读取方看到的是原子替换。
	if err := os.Rename(tempCoveragePath(), coverageFile); err != nil {
		klog.Errorf("Couldn't move coverage file from %s to %s", coverageFile, tempCoveragePath())
	}
}
