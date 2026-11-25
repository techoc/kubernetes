/*
Copyright 2023 The Kubernetes Authors.

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

// stdlib 示例演示如何同时使用标准库 log 风格的 klog 与 component-base 的
// 日志配置 API，包括特性门控驱动的上下文日志。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/component-base/logs/example"
	"k8s.io/klog/v2"

	_ "k8s.io/component-base/logs/json/register"
)

func main() {
	// 设置命令行，包括特性门控参数和日志选项。
	// 使用独立的 FeatureGate 实例而非全局默认值，示例之间互不影响。
	featureGate := featuregate.NewFeatureGate()
	runtime.Must(logsapi.AddFeatureGates(featureGate))
	flag.Var(featureGate, "feature-gate",
		"A set of key=value pairs that describe feature gates for alpha/experimental features. "+
			"Options are:\n"+strings.Join(featureGate.KnownFeatures(), "\n"))
	c := logsapi.NewLoggingConfiguration()
	logsapi.AddGoFlags(c, flag.CommandLine)

	// 解析 flag 并应用结果。日志上下文功能仍为 alpha 时，logs.InitLogs 会禁用它，
	// 必须通过特性门控参数显式启用。
	flag.Parse()
	logs.InitLogs()
	if err := logsapi.ValidateAndApply(c, featureGate); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}
	// 示例程序不接受位置参数，出现即说明调用方式有误，直接退出。
	args := flag.CommandLine.Args()
	if len(args) > 0 {
		fmt.Fprintf(os.Stderr, "Unexpected additional command line arguments:\n    %s\n", strings.Join(args, "\n    "))
		os.Exit(1)
	}

	// 初始化上下文日志。
	// 给 logger 取名字并附加固定字段，演示回调和调用链中的上下文传递。
	logger := klog.LoggerWithValues(klog.LoggerWithName(klog.Background(), "example"), "foo", "bar")
	ctx := klog.NewContext(context.Background(), logger)

	// 生成一些输出。
	example.Run(ctx)
}
