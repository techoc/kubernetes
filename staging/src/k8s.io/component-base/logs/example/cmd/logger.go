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

// Package main 是 component-base/logs 的可运行示例：
// 演示如何通过 cli.Run 串联 feature gate、日志配置 flag 与 contextual logging。
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/component-base/cli"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/component-base/logs/example"
	"k8s.io/klog/v2"

	_ "k8s.io/component-base/logs/json/register"
)

var featureGate = featuregate.NewFeatureGate()

// main 与真实组件一致：先注册特性门控，再交给 cli.Run 统一处理
// flag 解析、日志初始化和信号/退出码转换。
func main() {
	runtime.Must(logsapi.AddFeatureGates(featureGate))
	command := NewLoggerCommand()
	code := cli.Run(command)
	os.Exit(code)
}

// NewLoggerCommand 构造示例命令。
// 日志配置与 feature gate 的 flag 在构造时注册，实际生效则在 Run 中调用
// ValidateAndApply 完成，从而保证解析完成后再落配置。
func NewLoggerCommand() *cobra.Command {
	c := logsapi.NewLoggingConfiguration()
	cmd := &cobra.Command{
		Run: func(cmd *cobra.Command, args []string) {
			logs.InitLogs()
			if err := logsapi.ValidateAndApply(c, featureGate); err != nil {
				fmt.Fprintf(os.Stderr, "%v\n", err)
				os.Exit(1)
			}
			if len(args) > 0 {
				fmt.Fprintf(os.Stderr, "Unexpected additional command line arguments:\n    %s\n", strings.Join(args, "\n    "))
				os.Exit(1)
			}

			// 初始化上下文日志。
			// 命名 logger 并附加固定键值，展示如何通过 context 传递，而非依赖全局 logger。
			logger := klog.LoggerWithValues(klog.LoggerWithName(klog.Background(), "example"), "foo", "bar")
			ctx := klog.NewContext(context.Background(), logger)

			// 生成一些输出。
			example.Run(ctx)
		},
	}
	logsapi.AddFeatureGates(featureGate)
	featureGate.AddFlag(cmd.Flags())
	logsapi.AddFlags(c, cmd.Flags())
	return cmd
}
