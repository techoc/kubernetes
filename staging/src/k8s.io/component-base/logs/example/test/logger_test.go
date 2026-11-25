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

// Package logger_test 演示如何让单元测试输出按用例隔离；
// 前提是被测代码使用上下文日志，并能通过 ktesting 注入测试 logger。
package logger_test

import (
	"flag"
	"os"
	"testing"

	"k8s.io/component-base/logs/example"
	"k8s.io/klog/v2/ktesting"
	// 导入此包可为每个测试的输出添加命令行 flag，默认详细度为 5。
	// 本示例改为通过 TestMain 降低默认详细度。
	// "k8s.io/klog/v2/ktesting/init"
)

// TestLogger 并行运行同一逻辑两次，验证输出会分别归属到各自的子测试。
func TestLogger(t *testing.T) {
	// 两个并行的子测试各自产生日志，ktesting 会把日志关联到对应 t。
	for _, testcase := range []string{"abc", "xyz"} {
		t.Run(testcase, func(t *testing.T) {
			t.Parallel()
			_ /* logger */, ctx := ktesting.NewTestContext(t)
			example.Run(ctx)
		})
	}
}

// TestMain 把测试日志默认详细度调低到 2，与生产默认级别一致；
// 用户仍可通过 -testing.v / -testing.vmodule 覆盖。
func TestMain(m *testing.M) {
	// 以详细度 2 运行，这是生产环境的默认日志级别。
	// 此处也可以自定义 flag 名称，默认名称为 -testing.v 和 -testing.vmodule。
	ktesting.DefaultConfig = ktesting.NewConfig(ktesting.Verbosity(2))
	ktesting.DefaultConfig.AddFlags(flag.CommandLine)
	flag.Parse()
	os.Exit(m.Run())
}
