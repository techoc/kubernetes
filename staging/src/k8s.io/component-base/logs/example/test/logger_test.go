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

// Package logger_test demonstrates how to write unit tests with per-test
// output. Per-test output only works for code which supports contextual
// logging.
// Package logger_test 演示如何让单元测试输出按用例隔离；
// 前提是被测代码使用上下文日志，并能通过 ktesting 注入测试 logger。
package logger_test

import (
	"flag"
	"os"
	"testing"

	"k8s.io/component-base/logs/example"
	"k8s.io/klog/v2/ktesting"
	// This import could be used to add command line flags for per-test
	// output with a default verbosity of 5. This example instead
	// uses a TestMain where the default verbosity gets lowered.
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
	// Run with verbosity 2, this is the default log level in production.
	// The name of the flags also could be customized here. The defaults
	// are -testing.v and -testing.vmodule.
	ktesting.DefaultConfig = ktesting.NewConfig(ktesting.Verbosity(2))
	ktesting.DefaultConfig.AddFlags(flag.CommandLine)
	flag.Parse()
	os.Exit(m.Run())
}
