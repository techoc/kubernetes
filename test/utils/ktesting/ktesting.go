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

package ktesting

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"

	"k8s.io/klog/v2"
	_ "k8s.io/kubernetes/test/utils/ktesting/format" // 在 Gomega 中启用 YAML 格式。
)

var klogVFlag *flag.Flag
var haveVerbosityFromEnv = false

func init() {
	// 在全局命令行中注册 "v" 和 "vmodule"。
	//
	// 这正是 ktesting 有主张的地方：所有使用它的测试
	// 都应该有这些 flag，而单元测试作者无需
	// 手动申请。
	var klogFlags flag.FlagSet
	klog.InitFlags(&klogFlags)
	klogVFlag = klogFlags.Lookup("v")
	for _, name := range []string{"v", "vmodule"} {
		f := klogFlags.Lookup(name)

		// 其他代码可能已经这样做了，
		// 例如 ./staging/src/k8s.io/component-base/logs/testinit/testinit.go。
		//
		// 根据 https://tip.golang.org/doc/go1.21#language，
		// k8s.io/component-base 的 init 保证会在
		// 我们的 init 之前运行，因为该包名排序更靠前。
		// 通过在这里检查 flag 是否已经添加，
		// ktesting 无论是否使用 component-base/logs/testinit 都能工作。
		//
		// 我们可以要求测试二进制文件只使用 ktesting
		// 或 testinit 之一，但当前做法更灵活。
		if flag.CommandLine.Lookup(name) != nil {
			continue
		}

		flag.CommandLine.Var(f.Value, name, f.DefValue)
	}

	// 运行大量测试的 CI 任务不能假定
	// 所有测试都有这些命令行 flag。
	// 它们可以设置 KTESTING_VERBOSITY 环境变量，
	// 以改变那些使用 ktesting 的测试的默认详细程度，
	// 同时不影响不使用它的测试。
	if v, ok := os.LookupEnv("KTESTING_VERBOSITY"); ok {
		haveVerbosityFromEnv = true
		if err := klogVFlag.Value.Set(v); err != nil {
			panic(fmt.Sprintf("KTESTING_VERBOSITY: %v", err))
		}
	}
}

// 可在 init 期间调用 SetDefaultVerbosity，以修改程序
// 默认的日志详细程度。如果设置了 KTESTING_VERBOSITY 环境变量，
// 则使用该变量的值。
//
// 注意，这会立即重新配置 klog 的详细程度，而且在 flag 解析之前
// 就已生效。如果详细程度非零，且在 init 期间调用了
// SetDefaultVerbosity，那么其他 init 函数可能会开始输出日志，
// 而通常情况下它们不会记录任何内容。如果出现这种情况，
// 正确的做法是移除那些日志调用，因为不鼓励在 init 期间记录日志。
// 这会导致输出不可预测（init 顺序未指定），和/或毫无用处
// （init 期间日志尚未初始化，因此条件日志输出会被
// 省略）。
func SetDefaultVerbosity(v int) {
	if !haveVerbosityFromEnv {
		_ = klogVFlag.Value.Set(fmt.Sprintf("%d", v))
	}
}

// NewTestContext 是 ktesting.NewTestContext 的直接替代品。
//
// 结果可以转换为 TContext，并具有相同的功能
// （例如，在测试结束时会被取消）。
// 返回类型设为 context.Context，以免破坏期望该类型的代码。
func NewTestContext(tb testing.TB) (klog.Logger, context.Context) {
	tCtx := Init(tb)
	return tCtx.Logger(), tCtx
}
