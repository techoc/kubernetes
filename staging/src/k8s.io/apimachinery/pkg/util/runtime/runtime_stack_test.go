/*
Copyright 2014 The Kubernetes Authors.

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

package runtime

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"regexp"

	"k8s.io/klog/v2"
)

// ExampleHandleErrorWithContext 演示 HandleErrorWithContext 的错误日志输出：
// 日志应带有 "UnhandledError" logger，并包含调用点与上下文中的结构化字段。
// 该示例也是回归测试：若输出格式或调用点发生变化，示例输出比对会失败。
//
//nolint:logcheck // Several functions are normally not okay in a package.
func ExampleHandleErrorWithContext() {
	// 捕获并随后恢复 klog 全局状态，避免污染其他测试。
	state := klog.CaptureState()
	defer state.Restore()
	var fs flag.FlagSet
	klog.InitFlags(&fs)
	// 强制单文件输出且不写 stderr，使 buffer 能稳定捕获日志。
	for flag, value := range map[string]string{
		"one_output":  "true",
		"logtostderr": "false",
	} {
		if err := fs.Set(flag, value); err != nil {
			fmt.Printf("Unexpected error configuring klog: %v", err)
			return
		}
	}
	var buffer bytes.Buffer
	klog.SetOutput(&buffer)

	// 构造带 request=42 字段的 logger，并放入 context 中传给调用。
	logger := klog.Background()
	logger = klog.LoggerWithValues(logger, "request", 42)
	ctx := klog.NewContext(context.Background(), logger)

	// 下一行必须固定在第 60 行（示例输出断言该位置）；改动上方代码需相应增删空行。
	HandleErrorWithContext(ctx, errors.New("fake error"), "test")

	klog.Flush()
	// 去掉随环境变化的头部前缀，只保留需要断言的代码位置。
	output := buffer.String()
	output = regexp.MustCompile(`^.* ([^[:space:]]*.go:[[:digit:]]*)\] `).ReplaceAllString(output, `xxx $1] `)
	fmt.Print(output)

	// Output:
	// xxx runtime_stack_test.go:60] "test" err="fake error" logger="UnhandledError" request=42
}
