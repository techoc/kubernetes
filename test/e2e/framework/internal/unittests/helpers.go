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

package unittests

import (
	"bytes"
	"flag"
	"testing"

	"github.com/stretchr/testify/require"
	"k8s.io/kubernetes/test/e2e/framework"
)

// GetFrameworkOutput 捕获测试套件初始化期间写入 framework.Output 的内容，
// 连同显式 Exit 调用的代码一起返回；没有调用则返回 -1。
// 每个测试二进制只能调用一次。
func GetFrameworkOutput(t *testing.T, flags map[string]string) (output string, finalExitCode int) {
	// 模拟 test/e2e 使用 framework 以及用户调用 test/e2e 的方式。
	framework.RegisterCommonFlags(flag.CommandLine)
	framework.RegisterClusterFlags(flag.CommandLine)
	for flagname, value := range flags {
		require.NoError(t, flag.Set(flagname, value), "set %s", flagname)
	}
	var buffer bytes.Buffer
	framework.Output = &buffer
	framework.Exit = func(code int) {
		panic(exitCode(code))
	}
	finalExitCode = -1
	defer func() {
		if r := recover(); r != nil {
			if code, ok := r.(exitCode); ok {
				finalExitCode = int(code)
			} else {
				panic(r)
			}
		}
		output = buffer.String()
	}()
	framework.AfterReadingAllFlags(&framework.TestContext)

	// 结果由 defer 设置。
	return
}

type exitCode int
