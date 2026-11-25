/*
Copyright 2025 The Kubernetes Authors.

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

// nettesting 包提供网络功能测试工具。
// 不要在生产代码中使用这些工具，因为它们尚未经过安全审查。
package nettesting

import (
	"os"
	goruntime "runtime"
	"testing"
)

// MakeSocketNameForTest 返回供测试期间使用的 socket 名称。
// 在支持抽象套接字的操作系统上，名称会加上 `@` 前缀以成为抽象套接字（不落到文件系统，也就不会残留）；
// 在不支持抽象套接字的系统（darwin、windows）上，名称被当作文件名，并注册 cleanup 钩子在测试结束时删除它。
func MakeSocketNameForTest(t testing.TB, name string) string {
	var sockname = name
	switch goruntime.GOOS {
	case "darwin", "windows":
		t.Cleanup(func() { _ = os.Remove(sockname) })
	default:
		sockname = "@" + name
	}
	return sockname
}
