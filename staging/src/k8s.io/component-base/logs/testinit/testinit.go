/*
Copyright 2021 The Kubernetes Authors.

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

// Package testinit 在初始化期间为 Ginkgo 或 Go 测试程序添加日志 flag，
// 而 logs 包本身已不再执行此操作。
//
// 普通命令不应使用本包，应通过 logs.Options 和/或 cli.Run 管理日志 flag。
package testinit

import (
	"flag"

	"k8s.io/component-base/logs"
)

// init 在包导入时把 klog 的 Go flag 绑定到标准库 flag.CommandLine，
// 让测试程序无需显式初始化即可使用 -v、-vmodule 等日志参数。
// 放在 init 中是因为 Go 测试框架和 Ginkgo 都会在 main 之前解析 flag.CommandLine。
func init() {
	logs.AddGoFlags(flag.CommandLine)
}
