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

package main

import (
	"os"

	"k8s.io/component-base/cli"
	_ "k8s.io/component-base/logs/json/register" // 注册 JSON 日志格式
	"k8s.io/pod-security-admission/cmd/webhook/server"
)

// 本文件只做进程入口：命令构建交给 server 包，cli.Run 负责信号处理与错误输出。
// 用返回的 code 退出而非直接返回，是为了让 webhook 的退出状态对部署方可见。
func main() {
	command := server.NewServerCommand()
	code := cli.Run(command)
	os.Exit(code)
}
