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

package main

import (
	"os"

	"k8s.io/component-base/cli"
	_ "k8s.io/component-base/logs/json/register" // 注册 JSON 日志格式
	_ "k8s.io/component-base/metrics/prometheus/clientgo"
	_ "k8s.io/component-base/metrics/prometheus/version" // 注册版本指标
	"k8s.io/kubernetes/cmd/kube-scheduler/app"
)

// main 是 kube-scheduler 进程入口：将命令构造、参数解析和执行委托给 component-base CLI。
func main() {
	// 构造 Scheduler 的 Cobra 命令，集中注册参数、帮助信息和执行逻辑。
	command := app.NewSchedulerCommand()
	// 由统一 CLI 执行器处理命令，并返回符合操作系统约定的退出码。
	code := cli.Run(command)
	// 把退出码交给操作系统，便于脚本和进程管理器判断启动结果。
	os.Exit(code)
}
