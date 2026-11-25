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

// APIServer 是集群的主 API 服务器，负责提供集群管理 API。
package main

import (
	"os"
	_ "time/tzdata" // 为 CronJob 提供时区数据

	"k8s.io/component-base/cli"
	_ "k8s.io/component-base/logs/json/register"          // 注册 JSON 日志格式
	_ "k8s.io/component-base/metrics/prometheus/clientgo" // 加载全部 prometheus client-go 插件
	_ "k8s.io/component-base/metrics/prometheus/version"  // 注册版本指标
	"k8s.io/kubernetes/cmd/kube-apiserver/app"
)

func main() {
	command := app.NewAPIServerCommand()
	code := cli.Run(command)
	os.Exit(code)
}
