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

// The kubelet binary is responsible for maintaining a set of containers on a particular host VM.
// It syncs data from both configuration file(s) as well as from a quorum of etcd servers.
// It then communicates with the container runtime (or a CRI shim for the runtime) to see what is
// currently running.  It synchronizes the configuration data, with the running set of containers
// by starting or stopping containers.
package main

import (
	"context"
	"os"

	"k8s.io/component-base/cli"
	_ "k8s.io/component-base/logs/json/register"          // for JSON log format registration
	_ "k8s.io/component-base/metrics/prometheus/clientgo" // for client metric registration
	_ "k8s.io/component-base/metrics/prometheus/version"  // for version metric registration
	"k8s.io/kubernetes/cmd/kubelet/app"
)

// main 是 Kubelet 进程入口：构造命令、解析参数，并把实际启动交给 app 包。
func main() {
	// 把进程上下文传入命令构造过程，供启动日志和初始化步骤使用。
	command := app.NewKubeletCommand(context.Background())
	// 统一由 component-base CLI 执行命令并返回标准退出码。
	code := cli.Run(command)
	// 将启动结果交给操作系统，供服务管理器或 shell 判断执行状态。
	os.Exit(code)
}
