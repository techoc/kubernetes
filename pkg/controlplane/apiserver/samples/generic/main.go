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

// sample-generic-controlplane 是一个类似 kube 的通用控制面。
// 它与 kube-apiserver 兼容，但不包含容器领域相关的专用 API。
package main

import (
	"os"

	"k8s.io/component-base/cli"
	_ "k8s.io/component-base/logs/json/register"
	_ "k8s.io/component-base/metrics/prometheus/clientgo"
	_ "k8s.io/component-base/metrics/prometheus/version"
	"k8s.io/kubernetes/pkg/controlplane/apiserver/samples/generic/server"
)

// main 是 sample-generic-controlplane 的入口。
func main() {
	// 由 server 包构造命令，入口只负责执行并透传退出码。
	command := server.NewCommand()
	code := cli.Run(command)
	os.Exit(code)
}
