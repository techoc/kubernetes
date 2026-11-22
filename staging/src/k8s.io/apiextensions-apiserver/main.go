/*
Copyright 2017 The Kubernetes Authors.

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

	"k8s.io/apiextensions-apiserver/pkg/cmd/server"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
)

// main 是 apiextensions-apiserver 的入口点。
// 作为一个独立进程运行，提供 CRD（Custom Resource Definition）API。
// Kubernetes 主 API Server 通过 --enable-aggregator-routing 或 --api-audiences 与之集成。
func main() {
	// SetupSignalContext 创建响应 SIGTERM/SIGINT 的 context，用于优雅关闭
	ctx := genericapiserver.SetupSignalContext()
	cmd := server.NewServerCommand(ctx, os.Stdout, os.Stderr)
	code := cli.Run(cmd)
	os.Exit(code)
}
