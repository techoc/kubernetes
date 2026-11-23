/*
Copyright 2016 The Kubernetes Authors.

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

	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/component-base/cli"
	"k8s.io/sample-apiserver/pkg/cmd/server"
)

// 可执行文件入口只做组装：信号处理、选项与命令构造都在 pkg 里，便于单独测试。
func main() {
	// 用信号上下文贯穿启动流程，收到终止信号时能优雅关闭服务器。
	ctx := genericapiserver.SetupSignalContext()
	options := server.NewWardleServerOptions(os.Stdout, os.Stderr)
	// false 表示这不是 in-process 启动，命令退出码直接透传给操作系统。
	cmd := server.NewCommandStartWardleServer(ctx, options, false)
	code := cli.Run(cmd)
	os.Exit(code)
}
