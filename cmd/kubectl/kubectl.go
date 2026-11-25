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
	"k8s.io/component-base/logs"
	"k8s.io/kubectl/pkg/cmd"
	"k8s.io/kubectl/pkg/cmd/util"

	// 导入以初始化客户端认证插件。
	_ "k8s.io/client-go/plugin/pkg/client/auth"
)

func main() {
	// 我们需要在此手动解析参数、查找详细级别 flag 并设置相应级别，
	// 因为正常流程中包括日志详细级别在内的 flag 解析发生在
	// cli.RunNoErrOutput 内部。在此处理可确保在构造 kubectl 命令期间
	// 仍能使用 klog，例如处理插件和解析 .kuberc 文件时。
	logs.GlogSetter(cmd.GetLogVerbosity(os.Args)) // nolint:errcheck
	command := cmd.NewDefaultKubectlCommand()
	if err := cli.RunNoErrOutput(command); err != nil {
		// 友好地打印错误并以错误状态退出。
		util.CheckErr(err)
	}
}
