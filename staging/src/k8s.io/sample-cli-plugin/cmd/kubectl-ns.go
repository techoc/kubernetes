/*
Copyright 2018 The Kubernetes Authors.

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

// 本文件是 kubectl-ns 插件的进程入口，只负责把标准输入输出接到命令实现上。
// 之所以在这里自建 FlagSet 并替换 pflag.CommandLine，是为了让插件拥有独立的参数解析命名空间，
// 避免和宿主进程可能引入的其他 flag 定义相互干扰。

package main

import (
	"os"

	"github.com/spf13/pflag"

	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/sample-cli-plugin/pkg/cmd"
)

// main 把命令行执行的生命周期收敛到一次 Execute 调用：
// 命令自身决定如何解析参数、输出提示，这里只把失败映射为非零退出码交给 shell 感知。
func main() {
	flags := pflag.NewFlagSet("kubectl-ns", pflag.ExitOnError)
	pflag.CommandLine = flags

	root := cmd.NewCmdNamespace(genericiooptions.IOStreams{In: os.Stdin, Out: os.Stdout, ErrOut: os.Stderr})
	if err := root.Execute(); err != nil {
		// 命令层已经输出了具体原因，重复打印只会制造噪声，直接退出即可。
		os.Exit(1)
	}
}
