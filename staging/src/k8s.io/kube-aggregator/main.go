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
	"k8s.io/kube-aggregator/pkg/cmd/server"

	// force compilation of packages we'll later rely upon
	// 这些包通过空导入引入，只为触发其 init：把 API 类型注册进 scheme、注册校验函数、
	// 以及让生成的 client/lister 代码参与编译。等到服务真正运行时（例如 scheme 反序列化、
	// RESTMapper 映射资源时）才依赖这些副作用，提前编译能保证缺失问题在构建期暴露。
	_ "k8s.io/kube-aggregator/pkg/apis/apiregistration/install"
	_ "k8s.io/kube-aggregator/pkg/apis/apiregistration/validation"
	_ "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset"
	_ "k8s.io/kube-aggregator/pkg/client/listers/apiregistration/v1"
	_ "k8s.io/kube-aggregator/pkg/client/listers/apiregistration/v1beta1"
)

// main 是 kube-aggregator 的进程入口：先把信号上下文、标准输出/错误流接好，
// 再交给 server 包构造 cobra 命令并运行。命令返回码由 os.Exit 透出，
// 让上层（如 systemd）能据此判断进程是否正常退出。
func main() {
	ctx := genericapiserver.SetupSignalContext()
	options := server.NewDefaultOptions(os.Stdout, os.Stderr)
	cmd := server.NewCommandStartAggregator(ctx, options)
	code := cli.Run(cmd)
	os.Exit(code)
}
