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

// applyconfiguration-gen 为带 +genclient 的 API 类型生成 ApplyConfiguration 类型及其 With/WithXxx 构造方法，使客户端可以以声明式方式构造 apply 请求体。
//
// applyconfiguration-gen 为带 +genclient 的 API 类型生成 ApplyConfiguration
// 类型及其 With/WithXxx 构造方法，使客户端可以以声明式方式构造 apply 请求体。
package main

import (
	"flag"

	"github.com/spf13/pflag"
	"k8s.io/code-generator/cmd/applyconfiguration-gen/args"
	"k8s.io/code-generator/cmd/applyconfiguration-gen/generators"
	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/generator"
	"k8s.io/klog/v2"
)

func main() {
	klog.InitFlags(nil)
	// 默认输入路径指向 k8s 自己的内部 API 包；调用方（例如 make update）会显式覆盖。
	args := args.New()
	args.AddFlags(pflag.CommandLine, "k8s.io/kubernetes/pkg/apis") // TODO: move this input path out of applyconfiguration-gen
	// gengo 与 klog 都注册到标准 flag，需要显式打开 stderr 日志，否则生成失败时没有任何输出。
	if err := flag.Set("logtostderr", "true"); err != nil {
		klog.Fatalf("Error: %v", err)
	}
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()

	// 先校验参数再加载包，避免扫描完整个 API 树之后才发现参数错误。
	if err := args.Validate(); err != nil {
		klog.Fatalf("Error: %v", err)
	}

	// gengo 以回调方式获取 target，保证 context（类型宇宙）加载完成后再构造生成器。
	myTargets := func(context *generator.Context) []generator.Target {
		return generators.GetTargets(context, args)
	}

	// // 执行生成流程。
	if err := gengo.Execute(
		generators.NameSystems(),
		generators.DefaultNameSystem(),
		myTargets,
		gengo.StdBuildTag,
		pflag.Args(),
	); err != nil {
		klog.Fatalf("Error: %v", err)
	}
	klog.V(2).Info("Completed successfully.")
}
