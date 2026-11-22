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

// Command informer-gen 根据 API 类型生成 shared informer 与 informer factory。
package main

import (
	"flag"

	"github.com/spf13/pflag"
	"k8s.io/code-generator/cmd/informer-gen/args"
	"k8s.io/code-generator/cmd/informer-gen/generators"
	"k8s.io/code-generator/pkg/util"
	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/generator"
	"k8s.io/klog/v2"
)

// main 解析参数、校验后交给 gengo 执行 informer-gen 的 targets。
// informer 的名称依赖复数规则，因此需要把 --plural-exceptions 转成 map 后
// 传给命名系统。
func main() {
	// klog 与 pflag 共用同一个 FlagSet。
	klog.InitFlags(nil)
	args := args.New()

	args.AddFlags(pflag.CommandLine)
	flag.Set("logtostderr", "true")
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()

	// 参数非法立即退出。
	if err := args.Validate(); err != nil {
		klog.Fatalf("Error: %v", err)
	}

	// targets 依赖 universe 信息，延迟到 Context 构建后执行。
	myTargets := func(context *generator.Context) []generator.Target {
		return generators.GetTargets(context, args)
	}

	// Run it.
	if err := gengo.Execute(
		// 复数例外表影响生成的 informer/client 名称，必须在此注入。
		generators.NameSystems(util.PluralExceptionListToMapOrDie(args.PluralExceptions)),
		generators.DefaultNameSystem(),
		myTargets,
		gengo.StdBuildTag,
		pflag.Args(),
	); err != nil {
		klog.Fatalf("Error: %v", err)
	}
	klog.V(2).Info("Completed successfully.")
}
