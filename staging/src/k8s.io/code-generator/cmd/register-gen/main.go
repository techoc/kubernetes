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

// register-gen 是 gengo 驱动的代码生成器，为 API 包生成 scheme 注册代码。
//
// 典型用法：register-gen --output-file zz_generated.register.go \
// --go-header-file hack/boilerplate/boilerplate.generatego.txt \
// k8s.io/api/core/v1
package main

import (
	"flag"

	"github.com/spf13/pflag"
	"k8s.io/code-generator/cmd/register-gen/args"
	"k8s.io/code-generator/cmd/register-gen/generators"
	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/generator"
	"k8s.io/klog/v2"
)

func main() {
	// 先初始化 klog 并注册参数，再把它们并入 pflag，统一由 pflag 解析。
	klog.InitFlags(nil)
	args := args.New()
	args.AddFlags(pflag.CommandLine)
	flag.Set("logtostderr", "true")
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)

	pflag.Parse()
	if err := args.Validate(); err != nil {
		klog.Fatalf("Error: %v", err)
	}

	// 延迟到 gengo 加载完 universe 后再构造生成目标。
	myTargets := func(context *generator.Context) []generator.Target {
		return generators.GetTargets(context, args)
	}

	// pflag.Args() 是待处理的输入包路径列表；StdBuildTag 使用标准生成标记。
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
