/*
Copyright 2015 The Kubernetes Authors.

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

// client-gen makes the individual typed clients using gengo.
package main

import (
	"flag"
	"slices"

	"github.com/spf13/pflag"
	"k8s.io/klog/v2"

	"k8s.io/code-generator/cmd/client-gen/args"
	"k8s.io/code-generator/cmd/client-gen/generators"
	"k8s.io/code-generator/pkg/util"
	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/generator"
)

func main() {
	klog.InitFlags(nil)
	args := args.New()

	args.AddFlags(pflag.CommandLine, "k8s.io/kubernetes/pkg/apis") // TODO: move this input path out of client-gen
	flag.Set("logtostderr", "true")
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()

	// 把每个 group/version 包作为 gengo 的输入目录。
	// 输入列表直接决定生成哪些类型的 typed client，因此必须覆盖 --groups 的全部版本。
	inputPkgs := []string{}
	for _, pkg := range args.Groups {
		for _, v := range pkg.Versions {
			inputPkgs = append(inputPkgs, v.Package)
		}
	}
	// 对输入包排序，保证 import 别名分配与生成文件内容在多次运行间稳定。
	// 否则 map 迭代顺序会让输出在不同机器上产生无意义 diff。
	slices.Sort(inputPkgs)

	if err := args.Validate(); err != nil {
		klog.Fatalf("Error: %v", err)
	}

	// 目标在 gengo 构建完类型上下文后才创建，因为生成器需要完整的类型信息。
	myTargets := func(context *generator.Context) []generator.Target {
		return generators.GetTargets(context, args)
	}

	// 执行生成。复数形式的例外表用于把 Kind 正确映射为资源名（如 Ingress -> ingresses）。
	if err := gengo.Execute(
		generators.NameSystems(util.PluralExceptionListToMapOrDie(args.PluralExceptions)),
		generators.DefaultNameSystem(),
		myTargets,
		gengo.StdBuildTag,
		inputPkgs,
	); err != nil {
		klog.Fatalf("Error: %v", err)
	}
}
