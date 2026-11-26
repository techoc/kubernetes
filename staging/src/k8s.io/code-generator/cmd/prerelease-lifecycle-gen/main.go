/*
Copyright 2020 The Kubernetes Authors.

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

// // prerelease-lifecycle-gen 自动生成 api-status.csv 文件。
//
// // 给定输入目录，它为所有 beta API 生成 zz_api_status.go，记录各 Kind
// // 的引入版本、计划废弃版本与计划移除版本。
//
// // 生成由源码注释标签控制。包只需在某个文件的文件注释中写入：
// 生成由源码注释标签控制。包只需在某个文件的文件注释中写入：
// one file, of the form:
//
//	// +k8s:prerelease-lifecycle-gen=true
//
// // +k8s:prerelease-lifecycle-gen:introduced=1.19
// // +k8s:prerelease-lifecycle-gen:deprecated=1.22
// // +k8s:prerelease-lifecycle-gen:removed=1.25
// // +k8s:prerelease-lifecycle-gen:replacement=wardle.example.com,v1,Flunder
//
// // 注册是整包级选项，不能对单个类型开启。
// 注册是整包级选项，不能对单个类型开启。
package main

import (
	"flag"

	"github.com/spf13/pflag"
	"k8s.io/code-generator/cmd/prerelease-lifecycle-gen/args"
	statusgenerators "k8s.io/code-generator/cmd/prerelease-lifecycle-gen/prerelease-lifecycle-generators"
	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/generator"
	"k8s.io/klog/v2"
)

// main 是 prerelease-lifecycle-gen 的入口：解析 flag → 校验 → 交给 gengo 执行。
// 项目文档注释（上方包注释）说明标签语法；这里只做标准的生成器装配，
// 具体目标与命名在 prerelease-lifecycle-generators 包中实现。
func main() {
	klog.InitFlags(nil)
	args := args.New()

	args.AddFlags(pflag.CommandLine)
	// klog 的 flag 注册在标准库 flag 上，需要显式并入 pflag 才能被统一解析。
	flag.Set("logtostderr", "true")
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()

	if err := args.Validate(); err != nil {
		klog.Fatalf("Error: %v", err)
	}

	myTargets := func(context *generator.Context) []generator.Target {
		return statusgenerators.GetTargets(context, args)
	}

	// // 执行生成流程。
	if err := gengo.Execute(
		statusgenerators.NameSystems(),
		statusgenerators.DefaultNameSystem(),
		myTargets,
		gengo.StdBuildTag,
		pflag.Args(),
	); err != nil {
		klog.Fatalf("Error: %v", err)
	}
	klog.V(2).Info("Completed successfully.")
}
