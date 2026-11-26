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

// // defaulter-gen 自动生成 Defaulter 函数。给定输入目录，它扫描顶层类型，
//
// // 汇总对象树里的 SetDefault_* 方法，为整个对象生成高效的默认值函数。
// // 生成行为由源码注释标签控制。包可在包注释中写入：
// 汇总对象树里的 SetDefault_* 方法，生成整个对象的默认值函数。
//
// // 生成由源码注释标签控制。包只需在某个文件的文件注释中写入：
// 包可在包注释中写入一个或多个标签，
// 请求生成默认值函数：
//
//	// +k8s:defaulter-gen=<field-name-to-flag>
//
// // 为包含该字段名的类型生成默认值函数（前提是该类型有 defaulter）。类型可用
// // 以下标签显式开关默认化：
// 可用以下标签显式开关默认化：
//
//	// +k8s:defaulter-gen=true|false
//
// // 已有的 defaulter 方法（SetDefaults_TYPE）可标注：
// 可标注如下标签：
//
//	// +k8s:defaulter-gen=covers
//
// // 表示该方法不调用、也不应调用任何嵌套的 defaulter。
// 嵌套的 defaulter。
//
// 中文概述：defaulter-gen 扫描顶层类型，汇总其对象树中所有 SetDefault_* 方法，
// 为整个对象生成高效的 SetObjectDefaults_* 函数。生成由包级
// 指令 +k8s:defaulter-gen=<field-name> 标签、类型级 =true|false 标签以及
// SetDefaults_* 上的 =covers 标签共同控制。
package main

import (
	"flag"

	"github.com/spf13/pflag"
	"k8s.io/code-generator/cmd/defaulter-gen/args"
	"k8s.io/code-generator/cmd/defaulter-gen/generators"
	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/generator"
	"k8s.io/klog/v2"
)

// main 解析参数、校验后交给 gengo 执行 defaulter-gen 的 targets。
func main() {
	// klog 与 pflag 共用同一个 FlagSet，保持命令行行为与其他生成器一致。
	klog.InitFlags(nil)
	args := args.New()

	args.AddFlags(pflag.CommandLine)
	flag.Set("logtostderr", "true")
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()

	// 参数非法立即退出，避免产生半成品生成文件。
	if err := args.Validate(); err != nil {
		klog.Fatalf("Error: %v", err)
	}

	// targets 需要 universe 信息，因此延迟到 gengo 构建 Context 之后。
	myTargets := func(context *generator.Context) []generator.Target {
		return generators.GetTargets(context, args)
	}

	// // 执行生成流程。
	if err := gengo.Execute(
		generators.NameSystems(),
		generators.DefaultNameSystem(),
		myTargets,
		args.GeneratedBuildTag,
		pflag.Args(),
	); err != nil {
		klog.Fatalf("Error: %v", err)
	}
	klog.V(2).Info("Completed successfully.")
}
