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

// defaulter-gen is a tool for auto-generating Defaulter functions.
//
// Given a list of input directories, it will scan for top level types
// and generate efficient defaulters for an entire object from the sum
// of the SetDefault_* methods contained in the object tree.
//
// Generation is governed by comment tags in the source.  Any package may
// request defaulter generation by including one or more comment tags at
// the package comment level:
//
//	// +k8s:defaulter-gen=<field-name-to-flag>
//
// which will create defaulters for any type that contains the provided
// field name (if the type has defaulters). Any type may request explicit
// defaulting by providing the comment tag:
//
//	// +k8s:defaulter-gen=true|false
//
// An existing defaulter method (`SetDefaults_TYPE`) can provide the
// comment tag:
//
//	// +k8s:defaulter-gen=covers
//
// to indicate that the defaulter does not or should not call any nested
// defaulters.
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

	// Run it.
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
