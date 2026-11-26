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

// conversion-gen 自动生成 internal 与 external 类型之间的转换函数。
// 一次转换生成任务涉及三组包：(1) 若干包含 internal 类型的包，(2) 一个包含
// external 类型的包，(3) 一个目标包（生成的转换函数与开发者手写的转换函数
// 都放在这里）。包含 internal 类型的包在 Kubernetes 通用代码生成框架中
// 被称为“peer packages”。
//
// 对每个转换任务，`conversion-gen` 生成能在（internal, external）两个包的
// 同名类型之间高效转换的函数，包括如下形式的函数：
//
//	autoConvert_<pkg1>_<type>_To_<pkg2>_<type>
//
// （pkg1,pkg2）既取 (internal,external)，也取 (external,internal)。
// 生成的函数按数据结构递归转换：结构体按字段名匹配源字段与目标字段；
// 字段缺失或类型根本不兼容时，autoConvert_... 中只留下针对该字段的警告注释。
// 能用普通赋值的地方就直接赋值；复合类型则调用子类型的 `Convert...` 函数。
//
// 每对类型还会在同时满足以下条件时生成如下函数：
//
//	Convert_<pkg1>_<type>_To_<pkg2>_<type>
//
// 条件：(1) 目标包的非生成文件中没有同名函数；(2) 对应的 autoConvert_...
// 没有遇到缺失或类型根本不兼容的字段。生成的 Convert_... 只是调用对应的
// autoConvert...。`conversion_gen` 还会生成一个函数，把找到的和生成的
// Convert_... 全部注册到给定的 `runtime.Scheme`。因此开发者可以在非生成
// 文件中放同名的 Convert_... 来覆盖选定类型对的生成行为；字段缺失或类型
// 根本不兼容时，实际上必须手写覆盖。
//
// `conversion-gen` 扫描 `--input-dirs`，查找每个目录中的包里的注释标签，
// 以确定转换任务。包在 `doc.go` 中写入以下形式的注释即表示请求生成转换代码
// （当前放在文件任意位置均可，但建议在 `package` 语句之前）：
//
//	// +k8s:conversion-gen=<import-path-of-internal-package>
//
// 该标签引入一个转换任务：带标签的文件所在包是目标包，标签指向包含 internal
// 类型的包。若还有如下标签：
//
//	// +k8s:conversion-gen-external-types=<import-path-of-external-package>
//
// 它指向 external 类型所在的包；否则 external 类型就在目标包中。
//
// 每个任务的完整 internal 包集合（即 peer packages）由 `k8s:conversion-gen`
// 标签指定的包，加上命令行 `--base-peer-dirs` 与 `--extra-peer-dirs` 指定的包组成。
//
// 生成时，单个类型或结构体字段可用如下注释退出转换生成：
//
//	// +k8s:conversion-gen=false
//
// 中文概述：conversion-gen 根据 +k8s:conversion-gen 标签，在 internal 与 external
// 类型之间生成 autoConvert_/Convert_ 函数并注册到 runtime.Scheme。开发者可用同名的
// 手写 Convert_ 函数覆盖生成行为，也可以用 +k8s:conversion-gen=false 让某个类型或
// 字段退出转换生成。
package main

import (
	"flag"

	"github.com/spf13/pflag"
	"k8s.io/klog/v2"

	generatorargs "k8s.io/code-generator/cmd/conversion-gen/args"
	"k8s.io/code-generator/cmd/conversion-gen/generators"
	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/generator"
)

// main 解析参数、校验后把 gengo 引擎交给 conversion-gen 的 targets 回调。
// 本工具不直接处理输入参数，packages 由 pflag 的剩余参数传入 gengo。
func main() {
	// klog 的标志先初始化，再与 pflag 共享同一个 FlagSet。
	klog.InitFlags(nil)
	args := generatorargs.New()

	args.AddFlags(pflag.CommandLine)
	flag.Set("logtostderr", "true")
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()

	// 尽早失败，避免生成到一半才发现参数非法。
	if err := args.Validate(); err != nil {
		klog.Fatalf("Error: %v", err)
	}

	// 延迟到 gengo 构建好 Context 后再生成目标，因为 target 需要 universe 信息。
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
