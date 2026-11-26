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

// deepcopy-gen 自动为类型生成 DeepCopy 函数。
//
// 给定输入目录列表，它为每个类型生成 DeepCopy 与 DeepCopyInto，完成完整深拷贝。
// 若开发者已手写这些方法则直接复用。生成代码优先用普通赋值；赋值不够时调用
// 该类型的生成拷贝函数；再不行则退回到 `conversion.Cloner.DeepCopy(val)`。
// 生成文件与被处理的源包放在同一目录。
//
// 若类型引用了接口，则期望存在对应的 DeepCopyInterfaceName 方法（如
// runtime.Object 对应 DeepCopyObject）。这些方法可手写，也可通过下述标签生成；
// 但接口声明本身必须手工加上这些方法，例如：
//
//	type Object interface {
//	  ...
//	  DeepCopyObject() Object
//	}
//
// 生成行为由源码注释标签控制。包级生成在某个文件的文件注释里声明：
//
//	// +k8s:deepcopy-gen=package
//
// 只生成单个类型时，在类型定义上加：
//
//	// +k8s:deepcopy-gen=true
//
// 整包生成时，个别类型可在定义上声明退出：
//
//	// +k8s:deepcopy-gen=false
//
// 还可为类型生成额外的 DeepCopyInterfaceName 方法：
//
//	// +k8s:deepcopy-gen:interfaces=k8s.io/kubernetes/runtime.Object,k8s.io/kubernetes/runtime.List
//
// 这会生成以指定接口为返回类型的 DeepCopyObject 与 DeepCopyList，
// 相当于让打标类型为这些接口实现深拷贝。
//
// 上述 interfaces 标签生成的深拷贝函数使用类型指针作接收者。
// 若需要值类型实现接口，可另加：
//
//	// +k8s:deepcopy-gen:nonpointer-interfaces=true
package main

import (
	"flag"

	"github.com/spf13/pflag"
	"k8s.io/code-generator/cmd/deepcopy-gen/args"
	"k8s.io/code-generator/cmd/deepcopy-gen/generators"
	"k8s.io/gengo/v2"
	"k8s.io/gengo/v2/generator"
	"k8s.io/klog/v2"
)

func main() {
	klog.InitFlags(nil)
	args := args.New()

	args.AddFlags(pflag.CommandLine)
	flag.Set("logtostderr", "true")
	pflag.CommandLine.AddGoFlagSet(flag.CommandLine)
	pflag.Parse()

	if err := args.Validate(); err != nil {
		klog.Fatalf("Error: %v", err)
	}

	myTargets := func(context *generator.Context) []generator.Target {
		return generators.GetTargets(context, args)
	}

	// 执行生成；输入包取自位置参数，类型标注由 +k8s:deepcopy-gen 标签控制。
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
