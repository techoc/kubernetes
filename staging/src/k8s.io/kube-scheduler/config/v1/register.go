/*
Copyright 2022 The Kubernetes Authors.

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

package v1

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName is the group name used in this package
//
// 组名收敛成一个常量，是为了让 SchemeGroupVersion 和注册逻辑共用同一个来源，
// 避免版本演进时各处字符串各写各的而出现不一致。
const GroupName = "kubescheduler.config.k8s.io"

// SchemeGroupVersion is group version used to register these objects
//
// 把 Group 和 Version 合成一个值再传给 scheme，是 k8s API 注册的惯例：
// 序列化框架按 (group, version) 二元组定位类型，分开存两个字段只会让调用方反复拼接、也更容易拼错。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

var (
	// SchemeBuilder is the scheme builder with scheme init functions to run for this API package
	//
	// 用 Builder 把「注册哪些类型」延迟到调用方决定：包初始化时只登记函数，
	// 不往某个全局 scheme 里写。这样同一个包可以服务于彼此独立的 scheme 实例，
	// 测试也能从空 scheme 开始，只注册自己关心的类型。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme is a global function that registers this API group & version to a scheme
	//
	// 暴露成包级变量是给外部一个稳定入口，调用方不必知道 Builder 内部登记了哪些初始化函数。
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes registers known types to the given scheme
//
// 这里必须逐个列出类型：scheme 不会反射扫描包内结构体，漏掉任何一个，
// 该类型在这个版本下就无法序列化。返回 error 而非静默处理，是为了和 SchemeBuilder
// 的执行约定对齐，将来某个类型的注册需要额外校验时也有地方报告失败。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&KubeSchedulerConfiguration{},
		&DefaultPreemptionArgs{},
		&InterPodAffinityArgs{},
		&NodeResourcesBalancedAllocationArgs{},
		&NodeResourcesFitArgs{},
		&PodTopologySpreadArgs{},
		&VolumeBindingArgs{},
		&NodeAffinityArgs{},
		&DynamicResourcesArgs{},
	)
	return nil
}
