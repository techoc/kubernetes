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

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName is the group name used in this package
//
// 组名与 doc.go 中指令 +groupName=... 保持一致：前者供运行时 scheme 使用，
// 后者供代码生成器使用，两处必须同步，否则注册的类型与生成的模型会落在不同的组下。
// 用 .config.k8s.io 后缀是为了表明这是组件配置 API，而非 API Server 提供的工作负载 API。
const GroupName = "kubecontrollermanager.config.k8s.io"

// SchemeGroupVersion is group version used to register these objects
//
// 把组和版本打包成一个 GroupVersion，是为了让 AddKnownTypes 的调用点不必分别传递两者；
// 版本单独保留为 v1alpha1，表明配置结构仍可能调整，调用方不应把它当作稳定契约。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}

var (
	// SchemeBuilder is the scheme builder with scheme init functions to run for this API package
	//
	// 用 SchemeBuilder 而不是在包初始化时直接修改全局 scheme，是为了让调用方自行决定
	// 把本组类型注册到哪个 scheme 中，也便于测试构造彼此隔离的 scheme。
	// 目前只有一个注册函数 addKnownTypes，但保留 builder 形式便于以后追加注册步骤。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme is a global function that registers this API group & version to a scheme
	//
	// 这是 SchemeBuilder.AddToScheme 的便捷别名，调用方通常只需要这一个入口；
	// 它把“有哪些初始化函数”的实现细节藏在包内。
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes registers known types to the given scheme
//
// 只注册顶层配置对象 KubeControllerManagerConfiguration，子结构作为它的字段随顶层对象
// 一起参与序列化，不需要各自注册为独立 GVK。这样 scheme 里只有配置文件的唯一入口，
// 也避免了为内部辅助结构维护多余的类型注册。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&KubeControllerManagerConfiguration{},
	)
	return nil
}
