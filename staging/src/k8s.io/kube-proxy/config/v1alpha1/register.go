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
// GroupName 是本包使用的 API 组名。kube-proxy 的配置不属于核心 API 组，
// 单独起一个组名可以避免与其他组件的配置类型在 scheme 中发生冲突。
const GroupName = "kubeproxy.config.k8s.io"

// SchemeGroupVersion is group version used to register these objects
//
// SchemeGroupVersion 是这些类型注册到 scheme 时使用的组版本。
// 版本号写死在这里，是因为一个版本包只负责一个版本的对象；
// 将来格式不兼容时，新增一个版本包而不是修改这里的版本字符串。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}

var (
	// SchemeBuilder is the scheme builder with scheme init functions to run for this API package
	//
	// SchemeBuilder 保存本包需要执行的 scheme 初始化函数。这里只注册已知类型，
	// 不注册转换函数：组件配置的外部版本到内部版本的转换由 component-base 的默认化/解码路径处理，
	// 而非 apiserver 的多版本转换机制。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme is a global function that registers this API group & version to a scheme
	//
	// AddToScheme 是 SchemeBuilder.AddToScheme 的别名，供调用方（如命令行配置加载代码）
	// 一次性把该组版本注册到给定的 scheme 上。暴露变量而非直接暴露函数，
	// 是为了与 Kubernetes 其他 API 包的注册惯例保持一致。
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes registers known types to the given scheme
//
// addKnownTypes 把本版本支持的类型登记到 scheme 的 SchemeGroupVersion 下。
// scheme 需要知道「组版本 + Kind」对应哪个 Go 类型，才能正确解码配置文件。
// 这里没有调用 metav1.AddToGroupVersion：组件配置不走 apiserver 的 watch/list 通道，
// 因此无需注册 WatchEvent 等元类型。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&KubeProxyConfiguration{},
	)
	return nil
}
