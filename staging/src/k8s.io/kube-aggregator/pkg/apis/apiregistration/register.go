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

package apiregistration

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是 apiregistration 的 API 组名。
// 组名单独抽成常量，避免在多处硬编码字符串导致 scheme 注册与资源 URL 不一致。
const GroupName = "apiregistration.k8s.io"

// SchemeGroupVersion 是注册这些对象所使用的 group version。
// 这里注册的是 internal 版本：外部版本（v1/v1beta1）的对象先解码成内存中的 internal 类型，
// 各组件内部统一处理 internal 类型，从而在增加新 API 版本时无需改动核心逻辑。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Kind 接受不带 group 的 kind，返回带 Group 限定的 GroupKind。
// 把裸 Kind 补上 group，便于在校验、事件等场景中用完整的 GroupKind 标识对象类型，
// 避免不同 group 下同名 Kind 混淆。
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource 接受不带 group 的资源名，返回带 Group 限定的 GroupResource。
// 与 Kind 同理，为裸资源名补上 group，用于构造资源引用（如 RBAC 规则、事件 involvedObject）。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder 是带有一组 scheme init 函数的本 API 包的 scheme builder。
	// 把所有注册函数打包成 builders，调用方只需 AddToScheme 一次即可完成该包全部类型的注册，
	// 也便于与其他包的 builder 组合。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 是把包内 group/version 作用域键映射到 scheme 的通用注册函数。
	// 暴露成包级函数，是 scheme 注册约定的入口；外部包通常只依赖这个函数，而不关心 builder 细节。
	AddToScheme = SchemeBuilder.AddToScheme
)

// 将已知类型列表添加到给定的 scheme。
// internal 版本的类型注册必须包含资源和列表两种 Kind，否则反序列化 List 请求时会报“类型未注册”。
// 这里同时注册 APIService 与 APIServiceList，正是为了覆盖单对象和列表两种返回形态。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&APIService{},
		&APIServiceList{},
	)
	return nil
}
