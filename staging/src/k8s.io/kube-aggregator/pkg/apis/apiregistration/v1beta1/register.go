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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是 apiregistration 的 API 组名。
// 与 internal 版本保持同一个组名，只在 Version 上区分；同一资源在不同版本间的转换依赖这一点。
const GroupName = "apiregistration.k8s.io"

// SchemeGroupVersion 是注册这些对象所使用的 group version。
// 这是对外暴露的 v1beta1 版本；客户端请求该版本时由 scheme 解码为对应结构体，
// 再经转换函数变成 internal 版本供组件内部使用。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}

// Resource 接受不带 group 的资源名，返回带 Group 限定的 GroupResource。
// 把裸资源名补上 group 与 version，用于生成该版本下的资源引用。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder 是带有一组 scheme init 函数的本 API 包的 scheme builder。
	// TODO: move SchemeBuilder with zz_generated.deepcopy.go to k8s.io/api.
	// localSchemeBuilder and AddToScheme will stay in k8s.io/kubernetes.
	// 区分公开 SchemeBuilder 与 localSchemeBuilder：前者可被外部包追加注册函数，
	// 后者由本包 init 使用，把手写注册与生成代码注册分开，避免两者互相覆盖。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 是把包内 group/version 作用域键映射到 scheme 的通用注册函数。
	// 对外的统一注册入口；调用方拿到的是绑定了本包 builder 的函数，无需了解内部指针。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只注册手写的函数；生成函数的注册在生成的文件中进行。
	// 这样的划分使得即使生成文件缺失，代码仍然可以编译。
	// 这里只注册手写的注册函数与默认值函数；generated 的 deepcopy/conversion 等在各自的
	// zz_generated 文件中注册。这样即使生成文件暂时缺失，本文件仍能编译，便于本地开发。
	localSchemeBuilder.Register(addKnownTypes, addDefaultingFuncs)
}

// 将已知类型列表添加到给定的 scheme。
// 注册单对象与列表两种 Kind，保证 list/watch 请求能正确解码。
// 额外调用 metav1.AddToGroupVersion 注册 ListMeta、Time 等元类型，
// 这是外部版本必需的（internal 版本不需要），否则响应中的 metadata 字段无法解析。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&APIService{},
		&APIServiceList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
