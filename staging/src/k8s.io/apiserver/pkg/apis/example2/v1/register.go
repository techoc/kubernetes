/*
Copyright 2017 The Kubernetes Authors.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的组名
const GroupName = "example2.apiserver.k8s.io"

// SchemeGroupVersion 是用于注册这些对象的组版本
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

// Kind 接收非限定的 kind，返回带组限定的 GroupKind
//
// 用于在只需要 GroupKind 的场合（如 RESTMapper、事件引用）省略版本。
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource 接收非限定的资源名，返回带组限定的 GroupResource
//
// 与 Kind 对称，用于构造不带版本号的资源标识。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// TODO: 将 SchemeBuilder 与 zz_generated.deepcopy.go 一起移到 k8s.io/api。
	// localSchemeBuilder 和 AddToScheme 留在 k8s.io/kubernetes。
	//
	// 区分 SchemeBuilder 与 localSchemeBuilder 是为了让生成代码注册到前者，
	// 而本包只把手写函数注册到后者。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	AddToScheme        = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只注册手写的函数；生成函数的注册在生成的文件中进行。
	// 分离两者使生成文件缺失时代码仍能编译。
	//
	localSchemeBuilder.Register(addKnownTypes, addDefaultingFuncs)
}

// 将已知类型列表添加到给定 scheme。
//
// 除本组类型外还要注册 metav1 的公共类型，否则编解码与 discovery 会缺少
// ListOptions 等元数据操作所需的注册。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&ReplicaSet{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
