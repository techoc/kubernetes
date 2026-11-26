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
const GroupName = "example.apiserver.k8s.io"

// SchemeGroupVersion 是注册这些对象时使用的组版本
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

// Kind 接受非限定 kind，返回带组名的 GroupKind
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource 接受非限定资源名，返回带组名的 GroupResource
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// TODO: 把 SchemeBuilder 连同 zz_generated.deepcopy.go 移到 k8s.io/api。
	// localSchemeBuilder 与 AddToScheme 保留在 k8s.io/kubernetes。
	//
	// SchemeBuilder 收集安装方法；init 中注册的手写函数与生成函数分离，
	// 使得生成文件缺失时包仍可编译。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	AddToScheme        = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只注册手写函数；生成函数的注册在生成文件里完成。
	// 两者分离后，即使生成文件缺失，包也能编译。
	localSchemeBuilder.Register(addKnownTypes, addConversionFuncs, addDefaultingFuncs)
}

// 将已知类型列表注册到给定 scheme。
// 只注册 Pod 这一种示例类型，并补上 metav1 的通用类型（WatchEvent、List 等）。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Pod{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
