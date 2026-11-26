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

package example2

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/apis/example"
)

var (
	// SchemeBuilder 收集添加本组类型的函数。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 将本组类型加入 scheme。
	AddToScheme = SchemeBuilder.AddToScheme
)

// GroupName 是本包使用的组名
const GroupName = "example2.apiserver.k8s.io"

// SchemeGroupVersion 是注册这些对象所用的组版本
// 内部版本：Version 为 runtime.APIVersionInternal，仅供测试内部的类型注册。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Kind 接受非限定 kind，返回带组名的 GroupKind
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource 接受非限定资源名，返回带组名的 GroupResource
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

// 将已知类型列表添加到给定的 scheme。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&example.ReplicaSet{},
	)
	return nil
}
