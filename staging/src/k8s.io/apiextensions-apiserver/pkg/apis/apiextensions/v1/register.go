/*
Copyright 2019 The Kubernetes Authors.

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

// GroupName 是 apiextensions API 的组名。
const GroupName = "apiextensions.k8s.io"

// SchemeGroupVersion 是用于注册这些对象的组版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

// Kind 接收未限定的 kind，返回带组限定的 GroupKind。
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource 接收未限定的 resource，返回带组限定的 GroupResource。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

// SchemeBuilder 收集向 scheme 注册本组类型的函数。
// AddToScheme 将这些类型加入给定的 scheme。
var (
	SchemeBuilder      = runtime.NewSchemeBuilder(addKnownTypes, addDefaultingFuncs)
	localSchemeBuilder = &SchemeBuilder
	AddToScheme        = localSchemeBuilder.AddToScheme
)

// 将已知类型列表加入给定的 scheme。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&CustomResourceDefinition{},
		&CustomResourceDefinitionList{},
		&ConversionReview{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}

func init() {
	// 这里只注册手写的函数。生成函数的注册发生在生成文件中。
	// 这种分离使得生成文件缺失时代码仍能编译。
	localSchemeBuilder.Register(addDefaultingFuncs)
}
