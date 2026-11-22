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

package apiextensions

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是 apiextensions API 组的名称。
const GroupName = "apiextensions.k8s.io"

// SchemeGroupVersion 是内部版本的组版本标识，Version 为空字符串表示内部版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Kind 将简单的种类名称（如 "CustomResourceDefinition"）转换为带组前缀的 GroupKind。
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource 将简单的资源名称转换为带组前缀的 GroupResource。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder 用于将内部版本的已知类型注册到 Scheme 中。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 是将 CRD 内部版本类型添加到 Scheme 的便捷函数。
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes 将 CRD 内部版本类型注册到 Scheme 中，使运行时能够识别和转换这些类型。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&CustomResourceDefinition{},
		&CustomResourceDefinitionList{},
	)
	return nil
}
