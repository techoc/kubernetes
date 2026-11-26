/*
Copyright 2015 The Kubernetes Authors.

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

package example3

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// SchemeGroupVersion 描述该 group 的 internal 版本。
// group 名带点号（example.dots...），用于验证命名转换在含点 group 上的行为。
var SchemeGroupVersion = schema.GroupVersion{Group: "example.dots.apiserver.code-generator.k8s.io", Version: runtime.APIVersionInternal}

// SchemeBuilder/AddToScheme 是 scheme 注册的标准入口。
var (
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

// Resource 把不带 group 的资源名加上本 group，返回 GroupResource。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

// 把已知类型列表加入给定 scheme。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&TestType{},
		&TestTypeList{},
	)

	// 无参调用保留与其它 register.go 一致的调用形态，便于对比生成结果。
	scheme.AddKnownTypes(SchemeGroupVersion)
	return nil
}
