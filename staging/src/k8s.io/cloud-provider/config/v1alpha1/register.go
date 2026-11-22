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

// GroupName is the group name use in this package
// 与内部 config 包保持一致，才能在同一 Group 下完成版本转换。
const GroupName = "cloudcontrollermanager.config.k8s.io"

var (
	// GroupName is the group name use in this package
	// SchemeGroupVersion 是外部资源注册使用的 v1alpha1 版本，序列化首选它。
	SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}
	// SchemeBuilder is the scheme builder with scheme init functions to run for this API package
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// localSchemeBuilder ïs a pointer to SchemeBuilder instance. Using localSchemeBuilder
	// defaulting and conversion init funcs are registered as well.
	// 通过指针共享同一个 builder，生成代码才能把手写函数与自动生成函数注册到一起。
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme is a global function that registers this API group & version to a scheme
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// We only register manually written functions here. The registration of the
	// generated functions takes place in the generated files. The separation
	// makes the code compile even when the generated files are missing.
	// 手写默认化与生成代码分开注册，缺少生成文件时本包仍可编译。
	localSchemeBuilder.Register(addDefaultingFuncs)
}

// addKnownTypes registers known types to the given scheme
// 外部类型集合必须与内部 config 包登记的集合一一对应，否则转换会报未知类型。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&CloudControllerManagerConfiguration{},
	)
	return nil
}
