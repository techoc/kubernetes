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

// GroupName 是本包使用的 API 组名，必须与内部 config 包保持一致，才能在同一 Group 下完成版本转换。
const GroupName = "cloudcontrollermanager.config.k8s.io"

var (
	// SchemeGroupVersion 是注册外部资源使用的 v1alpha1 GroupVersion，序列化首选它。
	SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}
	// SchemeBuilder 是携带本 API 包各 scheme 初始化函数的构建器。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// localSchemeBuilder 是指向 SchemeBuilder 实例的指针，defaulting 和 conversion
	// 初始化函数也经由它注册。
	// 通过指针共享同一个 builder，生成代码才能把手写函数与自动生成函数注册到一起。
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 是全局函数，把本 API 组与版本注册到给定 scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只注册手动编写的函数，生成函数的注册在生成的文件中进行。
	// 分开注册使得缺少生成文件时本包仍可编译。
	localSchemeBuilder.Register(addDefaultingFuncs)
}

// addKnownTypes 把已知类型注册到给定 scheme。
// 外部类型集合必须与内部 config 包登记的集合一一对应，否则转换会报未知类型。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&CloudControllerManagerConfiguration{},
	)
	return nil
}
