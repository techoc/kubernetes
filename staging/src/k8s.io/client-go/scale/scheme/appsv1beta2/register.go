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

package appsv1beta2

import (
	appsapiv1beta2 "k8s.io/api/apps/v1beta2"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的组名。
const GroupName = appsapiv1beta2.GroupName

// SchemeGroupVersion 是注册这些对象所用的 group version。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta2"}

// Resource 接收非限定的资源名并返回带 Group 限定的 GroupResource，
// 把 "deployments" 之类的非限定资源名补全为 apps/v1beta2, Resource=deployments。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// localSchemeBuilder 复用上游 API 类型的 builder，使手写与生成的注册函数汇入同一 scheme。
	localSchemeBuilder = &appsapiv1beta2.SchemeBuilder
	// AddToScheme 把该 group/version 的类型与转换函数注册到目标 scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只注册手写的函数；其余函数的注册放在生成文件中。
	// 生成函数放在生成文件中；这种分离使代码在生成文件缺失时也能编译。
	// 中文：手写与生成代码分开注册，缺少生成文件时仍可编译，便于分阶段生成。
	localSchemeBuilder.Register()
}
