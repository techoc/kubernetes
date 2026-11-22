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

// GroupName is the group name use in this package
const GroupName = appsapiv1beta2.GroupName

// SchemeGroupVersion is group version used to register these objects
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta2"}

// Resource takes an unqualified resource and returns a Group qualified GroupResource
//
// 中文说明：把 "deployments" 之类的非限定资源名补全为 apps/v1beta2, Resource=deployments。
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
	// We only register manually written functions here. The registration of the
	// generated functions takes place in the generated files. The separation
	// makes the code compile even when the generated files are missing.
	// 中文：手写与生成代码分开注册，缺少生成文件时仍可编译，便于分阶段生成。
	localSchemeBuilder.Register()
}
