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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的组名。
const GroupName = "policy"

// SchemeGroupVersion 是用于注册这些对象的组版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}

// Resource 接受非限定资源名，返回带组限定的 GroupResource。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// TODO: 将 SchemeBuilder 连同 zz_generated.deepcopy.go 一起移到 k8s.io/api。
	// localSchemeBuilder 和 AddToScheme 将留在 k8s.io/kubernetes。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// localSchemeBuilder 指向 SchemeBuilder，供生成代码使用。
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 将打包的类型注册到 scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)

// addKnownTypes 将已知类型列表添加到给定 scheme。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&PodDisruptionBudget{},
		&PodDisruptionBudgetList{},
		&Eviction{},
	)
	// 注册适用的 watch 版本。
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
