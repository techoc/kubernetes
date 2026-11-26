/*
Copyright 2025 The Kubernetes Authors.

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
	"k8s.io/apimachinery/pkg/runtime/schema"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
)

const (
	// GroupName 为 flagz 端点所属的 API 组。flagz 与 zpages 类似属于诊断接口，
	// 不暴露在核心组中，避免污染 v1 API 的稳定性承诺。
	GroupName = "config.k8s.io"
	// Version 为 flagz API 的版本，目前仅 v1alpha1。
	Version = "v1alpha1"
)

// SchemeGroupVersion 是用于注册这些对象的组版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}

var (
	// SchemeBuilder 初始化 scheme builder。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 是把本组类型加入 scheme 的全局函数。
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes 向 scheme 注册 Flagz 类型，并加入组版本级的元数据（TypeMeta 等）。
// 只注册 Flagz 一个类型：该端点只读，不提供 create/update 语义。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Flagz{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
