/*
Copyright 2022 The Kubernetes Authors.

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

// GroupName is the group name use in this package
//
// 组名与内部版本保持一致，各版本只靠 Version 区分，便于跨版本转换与注册。
const GroupName = "credentialprovider.kubelet.k8s.io"

// SchemeGroupVersion is group version used to register these objects
//
// 请求/响应类型只在本版本下注册，序列化器靠 GVK 选择解码目标。
var (
	SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}
	localSchemeBuilder = &SchemeBuilder
)

var (
	// SchemeBuilder 聚合本包的类型注册函数；AddToScheme 是给外部调用的统一入口。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

// addKnownTypes 注册本版本类型，并补上 metav1 的通用类型，
// 这样 TypeMeta 里的 kind/apiVersion 才能被正确识别。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&CredentialProviderRequest{},
		&CredentialProviderResponse{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
