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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的 API 组名。
// 组名跨版本不变，以便内外部版本共用同一套 API group 约定。
const GroupName = "credentialprovider.kubelet.k8s.io"

// SchemeGroupVersion 是注册这些对象所用的 group version。
// v1 是当前稳定的外部版本，注册后 kubelet 才能按 GVK 解码插件回包。
var (
	SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}
	localSchemeBuilder = &SchemeBuilder
)

var (
	// SchemeBuilder 汇总本包的类型注册步骤，AddToScheme 暴露给调用方使用。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

// addKnownTypes 注册 v1 请求/响应类型，并加入 metav1 的通用类型以识别 TypeMeta。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&CredentialProviderRequest{},
		&CredentialProviderResponse{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
