/*
Copyright 2020 The Kubernetes Authors.

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

// 本文件把 alpha 版凭证插件 API 类型注册进 scheme，
// 供 scheme 编解码器识别插件使用的 apiVersion。
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的 API 组名。
// GroupName 与配置组不同，专用于 kubelet 与 exec 凭证插件之间的请求/响应消息。
const GroupName = "credentialprovider.kubelet.k8s.io"

// SchemeGroupVersion 是注册这些对象所用的 group version。
// SchemeGroupVersion 标识 alpha 版本；localSchemeBuilder 供本包生成器注入额外的初始化函数。
var (
	SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}
	localSchemeBuilder = &SchemeBuilder
)

// SchemeBuilder/AddToScheme 是 scheme 注册的入口，
// 外部包通过 AddToScheme 把这两个消息类型加入自己的 scheme。
var (
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

// addKnownTypes 注册请求与响应两个消息类型。
// 这里额外调用 metav1.AddToGroupVersion，是为了让 scheme 认识 TypeMeta，
// 否则解码插件 JSON 时会因缺少元数据类型而报错。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&CredentialProviderRequest{},
		&CredentialProviderResponse{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
