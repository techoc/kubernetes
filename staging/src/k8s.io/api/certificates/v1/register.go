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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的 API 组名。
const GroupName = "certificates.k8s.io"

// SchemeGroupVersion 是注册本包对象时使用的组版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

// Kind 接受一个未限定的 kind，返回带组限定的 GroupKind。
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource 接受一个未限定的 resource，返回带组限定的 GroupResource。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder 汇集本 API 包需要在 scheme 上执行的初始化函数。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// localSchemeBuilder 是 SchemeBuilder 的内部别名，避免外部直接改动。
	localSchemeBuilder = &SchemeBuilder

	// AddToScheme 是把本 API 组和版本注册到 scheme 的全局函数。
	AddToScheme = localSchemeBuilder.AddToScheme
)

// addKnownTypes 把本组的已知类型列表加入给定 scheme。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&CertificateSigningRequest{},
		&CertificateSigningRequestList{},
		&ClusterTrustBundle{},
		&ClusterTrustBundleList{},
		&PodCertificateRequest{},
		&PodCertificateRequestList{},
	)

	// watch 事件也要能按此组版本编解码，故补注册元类型。
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
