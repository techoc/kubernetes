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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的 API 组名。
const GroupName = "certificates.k8s.io"

// SchemeGroupVersion 是用于注册这些对象的组版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}

// Kind 接受非限定类型名，返回带组限定的 GroupKind。
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource 接受非限定资源名，返回带组限定的 GroupResource。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// TODO: 将 SchemeBuilder 与 zz_generated.deepcopy.go 一并移到 k8s.io/api。
	// localSchemeBuilder 和 AddToScheme 留在 k8s.io/kubernetes。
	// SchemeBuilder 聚合本包的类型注册函数；用 runtime.NewSchemeBuilder 是因为注册延迟到调用方决定 scheme 时执行。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// localSchemeBuilder 指向 SchemeBuilder，供本包内部复用。
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 将所有已知类型注册到给定 scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)

// addKnownTypes 将已知类型列表注册到给定 scheme。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&CertificateSigningRequest{},
		&CertificateSigningRequestList{},
		&ClusterTrustBundle{},
		&ClusterTrustBundleList{},
		&PodCertificateRequest{},
		&PodCertificateRequestList{},
	)

	// 注册对应的 watch 版本，否则该组版本无法支持监听。
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
