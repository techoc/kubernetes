/*
Copyright 2024 The Kubernetes Authors.

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

package v1alpha2

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的 API 组名。
const GroupName = "coordination.k8s.io"

// SchemeGroupVersion 是注册本包对象时使用的组版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha2"}

// Resource 接受一个未限定的 resource，返回带组限定的 GroupResource。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// TODO: 把 SchemeBuilder 连同 zz_generated.deepcopy.go 移到 k8s.io/api。
	// localSchemeBuilder 和 AddToScheme 仍留在 k8s.io/kubernetes。
	// SchemeBuilder 汇集向 scheme 注册已知类型的初始化函数。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// localSchemeBuilder 是 SchemeBuilder 的内部别名，避免外部直接改动。
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 是把本 API 组和版本注册到 scheme 的函数。
	AddToScheme = localSchemeBuilder.AddToScheme
)

// addKnownTypes 把本组的已知类型列表加入 api.Scheme。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&LeaseCandidate{},
		&LeaseCandidateList{},
	)

	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
