/*
Copyright 2019 The Kubernetes Authors.

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

// GroupName 是本 API 的组名。
const GroupName = "admissionregistration.k8s.io"

// SchemeGroupVersion 是用于注册这些对象的组版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

// Resource 接受非限定资源名，返回带组限定的 GroupResource。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

// TODO: move SchemeBuilder with zz_generated.deepcopy.go to k8s.io/api.
// localSchemeBuilder 和 AddToScheme 将留在 k8s.io/kubernetes 中。
var (
	// SchemeBuilder 指向要添加到 Scheme 的函数列表。
	SchemeBuilder      = runtime.NewSchemeBuilder(addKnownTypes)
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 是通用注册函数，将打包的作用域、组和版本键映射到 scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)

// addKnownTypes 将已知类型列表添加到 scheme。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&ValidatingWebhookConfiguration{},
		&ValidatingWebhookConfigurationList{},
		&MutatingWebhookConfiguration{},
		&MutatingWebhookConfigurationList{},
		&ValidatingAdmissionPolicy{},
		&ValidatingAdmissionPolicyList{},
		&ValidatingAdmissionPolicyBinding{},
		&ValidatingAdmissionPolicyBindingList{},
		&MutatingAdmissionPolicy{},
		&MutatingAdmissionPolicyList{},
		&MutatingAdmissionPolicyBinding{},
		&MutatingAdmissionPolicyBindingList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
