/*
Copyright The Kubernetes Authors.

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

package policyconfig

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// SchemeBuilder 收集本包的注册函数；外部通过 AddToScheme 一次性把
	// 内部版本类型挂到自己的 scheme 上。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 是 SchemeBuilder.AddToScheme 的别名，便于调用方书写。
	AddToScheme = SchemeBuilder.AddToScheme
)

// GroupName 是本包使用的 API 组名。
const GroupName = "apiserver.config.k8s.io"

// SchemeGroupVersion 是本包注册对象使用的组版本。
// 本包只注册内部版本（APIVersionInternal），配置的版本化表示由
// 外部版本包负责转换。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Kind 接收不带组限定的 kind，返回带组限定的 GroupKind。
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource 接收不带组限定的 resource，返回带组限定的 GroupResource。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

// addKnownTypes 向 scheme 注册本组的所有内部类型。
// 新增策略配置类型时必须同步添加，否则反序列化会失败。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&ValidatingAdmissionPolicyConfiguration{},
		&MutatingAdmissionPolicyConfiguration{},
	)
	return nil
}
