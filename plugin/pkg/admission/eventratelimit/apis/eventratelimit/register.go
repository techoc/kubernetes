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

package eventratelimit

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// SchemeBuilder 是 scheme 构建器，包含本 API 包需要执行的 scheme 初始化函数。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 用于把这些类型注册到 API 的编解码机制中。
	AddToScheme = SchemeBuilder.AddToScheme
)

// GroupName 是本包使用的 API 组名。
const GroupName = "eventratelimit.admission.k8s.io"

// SchemeGroupVersion 是注册这些对象时使用的组版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Kind 接受一个未限定的 kind，返回带组限定的 GroupKind。
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource 接受一个未限定的资源名，返回带组限定的 GroupResource。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

// addKnownTypes 向 scheme 注册本包已知的类型。
func addKnownTypes(scheme *runtime.Scheme) error {
	// TODO 待 scheme 类型修复后清理此处
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Configuration{},
	)
	return nil
}
