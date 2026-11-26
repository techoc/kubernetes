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

package audit

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的组名。
//
// 审计 API 组名；内部版本与外部版本共享此组名，版本号用 runtime.APIVersionInternal 表示。
const GroupName = "audit.k8s.io"

// SchemeGroupVersion 是注册这些对象所使用的组版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Kind 接受一个不带组名的 kind，返回带组限定的 GroupKind。
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource 接受一个不带组名的 resource，返回带组限定的 GroupResource。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder 收集向 runtime.Scheme 注册审计内部类型的方法。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 把本包的已知类型注册到给定 scheme。
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes 注册审计内部版本的全部顶层类型（Event/Policy 及其 List）。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Event{},
		&EventList{},
		&Policy{},
		&PolicyList{},
	)
	return nil
}
