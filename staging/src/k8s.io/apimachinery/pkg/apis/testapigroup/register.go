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

package testapigroup

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var (
	// SchemeBuilder 收集本包的类型注册函数（addKnownTypes）。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 把本包的类型加入给定的 Scheme。
	AddToScheme = SchemeBuilder.AddToScheme
)

// GroupName 是本包使用的 API 组名。
const GroupName = "testapigroup.apimachinery.k8s.io"

// SchemeGroupVersion 是注册这些对象所用的组/版本；内部版本用于测试转换。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Kind 接收不带组前缀的 kind，返回带组限定的 GroupKind。
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource 接收不带组前缀的 resource，返回带组限定的 GroupResource。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

// addKnownTypes 把 Car 及其列表类型注册到 Scheme 的内部版本。
// 只注册一对类型即可让测试覆盖列表与单项的编解码路径。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Carp{},
		&CarpList{},
	)
	return nil
}
