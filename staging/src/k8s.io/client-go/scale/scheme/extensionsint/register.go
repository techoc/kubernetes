/*
Copyright 2016 The Kubernetes Authors.

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

package extensionsint

import (
	extensionsv1beta1 "k8s.io/api/extensions/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	scalescheme "k8s.io/client-go/scale/scheme"
)

// GroupName 是本包使用的组名。
const GroupName = extensionsv1beta1.GroupName

// SchemeGroupVersion 是注册这些对象所使用的组版本。
// 内部版本不参与线缆传输，仅作为 extensions group 的转换枢纽。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Kind 接受非限定 kind，返回带组信息的 GroupKind。
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource 接受非限定 resource，返回带组信息的 GroupResource。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder 指向要加入 Scheme 的函数列表。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 把所有保存的函数应用到 scheme。
	AddToScheme = SchemeBuilder.AddToScheme
)

// 把已知类型列表注册到 api.Scheme。
// 复用 scale/scheme 的 Scale 类型，并以 extensions 的内部组版本注册，从而与其他
// group 的内部类型区分开。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&scalescheme.Scale{},
	)
	return nil
}
