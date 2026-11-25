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

package wardle

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的 API 组名。
// 内部版本与外部版本共用组名，转换时不用改 GVK 的 group 部分。
const GroupName = "wardle.example.com"

// SchemeGroupVersion 是注册本包对象所用的组版本。
// 内部版本只在内存中流转，不会出现在 etcd 或 API 响应里。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Kind 为未限定组的 Kind 补上本 API 组，返回 GroupKind。
// 补上组名，kind 在不同 API 组之间才不会撞名。
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource 为未限定组的资源名补上本 API 组，返回 GroupResource。
// 资源名本身不带组信息，这里补齐供存储层键路径使用。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder 保存本 API 包需要在 Scheme 上执行的注册函数。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 把本包的组版本及类型注册到 Scheme。
	// 内部版本只需注册已知类型，默认值与转换由版本化包处理。
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes 把本包的已知类型注册到 Scheme。
// 注册的是内存中的内部类型，外部版本通过转换函数与它们互转。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Flunder{},
		&FlunderList{},
		&Fischer{},
		&FischerList{},
	)
	return nil
}
