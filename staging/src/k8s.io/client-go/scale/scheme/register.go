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

package scheme

import (
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的组名。
const GroupName = autoscalingv1.GroupName

// SchemeGroupVersion 是注册这些对象所用的 group version。
// 内部版本（__internal）仅用于客户端内部的类型转换，不直接与 API server 交互。
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

// 把已知类型列表加入 api.Scheme。
func addKnownTypes(scheme *runtime.Scheme) error {
	// 只注册内部 Scale；外部版本由各自的 api 包注册，转换由 conversion-gen 生成。
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Scale{},
	)
	return nil
}
