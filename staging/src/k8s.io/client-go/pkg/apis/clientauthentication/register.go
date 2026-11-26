/*
Copyright 2018 The Kubernetes Authors.

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

package clientauthentication

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的 API group 名。
//
// ExecCredential 等类型在 API 上属于该组；实际序列化格式由 v1/v1beta1 等外部版本定义。
const GroupName = "client.authentication.k8s.io"

// SchemeGroupVersion 是注册这些对象所用的 group version。
//
// 这里注册的是内部版本（APIVersionInternal），只用于在内存中做类型转换，
// 不会被直接写入 exec 插件的 stdin/stdout 协议。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Kind 把裸 kind 补全为带 group 的 GroupKind。
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource 把裸资源名补全为带 group 的 GroupResource。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder 收集本组需要注册到 runtime.Scheme 的类型构造函数。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 将本组类型加入给定 Scheme，通常由 install 包聚合调用。
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes 注册内部版本类型；新增类型时必须同步实现 DeepCopy 与转换逻辑。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&ExecCredential{},
	)
	return nil
}
