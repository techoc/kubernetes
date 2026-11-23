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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName is the group name use in this package
// 本常量是 API group 的唯一来源，其他文件应引用它而不是重复写字符串。
const GroupName = "metrics.k8s.io"

// SchemeGroupVersion is group version used to register these objects
// 客户端构建 rest 请求时需要精确的 GroupVersion，不能用 GroupName 代替。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}

// Resource takes an unqualified resource and returns a Group qualified GroupResource
// 只补 group，不补 version；调用方再按需加版本，便于复用同一 helper。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder points to a list of functions added to Scheme.
	// 集中注册本版本的全部类型，避免调用方漏注册某个 List kind。
	SchemeBuilder      = runtime.NewSchemeBuilder(addKnownTypes)
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme applies all the stored functions to the scheme.
	// 外部通过此入口一次性完成注册；localSchemeBuilder 留给生成代码使用。
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes 同时注册 metav1 的通用类型，保证 ListMeta、Time 等字段能被正确解码。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&NodeMetrics{},
		&NodeMetricsList{},
		&PodMetrics{},
		&PodMetricsList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
