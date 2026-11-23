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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName is the group name use in this package
// 组名与 metrics.k8s.io API 组一致，客户端靠它定位该资源。
const GroupName = "metrics.k8s.io"

// SchemeGroupVersion is group version used to register these objects
// 组和版本固定成同一个值，注册与序列化共用，避免字符串散落各处。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}

// Resource takes an unqualified resource and returns a Group qualified GroupResource
// 用当前版本补全资源名，调用方就不必各自拼接组名。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder points to a list of functions added to Scheme.
	// 注册逻辑集中到 builder，新增类型时只需改 addKnownTypes。
	SchemeBuilder      = runtime.NewSchemeBuilder(addKnownTypes)
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme applies all the stored functions to the scheme.
	// 对外只暴露这个入口，调用方无需了解内部 builder 的构成。
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes 把本版本所有对外类型登记进 scheme，
// 并补上 metav1 公共类型，否则反序列化 ListMeta 会失败。
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
