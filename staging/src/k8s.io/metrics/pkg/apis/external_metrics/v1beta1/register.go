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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName is the group name use in this package
// 中文说明：外部指标独立于自定义指标，HPA 通过不同的 group 区分两类数据源。
const GroupName = "external.metrics.k8s.io"

// SchemeGroupVersion is group version used to register these objects
// 中文说明：这是当前对外提供的唯一版本，新版本需另行注册。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}

// Resource takes an unqualified resource and returns a Group qualified GroupResource
// 中文说明：补全 group 信息，供请求路径和 scheme 查询统一使用。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder points to a list of functions added to Scheme.
	// 中文说明：集中管理本版本的类型注册逻辑。
	SchemeBuilder      = runtime.NewSchemeBuilder(addKnownTypes)
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme applies all the stored functions to the scheme.
	// 中文说明：通过 local builder 调用，便于未来在本文件内插入更多注册步骤。
	AddToScheme = localSchemeBuilder.AddToScheme
)

// 中文说明：注册外部指标的单值和列表类型，并补上 metav1 内嵌类型。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&ExternalMetricValue{},
		&ExternalMetricValueList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
