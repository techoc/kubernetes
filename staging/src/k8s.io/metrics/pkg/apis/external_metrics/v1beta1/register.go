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

// GroupName 是本包使用的 API 组名称。
// 外部指标独立于自定义指标，HPA 通过不同的组区分两类数据源。
const GroupName = "external.metrics.k8s.io"

// SchemeGroupVersion 是用于注册本包对象的组版本。
// 这是当前对外提供的唯一版本，新版本需另行注册。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}

// Resource 接收不限定的 resource，返回包含组名的 GroupResource。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder 保存需要添加到 Scheme 的注册函数。
	// 集中管理本版本的类型注册逻辑。
	SchemeBuilder      = runtime.NewSchemeBuilder(addKnownTypes)
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 将所有已注册的函数应用到 scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)

// addKnownTypes 注册外部指标的单值和列表类型，并补上 metav1 内嵌类型。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&ExternalMetricValue{},
		&ExternalMetricValueList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
