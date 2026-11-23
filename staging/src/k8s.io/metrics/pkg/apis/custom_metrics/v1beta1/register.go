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
// 自定义指标挂在独立 API 组下，与核心资源和 metrics.k8s.io 分开。
const GroupName = "custom.metrics.k8s.io"

// SchemeGroupVersion is group version used to register these objects
// 该版本只用于编解码 v1beta1 的自定义指标对象。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}

// Resource takes an unqualified resource and returns a Group qualified GroupResource
// 补全组名后交给 REST 映射，避免调用方手拼字符串。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder points to a list of functions added to Scheme.
	// 集中登记本版本的已知类型，新增类型只改 addKnownTypes。
	SchemeBuilder      = runtime.NewSchemeBuilder(addKnownTypes)
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme applies all the stored functions to the scheme.
	// 暴露统一入口，内部 builder 细节不外泄。
	AddToScheme = localSchemeBuilder.AddToScheme
)

// addKnownTypes 同时注册指标实体、列表和查询选项，
// 并加入 metav1 公共类型以正确解析 TypeMeta/ListMeta。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&MetricValue{},
		&MetricValueList{},
		&MetricListOptions{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
