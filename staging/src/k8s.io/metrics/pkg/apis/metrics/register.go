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

package metrics

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName is the group name use in this package
// 中文说明：指标 API 的 group 名固定为 metrics.k8s.io，外部版本注册时复用它。
const GroupName = "metrics.k8s.io"

// SchemeGroupVersion is group version used to register these objects
// 中文说明：内部版本不作为线上 API 暴露，只承载版本间转换。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Kind takes an unqualified kind and returns back a Group qualified GroupKind
// 中文说明：把裸 Kind 补全 group，用于构造 API 请求路径或做类型判断。
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource takes an unqualified resource and returns back a Group qualified GroupResource
// 中文说明：与 Kind 成对，避免不同调用方各自拼接 group/resource 字符串。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder is the scheme builder with scheme init functions to run for this API package
	// 中文说明：集中管理本包的注册函数，方便聚合到更大的 scheme。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme is a common registration function for mapping packaged scoped group & version keys to a scheme
	// 中文说明：对外暴露统一入口，调用方无需了解内部注册步骤。
	AddToScheme = SchemeBuilder.AddToScheme
)

// 中文说明：只注册内部版本的指标类型；metav1 内嵌类型由编解码层另行处理。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&NodeMetrics{},
		&NodeMetricsList{},
		&PodMetrics{},
		&PodMetricsList{},
	)
	return nil
}
