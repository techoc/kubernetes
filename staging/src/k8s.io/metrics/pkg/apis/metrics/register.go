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

// GroupName 是本包使用的 API 组名称。
// 指标 API 的组名固定为 metrics.k8s.io，外部版本注册时复用它。
const GroupName = "metrics.k8s.io"

// SchemeGroupVersion 是用于注册本包对象的组版本。
// 内部版本不作为线上 API 暴露，只承载版本间转换。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// Kind 接收不限定的 kind，返回包含组名的 GroupKind。
func Kind(kind string) schema.GroupKind {
	return SchemeGroupVersion.WithKind(kind).GroupKind()
}

// Resource 接收不限定的 resource，返回包含组名的 GroupResource。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder 保存本 API 包需要执行的 scheme 注册函数。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 是通用的注册入口，将本包的组版本及类型映射到 scheme。
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes 只注册内部版本的指标类型，metav1 内嵌类型由编解码层另行处理。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&NodeMetrics{},
		&NodeMetricsList{},
		&PodMetrics{},
		&PodMetricsList{},
	)
	return nil
}
