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

package custom_metrics

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的 API 组名称。
// 各版本共用同一组名，仅通过 Version 区分，因此集中定义在此。
const GroupName = "custom.metrics.k8s.io"

// SchemeGroupVersion 是用于注册本包对象的组版本。
// 内部版本只存在于进程内，不出现在 API 响应中，仅作为跨版本转换的中转。
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
	// SchemeBuilder 注册内部版本类型，供转换函数在内存中使用。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 将本包的注册函数应用到 scheme。
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes 只注册内部版本类型，外部版本由各自的 register.go 负责。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&MetricValue{},
		&MetricValueList{},
		&MetricListOptions{},
	)
	return nil
}
