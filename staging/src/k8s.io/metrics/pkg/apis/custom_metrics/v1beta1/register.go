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

// GroupName 是本包使用的 API 组名称。
// 自定义指标位于独立 API 组，与核心资源和 metrics.k8s.io 分开。
const GroupName = "custom.metrics.k8s.io"

// SchemeGroupVersion 是用于注册本包对象的组版本。
// 该版本用于编解码 v1beta1 的自定义指标对象。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}

// Resource 接收不限定的 resource，返回包含组名的 GroupResource。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder 保存需要添加到 Scheme 的注册函数。
	// 集中登记本版本的已知类型，新增类型时只需修改 addKnownTypes。
	SchemeBuilder      = runtime.NewSchemeBuilder(addKnownTypes)
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 将所有已注册的函数应用到 scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)

// addKnownTypes 注册指标实体、列表和查询选项，
// 并加入 metav1 公共类型以正确解析 TypeMeta 和 ListMeta。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&MetricValue{},
		&MetricValueList{},
		&MetricListOptions{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
