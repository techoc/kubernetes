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

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的 API 组名。
// 独立组名避免与其它组件配置类型在 scheme 中冲突。
const GroupName = "kubeproxy.config.k8s.io"

// SchemeGroupVersion 是本包类型注册到 scheme 时使用的组版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}

var (
	// SchemeBuilder 保存本包的 scheme 初始化函数。
	// 这里只注册已知类型；到内部组件配置的转换由 component-base 解码流程处理。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 是 SchemeBuilder.AddToScheme 的别名，用于把本组版本注册到指定 scheme。
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes 把本版本支持的类型登记到 scheme 的 SchemeGroupVersion 下。
// scheme 据此把组版本和 Kind 映射到 Go 类型；组件配置不走 apiserver watch/list，无需注册元类型。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&KubeProxyConfiguration{},
	)
	return nil
}
