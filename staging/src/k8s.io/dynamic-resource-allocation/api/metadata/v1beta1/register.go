/*
Copyright The Kubernetes Authors.

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

// GroupName is the group name for DRA device metadata.
//
// GroupName 是 DRA 设备元数据 API 的组名。
const GroupName = "metadata.resource.k8s.io"

// SchemeGroupVersion is the group version for v1beta1 metadata types.
//
// SchemeGroupVersion 是 v1beta1 元数据的组版本标识，供 scheme 注册和版本转换使用。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}

var (
	// localSchemeBuilder is used to add types and conversion functions to the scheme.
	// The generated conversion code references this variable by name.
	//
	// localSchemeBuilder 把 addKnownTypes 累积进 scheme 构造器；生成的转换代码按变量名引用它。
	localSchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme applies all the stored functions to the scheme.
	//
	// AddToScheme 是构造器入口，调用后会把登记的函数全部应用到目标 scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)

// addKnownTypes 向 scheme 注册 v1beta1 版本的 DeviceMetadata 类型以及 meta v1 的公共类型。
func addKnownTypes(scheme *runtime.Scheme) error {
	// 只注册本包唯一的顶层 API 类型 DeviceMetadata。
	scheme.AddKnownTypes(SchemeGroupVersion,
		&DeviceMetadata{},
	)
	// 补注册 meta v1 的公共类型（如 ListOptions、Time 等），保证 scheme 完整可用。
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	// 注册过程不会失败，始终返回 nil。
	return nil
}
