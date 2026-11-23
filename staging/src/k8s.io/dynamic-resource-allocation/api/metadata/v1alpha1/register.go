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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName is the group name for DRA device metadata.
//
// GroupName 是 DRA 设备元数据所属的 API 组名。
const GroupName = "metadata.resource.k8s.io"

// SchemeGroupVersion is the group version for v1alpha1 metadata types.
//
// SchemeGroupVersion 是 v1alpha1 元数据类型对应的组版本（组名 + "v1alpha1" 版本号）。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}

var (
	// localSchemeBuilder is used to add types and conversion functions to the scheme.
	// The generated conversion code references this variable by name.
	//
	// localSchemeBuilder 用于向 scheme 添加类型和转换函数。生成的转换代码会按名称引用该变量，
	// 因此不能随意改名；构造时登记了 addKnownTypes 回调。
	localSchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme applies all the stored functions to the scheme.
	//
	// AddToScheme 把 localSchemeBuilder 中保存的所有注册函数应用到给定的 scheme 上。
	AddToScheme = localSchemeBuilder.AddToScheme
)

// addKnownTypes 把 v1alpha1 版本的 DeviceMetadata 类型注册到 scheme 的 SchemeGroupVersion 下，
// 并调用 metav1.AddToGroupVersion 注册该组版本对应的元类型（如 WatchEvent 等）。
func addKnownTypes(scheme *runtime.Scheme) error {
	// 注册 v1alpha1 版本类型指针，使 scheme 可以识别该版本的 DeviceMetadata。
	scheme.AddKnownTypes(SchemeGroupVersion,
		&DeviceMetadata{},
	)
	// 为该组版本注册通用元类型，保证 scheme 能正确处理该版本的元数据事件等对象。
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
