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

package metadata

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName is the group name for DRA device metadata.
//
// GroupName 是 DRA 设备元数据所属的 API 组名。
const GroupName = "metadata.resource.k8s.io"

// SchemeGroupVersion is the group version used to register the internal types.
//
// SchemeGroupVersion 是用于注册内部（无版本）类型的组版本，版本号为 runtime.APIVersionInternal。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

var (
	// SchemeBuilder is used to add types to the scheme.
	//
	// SchemeBuilder 用于向 scheme 添加类型，这里在构造时就登记了 addKnownTypes 回调函数。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)

	// AddToScheme applies all the stored functions to the scheme.
	//
	// AddToScheme 把 SchemeBuilder 中保存的所有注册函数应用到给定的 scheme 上。
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes 把内部版本的 DeviceMetadata 类型注册到 scheme 的 SchemeGroupVersion 下。
func addKnownTypes(scheme *runtime.Scheme) error {
	// 注册 DeviceMetadata 的内部类型指针，使 scheme 能够识别并转换该类型。
	scheme.AddKnownTypes(SchemeGroupVersion,
		&DeviceMetadata{},
	)
	return nil
}
