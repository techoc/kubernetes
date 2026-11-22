/*
Copyright 2025 The Kubernetes Authors.

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

package v1beta2

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的 API 组名
const GroupName = "resource.k8s.io"

// SchemeGroupVersion 是注册这些对象所用的组版本
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta2"}

// Resource 接受一个不带组名的资源名，返回带组限定的 GroupResource
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder 收集本组类型的注册函数。
	// AddToScheme 将本组类型注册到给定 scheme。
	// 此处只注册手写的函数；生成函数的注册放在生成文件中。
	// 这样即使生成文件缺失，代码也能编译通过。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

// addKnownTypes 向给定 scheme 注册本组的所有已知类型。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&DeviceClass{},
		&DeviceClassList{},
		&DeviceTaintRule{},
		&DeviceTaintRuleList{},
		&ResourceClaim{},
		&ResourceClaimList{},
		&ResourceClaimTemplate{},
		&ResourceClaimTemplateList{},
		&ResourceSlice{},
		&ResourceSliceList{},
	)

	// 注册对应的 watch 版本，供 watches 使用
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
