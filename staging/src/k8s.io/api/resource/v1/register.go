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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的组名。
const GroupName = "resource.k8s.io"

// SchemeGroupVersion 是用于注册这些对象的组版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

// Resource 接受非限定资源名，返回带组限定的 GroupResource。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// 此处只注册手写函数；生成函数的注册在生成文件中完成。
	// 这样即使生成文件缺失，代码也能编译。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 将打包的类型注册到 scheme。
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes 将已知类型列表添加到给定 scheme。
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

	// 注册适用的 watch 版本。
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
