/*
Copyright 2019 The Kubernetes Authors.

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

// GroupName 是唯一标识该 API 所需的 group 名。
const GroupName = "controllermanager.config.k8s.io"

// SchemeGroupVersion 是注册这些对象所用的 group version。
// v1alpha1 版本的 GroupVersion。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}

var (
	// SchemeBuilder 收集本 API 包需要执行的 scheme 初始化函数。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// localSchemeBuilder 在 SchemeBuilder 上追加外部类型；本包还注册
	// 默认值与转换的初始化函数。
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 是把本 API 组与版本注册到 scheme 的全局入口。
	AddToScheme = localSchemeBuilder.AddToScheme
)

// 把已知类型列表注册到给定 scheme。
// 注册 v1alpha1 的类型以及默认化/转换所需的元信息。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&LeaderMigrationConfiguration{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
