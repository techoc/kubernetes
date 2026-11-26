/*
Copyright 2022 The Kubernetes Authors.

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

// GroupName is the "group" that is needed to uniquely identify the API
const GroupName = "controllermanager.config.k8s.io"

// SchemeGroupVersion 是注册这些对象所用的 group version。
// 对外版本 v1 的 GroupVersion。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

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
// 注册 v1 版本的已知类型，并加入 metav1 的通用类型（如 TypeMeta）。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&LeaderMigrationConfiguration{},
	)
	// 使 scheme 知道该版本下 TypeMeta 等元信息的处理方式。
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
