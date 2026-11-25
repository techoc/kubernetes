/*
Copyright 2020 The Kubernetes Authors.

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

package config

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是用于唯一标识 API 对象类型的组名，与版本组合后定位具体类型。
const GroupName = "controllermanager.config.k8s.io"

// SchemeGroupVersion 是注册这些对象所用的 group/version。
// 内部版本的 GroupVersion：Version 为 "__internal"，只用于进程内转换，不对外序列化。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

var (
	// SchemeBuilder 是携带本 API 包各 scheme 初始化函数的构建器，AddToScheme 时依次执行。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// localSchemeBuilder 是 SchemeBuilder 的本地别名：外部版本的 defaulting/conversion
	// 初始化函数也会注册到这里。
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 是全局入口函数，调用它把本 API 组/版本注册到给定 scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)

// addKnownTypes 把本包定义的已知类型列表注册进给定 scheme，使其可被序列化/反序列化与转换。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		// 目前内部版本只需要注册领导者迁移配置这一种类型。
		&LeaderMigrationConfiguration{},
	)
	return nil
}
