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

// GroupName is the "group" that is needed to uniquely identify the API
// 该 API 组的名字，与版本组合后用于唯一标识对象类型。
const GroupName = "controllermanager.config.k8s.io"

// SchemeGroupVersion is group version used to register these objects
// 内部版本的 GroupVersion：Version 为 "__internal"，只用于进程内转换，不对外序列化。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

var (
	// SchemeBuilder is the scheme builder with scheme init functions to run for this API package
	// SchemeBuilder 保存一组初始化函数，AddToScheme 时依次执行。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// localSchemeBuilder extends the SchemeBuilder instance with the external types. In this package,
	// defaulting and conversion init funcs are registered as well.
	// 本地别名：外部版本的 defaulting/conversion 初始化函数也会注册到这里。
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme is a global function that registers this API group & version to a scheme
	// 对外暴露的入口，调用它把本 API 组/版本注册到给定 scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)

// Adds the list of known types to the given scheme.
// 把本包定义的类型注册进 scheme，使其可被序列化/反序列化与转换。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		// 目前内部版本只需要注册领导者迁移配置这一种类型。
		&LeaderMigrationConfiguration{},
	)
	return nil
}
