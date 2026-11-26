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

package config

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的组名；该组名同时用作 /configz 的注册名，变更会破坏外部配置的兼容性。
const GroupName = "cloudcontrollermanager.config.k8s.io"

var (
	// SchemeGroupVersion 是注册这些对象所使用的 group/version。
	// 内部版本使用 runtime.APIVersionInternal，只用于进程内传递，不对外序列化。
	SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}
	// SchemeBuilder 收集本 API 包需要执行的 scheme 初始化函数。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 是把本 API 组与版本注册到 scheme 的全局入口。
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes 把已知类型注册到给定 scheme。
// 内部类型与外部类型必须一一对应，否则 v1alpha1 转换会失败。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&CloudControllerManagerConfiguration{},
	)
	return nil
}
