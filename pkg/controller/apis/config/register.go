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

// GroupName 是本包使用的 API 组名。
const GroupName = "kubecontrollermanager.config.k8s.io"

// SchemeGroupVersion 是注册本包对象所用的组版本，使用内部版本（APIVersionInternal）。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

var (
	// SchemeBuilder 持有本 API 包需要执行的 scheme 初始化函数（此处为 addKnownTypes）。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 是将本 API 组与版本注册到给定 scheme 的全局函数。
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes 向给定 scheme 注册本包已知的类型，
// 使 KubeControllerManagerConfiguration 能够在内部版本下被序列化/转换。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&KubeControllerManagerConfiguration{},
	)
	return nil
}
