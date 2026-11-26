/*
Copyright 2017 The Kubernetes Authors.

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

// GroupName 是旧组名，仅用于 AdmissionConfiguration 与 EgressSelectorConfiguration 的兼容。
const GroupName = "apiserver.k8s.io"

// ConfigGroupName 是配置类 API 的组名，用于认证/授权/追踪等配置。
const ConfigGroupName = "apiserver.config.k8s.io"

// SchemeGroupVersion 是用于注册这些对象的组版本
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}

// ConfigSchemeGroupVersion 是用于注册这些对象的组版本
var ConfigSchemeGroupVersion = schema.GroupVersion{Group: ConfigGroupName, Version: "v1alpha1"}

var (
	// TODO: 将 SchemeBuilder 与 zz_generated.deepcopy.go 一起移到 k8s.io/api。
	// localSchemeBuilder 和 AddToScheme 留在 k8s.io/kubernetes。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	AddToScheme        = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只注册手写的函数；生成函数的注册在生成的文件中进行。
	// 分离两者可使生成文件缺失时代码仍能编译。
	//
	// 默认值函数与类型注册分开登记，生成文件缺失时仍可编译（只是缺少生成部分）。
	localSchemeBuilder.Register(addKnownTypes, addDefaultingFuncs)
}

// 将已知类型列表添加到给定 scheme。
func addKnownTypes(scheme *runtime.Scheme) error {
	// 两组分别注册，保证同一 Go 类型可以出现在不同组名下而互不覆盖。
	scheme.AddKnownTypes(SchemeGroupVersion,
		&AdmissionConfiguration{},
		&EgressSelectorConfiguration{},
	)
	scheme.AddKnownTypes(ConfigSchemeGroupVersion,
		&AuthenticationConfiguration{},
		&AuthorizationConfiguration{},
		&TracingConfiguration{},
	)
	// 注册 TypeMeta 等通用元数据类型，否则解码时无法识别 apiVersion/kind。
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	metav1.AddToGroupVersion(scheme, ConfigSchemeGroupVersion)
	return nil
}
