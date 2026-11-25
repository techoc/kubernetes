/*
Copyright 2021 The Kubernetes Authors.

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

package v1beta1

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是配置对象所属的 API 组名，修改会破坏既有配置兼容性。
const GroupName = "pod-security.admission.config.k8s.io"

// SchemeGroupVersion 是 v1beta1 配置对象的注册版本，组名从 GroupName 派生以避免不一致。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}

var (
	// SchemeBuilder 收集本版本的手写与生成注册函数。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 将本版本类型注册到编解码 scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// 手写与生成注册分开，缺少生成文件时本包仍可编译。
	localSchemeBuilder.Register(addKnownTypes, addDefaultingFuncs)
}

// 只在此处登记 v1beta1 的类型；其它版本的注册由各自包负责，避免一个版本偷偷改动全局 scheme。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&PodSecurityConfiguration{},
	)
	return nil
}
