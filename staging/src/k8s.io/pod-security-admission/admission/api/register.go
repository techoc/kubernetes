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

package api

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是准入配置文件专用的 API 组名。
const GroupName = "pod-security.admission.config.k8s.io"

// SchemeGroupVersion 是内部配置对象的注册版本；APIVersionInternal 表示它不对外暴露。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

var (
	// SchemeBuilder 收集注册函数，供生成代码追加类型转换入口。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 一次性向 scheme 注册全部已知类型。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// 手写函数在此注册，生成函数在生成文件中注册；分开后缺少生成文件也能编译。
	localSchemeBuilder.Register(addKnownTypes)
}

// 只登记内部类型，外部版本的登记交给生成代码，这样缺了生成文件时也能编译。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&PodSecurityConfiguration{},
	)
	return nil
}
