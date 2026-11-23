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

// GroupName is the group name use in this package
// 本组名专供准入配置文件使用，不与 PodSecurity 命名空间标签的那套常量混用。
const GroupName = "pod-security.admission.config.k8s.io"

// SchemeGroupVersion is group version used to register these objects
// 内部版本用 APIVersionInternal，表示这是进程内表示而非对外暴露的版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

var (
	// SchemeBuilder is a pointer used to call AddToScheme
	// 把注册函数挂在这个指针上，生成的类型转换代码才能复用同一套注册入口。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme is used to register the types to API encoding/decoding machinery
	// 暴露给调用方一次性把所有已知类型装进 scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// We only register manually written functions here. The registration of the
	// generated functions takes place in the generated files. The separation
	// makes the code compile even when the generated files are missing.
	localSchemeBuilder.Register(addKnownTypes)
}

// 只登记内部类型，外部版本的登记交给生成代码，这样缺了生成文件时也能编译。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&PodSecurityConfiguration{},
	)
	return nil
}
