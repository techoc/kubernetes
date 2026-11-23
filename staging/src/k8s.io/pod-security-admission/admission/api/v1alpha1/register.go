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

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName is the group name use in this package
// 本包名与策略组名分离，配置对象统一挂在该 API 组下。
const GroupName = "pod-security.admission.config.k8s.io"

// SchemeGroupVersion is group version used to register these objects
// alpha 与 beta 共用 group，靠 version 区分，避免解码时类型冲突。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}

var (
	// SchemeBuilder is a pointer used to call AddToScheme
	// 对外暴露 builder，供调用方在自有 scheme 中装配本组类型。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme is used to register the types to API encoding/decoding machinery
	// 它是集成点：外部只需调用此函数，无需了解内部注册细节。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// We only register manually written functions here. The registration of the
	// generated functions takes place in the generated files. The separation
	// makes the code compile even when the generated files are missing.
	// 手写与生成注册分开，可让代码在生成文件缺失时仍通过编译。
	localSchemeBuilder.Register(addKnownTypes, addDefaultingFuncs)
}

// 只注册本版本的配置根对象；子结构随根对象一并参与编解码。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&PodSecurityConfiguration{},
	)
	return nil
}
