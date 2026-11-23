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

// GroupName is the group name use in this package
// 组名同时出现在配置文件 apiVersion 和序列化后的对象里，手改会破坏既有配置兼容性。
const GroupName = "pod-security.admission.config.k8s.io"

// SchemeGroupVersion is group version used to register these objects
// 由 GroupName 派生而非重写字面量，保证注册到的组和常量永远一致。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}

var (
	// SchemeBuilder is a pointer used to call AddToScheme
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme is used to register the types to API encoding/decoding machinery
	// 经 localSchemeBuilder 间接暴露，生成器后续追加注册函数时调用方无需改动。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// We only register manually written functions here. The registration of the
	// generated functions takes place in the generated files. The separation
	// makes the code compile even when the generated files are missing.
	// addDefaultingFuncs 由生成器提供，手写代码只声明引用，生成物缺失时才不会编译失败。
	localSchemeBuilder.Register(addKnownTypes, addDefaultingFuncs)
}

// 只在此处登记 v1beta1 的类型；其它版本的注册由各自包负责，避免一个版本偷偷改动全局 scheme。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&PodSecurityConfiguration{},
	)
	return nil
}
