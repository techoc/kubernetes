/*
Copyright 2019 The Kubernetes Authors.

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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const GroupName = "apiserver.config.k8s.io"

// SchemeGroupVersion 是注册这些对象所使用的组版本。
// 组名带 .config. 前缀，明确这些类型只用于配置文件而非 API 资源。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

var (
	// TODO: 将 SchemeBuilder 与 zz_generated.deepcopy.go 一起移到 k8s.io/api。
	// localSchemeBuilder 和 AddToScheme 留在 k8s.io/kubernetes。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	AddToScheme        = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只注册手工编写的函数，生成函数的注册发生在生成文件中。
	// 手工注册与生成代码分离，便于在没有执行代码生成的环境下编译。
	localSchemeBuilder.Register(addKnownTypes)
	localSchemeBuilder.Register(addDefaultingFuncs)
}

// 把已知类型列表添加到给定的 scheme。
// EncryptionConfiguration 额外以 core/v1 的 EncryptionConfig 名字注册，
// 是为了兼容早期文档与用户配置中的错误 apiVersion；不能删除，否则存量配置失效。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&AdmissionConfiguration{},
		&AuthenticationConfiguration{},
		&AuthorizationConfiguration{},
		&EncryptionConfiguration{},
		&TracingConfiguration{},
	)
	// 也注册到 v1 组中作为 EncryptionConfig（因为文档 bug）
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "EncryptionConfig"}, &EncryptionConfiguration{})
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
