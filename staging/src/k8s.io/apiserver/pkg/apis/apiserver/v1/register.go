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

// SchemeGroupVersion is group version used to register these objects
//
// 中文说明：组名带 .config. 前缀，明确这些类型只用于配置文件而非 API 资源。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

var (
	// TODO: move SchemeBuilder with zz_generated.deepcopy.go to k8s.io/api.
	// localSchemeBuilder and AddToScheme will stay in k8s.io/kubernetes.
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	AddToScheme        = localSchemeBuilder.AddToScheme
)

func init() {
	// We only register manually written functions here. The registration of the
	// generated functions takes place in the generated files. The separation
	// makes the code compile even when the generated files are missing.
	// 中文说明：手工注册与生成代码分离，便于在没有执行代码生成的环境下编译。
	localSchemeBuilder.Register(addKnownTypes)
	localSchemeBuilder.Register(addDefaultingFuncs)
}

// Adds the list of known types to the given scheme.
//
// 中文说明：EncryptionConfiguration 额外以 core/v1 的 EncryptionConfig 名字注册，
// 是为了兼容早期文档与用户配置中的错误 apiVersion；不能删除，否则存量配置失效。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&AdmissionConfiguration{},
		&AuthenticationConfiguration{},
		&AuthorizationConfiguration{},
		&EncryptionConfiguration{},
		&TracingConfiguration{},
	)
	// also register into the v1 group as EncryptionConfig (due to a docs bug)
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "EncryptionConfig"}, &EncryptionConfiguration{})
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
