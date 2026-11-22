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

package apiserver

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// LegacyGroupName 是早期组件配置使用的组名，仅保留以兼容旧配置文件；
// 新配置应使用 GroupName。
const LegacyGroupName = "apiserver.k8s.io"

// GroupName 是组件配置 API 的当前组名。
const GroupName = "apiserver.config.k8s.io"

// LegacySchemeGroupVersion is group version used to register these objects
var LegacySchemeGroupVersion = schema.GroupVersion{Group: LegacyGroupName, Version: runtime.APIVersionInternal}

// SchemeGroupVersion is group version used to register these objects
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

// SchemeBuilder 收集本包的手写注册函数；内部版本不生成默认值/转换函数。
var (
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

// Adds the list of known types to the given scheme.
func addKnownTypes(scheme *runtime.Scheme) error {
	// 旧组只注册历史上存在的两种配置，避免旧组名下意外暴露新类型。
	scheme.AddKnownTypes(LegacySchemeGroupVersion,
		&AdmissionConfiguration{},
		&EgressSelectorConfiguration{},
	)
	scheme.AddKnownTypes(SchemeGroupVersion,
		&AdmissionConfiguration{},
		&AuthenticationConfiguration{},
		&AuthorizationConfiguration{},
		&EncryptionConfiguration{},
		&EgressSelectorConfiguration{},
		&TracingConfiguration{},
	)
	return nil
}
