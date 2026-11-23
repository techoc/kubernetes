/*
Copyright 2020 The Kubernetes Authors.

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

package credentialprovider

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName is the group name use in this package
//
// 内部版本与对外版本共用组名，转换函数按 GVK 在它们之间映射。
const GroupName = "credentialprovider.kubelet.k8s.io"

// SchemeGroupVersion is group version used to register these objects
//
// 用 APIVersionInternal 标记内部版本：它只在进程内流转，不对外暴露。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: runtime.APIVersionInternal}

var (
	// SchemeBuilder 汇总内部版本类型注册，AddToScheme 供转换代码初始化 scheme。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	AddToScheme   = SchemeBuilder.AddToScheme
)

// addKnownTypes 只注册内部类型；这里不调用 metav1.AddToGroupVersion，
// 因为内部版本不参与对外的 TypeMeta 序列化。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&CredentialProviderRequest{},
		&CredentialProviderResponse{},
	)
	return nil
}
