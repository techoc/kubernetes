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

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包 API 所属的组名，kubelet 配置对象都归在 kubelet.config.k8s.io 下。
const GroupName = "kubelet.config.k8s.io"

// SchemeGroupVersion 把组名与 v1alpha1 版本号组合成 scheme 注册这些对象时使用的 GroupVersion。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}

var (
	// SchemeBuilder 延迟收集本包的 scheme 初始化函数，避免包初始化顺序带来的副作用。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 是 SchemeBuilder 的公开入口，调用方通过它把本 API 组与版本注册到自有 scheme。
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes 把本包的已知类型注册到给定 scheme：只列出本版本实际存在的对象，
// scheme 依赖它完成序列化时的类型查找。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&CredentialProviderConfig{},
		&ImagePullIntent{},
		&ImagePulledRecord{},
	)
	return nil
}
