/*
Copyright 2025 The Kubernetes Authors.

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
	"k8s.io/apimachinery/pkg/runtime/schema"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
)

const (
	// GroupName 是 statusz API 的 group。
	GroupName = "config.k8s.io"
	// Version 是 statusz API 的版本。
	Version = "v1alpha1"
)

// SchemeGroupVersion 是用于注册这些对象的 group version
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}

var (
	// SchemeBuilder 初始化 scheme builder
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 把本 group 的类型加入 scheme 的全局函数
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes 注册本版本的类型，并加入 metav1 的 TypeMeta/ListMeta 支持。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Statusz{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
