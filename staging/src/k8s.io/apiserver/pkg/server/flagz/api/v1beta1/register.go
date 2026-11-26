/*
Copyright The Kubernetes Authors.

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
	"k8s.io/apimachinery/pkg/runtime/schema"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtime "k8s.io/apimachinery/pkg/runtime"
)

const (
	// GroupName 是 flagz 使用的 API 组；Version 固定为 v1beta1。
	GroupName = "config.k8s.io"
	Version   = "v1beta1"
)

// SchemeGroupVersion 是注册这些对象时使用的组版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}

var (
	// SchemeBuilder initializes a scheme builder
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// 把本组的类型注册到 scheme 的全局函数。
	AddToScheme = SchemeBuilder.AddToScheme
)

func addKnownTypes(scheme *runtime.Scheme) error {
	// 只注册 Flagz；metav1.AddToGroupVersion 负责注册 ListOptions 等元类型。
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Flagz{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
