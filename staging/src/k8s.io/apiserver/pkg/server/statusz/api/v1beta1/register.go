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

// GroupName 是 statusz 资源所在的 API 组，Version 是该组当前暴露的版本。
const (
	GroupName = "config.k8s.io"
	Version   = "v1beta1"
)

// SchemeGroupVersion is group version used to register these objects
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}

var (
	// SchemeBuilder initializes a scheme builder
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme is a global function that adds this group's types to a scheme
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes 向 scheme 注册本组类型。
// 同时调用 metav1.AddToGroupVersion 注册 ListOptions 等公共元数据操作，
// 否则 discovery 和客户端生成器会缺少这些类型。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Statusz{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
