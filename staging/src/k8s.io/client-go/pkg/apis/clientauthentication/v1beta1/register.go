/*
Copyright 2018 The Kubernetes Authors.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的 API group 名：client.authentication.k8s.io，即 exec 凭据对象所属的 group。
const GroupName = "client.authentication.k8s.io"

// SchemeGroupVersion 是注册这些对象所用的 group version。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}

// Resource 把裸资源名补全为带 group 的 GroupResource，便于构造错误或 REST 路径。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder 收集本包所有注册函数，方便外部按需 AddToScheme。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	AddToScheme        = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只注册手写的函数；生成函数的注册在生成文件中进行。
	// 手写注册与生成代码分离，保证在未生成 deepcopy/defaults 等文件时仍可编译。
	localSchemeBuilder.Register(addKnownTypes)
}

// addKnownTypes 把本版本的类型注册到 scheme，并补充 metav1 通用类型。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&ExecCredential{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
