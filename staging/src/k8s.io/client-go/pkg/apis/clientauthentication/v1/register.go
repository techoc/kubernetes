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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的 API group 名。
//
// 这是 client-go 私有的 API 组：只用于 exec 插件与 client-go 进程之间的本地协议，
// 不会在 apiserver 上暴露。
const GroupName = "client.authentication.k8s.io"

// SchemeGroupVersion 是注册这些对象所用的 group version。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

// Resource 把裸资源名补全为带 group 的 GroupResource。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	// SchemeBuilder 汇总本包的所有注册函数，由 generated 代码与 addKnownTypes 共同填充。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 把本组版本的类型注册进给定的 runtime.Scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只注册手写函数，生成函数的注册放在生成文件中。两者分离，使得生成文件
	// 缺失时本包仍能编译。
	//
	// 这种分离让本包在未生成 zz_generated 文件时也能编译通过，便于代码生成前的引导。
	localSchemeBuilder.Register(addKnownTypes)
}

// addKnownTypes 注册本组版本下的手写类型，并补上 metav1 通用类型与版本元数据。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&ExecCredential{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
