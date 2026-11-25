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

// GroupName 保存本包的 API 组名。
// 示例组名用 example.com 后缀，避免和真实 API 组冲突。
const GroupName = "wardle.example.com"

// SchemeGroupVersion 是注册本包对象所用的组版本。
// 同一组下多版本共存时，注册和序列化都以它为身份标识。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}

var (
	// SchemeBuilder 保存本 API 包需要在 Scheme 上执行的注册函数。
	// TODO：将 SchemeBuilder 与 zz_generated.deepcopy.go 移至 k8s.io/api。
	// localSchemeBuilder 和 AddToScheme 将保留在 k8s.io/kubernetes。
	// 包变量而非 init 里直接注册，调用方可以在多个 Scheme 上重复安装。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder

	// AddToScheme 把本 API 组注册到 Scheme。
	// 暴露函数值而不是包级函数，保持与生成代码一致的注册入口。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// 手写和生成注册分开，缺生成文件时仍能编译，方便排查生成链问题。
	localSchemeBuilder.Register(addKnownTypes)
}

// addKnownTypes 把本版本的已知类型注册到 Scheme。
// 每新增一个该版本的类型都要加进这里，否则序列化时会报 unregistered type。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Flunder{},
		&FlunderList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}

// Resource 为未限定组的资源名补上本 API 组，返回 GroupResource。
// 让存储层和校验错误都带上完整组名，避免不同组的同名资源混淆。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}
