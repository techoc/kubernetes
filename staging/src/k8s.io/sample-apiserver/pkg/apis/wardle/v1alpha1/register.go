/*
Copyright 2016 The Kubernetes Authors.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的 API 组名。
// 组名就是 CRD 的 apiGroup，改动会让已持久化对象的 GVK 失配。
const GroupName = "wardle.example.com"

// SchemeGroupVersion 是注册本包对象所用的组版本。
// 外部版本标识，序列化到 etcd 和响应体里的就是这个版本号。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}

// 手写注册与生成代码分层，生成文件缺失时包仍然能编译。
var (
	// TODO：将 SchemeBuilder 与 zz_generated.deepcopy.go 移至 k8s.io/api。
	// localSchemeBuilder 和 AddToScheme 将保留在 k8s.io/kubernetes。
	// SchemeBuilder 保存本 API 包需要在 Scheme 上执行的注册函数。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 把本包的组版本及类型注册到 Scheme。
	// 对外只暴露这个入口，调用方不必关心内部 builder。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只挂手写函数，生成函数由生成文件自行注册；缺生成文件时仍能编译。
	localSchemeBuilder.Register(addKnownTypes, addDefaultingFuncs)
}

// addKnownTypes 把本版本的已知类型注册到 Scheme。
// 顺带注册元类型，让 scheme 认识 ListOptions 等通用对象。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Flunder{},
		&FlunderList{},
		&Fischer{},
		&FischerList{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}

// Resource 为未限定组的资源名补上本 API 组，返回 GroupResource。
// 把资源名限定到本组，避免与其他 API 组的同名资源混淆。
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}
