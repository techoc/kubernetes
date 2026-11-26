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

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的组名
const GroupName = "audit.k8s.io"

// SchemeGroupVersion 是注册这些对象所用的组版本
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

// Resource 接受非限定资源名，返回带组名的 GroupResource
func Resource(resource string) schema.GroupResource {
	return SchemeGroupVersion.WithResource(resource).GroupResource()
}

var (
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	AddToScheme        = localSchemeBuilder.AddToScheme
)

func init() {
	// 此处只注册手写函数，生成函数的注册位于生成文件中。
	// 两者分离使生成文件缺失时代码仍可编译。
	// 同时避免生成器覆盖手写注册内容。
	//
	// 手工编写的注册与 zz_generated 中的注册分开，保证在生成文件尚未产出时
	// 包仍可编译，也避免生成器覆盖这里的内容。
	localSchemeBuilder.Register(addKnownTypes)
}

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Event{},
		&EventList{},
		&Policy{},
		&PolicyList{},
	)
	// 注册 metav1 的公共类型（如 List/Status/DeleteOptions），否则这些类型
	// 在 audit.k8s.io/v1 下无法编解码。
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
