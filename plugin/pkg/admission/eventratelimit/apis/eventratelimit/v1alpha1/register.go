/*
Copyright 2017 The Kubernetes Authors.

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

// GroupName 是本包使用的 API 组名。
const GroupName = "eventratelimit.admission.k8s.io"

// SchemeGroupVersion 是注册这些对象时使用的组版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}

var (
	// TODO: move SchemeBuilder with zz_generated.deepcopy.go to k8s.io/api.
	// localSchemeBuilder and AddToScheme will stay in k8s.io/kubernetes.

	// SchemeBuilder 是用于调用 AddToScheme 的构建器。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 用于将类型注册到 API 编解码机制。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只注册手写的函数；生成函数的注册放在生成文件中。
	// 分离注册使生成文件缺失时代码仍能编译。
	localSchemeBuilder.Register(addKnownTypes, addDefaultingFuncs)
}

// addKnownTypes 将本组的类型添加到 scheme。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Configuration{},
	)
	return nil
}
