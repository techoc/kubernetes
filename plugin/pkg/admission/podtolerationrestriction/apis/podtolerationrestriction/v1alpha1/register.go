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
const GroupName = "podtolerationrestriction.admission.k8s.io"

// SchemeGroupVersion 是注册这些对象时使用的组版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}

var (
	// TODO: 将 SchemeBuilder 与 zz_generated.deepcopy.go 移至 k8s.io/api。
	// localSchemeBuilder 和 AddToScheme 将保留在 k8s.io/kubernetes 中。

	// SchemeBuilder 是用于调用 AddToScheme 的指针。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 用于向 API 编解码机制注册类型。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只注册手工编写的函数；生成函数的注册在生成文件中完成。
	// 分离注册使代码在生成文件缺失时也能编译。
	localSchemeBuilder.Register(addKnownTypes, addDefaultingFuncs)
}

// addKnownTypes 向 scheme 注册本包已知的类型。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Configuration{},
	)
	return nil
}
