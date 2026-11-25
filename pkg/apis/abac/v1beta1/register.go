/*
Copyright 2015 The Kubernetes Authors.

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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/kubernetes/pkg/apis/abac"
)

// GroupName 是本包对应的 API 组名。
const GroupName = "abac.authorization.kubernetes.io"

// SchemeGroupVersion 是 abac v1beta1 的 API 组与版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}

func init() {
	// TODO: Delete this init function, abac should not have its own scheme.
	utilruntime.Must(addKnownTypes(abac.Scheme))

	utilruntime.Must(RegisterConversions(abac.Scheme))
}

var (
	// SchemeBuilder 是本包的 scheme 构造器，保存了需要执行的初始化函数。
	// TODO: 将 SchemeBuilder 与 zz_generated.deepcopy.go 一起移到 k8s.io/api。
	SchemeBuilder runtime.SchemeBuilder
	// localSchemeBuilder 是指向 SchemeBuilder 实例的指针；使用 localSchemeBuilder
	// 时，defaulting 和 conversion 初始化函数也会被一并注册。
	// localSchemeBuilder 和 AddToScheme 仍留在 k8s.io/kubernetes。
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 是通用的注册函数，把本包的 group/version 与类型映射到 scheme。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只手动注册手写函数，生成器负责注册其余部分：
	// 在生成文件中完成。这种分离保证
	// 即使生成文件缺失，代码也能编译
	localSchemeBuilder.Register(addKnownTypes, RegisterDefaults)
}

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Policy{},
	)
	return nil
}
