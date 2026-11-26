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

package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const GroupName = "apiserver.k8s.io"
const ConfigGroupName = "apiserver.config.k8s.io"

// SchemeGroupVersion 是注册这些对象所使用的组版本。
// 这是“运行时对象”使用的组，与配置文件组 apiserver.config.k8s.io
// 分开，避免配置对象被当作普通 API 资源暴露。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}

// ConfigSchemeGroupVersion 是注册这些对象所用的组版本
var ConfigSchemeGroupVersion = schema.GroupVersion{Group: ConfigGroupName, Version: "v1beta1"}

var (
	// TODO: 将 SchemeBuilder 与 zz_generated.deepcopy.go 一起移到 k8s.io/api。
	// localSchemeBuilder 和 AddToScheme 会留在 k8s.io/kubernetes。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	AddToScheme        = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只注册手工编写的函数，生成函数的注册发生在生成文件中。
	// 手写注册与生成代码分离，保证未运行生成器时包仍可编译。
	localSchemeBuilder.Register(addKnownTypes, addDefaultingFuncs)
}

// 把已知类型列表添加到给定的 scheme。
// EgressSelectorConfiguration 属于运行时组，其余三个是配置文件组；
// 两组分别注册，避免 scheme 中出现跨组混淆。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&EgressSelectorConfiguration{},
	)
	scheme.AddKnownTypes(ConfigSchemeGroupVersion,
		&AuthenticationConfiguration{},
		&AuthorizationConfiguration{},
		&TracingConfiguration{},
	)
	metav1.AddToGroupVersion(scheme, SchemeGroupVersion)
	return nil
}
