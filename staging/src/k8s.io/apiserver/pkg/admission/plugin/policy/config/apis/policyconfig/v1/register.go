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

package v1

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的 API 组名。
// 准入策略配置使用单独的 API 组，避免与 workload 资源组混淆；
// 该组只承载配置文件，不通过 apiserver 对外提供 REST 端点。
const GroupName = "apiserver.config.k8s.io"

// SchemeGroupVersion 是本包注册对象使用的组版本。
// 当前唯一对外暴露的配置版本；内部版本间的转换由 install 包注册。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

var (
	// TODO: 把 SchemeBuilder（连同 zz_generated.deepcopy.go）移到 k8s.io/api。
	// localSchemeBuilder 和 AddToScheme 会留在 k8s.io/kubernetes。
	// SchemeBuilder 由生成代码与手写注册函数共同填充；localSchemeBuilder
	// 让生成代码只依赖本包，而 AddToScheme 供外部安装。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	AddToScheme        = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只注册手写的函数；生成函数的注册在生成文件中进行，
	// 这样即使生成文件缺失，本包仍可编译，便于 bootstrap 与代码生成。
	localSchemeBuilder.Register(addKnownTypes)
}

// addKnownTypes 把 v1 配置类型注册进 scheme，供解码与转换使用。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&ValidatingAdmissionPolicyConfiguration{},
		&MutatingAdmissionPolicyConfiguration{},
	)
	return nil
}
