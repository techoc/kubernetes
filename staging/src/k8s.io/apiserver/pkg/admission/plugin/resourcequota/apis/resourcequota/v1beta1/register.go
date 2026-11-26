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
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的组名。
//
// ResourceQuota 准入配置的组名。与主 resourcequota API 分离，
// 因为这是 API Server 侧插件配置，生命周期与集群对象不同。
const GroupName = "resourcequota.admission.k8s.io"

// SchemeGroupVersion 是用于注册这些对象的组版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}

var (
	// TODO: 把 SchemeBuilder 与 zz_generated.deepcopy.go 移到 k8s.io/api。
	// localSchemeBuilder 与 AddToScheme 保留在 k8s.io/kubernetes，避免生成的 scheme 代码被外部包覆盖。

	// SchemeBuilder 是指针，供调用方通过它执行 AddToScheme。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 把这些类型注册到 API 编解码机制中。
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只注册手写的函数；生成函数的注册在生成文件里完成。
	// 使生成文件缺失时代码也能编译。
	//
	// 默认值函数与类型注册一并挂载；默认值在解码配置时生效，
	// 因此必须在 scheme 层面而非使用点处理，保证所有入口行为一致。
	localSchemeBuilder.Register(addKnownTypes, addDefaultingFuncs)
}

// addKnownTypes 注册 Configuration 类型。组内只有这一个类型，
// 后续若要扩展配置需新增 kind，且不能改变既有 kind 名以保持配置兼容。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Configuration{},
	)
	return nil
}
