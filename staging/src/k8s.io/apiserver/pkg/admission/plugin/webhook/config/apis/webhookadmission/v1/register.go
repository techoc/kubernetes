/*
Copyright 2019 The Kubernetes Authors.

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
//
// 配置 API 组名。使用 apiserver.config.k8s.io 而非主 API 组，
// 是因为这类配置只被 API Server 自身读取，不通过 REST 端点暴露给普通客户端。
const GroupName = "apiserver.config.k8s.io"

// SchemeGroupVersion 是注册这些对象时使用的组版本。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

var (
	// TODO: 把 SchemeBuilder 连同 zz_generated.deepcopy.go 一起移到 k8s.io/api。
	// localSchemeBuilder 与 AddToScheme 会留在 k8s.io/kubernetes 中。
	//
	// SchemeBuilder 是类型注册的聚合点；localSchemeBuilder 让 AddToScheme
	// 绑定到本包的 builder，避免直接暴露可变全局状态。AddToScheme 供调用方把本组
	// 类型加入任意 scheme。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	AddToScheme        = localSchemeBuilder.AddToScheme
)

func init() {
	// 这里只注册手写的函数。生成函数的注册在生成文件中完成。
	// 这样拆分后，即使生成文件缺失，代码仍能编译。
	//
	// 手写注册与生成注册分离，使生成文件缺失时仍可编译——
	// 这对开发期与代码生成工具的自举很重要。
	localSchemeBuilder.Register(addKnownTypes)
}

// addKnownTypes 只注册 WebhookAdmission 一个类型；kind 名与 Go 类型名不同，
// 是因为配置文件里的 kind 是 WebhookAdmissionConfiguration，必须保持兼容。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypeWithName(SchemeGroupVersion.WithKind("WebhookAdmissionConfiguration"),
		&WebhookAdmission{},
	)
	return nil
}
