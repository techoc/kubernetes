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

// GroupName is the group name use in this package
//
// 中文说明：配置 API 组名。使用 apiserver.config.k8s.io 而非主 API 组，
// 是因为这类配置只被 API Server 自身读取，不通过 REST 端点暴露给普通客户端。
const GroupName = "apiserver.config.k8s.io"

// SchemeGroupVersion is group version used to register these objects
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

var (
	// TODO: move SchemeBuilder with zz_generated.deepcopy.go to k8s.io/api.
	// localSchemeBuilder and AddToScheme will stay in k8s.io/kubernetes.
	//
	// 中文说明：SchemeBuilder 是类型注册的聚合点；localSchemeBuilder 让 AddToScheme
	// 绑定到本包的 builder，避免直接暴露可变全局状态。AddToScheme 供调用方把本组
	// 类型加入任意 scheme。
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	AddToScheme        = localSchemeBuilder.AddToScheme
)

func init() {
	// We only register manually written functions here. The registration of the
	// generated functions takes place in the generated files. The separation
	// makes the code compile even when the generated files are missing.
	//
	// 中文说明：手写注册与生成注册分离，使生成文件缺失时仍可编译——
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
