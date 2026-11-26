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

// GroupName 是本包使用的组名
const GroupName = "resourcequota.admission.k8s.io"

// SchemeGroupVersion 是注册这些对象所用的组版本
//
// 该组版本只用于解析 ResourceQuota 准入插件的 Configuration 配置对象，
// 不对外暴露 REST API；修改组名会破坏现有配置文件的反序列化。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1alpha1"}

var (
	// TODO: 将 SchemeBuilder 与 zz_generated.deepcopy.go 一起移到 k8s.io/api。
	// localSchemeBuilder 和 AddToScheme 会留在 k8s.io/kubernetes。

	// SchemeBuilder 是用于调用 AddToScheme 的指针
	SchemeBuilder      runtime.SchemeBuilder
	localSchemeBuilder = &SchemeBuilder
	// AddToScheme 用于向 API 编解码机制注册类型
	AddToScheme = localSchemeBuilder.AddToScheme
)

func init() {
	// 此处只注册手写函数，生成函数的注册位于生成文件中。
	// 这样即使生成文件缺失，代码也能编译通过。
	// 否则生成文件缺失时包无法编译。
	//
	// 手写注册与生成代码分离：生成文件缺失时包仍可编译，只是缺少默认值等生成逻辑。
	localSchemeBuilder.Register(addKnownTypes, addDefaultingFuncs)
}

// addKnownTypes 将本组版本的 Configuration 注册进 scheme，供解码配置时识别类型。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&Configuration{},
	)
	return nil
}
