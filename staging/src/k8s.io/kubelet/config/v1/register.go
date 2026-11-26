/*
Copyright 2022 The Kubernetes Authors.

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

// 本文件把稳定版 kubelet 配置对象注册进 scheme，
// 供 kubelet 启动时按 apiVersion 解析配置文件。
package v1

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的 API 组名。
//
// GroupName 是节点本地配置的 API 组名，与集群 API 组隔离，
// 表明此类对象只由 kubelet 自己消费。
const GroupName = "kubelet.config.k8s.io"

// SchemeGroupVersion 是注册这些对象所用的 group version。
//
// SchemeGroupVersion 标识本组稳定版本，是 scheme 中类型注册的键。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1"}

var (
	// SchemeBuilder 收集本 API 包需要执行的 scheme 初始化函数。
	//
	// SchemeBuilder 把类型注册函数打包，便于与其他 API 包组合后一次性注册。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 是把本 API 组与版本注册到 scheme 的全局入口。
	//
	// AddToScheme 是 SchemeBuilder.AddToScheme 的别名，调用方用它完成注册。
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes 把已知类型注册到给定 scheme。
//
// addKnownTypes 只登记本版本实际支持的配置类型，
// 遗漏任何类型都会让包含该类型的配置文件无法反序列化。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&CredentialProviderConfig{},
	)
	return nil
}
