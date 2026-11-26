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

// 本文件负责把 kubelet 配置对象注册到 runtime scheme，
// 使配置文件反序列化时能根据 apiVersion 找到对应 Go 类型。
package v1beta1

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupName 是本包使用的 API 组名。
//
// GroupName 是 kubelet 本地配置的 API 组，与集群 API 组分开，
// 说明这些对象由节点上的 kubelet 自行解析，不经过 API server。
const GroupName = "kubelet.config.k8s.io"

// SchemeGroupVersion 是注册这些对象所用的 group version。
//
// SchemeGroupVersion 把组名与版本绑定，作为 scheme 中类型注册的唯一键。
var SchemeGroupVersion = schema.GroupVersion{Group: GroupName, Version: "v1beta1"}

var (
	// SchemeBuilder 收集本 API 包需要执行的 scheme 初始化函数。
	//
	// SchemeBuilder 延迟执行 addKnownTypes，便于调用方按需组合多个 API 包的注册逻辑。
	SchemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	// AddToScheme 是把本 API 组与版本注册到 scheme 的全局入口。
	//
	// AddToScheme 是 SchemeBuilder.AddToScheme 的便捷别名，供外部包一行完成注册。
	AddToScheme = SchemeBuilder.AddToScheme
)

// addKnownTypes 把已知类型注册到给定 scheme。
//
// addKnownTypes 列出本版本所有可反序列化的对象；
// 每新增一种配置对象都要在此登记，否则 kubelet 读取配置时会因“未知类型”失败。
func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(SchemeGroupVersion,
		&KubeletConfiguration{},
		&SerializedNodeConfigSource{},
		&CredentialProviderConfig{},
		&ImagePullIntent{},
		&ImagePulledRecord{},
	)
	return nil
}
