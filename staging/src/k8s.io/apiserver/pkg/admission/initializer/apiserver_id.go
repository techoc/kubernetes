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

// Package initializer 负责在准入插件构造完成后、开始服务前注入其运行所需的
// 依赖（kube 客户端、informer、授权器、特性开关等）。
package initializer

import "k8s.io/apiserver/pkg/admission"

// apiServerIDInitializer 把 API server 的身份注入实现了 WantsAPIServerID 的准入插件，
// 用于 HA 部署下的指标标签——此时可能同时运行多个 apiserver，且各自加载的准入配置并不相同。
//
// HA 部署中多个 apiserver 可能加载不同的准入配置，指标需要按实例
// 身份区分；身份只通过可选接口注入，未实现该接口的插件不受影响。
type apiServerIDInitializer struct {
	apiServerID string
}

var _ admission.PluginInitializer = &apiServerIDInitializer{}

// NewAPIServerIDInitializer 返回一个 PluginInitializer，把给定的 API server ID 注入
// 实现了 WantsAPIServerID 的准入插件。该初始化器必须排在初始化链中通用初始化器之前，
// 以保证调用 SetExternalKubeInformerFactory 时 API server ID 已经可用。
//
// 顺序约束来自插件初始化时机——部分插件在获得 informer factory
// 时就会构造带标签的指标，因此 ID 必须更早写入。
func NewAPIServerIDInitializer(apiServerID string) admission.PluginInitializer {
	return &apiServerIDInitializer{apiServerID: apiServerID}
}

// Initialize 检查插件是否实现了 WantsAPIServerID，若是则注入 ID。
//
// 类型断言而非强制要求，保证初始化器对任意插件都安全可复用。
func (i *apiServerIDInitializer) Initialize(plugin admission.Interface) {
	if wants, ok := plugin.(WantsAPIServerID); ok {
		wants.SetAPIServerID(i.apiServerID)
	}
}
