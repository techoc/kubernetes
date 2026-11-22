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

// apiServerIDInitializer injects the API server identity into admission plugins
// that implement WantsAPIServerID. This is used for metrics labeling in HA setups
// where multiple API servers may be running with different manifest configurations.
//
// 中文说明：HA 部署中多个 apiserver 可能加载不同的准入配置，指标需要按实例
// 身份区分；身份只通过可选接口注入，未实现该接口的插件不受影响。
type apiServerIDInitializer struct {
	apiServerID string
}

var _ admission.PluginInitializer = &apiServerIDInitializer{}

// NewAPIServerIDInitializer returns a PluginInitializer that injects the given
// API server ID into admission plugins that implement WantsAPIServerID.
// This initializer must be placed before the generic initializer in the chain
// so that the API server ID is available when SetExternalKubeInformerFactory is called.
//
// 中文说明：顺序约束来自插件初始化时机——部分插件在获得 informer factory
// 时就会构造带标签的指标，因此 ID 必须更早写入。
func NewAPIServerIDInitializer(apiServerID string) admission.PluginInitializer {
	return &apiServerIDInitializer{apiServerID: apiServerID}
}

// Initialize checks whether the plugin implements WantsAPIServerID and injects the ID.
//
// 中文说明：类型断言而非强制要求，保证初始化器对任意插件都安全可复用。
func (i *apiServerIDInitializer) Initialize(plugin admission.Interface) {
	if wants, ok := plugin.(WantsAPIServerID); ok {
		wants.SetAPIServerID(i.apiServerID)
	}
}
