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

package wardleinitializer

import (
	"k8s.io/apiserver/pkg/admission"
	informers "k8s.io/sample-apiserver/pkg/generated/informers/externalversions"
)

// pluginInitializer 只持有 informer factory，按插件实现的接口按需注入依赖。
type pluginInitializer struct {
	informers informers.SharedInformerFactory
}

// 编译期断言确保仍满足 apiserver 的初始化器接口。
var _ admission.PluginInitializer = pluginInitializer{}

// New 创建 Wardle 准入插件初始化器。
// 把 factory 缓存下来，等插件注册后再统一注入。
func New(informers informers.SharedInformerFactory) pluginInitializer {
	return pluginInitializer{
		informers: informers,
	}
}

// Initialize 按插件实现的可选接口注入相应依赖。
// 用可选接口而非强制参数，让不需要 informer 的插件保持零依赖。
func (i pluginInitializer) Initialize(plugin admission.Interface) {
	// 只有声明需要的插件才注入，类型断言失败时静默跳过。
	if wants, ok := plugin.(WantsInternalWardleInformerFactory); ok {
		wants.SetInternalWardleInformerFactory(i.informers)
	}
}
