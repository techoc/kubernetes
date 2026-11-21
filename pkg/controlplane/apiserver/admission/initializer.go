/*
Copyright 2024 The Kubernetes Authors.

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

package admission

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/initializer"
	quota "k8s.io/apiserver/pkg/quota/v1"
)

// PluginInitializer 用于初始化通用控制面的准入插件。
type PluginInitializer struct {
	quotaConfiguration         quota.Configuration
	excludedAdmissionResources []schema.GroupResource
}

var _ admission.PluginInitializer = &PluginInitializer{}

// NewPluginInitializer 构造一个新的 PluginInitializer 实例。
func NewPluginInitializer(
	quotaConfiguration quota.Configuration,
	excludedAdmissionResources []schema.GroupResource,
) *PluginInitializer {
	return &PluginInitializer{
		quotaConfiguration:         quotaConfiguration,
		excludedAdmissionResources: excludedAdmissionResources,
	}
}

// Initialize 检查每个插件实现的初始化接口，并提供相应的初始化数据。
func (i *PluginInitializer) Initialize(plugin admission.Interface) {
	// 通过接口断言按需注入，插件无需依赖本包的具体类型。
	if wants, ok := plugin.(initializer.WantsQuotaConfiguration); ok {
		wants.SetQuotaConfiguration(i.quotaConfiguration)
	}

	if wants, ok := plugin.(initializer.WantsExcludedAdmissionResources); ok {
		wants.SetExcludedAdmissionResources(i.excludedAdmissionResources)
	}
}
