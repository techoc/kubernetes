/*
Copyright 2025 The Kubernetes Authors.

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

// Package apicalls 定义调度器框架中 API 调用的类型、关联关系与实现注册。
package apicalls

import (
	fwk "k8s.io/kube-scheduler/framework"
)

const (
	// PodStatusPatch 表示更新 Pod 状态的 API 调用类型。
	PodStatusPatch fwk.APICallType = "pod_status_patch"
	// PodBinding 表示将 Pod 绑定到节点的 API 调用类型。
	PodBinding fwk.APICallType = "pod_binding"
)

// Relevances 是内置的类型到关联度的映射。
// 只有针对不同对象类型时，才应为相同关联度定义多个类型。
// 该映射配置错误会导致系统出现非预期行为，任何修改都必须充分测试并谨慎进行。
// 如有需要，树外插件可在其 init() 函数中替换此映射。
var Relevances = fwk.APICallRelevances{
	PodStatusPatch: 1,
	PodBinding:     2,
}

// Implementations 是内置的类型到调用构造函数的映射。
// 它用于在调度器框架中构造调用对象，也便于替换这些实现。
// 如有需要，树外插件可在其 init() 函数中替换此映射。
var Implementations = fwk.APICallImplementations[*PodStatusPatchCall, *PodBindingCall]{
	PodStatusPatch: NewPodStatusPatchCall,
	PodBinding:     NewPodBindingCall,
}
