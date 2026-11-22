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

package initializer

import (
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/component-base/compatibility"
	"k8s.io/component-base/featuregate"
)

// pluginInitializer 持有一切可能被准入插件依赖的共享基础设施，
// 通过 Initialize 按插件声明的 "Wants*" 接口做按需注入。
// 字段全部为只读引用，插件只应读取而不应替换。
type pluginInitializer struct {
	externalClient    kubernetes.Interface
	dynamicClient     dynamic.Interface
	externalInformers informers.SharedInformerFactory
	authorizer        authorizer.Authorizer
	featureGates      featuregate.FeatureGate
	effectiveVersion  compatibility.EffectiveVersion
	stopCh            <-chan struct{}
	restMapper        meta.RESTMapper
}

// New creates an instance of admission plugins initializer.
// This constructor is public with a long param list so that callers immediately know that new information can be expected
// during compilation when they update a level.
func New(
	extClientset kubernetes.Interface,
	dynamicClient dynamic.Interface,
	extInformers informers.SharedInformerFactory,
	authz authorizer.Authorizer,
	featureGates featuregate.FeatureGate,
	effectiveVersion compatibility.EffectiveVersion,
	stopCh <-chan struct{},
	restMapper meta.RESTMapper,
) pluginInitializer {
	// 显式列出所有依赖，新增字段时编译器会强制所有调用点更新，
	// 这正是该构造函数不使用选项模式的原因。
	return pluginInitializer{
		externalClient:    extClientset,
		dynamicClient:     dynamicClient,
		externalInformers: extInformers,
		authorizer:        authz,
		featureGates:      featureGates,
		effectiveVersion:  effectiveVersion,
		stopCh:            stopCh,
		restMapper:        restMapper,
	}
}

// Initialize checks the initialization interfaces implemented by a plugin
// and provide the appropriate initialization data
func (i pluginInitializer) Initialize(plugin admission.Interface) {
	// First tell the plugin about drained notification, so it can pass it to further initializations.
	// 必须最先注入：后续初始化（如启动 informer）需要据此在关闭时停止。
	if wants, ok := plugin.(WantsDrainedNotification); ok {
		wants.SetDrainedNotification(i.stopCh)
	}

	// Second tell the plugin about enabled features, so it can decide whether to start informers or not
	if wants, ok := plugin.(WantsEffectiveVersion); ok {
		wants.InspectEffectiveVersion(i.effectiveVersion)
	}
	if wants, ok := plugin.(WantsFeatures); ok {
		wants.InspectFeatureGates(i.featureGates)
	}

	// 客户端与 informer 工厂在特性开关之后注入，插件可在启动 informer 前按开关决策。
	if wants, ok := plugin.(WantsExternalKubeClientSet); ok {
		wants.SetExternalKubeClientSet(i.externalClient)
	}

	if wants, ok := plugin.(WantsDynamicClient); ok {
		wants.SetDynamicClient(i.dynamicClient)
	}

	if wants, ok := plugin.(WantsExternalKubeInformerFactory); ok {
		wants.SetExternalKubeInformerFactory(i.externalInformers)
	}

	// WantsUnconditionalAuthorizer 表示插件可以接受不经 SubjectAccessReview 包装的授权器，
	// 通常用于插件自身已有明确授权语义的场景；两者都实现时按先无条件、后普通顺序注入。
	if wants, ok := plugin.(WantsUnconditionalAuthorizer); ok {
		wants.SetUnconditionalAuthorizer(i.authorizer)
	}
	if wants, ok := plugin.(WantsAuthorizer); ok {
		wants.SetAuthorizer(i.authorizer)
	}
	if wants, ok := plugin.(WantsRESTMapper); ok {
		wants.SetRESTMapper(i.restMapper)
	}
}

// 编译期断言：pluginInitializer 必须满足 admission.PluginInitializer 接口。
var _ admission.PluginInitializer = pluginInitializer{}
