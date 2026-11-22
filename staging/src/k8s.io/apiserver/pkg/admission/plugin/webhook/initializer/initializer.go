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
	"net/url"

	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/util/webhook"
)

// WantsServiceResolver defines a function that accepts a ServiceResolver for
// admission plugins that need to make calls to services.
//
// 中文说明：插件通过实现该接口声明「我需要把 service 引用解析为真实地址」。
// 用接口而非直接注入字段，是为了让插件与 apiserver 的依赖注入机制解耦，
// 插件可以独立编译与单测。
type WantsServiceResolver interface {
	SetServiceResolver(webhook.ServiceResolver)
}

// ServiceResolver knows how to convert a service reference into an actual
// location.
//
// 中文说明：由 apiserver 侧注入，通常基于集群内 DNS 规则实现；
// 单独抽象是为了让 webhook 配置校验与调用方不依赖真实网络。
type ServiceResolver interface {
	ResolveEndpoint(namespace, name string, port int32) (*url.URL, error)
}

// WantsAuthenticationInfoResolverWrapper defines a function that wraps the standard AuthenticationInfoResolver
// to allow the apiserver to control what is returned as auth info
//
// 中文说明：webhook 客户端向外部服务发起请求时需要认证信息；apiserver 通过 wrapper
// 决定以何种身份（通常是聚合层代理身份）访问 webhook，插件本身不持有凭证，
// 这是一个明确的安全边界。同时要求实现 InitializationValidator，确保注入后配置可用。
type WantsAuthenticationInfoResolverWrapper interface {
	SetAuthenticationInfoResolverWrapper(wrapper webhook.AuthenticationInfoResolverWrapper)
	admission.InitializationValidator
}

// PluginInitializer is used for initialization of the webhook admission plugin.
//
// 中文说明：只负责把两个依赖分发给声明了需求的插件，不关心插件内部如何实现。
// 依赖可为 nil（例如未配置聚合层），此时对应接口不会被注入。
type PluginInitializer struct {
	serviceResolver                   webhook.ServiceResolver
	authenticationInfoResolverWrapper webhook.AuthenticationInfoResolverWrapper
}

var _ admission.PluginInitializer = &PluginInitializer{}

// NewPluginInitializer constructs new instance of PluginInitializer
func NewPluginInitializer(
	authenticationInfoResolverWrapper webhook.AuthenticationInfoResolverWrapper,
	serviceResolver webhook.ServiceResolver,
) *PluginInitializer {
	return &PluginInitializer{
		authenticationInfoResolverWrapper: authenticationInfoResolverWrapper,
		serviceResolver:                   serviceResolver,
	}
}

// Initialize checks the initialization interfaces implemented by each plugin
// and provide the appropriate initialization data
//
// 中文说明：采用「按需注入」——只有实现了对应接口的插件才会收到依赖，
// 这样插件可以按需选择能力，避免所有插件被迫感知 webhook 相关类型。
// wrapper 为 nil 时跳过注入，让插件保留自身默认行为（例如直接返回未认证）。
func (i *PluginInitializer) Initialize(plugin admission.Interface) {
	if wants, ok := plugin.(WantsServiceResolver); ok {
		wants.SetServiceResolver(i.serviceResolver)
	}

	if wants, ok := plugin.(WantsAuthenticationInfoResolverWrapper); ok {
		if i.authenticationInfoResolverWrapper != nil {
			wants.SetAuthenticationInfoResolverWrapper(i.authenticationInfoResolverWrapper)
		}
	}
}
