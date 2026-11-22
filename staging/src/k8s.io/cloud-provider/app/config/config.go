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

// Package config 定义 cloud-controller-manager 的运行期上下文对象，
// 集中承载组件配置、认证授权信息、Kubernetes 客户端与 informer 等共享依赖。
package config

import (
	apiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	cloudprovider "k8s.io/cloud-provider"
	"k8s.io/cloud-provider/config"
)

// Config is the main context object for the cloud controller manager.
// Config 是 cloud-controller-manager 的运行时上下文：控制器初始化函数只依赖它，
// 而不直接读取命令行参数，从而让配置解析与业务逻辑解耦。
type Config struct {
	// ComponentConfig 是已完成默认化和内部版本转换的组件配置，是所有控制器共享的配置来源。
	ComponentConfig config.CloudControllerManagerConfiguration

	// SecureServing 承载主服务端口（healthz/configz/metrics 等）的 TLS 与监听配置。
	SecureServing *apiserver.SecureServingInfo
	// LoopbackClientConfig is a config for a privileged loopback connection
	// 回环客户端配置使用特权身份访问自身 API，供健康检查等内部调用使用，不对外暴露。
	LoopbackClientConfig *restclient.Config

	// Authentication/Authorization 保存委托认证与授权所需的信息。
	Authentication apiserver.AuthenticationInfo
	Authorization  apiserver.AuthorizationInfo

	// WebhookSecureServing is a separate SecureServing configuration from
	// healthz, configz, and metrics.
	// Webhook 使用独立端口，避免准入请求与运维端点共享监听器而互相影响。
	WebhookSecureServing *apiserver.SecureServingInfo

	// the general kube client
	// Client 是使用组件 user-agent 构造的通用客户端，供控制器直接调用 API。
	Client *clientset.Clientset

	// the rest config for the master
	// Kubeconfig 是所有派生客户端（含按身份区分的 ClientBuilder）的 rest 配置基础。
	Kubeconfig *restclient.Config

	// EventBroadcaster is broadcaster events to all sinks.
	// 事件广播器负责把同一事件分发到日志与 API Server 等多个 sink。
	EventBroadcaster record.EventBroadcaster

	// EventRecord is a sink for events.
	// EventRecorder 是控制器写事件时使用的入口，背后由 EventBroadcaster 驱动。
	EventRecorder record.EventRecorder

	// ClientBuilder will provide a client for this controller to use
	// ClientBuilder 依据控制器 ClientName 生成带独立身份的客户端；启用
	// UseServiceAccountCredentials 时每个控制器使用各自的最小权限 ServiceAccount。
	ClientBuilder cloudprovider.ControllerClientBuilder

	// VersionedClient will provide a client for informers
	// VersionedClient 专供 informer 使用，与控制器自己的客户端分离，便于审计与限流。
	VersionedClient clientset.Interface

	// SharedInformers gives access to informers for the controller.
	// 所有控制器共享同一 informer factory，避免对同一资源重复建立 watch。
	SharedInformers informers.SharedInformerFactory
}

// completedConfig 保存已完成校验与默认化的配置，只能通过 Complete 构造，
// 防止控制器拿到字段不完整的配置。
type completedConfig struct {
	*Config
}

// CompletedConfig same as Config, just to swap private object.
// 外部包只能持有 CompletedConfig，从而在类型层面保证配置已经过 Complete 处理。
type CompletedConfig struct {
	// Embed a private pointer that cannot be instantiated outside of this package.
	*completedConfig
}

// Complete fills in any fields not set that are required to have valid data. It's mutating the receiver.
// Complete 会对接收者做原地修改，用回环客户端配置补全委托认证/授权信息。
func (c *Config) Complete() *CompletedConfig {
	cc := completedConfig{c}

	// 使用回环 token 填充 Authentication/Authorization，保证主服务端可以校验内部调用者。
	apiserver.AuthorizeClientBearerToken(c.LoopbackClientConfig, &c.Authentication, &c.Authorization)

	return &CompletedConfig{&cc}
}
