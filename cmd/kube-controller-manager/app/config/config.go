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

package config

import (
	apiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/flagz"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/record"
	basecompatibility "k8s.io/component-base/compatibility"
	kubectrlmgrconfig "k8s.io/kubernetes/pkg/controller/apis/config"
	"time"
)

// Config 保存控制器管理器运行所需的主要上下文。
type Config struct {
	// Flagz 提供 flagz 页面读取命令行参数的接口。
	Flagz flagz.Reader

	// ComponentConfig 保存各控制器的组件配置。
	ComponentConfig kubectrlmgrconfig.KubeControllerManagerConfiguration

	// SecureServing 配置管理服务器的安全监听端点。
	SecureServing *apiserver.SecureServingInfo

	// Authentication 和 Authorization 分别配置客户端身份认证与授权。
	Authentication apiserver.AuthenticationInfo
	// Authorization 决定管理端点允许哪些已认证请求，未启用 HTTPS 时不会安装该处理器。
	Authorization apiserver.AuthorizationInfo

	// Client 是供管理器使用的通用 Kubernetes 客户端。
	Client *clientset.Clientset

	// Kubeconfig 保存连接 API 服务器的 REST 配置。
	Kubeconfig *restclient.Config

	// EventBroadcaster 和 EventRecorder 用于向 API 服务器发送事件。
	EventBroadcaster record.EventBroadcaster
	// EventRecorder 供控制器上报事件，通常使用 broadcast 事件源以复用后台写回管道。
	EventRecorder record.EventRecorder

	// ControllerShutdownTimeout 限制控制器优雅关闭的等待时间。
	ControllerShutdownTimeout time.Duration

	// ComponentGlobalsRegistry 保存各组件生效的版本和特性门控。
	ComponentGlobalsRegistry basecompatibility.ComponentGlobalsRegistry
}

type completedConfig struct {
	*Config
}

// CompletedConfig 封装已完成的配置，防止包外代码直接构造内部完成态。
type CompletedConfig struct {
	// 嵌入包外无法初始化的私有指针。
	*completedConfig
}

// Complete 原地补齐有效运行所需的未设置字段，并返回完成态配置。
func (c *Config) Complete() *CompletedConfig {
	cc := completedConfig{c}

	return &CompletedConfig{&cc}
}
