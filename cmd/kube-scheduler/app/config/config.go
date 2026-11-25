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
	"time"

	apiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/flagz"
	"k8s.io/client-go/dynamic/dynamicinformer"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/events"
	"k8s.io/client-go/tools/leaderelection"
	basecompatibility "k8s.io/component-base/compatibility"
	kubeschedulerconfig "k8s.io/kubernetes/pkg/scheduler/apis/config"
)

// Config 汇总 Scheduler 运行期间需要的配置和外部依赖；Options 负责创建它们，Run 负责消费它们。
type Config struct {
	// Flagz 为 /flagz 调试端点提供已解析命令行参数的只读视图。
	Flagz flagz.Reader

	// ComponentConfig 是经过默认值填充和校验后的 Scheduler 内部组件配置。
	ComponentConfig kubeschedulerconfig.KubeSchedulerConfiguration

	// Authentication / Authorization 分别负责请求身份认证和访问授权；SecureServing 保存 HTTPS 服务端配置。
	Authentication apiserver.AuthenticationInfo
	// Authorization 配置 HTTPS 管理端点的访问授权；未启用安全监听时无需填充。
	Authorization apiserver.AuthorizationInfo
	// SecureServing 保存监听地址、证书和优雅关闭参数；nil 表示不启动 HTTPS 端点。
	SecureServing *apiserver.SecureServingInfo

	// Client 访问 Kubernetes API，KubeConfig 保存 REST 配置。
	// 两类 InformerFactory 分别服务内置资源和动态资源，后者不受编译期资源类型集合限制。
	Client clientset.Interface
	// KubeConfig 是创建客户端和资源锁时使用的 REST 配置。
	KubeConfig *restclient.Config
	// InformerFactory 为内置资源提供共享缓存，启动前不得替换。
	InformerFactory informers.SharedInformerFactory
	// DynInformerFactory 提供动态资源缓存；测试中可为 nil。
	DynInformerFactory dynamicinformer.DynamicSharedInformerFactory

	//nolint:staticcheck // SA1019 迁移完成前仍需使用此废弃字段。
	// EventBroadcaster 将 Scheduler 产生的事件异步写回 API Server。
	EventBroadcaster events.EventBroadcasterAdapter

	// LeaderElection 为空表示不启用选主；非空时保存 Lease 锁和选主回调所需参数。
	LeaderElection *leaderelection.LeaderElectionConfig

	// PodMaxInUnschedulablePodsDuration 限制 Pod 在 unschedulablePods 中的停留时间。
	// 超时后 Pod 会移入 backoffQ 或 activeQ 重新尝试调度；零值使用默认的 5 分钟。
	PodMaxInUnschedulablePodsDuration time.Duration

	// ComponentGlobalsRegistry 保存各组件的有效版本和 Feature Gate，用于版本兼容判断与运行期开关管理。
	ComponentGlobalsRegistry basecompatibility.ComponentGlobalsRegistry
}

type completedConfig struct {
	*Config
}

// CompletedConfig 表示可以交给运行阶段使用的配置；非导出嵌入类型阻止包外伪造完成态对象。
type CompletedConfig struct {
	// 嵌入包内私有指针，阻止包外代码直接伪造完成态配置。
	*completedConfig
}

// Complete 将 Config 包装为 CompletedConfig，以类型区分配置组装阶段和运行阶段。
func (c *Config) Complete() CompletedConfig {
	cc := completedConfig{c}

	return CompletedConfig{&cc}
}
