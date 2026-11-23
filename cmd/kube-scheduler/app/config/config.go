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
	Authorization  apiserver.AuthorizationInfo
	SecureServing  *apiserver.SecureServingInfo

	// Client 访问 Kubernetes API，KubeConfig 保存 REST 配置。
	// 两类 InformerFactory 分别服务内置资源和动态资源，后者不受编译期资源类型集合限制。
	Client             clientset.Interface
	KubeConfig         *restclient.Config
	InformerFactory    informers.SharedInformerFactory
	DynInformerFactory dynamicinformer.DynamicSharedInformerFactory

	//nolint:staticcheck // SA1019 this deprecated field still needs to be used for now. It will be removed once the migration is done.
	// EventBroadcaster 将 Scheduler 产生的事件异步写回 API Server。
	EventBroadcaster events.EventBroadcasterAdapter

	// LeaderElection 为空表示不启用选主；非空时保存 Lease 锁和选主回调所需参数。
	LeaderElection *leaderelection.LeaderElectionConfig

	// PodMaxInUnschedulablePodsDuration is the maximum time a pod can stay in
	// unschedulablePods. If a pod stays in unschedulablePods for longer than this
	// value, the pod will be moved from unschedulablePods to backoffQ or activeQ.
	// If this value is empty, the default value (5min) will be used.
	PodMaxInUnschedulablePodsDuration time.Duration

	// ComponentGlobalsRegistry is the registry where the effective versions and feature gates for all components are stored.
	ComponentGlobalsRegistry basecompatibility.ComponentGlobalsRegistry
}

type completedConfig struct {
	*Config
}

// CompletedConfig 表示可以交给运行阶段使用的配置；非导出嵌入类型阻止包外伪造完成态对象。
type CompletedConfig struct {
	// Embed a private pointer that cannot be instantiated outside of this package.
	*completedConfig
}

// Complete 将 Config 包装为 CompletedConfig，以类型区分配置组装阶段和运行阶段。
func (c *Config) Complete() CompletedConfig {
	cc := completedConfig{c}

	return CompletedConfig{&cc}
}
