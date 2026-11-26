/*
Copyright 2020 The Kubernetes Authors.

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

package app

import (
	"time"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/restmapper"
	controllersmetrics "k8s.io/component-base/metrics/prometheus/controllers"
	"k8s.io/controller-manager/pkg/clientbuilder"
	"k8s.io/controller-manager/pkg/informerfactory"
)

// ControllerContext 是 controller-manager 传给每个控制器的“启动上下文”。
// 管理器在启动时先把客户端构建器、Informer 工厂、RESTMapper、停止通道等公共设施准备好，
// 再打包进这个对象交给控制器。这样控制器无需关心如何连接 apiserver、如何共享 Informer
// 等横切逻辑，只需从上下文中取用所需能力即可。
type ControllerContext struct {
	// ClientBuilder 为该 controller 提供要用的客户端。
	ClientBuilder clientbuilder.ControllerClientBuilder

	// InformerFactory 为 controller 提供 informer 的访问入口。
	InformerFactory informers.SharedInformerFactory

	// ObjectOrMetadataInformerFactory 提供访问 Informer 的入口：既支持结构化（typed）资源，
	// 也支持仅通过元数据访问的动态资源。
	// 目前通用控制器只使用对象元数据；若将来需要完整对象，这里会演变成 GenericInformerFactory 并接收动态客户端。
	ObjectOrMetadataInformerFactory informerfactory.InformerFactory

	// DeferredDiscoveryRESTMapper 把 RESTMapper 的初始化推迟到首次映射请求，
	// 避免 controller 启动阶段就依赖 discovery 可用。
	// requested.
	// 延迟初始化的 RESTMapper：只有在第一次需要做资源映射时才向 apiserver 发现 API，
	// 避免启动阶段对 apiserver 产生不必要的请求。
	RESTMapper *restmapper.DeferredDiscoveryRESTMapper

	// Stop 是停止信号通道：管理器和各控制器共享它，通道关闭即表示进程正在退出，
	// 所有 workers 应据此停止自己的循环。
	Stop <-chan struct{}

	// InformersStarted 在所有控制器完成初始化并开始运行后被关闭。
	// 在此之前，单个控制器不应自行启动共享 Informer；关闭之后再启动才是安全的。
	InformersStarted chan struct{}

	// ResyncPeriod 每次调用都返回一个随机化的 resync 周期。
	// 目的是让各控制器的重同步时间错开，避免多个 reflector 同时向 apiserver 发起 list 请求。
	ResyncPeriod func() time.Duration

	// ControllerManagerMetrics 是用于注册/设置 controller-manager 专属指标的代理对象。
	ControllerManagerMetrics *controllersmetrics.ControllerManagerMetrics
}
