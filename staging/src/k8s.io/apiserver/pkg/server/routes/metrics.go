/*
Copyright 2014 The Kubernetes Authors.

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

package routes

import (
	handlersmetrics "k8s.io/apiserver/pkg/endpoints/handlers/metrics"
	apimetrics "k8s.io/apiserver/pkg/endpoints/metrics"
	"k8s.io/apiserver/pkg/server/mux"
	cachermetrics "k8s.io/apiserver/pkg/storage/cacher/metrics"
	etcd3metrics "k8s.io/apiserver/pkg/storage/etcd3/metrics"
	storagemetrics "k8s.io/apiserver/pkg/storage/metrics"
	flowcontrolmetrics "k8s.io/apiserver/pkg/util/flowcontrol/metrics"
	peerproxymetrics "k8s.io/apiserver/pkg/util/peerproxy/metrics"
	proxymetrics "k8s.io/apiserver/pkg/util/proxy/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// 只读端点：用于标准 /metrics 暴露。
type DefaultMetrics struct{}

// register 必须在使用处理器前调用，否则第一次抓取会缺少指标元数据。
func (m DefaultMetrics) Install(c *mux.PathRecorderMux) {
	register()
	c.Handle("/metrics", legacyregistry.Handler())
}

// MetricsWithReset 安装支持 DELETE 方法重置指标的 prometheus metrics 处理器。
// 重置能力主要服务于测试与调试；生产环境应通过鉴权限制访问。
type MetricsWithReset struct{}

// 注册 MetricsWithReset handler。
func (m MetricsWithReset) Install(c *mux.PathRecorderMux) {
	register()
	c.Handle("/metrics", legacyregistry.HandlerWithReset())
}

// 集中注册可确保各子系统指标在处理器安装时全部可见，避免抓取结果随注册顺序变化。
func register() {
	apimetrics.Register()
	storagemetrics.Register()
	cachermetrics.Register()
	etcd3metrics.Register()
	flowcontrolmetrics.Register()
	peerproxymetrics.Register()
	handlersmetrics.Register()
	proxymetrics.Register()
}
