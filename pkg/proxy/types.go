/*
Copyright 2015 The Kubernetes Authors.

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

// Package proxy 定义 kube-proxy 各后端实现共用的核心类型与接口。
// 通过抽象出 Provider 等接口，使配置层无需依赖具体的代理实现（iptables、ipvs 等）。
package proxy

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/proxy/config"
)

// Provider 是各 proxier 实现需要提供的接口。
// 该接口将配置来源与具体代理后端解耦：配置层只调用统一的事件处理方法，
// 而不必关心规则最终由 iptables、ipvs 还是其他机制落地。
type Provider interface {
	config.EndpointSliceHandler
	config.ServiceHandler
	config.NodeTopologyHandler
	config.ServiceCIDRHandler

	// Sync 立即把 Provider 的当前状态同步为代理规则。
	Sync()
	// SyncLoop 运行周期性任务。
	// 它应当以 goroutine 形式运行，或作为应用的主循环运行。
	// 该方法不会返回。
	SyncLoop()
}

// ServicePortName 携带 namespace、name 和 portname，是负载均衡 Service 的唯一标识符。
type ServicePortName struct {
	types.NamespacedName
	Port     string
	Protocol v1.Protocol
}

// String 返回 ServicePortName 的字符串表示，在名字后附加端口（若端口非空）。
func (spn ServicePortName) String() string {
	return fmt.Sprintf("%s%s", spn.NamespacedName.String(), fmtPortName(spn.Port))
}

// fmtPortName 将端口格式化为带前导冒号的形式；空端口返回空串，避免生成多余的冒号。
func fmtPortName(in string) string {
	if in == "" {
		return ""
	}
	return fmt.Sprintf(":%s", in)
}

// ServiceEndpoint 用于标识一个 Service 及其对应的一个 endpoint 对。
type ServiceEndpoint struct {
	Endpoint        string
	ServicePortName ServicePortName
}
