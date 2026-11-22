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

package proxy

import (
	"net"
	"strconv"

	"k8s.io/apimachinery/pkg/util/sets"
)

// Endpoint 是一个抽象端点信息的接口。
// 它屏蔽了不同 proxier 实现之间的差异，使规则生成逻辑只依赖统一的端点视图。
type Endpoint interface {
	// String 返回端点的字符串表示，示例格式为 `IP:Port`。
	// 返回值会被用作 ServiceEndpoint.Endpoint，作为端点在全过程中的稳定标识。
	String() string
	// IP 返回端点中的 IP 部分。
	IP() string
	// Port 返回端点中的端口部分。
	Port() int

	// IsLocal 在端点与 kube-proxy 运行于同一节点时返回 true。
	// 该信息用于优先将流量路由到本地端点，避免额外的跨节点转发。
	IsLocal() bool
	// IsReady 在端点就绪且未处于终止状态时返回 true；
	// 若 Service 设置了 PublishNotReadyAddresses，则总是返回 true。
	IsReady() bool
	// IsServing 在端点就绪时返回 true，不区分其是否处于终止状态。
	// 这样终止中的端点仍可继续接收已有连接。
	IsServing() bool
	// IsTerminating 在端点处于终止状态时返回 true。对 Pod 而言，
	// 即带有删除时间戳的 Pod。
	IsTerminating() bool

	// ZoneHints 返回端点的可用区提示，来源于 EndpointSlice API 中的
	// endpoint.hints.forZones[*].name，用于拓扑感知路由。
	ZoneHints() sets.Set[string]
	// NodeHints 返回端点的节点提示，来源于 EndpointSlice API 中的
	// endpoint.hints.forNodes[*].name，用于拓扑感知路由。
	NodeHints() sets.Set[string]
}

// BaseEndpointInfo 包含定义一个端点所需的基础信息。
// proxier 可以直接使用它处理端点，也可以基于它构造自己专属的、
// 携带更多字段的 EndpointInfo 结构体。
type BaseEndpointInfo struct {
	// 缓存这些值以避免每次调用 String/IP/Port 时重复拼接和转换，提升性能。
	ip   string
	port int
	// endpoint 等价于 net.JoinHostPort(ip, port)。
	endpoint string

	// isLocal 表示端点是否与 kube-proxy 运行在同一节点。
	isLocal bool

	// ready 表示端点就绪且未处于终止状态；若 Service 设置了
	// PublishNotReadyAddresses，则该值恒为 true。
	ready bool
	// serving 表示端点是否就绪，与其终止状态无关。
	// 对 Pod 而言，只要状态为 Ready，即使带有删除时间戳也为 true。
	serving bool
	// terminating 表示端点是否正在终止。对 Pod 而言，
	// 即带有非空删除时间戳的 Pod。
	terminating bool

	// zoneHints 是端点的可用区提示，来源于 EndpointSlice API 中的
	// endpoint.hints.forZones[*].name。
	zoneHints sets.Set[string]
	// nodeHints 是端点的节点提示，来源于 EndpointSlice API 中的
	// endpoint.hints.forNodes[*].name。
	nodeHints sets.Set[string]
}

var _ Endpoint = &BaseEndpointInfo{}

// String 实现 proxy.Endpoint 接口的 String 方法。
func (info *BaseEndpointInfo) String() string {
	return info.endpoint
}

// IP 实现 proxy.Endpoint 接口，仅返回端点的 IP 部分。
func (info *BaseEndpointInfo) IP() string {
	return info.ip
}

// Port 返回端点的端口部分。
func (info *BaseEndpointInfo) Port() int {
	return info.port
}

// IsLocal 实现 proxy.Endpoint 接口，报告端点是否在本节点。
func (info *BaseEndpointInfo) IsLocal() bool {
	return info.isLocal
}

// IsReady 在端点就绪且未终止时返回 true。
func (info *BaseEndpointInfo) IsReady() bool {
	return info.ready
}

// IsServing 在端点就绪时返回 true，不区分其是否处于终止状态。
func (info *BaseEndpointInfo) IsServing() bool {
	return info.serving
}

// IsTerminating 在端点处于终止状态时返回 true。对 Pod 而言，
// 即带有删除时间戳的 Pod。
func (info *BaseEndpointInfo) IsTerminating() bool {
	return info.terminating
}

// ZoneHints 返回端点的可用区提示。
func (info *BaseEndpointInfo) ZoneHints() sets.Set[string] {
	return info.zoneHints
}

// NodeHints 返回端点的节点提示。
func (info *BaseEndpointInfo) NodeHints() sets.Set[string] {
	return info.nodeHints
}

// newBaseEndpointInfo 根据给定的 IP、端口和状态标志构造 BaseEndpointInfo。
// 这里提前算好 endpoint 字符串，避免后续生成规则时反复拼接。
func newBaseEndpointInfo(ip string, port int, isLocal, ready, serving, terminating bool, zoneHints, nodeHints sets.Set[string]) *BaseEndpointInfo {
	return &BaseEndpointInfo{
		ip:          ip,
		port:        port,
		endpoint:    net.JoinHostPort(ip, strconv.Itoa(port)),
		isLocal:     isLocal,
		ready:       ready,
		serving:     serving,
		terminating: terminating,
		zoneHints:   zoneHints,
		nodeHints:   nodeHints,
	}
}
