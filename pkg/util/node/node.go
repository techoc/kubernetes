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

package node

import (
	"fmt"
	"net"

	v1 "k8s.io/api/core/v1"
	netutils "k8s.io/utils/net"
)

const (
	// NodeUnreachablePodReason 是 Pod 状态无法确认时的原因：节点上 kubelet 无响应。
	NodeUnreachablePodReason = "NodeLost"
	// NodeUnreachablePodMessage 是 Pod 状态无法确认时的消息：运行该 Pod 的节点 kubelet 无响应。
	NodeUnreachablePodMessage = "Node %v which was running pod %v is unresponsive"
)

// NoMatchError 是 error 接口的具体实现，表示未能找到匹配的节点地址。
type NoMatchError struct {
	addresses []v1.NodeAddress
}

// Error 实现 error 接口，nil 表示没有错误。
func (e *NoMatchError) Error() string {
	return fmt.Sprintf("no preferred addresses found; known addresses: %v", e.addresses)
}

// GetPreferredNodeAddress 按给定的优先顺序返回节点的地址。
// 若找不到任何优先地址类型，则返回错误。
func GetPreferredNodeAddress(node *v1.Node, preferredAddressTypes []v1.NodeAddressType) (string, error) {
	for _, addressType := range preferredAddressTypes {
		for _, address := range node.Status.Addresses {
			if address.Type == addressType {
				return address.Address, nil
			}
		}
	}
	return "", &NoMatchError{addresses: node.Status.Addresses}
}

// GetNodeHostIPs 返回节点的 IP：单栈集群返回单个"主 IP"，双栈集群（节点确实拥有双栈 IP 时）
// 返回一对 IP。返回的 IP 一方面用作节点上 hostNetwork Pod 的 `.status.PodIPs`，
// 其中第一个 IP 还用作节点上所有 Pod 的 `.status.HostIP`。
func GetNodeHostIPs(node *v1.Node) ([]net.IP, error) {
	// 重新排序地址：InternalIP 在前，ExternalIP 在后。
	allIPs := make([]net.IP, 0, len(node.Status.Addresses))
	for _, addr := range node.Status.Addresses {
		if addr.Type == v1.NodeInternalIP {
			ip := netutils.ParseIPSloppy(addr.Address)
			if ip != nil {
				allIPs = append(allIPs, ip)
			}
		}
	}
	for _, addr := range node.Status.Addresses {
		if addr.Type == v1.NodeExternalIP {
			ip := netutils.ParseIPSloppy(addr.Address)
			if ip != nil {
				allIPs = append(allIPs, ip)
			}
		}
	}
	if len(allIPs) == 0 {
		return nil, fmt.Errorf("host IP unknown; known addresses: %v", node.Status.Addresses)
	}

	// 取第一个 IP 作为主 IP；再找第一个协议族不同的 IP 组成双栈对。
	nodeIPs := []net.IP{allIPs[0]}
	for _, ip := range allIPs {
		if netutils.IsIPv6(ip) != netutils.IsIPv6(nodeIPs[0]) {
			nodeIPs = append(nodeIPs, ip)
			break
		}
	}

	return nodeIPs, nil
}

// IsNodeReady 判断节点是否处于 Ready 状态。
func IsNodeReady(node *v1.Node) bool {
	for _, c := range node.Status.Conditions {
		if c.Type == v1.NodeReady {
			return c.Status == v1.ConditionTrue
		}
	}
	return false
}
