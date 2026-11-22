/*
Copyright 2019 The Kubernetes Authors.

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

// Package helpers 提供 cloud-provider 各控制器共用的节点工具函数：
// 地址合并与筛选、标签更新、污点读写。
//
// 中文说明：这些函数被 in-tree 云厂商与 CCM 共同引用，放在这里可以避免依赖
// k8s.io/kubernetes 内部包。
package helpers

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	nodeutil "k8s.io/component-helpers/node/util"
	netutils "k8s.io/utils/net"
)

// AddToNodeAddresses appends the NodeAddresses to the passed-by-pointer slice,
// only if they do not already exist
//
// 中文说明：以 (Type, Address) 二元组判重，顺序无关；函数原地修改切片，
// 调用方需自行保证该切片不被其他 goroutine 共享。
func AddToNodeAddresses(addresses *[]v1.NodeAddress, addAddresses ...v1.NodeAddress) {
	for _, add := range addAddresses {
		exists := false
		for _, existing := range *addresses {
			if existing.Address == add.Address && existing.Type == add.Type {
				exists = true
				break
			}
		}
		if !exists {
			*addresses = append(*addresses, add)
		}
	}
}

// GetNodeAddressesFromNodeIP filters the provided list of nodeAddresses to match the
// providedNodeIP from the Node annotation (which is assumed to be non-empty). This is
// used for external cloud providers.
//
// It will return node addresses filtered such that:
//   - Any address matching nodeIP will be listed first.
//   - If nodeIP matches an address of a particular type (internal or external),
//     that will be the *only* address of that type returned.
//   - All remaining addresses are listed after.
func GetNodeAddressesFromNodeIP(providedNodeIP string, cloudNodeAddresses []v1.NodeAddress) ([]v1.NodeAddress, error) {
	nodeIPs, err := nodeutil.ParseNodeIPAnnotation(providedNodeIP)
	if err != nil {
		return nil, fmt.Errorf("failed to parse node IP %q: %v", providedNodeIP, err)
	}

	// enforcedNodeAddresses 保持“先匹配项、后其余项”的顺序，enforced 在前。
	enforcedNodeAddresses := []v1.NodeAddress{}
	nodeIPTypes := make(map[v1.NodeAddressType]bool)

	for _, nodeIP := range nodeIPs {
		// For every address supplied by the cloud provider that matches nodeIP,
		// nodeIP is the enforced node address for that address Type (like
		// InternalIP and ExternalIP), meaning other addresses of the same Type
		// are discarded. See #61921 for more information: some cloud providers
		// may supply secondary IPs, so nodeIP serves as a way to ensure that the
		// correct IPs show up on a Node object.

		matched := false
		for _, nodeAddress := range cloudNodeAddresses {
			// ParseIPSloppy 允许带前导零等历史格式，保持与旧集群行为一致。
			if netutils.ParseIPSloppy(nodeAddress.Address).Equal(nodeIP) {
				enforcedNodeAddresses = append(enforcedNodeAddresses, v1.NodeAddress{Type: nodeAddress.Type, Address: nodeAddress.Address})
				nodeIPTypes[nodeAddress.Type] = true
				matched = true
			}
		}

		// nodeIP must be among the addresses supplied by the cloud provider
		// 中文说明：用户显式指定的 IP 与云侧枚举结果不一致时直接报错，
		// 让节点保持未初始化状态，而不是悄悄采用错误地址。
		if !matched {
			return nil, fmt.Errorf("failed to get node address from cloud provider that matches ip: %v", nodeIP)
		}
	}

	// Now use all other addresses supplied by the cloud provider NOT of the same Type
	// as any nodeIP.
	// 中文说明：被 nodeIP 指定的地址类型只保留匹配项，其余类型原样保留。
	for _, nodeAddress := range cloudNodeAddresses {
		if !nodeIPTypes[nodeAddress.Type] {
			enforcedNodeAddresses = append(enforcedNodeAddresses, v1.NodeAddress{Type: nodeAddress.Type, Address: nodeAddress.Address})
		}
	}

	return enforcedNodeAddresses, nil
}
