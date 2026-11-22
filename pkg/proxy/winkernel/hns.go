//go:build windows

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

package winkernel

import (
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Microsoft/hnslib/hcn"

	"k8s.io/klog/v2"
)

// HostNetworkService 封装 proxier 需要的网络、端点与负载均衡器操作。
// 以接口形式暴露便于在测试中注入模拟的 HNS 实现。
type HostNetworkService interface {
	getNetworkByName(name string) (*hnsNetworkInfo, error)
	// getAllEndpointsByNetwork 返回指定网络上的全部端点，分别以端点 ID 和 IP 地址为键；
	// 同时返回 IP 冲突、需要删除的远端端点集合。
	getAllEndpointsByNetwork(networkName string) (map[string]*endpointInfo, map[string]bool, error)
	// deleteAllRemoteEndpointsWithDupIP 删除 getAllEndpointsByNetwork 发现的 IP 重复的远端端点。
	// Windows 的缺陷可能遗留此类陈旧端点，需要显式清理。
	deleteAllRemoteEndpointsWithDupIP(remoteEPsWithDupIP map[string]bool)
	getEndpointByID(id string) (*endpointInfo, error)
	getEndpointByIpAddress(ip string, networkName string) (*endpointInfo, error)
	getEndpointByName(id string) (*endpointInfo, error)
	createEndpoint(ep *endpointInfo, networkName string) (*endpointInfo, error)
	deleteEndpoint(hnsID string) error
	getLoadBalancer(endpoints []endpointInfo, flags loadBalancerFlags, sourceVip string, vip string, protocol uint16, internalPort uint16, externalPort uint16, previousLoadBalancers map[loadBalancerIdentifier]*loadBalancerInfo) (*loadBalancerInfo, error)
	getAllLoadBalancers() (map[loadBalancerIdentifier]*loadBalancerInfo, error)
	createOrReplaceLoadbalancer(proposedLB *hcn.HostComputeLoadBalancer, existingLBs map[loadBalancerIdentifier]*loadBalancerInfo) (*hcn.HostComputeLoadBalancer, error)
	updateLoadBalancer(hnsID string, sourceVip, vip string, endpoints []endpointInfo, flags loadBalancerFlags, protocol, internalPort, externalPort uint16, previousLoadBalancers map[loadBalancerIdentifier]*loadBalancerInfo) (*loadBalancerInfo, error)
	deleteLoadBalancer(hnsID string) error
}

// hns 是基于 HCN 实现的 HostNetworkService。
type hns struct {
	hcn HcnService
}

// 供 HCN 负载均衡器与端口映射使用的扩展标志位。
var (
	// LoadBalancerFlagsIPv6 表示 IPv6 负载均衡器。
	LoadBalancerFlagsIPv6 hcn.LoadBalancerFlags = 2
	// LoadBalancerPortMappingFlagsVipExternalIP 表示端口映射使用 VIP 外部 IP。
	LoadBalancerPortMappingFlagsVipExternalIP hcn.LoadBalancerPortMappingFlags = 16
)

// 用于识别 HCN/HNS 特定错误的 Win32 错误码（取自错误消息文本）。
const (

	// 0x6b5 是标准的 Win32 RPC 错误（RPC_S_UNKNOWN_IF）：
	// 当目标服务的 RPC 接口未注册时由 RPC 运行时返回；在此场景下通常意味着
	// HNS 服务未运行或尚未完成初始化。
	errorCodeHnsNotRunning = "0x6b5"

	// 0xb7（ERROR_ALREADY_EXISTS）表示对象已存在：
	// 在创建同名资源时由系统返回。
	errorCodeFileAlreadyExists = "0xb7"
)

// IsHnsNotRunningError 通过错误消息中的特定错误码判断错误是否由 HNS 服务未运行引起。
func IsHnsNotRunningError(err error) bool {
	return err != nil && strings.Contains(err.Error(), errorCodeHnsNotRunning)
}

// IsPolicyAlreadyExists 判断错误是否由前端配置相同的负载均衡器策略已存在引起：
// 既检查 HCN 的端口冲突错误，也匹配表示资源已存在的错误码。
func IsPolicyAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	return hcn.IsPortAlreadyExistsError(err) || strings.Contains(err.Error(), errorCodeFileAlreadyExists)
}

// findExistingLBIdByFrontend 查找前端配置与待创建负载均衡器相同的现存策略 ID。
func findExistingLBIdByFrontend(proposedLB *hcn.HostComputeLoadBalancer, existingLBs map[loadBalancerIdentifier]*loadBalancerInfo) string {
	if len(proposedLB.PortMappings) == 0 {
		return ""
	}
	lbID := ""
	frontEndVIP, isProposedLbIPv6 := "", (proposedLB.Flags&LoadBalancerFlagsIPv6) == LoadBalancerFlagsIPv6
	if len(proposedLB.FrontendVIPs) != 0 {
		frontEndVIP = proposedLB.FrontendVIPs[0]
	}
	for id, lbInfo := range existingLBs {
		if id.vip == frontEndVIP && id.protocol == uint16(proposedLB.PortMappings[0].Protocol) && id.internalPort == proposedLB.PortMappings[0].InternalPort && id.externalPort == proposedLB.PortMappings[0].ExternalPort && id.isIPv6 == isProposedLbIPv6 {
			if lbID != "" {
				// 多个现存策略匹配同一前端配置时返回空 ID，避免误删其中任何一个。
				klog.Warning("More than 1 existing lb has matching frontend, will not delete any existing lbs", "existingLBID1", lbID, "existingLBID2", lbInfo.hnsID, "frontendVIP", frontEndVIP, "protocol", proposedLB.PortMappings[0].Protocol, "internalPort", proposedLB.PortMappings[0].InternalPort, "externalPort", proposedLB.PortMappings[0].ExternalPort, "isIPv6", isProposedLbIPv6)
				return ""
			}
			lbID = lbInfo.hnsID
		}
	}
	klog.V(4).InfoS("Search for existing lb with matching frontend completed", "lbID", lbID, "frontendVIP", frontEndVIP, "protocol", proposedLB.PortMappings[0].Protocol, "internalPort", proposedLB.PortMappings[0].InternalPort, "externalPort", proposedLB.PortMappings[0].ExternalPort, "isIPv6", isProposedLbIPv6)
	return lbID
}

// getLoadBalancerPolicyFlags 将内部标志转换为 HCN 的端口映射标志和负载均衡器标志。
func getLoadBalancerPolicyFlags(flags loadBalancerFlags) (lbPortMappingFlags hcn.LoadBalancerPortMappingFlags, lbFlags hcn.LoadBalancerFlags) {
	lbPortMappingFlags = hcn.LoadBalancerPortMappingFlagsNone
	if flags.isILB {
		lbPortMappingFlags |= hcn.LoadBalancerPortMappingFlagsILB
	}
	if flags.useMUX {
		lbPortMappingFlags |= hcn.LoadBalancerPortMappingFlagsUseMux
	}
	if flags.preserveDIP {
		lbPortMappingFlags |= hcn.LoadBalancerPortMappingFlagsPreserveDIP
	}
	if flags.localRoutedVIP {
		lbPortMappingFlags |= hcn.LoadBalancerPortMappingFlagsLocalRoutedVIP
	}
	if flags.isVipExternalIP {
		lbPortMappingFlags |= LoadBalancerPortMappingFlagsVipExternalIP
	}
	lbFlags = hcn.LoadBalancerFlagsNone
	if flags.isDSR {
		lbFlags |= hcn.LoadBalancerFlagsDSR
	}
	if flags.isIPv6 {
		lbFlags |= LoadBalancerFlagsIPv6
	}
	return
}

// getNetworkByName 查询 HNS 网络，并解析其中携带的远程子网路由策略。
func (hns hns) getNetworkByName(name string) (*hnsNetworkInfo, error) {
	hnsnetwork, err := hns.hcn.GetNetworkByName(name)
	if err != nil {
		klog.ErrorS(err, "Error getting network by name")
		return nil, err
	}

	var remoteSubnets []*remoteSubnetInfo
	for _, policy := range hnsnetwork.Policies {
		if policy.Type == hcn.RemoteSubnetRoute {
			policySettings := hcn.RemoteSubnetRoutePolicySetting{}
			err = json.Unmarshal(policy.Settings, &policySettings)
			if err != nil {
				return nil, fmt.Errorf("failed to unmarshal Remote Subnet policy settings")
			}
			rs := &remoteSubnetInfo{
				destinationPrefix: policySettings.DestinationPrefix,
				isolationID:       policySettings.IsolationId,
				providerAddress:   policySettings.ProviderAddress,
				drMacAddress:      policySettings.DistributedRouterMacAddress,
			}
			remoteSubnets = append(remoteSubnets, rs)
		}
	}

	return &hnsNetworkInfo{
		id:            hnsnetwork.Id,
		name:          hnsnetwork.Name,
		networkType:   string(hnsnetwork.Type),
		remoteSubnets: remoteSubnets,
	}, nil
}

// getAllEndpointsByNetwork 查询网络上的全部端点，按 ID 和 IP 建立索引，
// 并返回 IP 重复、需要清理的远端端点集合。
func (hns hns) getAllEndpointsByNetwork(networkName string) (map[string]*(endpointInfo), map[string]bool, error) {
	hcnnetwork, err := hns.hcn.GetNetworkByName(networkName)
	if err != nil {
		klog.ErrorS(err, "failed to get HNS network by name", "name", networkName)
		return nil, nil, err
	}
	endpoints, err := hns.hcn.ListEndpointsOfNetwork(hcnnetwork.Id)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to list endpoints: %w", err)
	}

	endpointInfos := make(map[string]*(endpointInfo))
	remoteEPsWithDupIP := make(map[string]bool)

	for _, ep := range endpoints {

		if len(ep.IpConfigurations) == 0 {
			klog.V(3).InfoS("No IpConfigurations found in endpoint info of queried endpoints", "endpoint", ep)
			continue
		}

		for index, ipConfig := range ep.IpConfigurations {

			if index > 1 {
				// 预期最多只有 IPv4 和 IPv6 两个地址；
				// 出现更多地址属于异常情况，记录警告后跳出循环，避免使用意外数据。
				klog.Warning("Endpoint ipconfiguration holds more than 2 IP addresses.", "hnsID", ep.Id, "IP", ipConfig.IpAddress, "ipConfigCount", len(ep.IpConfigurations))
				break
			}

			curEpIsLocal := uint32(ep.Flags&hcn.EndpointFlagsRemoteEndpoint) == 0

			if existingEp, ok := endpointInfos[ipConfig.IpAddress]; ok {
				if curEpIsLocal && !existingEp.isLocal {
					// 本地端点优先：发现残留的远端端点时标记删除，并用本地端点覆盖。
					remoteEPsWithDupIP[existingEp.hnsID] = true
					delete(endpointInfos, existingEp.hnsID)
					delete(endpointInfos, existingEp.ip)
					// 继续执行下面的逻辑，用本地端点更新映射。
				} else if !curEpIsLocal && existingEp.isLocal {
					// 映射中已有本地端点时，后到的远端端点标记删除并跳过。
					remoteEPsWithDupIP[ep.Id] = true
					continue
				} else {
					continue // 同类端点保留首次记录即可
				}
			}

			// 同时以端点 ID 和 IP 作为键存入映射：虽然额外占用内存，但 Windows Server 2019/2022
			// 存在本地与远端端点使用相同 IP 的缺陷，必须支持两种查找方式。
			// TODO: 仅按 IP 存储，并移除所有按端点 ID 的查找。
			epInfo := &endpointInfo{
				ip:         ipConfig.IpAddress,
				isLocal:    curEpIsLocal,
				macAddress: ep.MacAddress,
				hnsID:      ep.Id,
				hns:        hns,
				// 只有 ready 且非 terminating 的端点才会写入 HNS，这里直接置为服务状态。
				ready:       true,
				serving:     true,
				terminating: false,
			}
			endpointInfos[ep.Id] = epInfo
			endpointInfos[ipConfig.IpAddress] = epInfo
		}
	}
	klog.V(3).InfoS("Queried endpoints from network", "network", networkName, "count", len(endpointInfos))
	klog.V(5).InfoS("Queried endpoints details", "network", networkName, "endpointInfos", endpointInfos)
	return endpointInfos, remoteEPsWithDupIP, nil
}

// deleteAllRemoteEndpointsWithDupIP 清理 IP 重复、已不再使用的远端端点。
func (hns hns) deleteAllRemoteEndpointsWithDupIP(remoteEPsWithDupIP map[string]bool) {
	for hnsID := range remoteEPsWithDupIP {
		klog.V(3).InfoS("Deleting stale remote endpoint with duplicate IP", "hnsID", hnsID)
		err := hns.deleteEndpoint(hnsID)
		if err != nil {
			klog.ErrorS(err, "Failed to delete stale remote endpoint with duplicate IP", "hnsID", hnsID)
		}
	}
}

// getEndpointByID 按 HNS ID 查询端点并转换为内部表示。
func (hns hns) getEndpointByID(id string) (*endpointInfo, error) {
	hnsendpoint, err := hns.hcn.GetEndpointByID(id)
	if err != nil {
		return nil, err
	}
	return &endpointInfo{ //TODO: fill out PA
		ip:         hnsendpoint.IpConfigurations[0].IpAddress,
		isLocal:    uint32(hnsendpoint.Flags&hcn.EndpointFlagsRemoteEndpoint) == 0, //TODO: Change isLocal to isRemote
		macAddress: hnsendpoint.MacAddress,
		hnsID:      hnsendpoint.Id,
		hns:        hns,
	}, nil
}

// getEndpointByIpAddress 在指定网络中按 IP 查找端点。
func (hns hns) getEndpointByIpAddress(ip string, networkName string) (*endpointInfo, error) {
	hnsnetwork, err := hns.hcn.GetNetworkByName(networkName)
	if err != nil {
		klog.ErrorS(err, "Error getting network by name")
		return nil, err
	}

	endpoints, err := hns.hcn.ListEndpoints()
	if err != nil {
		return nil, fmt.Errorf("failed to list endpoints: %w", err)
	}
	for _, endpoint := range endpoints {
		equal := false
		if len(endpoint.IpConfigurations) > 0 {
			equal = endpoint.IpConfigurations[0].IpAddress == ip

			if !equal && len(endpoint.IpConfigurations) > 1 {
				equal = endpoint.IpConfigurations[1].IpAddress == ip
			}
		}
		if equal && strings.EqualFold(endpoint.HostComputeNetwork, hnsnetwork.Id) {
			return &endpointInfo{
				ip:         ip,
				isLocal:    uint32(endpoint.Flags&hcn.EndpointFlagsRemoteEndpoint) == 0, //TODO: Change isLocal to isRemote
				macAddress: endpoint.MacAddress,
				hnsID:      endpoint.Id,
				hns:        hns,
			}, nil
		}
	}
	return nil, fmt.Errorf("Endpoint %v not found on network %s", ip, networkName)
}

// getEndpointByName 按名称查询端点并转换为内部表示。
func (hns hns) getEndpointByName(name string) (*endpointInfo, error) {
	hnsendpoint, err := hns.hcn.GetEndpointByName(name)
	if err != nil {
		return nil, err
	}
	return &endpointInfo{ //TODO: fill out PA
		ip:         hnsendpoint.IpConfigurations[0].IpAddress,
		isLocal:    uint32(hnsendpoint.Flags&hcn.EndpointFlagsRemoteEndpoint) == 0, //TODO: Change isLocal to isRemote
		macAddress: hnsendpoint.MacAddress,
		hnsID:      hnsendpoint.Id,
		hns:        hns,
	}, nil
}

// createEndpoint 在指定网络上创建本地或远端端点；
// 远端端点会附带 ProviderAddress 策略以供 Overlay 网络路由使用。
func (hns hns) createEndpoint(ep *endpointInfo, networkName string) (*endpointInfo, error) {
	hnsNetwork, err := hns.hcn.GetNetworkByName(networkName)
	if err != nil {
		return nil, err
	}
	var flags hcn.EndpointFlags
	if !ep.isLocal {
		flags |= hcn.EndpointFlagsRemoteEndpoint
	}
	ipConfig := &hcn.IpConfig{
		IpAddress: ep.ip,
	}
	hnsEndpoint := &hcn.HostComputeEndpoint{
		IpConfigurations: []hcn.IpConfig{*ipConfig},
		MacAddress:       ep.macAddress,
		Flags:            flags,
		SchemaVersion: hcn.SchemaVersion{
			Major: 2,
			Minor: 0,
		},
	}

	var createdEndpoint *hcn.HostComputeEndpoint
	if !ep.isLocal {
		if len(ep.providerAddress) != 0 {
			policySettings := hcn.ProviderAddressEndpointPolicySetting{
				ProviderAddress: ep.providerAddress,
			}
			policySettingsJson, err := json.Marshal(policySettings)
			if err != nil {
				return nil, fmt.Errorf("PA Policy creation failed: %v", err)
			}
			paPolicy := hcn.EndpointPolicy{
				Type:     hcn.NetworkProviderAddress,
				Settings: policySettingsJson,
			}
			hnsEndpoint.Policies = append(hnsEndpoint.Policies, paPolicy)
		}
		createdEndpoint, err = hns.hcn.CreateRemoteEndpoint(hnsNetwork, hnsEndpoint)
		if err != nil {
			return nil, err
		}
		klog.V(3).InfoS("Created remote endpoint resource", "hnsID", createdEndpoint.Id)
	} else {
		createdEndpoint, err = hns.hcn.CreateEndpoint(hnsNetwork, hnsEndpoint)
		if err != nil {
			return nil, err
		}
		klog.V(3).InfoS("Created local endpoint resource", "hnsID", createdEndpoint.Id)
	}
	return &endpointInfo{
		ip:              createdEndpoint.IpConfigurations[0].IpAddress,
		isLocal:         uint32(createdEndpoint.Flags&hcn.EndpointFlagsRemoteEndpoint) == 0,
		macAddress:      createdEndpoint.MacAddress,
		hnsID:           createdEndpoint.Id,
		providerAddress: ep.providerAddress, //TODO get from createdEndpoint
		hns:             hns,
	}, nil
}

// deleteEndpoint 按 HNS ID 删除端点。
func (hns hns) deleteEndpoint(hnsID string) error {
	hnsendpoint, err := hns.hcn.GetEndpointByID(hnsID)
	if err != nil {
		return err
	}
	err = hns.hcn.DeleteEndpoint(hnsendpoint)
	if err == nil {
		klog.V(3).InfoS("Remote endpoint resource deleted", "hnsID", hnsID)
	}
	return err
}

// findLoadBalancerID 根据负载均衡器字段构造唯一标识，后端集合以哈希形式参与标识。
func findLoadBalancerID(endpoints []endpointInfo, vip string, protocol, internalPort, externalPort uint16, isIpv6 bool) (loadBalancerIdentifier, error) {
	// 对后端端点 ID 求解哈希，使后端集合变化能够反映到标识中。
	hash, err := hashEndpoints(endpoints)
	if err != nil {
		klog.V(2).ErrorS(err, "Error hashing endpoints", "endpoints", endpoints)
		return loadBalancerIdentifier{}, err
	}
	return loadBalancerIdentifier{protocol: protocol, internalPort: internalPort, externalPort: externalPort, vip: vip, endpointsHash: hash, isIPv6: isIpv6}, nil
}

// getAllLoadBalancers 查询所有 HCN 负载均衡器，按前端配置与后端哈希建立索引。
func (hns hns) getAllLoadBalancers() (map[loadBalancerIdentifier]*loadBalancerInfo, error) {
	lbs, err := hns.hcn.ListLoadBalancers()
	var id loadBalancerIdentifier
	if err != nil {
		return nil, err
	}
	loadBalancers := make(map[loadBalancerIdentifier]*(loadBalancerInfo))
	for _, lb := range lbs {
		// PortMappings 由 HNS 填充，在策略只创建了一半时可能为空；
		// 这种条目无法按端口建立索引，只能跳过。
		if len(lb.PortMappings) == 0 {
			klog.V(2).InfoS("Skipping load balancer with no port mappings", "hnsLbID", lb.Id)
			continue
		}
		portMap := lb.PortMappings[0]
		isIPv6 := (lb.Flags & LoadBalancerFlagsIPv6) == LoadBalancerFlagsIPv6
		// 对后端端点 ID 求解哈希，使后端集合变化能够反映到标识中。
		hash, err := hashEndpoints(lb.HostComputeEndpoints)
		if err != nil {
			klog.V(2).ErrorS(err, "Error hashing endpoints", "policy", lb)
			return nil, err
		}
		if len(lb.FrontendVIPs) == 0 {
			// 没有前端 VIP 时保持该字段为空。
			id = loadBalancerIdentifier{protocol: uint16(portMap.Protocol), internalPort: portMap.InternalPort, externalPort: portMap.ExternalPort, endpointsHash: hash, isIPv6: isIPv6}
		} else {
			id = loadBalancerIdentifier{protocol: uint16(portMap.Protocol), internalPort: portMap.InternalPort, externalPort: portMap.ExternalPort, vip: lb.FrontendVIPs[0], endpointsHash: hash, isIPv6: isIPv6}
		}
		loadBalancers[id] = &loadBalancerInfo{
			hnsID: lb.Id,
		}
	}
	klog.V(3).InfoS("Queried load balancers", "count", len(lbs))
	return loadBalancers, nil
}

// createOrReplaceLoadbalancer 创建负载均衡器；若前端配置冲突，则删除冲突的旧策略后重试。
func (hns hns) createOrReplaceLoadbalancer(proposedLB *hcn.HostComputeLoadBalancer, existingLBs map[loadBalancerIdentifier]*loadBalancerInfo) (*hcn.HostComputeLoadBalancer, error) {
	lb, err := hns.hcn.CreateLoadBalancer(proposedLB)
	if IsPolicyAlreadyExists(err) && len(proposedLB.PortMappings) > 0 {
		frontEndVIP := ""
		if len(proposedLB.FrontendVIPs) != 0 {
			frontEndVIP = proposedLB.FrontendVIPs[0]
		}
		existingLbID := findExistingLBIdByFrontend(proposedLB, existingLBs)
		if existingLbID != "" {
			klog.V(4).InfoS("Deleting matching load balancer and retrying create load balancer again", "existingLBID", existingLbID, "frontendVIP", frontEndVIP, "protocol", proposedLB.PortMappings[0].Protocol, "internalPort", proposedLB.PortMappings[0].InternalPort, "externalPort", proposedLB.PortMappings[0].ExternalPort)
			err = hns.deleteLoadBalancer(existingLbID)
			if err != nil {
				klog.V(1).InfoS("Failed to delete existing load balancer", "lbID", existingLbID, "error", err)
				return nil, err
			}
			lb, err = hns.hcn.CreateLoadBalancer(proposedLB)
		} else {
			klog.V(4).InfoS("Did not find a unique existing load balancer with matching frontend configuration", "frontendVIP", frontEndVIP, "protocol", proposedLB.PortMappings[0].Protocol, "internalPort", proposedLB.PortMappings[0].InternalPort, "externalPort", proposedLB.PortMappings[0].ExternalPort)
		}
	}
	return lb, err
}

// getLoadBalancer 优先复用缓存中的策略，未命中时按配置创建 HNS 负载均衡器。
func (hns hns) getLoadBalancer(endpoints []endpointInfo, flags loadBalancerFlags, sourceVip string, vip string, protocol uint16, internalPort uint16, externalPort uint16, previousLoadBalancers map[loadBalancerIdentifier]*loadBalancerInfo) (*loadBalancerInfo, error) {
	var id loadBalancerIdentifier
	vips := []string{}
	id, lbIdErr := findLoadBalancerID(
		endpoints,
		vip,
		protocol,
		internalPort,
		externalPort,
		flags.isIPv6,
	)

	if lbIdErr != nil {
		klog.V(2).ErrorS(lbIdErr, "Error hashing endpoints", "endpoints", endpoints)
		return nil, lbIdErr
	}

	if len(vip) > 0 {
		vips = append(vips, vip)
	}

	if lb, found := previousLoadBalancers[id]; found {
		klog.V(1).InfoS("Found cached Hns loadbalancer policy resource", "policies", lb)
		return lb, nil
	}

	lbPortMappingFlags := hcn.LoadBalancerPortMappingFlagsNone
	if flags.isILB {
		lbPortMappingFlags |= hcn.LoadBalancerPortMappingFlagsILB
	}
	if flags.useMUX {
		lbPortMappingFlags |= hcn.LoadBalancerPortMappingFlagsUseMux
	}
	if flags.preserveDIP {
		lbPortMappingFlags |= hcn.LoadBalancerPortMappingFlagsPreserveDIP
	}
	if flags.localRoutedVIP {
		lbPortMappingFlags |= hcn.LoadBalancerPortMappingFlagsLocalRoutedVIP
	}
	if flags.isVipExternalIP {
		lbPortMappingFlags |= LoadBalancerPortMappingFlagsVipExternalIP
	}

	lbFlags := hcn.LoadBalancerFlagsNone
	if flags.isDSR {
		lbFlags |= hcn.LoadBalancerFlagsDSR
	}

	if flags.isIPv6 {
		lbFlags |= LoadBalancerFlagsIPv6
	}

	lbDistributionType := hcn.LoadBalancerDistributionNone

	if flags.sessionAffinity {
		lbDistributionType = hcn.LoadBalancerDistributionSourceIP
	}

	loadBalancer := &hcn.HostComputeLoadBalancer{
		SourceVIP: sourceVip,
		PortMappings: []hcn.LoadBalancerPortMapping{
			{
				Protocol:         uint32(protocol),
				InternalPort:     internalPort,
				ExternalPort:     externalPort,
				DistributionType: lbDistributionType,
				Flags:            lbPortMappingFlags,
			},
		},
		FrontendVIPs: vips,
		SchemaVersion: hcn.SchemaVersion{
			Major: 2,
			Minor: 0,
		},
		Flags: lbFlags,
	}

	for _, ep := range endpoints {
		loadBalancer.HostComputeEndpoints = append(loadBalancer.HostComputeEndpoints, ep.hnsID)
	}

	lb, err := hns.createOrReplaceLoadbalancer(loadBalancer, previousLoadBalancers)

	if err != nil {
		klog.V(2).ErrorS(err, "Error creating Hns loadbalancer policy resource", "error", err, "endpoints", endpoints)
		return nil, err
	}

	klog.V(1).InfoS("Created Hns loadbalancer policy resource", "loadBalancer", lb)
	lbInfo := &loadBalancerInfo{
		hnsID: lb.Id,
	}
	// 写入缓存，供本轮后续同步复用。
	previousLoadBalancers[id] = lbInfo
	return lbInfo, err
}

// updateLoadBalancer 原地更新指定 HNS ID 的负载均衡器策略。
func (hns hns) updateLoadBalancer(hnsID string,
	sourceVip,
	vip string,
	endpoints []endpointInfo,
	flags loadBalancerFlags,
	protocol,
	internalPort,
	externalPort uint16,
	previousLoadBalancers map[loadBalancerIdentifier]*loadBalancerInfo) (*loadBalancerInfo, error) {
	klog.V(3).InfoS("Updating existing loadbalancer called", "hnsLbID", hnsID, "endpointCount", len(endpoints), "vip", vip, "sourceVip", sourceVip, "internalPort", internalPort, "externalPort", externalPort)

	var id loadBalancerIdentifier
	vips := []string{}
	// 对后端端点 ID 求解哈希，使后端集合变化能够反映到标识中。
	hash, err := hashEndpoints(endpoints)
	if err != nil {
		klog.V(2).ErrorS(err, "Error hashing endpoints", "endpoints", endpoints)
		return nil, err
	}
	if len(vip) > 0 {
		id = loadBalancerIdentifier{protocol: protocol, internalPort: internalPort, externalPort: externalPort, vip: vip, endpointsHash: hash, isIPv6: flags.isIPv6}
		vips = append(vips, vip)
	} else {
		id = loadBalancerIdentifier{protocol: protocol, internalPort: internalPort, externalPort: externalPort, endpointsHash: hash, isIPv6: flags.isIPv6}
	}

	if lb, found := previousLoadBalancers[id]; found {
		klog.V(1).InfoS("Found cached Hns loadbalancer policy resource", "policies", lb)
		return lb, nil
	}

	lbPortMappingFlags, lbFlags := getLoadBalancerPolicyFlags(flags)

	lbDistributionType := hcn.LoadBalancerDistributionNone

	if flags.sessionAffinity {
		lbDistributionType = hcn.LoadBalancerDistributionSourceIP
	}

	loadBalancer := &hcn.HostComputeLoadBalancer{
		SourceVIP: sourceVip,
		PortMappings: []hcn.LoadBalancerPortMapping{
			{
				Protocol:         uint32(protocol),
				InternalPort:     internalPort,
				ExternalPort:     externalPort,
				DistributionType: lbDistributionType,
				Flags:            lbPortMappingFlags,
			},
		},
		FrontendVIPs: vips,
		SchemaVersion: hcn.SchemaVersion{
			Major: 2,
			Minor: 0,
		},
		Flags: lbFlags,
	}

	for _, ep := range endpoints {
		loadBalancer.HostComputeEndpoints = append(loadBalancer.HostComputeEndpoints, ep.hnsID)
	}

	lb, err := hns.hcn.UpdateLoadBalancer(loadBalancer, hnsID)

	if err != nil {
		klog.V(2).ErrorS(err, "Error updating existing loadbalancer", "hnsLbID", hnsID, "error", err, "endpoints", endpoints)
		return nil, err
	}

	klog.V(1).InfoS("Update loadbalancer is successful", "loadBalancer", lb)
	lbInfo := &loadBalancerInfo{
		hnsID: lb.Id,
	}
	// 写入缓存，供本轮后续同步复用。
	previousLoadBalancers[id] = lbInfo
	return lbInfo, err
}

// deleteLoadBalancer 按 HNS ID 删除负载均衡器策略；对偶发失败重试一次。
func (hns hns) deleteLoadBalancer(hnsID string) error {
	lb, err := hns.hcn.GetLoadBalancerByID(hnsID)
	if err != nil {
		if hcn.IsNotFoundError(err) {
			klog.V(1).InfoS("LoadBalancer policy resource not found, may have already been deleted", "lbID", hnsID)
			// 策略已经不存在，视为删除成功。
			return nil
		}
		if IsHnsNotRunningError(err) {
			klog.V(1).ErrorS(err, "HNS is not running, skipping delete loadbalancer", "lbID", hnsID)
			return err
		}
		klog.V(2).ErrorS(err, "Error getting Hns loadbalancer policy resource by ID", "lbID", hnsID)
		return err
	}

	err = hns.hcn.DeleteLoadBalancer(lb)
	if err != nil {
		// Windows Server 2019 的缺陷会导致删除调用偶发失败，这里再重试一次。
		// TODO: 未来应重写 syncProxyRules 的逻辑，借助 policyApplied 字段分阶段处理此类失败。
		klog.V(1).ErrorS(err, "Error deleting Hns loadbalancer policy resource. Attempting one more time...", "loadBalancer", lb)
		err = hns.hcn.DeleteLoadBalancer(lb)
	}
	if err != nil {
		klog.V(2).ErrorS(err, "Error deleting Hns loadbalancer policy resource again.", "hnsID", hnsID)
		return err
	}
	klog.V(3).InfoS("Deleted Hns loadbalancer policy resource", "hnsID", hnsID)
	return err
}

// hashEndpoints 根据给定的端点 ID 计算哈希（使用 SHA-1）。
func hashEndpoints[T string | endpointInfo](endpoints []T) (hash [20]byte, err error) {
	var id string
	// 捕获处理过程中的 panic：出错时返回错误和空哈希，避免拖垮同步流程。
	defer func() {
		if r := recover(); r != nil {
			err = r.(error)
			hash = [20]byte{}
		}
	}()

	// 遍历端点并计算哈希。
	for _, ep := range endpoints {
		switch x := any(ep).(type) {
		case endpointInfo:
			id = strings.ToUpper(x.hnsID)
		case string:
			id = strings.ToUpper(x)
		}
		if len(id) > 0 {
			// 端点是无序集合，因此对各端点哈希做异或；
			// 这样可能产生碰撞，但还有其他键共同标识负载均衡器，因此可以接受。
			hash = xor(hash, sha1.Sum(([]byte(id))))
		}
	}
	return
}

// xor 逐字节对两个 20 字节数组做异或。
func xor(b1 [20]byte, b2 [20]byte) (xorbytes [20]byte) {
	for i := 0; i < 20; i++ {
		xorbytes[i] = b1[i] ^ b2[i]
	}
	return xorbytes
}
