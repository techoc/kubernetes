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

package v1

const (
	// LabelHostname 是节点的主机名标签。
	LabelHostname = "kubernetes.io/hostname"

	// 标签值是以 <ip:port> 格式保存的 kube-apiserver 网络位置。
	// 存于 APIServer Identity lease 对象中，用于查看 peer proxy 使用的地址。
	AnnotationPeerAdvertiseAddress = "kubernetes.io/peer-advertise-address"

	// LabelTopologyZone 是节点所在的拓扑区域标签。
	LabelTopologyZone = "topology.kubernetes.io/zone"
	// LabelTopologyRegion 是节点所在的拓扑地域标签。
	LabelTopologyRegion = "topology.kubernetes.io/region"

	// 这些标签自 1.17 起已废弃，但在可预见的未来仍会支持，
	// 以兼容使用它们的长生命周期 PV 等对象。新用户应使用
	// "topology.kubernetes.io/*" 等价标签。
	LabelFailureDomainBetaZone   = "failure-domain.beta.kubernetes.io/zone"   // 已废弃
	LabelFailureDomainBetaRegion = "failure-domain.beta.kubernetes.io/region" // 已废弃

	// 为 vendor 兼容而保留。新代码不要使用这些常量。
	LabelZoneFailureDomain       = LabelFailureDomainBetaZone   // 已废弃
	LabelZoneRegion              = LabelFailureDomainBetaRegion // 已废弃
	LabelZoneFailureDomainStable = LabelTopologyZone            // 已废弃
	LabelZoneRegionStable        = LabelTopologyRegion          // 已废弃

	// LabelInstanceType 是 beta 版的节点实例类型标签。
	LabelInstanceType = "beta.kubernetes.io/instance-type"
	// LabelInstanceTypeStable 是稳定的节点实例类型标签。
	LabelInstanceTypeStable = "node.kubernetes.io/instance-type"

	// LabelOSStable 是稳定的节点操作系统标签。
	LabelOSStable = "kubernetes.io/os"
	// LabelArchStable 是稳定的节点架构标签。
	LabelArchStable = "kubernetes.io/arch"

	// LabelWindowsBuild 自 v1.17.0 起用于 Windows 节点，指定 Windows 构建号。
	// 格式为 MajorVersion.MinorVersion.BuildNumber（例如 10.0.17763）。
	LabelWindowsBuild = "node.kubernetes.io/windows-build"

	// LabelNamespaceSuffixKubelet 是 kubelet 可自行设置的标签命名空间后缀（[*.]kubelet.kubernetes.io/*）。
	LabelNamespaceSuffixKubelet = "kubelet.kubernetes.io"
	// LabelNamespaceSuffixNode 是 kubelet 可自行设置的标签命名空间后缀（[*.]node.kubernetes.io/*）。
	LabelNamespaceSuffixNode = "node.kubernetes.io"

	// LabelNamespaceNodeRestriction 是启用 NodeRestriction 准入插件时 kubelet 不得自行设置的禁用标签命名空间。
	LabelNamespaceNodeRestriction = "node-restriction.kubernetes.io"

	// IsHeadlessService 由 Controller 添加到 Endpoint，表示其父 Service 是否为 Headless。
	// 其他控制器和 kube-proxy 可据此判断使用 Headless Service 时是否应复制 Endpoint 对象。
	IsHeadlessService = "service.kubernetes.io/headless"

	// LabelNodeExcludeBalancers 指定该节点不应作为以节点为第二跳的外部负载均衡器的目标
	// （例如许多只识别节点的云 LB）。对外服务若使用 externalTrafficPolicy=Local，
	// 这可能意味着被排除节点上的后端对这些外部负载均衡器不可达。
	// 具体排除实现因云厂商而异。
	LabelNodeExcludeBalancers = "node.kubernetes.io/exclude-from-external-load-balancers"
	// LabelMetadataName 是 tree 内用于自动标注命名空间标签名，便于需要确定性标签的工具选择命名空间。
	LabelMetadataName = "kubernetes.io/metadata.name"
)
