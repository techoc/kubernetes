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

package validation

import (
	"fmt"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	metavalidation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	apivalidation "k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/apis/discovery"
	netutils "k8s.io/utils/net"
)

// 以下变量集中定义 EndpointSlice 的规模上限与受支持取值，
// 目的是给 apiserver 的校验与存储划出可预期的边界，避免单个对象无限膨胀。
var (
	// 支持的地址族集合：IPv4、IPv6 与完全限定域名（FQDN）。
	supportedAddressTypes = sets.New(
		discovery.AddressTypeIPv4,
		discovery.AddressTypeIPv6,
		discovery.AddressTypeFQDN,
	)
	// 端口协议集合，与 Service 的端口协议保持一致。
	supportedPortProtocols = sets.New(
		corev1.ProtocolTCP,
		corev1.ProtocolUDP,
		corev1.ProtocolSCTP,
	)
	// 依次为：topology 标签数、单端点地址数、端口数、端点数、
	// zone 提示数、节点提示数。超限即拒绝，避免校验和存储成本失控。
	maxTopologyLabels = 16
	maxAddresses      = 100
	maxPorts          = 20000
	maxEndpoints      = 1000
	maxZoneHints      = 8
	maxNodeHints      = 8
)

// ValidateEndpointSliceName 校验 EndpointSlice 名称是否合法，复用 DNS 子域名规则：
// 仅允许小写字母、数字和 '-'，以字母或数字开头和结尾，且长度受限。
// prefix 参数表示名称将作为生成式名称的前缀使用，此时允许以 '-' 结尾，
// 因为控制器会在其后追加随机后缀。
var ValidateEndpointSliceName = apimachineryvalidation.NameIsDNSSubdomain

// ValidateEndpointSlice 对 EndpointSlice 做完整校验并汇总所有字段错误。
// 校验顺序：metadata -> addressType -> ports -> endpoints。
// oldEndpointSlice 仅在更新时传入，用来对存量数据做兼容处理（见 endpoints 校验）。
func ValidateEndpointSlice(endpointSlice, oldEndpointSlice *discovery.EndpointSlice) field.ErrorList {
	// 第二个参数为 true：EndpointSlice 允许由控制器通过 generateName 生成名称。
	allErrs := apivalidation.ValidateObjectMeta(&endpointSlice.ObjectMeta, true, ValidateEndpointSliceName, field.NewPath("metadata"))
	// 先确定地址类型，后续端点地址该按哪种格式校验取决于它。
	allErrs = append(allErrs, validateAddressType(endpointSlice.AddressType)...)
	allErrs = append(allErrs, validatePorts(endpointSlice.Ports, field.NewPath("ports"))...)

	endpointsErrs := validateEndpoints(endpointSlice.Endpoints, endpointSlice.AddressType, field.NewPath("endpoints"))
	if len(endpointsErrs) != 0 {
		// 更新场景下，如果 Endpoints 没有任何变化，就忽略这里发现的校验错误：
		// 老版本 Kubernetes 曾认为这些数据合法，不应因为升级而导致存量对象无法更新。
		// 只有先拿到错误才做 DeepEqual，是因为 Endpoints 可能很大、比较开销高；
		// 没有错误时无需付出这份代价。
		if oldEndpointSlice != nil && apiequality.Semantic.DeepEqual(oldEndpointSlice.Endpoints, endpointSlice.Endpoints) {
			endpointsErrs = nil
		}
	}
	allErrs = append(allErrs, endpointsErrs...)

	return allErrs
}

// ValidateEndpointSliceCreate 在创建 EndpointSlice 时调用。
// 创建时没有旧对象，因此以 nil 传入通用校验。
func ValidateEndpointSliceCreate(endpointSlice *discovery.EndpointSlice) field.ErrorList {
	return ValidateEndpointSlice(endpointSlice, nil)
}

// ValidateEndpointSliceUpdate 在更新 EndpointSlice 时调用。
// 除通用校验外，还要求 addressType 不可变，防止同一对象的地址语义被中途改写。
func ValidateEndpointSliceUpdate(newEndpointSlice, oldEndpointSlice *discovery.EndpointSlice) field.ErrorList {
	allErrs := ValidateEndpointSlice(newEndpointSlice, oldEndpointSlice)
	// addressType 一经创建不可修改；MarkCoveredByDeclarative 表明该规则已由声明式校验覆盖，避免重复报错。
	allErrs = append(allErrs, apivalidation.ValidateImmutableField(newEndpointSlice.AddressType, oldEndpointSlice.AddressType, field.NewPath("addressType")).WithOrigin("immutable").MarkCoveredByDeclarative()...)

	return allErrs
}

// validateEndpoints 校验端点列表的整体规模，并逐个校验地址、节点名、
// 已弃用的 topology、hostname 以及 hints。
func validateEndpoints(endpoints []discovery.Endpoint, addrType discovery.AddressType, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// 端点数超限时立即返回，避免对大量端点逐一校验。
	if len(endpoints) > maxEndpoints {
		allErrs = append(allErrs, field.TooMany(fldPath, len(endpoints), maxEndpoints))
		return allErrs
	}

	for i, endpoint := range endpoints {
		idxPath := fldPath.Index(i)
		addressPath := idxPath.Child("addresses")

		if len(endpoint.Addresses) == 0 {
			allErrs = append(allErrs, field.Required(addressPath, "must contain at least 1 address").MarkCoveredByDeclarative())
		} else if len(endpoint.Addresses) > maxAddresses {
			allErrs = append(allErrs, field.TooMany(addressPath, len(endpoint.Addresses), maxAddresses).WithOrigin("maxItems").MarkCoveredByDeclarative())
		}
		for i, address := range endpoint.Addresses {
			// 只按已知地址类型校验；未知类型不匹配任何分支，直接跳过，
			// 从而向前兼容未来可能新增的地址类型。
			switch addrType {
			case discovery.AddressTypeIPv4:
				// IPv4：通用 IP 格式 -> IPv4 地址族确认 -> 端点 IP 专项校验。
				ipErrs := apivalidation.IsValidIPForLegacyField(addressPath.Index(i), address, nil)
				if len(ipErrs) > 0 {
					allErrs = append(allErrs, ipErrs...)
				} else {
					if !netutils.IsIPv4String(address) {
						allErrs = append(allErrs, field.Invalid(addressPath, address, "must be an IPv4 address"))
					}
					allErrs = append(allErrs, apivalidation.ValidateEndpointIP(address, addressPath.Index(i))...)
				}
			case discovery.AddressTypeIPv6:
				// IPv6 流程与 IPv4 一致，只是地址族判断换成 IPv6。
				ipErrs := validation.IsValidIP(addressPath.Index(i), address)
				if len(ipErrs) > 0 {
					allErrs = append(allErrs, ipErrs...)
				} else {
					if !netutils.IsIPv6String(address) {
						allErrs = append(allErrs, field.Invalid(addressPath, address, "must be an IPv6 address"))
					}
					allErrs = append(allErrs, apivalidation.ValidateEndpointIP(address, addressPath.Index(i))...)
				}
			case discovery.AddressTypeFQDN:
				// FQDN 不是 IP 地址，按完全限定域名规则校验即可。
				allErrs = append(allErrs, validation.IsFullyQualifiedDomainName(addressPath.Index(i), address)...)
			}
		}

		// nodeName 可选，填写时必须是合法节点名。
		if endpoint.NodeName != nil {
			nnPath := idxPath.Child("nodeName")
			for _, msg := range apivalidation.ValidateNodeName(*endpoint.NodeName, false) {
				allErrs = append(allErrs, field.Invalid(nnPath, *endpoint.NodeName, msg))
			}
		}

		// topology 虽已弃用，仍需限制数量并校验标签格式。
		topologyPath := idxPath.Child("deprecatedTopology")
		if len(endpoint.DeprecatedTopology) > maxTopologyLabels {
			allErrs = append(allErrs, field.TooMany(topologyPath, len(endpoint.DeprecatedTopology), maxTopologyLabels))
		}
		allErrs = append(allErrs, metavalidation.ValidateLabels(endpoint.DeprecatedTopology, topologyPath)...)
		// zone 键在转换时应被提取到独立的 zone 字段；若仍残留说明转换逻辑有 bug，属内部错误。
		if _, found := endpoint.DeprecatedTopology[corev1.LabelTopologyZone]; found {
			allErrs = append(allErrs, field.InternalError(topologyPath.Key(corev1.LabelTopologyZone), fmt.Errorf("reserved key was not removed in conversion")))
		}

		// hostname 需符合 DNS1123 标签规则。
		if endpoint.Hostname != nil {
			allErrs = append(allErrs, apivalidation.ValidateDNS1123Label(*endpoint.Hostname, idxPath.Child("hostname"))...)
		}

		// hints 是可空指针，只有存在时才需要校验。
		if endpoint.Hints != nil {
			allErrs = append(allErrs, validateHints(endpoint.Hints, idxPath.Child("hints"))...)
		}
	}

	return allErrs
}

// validatePorts 校验端口列表：数量、名称合法性及唯一性、协议取值与 appProtocol 格式。
func validatePorts(endpointPorts []discovery.EndpointPort, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// 端口数量超限时直接返回。
	if len(endpointPorts) > maxPorts {
		allErrs = append(allErrs, field.TooMany(fldPath, len(endpointPorts), maxPorts))
		return allErrs
	}

	// 判重本可用 sets.Set，但这里用 []string 以避免额外内存分配；
	// 况且端口数量通常很少，线性查找的开销可以接受。
	portNames := make([]string, 0, len(endpointPorts))
	for i, endpointPort := range endpointPorts {
		idxPath := fldPath.Index(i)

		// 端口名可为空（匿名端口）；非空时须是 DNS1123 标签。
		if len(*endpointPort.Name) > 0 {
			allErrs = append(allErrs, apivalidation.ValidateDNS1123Label(*endpointPort.Name, idxPath.Child("name"))...)
		}

		// 同一 EndpointSlice 内端口名必须唯一，否则无法按名定位端口。
		if slices.Contains(portNames, *endpointPort.Name) {
			allErrs = append(allErrs, field.Duplicate(idxPath.Child("name"), endpointPort.Name))
		} else {
			portNames = append(portNames, *endpointPort.Name)
		}

		// 协议必填，且只能是 TCP、UDP 或 SCTP。
		if endpointPort.Protocol == nil {
			allErrs = append(allErrs, field.Required(idxPath.Child("protocol"), ""))
		} else if !supportedPortProtocols.Has(*endpointPort.Protocol) {
			allErrs = append(allErrs, field.NotSupported(idxPath.Child("protocol"), *endpointPort.Protocol, sets.List(supportedPortProtocols)))
		}

		// appProtocol 可选，用于标记应用层协议（如 kubernetes.io/h2c），须是合法限定名。
		if endpointPort.AppProtocol != nil {
			allErrs = append(allErrs, apivalidation.ValidateQualifiedName(*endpointPort.AppProtocol, idxPath.Child("appProtocol"))...)
		}
	}

	return allErrs
}

// validateAddressType 校验地址类型是否已填写且属于受支持的集合。
// MarkCoveredByDeclarative 表示这条规则已由声明式校验覆盖。
func validateAddressType(addressType discovery.AddressType) field.ErrorList {
	allErrs := field.ErrorList{}

	// 空值视为漏填；非空但不在集合内时列出所有合法值，便于用户修正。
	if addressType == "" {
		allErrs = append(allErrs, field.Required(field.NewPath("addressType"), "").MarkCoveredByDeclarative())
	} else if !supportedAddressTypes.Has(addressType) {
		allErrs = append(allErrs, field.NotSupported(field.NewPath("addressType"), addressType, sets.List(supportedAddressTypes)).MarkCoveredByDeclarative())
	}

	return allErrs
}

// validateHints 校验端点的路由提示：forZones 与 forNodes 的数量上限、
// 名称唯一性，以及名称格式（标签值 / 节点名）。
func validateHints(endpointHints *discovery.EndpointHints, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	fzPath := fldPath.Child("forZones")
	// zone 提示数量超限直接返回。
	if len(endpointHints.ForZones) > maxZoneHints {
		allErrs = append(allErrs, field.TooMany(fzPath, len(endpointHints.ForZones), maxZoneHints))
		return allErrs
	}

	// 与 validatePorts 同理，用切片而非 sets.Set 判重以减少分配；
	// 通常一个端点只有一个 zone 提示。
	zoneNames := make([]string, 0, len(endpointHints.ForZones))
	for i, forZone := range endpointHints.ForZones {
		zonePath := fzPath.Index(i).Child("name")
		if slices.Contains(zoneNames, forZone.Name) {
			allErrs = append(allErrs, field.Duplicate(zonePath, forZone.Name))
		} else {
			zoneNames = append(zoneNames, forZone.Name)
		}

		// zone 名还须是合法的标签值。
		for _, msg := range validation.IsValidLabelValue(forZone.Name) {
			allErrs = append(allErrs, field.Invalid(zonePath, forZone.Name, msg))
		}
	}

	// 节点提示有独立的数量上限。
	fnPath := fldPath.Child("forNodes")
	if len(endpointHints.ForNodes) > maxNodeHints {
		allErrs = append(allErrs, field.TooMany(fnPath, len(endpointHints.ForNodes), maxNodeHints))
		return allErrs
	}

	nodeNames := make([]string, 0, len(endpointHints.ForNodes))
	for i, forNode := range endpointHints.ForNodes {
		nodePath := fnPath.Index(i).Child("name")
		if slices.Contains(nodeNames, forNode.Name) {
			allErrs = append(allErrs, field.Duplicate(nodePath, forNode.Name))
		} else {
			nodeNames = append(nodeNames, forNode.Name)
		}

		// 节点名规则与 endpoint.nodeName 保持一致。
		for _, msg := range apivalidation.ValidateNodeName(forNode.Name, false) {
			allErrs = append(allErrs, field.Invalid(nodePath, forNode.Name, msg))
		}
	}

	return allErrs
}
