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

package config

// NodeIPAMControllerConfiguration 描述 NodeIPAMController（节点 IP 地址管理控制器）的配置参数。
type NodeIPAMControllerConfiguration struct {
	// serviceCIDR 是集群中 Service 使用的 CIDR 地址段。
	ServiceCIDR string
	// secondaryServiceCIDR 是双栈集群中 Service 使用的第二个 CIDR 地址段；
	// 其 IP 地址族必须与 ServiceCIDR 不同。
	SecondaryServiceCIDR string
	// NodeCIDRMaskSize 是单栈集群中节点 CIDR 的掩码大小。
	// 仅可用于单栈集群，与双栈集群不兼容。
	NodeCIDRMaskSize int32
	// NodeCIDRMaskSizeIPv4 是双栈集群中 IPv4 节点 CIDR 的掩码大小。
	// 仅可用于双栈集群，与单栈集群不兼容。
	NodeCIDRMaskSizeIPv4 int32
	// NodeCIDRMaskSizeIPv6 是双栈集群中 IPv6 节点 CIDR 的掩码大小。
	// 仅可用于双栈集群，与单栈集群不兼容。
	NodeCIDRMaskSizeIPv6 int32
}
