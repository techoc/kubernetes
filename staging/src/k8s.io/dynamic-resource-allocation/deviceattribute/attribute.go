/*
Copyright 2025 The Kubernetes Authors.

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

package deviceattribute

import (
	resourceapi "k8s.io/api/resource/v1"
)

const (
	// StandardDeviceAttributePrefix 是所有标准设备属性名共用的命名空间前缀。
	StandardDeviceAttributePrefix = "resource.kubernetes.io/"

	// StandardDeviceAttributePCIeRoot 描述设备所属的 PCIe 根复合体，值为 pci<domain>:<bus> 形式的字符串，
	// 可用于将共享同一根复合体的设备归为一组。
	StandardDeviceAttributePCIeRoot resourceapi.QualifiedName = StandardDeviceAttributePrefix + "pcieRoot"

	// StandardDeviceAttributePCIBusID 描述 PCI 设备的总线地址，值为扩展 BDF（域:总线:设备.功能）格式的字符串，
	// 可用于唯一标识一个 PCI 设备。
	StandardDeviceAttributePCIBusID resourceapi.QualifiedName = StandardDeviceAttributePrefix + "pciBusID"

	// StandardDeviceAttributeNUMANode 描述设备的 NUMA 拓扑：标量形式记录物理 NUMA 节点（来自内核 numa_node sysfs 项，
	// 无需特性门控），列表形式（需 DRAListTypeAttributes 门控）以物理节点开头、后接同插槽的最小 SLIT 距离节点。
	// 无 NUMA 亲和性的设备（numa_node = -1）不得发布该属性。
	StandardDeviceAttributeNUMANode resourceapi.QualifiedName = StandardDeviceAttributePrefix + "numaNode"
)

// DeviceAttribute 把一个设备属性的限定名与其取值配对，作为 DRA 驱动发布属性的基本单位。
type DeviceAttribute struct {
	// Name 是该属性的限定名（含域名前缀）。
	Name resourceapi.QualifiedName
	// Value 是该属性的具体取值，可为字符串、整数或整数列表等。
	Value resourceapi.DeviceAttribute
}
