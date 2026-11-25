/*
Copyright The Kubernetes Authors.

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
	"fmt"
	"io/fs"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/utils/cpuset"
)

// AttributeForm 决定 numaNode 属性以标量 IntValue 还是列表 IntValues 形式发布。
type AttributeForm int

const (
	// ScalarAttribute 生成仅含物理 NUMA 节点的标量 IntValue，不依赖任何特性门控。
	ScalarAttribute AttributeForm = iota
	// ListAttribute 生成 IntValues 列表：首元素为物理 NUMA 节点，其余为最小 ACPI SLIT 距离的同插槽节点；
	// 距离信息不可用时退化为单元素列表。使用该形式要求集群启用 DRAListTypeAttributes。
	ListAttribute
)

// maxNUMANodes 是合法 NUMA 节点 ID 的上界。Linux 在 include/linux/nodemask_types.h 中
// 定义 MAX_NUMNODES = (1 << CONFIG_NODES_SHIFT)（截至 Linux v7.2）；各架构的
// CONFIG_NODES_SHIFT 最大为 10，故上界为 1024。
const maxNUMANodes = 1024

// GetNUMANodeAttributeByPCIBusID 根据 PCI 设备的 BDF 地址从 sysfs 的 numa_node 读取物理 NUMA 节点，
// 并按 attrForm 生成标量或列表形式的 numaNode 属性。attrForm 必须反映集群 DRAListTypeAttributes 状态。
// 特性门控按进程生效，驱动无法探测；由运维在驱动上配置，并与 API Server、调度器的设置保持一致。
// 本函数只按传入形式渲染，不做探测。若集群关闭门控时使用 ListAttribute，API Server 会丢弃列表值，
// 并拒绝由此产生的无值属性；驱动必须把该发布失败视为致命错误，而不是自动适配。
func GetNUMANodeAttributeByPCIBusID(pciBusID string, attrForm AttributeForm, mods ...MachineModifier) (DeviceAttribute, error) {
	// 先初始化默认 sysfs（/sys），再依次应用调用方传入的选项，便于测试替换文件系统。
	var mc machine
	initDefaultMachine(&mc)
	for _, mod := range mods {
		mod(&mc)
	}

	// 校验 BDF 格式，非法地址直接返回，避免拼出错误的 sysfs 路径。
	if err := verifyPCIBDFFormat(pciBusID); err != nil {
		return DeviceAttribute{}, err
	}

	// 从 bus/pci/devices/<bdf>/numa_node 读取物理 NUMA 节点。
	physicalNode, err := readNUMANode(mc, pciBusID)
	if err != nil {
		return DeviceAttribute{}, err
	}

	// numa_node = -1 表示无 NUMA 亲和性：此类设备不得发布该属性，否则 -1 会在属性匹配时
	// 与所有其他无亲和性设备误匹配；返回错误使驱动选择省略该属性。
	if physicalNode < 0 {
		return DeviceAttribute{}, fmt.Errorf("PCI device %s has no NUMA affinity (numa_node=%d); do not publish the numaNode attribute for it", pciBusID, physicalNode)
	}

	// 按指定形式生成属性：列表形式会继续读取 SLIT 距离信息。
	if attrForm == ListAttribute {
		return makeNUMANodeListAttribute(mc, physicalNode), nil
	}
	return makeNUMANodeScalarAttribute(physicalNode), nil
}

// GetNUMANodeAttribute 为已知 NUMA 节点号的设备（如 CPU、内存设备）生成 numaNode 属性；
// attrForm 的含义同 GetNUMANodeAttributeByPCIBusID。节点号必须非负且小于 maxNUMANodes，
// 负值表示无 NUMA 亲和性，返回错误以避免错误发布。
func GetNUMANodeAttribute(numaNode int, attrForm AttributeForm, mods ...MachineModifier) (DeviceAttribute, error) {
	// 负数节点表示无 NUMA 亲和性，拒绝发布。
	if numaNode < 0 {
		return DeviceAttribute{}, fmt.Errorf("invalid NUMA node %d: do not publish the numaNode attribute for a device with no NUMA affinity", numaNode)
	}
	// 超出内核支持上限的节点号视为非法。
	if numaNode >= maxNUMANodes {
		return DeviceAttribute{}, fmt.Errorf("invalid NUMA node %d: exceeds maximum supported (%d)", numaNode, maxNUMANodes-1)
	}

	// 标量形式无需读取 sysfs，直接构造属性。
	if attrForm == ScalarAttribute {
		return makeNUMANodeScalarAttribute(numaNode), nil
	}

	// 列表形式需要 sysfs 中的 SLIT 距离信息，因此初始化文件系统并应用选项。
	var mc machine
	initDefaultMachine(&mc)
	for _, mod := range mods {
		mod(&mc)
	}

	return makeNUMANodeListAttribute(mc, numaNode), nil
}

// makeNUMANodeScalarAttribute 生成仅含物理 NUMA 节点的标量形式 numaNode 属性。
func makeNUMANodeScalarAttribute(numaNode int) DeviceAttribute {
	// 把 int 转为 API 所需的 int64，并取其地址填入 IntValue。
	value := int64(numaNode)
	return DeviceAttribute{
		Name:  StandardDeviceAttributeNUMANode,
		Value: resourceapi.DeviceAttribute{IntValue: &value},
	}
}

// GetNUMANodeForCPU 通过匹配 /sys/devices/system/cpu/cpuX/nodeY 符号链接，返回指定 CPU 所属的 NUMA 节点号；
// 找不到或节点号解析失败时返回 -1 与错误。
func GetNUMANodeForCPU(cpuID int, mods ...MachineModifier) (int, error) {
	// 初始化 sysfs 并应用可选的文件系统替换（主要用于测试）。
	var mc machine
	initDefaultMachine(&mc)
	for _, mod := range mods {
		mod(&mc)
	}

	// 用通配符匹配 nodeY 链接；fs.Glob 只会返回实际存在的 node 目录。
	pattern := filepath.Join("devices", "system", "cpu", fmt.Sprintf("cpu%d", cpuID), "node[0-9]*")
	matches, err := fs.Glob(mc.sysfs, pattern)
	if err != nil {
		return -1, fmt.Errorf("failed to find NUMA node for CPU %d: %w", cpuID, err)
	}
	// 没有任何匹配说明该 CPU 不属于任何 NUMA 节点（或 CPU 号不存在）。
	if len(matches) == 0 {
		return -1, fmt.Errorf("CPU %d not found in any NUMA node", cpuID)
	}

	// 取首个匹配的末段目录名（如 node3），去掉 node 前缀后解析为整数。
	nodeDir := filepath.Base(matches[0])
	nodeNum, err := strconv.Atoi(strings.TrimPrefix(nodeDir, "node"))
	if err != nil {
		return -1, fmt.Errorf("failed to parse NUMA node number from %s: %w", nodeDir, err)
	}
	return nodeNum, nil
}

// makeNUMANodeListAttribute 生成列表形式的 numaNode 属性：物理节点始终位于首位，其后为同插槽等距节点；
// 读取 SLIT 距离失败或没有对等节点时退化为仅含物理节点的单元素列表。调用方须保证 physicalNode 非负。
func makeNUMANodeListAttribute(mc machine, physicalNode int) DeviceAttribute {
	equidistant, err := getEquidistantNUMANodes(mc, physicalNode)
	// 距离信息不可用或只有物理节点自身时，退化为单元素列表。
	if err != nil || len(equidistant) <= 1 {
		return DeviceAttribute{
			Name:  StandardDeviceAttributeNUMANode,
			Value: resourceapi.DeviceAttribute{IntValues: []int64{int64(physicalNode)}},
		}
	}

	// 该函数结果总包含物理节点自身；这里先放入物理节点，再跳过它追加其余节点，保证物理节点排第一。
	result := []int64{int64(physicalNode)}
	for _, n := range equidistant {
		if n != physicalNode {
			result = append(result, int64(n))
		}
	}

	return DeviceAttribute{
		Name:  StandardDeviceAttributeNUMANode,
		Value: resourceapi.DeviceAttribute{IntValues: result},
	}
}

// readSysfsInt 读取指定 sysfs 路径下的整数：先读文件、去除首尾空白，再解析为 int。
func readSysfsInt(mc machine, path string) (int, error) {
	data, err := fs.ReadFile(mc.sysfs, path)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(strings.TrimSpace(string(data)))
}

// readNUMANode 从 bus/pci/devices/<bdf>/numa_node 读取 PCI 设备的物理 NUMA 节点号。
func readNUMANode(mc machine, pciBusID string) (int, error) {
	numaNodePath := filepath.Join("bus", "pci", "devices", pciBusID, "numa_node")
	node, err := readSysfsInt(mc, numaNodePath)
	if err != nil {
		return -1, fmt.Errorf("failed to read NUMA node for PCI Bus ID %s: %w", pciBusID, err)
	}
	return node, nil
}

// getEquidistantNUMANodes 读取指定节点的 SLIT 距离行，返回与最小非自身距离相等、且同插槽的所有节点，
// 结果中始终包含节点自身。若节点跨插槽信息无法确定，则采用包含候选节点的保守策略。
func getEquidistantNUMANodes(mc machine, node int) ([]int, error) {
	// 拒绝负数节点号。
	if node < 0 {
		return nil, fmt.Errorf("invalid NUMA node %d", node)
	}

	// 距离信息位于 devices/system/node/nodeX/distance，每行以空白分隔。
	distPath := filepath.Join("devices", "system", "node", fmt.Sprintf("node%d", node), "distance")
	data, err := fs.ReadFile(mc.sysfs, distPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read SLIT distances for NUMA node %d: %w", node, err)
	}

	fields := strings.Fields(strings.TrimSpace(string(data)))
	if len(fields) == 0 {
		return nil, fmt.Errorf("empty distance file for NUMA node %d", node)
	}

	// 节点号必须落在距离表范围内，否则按索引取值会越界。
	if node >= len(fields) {
		return nil, fmt.Errorf("NUMA node %d out of range for distance table with %d entries", node, len(fields))
	}

	// 逐项把距离字符串解析为整数数组。
	distances := make([]int, len(fields))
	for i, f := range fields {
		d, err := strconv.Atoi(f)
		if err != nil {
			return nil, fmt.Errorf("failed to parse distance value %q for NUMA node %d: %w", f, node, err)
		}
		distances[i] = d
	}

	// 求除自身以外的最小距离，作为“同插槽/等距”的判定阈值。
	minDist := -1
	for i, d := range distances {
		if i == node {
			continue
		}
		if minDist == -1 || d < minDist {
			minDist = d
		}
	}

	// 只有一个节点时没有对等节点，直接返回自身。
	if minDist == -1 {
		return []int{node}, nil
	}

	// 获取本节点所属插槽，用于过滤跨插槽的候选节点。
	reportedSocket, haveSocket := getSocketForNUMANode(mc, node)

	var nodes []int
	for i, d := range distances {
		// 节点自身总是保留在结果中。
		if i == node {
			nodes = append(nodes, i)
			continue
		}
		// 只考虑距离等于最小值的候选节点。
		if d != minDist {
			continue
		}
		if haveSocket {
			// 仅当候选节点插槽已知且与本节点不同才剔除；若无法确定其插槽（例如没有 cpulist 的无 CPU 内存节点），
			// 则予以保留。这是有意的过近似：宁可多收入可能的同插槽节点，也不误删真正的同插槽节点。
			candidateSocket, ok := getSocketForNUMANode(mc, i)
			if ok && candidateSocket != reportedSocket {
				continue
			}
		}
		nodes = append(nodes, i)
	}

	// 排序以保证输出顺序稳定、便于比较。
	slices.Sort(nodes)
	return nodes, nil
}

// getSocketForNUMANode 推断 NUMA 节点所属的 CPU 插槽（物理 package）ID。
// 它通过 nodeX/cpulist 找到该节点上的一个 CPU，再读取对应 CPU 的 physical_package_id；
// 任一环节失败或该节点无 CPU 时返回 (..., false)。
func getSocketForNUMANode(mc machine, node int) (int, bool) {
	cpulistPath := filepath.Join("devices", "system", "node", fmt.Sprintf("node%d", node), "cpulist")
	data, err := fs.ReadFile(mc.sysfs, cpulistPath)
	if err != nil {
		return -1, false
	}

	// 解析 cpulist（如 "0-3,8"）为 CPU 集合；空集合表示无 CPU，无法确定插槽。
	cpus, err := cpuset.Parse(strings.TrimSpace(string(data)))
	if err != nil || cpus.Size() == 0 {
		return -1, false
	}

	// 取集合中任意一个 CPU，读取其拓扑中的 physical_package_id 作为插槽号。
	pkgPath := filepath.Join("devices", "system", "cpu", fmt.Sprintf("cpu%d", cpus.UnsortedList()[0]), "topology", "physical_package_id")
	socketID, err := readSysfsInt(mc, pkgPath)
	if err != nil {
		return -1, false
	}

	return socketID, true
}
