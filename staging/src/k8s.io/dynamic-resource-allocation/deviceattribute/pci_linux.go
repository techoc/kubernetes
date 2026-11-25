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
	"fmt"
	"io/fs"
	"path/filepath"
	"regexp"
	"strings"

	resourceapi "k8s.io/api/resource/v1"
)

// GetPCIeRootAttributeByPCIBusID 根据 BDF 格式的 PCI 总线地址解析其所属的 PCIe 根复合体，
// 并生成字符串形式的 pcieRoot 属性；地址非法或无法解析符号链接时返回错误。
// BDF 记法参见 https://wiki.xenproject.org/wiki/Bus:Device.Function_(BDF)_Notation。
func GetPCIeRootAttributeByPCIBusID(pciBusID string, mods ...MachineModifier) (DeviceAttribute, error) {
	// 初始化默认 sysfs 并应用调用方选项（主要用于测试）。
	var mc machine
	initDefaultMachine(&mc)
	for _, mod := range mods {
		mod(&mc)
	}

	// 先校验 BDF 格式。
	if err := verifyPCIBDFFormat(pciBusID); err != nil {
		return DeviceAttribute{}, err
	}

	// 解析符号链接得到根复合体名（如 pci0000:00）。
	pcieRoot, err := resolvePCIeRoot(mc, pciBusID)
	if err != nil {
		return DeviceAttribute{}, fmt.Errorf("failed to resolve PCIe Root Complex for PCI Bus ID %s: %w", pciBusID, err)
	}

	// 把结果包装为字符串属性。
	attr := DeviceAttribute{
		Name:  StandardDeviceAttributePCIeRoot,
		Value: resourceapi.DeviceAttribute{StringValue: &pcieRoot},
	}

	return attr, nil
}

// resolvePCIeRoot 通过读取 /sys/bus/pci/devices/<bdf> 符号链接来解析 PCIe 根复合体。
// BDF 记法参见 https://wiki.xenproject.org/wiki/Bus:Device.Function_(BDF)_Notation。
// 由于 /sys/devices 的层级中包含中间 PCI 设备目录，无法直接由 BDF 推出路径；符号链接目标形如
// devices/pci0000:00/.../0000:04:1f.0，其中第一段 pci0000:00 即为根复合体。
func resolvePCIeRoot(mc machine, pciBusID string) (string, error) {
	sysBusPath := filepath.Join("bus", "pci", "devices", pciBusID)

	// 读取符号链接目标（相对或绝对路径）。
	target, err := fs.ReadLink(mc.sysfs, sysBusPath)
	if err != nil {
		return "", fmt.Errorf("failed to read symlink for PCI Bus ID %s: %w", sysBusPath, err)
	}

	// 相对路径需按符号链接所在目录解析并规范化。
	if !filepath.IsAbs(target) {
		target = filepath.Clean(filepath.Join(filepath.Dir(sysBusPath), target))
	}

	// 校验解析后的路径以 devices/pci 开头，且末段正是该 BDF，防止拿到异常链接。
	devicePathPrefix := filepath.Join("devices", "pci")
	if !strings.HasPrefix(target, devicePathPrefix) {
		return "", fmt.Errorf("symlink target for PCI Bus ID %s is invalid: it must start with %s: %s", pciBusID, devicePathPrefix, target)
	}
	if filepath.Base(target) != pciBusID {
		return "", fmt.Errorf("symlink target for PCI Bus ID %s is invalid: it must end with %s: %s", pciBusID, pciBusID, target)
	}

	// 去掉 devices/ 前缀后按分隔符拆分，第一段即为 PCIe 根复合体。
	target = strings.TrimPrefix(target, "devices"+string(filepath.Separator))
	pcieRootParts := strings.Split(target, string(filepath.Separator))
	return pcieRootParts[0], nil
}

// GetPCIBusIDAttribute 把合法的 BDF 字符串包装为 pciBusID 字符串属性，不读取 sysfs；地址非法时返回错误。
// BDF 记法参见 https://wiki.xenproject.org/wiki/Bus:Device.Function_(BDF)_Notation。
func GetPCIBusIDAttribute(pciBusID string) (DeviceAttribute, error) {
	// 仅校验 BDF 格式。
	if err := verifyPCIBDFFormat(pciBusID); err != nil {
		return DeviceAttribute{}, err
	}

	// 把地址本身作为字符串属性值。
	attr := DeviceAttribute{
		Name:  StandardDeviceAttributePCIBusID,
		Value: resourceapi.DeviceAttribute{StringValue: &pciBusID},
	}

	return attr, nil
}

// verifyPCIBDFFormat 校验 PCI 总线地址是否为扩展 BDF 格式（域4位:总线2位:设备2位.功能1位，均为小写十六进制）；
// 空字符串或格式不符均返回错误。
// BDF 记法参见 https://wiki.xenproject.org/wiki/Bus:Device.Function_(BDF)_Notation。
func verifyPCIBDFFormat(pciBusID string) error {
	// 空地址直接拒绝。
	if pciBusID == "" {
		return fmt.Errorf("PCI Bus ID cannot be empty")
	}

	// 正则严格匹配完整字符串，避免接受多余字符。
	bdfRegexp := regexp.MustCompile(`^([0-9a-f]{4}):([0-9a-f]{2}):([0-9a-f]{2})\.([0-9a-f]{1})$`)
	if !bdfRegexp.MatchString(pciBusID) {
		return fmt.Errorf("invalid PCI Bus ID format: %s", pciBusID)
	}

	return nil
}
