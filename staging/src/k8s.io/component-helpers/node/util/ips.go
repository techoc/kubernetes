/*
Copyright 2023 The Kubernetes Authors.

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

package util

import (
	"fmt"
	"net"
	"strings"

	netutils "k8s.io/utils/net"
)

// 云提供方是否支持双栈，取决于配置：留空表示未指定，"external" 表示外部云提供方，
// 两者都允许双栈；其余取值被视为不兼容，禁止双栈以免给云提供方分配它处理不了的地址。
const (
	cloudProviderNone     = ""
	cloudProviderExternal = "external"
)

// parseNodeIP 收拢参数与注解两种来源的解析逻辑：两者在"允许双栈"和
// "遇错即停还是容忍"上不同，用布尔参数而非复制两份代码来表达这种差异。
// 返回无效项是为了让宽松模式（命令行参数）的调用方能自行决定如何处理。
// parseNodeIP 是 ParseNodeIPArgument 与 ParseNodeIPAnnotation 的公共实现。
func parseNodeIP(nodeIP string, allowDual, sloppy bool) ([]net.IP, []string, error) {
	// nodeIPs 收集解析成功的地址；invalidIPs 收集坏片段，供宽松模式的调用方处理。
	var nodeIPs []net.IP
	var invalidIPs []string
	// 宽松模式下空串意味着"没有配置"，直接返回空列表；
	// 严格模式下注解为空属于错误输入，仍要走下面的解析以返回失败。
	if nodeIP != "" || !sloppy {
		for _, ip := range strings.Split(nodeIP, ",") {
			if sloppy {
				ip = strings.TrimSpace(ip)
			}
			// 用宽松解析器容忍 IPv4 的前导零等历史写法，
			// 与 kubelet 对节点 IP 的一贯处理方式保持一致。
			parsedNodeIP := netutils.ParseIPSloppy(ip)
			if parsedNodeIP == nil {
				invalidIPs = append(invalidIPs, ip)
				if !sloppy {
					return nil, invalidIPs, fmt.Errorf("could not parse %q", ip)
				}
			} else {
				nodeIPs = append(nodeIPs, parsedNodeIP)
			}
		}
	}

	// 节点 IP 只接受单个地址或一个 IPv4/IPv6 组合；
	// 同为 IPv4 或同为 IPv6 的两个地址不构成双栈，属于配置错误。
	if len(nodeIPs) > 2 || (len(nodeIPs) == 2 && netutils.IsIPv6(nodeIPs[0]) == netutils.IsIPv6(nodeIPs[1])) {
		return nil, invalidIPs, fmt.Errorf("must contain either a single IP or a dual-stack pair of IPs")
	} else if len(nodeIPs) == 2 && !allowDual {
		return nil, invalidIPs, fmt.Errorf("dual-stack not supported in this configuration")
	} else if len(nodeIPs) == 2 && (nodeIPs[0].IsUnspecified() || nodeIPs[1].IsUnspecified()) {
		return nil, invalidIPs, fmt.Errorf("dual-stack node IP cannot include '0.0.0.0' or '::'")
	}

	return nodeIPs, invalidIPs, nil
}

// ParseNodeIPArgument 解析 kubelet 的 --node-ip 参数。
// 命令行参数历史上允许夹带坏值，所以走宽松模式并把无效项单独返回，
// 让调用方决定是忽略还是上报；只有结构性问题（如超过双栈）才直接判错。
// 即使返回错误，无效项也会一并返回；cloudProvider 未设置或为 "external" 时允许双栈。
func ParseNodeIPArgument(nodeIP, cloudProvider string) ([]net.IP, []string, error) {
	// 未指定或使用外部云提供方时才允许双栈；内置 provider 未必能处理双栈地址分配。
	var allowDualStack bool
	if cloudProvider == cloudProviderNone || cloudProvider == cloudProviderExternal {
		allowDualStack = true
	}
	return parseNodeIP(nodeIP, allowDualStack, true)
}

// ParseNodeIPAnnotation 解析 `alpha.kubernetes.io/provided-node-ip` 注解。
// 该注解可以是单个 IP，也可以是逗号分隔的一对 IP。
// 注解由云提供方写入，是内部契约而非用户输入，坏值应当直接失败，
// 所以走严格模式：不返回无效项列表，有问题就报错，这与 ParseNodeIPArgument 的宽容策略相反。
func ParseNodeIPAnnotation(nodeIP string) ([]net.IP, error) {
	nodeIps, _, err := parseNodeIP(nodeIP, true, false)
	return nodeIps, err
}
