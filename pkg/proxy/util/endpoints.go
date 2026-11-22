/*
Copyright 2017 The Kubernetes Authors.

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
	"net"

	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"
)

// IPPart 从 IP、IP:port 或 endpoint 字符串中只提取 IP 部分。
// 如果输入是方括号包裹的 IPv6 地址加端口（如 "[fd00:1::5]:9999"），方括号也会被去掉。
func IPPart(s string) string {
	if ip := netutils.ParseIPSloppy(s); ip != nil {
		// 整个字符串能被解析为 IP，说明它本来就不含端口，原样返回即可。
		return s
	}
	// 否则按 IP:port 形式拆分。
	host, _, err := net.SplitHostPort(s)
	if err != nil {
		klog.ErrorS(err, "Failed to parse host-port", "input", s)
		return ""
	}
	// SplitHostPort 只做字符串拆分，还需确认 host 确实是合法 IP，避免把主机名当成地址返回。
	ip := netutils.ParseIPSloppy(host)
	if ip == nil {
		klog.ErrorS(nil, "Failed to parse IP", "input", host)
		return ""
	}
	return ip.String()
}
