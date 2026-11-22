/*
Copyright 2016 The Kubernetes Authors.

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

package server

import (
	"net"
	"testing"

	netutils "k8s.io/utils/net"
)

// TestLoopbackHostPortIPv4 验证当监听地址是 0.0.0.0 时，自客户端应改用
// 回环地址而不是 0.0.0.0；显式给出的具体 IP 则保持不变。
func TestLoopbackHostPortIPv4(t *testing.T) {
	_, ipv6only, err := isIPv6LoopbackSupported()
	if err != nil {
		t.Fatalf("fail to enumerate network interface, %s", err)
	}
	if ipv6only {
		t.Fatalf("no ipv4 loopback interface")
	}

	host, port, err := LoopbackHostPort("1.2.3.4:443")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if host != "1.2.3.4" {
		t.Fatalf("expected 1.2.3.4 as host, got %q", host)
	}
	if port != "443" {
		t.Fatalf("expected 443 as port, got %q", port)
	}

	host, port, err = LoopbackHostPort("0.0.0.0:443")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ip := netutils.ParseIPSloppy(host); ip == nil || !ip.IsLoopback() {
		t.Fatalf("expected host to be loopback, got %q", host)
	}
	if port != "443" {
		t.Fatalf("expected 443 as port, got %q", port)
	}
}

// TestLoopbackHostPortIPv6 是 IPv4 用例的 IPv6 版本；主机未提供 IPv6 回环
// 接口时跳过，避免在受限环境里误报。
func TestLoopbackHostPortIPv6(t *testing.T) {
	ipv6, _, err := isIPv6LoopbackSupported()
	if err != nil {
		t.Fatalf("fail to enumerate network interface, %s", err)
	}
	if !ipv6 {
		t.Skip("no ipv6 loopback interface")
	}

	host, port, err := LoopbackHostPort("[ff06:0:0:0:0:0:0:c3]:443")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if host != "ff06:0:0:0:0:0:0:c3" {
		t.Fatalf("expected ff06:0:0:0:0:0:0:c3 as host, got %q", host)
	}
	if port != "443" {
		t.Fatalf("expected 443 as port, got %q", port)
	}

	host, port, err = LoopbackHostPort("[::]:443")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ip := netutils.ParseIPSloppy(host); ip == nil || !ip.IsLoopback() || ip.To4() != nil {
		t.Fatalf("expected IPv6 host to be loopback, got %q", host)
	}
	if port != "443" {
		t.Fatalf("expected 443 as port, got %q", port)
	}
}

// isIPv6LoopbackSupported 探测本机是否存在回环地址，并区分“只有 IPv6 回环”
// 与“同时有 IPv4 回环”两种情况，供测试决定是否执行/跳过。
func isIPv6LoopbackSupported() (ipv6 bool, ipv6only bool, err error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return false, false, err
	}
	ipv4 := false
	for _, address := range addrs {
		ipnet, ok := address.(*net.IPNet)
		if !ok || !ipnet.IP.IsLoopback() {
			continue
		}
		if netutils.IsIPv6(ipnet.IP) {
			ipv6 = true
			continue
		}
		ipv4 = true
	}
	ipv6only = ipv6 && !ipv4
	return ipv6, ipv6only, nil
}
