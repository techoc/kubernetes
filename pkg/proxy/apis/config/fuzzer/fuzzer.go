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

package fuzzer

import (
	"fmt"
	"net/netip"
	"time"

	"sigs.k8s.io/randfill"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	kubeproxyconfig "k8s.io/kubernetes/pkg/proxy/apis/config"
	"k8s.io/utils/ptr"
)

// generateRandomIP 生成随机 IP 字符串，实现复制自 pkg/apis/networking/fuzzer/fuzzer.go。
func generateRandomIP(is6 bool, c randfill.Continue) string {
	n := 4
	if is6 {
		n = 16
	}
	bytes := make([]byte, n)
	for i := 0; i < n; i++ {
		bytes[i] = uint8(c.Rand.Intn(255))
	}

	ip, ok := netip.AddrFromSlice(bytes)
	if ok {
		return ip.String()
	}
	// 由合法字节构造的地址必然有效，走到这里说明出现了意外情况。
	panic(fmt.Sprintf("invalid IP %v", bytes))
}

// generateRandomCIDR 生成随机 CIDR 字符串，实现复制自 pkg/apis/networking/fuzzer/fuzzer.go。
func generateRandomCIDR(is6 bool, c randfill.Continue) string {
	ip, err := netip.ParseAddr(generateRandomIP(is6, c))
	if err != nil {
		// generateRandomIP 在生成非法 IP 时会直接 panic，这里只是防御性处理。
		panic(err)
	}

	n := 32
	if is6 {
		n = 128
	}

	// 掩码长度取 [0, n) 区间；PrefixFrom/Masked 会规范化为网络地址。
	bits := c.Rand.Intn(n)
	prefix := netip.PrefixFrom(ip, bits)
	return prefix.Masked().String()
}

// getRandomDualStackCIDR 返回随机的双栈 CIDR 列表：IPv4/IPv6 顺序随机，数量为一个或两个。
func getRandomDualStackCIDR(c randfill.Continue) []string {
	cidrIPv4 := generateRandomCIDR(false, c)
	cidrIPv6 := generateRandomCIDR(true, c)

	cidrs := []string{cidrIPv4, cidrIPv6}
	// 随机打乱两个地址族的顺序，覆盖不同的配置排列。
	if c.Bool() {
		cidrs = []string{cidrIPv6, cidrIPv4}
	}
	return cidrs[:1+c.Intn(2)]
}

// Funcs 返回 kube-proxy API 的 fuzzer 函数，用于 round-trip 测试中的随机对象生成。
func Funcs(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		func(obj *kubeproxyconfig.KubeProxyConfiguration, c randfill.Continue) {
			// 先随机填充通用字段，再针对需要合法格式或特定语义的字段单独赋值。
			c.FillNoCustom(obj)
			obj.BindAddress = fmt.Sprintf("%d.%d.%d.%d", c.Intn(256), c.Intn(256), c.Intn(256), c.Intn(256))
			obj.ClientConnection.ContentType = c.String(0)
			obj.DetectLocal.ClusterCIDRs = getRandomDualStackCIDR(c)
			obj.Linux.Conntrack.MaxPerCore = ptr.To(c.Int31())
			obj.Linux.Conntrack.Min = ptr.To(c.Int31())
			obj.Linux.Conntrack.TCPCloseWaitTimeout = &metav1.Duration{Duration: time.Duration(c.Int63()) * time.Hour}
			obj.Linux.Conntrack.TCPEstablishedTimeout = &metav1.Duration{Duration: time.Duration(c.Int63()) * time.Hour}
			obj.FeatureGates = map[string]bool{c.String(0): true}
			obj.HealthzBindAddress = fmt.Sprintf("%d.%d.%d.%d:%d", c.Intn(256), c.Intn(256), c.Intn(256), c.Intn(256), c.Intn(65536))
			obj.IPTables.MasqueradeBit = ptr.To(c.Int31())
			obj.IPTables.LocalhostNodePorts = ptr.To(c.Bool())
			obj.NFTables.MasqueradeBit = ptr.To(c.Int31())
			obj.MetricsBindAddress = fmt.Sprintf("%d.%d.%d.%d:%d", c.Intn(256), c.Intn(256), c.Intn(256), c.Intn(256), c.Intn(65536))
			obj.Linux.OOMScoreAdj = ptr.To(c.Int31())
			obj.ClientConnection.ContentType = "bar"
			obj.NodePortAddresses = []string{"1.2.3.0/24"}
			// 日志格式为空会导致校验失败，需补一个合法值。
			if obj.Logging.Format == "" {
				obj.Logging.Format = "text"
			}
		},
	}
}
