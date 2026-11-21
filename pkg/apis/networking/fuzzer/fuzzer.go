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

package fuzzer

import (
	"fmt"
	"net/netip"

	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/kubernetes/pkg/apis/networking"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/randfill"
)

// Funcs 返回 networking API 组使用的 fuzzer 函数集合。
// 这些自定义函数把随机生成的数据收敛到校验器可接受的取值范围内，
// 这样模糊测试才能覆盖真实的 API 对象而不是在校验阶段被大量拒绝。
var Funcs = func(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		func(np *networking.NetworkPolicyPeer, c randfill.Continue) {
			c.FillNoCustom(np) // 填充除本函数外的其它字段，避免递归调用自身
			// TODO: 实现能够为 selector requirement 生成合法 key、value 和 operator 的 fuzzer。
			// IPBlock 里的 CIDR/Except 必须能被 net.ParseCIDR 解析，随机字符串无法满足，
			// 因此直接固定为一组格式合法的值。
			if np.IPBlock != nil {
				np.IPBlock = &networking.IPBlock{
					CIDR:   "192.168.1.0/24",
					Except: []string{"192.168.1.1/24", "192.168.1.2/24"},
				}
			}
		},
		func(np *networking.NetworkPolicy, c randfill.Continue) {
			c.FillNoCustom(np) // 填充除本函数外的其它字段，避免递归调用自身
			// TODO: 实现能够为 selector requirement 生成合法 key、value 和 operator 的 fuzzer。
			// 校验器要求 policyTypes 至少有一种类型，空切片会让对象直接被判非法。
			if len(np.Spec.PolicyTypes) == 0 {
				np.Spec.PolicyTypes = []networking.PolicyType{networking.PolicyTypeIngress}
			}
		},
		func(path *networking.HTTPIngressPath, c randfill.Continue) {
			c.FillNoCustom(path) // 填充除本函数外的其它字段，避免递归调用自身
			// PathType 是指针枚举，随机字节几乎不可能命中合法常量，这里从三个枚举值中随机挑选。
			pathTypes := []networking.PathType{networking.PathTypeExact, networking.PathTypePrefix, networking.PathTypeImplementationSpecific}
			path.PathType = &pathTypes[c.Rand.Intn(len(pathTypes))]
		},
		func(p *networking.ServiceBackendPort, c randfill.Continue) {
			c.FillNoCustom(p)
			// Name 与 Number 互斥且必须至少设置一个，因此随机保留一个字段、清空另一个。
			// clear one of the fields
			if c.Bool() {
				p.Name = ""
				if p.Number == 0 {
					p.Number = 1
				}
			} else {
				p.Number = 0
				if p.Name == "" {
					p.Name = "portname"
				}
			}
		},
		func(p *networking.IngressClass, c randfill.Continue) {
			c.FillNoCustom(p) // 填充除本函数外的其它字段，避免递归调用自身
			// 当 Parameters 存在时 Scope 是必填字段，缺省为默认值 Cluster 以保证对象合法。
			// default Parameters to Cluster
			if p.Spec.Parameters == nil || p.Spec.Parameters.Scope == nil {
				p.Spec.Parameters = &networking.IngressClassParametersReference{
					Scope: ptr.To(networking.IngressClassParametersReferenceScopeCluster),
				}
			}
		},
		func(obj *networking.IPAddress, c randfill.Continue) {
			c.FillNoCustom(obj) // 填充除本函数外的其它字段，避免递归调用自身
			// IPAddress 的 name 必须是合法 IP 字符串，随机字节序列无法保证，这里按 4/16 字节生成。
			// length in bytes of the IP Family: IPv4: 4 bytes IPv6: 16 bytes
			boolean := []bool{false, true}
			is6 := boolean[c.Rand.Intn(2)]
			ip := generateRandomIP(is6, c)
			obj.Name = ip
		},
		func(obj *networking.ServiceCIDR, c randfill.Continue) {
			c.FillNoCustom(obj) // 填充除本函数外的其它字段，避免递归调用自身
			boolean := []bool{false, true}

			// 至少生成一个合法 CIDR；随机 IPv4/IPv6 地址族，保证前缀可被正常解析。
			is6 := boolean[c.Rand.Intn(2)]
			primary := generateRandomCIDR(is6, c)
			obj.Spec.CIDRs = []string{primary}

			// 再随机追加一个不同地址族的 CIDR，以便覆盖双栈场景。
			if boolean[c.Rand.Intn(2)] {
				obj.Spec.CIDRs = append(obj.Spec.CIDRs, generateRandomCIDR(!is6, c))
			}
		},
	}
}

// generateRandomIP 按指定地址族生成一个随机且合法的 IP 字符串。
// is6 为 true 时生成 16 字节的 IPv6，否则生成 4 字节的 IPv4。
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
	// 长度只有 4 或 16 字节，AddrFromSlice 理论上必然成功；失败说明输入假设被破坏，直接失败比返回非法值更好。
	// this should not happen
	panic(fmt.Sprintf("invalid IP %v", bytes))
}

// generateRandomCIDR 基于随机 IP 和随机前缀长度生成一个规范化（掩码后）的 CIDR 字符串。
func generateRandomCIDR(is6 bool, c randfill.Continue) string {
	ip, err := netip.ParseAddr(generateRandomIP(is6, c))
	if err != nil {
		// generateRandomIP 对非法结果会直接 panic，因此这里走到 panic 只可能是上游行为异常。
		// generateRandomIP already panics if returns a not valid ip
		panic(err)
	}

	// 前缀长度上限随地址族变化：IPv4 为 32，IPv6 为 128。
	n := 32
	if is6 {
		n = 128
	}

	// Masked 会清掉主机位，确保输出是网络地址形式的合法 CIDR。
	bits := c.Rand.Intn(n)
	prefix := netip.PrefixFrom(ip, bits)
	return prefix.Masked().String()
}
