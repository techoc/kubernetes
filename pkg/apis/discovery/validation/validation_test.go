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
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/apis/discovery"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/utils/ptr"
)

// // TestValidateEndpointSlice 通过表驱动测试覆盖 ValidateEndpointSlice 的通用校验逻辑。
// // 表格的正例覆盖 IPv4/IPv6/FQDN 三种地址类型、全部合法协议、appProtocol 的多种写法、空端口名、
// // 边界长度（63 字符端口名）、空集合以及各数量上限（端点数、端口数、地址数、拓扑键数、hints）。
// // 同时验证特性开关 StrictIPCIDRValidation 的行为：开启时严格拒绝前导零的 IP（legacy-ip-with-strict-validation 报错），
// // 关闭时兼容旧格式（legacy-ip-with-legacy-validation 通过）。
// // 反例逐个对应一条校验规则：端口名重复/大小写/非法字符/超长、非法协议、数量超限、
// // 空地址、非法拓扑键、-INVALID 主机名、非法元数据名、非法 IP（含多地址用例累加错误数）、
// // 非法 FQDN（大小写/下划线/通配符）、非法 appProtocol、非法/重复/过多 zone 与 node hints、
// // 全空对象（3 个错误）、保留的 zone 拓扑键以及环回/链路本地等特殊 IP。
// // 每个子用例仅校验错误总数，因为具体错误文案属于实现细节，而错误数量已经能捕捉校验规则的回归。
func TestValidateEndpointSlice(t *testing.T) {
	// // 合法的元数据是大多数用例的共同前提，单独定义以便聚焦于被校验的字段。
	standardMeta := metav1.ObjectMeta{
		Name:      "hello",
		Namespace: "world",
	}

	testCases := map[string]struct {
		expectedErrors int
		legacyIPs      bool
		endpointSlice  *discovery.EndpointSlice
	}{
		// // 正例：一个最小但完整的 EndpointSlice，作为其他用例的对照基线。
		"good-slice": {
			expectedErrors: 0,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(1),
					Hostname:  ptr.To("valid-123"),
				}},
			},
		},
		"good-ipv6": {
			expectedErrors: 0,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv6,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: []string{"a00:100::4"},
					Hostname:  ptr.To("valid-123"),
				}},
			},
		},
		// // FQDN 允许结尾带点以及连字符，但大小写与下划线不合法（见 bad-fqdns）。
		"good-fqdns": {
			expectedErrors: 0,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeFQDN,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: []string{"foo.example.com", "example.com", "example.com.", "hyphens-are-good.example.com"},
					Hostname:  ptr.To("valid-123"),
				}},
			},
		},
		// // TCP/UDP/SCTP 都被允许，覆盖协议枚举的全部合法取值。
		"all-protocols": {
			expectedErrors: 0,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("tcp"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}, {
					Name:     ptr.To("udp"),
					Protocol: ptr.To(corev1.ProtocolUDP),
				}, {
					Name:     ptr.To("sctp"),
					Protocol: ptr.To(corev1.ProtocolSCTP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(1),
					Hostname:  ptr.To("valid-123"),
				}},
			},
		},
		// // appProtocol 既接受预定义写法也接受自定义 IANA 风格名称，但必须是合法格式（见 bad-app-protocol）。
		"app-protocols": {
			expectedErrors: 0,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:        ptr.To("one"),
					Protocol:    ptr.To(corev1.ProtocolTCP),
					AppProtocol: ptr.To("HTTP"),
				}, {
					Name:        ptr.To("two"),
					Protocol:    ptr.To(corev1.ProtocolTCP),
					AppProtocol: ptr.To("https"),
				}, {
					Name:        ptr.To("three"),
					Protocol:    ptr.To(corev1.ProtocolTCP),
					AppProtocol: ptr.To("my-protocol"),
				}, {
					Name:        ptr.To("four"),
					Protocol:    ptr.To(corev1.ProtocolTCP),
					AppProtocol: ptr.To("example.com/custom-protocol"),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(1),
					Hostname:  ptr.To("valid-123"),
				}},
			},
		},
		// // 端口名允许为空，但多个端口同时使用空名会冲突（见 duplicate-port-name）。
		"empty-port-name": {
			expectedErrors: 0,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}, {
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(1),
				}},
			},
		},
		"long-port-name": {
			expectedErrors: 0,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(strings.Repeat("a", 63)),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(1),
				}},
			},
		},
		// // 空集合是合法的：EndpointSlice 可以表示当前没有可用后端。
		"empty-ports-and-endpoints": {
			expectedErrors: 0,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports:       []discovery.EndpointPort{},
				Endpoints:   []discovery.Endpoint{},
			},
		},
		// // 恰好达到上限应当通过，确认校验用的是 "大于" 而不是 "大于等于"。
		"max-endpoints": {
			expectedErrors: 0,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports:       generatePorts(1),
				Endpoints:   generateEndpoints(maxEndpoints),
			},
		},
		"max-ports": {
			expectedErrors: 0,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports:       generatePorts(maxPorts),
			},
		},
		"max-addresses": {
			expectedErrors: 0,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(maxAddresses),
				}},
			},
		},
		// // 拓扑键数量达到上限时合法；保留键 topology.kubernetes.io/zone 单独由 zone-key-topology 覆盖。
		"max-topology-keys": {
			expectedErrors: 0,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses:          generateIPAddresses(1),
					DeprecatedTopology: generateTopology(maxTopologyLabels),
				}},
			},
		},
		// // zone 与 node hints 同时出现且合法，验证两种提示可以共存。
		"valid-hints": {
			expectedErrors: 0,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(1),
					Hints: &discovery.EndpointHints{
						ForZones: []discovery.ForZone{{Name: "zone-a"}},
						ForNodes: []discovery.ForNode{{Name: "node-1"}},
					},
				}},
			},
		},
		// // 前导零 IPv4 在关闭严格校验时应被容忍，体现向后兼容的升级路径。
		"legacy-ip-with-legacy-validation": {
			expectedErrors: 0,
			legacyIPs:      true,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: []string{"012.034.056.078"},
					Hostname:  ptr.To("valid-123"),
				}},
			},
		},

		// 以下用例应当产生错误。
		// // 两个端口同名（包括都为空名）违反唯一性，否则客户端无法区分端口。
		"duplicate-port-name": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}, {
					Name:     ptr.To(""),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{},
			},
		},
		// // 端口名必须符合 DNS_LABEL 规则，大写字母不合法。
		"bad-port-name-caps": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("aCapital"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{},
			},
		},
		"bad-port-name-chars": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("almost_valid"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{},
			},
		},
		// // 64 字符超过 DNS_LABEL 的 63 字符上限，验证边界。
		"bad-port-name-length": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(strings.Repeat("a", 64)),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{},
			},
		},
		// // 协议必须是已知枚举值，任意字符串 "foo" 不可接受。
		"invalid-port-protocol": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.Protocol("foo")),
				}},
			},
		},
		// // 端口数超过上限，验证集合大小约束。
		"too-many-ports": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports:       generatePorts(maxPorts + 1),
			},
		},
		"too-many-endpoints": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports:       generatePorts(1),
				Endpoints:   generateEndpoints(maxEndpoints + 1),
			},
		},
		// // 端点至少需要有一个地址，否则该端点不可用。
		"no-endpoint-addresses": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(0),
				}},
			},
		},
		"too-many-addresses": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(maxAddresses + 1),
				}},
			},
		},
		// // 拓扑键必须是合法限定名，"--INVALID" 故意使用非法字符。
		"bad-topology-key": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses:          generateIPAddresses(1),
					DeprecatedTopology: map[string]string{"--INVALID": "example"},
				}},
			},
		},
		"too-many-topology-keys": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses:          generateIPAddresses(1),
					DeprecatedTopology: generateTopology(maxTopologyLabels + 1),
				}},
			},
		},
		// // 主机名同样必须是合法 DNS_LABEL。
		"bad-hostname": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(1),
					Hostname:  ptr.To("--INVALID"),
				}},
			},
		},
		// // 对象名含非法字符，验证元数据校验也被纳入。
		"bad-meta": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "*&^",
					Namespace: "foo",
				},
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(1),
					Hostname:  ptr.To("valid-123"),
				}},
			},
		},
		// // 超过 255 的十进制段不是合法 IPv4。
		"bad-ip": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: []string{"123.456.789.012"},
					Hostname:  ptr.To("valid-123"),
				}},
			},
		},
		// // 同一前导零 IP 在严格校验开启时必须报错，与上面的 legacy 用例形成对照。
		"legacy-ip-with-strict-validation": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: []string{"012.034.056.078"},
					Hostname:  ptr.To("valid-123"),
				}},
			},
		},
		// // 同时包含一个非法 IPv4 和一个 IPv6 地址，期望 2 个错误：地址类型必须全部匹配 slice 声明。
		"bad-ipv4": {
			expectedErrors: 2,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: []string{"123.456.789.012", "2001:4860:4860::8888"},
					Hostname:  ptr.To("valid-123"),
				}},
			},
		},
		// // IPv6 切片中混入非法 IPv4 与带非法十六进制字符的地址，累计 2 个错误。
		"bad-ipv6": {
			expectedErrors: 2,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv6,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: []string{"123.456.789.012", "2001:4860:4860:defg"},
					Hostname:  ptr.To("valid-123"),
				}},
			},
		},
		// // 4 个 FQDN 全部非法（通配符、大写、下划线、通配符前缀），验证逐地址报错并累加。
		"bad-fqdns": {
			expectedErrors: 4,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeFQDN,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: []string{"foo.*", "FOO.example.com", "underscores_are_bad.example.com", "*.example.com"},
					Hostname:  ptr.To("valid-123"),
				}},
			},
		},
		"bad-app-protocol": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:        ptr.To("http"),
					Protocol:    ptr.To(corev1.ProtocolTCP),
					AppProtocol: ptr.To("--"),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(1),
					Hostname:  ptr.To("valid-123"),
				}},
			},
		},
		"invalid-zone-hint": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(1),
					Hints: &discovery.EndpointHints{
						ForZones: []discovery.ForZone{{Name: "inv@lid"}},
					},
				}},
			},
		},
		"overlapping-zone-hints": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(1),
					Hints: &discovery.EndpointHints{
						ForZones: []discovery.ForZone{
							{Name: "zone-a"},
							{Name: "zone-b"},
							{Name: "zone-a"},
						},
					},
				}},
			},
		},
		"too-many-zone-hints": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(1),
					Hints: &discovery.EndpointHints{
						ForZones: []discovery.ForZone{
							{Name: "zone-a"},
							{Name: "zone-b"},
							{Name: "zone-c"},
							{Name: "zone-d"},
							{Name: "zone-e"},
							{Name: "zone-f"},
							{Name: "zone-g"},
							{Name: "zone-h"},
							{Name: "zone-i"},
						},
					},
				}},
			},
		},
		// // 两个 node hint 名称均非法（特殊字符与空串），累计 2 个错误。
		"invalid-node-hints": {
			expectedErrors: 2,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(1),
					Hints: &discovery.EndpointHints{
						ForNodes: []discovery.ForNode{
							{Name: "!@#$!@"},
							{Name: ""},
						},
					},
				}},
			},
		},
		// // 重复的 node hint 违反去重约束。
		"overlapping-node-hints": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(1),
					Hints: &discovery.EndpointHints{
						ForNodes: []discovery.ForNode{
							{Name: "node-1"},
							{Name: "node-2"},
							{Name: "node-1"},
						},
					},
				}},
			},
		},
		"too-many-node-hints": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(1),
					Hints: &discovery.EndpointHints{
						ForNodes: []discovery.ForNode{
							{Name: "node-1"},
							{Name: "node-2"},
							{Name: "node-3"},
							{Name: "node-4"},
							{Name: "node-5"},
							{Name: "node-6"},
							{Name: "node-7"},
							{Name: "node-8"},
							{Name: "node-9"},
						},
					},
				}},
			},
		},
		// // 完全空的对象同时违反地址类型、元数据和端点约束，期望 3 个错误。
		"empty-everything": {
			expectedErrors: 3,
			endpointSlice:  &discovery.EndpointSlice{},
		},
		// // deprecated topology 中禁止使用保留的 zone 键，避免与专用字段语义冲突。
		"zone-key-topology": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses:          generateIPAddresses(1),
					DeprecatedTopology: map[string]string{corev1.LabelTopologyZone: "zone1"},
				}},
			},
		},
		// // 环回地址不适合作为服务端点，即使格式合法也应拒绝。
		"special-ipv4": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: []string{"127.0.0.1"},
					Hostname:  ptr.To("valid-123"),
				}},
			},
		},
		// // 链路本地地址同样不适合作为服务端点。
		"special-ipv6": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv6,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: []string{"fe80::9656:d028:8652:66b6"},
					Hostname:  ptr.To("valid-123"),
				}},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			// // 严格 IP 校验开关的值由 legacyIPs 取反得到：legacy 用例关闭，其余用例开启。
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.StrictIPCIDRValidation, !testCase.legacyIPs)
			errs := ValidateEndpointSlice(testCase.endpointSlice, nil)
			if len(errs) != testCase.expectedErrors {
				t.Errorf("Expected %d errors, got %d errors: %v", testCase.expectedErrors, len(errs), errs)
			}
		})
	}
}

// // TestValidateEndpointSliceCreate 校验创建路径特有的规则。
// // 表格覆盖带合法 nodeName 的正例、非法节点名、已废弃的 "IP" 地址类型以及完全未知的地址类型。
// // 通用字段校验由 TestValidateEndpointSlice 覆盖，这里只关注创建时额外生效的约束；
// // 地址类型校验尤其重要，因为它决定后续地址格式校验走哪条分支。
func TestValidateEndpointSliceCreate(t *testing.T) {
	standardMeta := metav1.ObjectMeta{
		Name:      "hello",
		Namespace: "world",
	}

	testCases := map[string]struct {
		expectedErrors      int
		endpointSlice       *discovery.EndpointSlice
		nodeNameGateEnabled bool
	}{
		"good-slice": {
			expectedErrors: 0,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(1),
					Hostname:  ptr.To("valid-123"),
				}},
			},
		},
		"good-slice-node-name": {
			expectedErrors: 0,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(1),
					Hostname:  ptr.To("valid-123"),
					NodeName:  ptr.To("valid-node-name"),
				}},
			},
		},

		// expected failures
		// // 节点名含大写字母不合法，验证 nodeName 的格式约束。
		"bad-node-name": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(1),
					Hostname:  ptr.To("valid-123"),
					NodeName:  ptr.To("INvalid-node-name"),
				}},
			},
		},
		// // "IP" 是已废弃值，创建时必须拒绝以阻止新增对象继续使用它。
		"deprecated-address-type": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressType("IP"),
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(1),
				}},
			},
		},
		// // 未知地址类型无法决定地址校验策略，应直接报错。
		"bad-address-type": {
			expectedErrors: 1,
			endpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressType("other"),
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To("http"),
					Protocol: ptr.To(corev1.ProtocolTCP),
				}},
				Endpoints: []discovery.Endpoint{{
					Addresses: generateIPAddresses(1),
				}},
			},
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.StrictIPCIDRValidation, true)
			errs := ValidateEndpointSliceCreate(testCase.endpointSlice)
			if len(errs) != testCase.expectedErrors {
				t.Errorf("Expected %d errors, got %d errors: %v", testCase.expectedErrors, len(errs), errs)
			}
		})
	}
}

// // TestValidateEndpointSliceUpdate 校验更新路径的语义，重点在于地址类型不可变以及 IP 的增量校验。
// // 表格中 "valid and identical slices" 与 "unchanged IPs are not revalidated" 说明：
// // 旧对象中已有的非严格 IP 在未被修改时不会被重新校验，从而保证升级前写入的存量对象仍可更新。
// // "changed IPs are revalidated" 则说明一旦地址被改动，新值必须按当前严格规则校验。
// // 其余反例覆盖非法 nodeName、废弃/变化的地址类型以及 spec 本身的非法字段。
func TestValidateEndpointSliceUpdate(t *testing.T) {
	standardMeta := metav1.ObjectMeta{Name: "es1", Namespace: "test"}

	testCases := map[string]struct {
		expectedErrors      int
		nodeNameGateEnabled bool
		oldEndpointSlice    *discovery.EndpointSlice
		newEndpointSlice    *discovery.EndpointSlice
	}{
		"valid and identical slices": {
			oldEndpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv6,
			},
			newEndpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv6,
			},
			expectedErrors: 0,
		},
		// // "10.1.2.04" 含前导零，属于存量合法数据；新旧一致时不应因校验规则收紧而阻断更新。
		"unchanged IPs are not revalidated": {
			oldEndpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{{
					Addresses: []string{"10.1.2.04"},
				}},
			},
			newEndpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{{
					Addresses: []string{"10.1.2.04"},
				}},
			},
			expectedErrors: 0,
		},

		// expected errors
		// // 更新时新增了非法 nodeName，验证节点名在更新路径同样被校验。
		"invalid node name set": {
			oldEndpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{{
					Addresses: []string{"10.1.2.3"},
				}},
			},
			newEndpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{{
					Addresses: []string{"10.1.2.3"},
					NodeName:  ptr.To("INVALID foo"),
				}},
			},
			expectedErrors: 1,
		},

		// // 即使旧对象已经是废弃值，更新时仍会报错，防止该值被继续保留。
		"deprecated address type": {
			expectedErrors: 1,
			oldEndpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressType("IP"),
			},
			newEndpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressType("IP"),
			},
		},
		// // 修改 addressType 本身即违规，因为地址语义会改变，旧端点地址将无法按新类型解释。
		"valid and identical slices with different address types": {
			oldEndpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressType("other"),
			},
			newEndpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
			},
			expectedErrors: 1,
		},
		"invalid slices with valid address types": {
			oldEndpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
			},
			newEndpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Ports: []discovery.EndpointPort{{
					Name:     ptr.To(""),
					Protocol: ptr.To(corev1.Protocol("invalid")),
				}},
			},
			expectedErrors: 1,
		},
		// // 地址从 10.1.2.3 改为带前导零的 10.1.2.03，必须按严格规则报错，体现增量校验的边界。
		"changed IPs are revalidated": {
			oldEndpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{{
					Addresses: []string{"10.1.2.3"},
				}},
			},
			newEndpointSlice: &discovery.EndpointSlice{
				ObjectMeta:  standardMeta,
				AddressType: discovery.AddressTypeIPv4,
				Endpoints: []discovery.Endpoint{{
					Addresses: []string{"10.1.2.03"},
				}},
			},
			expectedErrors: 1,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.StrictIPCIDRValidation, true)
			errs := ValidateEndpointSliceUpdate(testCase.newEndpointSlice, testCase.oldEndpointSlice)
			if len(errs) != testCase.expectedErrors {
				t.Errorf("Expected %d errors, got %d errors: %v", testCase.expectedErrors, len(errs), errs)
			}
		})
	}
}

// 测试辅助函数：批量构造端口、端点、IP 地址与拓扑标签，避免每个用例重复手写长列表。

// // generatePorts 生成 n 个名字互不相同的合法端口，用于构造达到数量边界的测试对象。
func generatePorts(n int) []discovery.EndpointPort {
	ports := []discovery.EndpointPort{}
	for i := 0; i < n; i++ {
		ports = append(ports, discovery.EndpointPort{
			Name:     ptr.To(fmt.Sprintf("http-%d", i)),
			Protocol: ptr.To(corev1.ProtocolTCP),
		})
	}
	return ports
}

// // generateEndpoints 生成 n 个仅含单一合法 IPv4 地址的端点，避免地址数量约束干扰端点数量测试。
func generateEndpoints(n int) []discovery.Endpoint {
	endpoints := []discovery.Endpoint{}
	for i := 0; i < n; i++ {
		endpoints = append(endpoints, discovery.Endpoint{
			Addresses: []string{fmt.Sprintf("10.1.2.%d", i%255)},
		})
	}
	return endpoints
}

// // generateIPAddresses 生成 n 个不同的合法 IPv4 地址（用取模保证末段在字节范围内）。
func generateIPAddresses(n int) []string {
	addresses := []string{}
	for i := 0; i < n; i++ {
		addresses = append(addresses, fmt.Sprintf("10.1.2.%d", i%255))
	}
	return addresses
}

// // generateTopology 生成 n 个合法的拓扑键值对，用于构造拓扑键数量的边界用例。
func generateTopology(n int) map[string]string {
	topology := map[string]string{}
	for i := 0; i < n; i++ {
		topology[fmt.Sprintf("topology-%d", i)] = "example"
	}
	return topology
}
