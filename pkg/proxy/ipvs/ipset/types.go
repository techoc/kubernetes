//go:build linux

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

package ipset

// Type 表示 ipset 集合的类型。
type Type string

const (
	// HashIPPort 表示 `hash:ip,port` 类型的 ipset。它与 hash:ip 类似，
	// 但可存放 IP 地址与"协议-端口"的组合。支持带端口号的 TCP、SCTP、UDP、UDPLITE、ICMP 和 ICMPv6，
	// 也支持 ICMP(v6) 类型，其他协议号则不带端口信息。
	HashIPPort Type = "hash:ip,port"
	// HashIPPortIP 表示 `hash:ip,port,ip` 类型的 ipset。它用哈希表存放
	// IP 地址、端口号和第二个 IP 地址组成的三元组。端口号需与协议（默认 TCP）一起解释，且协议号不能为 0。
	HashIPPortIP Type = "hash:ip,port,ip"
	// HashIPPortNet 表示 `hash:ip,port,net` 类型的 ipset。它用哈希表存放
	// IP 地址、端口号和 IP 网段组成的三元组。端口号需与协议（默认 TCP）一起解释，协议号不能为 0；
	// 前缀长度为 0 的网络地址同样无法存入。
	HashIPPortNet Type = "hash:ip,port,net"
	// BitmapPort 表示 `bitmap:port` 类型的 ipset。它用一段连续内存存放位图，每个 bit 代表一个 TCP/UDP 端口，
	// 最多可存储 65535 个端口。
	BitmapPort Type = "bitmap:port"
	// HashIP 表示 `hash:ip` 类型的 ipset。
	HashIP Type = "hash:ip"
)

// DefaultPortRange 定义 bitmap:port 集合默认的有效端口范围。
const DefaultPortRange string = "0-65535"

const (
	// ProtocolFamilyIPV4 表示 IPv4 协议族。
	ProtocolFamilyIPV4 = "inet"
	// ProtocolFamilyIPV6 表示 IPv6 协议族。
	ProtocolFamilyIPV6 = "inet6"
	// ProtocolTCP 表示 TCP 协议。
	ProtocolTCP = "tcp"
	// ProtocolUDP 表示 UDP 协议。
	ProtocolUDP = "udp"
	// ProtocolSCTP 表示 SCTP 协议。
	ProtocolSCTP = "sctp"
)

// ValidIPSetTypes 列出当前支持的 ipset 类型。
var ValidIPSetTypes = []Type{
	HashIPPort,
	HashIPPortIP,
	BitmapPort,
	HashIPPortNet,
	HashIP,
}
