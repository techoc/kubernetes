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

package ipvs

import (
	"net"
	"strconv"
	"strings"
	"time"
)

// Interface 是可注入的 ipvs 命令操作接口，实现必须保证并发安全。
type Interface interface {
	// Flush 清空系统中的所有虚拟服务器，一旦出错立即返回该错误。
	Flush() error
	// AddVirtualServer 创建指定的虚拟服务器
	AddVirtualServer(*VirtualServer) error
	// UpdateVirtualServer 更新一个已存在的虚拟服务器；若该虚拟服务器不存在则返回错误。
	UpdateVirtualServer(*VirtualServer) error
	// DeleteVirtualServer 删除指定的虚拟服务器；若该虚拟服务器不存在则返回错误。
	DeleteVirtualServer(*VirtualServer) error
	// GetVirtualServer 接收一个部分指定的虚拟服务器，返回系统中与之匹配的虚拟服务器信息。
	GetVirtualServer(*VirtualServer) (*VirtualServer, error)
	// GetVirtualServers 列出系统中的所有虚拟服务器
	GetVirtualServers() ([]*VirtualServer, error)
	// AddRealServer 为指定的虚拟服务器创建真实服务器
	AddRealServer(*VirtualServer, *RealServer) error
	// GetRealServers 返回指定虚拟服务器下的所有真实服务器
	GetRealServers(*VirtualServer) ([]*RealServer, error)
	// DeleteRealServer 从指定的虚拟服务器中删除真实服务器
	DeleteRealServer(*VirtualServer, *RealServer) error
	// UpdateRealServer 更新指定虚拟服务器下的真实服务器
	UpdateRealServer(*VirtualServer, *RealServer) error
	// ConfigureTimeouts 等价于执行 "ipvsadm --set" 来配置 tcp、tcpfin 和 udp 的超时时间
	ConfigureTimeouts(time.Duration, time.Duration, time.Duration) error
}

// VirtualServer 是面向用户的 IPVS 虚拟服务器完整定义
type VirtualServer struct {
	Address   net.IP
	Protocol  string
	Port      uint16
	Scheduler string
	Flags     ServiceFlags
	Timeout   uint32
}

// ServiceFlags 用于指定会话保持、IP 哈希等服务标志
type ServiceFlags uint32

// 以下常量定义 IPVS 服务的标志位
const (
	// FlagPersistent 指定 IPVS 服务的会话保持
	FlagPersistent = 0x1
	// FlagHashed 指定 IPVS 服务的哈希标志
	FlagHashed = 0x2
	// FlagSourceHash 基于源端口和源 IP 对 IPVS 服务启用哈希
	FlagSourceHash = 0x10
)

// Equal 检查两个虚拟服务器是否相等。
// 由于结构体中包含切片，不能用 struct == 直接比较。
func (svc *VirtualServer) Equal(other *VirtualServer) bool {
	return svc.Address.Equal(other.Address) &&
		svc.Protocol == other.Protocol &&
		svc.Port == other.Port &&
		svc.Scheduler == other.Scheduler &&
		svc.Flags == other.Flags &&
		svc.Timeout == other.Timeout
}

// String 返回虚拟服务器的可读表示，格式为 "地址:端口/协议"
func (svc *VirtualServer) String() string {
	return net.JoinHostPort(svc.Address.String(), strconv.Itoa(int(svc.Port))) + "/" + svc.Protocol
}

// RealServer 是面向用户的 IPVS 真实服务器完整定义
type RealServer struct {
	Address      net.IP
	Port         uint16
	Weight       int
	ActiveConn   int
	InactiveConn int
}

// String 返回真实服务器的可读表示，格式为 "地址:端口"
func (rs *RealServer) String() string {
	return net.JoinHostPort(rs.Address.String(), strconv.Itoa(int(rs.Port)))
}

// Equal 检查两个真实服务器是否相等。
// 由于结构体中包含切片，不能用 struct == 直接比较。
func (rs *RealServer) Equal(other *RealServer) bool {
	return rs.Address.Equal(other.Address) &&
		rs.Port == other.Port
}

// IsRsGracefulTerminationNeeded 当协议需要为残留连接执行优雅终止时返回 true
func IsRsGracefulTerminationNeeded(proto string) bool {
	return !strings.EqualFold(proto, "UDP") && !strings.EqualFold(proto, "SCTP")
}
