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
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	libipvs "github.com/moby/ipvs"
	"golang.org/x/sys/unix"

	"k8s.io/klog/v2"
)

// runner 是 ipvs.Interface 的具体实现，封装 libipvs 句柄。
type runner struct {
	ipvsHandle *libipvs.Handle
	mu         sync.Mutex // 保护 Netlink 调用，避免并发修改内核 IPVS 状态
}

// Protocol 表示 IPVS 服务的协议类型。
type Protocol uint16

// New 创建并返回一个调用 IPVS API 的 Interface 实例。
// 初始化失败时返回 nil，由调用方按不可用后端处理。
func New() Interface {
	handle, err := libipvs.New("")
	if err != nil {
		klog.ErrorS(err, "IPVS interface can't be initialized")
		return nil
	}
	return &runner{
		ipvsHandle: handle,
	}
}

// AddVirtualServer 实现 ipvs.Interface，新增一个 IPVS 虚拟服务。
func (runner *runner) AddVirtualServer(vs *VirtualServer) error {
	svc, err := toIPVSService(vs)
	if err != nil {
		return fmt.Errorf("could not convert local virtual server to IPVS service: %w", err)
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.ipvsHandle.NewService(svc)
}

// UpdateVirtualServer 实现 ipvs.Interface，更新已有 IPVS 虚拟服务。
func (runner *runner) UpdateVirtualServer(vs *VirtualServer) error {
	svc, err := toIPVSService(vs)
	if err != nil {
		return fmt.Errorf("could not convert local virtual server to IPVS service: %w", err)
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.ipvsHandle.UpdateService(svc)
}

// DeleteVirtualServer 实现 ipvs.Interface，删除一个 IPVS 虚拟服务。
func (runner *runner) DeleteVirtualServer(vs *VirtualServer) error {
	svc, err := toIPVSService(vs)
	if err != nil {
		return fmt.Errorf("could not convert local virtual server to IPVS service: %w", err)
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.ipvsHandle.DelService(svc)
}

// GetVirtualServer 实现 ipvs.Interface，查询指定的 IPVS 虚拟服务。
func (runner *runner) GetVirtualServer(vs *VirtualServer) (*VirtualServer, error) {
	svc, err := toIPVSService(vs)
	if err != nil {
		return nil, fmt.Errorf("could not convert local virtual server to IPVS service: %w", err)
	}
	runner.mu.Lock()
	ipvsSvc, err := runner.ipvsHandle.GetService(svc)
	runner.mu.Unlock()

	if err != nil {
		return nil, fmt.Errorf("could not get IPVS service: %w", err)
	}
	vServ, err := toVirtualServer(ipvsSvc)
	if err != nil {
		return nil, fmt.Errorf("could not convert IPVS service to local virtual server: %w", err)
	}
	return vServ, nil
}

// GetVirtualServers 实现 ipvs.Interface，列出所有 IPVS 虚拟服务。
func (runner *runner) GetVirtualServers() ([]*VirtualServer, error) {
	runner.mu.Lock()
	ipvsSvcs, err := runner.ipvsHandle.GetServices()
	runner.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("could not get IPVS services: %w", err)
	}
	vss := make([]*VirtualServer, 0, len(ipvsSvcs))
	for _, ipvsSvc := range ipvsSvcs {
		vs, err := toVirtualServer(ipvsSvc)
		if err != nil {
			return nil, fmt.Errorf("could not convert IPVS service to local virtual server: %w", err)
		}
		vss = append(vss, vs)
	}
	return vss, nil
}

// Flush 实现 ipvs.Interface，清空全部 IPVS 服务。
// 当前实现是逐个删除 IPVS 服务，而不是一次性重置内核表。
func (runner *runner) Flush() error {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.ipvsHandle.Flush()
}

// AddRealServer 实现 ipvs.Interface，为虚拟服务新增一个真实后端。
func (runner *runner) AddRealServer(vs *VirtualServer, rs *RealServer) error {
	svc, err := toIPVSService(vs)
	if err != nil {
		return fmt.Errorf("could not convert local virtual server to IPVS service: %w", err)
	}
	dst, err := toIPVSDestination(rs)
	if err != nil {
		return fmt.Errorf("could not convert local real server to IPVS destination: %w", err)
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.ipvsHandle.NewDestination(svc, dst)
}

// DeleteRealServer 实现 ipvs.Interface，从虚拟服务删除一个真实后端。
func (runner *runner) DeleteRealServer(vs *VirtualServer, rs *RealServer) error {
	svc, err := toIPVSService(vs)
	if err != nil {
		return fmt.Errorf("could not convert local virtual server to IPVS service: %w", err)
	}
	dst, err := toIPVSDestination(rs)
	if err != nil {
		return fmt.Errorf("could not convert local real server to IPVS destination: %w", err)
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.ipvsHandle.DelDestination(svc, dst)
}

// UpdateRealServer 实现 ipvs.Interface，更新虚拟服务下某个真实后端的配置。
func (runner *runner) UpdateRealServer(vs *VirtualServer, rs *RealServer) error {
	svc, err := toIPVSService(vs)
	if err != nil {
		return fmt.Errorf("could not convert local virtual server to IPVS service: %w", err)
	}
	dst, err := toIPVSDestination(rs)
	if err != nil {
		return fmt.Errorf("could not convert local real server to IPVS destination: %w", err)
	}
	runner.mu.Lock()
	defer runner.mu.Unlock()
	return runner.ipvsHandle.UpdateDestination(svc, dst)
}

// GetRealServers 实现 ipvs.Interface，查询虚拟服务下的全部真实后端。
func (runner *runner) GetRealServers(vs *VirtualServer) ([]*RealServer, error) {
	svc, err := toIPVSService(vs)
	if err != nil {
		return nil, fmt.Errorf("could not convert local virtual server to IPVS service: %w", err)
	}
	runner.mu.Lock()
	dsts, err := runner.ipvsHandle.GetDestinations(svc)
	runner.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("could not get IPVS destination for service: %w", err)
	}
	rss := make([]*RealServer, 0, len(dsts))
	for _, dst := range dsts {
		dst, err := toRealServer(dst)
		// TODO: aggregate errors?
		if err != nil {
			return nil, fmt.Errorf("could not convert IPVS destination to local real server: %w", err)
		}
		rss = append(rss, dst)
	}
	return rss, nil
}

// ConfigureTimeouts 等价于执行 "ipvsadm --set"，设置 tcp、tcpfin 和 udp 的超时时间。
func (runner *runner) ConfigureTimeouts(tcpTimeout, tcpFinTimeout, udpTimeout time.Duration) error {
	ipvsConfig := &libipvs.Config{
		TimeoutTCP:    tcpTimeout,
		TimeoutTCPFin: tcpFinTimeout,
		TimeoutUDP:    udpTimeout,
	}

	return runner.ipvsHandle.SetConfig(ipvsConfig)
}

// toVirtualServer 将 IPVS Service 转换为对应的 VirtualServer 结构。
func toVirtualServer(svc *libipvs.Service) (*VirtualServer, error) {
	if svc == nil {
		return nil, errors.New("ipvs svc should not be empty")
	}
	vs := &VirtualServer{
		Address:   svc.Address,
		Port:      svc.Port,
		Scheduler: svc.SchedName,
		Protocol:  protocolToString(Protocol(svc.Protocol)),
		Timeout:   svc.Timeout,
	}

	// 校验 FlagHashed (0x2)：每个服务都会被哈希进服务表，因此该标志必须置位。
	if svc.Flags&FlagHashed == 0 {
		return nil, fmt.Errorf("Flags of successfully created IPVS service should enable the flag (%x) since every service is hashed into the service table", FlagHashed)
	}
	// 去掉 FlagHashed 位后再保存自定义标志。
	// 011 -> 001，010 -> 000
	vs.Flags = ServiceFlags(svc.Flags &^ uint32(FlagHashed))

	// 内核返回空地址时按地址族补零，避免下游拿到 nil 地址。
	if vs.Address == nil {
		if svc.AddressFamily == unix.AF_INET {
			vs.Address = net.IPv4zero
		} else {
			vs.Address = net.IPv6zero
		}
	}
	return vs, nil
}

// toRealServer 将 IPVS Destination 转换为对应的 RealServer 结构。
func toRealServer(dst *libipvs.Destination) (*RealServer, error) {
	if dst == nil {
		return nil, errors.New("ipvs destination should not be empty")
	}
	return &RealServer{
		Address:      dst.Address,
		Port:         dst.Port,
		Weight:       dst.Weight,
		ActiveConn:   dst.ActiveConnections,
		InactiveConn: dst.InactiveConnections,
	}, nil
}

// toIPVSService 将 VirtualServer 转换为对应的 IPVS Service 结构。
func toIPVSService(vs *VirtualServer) (*libipvs.Service, error) {
	if vs == nil {
		return nil, errors.New("virtual server should not be empty")
	}
	ipvsSvc := &libipvs.Service{
		Address:   vs.Address,
		Protocol:  stringToProtocol(vs.Protocol),
		Port:      vs.Port,
		SchedName: vs.Scheduler,
		Flags:     uint32(vs.Flags),
		Timeout:   vs.Timeout,
	}

	// Linux 内核 IPVS 接口要求显式指定地址族与掩码：IPv4 用 32 位全 1，IPv6 用前缀长度 128。
	if ip4 := vs.Address.To4(); ip4 != nil {
		ipvsSvc.AddressFamily = unix.AF_INET
		ipvsSvc.Netmask = 0xffffffff
	} else {
		ipvsSvc.AddressFamily = unix.AF_INET6
		ipvsSvc.Netmask = 128
	}
	return ipvsSvc, nil
}

// toIPVSDestination 将 RealServer 转换为对应的 IPVS Destination 结构。
func toIPVSDestination(rs *RealServer) (*libipvs.Destination, error) {
	if rs == nil {
		return nil, errors.New("real server should not be empty")
	}
	return &libipvs.Destination{
		Address: rs.Address,
		Port:    rs.Port,
		Weight:  rs.Weight,
	}, nil
}

// stringToProtocol 返回给定协议名称对应的协议编号。
func stringToProtocol(protocol string) uint16 {
	switch strings.ToLower(protocol) {
	case "tcp":
		return uint16(unix.IPPROTO_TCP)
	case "udp":
		return uint16(unix.IPPROTO_UDP)
	case "sctp":
		return uint16(unix.IPPROTO_SCTP)
	}
	return uint16(0)
}

// protocolToString 返回给定协议编号对应的名称。
func protocolToString(proto Protocol) string {
	switch proto {
	case unix.IPPROTO_TCP:
		return "TCP"
	case unix.IPPROTO_UDP:
		return "UDP"
	case unix.IPPROTO_SCTP:
		return "SCTP"
	}
	return ""
}
