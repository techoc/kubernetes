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

package testing

import (
	"fmt"
	"net"
	"sort"
	"strconv"
	"time"

	utilipvs "k8s.io/kubernetes/pkg/proxy/ipvs/util"
)

// FakeIPVS 是 ipvs Interface 的空操作实现，用内存缓存模拟 ipvs 状态
type FakeIPVS struct {
	Scheduler    string
	Services     map[ServiceKey]*utilipvs.VirtualServer
	Destinations map[ServiceKey][]*utilipvs.RealServer
}

// ServiceKey 唯一标识一个 IPVS 虚拟服务器对应的 Service
type ServiceKey struct {
	IP       string
	Port     uint16
	Protocol string
}

// String 返回 ServiceKey 的可读表示，格式为 "IP:端口/协议"
func (s *ServiceKey) String() string {
	return fmt.Sprintf("%s:%d/%s", s.IP, s.Port, s.Protocol)
}

// RealServerKey 唯一标识一个 IPVS 真实服务器对应的 Endpoint
type RealServerKey struct {
	Address net.IP
	Port    uint16
}

// String 返回 RealServerKey 的可读表示，格式为 "地址:端口"
func (r *RealServerKey) String() string {
	return net.JoinHostPort(r.Address.String(), strconv.Itoa(int(r.Port)))
}

// byAddress 按地址对真实服务器排序，实现 https://pkg.go.dev/sort#Interface
type byAddress []*utilipvs.RealServer

// Len 返回待排序真实服务器的数量
func (a byAddress) Len() int {
	return len(a)
}

// Less 按真实服务器的可读地址字符串比较大小
func (a byAddress) Less(i, j int) bool {
	return a[i].String() < a[j].String()
}

// Swap 交换两个真实服务器的位置
func (a byAddress) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// NewFake 创建一个 fake ipvs 实现，本质上是一个缓存存储
func NewFake() *FakeIPVS {
	return &FakeIPVS{
		Services:     make(map[ServiceKey]*utilipvs.VirtualServer),
		Destinations: make(map[ServiceKey][]*utilipvs.RealServer),
	}
}

// toServiceKey 从虚拟服务器中提取用于缓存索引的 ServiceKey
func toServiceKey(serv *utilipvs.VirtualServer) ServiceKey {
	return ServiceKey{
		IP:       serv.Address.String(),
		Port:     serv.Port,
		Protocol: serv.Protocol,
	}
}

// toRealServerKey 从真实服务器中提取用于比较的 RealServerKey
func toRealServerKey(rs *utilipvs.RealServer) *RealServerKey {
	return &RealServerKey{
		Address: rs.Address,
		Port:    rs.Port,
	}
}

// AddVirtualServer 是 fake 实现，仅将虚拟服务器加入缓存存储
func (f *FakeIPVS) AddVirtualServer(serv *utilipvs.VirtualServer) error {
	if serv == nil {
		return fmt.Errorf("failed to add virtual server, error: virtual server can't be nil")
	}
	key := toServiceKey(serv)
	f.Services[key] = serv
	// 新建服务时确保没有任何真实服务器残留，避免复用旧的 destinations
	f.Destinations[key] = make([]*utilipvs.RealServer, 0)
	return nil
}

// UpdateVirtualServer 是 fake 实现，更新缓存存储中的虚拟服务器
func (f *FakeIPVS) UpdateVirtualServer(serv *utilipvs.VirtualServer) error {
	if serv == nil {
		return fmt.Errorf("failed to update service, service can't be nil")
	}
	key := toServiceKey(serv)
	f.Services[key] = serv
	return nil
}

// DeleteVirtualServer 是 fake 实现，从缓存存储中删除虚拟服务器
func (f *FakeIPVS) DeleteVirtualServer(serv *utilipvs.VirtualServer) error {
	if serv == nil {
		return fmt.Errorf("failed to delete service: service can't be nil")
	}
	key := toServiceKey(serv)
	delete(f.Services, key)
	// 服务删除时其下属的真实服务器也必须一并清除
	f.Destinations[key] = nil
	return nil
}

// GetVirtualServer 是 fake 实现，尝试从缓存存储中查找指定的虚拟服务器
func (f *FakeIPVS) GetVirtualServer(serv *utilipvs.VirtualServer) (*utilipvs.VirtualServer, error) {
	if serv == nil {
		return nil, fmt.Errorf("failed to get service: service can't be nil")
	}
	key := toServiceKey(serv)
	svc, found := f.Services[key]
	if found {
		return svc, nil
	}
	return nil, fmt.Errorf("not found serv: %v", key.String())
}

// GetVirtualServers 是 fake 实现，返回缓存存储中的所有虚拟服务器
func (f *FakeIPVS) GetVirtualServers() ([]*utilipvs.VirtualServer, error) {
	res := make([]*utilipvs.VirtualServer, 0)
	for _, svc := range f.Services {
		res = append(res, svc)
	}
	return res, nil
}

// Flush 是 fake 实现，清空整个缓存存储
func (f *FakeIPVS) Flush() error {
	// 直接丢弃旧数据，模拟 ipvs 规则的完全清空
	f.Services = nil
	f.Destinations = nil
	return nil
}

// AddRealServer 是 fake 实现，在缓存存储中为虚拟服务器创建真实服务器
func (f *FakeIPVS) AddRealServer(serv *utilipvs.VirtualServer, dest *utilipvs.RealServer) error {
	if serv == nil || dest == nil {
		return fmt.Errorf("failed to add destination for service, neither service nor destination shouldn't be nil")
	}
	key := toServiceKey(serv)
	if _, ok := f.Services[key]; !ok {
		return fmt.Errorf("failed to add destination for service %v, service not found", key.String())
	}
	dests := f.Destinations[key]
	if dests == nil {
		dests = make([]*utilipvs.RealServer, 0)
		f.Destinations[key] = dests
	}
	f.Destinations[key] = append(f.Destinations[key], dest)
	// 测试假定该切片始终有序，因此每次插入后都要重新排序
	sort.Sort(byAddress(f.Destinations[key]))
	return nil
}

// GetRealServers 是 fake 实现，返回缓存存储中指定虚拟服务器下的所有真实服务器
func (f *FakeIPVS) GetRealServers(serv *utilipvs.VirtualServer) ([]*utilipvs.RealServer, error) {
	if serv == nil {
		return nil, fmt.Errorf("failed to get destination for nil service")
	}
	key := toServiceKey(serv)
	if _, ok := f.Services[key]; !ok {
		return nil, fmt.Errorf("failed to get destinations for service %v, service not found", key.String())
	}
	return f.Destinations[key], nil
}

// DeleteRealServer 是 fake 实现，从缓存存储中删除指定的真实服务器
func (f *FakeIPVS) DeleteRealServer(serv *utilipvs.VirtualServer, dest *utilipvs.RealServer) error {
	if serv == nil || dest == nil {
		return fmt.Errorf("failed to delete destination, neither service nor destination can't be nil")
	}
	key := toServiceKey(serv)
	if _, ok := f.Services[key]; !ok {
		return fmt.Errorf("failed to delete destination for service %v, service not found", key.String())
	}
	dests := f.Destinations[key]
	exist := false
	for i := range dests {
		if toRealServerKey(dests[i]).String() == toRealServerKey(dest).String() {
			// 通过切片拼接删除单个元素，找到后即停止遍历
			f.Destinations[key] = append(f.Destinations[key][:i], f.Destinations[key][i+1:]...)
			exist = true
			break
		}
	}
	// 未找到目标真实服务器时返回错误，与真实 ipvs 行为保持一致
	if !exist {
		return fmt.Errorf("failed to delete real server for service %v, real server not found", key.String())
	}
	return nil
}

// UpdateRealServer 是 fake 实现，先删除旧的真实服务器再添加新的（用 Key 匹配实现更新语义）
func (f *FakeIPVS) UpdateRealServer(serv *utilipvs.VirtualServer, dest *utilipvs.RealServer) error {
	err := f.DeleteRealServer(serv, dest)
	if err != nil {
		return err
	}
	return f.AddRealServer(serv, dest)
}

// ConfigureTimeouts 在 fake IPVS 中不支持，调用即返回错误
func (f *FakeIPVS) ConfigureTimeouts(time.Duration, time.Duration, time.Duration) error {
	return fmt.Errorf("not supported in fake IPVS")
}

// 编译期断言：确保 FakeIPVS 实现了 utilipvs.Interface 接口
var _ = utilipvs.Interface(&FakeIPVS{})
