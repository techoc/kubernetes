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
	"k8s.io/apimachinery/pkg/util/sets"
)

// NetLinkHandle 封装用于操作 netlink 网络接口的接口
type NetLinkHandle interface {
	// EnsureAddressBind 检查地址是否已绑定到接口，若未绑定则绑定它；若已绑定则返回 true。
	EnsureAddressBind(address, devName string) (exist bool, err error)
	// UnbindAddress 从接口上解绑指定地址
	UnbindAddress(address, devName string) error
	// EnsureDummyDevice 检查 dummy 设备是否存在，若不存在则创建一个；若已存在则返回 true。
	EnsureDummyDevice(devName string) (exist bool, err error)
	// DeleteDummyDevice 按名称删除指定的 dummy 设备
	DeleteDummyDevice(devName string) error
	// ListBindAddress 列出指定接口上绑定的所有 IP 地址
	ListBindAddress(devName string) ([]string, error)
	// GetAllLocalAddresses 返回节点上的所有本地地址。
	// 仅返回当前地址族（IPv4 或 IPv6）的地址。
	// 不包括 IPv6 链路本地地址和回环地址。
	GetAllLocalAddresses() (sets.Set[string], error)
	// GetLocalAddresses 返回指定接口上的所有本地地址。
	// 仅返回当前地址族（IPv4 或 IPv6）的地址。
	// 不包括 IPv6 链路本地地址和回环地址。
	GetLocalAddresses(dev string) (sets.Set[string], error)
	// GetAllLocalAddressesExcept 返回节点上除指定接口之外的所有本地地址。
	// 这不同于直接对 GetAllLocalAddresses 和 GetLocalAddresses 的结果做差集，
	// 因为同一个地址可能同时分配给多个接口。相关背景见
	// https://github.com/kubernetes/kubernetes/issues/114815
	GetAllLocalAddressesExcept(dev string) (sets.Set[string], error)
}
