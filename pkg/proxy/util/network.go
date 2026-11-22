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

package util

import (
	"net"
)

// NetworkInterfacer 封装若干 net 标准库函数。生产代码直接转发给标准库，
// 单元测试则替换该实现来模拟网络接口状态，避免依赖测试机器真实的网络配置。
type NetworkInterfacer interface {
	InterfaceAddrs() ([]net.Addr, error)
}

// RealNetwork 是 NetworkInterfacer 的生产实现，仅对底层 net 库调用做一层薄封装。
type RealNetwork struct{}

// InterfaceAddrs 转发到 net.InterfaceAddrs()，是 NetworkInterfacer 接口的一部分。
func (RealNetwork) InterfaceAddrs() ([]net.Addr, error) {
	return net.InterfaceAddrs()
}

// 编译期断言：确保生产实现不会遗漏接口方法，从而能随时替换测试桩。
var _ NetworkInterfacer = &RealNetwork{}
