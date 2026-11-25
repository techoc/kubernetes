/*
Copyright 2021 The Kubernetes Authors.

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

package driver

import (
	"net"

	"google.golang.org/grpc"
)

type MockCSIDriverServers struct {
	Controller *MockControllerServer
	Identity   *MockIdentityServer
	Node       *MockNodeServer
}

type MockCSIDriver struct {
	CSIDriver
	conn        *grpc.ClientConn
	interceptor grpc.UnaryServerInterceptor
}

// StartOnAddress 启动一个新的 gRPC server，监听指定地址。
func (m *MockCSIDriver) StartOnAddress(network, address string) error {
	l, err := net.Listen(network, address)
	if err != nil {
		return err
	}

	if err := m.CSIDriver.Start(l, m.interceptor); err != nil {
		l.Close()
		return err
	}

	return nil
}

// Start 启动一个新的 gRPC server，监听一个随机的 TCP 回环端口。
func (m *MockCSIDriver) Start() error {
	// 监听由 net 包分配的端口
	return m.StartOnAddress("tcp", "127.0.0.1:0")
}

func (m *MockCSIDriver) Close() {
	m.conn.Close()
	m.server.Stop()
}
