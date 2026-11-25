/*
Copyright 2016 The Kubernetes Authors.

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

package nat

/*
用于测试 iptables NAT 中 CLOSE_WAIT 超时情况的客户端/服务端。

client              server
  |                   |
  |<--tcp handshake-->|
  |<-------fin--------| 服务端半关闭
  |                   | 客户端处于 CLOSE_WAIT 状态
*/

import (
	"errors"
	"io"
	"log"
	"net"
	"time"

	"k8s.io/kubernetes/test/images/agnhost/net/common"
)

// leakedConnection 是一个全局变量，用于故意泄漏赋给它的活动连接。
//
//nolint:unused // U1000 intentional unused variable
var leakedConnection *net.TCPConn

// CloseWaitServerOptions 保存服务端的 JSON 选项。
type CloseWaitServerOptions struct {
	// 测试要绑定的地址
	LocalAddr string
	// 发送 FIN 之后要等待的超时时间。
	PostFinTimeoutSeconds int
}

type closeWaitServer struct {
	options *CloseWaitServerOptions
}

// NewCloseWaitServer 返回一个新的 Runner。
func NewCloseWaitServer() common.Runner {
	return &closeWaitServer{}
}

// NewOptions 分配新的 options 结构体。
func (server *closeWaitServer) NewOptions() interface{} {
	return &CloseWaitServerOptions{}
}

// Run 运行测试的服务端部分。
func (server *closeWaitServer) Run(logger *log.Logger, rawOptions interface{}) error {
	if options, ok := rawOptions.(*CloseWaitServerOptions); ok {
		server.options = options
	} else {
		return errors.New("invalid type")
	}

	logger.Printf("Run %v", server.options)

	addr, err := net.ResolveTCPAddr("tcp", server.options.LocalAddr)
	if err != nil {
		return err
	}

	listener, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()

	logger.Printf("Server listening on %v", addr)

	conn, err := listener.AcceptTCP()
	if err != nil {
		return err
	}
	defer conn.Close()

	logger.Printf("Client connected")

	// 向客户端发送半关闭的 FIN，使客户端进入 CLOSE_WAIT 状态。
	// 我们保持 client -> server 方向的管道开放，
	// 以验证 NAT 是否丢弃了我们的连接。
	if err := conn.CloseWrite(); err != nil {
		return err
	}

	logger.Printf("Server sent FIN, waiting %v seconds",
		server.options.PostFinTimeoutSeconds)

	<-time.After(time.Duration(server.options.PostFinTimeoutSeconds) * time.Second)

	logger.Printf("Done")

	return nil
}

// CloseWaitClientOptions 保存客户端的 JSON 选项。
type CloseWaitClientOptions struct {
	// 要连接的服务端的 RemoteAddr。
	RemoteAddr string
	// 与服务端进行 I/O 的超时秒数。
	TimeoutSeconds int
	// 半关闭后的等待时间（给测试留出检查 conntrack 表项状态的时间）。
	PostFinTimeoutSeconds int
	// 是否泄漏连接（赋值给全局变量，使连接在进程存活期间一直保持）。
	LeakConnection bool
}

type closeWaitClient struct {
	options *CloseWaitClientOptions
}

// NewCloseWaitClient 创建一个新的 runner
func NewCloseWaitClient() common.Runner {
	return &closeWaitClient{}
}

// NewOptions 分配新的 options 结构体。
func (client *closeWaitClient) NewOptions() interface{} {
	return &CloseWaitClientOptions{}
}

// Run 运行客户端。
func (client *closeWaitClient) Run(logger *log.Logger, rawOptions interface{}) error {
	if options, ok := rawOptions.(*CloseWaitClientOptions); ok {
		client.options = options
	} else {
		return errors.New("invalid type")
	}

	logger.Printf("Run %v", client.options)

	addr, err := net.ResolveTCPAddr("tcp", client.options.RemoteAddr)
	if err != nil {
		return err
	}

	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return err
	}
	if !client.options.LeakConnection {
		defer conn.Close()
	}

	logger.Printf("Connected to server")

	if client.options.TimeoutSeconds > 0 {
		delay := time.Duration(client.options.TimeoutSeconds) * time.Second
		conn.SetReadDeadline(time.Now().Add(delay))
	}

	buf := make([]byte, 1, 1)
	size, err := conn.Read(buf)

	if err != nil && err != io.EOF {
		return err
	}

	if size != 0 {
		return errors.New("Got data but expected EOF")
	}

	logger.Printf("Server has half-closed the connection, waiting %v seconds",
		client.options.PostFinTimeoutSeconds)

	if client.options.LeakConnection {
		logger.Printf("Leaking client connection (assigning to global variable)")
		leakedConnection = conn
	}

	<-time.After(
		time.Duration(client.options.PostFinTimeoutSeconds) * time.Second)

	logger.Printf("Done")

	return nil
}
