//go:build windows

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

package fake

import (
	"fmt"
	"net"
)

// GenerateEndpoint 返回一个新的 TCP 端点。与 Unix 平台不同，这里没有固定路径模板可用，
// 因此先监听 localhost:0 让内核分配一个空闲端口，再据此拼出 tcp:// 地址。
func GenerateEndpoint() (string, error) {
	// 解析 localhost:0，端口 0 表示由内核自动挑选可用端口。
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
	if err != nil {
		// 注意：解析失败时返回空字符串且错误为 nil，与常规错误传递方式不同。
		return "", nil
	}

	// 真正监听该地址，目的是拿到内核分配的具体端口号。
	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return "", err
	}

	// 端点字符串只需要端口，监听器用完后立即关闭以释放端口。
	defer l.Close()
	// 从监听器地址中断言出 TCPAddr 并取出端口，拼成 gRPC 使用的 tcp:// 端点。
	return fmt.Sprintf("tcp://127.0.0.1:%d", l.Addr().(*net.TCPAddr).Port), nil
}
