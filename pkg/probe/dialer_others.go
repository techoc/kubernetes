//go:build !windows

/*
Copyright 2023 The Kubernetes Authors.

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

package probe

import (
	"net"
	"syscall"
)

// ProbeDialer 返回一个为探针优化的 TCP 拨号器，避免 socket 长时间停留在 TIME-WAIT 状态。
//
// 问题背景：高频探针（如每1秒一次）会产生大量短连接，默认 OS 的 TIME-WAIT 周期为60秒，
// 导致端口耗尽（"port exhaustion"）。此拨号器将 SO_LINGER 设置为1秒，显著减少端口占用时间。
//
// 为什么用1秒而不是0秒：SO_LINGER 设为0会导致待发送数据被丢弃，连接以 RST 包强制终止，
// 而不是正常发送完数据后用 FIN 包优雅关闭。1秒是一个折中值，既减少了 TIME-WAIT 时间，
// 又保证了数据能完整传输。
//
// Ref: https://issues.k8s.io/89898
func ProbeDialer() *net.Dialer {
	dialer := &net.Dialer{
		// Control 函数在连接建立前执行，用于设置 socket 选项
		Control: func(network, address string, c syscall.RawConn) error {
			return c.Control(func(fd uintptr) {
				// 设置 SO_LINGER：启用 linger，超时1秒
				// Onoff=1 表示启用，Linger=1 表示等待1秒后强制关闭
				syscall.SetsockoptLinger(int(fd), syscall.SOL_SOCKET, syscall.SO_LINGER, &syscall.Linger{Onoff: 1, Linger: 1})
			})
		},
	}
	return dialer
}
