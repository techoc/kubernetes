/*
Copyright 2015 The Kubernetes Authors.

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

// Package tcp 实现 TCP Socket 探针，用于检查容器是否接受 TCP 连接。
//
// TCP 探针尝试与容器的指定地址建立 TCP 连接：
//   - 连接成功：返回 Success
//   - 连接失败或超时：返回 Failure
//
// 使用 ProbeDialer 减少 TIME-WAIT 状态的 socket 残留。
package tcp

import (
	"net"
	"strconv"
	"time"

	"k8s.io/kubernetes/pkg/probe"

	"k8s.io/klog/v2"
)

// New 创建 TCP 探针。
func New() Prober {
	return tcpProber{}
}

// Prober 是 TCP 探针接口，定义 TCP 就绪/存活检查的方法。
type Prober interface {
	// Probe 检查是否能与指定地址建立 TCP 连接
	Probe(host string, port int, timeout time.Duration) (probe.Result, string, error)
}

type tcpProber struct{}

// Probe 检查是否能与指定地址建立 TCP 连接。
func (pr tcpProber) Probe(host string, port int, timeout time.Duration) (probe.Result, string, error) {
	return DoTCPProbe(net.JoinHostPort(host, strconv.Itoa(port)), timeout)
}

// DoTCPProbe 检查是否能与指定地址建立 TCP 连接。
//
//   - 连接成功：返回 Success
//   - 连接失败（包括超时）：返回 Failure
//
// 导出此函数以便其他包直接执行 TCP 探针。
func DoTCPProbe(addr string, timeout time.Duration) (probe.Result, string, error) {
	d := probe.ProbeDialer()
	d.Timeout = timeout
	conn, err := d.Dial("tcp", addr)
	if err != nil {
		// 将错误转换为 Failure，包括超时在内的所有连接错误都视为探针失败
		return probe.Failure, err.Error(), nil
	}
	// 连接成功后立即关闭，探针只检查连接能力，不传输数据
	err = conn.Close()
	if err != nil {
		// 关闭连接时出错是意外情况，记录错误日志
		klog.Errorf("Unexpected error closing TCP probe socket: %v (%#v)", err, err)
	}
	return probe.Success, "", nil
}
