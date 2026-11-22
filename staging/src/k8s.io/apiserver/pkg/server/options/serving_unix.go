//go:build !windows

/*
Copyright 2020 The Kubernetes Authors.

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

package options

import (
	"syscall"

	"golang.org/x/sys/unix"

	"k8s.io/klog/v2"
)

// permitPortReuse 在建立 socket 前设置 SO_REUSEPORT，允许多个 apiserver 实例监听同一端口，
// 由内核做连接分发（例如滚动升级期间的零停机）。
func permitPortReuse(network, addr string, conn syscall.RawConn) error {
	return conn.Control(func(fd uintptr) {
		if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, unix.SO_REUSEPORT, 1); err != nil {
			// 设置失败只告警：可能是不支持该选项的平台，此时退化为独占端口。
			klog.Warningf("failed to set SO_REUSEPORT on socket: %v", err)
		}
	})
}

// permitAddressReuse 设置 SO_REUSEADDR，使重启时能立即重用仍处于 TIME_WAIT 的地址，
// 避免因端口残留而启动失败。
func permitAddressReuse(network, addr string, conn syscall.RawConn) error {
	return conn.Control(func(fd uintptr) {
		if err := syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
			klog.Warningf("failed to set SO_REUSEADDR on socket: %v", err)
		}
	})
}
