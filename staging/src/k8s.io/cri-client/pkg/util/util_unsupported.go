//go:build !freebsd && !linux && !windows && !darwin

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

package util

import (
	"context"
	"fmt"
	"net"
)

// CreateListener creates a listener on the specified endpoint.
// CreateListener 在不支持的平台上没有实现：本文件通过 //go:build 约束只在
// 非 freebsd/linux/windows/darwin 的平台上编译，此时直接返回"不支持"错误。
// 这样 cri-client 在这些平台上仍能编译通过，只是无法建立本地运行时连接。
func CreateListener(endpoint string) (net.Listener, error) {
	return nil, fmt.Errorf("CreateListener is unsupported in this build")
}

// GetAddressAndDialer returns the address parsed from the given endpoint and a context dialer.
// GetAddressAndDialer 同样是不支持平台上的占位实现，直接返回错误。
// 它保证 util 包在所有平台上都有相同的 API 签名，调用方无需写构建标签做条件编译。
func GetAddressAndDialer(endpoint string) (string, func(ctx context.Context, addr string) (net.Conn, error), error) {
	return "", nil, fmt.Errorf("GetAddressAndDialer is unsupported in this build")
}
