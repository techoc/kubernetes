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

package net

import (
	"errors"
	"net"
	"reflect"
	"strings"
	"syscall"
)

// IPNetEqual 检查两个 IPNet 是否表示同一子网。
// 例如，
//
// 10.0.0.1/24 与 10.0.0.0/24 是同一子网。
// 10.0.0.1/24 与 10.0.0.0/25 不是同一子网。
//
// 不能只比较 IP 字段（主机位可能不同），也不能只比较 mask，必须
// 互相 Contains 才能确认二者覆盖同一网段。
func IPNetEqual(ipnet1, ipnet2 *net.IPNet) bool {
	if ipnet1 == nil || ipnet2 == nil {
		return false
	}
	if reflect.DeepEqual(ipnet1.Mask, ipnet2.Mask) && ipnet1.Contains(ipnet2.IP) && ipnet2.Contains(ipnet1.IP) {
		return true
	}
	return false
}

// 如果给定 err 是 "connection reset by peer" 错误则返回 true。
// 用 errors.As 而非字符串匹配，以兼容包装过的错误。
func IsConnectionReset(err error) bool {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno == syscall.ECONNRESET
	}
	return false
}

// 如果给定 err 是 "http2: client connection lost" 错误则返回 true。
// HTTP/2 的这个错误没有导出类型/哨兵值，只能退化为字符串匹配。
func IsHTTP2ConnectionLost(err error) bool {
	return err != nil && strings.Contains(err.Error(), "http2: client connection lost")
}

// 如果给定 err 是 "connection refused" 错误则返回 true。
func IsConnectionRefused(err error) bool {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno == syscall.ECONNREFUSED
	}
	return false
}
