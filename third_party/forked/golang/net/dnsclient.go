// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// 本包从 Go 标准库 net 复制而来。
// https://golang.org/src/net/dnsclient.go
// 原私有函数 reverseaddr 在此导出为公开函数。

package net

import "net"

// Reverseaddr 返回适用于 rDNS（PTR）查询的 in-addr.arpa. 或 ip6.arpa. 主机名；
// 解析 IP 地址失败时返回错误。
func Reverseaddr(addr string) (arpa string, err error) {
	ip := net.ParseIP(addr)
	if ip == nil {
		return "", &net.DNSError{Err: "unrecognized address", Name: addr}
	}
	if ip.To4() != nil {
		return uitoa(uint(ip[15])) + "." + uitoa(uint(ip[14])) + "." + uitoa(uint(ip[13])) + "." + uitoa(uint(ip[12])) + ".in-addr.arpa.", nil
	}
	// 能走到这里说明是 IPv6
	buf := make([]byte, 0, len(ip)*4+len("ip6.arpa."))
	// 逆序写入缓冲区
	for i := len(ip) - 1; i >= 0; i-- {
		v := ip[i]
		buf = append(buf, hexDigit[v&0xF],
			'.',
			hexDigit[v>>4],
			'.')
	}
	// 补上 "ip6.arpa." 后返回（buf 末尾已有 "."）
	buf = append(buf, "ip6.arpa."...)
	return string(buf), nil
}

// uitoa 将无符号整数转为十进制字符串。
func uitoa(val uint) string {
	if val == 0 { // 避免分配字符串
		return "0"
	}
	var buf [20]byte // 足以容纳 64 位十进制值
	i := len(buf) - 1
	for val >= 10 {
		q := val / 10
		buf[i] = byte('0' + val - q*10)
		i--
		val = q
	}
	// 此时 val < 10
	buf[i] = byte('0' + val)
	return string(buf[i:])
}

const hexDigit = "0123456789abcdef"
