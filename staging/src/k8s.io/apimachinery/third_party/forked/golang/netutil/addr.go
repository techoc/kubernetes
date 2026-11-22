// Package netutil 是从 Go 标准库 net/http 分叉（fork）而来的网络地址工具，
// 提供 CanonicalAddr 等辅助函数；保留分叉是为了在不依赖标准库内部实现的前提下复用其行为。
package netutil

import (
	"net/url"
	"strings"
)

// FROM: http://golang.org/src/net/http/client.go
// 给定形如 "host"、"host:port" 或 "[ipv6::address]:port" 的字符串，
// 判断其是否包含端口。
// 通过比较最后一个 ':' 与最后一个 ']' 的位置，正确处理 IPv6 字面量中的冒号。
func hasPort(s string) bool { return strings.LastIndex(s, ":") > strings.LastIndex(s, "]") }

// FROM: http://golang.org/src/net/http/transport.go
// portMap 记录各 scheme 的默认端口，仅在地址未显式给出端口时使用。
var portMap = map[string]string{
	"http":   "80",
	"https":  "443",
	"socks5": "1080",
}

// FROM: http://golang.org/src/net/http/transport.go
// canonicalAddr 返回 url.Host，并始终补上 ":port" 后缀。
// 若地址已含端口则原样返回；未知 scheme 会得到空端口（":")，这是标准库的既有行为。
func CanonicalAddr(url *url.URL) string {
	addr := url.Host
	if !hasPort(addr) {
		return addr + ":" + portMap[url.Scheme]
	}
	return addr
}
