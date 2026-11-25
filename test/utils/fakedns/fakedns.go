/*
Copyright 2025 The Kubernetes Authors.

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

package fakedns

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	netutils "k8s.io/utils/net"

	"golang.org/x/net/dns/dnsmessage"

	nettesting "k8s.io/kubernetes/third_party/forked/golang/net/testing"
)

// dnsHijackLock 是一个互斥锁，用于确保同一时间只有一个测试能劫持 DNS。
// 这是必要的，因为 net.DefaultResolver 是一个全局变量。
var dnsHijackLock sync.Mutex

// Server 是一个 fake DNS 服务器，可用于在测试中覆盖 DNS 解析。
// 它对于测试依赖 DNS 的组件特别有用，而无需搭建真正的 DNS 服务器。
// 可以用域名到 IP 地址的映射来配置该服务器，它会据此响应 DNS 查询。
// 要使用这个 fake DNS 服务器，先用 NewServer 创建实例，
// 然后调用 Hijack 将 DNS 查询重定向到该 fake 服务器。
// 该辅助工具不支持并行运行测试。
type Server struct {
	dnsServer nettesting.FakeDNSServer
}

// NewServer 创建一个新的 fake DNS 服务器。hosts map 应以域名为键，
// 以对应的 IP 地址字符串为值。
// 如果任何一个 IP 地址无效，则返回错误。
func NewServer(hosts map[string]string) (*Server, error) {
	parsedHosts := make(map[string]net.IP, len(hosts))
	for domain, ipStr := range hosts {
		ip := netutils.ParseIPSloppy(ipStr)
		if ip == nil {
			return nil, fmt.Errorf("invalid IP address for domain %s: %s", domain, ipStr)
		}
		// 确保域名是干净的，作为 map 键时没有结尾的点号
		parsedHosts[strings.TrimSuffix(domain, ".")] = ip
	}
	dnsServer := nettesting.FakeDNSServer{}
	dnsServer.ResponseHandler = func(n, s string, q dnsmessage.Message, t time.Time) (dnsmessage.Message, error) {
		resp := dnsmessage.Message{
			Header: dnsmessage.Header{
				ID:            q.Header.ID,
				Response:      true,
				Authoritative: true,
			},
			Questions: q.Questions,
		}

		if len(q.Questions) == 0 {
			return resp, nil
		}

		question := q.Questions[0]
		domain := strings.TrimSuffix(question.Name.String(), ".")

		if ip, ok := parsedHosts[domain]; ok {
			var resource dnsmessage.Resource
			if ipv4 := ip.To4(); ipv4 != nil {
				resource = dnsmessage.Resource{
					Header: dnsmessage.ResourceHeader{Name: question.Name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET, TTL: 300},
					Body:   &dnsmessage.AResource{A: [4]byte(ipv4)},
				}
			} else { // 假定为 IPv6
				resource = dnsmessage.Resource{
					Header: dnsmessage.ResourceHeader{Name: question.Name, Type: dnsmessage.TypeAAAA, Class: dnsmessage.ClassINET, TTL: 300},
					Body:   &dnsmessage.AAAAResource{AAAA: [16]byte(ip)},
				}
			}
			resp.Answers = []dnsmessage.Resource{resource}
		}

		return resp, nil
	}

	return &Server{
		dnsServer: dnsServer,
	}, nil
}

// Hijack 接管 net.DefaultResolver，将 DNS 查询重定向到 fake 服务器。
// 它使用 t.Cleanup 在测试结束后自动恢复原来的 resolver。
// 该辅助工具不支持并行运行测试。
//
// 示例：
//
//	dnsServer, _ := fakedns.NewServer(hosts)
//	dnsServer.Hijack(t)
func (s *Server) Hijack(t *testing.T) {
	t.Helper()

	dnsHijackLock.Lock()

	originalDial := net.DefaultResolver.Dial
	originalPreferGo := net.DefaultResolver.PreferGo

	net.DefaultResolver.PreferGo = true
	net.DefaultResolver.Dial = s.dnsServer.DialContext

	t.Cleanup(func() {
		net.DefaultResolver.Dial = originalDial
		net.DefaultResolver.PreferGo = originalPreferGo
		dnsHijackLock.Unlock()
	})
}
