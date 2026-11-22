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

package discovery

import (
	"net/http"
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	netutils "k8s.io/utils/net"
)

// TestGetServerAddressByClientCIDRs 验证 /api 与 /apis 返回的 serverAddressByClientCIDRs
// 会依据客户端 IP 命中的 CIDR 规则选择服务端地址；客户端 IP 依次从
// X-Real-Ip、X-Forwarded-For、RemoteAddr 中获取，无法解析时回退到默认地址。
func TestGetServerAddressByClientCIDRs(t *testing.T) {
	// 默认规则覆盖所有地址；internal 规则额外匹配 10.0.0.0/24。
	publicAddressCIDRMap := []metav1.ServerAddressByClientCIDR{
		{
			ClientCIDR:    "0.0.0.0/0",
			ServerAddress: "ExternalAddress",
		},
	}
	internalAddressCIDRMap := []metav1.ServerAddressByClientCIDR{
		publicAddressCIDRMap[0],
		{
			ClientCIDR:    "10.0.0.0/24",
			ServerAddress: "serviceIP",
		},
	}
	internalIP := "10.0.0.1"
	publicIP := "1.1.1.1"
	testCases := []struct {
		Request     http.Request
		ExpectedMap []metav1.ServerAddressByClientCIDR
	}{
		{
			// 没有任何客户端 IP 信息时使用默认地址
			Request:     http.Request{},
			ExpectedMap: publicAddressCIDRMap,
		},
		{
			// 内网 IP 命中 10.0.0.0/24 规则
			Request: http.Request{
				Header: map[string][]string{
					"X-Real-Ip": {internalIP},
				},
			},
			ExpectedMap: internalAddressCIDRMap,
		},
		{
			// 公网 IP 只命中 0.0.0.0/0
			Request: http.Request{
				Header: map[string][]string{
					"X-Real-Ip": {publicIP},
				},
			},
			ExpectedMap: publicAddressCIDRMap,
		},
		{
			// X-Forwarded-For 与 X-Real-Ip 行为一致
			Request: http.Request{
				Header: map[string][]string{
					"X-Forwarded-For": {internalIP},
				},
			},
			ExpectedMap: internalAddressCIDRMap,
		},
		{
			Request: http.Request{
				Header: map[string][]string{
					"X-Forwarded-For": {publicIP},
				},
			},
			ExpectedMap: publicAddressCIDRMap,
		},

		{
			// 无代理头时退回 RemoteAddr
			Request: http.Request{
				RemoteAddr: internalIP,
			},
			ExpectedMap: internalAddressCIDRMap,
		},
		{
			Request: http.Request{
				RemoteAddr: publicIP,
			},
			ExpectedMap: publicAddressCIDRMap,
		},
		{
			// 无法解析的地址不能命中内网规则，只能回退到默认地址
			Request: http.Request{
				RemoteAddr: "invalidIP",
			},
			ExpectedMap: publicAddressCIDRMap,
		},
	}

	_, ipRange, _ := netutils.ParseCIDRSloppy("10.0.0.0/24")
	// DefaultAddresses 的默认地址用于不命中任何 CIDR 规则的客户端。
	discoveryAddresses := DefaultAddresses{DefaultAddress: "ExternalAddress"}
	discoveryAddresses.CIDRRules = append(discoveryAddresses.CIDRRules,
		CIDRRule{IPRange: *ipRange, Address: "serviceIP"})

	for i, test := range testCases {
		// 用 GetClientIP 从请求中提取客户端 IP，再交给 ServerAddressByClientCIDRs 匹配。
		if a, e := discoveryAddresses.ServerAddressByClientCIDRs(utilnet.GetClientIP(&test.Request)), test.ExpectedMap; reflect.DeepEqual(e, a) != true {
			t.Fatalf("test case %d failed. expected: %v, actual: %v", i+1, e, a)
		}
	}
}
