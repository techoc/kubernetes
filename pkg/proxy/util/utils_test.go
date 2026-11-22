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
	"net"
	"reflect"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	netutils "k8s.io/utils/net"
)

// TestShouldSkipService 验证 ShouldSkipService 在 ClusterIP 为 None、为空或服务类型为
// ExternalName 时判定跳过代理，而对已分配 ClusterIP 的其他类型服务不跳过。
func TestShouldSkipService(t *testing.T) {
	testCases := []struct {
		service    *v1.Service
		shouldSkip bool
	}{
		{
			// ClusterIP 为 None，headless 服务没有可代理的 IP
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{Namespace: "foo", Name: "bar"},
				Spec: v1.ServiceSpec{
					ClusterIP: v1.ClusterIPNone,
				},
			},
			shouldSkip: true,
		},
		{
			// ClusterIP 为空，尚未分配，不能建立代理规则
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{Namespace: "foo", Name: "bar"},
				Spec: v1.ServiceSpec{
					ClusterIP: "",
				},
			},
			shouldSkip: true,
		},
		{
			// ExternalName 类型服务由 DNS CNAME 解析，不走代理转发
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{Namespace: "foo", Name: "bar"},
				Spec: v1.ServiceSpec{
					ClusterIP: "1.2.3.4",
					Type:      v1.ServiceTypeExternalName,
				},
			},
			shouldSkip: true,
		},
		{
			// ClusterIP 类型且已分配 ClusterIP，应正常代理
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{Namespace: "foo", Name: "bar"},
				Spec: v1.ServiceSpec{
					ClusterIP: "1.2.3.4",
					Type:      v1.ServiceTypeClusterIP,
				},
			},
			shouldSkip: false,
		},
		{
			// NodePort 类型且已分配 ClusterIP，应正常代理
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{Namespace: "foo", Name: "bar"},
				Spec: v1.ServiceSpec{
					ClusterIP: "1.2.3.4",
					Type:      v1.ServiceTypeNodePort,
				},
			},
			shouldSkip: false,
		},
		{
			// LoadBalancer 类型且已分配 ClusterIP，应正常代理
			service: &v1.Service{
				ObjectMeta: metav1.ObjectMeta{Namespace: "foo", Name: "bar"},
				Spec: v1.ServiceSpec{
					ClusterIP: "1.2.3.4",
					Type:      v1.ServiceTypeLoadBalancer,
				},
			},
			shouldSkip: false,
		},
	}

	// 表驱动同时覆盖 skip=true 与 skip=false，避免只验证跳过分支。
	for i := range testCases {
		skip := ShouldSkipService(testCases[i].service)
		if skip != testCases[i].shouldSkip {
			t.Errorf("case %d: expect %v, got %v", i, testCases[i].shouldSkip, skip)
		}
	}
}

// TestAppendPortIfNeeded 验证 AppendPortIfNeeded 只给尚未携带端口的可解析 IP 追加端口：
// 已带端口的地址和非法地址原样返回，IPv6 地址需用方括号包裹。
func TestAppendPortIfNeeded(t *testing.T) {
	testCases := []struct {
		name   string
		addr   string
		port   int32
		expect string
	}{
		{
			name:   "IPv4 all-zeros bind address has port",
			addr:   "0.0.0.0:12345",
			port:   23456,
			expect: "0.0.0.0:12345",
		},
		{
			name:   "non-zeros IPv4 config",
			addr:   "9.8.7.6",
			port:   12345,
			expect: "9.8.7.6:12345",
		},
		{
			name:   "IPv6 \"[::]\" bind address has port",
			addr:   "[::]:12345",
			port:   23456,
			expect: "[::]:12345",
		},
		{
			name:   "IPv6 config",
			addr:   "fd00:1::5",
			port:   23456,
			expect: "[fd00:1::5]:23456",
		},
		{
			name:   "Invalid IPv6 Config",
			addr:   "[fd00:1::5]",
			port:   12345,
			expect: "[fd00:1::5]",
		},
	}

	// 已带端口的地址必须保持不变，否则会重复追加端口产生非法地址。
	for i := range testCases {
		got := AppendPortIfNeeded(testCases[i].addr, testCases[i].port)
		if testCases[i].expect != got {
			t.Errorf("case %s: expected %v, got %v", testCases[i].name, testCases[i].expect, got)
		}
	}
}

// TestMapIPsByIPFamily 验证 MapIPsByIPFamily 将 IP 字符串按 IPv4/IPv6 分别归类，
// 并忽略无法解析的输入。
func TestMapIPsByIPFamily(t *testing.T) {
	testCases := []struct {
		desc            string
		ipString        []string
		wantIPv6        bool
		expectCorrect   []string
		expectIncorrect []string
	}{
		{
			desc:            "empty input IPv4",
			ipString:        []string{},
			wantIPv6:        false,
			expectCorrect:   nil,
			expectIncorrect: nil,
		},
		{
			desc:            "empty input IPv6",
			ipString:        []string{},
			wantIPv6:        true,
			expectCorrect:   nil,
			expectIncorrect: nil,
		},
		{
			desc:            "want IPv4 and receive IPv6",
			ipString:        []string{"fd00:20::1"},
			wantIPv6:        false,
			expectCorrect:   nil,
			expectIncorrect: []string{"fd00:20::1"},
		},
		{
			desc:            "want IPv6 and receive IPv4",
			ipString:        []string{"192.168.200.2"},
			wantIPv6:        true,
			expectCorrect:   nil,
			expectIncorrect: []string{"192.168.200.2"},
		},
		{
			desc:            "want IPv6 and receive IPv4 and IPv6",
			ipString:        []string{"192.168.200.2", "192.1.34.23", "fd00:20::1", "2001:db9::3"},
			wantIPv6:        true,
			expectCorrect:   []string{"fd00:20::1", "2001:db9::3"},
			expectIncorrect: []string{"192.168.200.2", "192.1.34.23"},
		},
		{
			desc:            "want IPv4 and receive IPv4 and IPv6",
			ipString:        []string{"192.168.200.2", "192.1.34.23", "fd00:20::1", "2001:db9::3"},
			wantIPv6:        false,
			expectCorrect:   []string{"192.168.200.2", "192.1.34.23"},
			expectIncorrect: []string{"fd00:20::1", "2001:db9::3"},
		},
		{
			desc:            "want IPv4 and receive IPv4 only",
			ipString:        []string{"192.168.200.2", "192.1.34.23"},
			wantIPv6:        false,
			expectCorrect:   []string{"192.168.200.2", "192.1.34.23"},
			expectIncorrect: nil,
		},
		{
			desc:            "want IPv6 and receive IPv4 only",
			ipString:        []string{"192.168.200.2", "192.1.34.23"},
			wantIPv6:        true,
			expectCorrect:   nil,
			expectIncorrect: []string{"192.168.200.2", "192.1.34.23"},
		},
		{
			desc:            "want IPv4 and receive IPv6 only",
			ipString:        []string{"fd00:20::1", "2001:db9::3"},
			wantIPv6:        false,
			expectCorrect:   nil,
			expectIncorrect: []string{"fd00:20::1", "2001:db9::3"},
		},
		{
			desc:            "want IPv6 and receive IPv6 only",
			ipString:        []string{"fd00:20::1", "2001:db9::3"},
			wantIPv6:        true,
			expectCorrect:   []string{"fd00:20::1", "2001:db9::3"},
			expectIncorrect: nil,
		},
	}

	for _, testcase := range testCases {
		t.Run(testcase.desc, func(t *testing.T) {
			ipFamily := v1.IPv4Protocol
			otherIPFamily := v1.IPv6Protocol

			if testcase.wantIPv6 {
				ipFamily = v1.IPv6Protocol
				otherIPFamily = v1.IPv4Protocol
			}

			// 同一组输入分别从两个地址族的视角检查，错分的 IP 也能被发现。
			ipMap := MapIPsByIPFamily(testcase.ipString)

			var ipStr []string
			for _, ip := range ipMap[ipFamily] {
				ipStr = append(ipStr, ip.String())
			}
			if !reflect.DeepEqual(testcase.expectCorrect, ipStr) {
				t.Errorf("Test %v failed: expected %v, got %v", testcase.desc, testcase.expectCorrect, ipMap[ipFamily])
			}
			// 复用切片变量前清空，避免上一轮分类结果混入本轮断言。
			ipStr = nil
			for _, ip := range ipMap[otherIPFamily] {
				ipStr = append(ipStr, ip.String())
			}
			if !reflect.DeepEqual(testcase.expectIncorrect, ipStr) {
				t.Errorf("Test %v failed: expected %v, got %v", testcase.desc, testcase.expectIncorrect, ipMap[otherIPFamily])
			}
		})
	}
}

// TestMapCIDRsByIPFamily 验证 MapCIDRsByIPFamily 将 CIDR 字符串按 IPv4/IPv6 分别归类，
// 并忽略无法解析的输入。
func TestMapCIDRsByIPFamily(t *testing.T) {
	testCases := []struct {
		desc            string
		ipString        []string
		wantIPv6        bool
		expectCorrect   []string
		expectIncorrect []string
	}{
		{
			desc:            "empty input IPv4",
			ipString:        []string{},
			wantIPv6:        false,
			expectCorrect:   nil,
			expectIncorrect: nil,
		},
		{
			desc:            "empty input IPv6",
			ipString:        []string{},
			wantIPv6:        true,
			expectCorrect:   nil,
			expectIncorrect: nil,
		},
		{
			desc:            "want IPv4 and receive IPv6",
			ipString:        []string{"fd00:20::/64"},
			wantIPv6:        false,
			expectCorrect:   nil,
			expectIncorrect: []string{"fd00:20::/64"},
		},
		{
			desc:            "want IPv6 and receive IPv4",
			ipString:        []string{"192.168.200.0/24"},
			wantIPv6:        true,
			expectCorrect:   nil,
			expectIncorrect: []string{"192.168.200.0/24"},
		},
		{
			desc:            "want IPv6 and receive IPv4 and IPv6",
			ipString:        []string{"192.168.200.0/24", "192.1.34.0/24", "fd00:20::/64", "2001:db9::/64"},
			wantIPv6:        true,
			expectCorrect:   []string{"fd00:20::/64", "2001:db9::/64"},
			expectIncorrect: []string{"192.168.200.0/24", "192.1.34.0/24"},
		},
		{
			desc:            "want IPv4 and receive IPv4 and IPv6",
			ipString:        []string{"192.168.200.0/24", "192.1.34.0/24", "fd00:20::/64", "2001:db9::/64"},
			wantIPv6:        false,
			expectCorrect:   []string{"192.168.200.0/24", "192.1.34.0/24"},
			expectIncorrect: []string{"fd00:20::/64", "2001:db9::/64"},
		},
		{
			desc:            "want IPv4 and receive IPv4 only",
			ipString:        []string{"192.168.200.0/24", "192.1.34.0/24"},
			wantIPv6:        false,
			expectCorrect:   []string{"192.168.200.0/24", "192.1.34.0/24"},
			expectIncorrect: nil,
		},
		{
			desc:            "want IPv6 and receive IPv4 only",
			ipString:        []string{"192.168.200.0/24", "192.1.34.0/24"},
			wantIPv6:        true,
			expectCorrect:   nil,
			expectIncorrect: []string{"192.168.200.0/24", "192.1.34.0/24"},
		},
		{
			desc:            "want IPv4 and receive IPv6 only",
			ipString:        []string{"fd00:20::/64", "2001:db9::/64"},
			wantIPv6:        false,
			expectCorrect:   nil,
			expectIncorrect: []string{"fd00:20::/64", "2001:db9::/64"},
		},
		{
			desc:            "want IPv6 and receive IPv6 only",
			ipString:        []string{"fd00:20::/64", "2001:db9::/64"},
			wantIPv6:        true,
			expectCorrect:   []string{"fd00:20::/64", "2001:db9::/64"},
			expectIncorrect: nil,
		},
	}

	for _, testcase := range testCases {
		t.Run(testcase.desc, func(t *testing.T) {
			ipFamily := v1.IPv4Protocol
			otherIPFamily := v1.IPv6Protocol

			if testcase.wantIPv6 {
				ipFamily = v1.IPv6Protocol
				otherIPFamily = v1.IPv4Protocol
			}

			cidrMap := MapCIDRsByIPFamily(testcase.ipString)

			var cidrStr []string
			for _, cidr := range cidrMap[ipFamily] {
				cidrStr = append(cidrStr, cidr.String())
			}
			// 同时收集目标族和其他族的 CIDR，既能验证正确分组也能发现错分。
			var cidrStrOther []string
			for _, cidr := range cidrMap[otherIPFamily] {
				cidrStrOther = append(cidrStrOther, cidr.String())
			}

			if !reflect.DeepEqual(testcase.expectCorrect, cidrStr) {
				t.Errorf("Test %v failed: expected %v, got %v", testcase.desc, testcase.expectCorrect, cidrStr)
			}
			if !reflect.DeepEqual(testcase.expectIncorrect, cidrStrOther) {
				t.Errorf("Test %v failed: expected %v, got %v", testcase.desc, testcase.expectIncorrect, cidrStrOther)
			}
		})
	}
}

// TestGetClusterIPByFamily 验证 GetClusterIPByFamily 从旧式单 ClusterIP 字段、单栈及双栈
// ClusterIPs/IPFamilies 中按地址族取出 ClusterIP；族不匹配时返回空串。
func TestGetClusterIPByFamily(t *testing.T) {
	testCases := []struct {
		name           string
		service        v1.Service
		requestFamily  v1.IPFamily
		expectedResult string
	}{
		{
			name:           "old style service ipv4. want ipv4",
			requestFamily:  v1.IPv4Protocol,
			expectedResult: "10.0.0.10",
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIP: "10.0.0.10",
				},
			},
		},

		{
			name:           "old style service ipv4. want ipv6",
			requestFamily:  v1.IPv6Protocol,
			expectedResult: "",
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIP: "10.0.0.10",
				},
			},
		},

		{
			name:           "old style service ipv6. want ipv6",
			requestFamily:  v1.IPv6Protocol,
			expectedResult: "2000::1",
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIP: "2000::1",
				},
			},
		},

		{
			name:           "old style service ipv6. want ipv4",
			requestFamily:  v1.IPv4Protocol,
			expectedResult: "",
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIP: "2000::1",
				},
			},
		},

		{
			name:           "service single stack ipv4. want ipv4",
			requestFamily:  v1.IPv4Protocol,
			expectedResult: "10.0.0.10",
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIPs: []string{"10.0.0.10"},
					IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
				},
			},
		},

		{
			name:           "service single stack ipv4. want ipv6",
			requestFamily:  v1.IPv6Protocol,
			expectedResult: "",
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIPs: []string{"10.0.0.10"},
					IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
				},
			},
		},

		{
			name:           "service single stack ipv6. want ipv6",
			requestFamily:  v1.IPv6Protocol,
			expectedResult: "2000::1",
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIPs: []string{"2000::1"},
					IPFamilies: []v1.IPFamily{v1.IPv6Protocol},
				},
			},
		},

		{
			name:           "service single stack ipv6. want ipv4",
			requestFamily:  v1.IPv4Protocol,
			expectedResult: "",
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIPs: []string{"2000::1"},
					IPFamilies: []v1.IPFamily{v1.IPv6Protocol},
				},
			},
		},
		// 双栈服务：IPFamilies 与 ClusterIPs 按下标一一对应
		{
			name:           "service dual stack ipv4,6. want ipv4",
			requestFamily:  v1.IPv4Protocol,
			expectedResult: "10.0.0.10",
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIPs: []string{"10.0.0.10", "2000::1"},
					IPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
				},
			},
		},

		{
			name:           "service dual stack ipv4,6. want ipv6",
			requestFamily:  v1.IPv6Protocol,
			expectedResult: "2000::1",
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIPs: []string{"10.0.0.10", "2000::1"},
					IPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
				},
			},
		},

		{
			name:           "service dual stack ipv6,4. want ipv6",
			requestFamily:  v1.IPv6Protocol,
			expectedResult: "2000::1",
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIPs: []string{"2000::1", "10.0.0.10"},
					IPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
				},
			},
		},

		{
			name:           "service dual stack ipv6,4. want ipv4",
			requestFamily:  v1.IPv4Protocol,
			expectedResult: "10.0.0.10",
			service: v1.Service{
				Spec: v1.ServiceSpec{
					ClusterIPs: []string{"2000::1", "10.0.0.10"},
					IPFamilies: []v1.IPFamily{v1.IPv6Protocol, v1.IPv4Protocol},
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			// 旧式服务只有单个 ClusterIP，需按字符串判断地址族；
			// 双栈服务则按 IPFamilies 中的下标定位对应的 ClusterIP。
			ip := GetClusterIPByFamily(testCase.requestFamily, &testCase.service)
			if ip != testCase.expectedResult {
				t.Fatalf("expected ip:%v got %v", testCase.expectedResult, ip)
			}
		})
	}
}

// mustParseIPAddr 把字符串解析为 IPAddr，失败时 panic；测试中只允许出现合法输入。
func mustParseIPAddr(str string) net.Addr {
	a, err := net.ResolveIPAddr("ip", str)
	if err != nil {
		panic("mustParseIPAddr")
	}
	return a
}

// mustParseIPNet 把宽松格式的 CIDR 解析为 IPNet 并返回，失败时 panic。
func mustParseIPNet(str string) net.Addr {
	_, n, err := netutils.ParseCIDRSloppy(str)
	if err != nil {
		panic("mustParseIPNet")
	}
	return n
}

// mustParseUnix 把路径解析为 Unix socket 地址，失败时 panic。
func mustParseUnix(str string) net.Addr {
	n, err := net.ResolveUnixAddr("unix", str)
	if err != nil {
		panic("mustParseUnix")
	}
	return n
}

// cidrValidator 把 CIDR 包含关系包装成谓词，供 AddressSet 的 isValid 参数使用。
type cidrValidator struct {
	cidr *net.IPNet
}

// isValid 判断 IP 是否落在关联的 CIDR 内。
func (v *cidrValidator) isValid(ip net.IP) bool {
	return v.cidr.Contains(ip)
}

// newCidrValidator 解析 CIDR 并返回对应的校验闭包；测试输入非法时直接 panic。
func newCidrValidator(cidr string) func(ip net.IP) bool {
	_, n, err := netutils.ParseCIDRSloppy(cidr)
	if err != nil {
		panic("mustParseIPNet")
	}
	obj := cidrValidator{n}
	return obj.isValid
}

// TestAddressSet 验证 AddressSet 只保留通过校验的 IPAddr/IPNet 地址，
// 忽略 Unix socket 地址，并以字符串集合形式返回。
func TestAddressSet(t *testing.T) {
	testCases := []struct {
		name      string
		validator func(ip net.IP) bool
		input     []net.Addr
		expected  sets.Set[string]
	}{
		{
			"Empty",
			func(ip net.IP) bool { return false },
			nil,
			nil,
		},
		{
			"Reject IPAddr x 2",
			func(ip net.IP) bool { return false },
			[]net.Addr{
				mustParseIPAddr("8.8.8.8"),
				mustParseIPAddr("1000::"),
			},
			nil,
		},
		{
			"Accept IPAddr x 2",
			func(ip net.IP) bool { return true },
			[]net.Addr{
				mustParseIPAddr("8.8.8.8"),
				mustParseIPAddr("1000::"),
			},
			sets.New("8.8.8.8", "1000::"),
		},
		{
			"Accept IPNet x 2",
			func(ip net.IP) bool { return true },
			[]net.Addr{
				mustParseIPNet("8.8.8.8/32"),
				mustParseIPNet("1000::/128"),
			},
			sets.New("8.8.8.8", "1000::"),
		},
		{
			"Accept Unix x 2",
			func(ip net.IP) bool { return true },
			[]net.Addr{
				mustParseUnix("/tmp/sock1"),
				mustParseUnix("/tmp/sock2"),
			},
			nil,
		},
		{
			"Cidr IPv4",
			newCidrValidator("192.168.1.0/24"),
			[]net.Addr{
				mustParseIPAddr("8.8.8.8"),
				mustParseIPAddr("1000::"),
				mustParseIPAddr("192.168.1.1"),
			},
			sets.New("192.168.1.1"),
		},
		{
			"Cidr IPv6",
			newCidrValidator("1000::/64"),
			[]net.Addr{
				mustParseIPAddr("8.8.8.8"),
				mustParseIPAddr("1000::"),
				mustParseIPAddr("192.168.1.1"),
			},
			sets.New("1000::"),
		},
	}

	// Unix socket 地址不含 IP，必须被跳过；空结果用 nil 集合表示。
	for _, tc := range testCases {
		if !tc.expected.Equal(AddressSet(tc.validator, tc.input)) {
			t.Errorf("%s", tc.name)
		}
	}
}

// TestIsZeroCIDR 验证 IsZeroCIDR 仅把前缀长度为 0 的 CIDR 视为通配 CIDR，
// 非法输入解析出 nil CIDR 时返回 false。
func TestIsZeroCIDR(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "invalide cidr",
			input:    "",
			expected: false,
		},
		{
			name:     "ipv4 cidr",
			input:    "172.10.0.0/16",
			expected: false,
		},
		{
			name:     "ipv4 zero cidr",
			input:    IPv4ZeroCIDR,
			expected: true,
		},
		{
			name:     "ipv6 cidr",
			input:    "::/128",
			expected: false,
		},
		{
			name:     "ipv6 zero cidr",
			input:    IPv6ZeroCIDR,
			expected: true,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 空字符串解析失败会得到 nil CIDR，IsZeroCIDR 必须能容忍 nil。
			_, cidr, _ := netutils.ParseCIDRSloppy(tc.input)
			if got := IsZeroCIDR(cidr); tc.expected != got {
				t.Errorf("IsZeroCIDR() = %t, want %t", got, tc.expected)
			}
		})
	}
}
