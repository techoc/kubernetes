/*
Copyright 2022 The Kubernetes Authors.

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

package service

import (
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/utils/ptr"
)

// TestGetWarningsForService 验证 GetWarningsForService 对冲突/弃用配置的告警条数：
// 弃用注解、类型与字段不匹配、headless Service 上的无效配置各产生一条告警，
// 同一对象上的多个问题分别计数；合法配置不告警。
func TestGetWarningsForService(t *testing.T) {
	testCases := []struct {
		name        string
		tweakSvc    func(svc *api.Service) // 每个用例在一个基础合法 Service 上做最小改动。
		numWarnings int
	}{{
		// 新版 topology mode 注解是当前推荐写法，不应告警。
		name: "new topology mode set",
		tweakSvc: func(s *api.Service) {
			s.Annotations = map[string]string{api.AnnotationTopologyMode: "foo"}
		},
		numWarnings: 0,
	}, {
		// 已弃用的 topology aware hints 注解仍可用，但要提示迁移。
		name: "deprecated hints annotation set",
		tweakSvc: func(s *api.Service) {
			s.Annotations = map[string]string{api.DeprecatedAnnotationTopologyAwareHints: "foo"}
		},
		numWarnings: 1,
	}, {
		// ExternalName 类型只是 DNS 别名，配置 ExternalIPs 没有意义。
		name: "externalIPs set when type is ExternalName",
		tweakSvc: func(s *api.Service) {
			s.Spec.Type = api.ServiceTypeExternalName
			s.Spec.ExternalIPs = []string{"1.2.3.4"} //nolint:staticcheck // SA1019 testing deprecated field
		},
		numWarnings: 1,
	}, {
		// ExternalName 字段只在类型为 ExternalName 时有意义，这里类型不匹配。
		name: "externalName set when type is not ExternalName",
		tweakSvc: func(s *api.Service) {
			s.Spec.Type = api.ServiceTypeClusterIP
			s.Spec.ExternalName = "example.com"
		},
		numWarnings: 1,
	}, {
		// headless Service 没有 LB 入口，配置 LoadBalancerIP 无效。
		name: "LoadBalancerIP set when headless service",
		tweakSvc: func(s *api.Service) {
			s.Spec.Type = api.ServiceTypeClusterIP
			s.Spec.ClusterIP = api.ClusterIPNone
			s.Spec.LoadBalancerIP = "1.2.3.4"
			s.Spec.SessionAffinity = api.ServiceAffinityNone // 默认值
		},
		numWarnings: 1,
	}, {
		// headless Service 不需要 ExternalIPs 来提供入口。
		name: "ExternalIPs set when headless service",
		tweakSvc: func(s *api.Service) {
			s.Spec.Type = api.ServiceTypeClusterIP
			s.Spec.ClusterIP = api.ClusterIPNone
			s.Spec.ExternalIPs = []string{"1.2.3.4"}
			s.Spec.SessionAffinity = api.ServiceAffinityNone // 默认值
		},
		numWarnings: 1,
	}, {
		// headless Service 没有 kube-proxy 负载均衡，ClientIP 亲和性无意义。
		name: "SessionAffinity Client IP set when headless service",
		tweakSvc: func(s *api.Service) {
			s.Spec.Type = api.ServiceTypeClusterIP
			s.Spec.ClusterIP = api.ClusterIPNone
			s.Spec.SessionAffinity = api.ServiceAffinityClientIP
		},
		numWarnings: 1,
	}, {
		// SessionAffinityNone 是默认值，headless Service 上无需告警。
		name: "SessionAffinity None set when headless service",
		tweakSvc: func(s *api.Service) {
			s.Spec.Type = api.ServiceTypeClusterIP
			s.Spec.ClusterIP = api.ClusterIPNone
			s.Spec.SessionAffinity = api.ServiceAffinityNone
		},
		numWarnings: 0,
	}, {
		// 三个问题同时存在时应分别计数，验证告警不会互相覆盖。
		name: "ExternalIPs, LoadBalancerIP and SessionAffinity set when headless service",
		tweakSvc: func(s *api.Service) {
			s.Spec.Type = api.ServiceTypeClusterIP
			s.Spec.ClusterIP = api.ClusterIPNone
			s.Spec.ExternalIPs = []string{"1.2.3.4"}
			s.Spec.LoadBalancerIP = "1.2.3.4"
			s.Spec.SessionAffinity = api.ServiceAffinityClientIP
		},
		numWarnings: 3,
	}, {
		// PreferSameZone 是当前支持的流量分发策略。
		name: "trafficDistribution: PreferSameZone",
		tweakSvc: func(s *api.Service) {
			s.Spec.Type = api.ServiceTypeClusterIP
			s.Spec.TrafficDistribution = ptr.To(api.ServiceTrafficDistributionPreferSameZone)
		},
		numWarnings: 0,
	}, {
		// PreferClose 已由 PreferSameZone 取代，需提示迁移。
		name: "trafficDistribution: PreferClose",
		tweakSvc: func(s *api.Service) {
			s.Spec.Type = api.ServiceTypeClusterIP
			s.Spec.TrafficDistribution = ptr.To(api.ServiceTrafficDistributionPreferClose)
		},
		numWarnings: 1,
	}, {
		// ExternalIPs 本身已弃用，即使类型匹配也会产生一条告警。
		name: "ExternalIPs set on ClusterIP service",
		tweakSvc: func(s *api.Service) {
			s.Spec.ExternalIPs = []string{"1.2.3.4"} //nolint:staticcheck // SA1019 testing deprecated field
		},
		numWarnings: 1,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			svc := &api.Service{}
			tc.tweakSvc(svc)
			// 新旧对象相同，覆盖创建/更新两条路径上的告警逻辑。
			warnings := GetWarningsForService(svc, svc)
			if want, got := tc.numWarnings, len(warnings); got != want {
				t.Errorf("Unexpected warning list: expected %d, got %d\n%q", want, got, warnings)
			}
		})
	}
}

// TestGetWarningsForServiceClusterIPs 验证对 IP 字段的非标准写法给出的告警：
// IPv4 前导零、IPv6 非 RFC 5952 规范格式会按字段下标逐条报告，
// 多个字段同时出错时告警按固定顺序全部保留，格式正确时无告警。
func TestGetWarningsForServiceClusterIPs(t *testing.T) {
	service := func(clusterIPs []string) *api.Service {
		svc := api.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "svc-test",
				Namespace: "ns",
			},
			Spec: api.ServiceSpec{
				Type: api.ServiceTypeClusterIP,
			},
		}

		if len(clusterIPs) > 0 {
			// ClusterIP 与 ClusterIPs[0] 保持一致，模拟真实 Service。
			svc.Spec.ClusterIP = clusterIPs[0]
			svc.Spec.ClusterIPs = clusterIPs
		}
		return &svc
	}

	tests := []struct {
		name       string
		service    *api.Service
		oldService *api.Service
		want       []string
	}{
		// 单个标准 IPv4 / IPv6 都不应产生告警。
		{
			name:    "IPv4 No failures",
			service: service([]string{"192.12.2.2"}),
		},
		{
			name:    "IPv6 No failures",
			service: service([]string{"2001:db8::2"}),
		},
		// IPv4 段带前导零：需给出规范化后的地址建议。
		{
			name:    "IPv4 with leading zeros",
			service: service([]string{"192.012.2.2"}),
			want: []string{
				`spec.clusterIPs[0]: non-standard IP address "192.012.2.2" is invalid: use "192.12.2.2"`,
			},
		},
		// 双栈场景要用下标定位到第几个地址出错。
		{
			name:    "Dual Stack IPv4-IPv6 and IPv4 with leading zeros",
			service: service([]string{"192.012.2.2", "2001:db8::2"}),
			want: []string{
				`spec.clusterIPs[0]: non-standard IP address "192.012.2.2" is invalid: use "192.12.2.2"`,
			},
		},
		{
			name:    "Dual Stack IPv6-IPv4 and IPv4 with leading zeros",
			service: service([]string{"2001:db8::2", "192.012.2.2"}),
			want: []string{
				`spec.clusterIPs[1]: non-standard IP address "192.012.2.2" is invalid: use "192.12.2.2"`,
			},
		},
		// IPv6 地址未压缩到 RFC 5952 规范形式时需告警。
		{
			name:    "IPv6 non canonical format",
			service: service([]string{"2001:db8:0:0::2"}),
			want: []string{
				`spec.clusterIPs[0]: IPv6 address "2001:db8:0:0::2" should be in RFC 5952 canonical format ("2001:db8::2")`,
			},
		},
		{
			name:    "Dual Stack IPv4-IPv6 and IPv6 non-canonical format",
			service: service([]string{"192.12.2.2", "2001:db8:0:0::2"}),
			want: []string{
				`spec.clusterIPs[1]: IPv6 address "2001:db8:0:0::2" should be in RFC 5952 canonical format ("2001:db8::2")`,
			},
		},
		{
			name:    "Dual Stack IPv6-IPv4 and IPv6 non-canonical formats",
			service: service([]string{"2001:db8:0:0::2", "192.12.2.2"}),
			want: []string{
				`spec.clusterIPs[0]: IPv6 address "2001:db8:0:0::2" should be in RFC 5952 canonical format ("2001:db8::2")`,
			},
		},
		// 两类错误同时出现：验证所有告警按字段顺序收集，不会遗漏。
		{
			name:    "Dual Stack IPv4-IPv6 and IPv4 with leading zeros and IPv6 non-canonical format",
			service: service([]string{"192.012.2.2", "2001:db8:0:0::2"}),
			want: []string{
				`spec.clusterIPs[0]: non-standard IP address "192.012.2.2" is invalid: use "192.12.2.2"`,
				`spec.clusterIPs[1]: IPv6 address "2001:db8:0:0::2" should be in RFC 5952 canonical format ("2001:db8::2")`,
			},
		},
		{
			name:    "Dual Stack IPv6-IPv4 and IPv4 with leading zeros and IPv6 non-canonical format",
			service: service([]string{"2001:db8:0:0::2", "192.012.2.2"}),
			want: []string{
				`spec.clusterIPs[0]: IPv6 address "2001:db8:0:0::2" should be in RFC 5952 canonical format ("2001:db8::2")`,
				`spec.clusterIPs[1]: non-standard IP address "192.012.2.2" is invalid: use "192.12.2.2"`,
			},
		},
		// 集中检查所有 IP 字段：告警应覆盖 ClusterIPs、ExternalIPs、
		// LoadBalancerIP 和 LoadBalancerSourceRanges，并带上弃用字段的提示。
		{
			name: "Service with all IPs fields with errors",
			service: &api.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "svc-test",
					Namespace: "ns",
				},
				Spec: api.ServiceSpec{
					ClusterIP:                "2001:db8:0:0::2",
					ClusterIPs:               []string{"2001:db8:0:0::2", "192.012.2.2"},
					ExternalIPs:              []string{"2001:db8:1:0::2", "10.012.2.2"},
					LoadBalancerIP:           "10.001.1.1",
					LoadBalancerSourceRanges: []string{"2001:db8:1:0::/64", "10.012.2.0/24"},
					Type:                     api.ServiceTypeClusterIP,
				},
			},
			want: []string{
				`spec.clusterIPs[0]: IPv6 address "2001:db8:0:0::2" should be in RFC 5952 canonical format ("2001:db8::2")`,
				`spec.clusterIPs[1]: non-standard IP address "192.012.2.2" is invalid: use "192.12.2.2"`,
				`spec.externalIPs[0]: IPv6 address "2001:db8:1:0::2" should be in RFC 5952 canonical format ("2001:db8:1::2")`,
				`spec.externalIPs[1]: non-standard IP address "10.012.2.2" is invalid: use "10.12.2.2"`,
				`spec.externalIPs is deprecated and may no longer be implemented in some clusters`,
				`spec.loadBalancerIP: non-standard IP address "10.001.1.1" is invalid: use "10.1.1.1"`,
				`spec.loadBalancerSourceRanges[0]: IPv6 CIDR value "2001:db8:1:0::/64" should be in RFC 5952 canonical format ("2001:db8:1::/64")`,
				`spec.loadBalancerSourceRanges[1]: non-standard CIDR value "10.012.2.0/24" is invalid: use "10.12.2.0/24"`,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 用 DeepEqual 比较整个切片，顺序也是告警接口的一部分。
			if got := GetWarningsForService(tt.service, tt.oldService); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GetWarningsForService() = %v", cmp.Diff(got, tt.want))
			}
		})
	}
}
