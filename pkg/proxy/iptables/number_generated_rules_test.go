//go:build linux

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

package iptables

import (
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	iptablestest "k8s.io/kubernetes/pkg/util/iptables/testing"
	netutils "k8s.io/utils/net"
	"k8s.io/utils/ptr"
)

// TestNumberIptablesRules 统计 kube-proxy 为将 Service 流量转发到 Endpoint 而生成的 iptables 规则数量。
// kube-proxy 通过 iptables-restore 原子地配置规则；但规则数量过大时，提交耗时很长，会造成流量中断。
// 影响规则数量的主要参数有：Service 类型、Service 数量以及每个 Service 的 Endpoint 数量。
// 当规则数量发生变化时本测试会失败，这样修改代码的人能立即得到性能影响的反馈；
// 同时通过多组不同规模的用例，检查规则数量是否随 Service/Endpoint 数量线性增长。
func TestNumberIptablesRules(t *testing.T) {
	testCases := []struct {
		name                string
		epsFunc             func(eps *discovery.EndpointSlice)
		svcFunc             func(svc *v1.Service)
		services            int
		epPerService        int
		expectedFilterRules int
		expectedNatRules    int
	}{
		{
			name:                "0 Services 0 EndpointsPerService - ClusterIP",
			services:            0,
			epPerService:        0,
			expectedFilterRules: 4,
			expectedNatRules:    5,
		},
		{
			name:                "1 Services 0 EndpointPerService - ClusterIP",
			services:            1,
			epPerService:        0,
			expectedFilterRules: 5,
			expectedNatRules:    5,
		},
		{
			name:                "1 Services 1 EndpointPerService - ClusterIP",
			services:            1,
			epPerService:        1,
			expectedFilterRules: 4,
			expectedNatRules:    10,
		},
		{
			name:                "1 Services 2 EndpointPerService - ClusterIP",
			services:            1,
			epPerService:        2,
			expectedFilterRules: 4,
			expectedNatRules:    13,
		},
		{
			name:                "1 Services 10 EndpointPerService - ClusterIP",
			services:            1,
			epPerService:        10,
			expectedFilterRules: 4,
			expectedNatRules:    37,
		},
		{
			name:                "10 Services 0 EndpointsPerService - ClusterIP",
			services:            10,
			epPerService:        0,
			expectedFilterRules: 14,
			expectedNatRules:    5,
		},
		{
			name:                "10 Services 1 EndpointPerService - ClusterIP",
			services:            10,
			epPerService:        1,
			expectedFilterRules: 4,
			expectedNatRules:    55,
		},
		{
			name:                "10 Services 2 EndpointPerService - ClusterIP",
			services:            10,
			epPerService:        2,
			expectedFilterRules: 4,
			expectedNatRules:    85,
		},
		{
			name:                "10 Services 10 EndpointPerService - ClusterIP",
			services:            10,
			epPerService:        10,
			expectedFilterRules: 4,
			expectedNatRules:    325,
		},

		{
			name: "0 Services 0 EndpointsPerService - LoadBalancer",
			svcFunc: func(svc *v1.Service) {
				svc.Spec.Type = v1.ServiceTypeLoadBalancer
				svc.Spec.ExternalIPs = []string{"1.2.3.4"}
				svc.Spec.LoadBalancerSourceRanges = []string{" 1.2.3.4/28"}
				svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
					IP: "1.2.3.4",
				}}
			},
			services:            0,
			epPerService:        0,
			expectedFilterRules: 4,
			expectedNatRules:    5,
		},
		{
			name: "1 Services 0 EndpointPerService - LoadBalancer",
			svcFunc: func(svc *v1.Service) {
				svc.Spec.Type = v1.ServiceTypeLoadBalancer
				svc.Spec.ExternalIPs = []string{"1.2.3.4"}
				svc.Spec.LoadBalancerSourceRanges = []string{" 1.2.3.4/28"}
				svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
					IP: "1.2.3.4",
				}}
			},
			services:            1,
			epPerService:        0,
			expectedFilterRules: 8,
			expectedNatRules:    5,
		},
		{
			name: "1 Services 1 EndpointPerService - LoadBalancer",
			svcFunc: func(svc *v1.Service) {
				svc.Spec.Type = v1.ServiceTypeLoadBalancer
				svc.Spec.ExternalIPs = []string{"1.2.3.4"}
				svc.Spec.LoadBalancerSourceRanges = []string{" 1.2.3.4/28"}
				svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
					IP: "1.2.3.4",
				}}
			},
			services:            1,
			epPerService:        1,
			expectedFilterRules: 5,
			expectedNatRules:    18,
		},
		{
			name: "1 Services 2 EndpointPerService - LoadBalancer",
			svcFunc: func(svc *v1.Service) {
				svc.Spec.Type = v1.ServiceTypeLoadBalancer
				svc.Spec.ExternalIPs = []string{"1.2.3.4"}
				svc.Spec.LoadBalancerSourceRanges = []string{" 1.2.3.4/28"}
				svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
					IP: "1.2.3.4",
				}}
			},
			services:            1,
			epPerService:        2,
			expectedFilterRules: 5,
			expectedNatRules:    21,
		},
		{
			name: "1 Services 10 EndpointPerService - LoadBalancer",
			svcFunc: func(svc *v1.Service) {
				svc.Spec.Type = v1.ServiceTypeLoadBalancer
				svc.Spec.ExternalIPs = []string{"1.2.3.4"}
				svc.Spec.LoadBalancerSourceRanges = []string{" 1.2.3.4/28"}
				svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
					IP: "1.2.3.4",
				}}
			},
			services:            1,
			epPerService:        10,
			expectedFilterRules: 5,
			expectedNatRules:    45,
		},
		{
			name: "10 Services 0 EndpointsPerService - LoadBalancer",
			svcFunc: func(svc *v1.Service) {
				svc.Spec.Type = v1.ServiceTypeLoadBalancer
				svc.Spec.ExternalIPs = []string{"1.2.3.4"}
				svc.Spec.LoadBalancerSourceRanges = []string{" 1.2.3.4/28"}
				svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
					IP: "1.2.3.4",
				}}
			},
			services:            10,
			epPerService:        0,
			expectedFilterRules: 44,
			expectedNatRules:    5,
		},
		{
			name: "10 Services 1 EndpointPerService - LoadBalancer",
			svcFunc: func(svc *v1.Service) {
				svc.Spec.Type = v1.ServiceTypeLoadBalancer
				svc.Spec.ExternalIPs = []string{"1.2.3.4"}
				svc.Spec.LoadBalancerSourceRanges = []string{" 1.2.3.4/28"}
				svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
					IP: "1.2.3.4",
				}}
			},
			services:            10,
			epPerService:        1,
			expectedFilterRules: 14,
			expectedNatRules:    135,
		},
		{
			name: "10 Services 2 EndpointPerService - LoadBalancer",
			svcFunc: func(svc *v1.Service) {
				svc.Spec.Type = v1.ServiceTypeLoadBalancer
				svc.Spec.ExternalIPs = []string{"1.2.3.4"}
				svc.Spec.LoadBalancerSourceRanges = []string{" 1.2.3.4/28"}
				svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
					IP: "1.2.3.4",
				}}
			},
			services:            10,
			epPerService:        2,
			expectedFilterRules: 14,
			expectedNatRules:    165,
		},
		{
			name: "10 Services 10 EndpointPerService - LoadBalancer",
			svcFunc: func(svc *v1.Service) {
				svc.Spec.Type = v1.ServiceTypeLoadBalancer
				svc.Spec.ExternalIPs = []string{"1.2.3.4"}
				svc.Spec.LoadBalancerSourceRanges = []string{" 1.2.3.4/28"}
				svc.Status.LoadBalancer.Ingress = []v1.LoadBalancerIngress{{
					IP: "1.2.3.4",
				}}
			},
			services:            10,
			epPerService:        10,
			expectedFilterRules: 14,
			expectedNatRules:    405,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			ipt := iptablestest.NewFake()
			fp := NewFakeProxier(ipt)

			svcs, eps := generateServiceEndpoints(test.services, test.epPerService, test.epsFunc, test.svcFunc)

			makeServiceMap(fp, svcs...)
			populateEndpointSlices(fp, eps...)

			// 记录同步耗时：规则体量越大，iptables-restore 的提交时间越长。
			now := time.Now()
			fp.syncProxyRules()
			t.Logf("time to sync rule: %v", time.Since(now))
			t.Logf("iptables data size: %d bytes", fp.iptablesData.Len())

			// 固定预期值：规则数一旦意外增加，就是性能回归的信号。
			if fp.filterRules.Lines() != test.expectedFilterRules {
				t.Errorf("expected number of Filter rules: %d, got: %d", test.expectedFilterRules, fp.filterRules.Lines())
			}

			if fp.natRules.Lines() != test.expectedNatRules {
				t.Errorf("expected number of NAT rules: %d, got: %d", test.expectedNatRules, fp.natRules.Lines())
			}

			// 需要排查规则数变化时，可打开下面的日志打印实际生成的规则。
			// t.Logf("Generated rules:\n %s", fp.iptablesData.String())
		})
	}
}

// Test_generateServiceEndpoints 验证测试数据工厂：生成的 Service/EndpointSlice 数量、
// Service 类型、Service 名称与 EndpointSlice 的关联标签，以及注入的 Endpoint 节点名都符合预期。
// 该工厂被规则数量测试复用，若生成的数据不符合预期，规则计数也就失去意义。
func Test_generateServiceEndpoints(t *testing.T) {
	testCases := []struct {
		name         string
		services     int
		epPerService int
		svcType      v1.ServiceType
	}{
		{
			name:         "Generate 10 Services with 10 Endpoints per Service and LoadBalancer Type",
			services:     10,
			epPerService: 10,
			svcType:      v1.ServiceTypeLoadBalancer,
		},
		{
			name:         "Generate 10 Services with 20 Endpoints per Service and NodePort Type",
			services:     10,
			epPerService: 20,
			svcType:      v1.ServiceTypeNodePort,
		},
	}

	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			// 注入 Service 类型变更函数，覆盖非默认类型的规则生成路径。
			svcFunc := func(svc *v1.Service) {
				svc.Spec.Type = test.svcType
			}
			// 注入 EndpointSlice 变更函数，为每个 Endpoint 设置节点名（拓扑提示所需）。
			epsFunc := func(eps *discovery.EndpointSlice) {
				for i := range eps.Endpoints {
					nodeName := fmt.Sprintf("node-%d", i)
					eps.Endpoints[i].NodeName = &nodeName
				}
			}

			svcs, eps := generateServiceEndpoints(test.services, test.epPerService, epsFunc, svcFunc)

			if len(svcs) != test.services {
				t.Fatalf("expected %d service, received %d", test.services, len(svcs))
			}
			if len(eps) != test.services {
				t.Fatalf("expected %d endpoint slice , received %d", test.services, len(eps))
			}

			for i := 0; i < test.services; i++ {
				if svcs[i].Spec.Type != test.svcType {
					t.Fatalf("expected Service Type %s, got %s", test.svcType, svcs[i].Spec.Type)
				}
				if eps[i].ObjectMeta.Labels[discovery.LabelServiceName] != svcs[i].Name {
					t.Fatalf("endpoint slice reference %s instead of Service %s", eps[i].ObjectMeta.Labels[discovery.LabelServiceName], svcs[i].Name)
				}
				if len(eps[i].Endpoints) != test.epPerService {
					t.Fatalf("expected %d endpoints per slice , received %d", test.epPerService, len(eps[i].Endpoints))
				}
				for j := 0; j < test.epPerService; j++ {
					nodeName := fmt.Sprintf("node-%d", j)
					if *eps[i].Endpoints[j].NodeName != nodeName {
						t.Errorf("Endpoint %d on EndpointSlice %d expected Nodename %s, got %s", j, i, nodeName, *eps[i].Endpoints[j].NodeName)
					}
				}
			}
		})
	}

}

// generateServiceEndpoints 生成指定数量的 Service，每个 Service 带 N 个 Endpoint，
// 并通过 epsFunc/svcFunc 让调用方注入自定义变更（如 Service 类型、节点名）。
// 它返回的 Service 与 EndpointSlice 一一对应，供规则数量测试构造稳定可复现的输入。
func generateServiceEndpoints(nServices, nEndpoints int, epsFunc func(eps *discovery.EndpointSlice), svcFunc func(svc *v1.Service)) ([]*v1.Service, []*discovery.EndpointSlice) {
	services := make([]*v1.Service, nServices)
	endpointSlices := make([]*discovery.EndpointSlice, nServices)

	// 基地址+偏移量保证多次运行生成的 IP 稳定且不冲突。
	basePort := 80
	base := netutils.BigForIP(netutils.ParseIPSloppy("10.0.0.1"))

	// 先构造一个 EndpointSlice 模板，再深拷贝给每个 Service，避免重复构造。
	baseEp := netutils.BigForIP(netutils.ParseIPSloppy("172.16.0.1"))
	epPort := 8080

	eps := &discovery.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "ep",
			Namespace: "namespace",
		},
		AddressType: discovery.AddressTypeIPv4,
		Endpoints:   []discovery.Endpoint{},
		Ports: []discovery.EndpointPort{{
			Name:     ptr.To(fmt.Sprintf("%d", epPort)),
			Port:     ptr.To(int32(epPort)),
			Protocol: ptr.To(v1.ProtocolTCP),
		}},
	}

	for j := 0; j < nEndpoints; j++ {
		ipEp := netutils.AddIPOffset(baseEp, j)
		eps.Endpoints = append(eps.Endpoints, discovery.Endpoint{
			Addresses: []string{ipEp.String()},
		})
	}

	if epsFunc != nil {
		epsFunc(eps)
	}

	// Service 模板同理；调用方注入的类型变更在深拷贝之前完成。
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "svc",
			Namespace: "namespace",
		},
		Spec: v1.ServiceSpec{
			Type: v1.ServiceTypeClusterIP,
		},
	}

	if svcFunc != nil {
		svcFunc(svc)
	}

	// 逐个深拷贝模板并为每个 Service 分配独立的 ClusterIP 和端口，
	// 这样规则计数反映的是真实多 Service 场景而非重复对象。
	for i := 0; i < nServices; i++ {
		ip := netutils.AddIPOffset(base, i)
		services[i] = svc.DeepCopy()
		services[i].Name = fmt.Sprintf("svc%d", i)
		services[i].Spec.ClusterIP = ip.String()
		services[i].Spec.Ports = []v1.ServicePort{
			{
				Name:       fmt.Sprintf("%d", epPort),
				Protocol:   v1.ProtocolTCP,
				Port:       int32(basePort + i),
				TargetPort: intstr.FromInt32(int32(epPort)),
			},
		}

		if svc.Spec.Type == v1.ServiceTypeNodePort || svc.Spec.Type == v1.ServiceTypeLoadBalancer {
			services[i].Spec.Ports[0].NodePort = int32(30000 + i)

		}
		if svc.Spec.Type == v1.ServiceTypeLoadBalancer {
			services[i].Spec.HealthCheckNodePort = int32(32000 + nServices + i)
		}

		endpointSlices[i] = eps.DeepCopy()
		endpointSlices[i].Name = services[i].Name
		endpointSlices[i].ObjectMeta.Labels = map[string]string{
			discovery.LabelServiceName: services[i].Name,
		}

	}

	return services, endpointSlices
}
