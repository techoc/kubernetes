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

package proxy

import (
	"net/url"
	"testing"

	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	v1listers "k8s.io/client-go/listers/core/v1"
	discoveryv1listers "k8s.io/client-go/listers/discovery/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"
)

// TestResolve 验证 ClusterIP 与 Endpoint 两种解析模式在各 Service 类型下的行为。
//
// cluster 模式只依赖 Service 的 ClusterIP；endpoint 模式依赖
// EndpointSlice 中的实际地址。用例覆盖无 443 端口、无切片、ExternalName、
// 不支持的 Service 类型与缺失 Service 等失败路径。
func TestResolve(t *testing.T) {
	// matchingEndpointSlices 生成与 Service 端口一一对应的 EndpointSlice。
	// 只保留 TargetPort 为整数的端口，与解析逻辑的假设一致。
	matchingEndpointSlices := func(svc *v1.Service) []*discoveryv1.EndpointSlice {
		ports := []discoveryv1.EndpointPort{}
		for _, p := range svc.Spec.Ports {
			if p.TargetPort.Type != intstr.Int {
				continue
			}
			ports = append(ports, discoveryv1.EndpointPort{Name: &p.Name, Port: &p.TargetPort.IntVal})
		}

		return []*discoveryv1.EndpointSlice{{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: svc.Namespace,
				Name:      svc.Name + "-xxx",
				Labels: map[string]string{
					discoveryv1.LabelServiceName: svc.Name,
				},
			},
			Endpoints: []discoveryv1.Endpoint{{
				Hostname:  ptr.To("dummy-host"),
				Addresses: []string{"127.0.0.1"},
			}},
			Ports: ports,
		}}
	}

	type expectation struct {
		url   string
		error bool
	}

	tests := []struct {
		name           string
		services       []*v1.Service
		endpointSlices func(svc *v1.Service) []*discoveryv1.EndpointSlice

		clusterMode  expectation
		endpointMode expectation
	}{
		{
			name: "cluster ip without 443 port",
			services: []*v1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "one", Name: "alfa"},
					Spec: v1.ServiceSpec{
						Type:      v1.ServiceTypeClusterIP,
						ClusterIP: "hit",
						Ports: []v1.ServicePort{
							{Port: 1234, TargetPort: intstr.FromInt32(1234)},
						},
					},
				},
			},
			endpointSlices: matchingEndpointSlices,

			clusterMode:  expectation{error: true},
			endpointMode: expectation{error: true},
		},
		{
			name: "cluster ip",
			services: []*v1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "one", Name: "alfa"},
					Spec: v1.ServiceSpec{
						Type:      v1.ServiceTypeClusterIP,
						ClusterIP: "hit",
						Ports: []v1.ServicePort{
							{Name: "https", Port: 443, TargetPort: intstr.FromInt32(1443)},
							{Port: 1234, TargetPort: intstr.FromInt32(1234)},
						},
					},
				},
			},
			endpointSlices: matchingEndpointSlices,

			clusterMode:  expectation{url: "https://hit:443"},
			endpointMode: expectation{url: "https://127.0.0.1:1443"},
		},
		{
			name: "cluster ip without endpointslices",
			services: []*v1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "one", Name: "alfa"},
					Spec: v1.ServiceSpec{
						Type:      v1.ServiceTypeClusterIP,
						ClusterIP: "hit",
						Ports: []v1.ServicePort{
							{Name: "https", Port: 443, TargetPort: intstr.FromInt32(1443)},
							{Port: 1234, TargetPort: intstr.FromInt32(1234)},
						},
					},
				},
			},
			endpointSlices: nil,

			clusterMode:  expectation{url: "https://hit:443"},
			endpointMode: expectation{error: true},
		},
		{
			name: "endpointslice without addresses",
			services: []*v1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "one", Name: "alfa"},
					Spec: v1.ServiceSpec{
						Type:      v1.ServiceTypeClusterIP,
						ClusterIP: "hit",
						Ports: []v1.ServicePort{
							{Name: "https", Port: 443, TargetPort: intstr.FromInt32(1443)},
							{Port: 1234, TargetPort: intstr.FromInt32(1234)},
						},
					},
				},
			},
			endpointSlices: func(svc *v1.Service) []*discoveryv1.EndpointSlice {
				return []*discoveryv1.EndpointSlice{{
					ObjectMeta: metav1.ObjectMeta{
						Namespace: svc.Namespace,
						Name:      svc.Name + "-xxx",
						Labels: map[string]string{
							discoveryv1.LabelServiceName: svc.Name,
						},
					},
				}}
			},

			clusterMode:  expectation{url: "https://hit:443"},
			endpointMode: expectation{error: true},
		},
		{
			name: "none cluster ip",
			services: []*v1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "one", Name: "alfa"},
					Spec: v1.ServiceSpec{
						Type:      v1.ServiceTypeClusterIP,
						ClusterIP: v1.ClusterIPNone,
					},
				},
			},
			endpointSlices: nil,

			clusterMode:  expectation{error: true},
			endpointMode: expectation{error: true},
		},
		{
			name: "loadbalancer",
			services: []*v1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "one", Name: "alfa"},
					Spec: v1.ServiceSpec{
						Type:      v1.ServiceTypeLoadBalancer,
						ClusterIP: "lb",
						Ports: []v1.ServicePort{
							{Name: "https", Port: 443, TargetPort: intstr.FromInt32(1443)},
							{Port: 1234, TargetPort: intstr.FromInt32(1234)},
						},
					},
				},
			},
			endpointSlices: matchingEndpointSlices,

			clusterMode:  expectation{url: "https://lb:443"},
			endpointMode: expectation{url: "https://127.0.0.1:1443"},
		},
		{
			name: "node port",
			services: []*v1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "one", Name: "alfa"},
					Spec: v1.ServiceSpec{
						Type:      v1.ServiceTypeNodePort,
						ClusterIP: "np",
						Ports: []v1.ServicePort{
							{Name: "https", Port: 443, TargetPort: intstr.FromInt32(1443)},
							{Port: 1234, TargetPort: intstr.FromInt32(1234)},
						},
					},
				},
			},
			endpointSlices: matchingEndpointSlices,

			clusterMode:  expectation{url: "https://np:443"},
			endpointMode: expectation{url: "https://127.0.0.1:1443"},
		},
		{
			name: "external name",
			services: []*v1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "one", Name: "alfa"},
					Spec: v1.ServiceSpec{
						Type:         v1.ServiceTypeExternalName,
						ExternalName: "foo.bar.com",
					},
				},
			},
			endpointSlices: nil,

			clusterMode:  expectation{url: "https://foo.bar.com:443"},
			endpointMode: expectation{error: true},
		},
		{
			name: "unsupported service",
			services: []*v1.Service{
				{
					ObjectMeta: metav1.ObjectMeta{Namespace: "one", Name: "alfa"},
					Spec: v1.ServiceSpec{
						Type: "unsupported",
					},
				},
			},
			endpointSlices: nil,

			clusterMode:  expectation{error: true},
			endpointMode: expectation{error: true},
		},
		{
			name:           "missing service",
			services:       nil,
			endpointSlices: nil,

			clusterMode:  expectation{error: true},
			endpointMode: expectation{error: true},
		},
	}

	for _, test := range tests {
		serviceCache := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
		serviceLister := v1listers.NewServiceLister(serviceCache)
		for i := range test.services {
			if err := serviceCache.Add(test.services[i]); err != nil {
				t.Fatalf("%s unexpected service add error: %v", test.name, err)
			}
		}

		endpointSliceCache := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
		endpointSliceLister := discoveryv1listers.NewEndpointSliceLister(endpointSliceCache)
		if test.endpointSlices != nil {
			for _, svc := range test.services {
				for _, ep := range test.endpointSlices(svc) {
					if err := endpointSliceCache.Add(ep); err != nil {
						t.Fatalf("%s unexpected endpointslice add error: %v", test.name, err)
					}
				}
			}
		}

		check := func(mode string, expected expectation, url *url.URL, err error) {
			// 统一的断言入口，避免两种模式各写一套容易漂移的校验逻辑。
			switch {
			case err == nil && expected.error:
				t.Errorf("%s in %s mode expected error, got none", test.name, mode)
			case err != nil && expected.error:
				// ignore
			case err != nil:
				t.Errorf("%s in %s mode unexpected error: %v", test.name, mode, err)
			case url.String() != expected.url:
				t.Errorf("%s in %s mode expected url %q, got %q", test.name, mode, expected.url, url.String())
			}
		}

		clusterURL, err := ResolveCluster(serviceLister, "one", "alfa", 443)
		check("cluster", test.clusterMode, clusterURL, err)

		endpointSliceGetter, err := NewEndpointSliceListerGetter(endpointSliceLister)
		if err != nil {
			t.Fatal(err)
		}

		endpointURL, err := ResolveEndpoint(serviceLister, endpointSliceGetter, "one", "alfa", 443)
		check("endpoint", test.endpointMode, endpointURL, err)
	}
}

// 验证 ResolveEndpoint 按预期在多个端点之间随机选择。
// 随机选择意味着单次调用无法断言结果，因此重复采样 100 次，
// 用集合覆盖来验证"所有合法端点都可能被选中、非法端点从不被选中"。
func TestResolveEndpointDistribution(t *testing.T) {
	singleStackService := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "single-stack"},
		Spec: v1.ServiceSpec{
			Type:       v1.ServiceTypeClusterIP,
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol},
			Ports: []v1.ServicePort{
				{Name: "https", Port: 443, TargetPort: intstr.FromInt32(443)},
			},
		},
	}
	dualStackService := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "dual-stack"},
		Spec: v1.ServiceSpec{
			Type:       v1.ServiceTypeClusterIP,
			IPFamilies: []v1.IPFamily{v1.IPv4Protocol, v1.IPv6Protocol},
			Ports: []v1.ServicePort{
				{Name: "https", Port: 443, TargetPort: intstr.FromInt32(443)},
			},
		},
	}
	svcPort := singleStackService.Spec.Ports[0]
	wrongPort := v1.ServicePort{Name: "http", Port: 80, TargetPort: intstr.FromInt32(443)}

	makeEndpointSlice := func(svc *v1.Service, suffix string, addressType discoveryv1.AddressType, port v1.ServicePort, endpoints ...discoveryv1.Endpoint) *discoveryv1.EndpointSlice {
		// 用 addressType 与端口区分切片，便于构造"地址族不匹配"或"端口名不匹配"的负向场景。
		return &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: svc.Namespace,
				Name:      svc.Name + "-" + suffix,
				Labels: map[string]string{
					discoveryv1.LabelServiceName: svc.Name,
				},
			},
			AddressType: addressType,
			Endpoints:   endpoints,
			Ports: []discoveryv1.EndpointPort{{
				Name: &port.Name,
				Port: &port.TargetPort.IntVal,
			}},
		}
	}

	testCases := []struct {
		name           string
		service        *v1.Service
		endpointSlices []*discoveryv1.EndpointSlice

		expectedURLs []string
	}{
		{
			name:    "simple",
			service: singleStackService,
			endpointSlices: []*discoveryv1.EndpointSlice{
				makeEndpointSlice(singleStackService, "1",
					discoveryv1.AddressTypeIPv4, svcPort,
					discoveryv1.Endpoint{
						Addresses: []string{"10.0.0.1"},
					},
				),
				makeEndpointSlice(singleStackService, "2",
					discoveryv1.AddressTypeIPv4, svcPort,
					discoveryv1.Endpoint{
						Addresses: []string{"10.0.0.2"},
					},
				),
			},
			expectedURLs: []string{
				"https://10.0.0.1:443",
				"https://10.0.0.2:443",
			},
		},
		{
			name:    "multiple endpoints, some non-ready",
			service: singleStackService,
			// Ready 为 nil 时按就绪处理；显式 false 的端点必须被排除。
			endpointSlices: []*discoveryv1.EndpointSlice{
				makeEndpointSlice(singleStackService, "1",
					discoveryv1.AddressTypeIPv4, svcPort,
					discoveryv1.Endpoint{
						Addresses:  []string{"10.0.0.1"},
						Conditions: discoveryv1.EndpointConditions{
							// implied Ready
						},
					},
					discoveryv1.Endpoint{
						Addresses: []string{"10.0.0.2"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: ptr.To(false),
						},
					},
					discoveryv1.Endpoint{
						Addresses: []string{"10.0.0.3"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: ptr.To(true),
						},
					},
				),
				makeEndpointSlice(singleStackService, "2",
					discoveryv1.AddressTypeIPv4, svcPort,
					discoveryv1.Endpoint{
						Addresses: []string{"10.0.0.4"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: ptr.To(true),
						},
					},
				),
				makeEndpointSlice(singleStackService, "3",
					discoveryv1.AddressTypeIPv4, svcPort,
					discoveryv1.Endpoint{
						Addresses: []string{"10.0.0.5"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: ptr.To(false),
						},
					},
					discoveryv1.Endpoint{
						Addresses: []string{"10.0.0.6"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: ptr.To(false),
						},
					},
				),
			},
			expectedURLs: []string{
				"https://10.0.0.1:443",
				"https://10.0.0.3:443",
				"https://10.0.0.4:443",
			},
		},
		{
			name:    "dual-stack, primary-family endpoints ready",
			service: dualStackService,
			// 双栈服务优先选择主地址族（此处 IPv4），IPv6 端点应被忽略。
			endpointSlices: []*discoveryv1.EndpointSlice{
				makeEndpointSlice(dualStackService, "v6",
					discoveryv1.AddressTypeIPv6, svcPort,
					discoveryv1.Endpoint{
						Addresses: []string{"fd00::1"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: ptr.To(true),
						},
					},
					discoveryv1.Endpoint{
						Addresses: []string{"fd00::2"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: ptr.To(true),
						},
					},
				),
				makeEndpointSlice(dualStackService, "v4",
					discoveryv1.AddressTypeIPv4, svcPort,
					discoveryv1.Endpoint{
						Addresses: []string{"10.0.0.1"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: ptr.To(true),
						},
					},
					discoveryv1.Endpoint{
						Addresses: []string{"10.0.0.2"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: ptr.To(true),
						},
					},
				),
			},
			expectedURLs: []string{
				"https://10.0.0.1:443",
				"https://10.0.0.2:443",
			},
		},
		{
			name:    "dual-stack, primary-family endpoints non-ready",
			service: dualStackService,
			// 主地址族全部未就绪时回退到另一地址族，保证可用性优先。
			endpointSlices: []*discoveryv1.EndpointSlice{
				makeEndpointSlice(dualStackService, "v4",
					discoveryv1.AddressTypeIPv4, svcPort,
					discoveryv1.Endpoint{
						Addresses: []string{"10.0.0.1"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: ptr.To(false),
						},
					},
					discoveryv1.Endpoint{
						Addresses: []string{"10.0.0.2"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: ptr.To(false),
						},
					},
				),
				makeEndpointSlice(dualStackService, "v6",
					discoveryv1.AddressTypeIPv6, svcPort,
					discoveryv1.Endpoint{
						Addresses: []string{"fd00::1"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: ptr.To(true),
						},
					},
					discoveryv1.Endpoint{
						Addresses: []string{"fd00::2"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: ptr.To(true),
						},
					},
				),
			},
			expectedURLs: []string{
				"https://[fd00::1]:443",
				"https://[fd00::2]:443",
			},
		},
		{
			name:    "many slices, many endpoints, most unusable",
			service: dualStackService,
			endpointSlices: []*discoveryv1.EndpointSlice{
				makeEndpointSlice(dualStackService, "v4-1",
					discoveryv1.AddressTypeIPv4, svcPort,
					discoveryv1.Endpoint{
						// 未就绪。
						Addresses: []string{"10.0.0.1"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: ptr.To(false),
						},
					},
				),
				makeEndpointSlice(dualStackService, "v6-1",
					discoveryv1.AddressTypeIPv6, svcPort,
					discoveryv1.Endpoint{
						// wrong IP family
						Addresses: []string{"fd00::1"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: ptr.To(true),
						},
					},
					discoveryv1.Endpoint{
						// wrong IP family
						Addresses: []string{"fd00::2"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: ptr.To(true),
						},
					},
				),
				makeEndpointSlice(dualStackService, "v4-2",
					discoveryv1.AddressTypeIPv4, svcPort,
					// (no endpoints)
				),
				makeEndpointSlice(dualStackService, "v4-3",
					discoveryv1.AddressTypeIPv4, svcPort,
					discoveryv1.Endpoint{
						// 这是正确的那个。
						Addresses: []string{"10.0.0.2"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: ptr.To(true),
						},
					},
				),
				makeEndpointSlice(dualStackService, "v4-4",
					discoveryv1.AddressTypeIPv4, wrongPort,
					discoveryv1.Endpoint{
						// 使用上面的 wrongPort，因此它不会有
						// 正确的端口名。
						Addresses: []string{"10.0.0.3"},
						Conditions: discoveryv1.EndpointConditions{
							Ready: ptr.To(true),
						},
					},
				),
			},
			expectedURLs: []string{
				"https://10.0.0.2:443",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			serviceCache := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
			serviceLister := v1listers.NewServiceLister(serviceCache)
			if err := serviceCache.Add(tc.service); err != nil {
				t.Fatalf("unexpected service add error: %v", err)
			}

			endpointSliceCache := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
			endpointSliceLister := discoveryv1listers.NewEndpointSliceLister(endpointSliceCache)
			for _, ep := range tc.endpointSlices {
				if err := endpointSliceCache.Add(ep); err != nil {
					t.Fatalf("unexpected endpointslice add error: %v", err)
				}
			}

			endpointSliceGetter, err := NewEndpointSliceListerGetter(endpointSliceLister)
			if err != nil {
				t.Fatal(err)
			}

			expectedURLs := sets.New(tc.expectedURLs...)
			gotURLs := sets.New[string]()
			// 多次采样覆盖随机选择：只允许出现期望集合内的地址，且期望集合必须被全部覆盖。
			for i := 0; i < 100; i++ {
				endpointURL, err := ResolveEndpoint(serviceLister, endpointSliceGetter, tc.service.Namespace, tc.service.Name, tc.service.Spec.Ports[0].Port)
				if err != nil {
					t.Fatalf("unexpected error from ResolveEndpoint: %v", err)
				}
				gotURLs.Insert(endpointURL.String())
			}

			extraURLs := gotURLs.Difference(expectedURLs)
			if len(extraURLs) > 0 {
				t.Errorf("ResolveEndpoint picked invalid endpoints: %v", sets.List(extraURLs))
			}
			missingURLs := expectedURLs.Difference(gotURLs)
			if len(missingURLs) > 0 {
				t.Errorf("ResolveEndpoint failed to pick some valid endpoints: %v", sets.List(missingURLs))
			}
		})
	}
}
