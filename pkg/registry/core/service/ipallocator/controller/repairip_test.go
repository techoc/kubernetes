/*
Copyright 2023 The Kubernetes Authors.

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

package controller

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/events"
	"k8s.io/kubernetes/pkg/registry/core/service/ipallocator"
	testclock "k8s.io/utils/clock/testing"
)

var (
	serviceCIDRv4 = "10.0.0.0/16"
	serviceCIDRv6 = "2001:db8::/64"

	// testTimeNow 用于固定伪造时钟看到的当前时间。
	testTimeNow = time.Date(2012, 1, 1, 0, 0, 0, 0, time.UTC)
)

type fakeRepair struct {
	*RepairIPAddress
	serviceStore     cache.Store
	ipAddressStore   cache.Store
	serviceCIDRStore cache.Store
}

func newFakeRepair(testTime time.Time) (*fake.Clientset, *fakeRepair) {
	fakeClient := fake.NewSimpleClientset()

	informerFactory := informers.NewSharedInformerFactory(fakeClient, 0*time.Second)
	serviceInformer := informerFactory.Core().V1().Services()
	serviceIndexer := serviceInformer.Informer().GetIndexer()

	serviceCIDRInformer := informerFactory.Networking().V1().ServiceCIDRs()
	serviceCIDRIndexer := serviceCIDRInformer.Informer().GetIndexer()

	ipInformer := informerFactory.Networking().V1().IPAddresses()
	ipIndexer := ipInformer.Informer().GetIndexer()

	fakeClient.PrependReactor("create", "ipaddresses", k8stesting.ReactionFunc(func(action k8stesting.Action) (bool, runtime.Object, error) {
		ip := action.(k8stesting.CreateAction).GetObject().(*networkingv1.IPAddress)
		err := ipIndexer.Add(ip)
		return false, ip, err
	}))
	fakeClient.PrependReactor("update", "ipaddresses", k8stesting.ReactionFunc(func(action k8stesting.Action) (bool, runtime.Object, error) {
		ip := action.(k8stesting.UpdateAction).GetObject().(*networkingv1.IPAddress)
		return false, ip, fmt.Errorf("IPAddress is inmutable after creation")
	}))
	fakeClient.PrependReactor("delete", "ipaddresses", k8stesting.ReactionFunc(func(action k8stesting.Action) (bool, runtime.Object, error) {
		ip := action.(k8stesting.DeleteAction).GetName()
		err := ipIndexer.Delete(ip)
		return false, &networkingv1.IPAddress{}, err
	}))
	fakeClock := testclock.NewFakeClock(testTime)
	r := newRepairIPAddress(0*time.Second,
		fakeClient,
		serviceInformer,
		serviceCIDRInformer,
		ipInformer,
		fakeClock,
	)
	return fakeClient, &fakeRepair{r, serviceIndexer, ipIndexer, serviceCIDRIndexer}
}

// TestRepairServiceIP 验证 Service 与 IPAddress 双向引用不一致、缺失或重复时的修复行为。
func TestRepairServiceIP(t *testing.T) {
	tests := []struct {
		name        string
		svcs        []*v1.Service
		ipAddresses []*networkingv1.IPAddress
		cidrs       []*networkingv1.ServiceCIDR
		testTime    time.Time
		expectedIPs []string
		actions     [][]string // verb 与 resource
		events      []string
	}{
		{
			name: "no changes needed single stack",
			svcs: []*v1.Service{
				newServiceWithCreationTimestamp("test-svc", []string{"10.0.1.1"}, testTimeNow.Add(1*time.Second)),
			},
			ipAddresses: []*networkingv1.IPAddress{
				newIPAddressWithCreationTimestamp("10.0.1.1", newService("test-svc", []string{"10.0.1.1"}), testTimeNow),
			},
			cidrs: []*networkingv1.ServiceCIDR{
				newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6),
			},
			testTime:    testTimeNow,
			expectedIPs: []string{"10.0.1.1"},
			actions:     [][]string{},
			events:      []string{},
		},
		{
			name: "no changes needed dual stack",
			svcs: []*v1.Service{
				newServiceWithCreationTimestamp("test-svc", []string{"10.0.1.1", "2001:db8::10"}, testTimeNow.Add(1*time.Second)),
			},
			ipAddresses: []*networkingv1.IPAddress{
				newIPAddressWithCreationTimestamp("10.0.1.1", newService("test-svc", []string{"10.0.1.1"}), testTimeNow),
				newIPAddressWithCreationTimestamp("2001:db8::10", newService("test-svc", []string{"2001:db8::10"}), testTimeNow),
			},
			cidrs: []*networkingv1.ServiceCIDR{
				newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6),
			},
			testTime:    testTimeNow,
			expectedIPs: []string{"10.0.1.1", "2001:db8::10"},
			actions:     [][]string{},
			events:      []string{},
		},
		{
			name: "no changes needed dual stack multiple cidrs",
			svcs: []*v1.Service{
				newServiceWithCreationTimestamp("test-svc", []string{"192.168.0.1", "2001:db8:a:b::10"}, testTimeNow.Add(1*time.Second)),
			},
			ipAddresses: []*networkingv1.IPAddress{
				newIPAddressWithCreationTimestamp("192.168.0.1", newService("test-svc", []string{"192.168.0.1"}), testTimeNow),
				newIPAddressWithCreationTimestamp("2001:db8:a:b::10", newService("test-svc", []string{"2001:db8:a:b::10"}), testTimeNow),
			},
			cidrs: []*networkingv1.ServiceCIDR{
				newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6),
				newServiceCIDR("custom", "192.168.0.0/24", "2001:db8:a:b::/64"),
			},
			testTime:    testTimeNow,
			expectedIPs: []string{"192.168.0.1", "2001:db8:a:b::10"},
			actions:     [][]string{},
			events:      []string{},
		},
		// 这两个用例模拟从位图迁移到 IPAddress 对象的过程
		{
			name: "create IPAddress single stack",
			svcs: []*v1.Service{
				newServiceWithCreationTimestamp("test-svc", []string{"10.0.1.1"}, testTimeNow.Add(1*time.Second)),
			},
			cidrs: []*networkingv1.ServiceCIDR{
				newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6),
			},
			testTime:    testTimeNow,
			expectedIPs: []string{"10.0.1.1"},
			actions:     [][]string{{"create", "ipaddresses"}},
			events:      []string{"Warning ClusterIPNotAllocated Cluster IP [IPv4]: 10.0.1.1 is not allocated; repairing"},
		},
		{
			name: "create IPAddresses dual stack",
			svcs: []*v1.Service{
				newServiceWithCreationTimestamp("test-svc", []string{"10.0.1.1", "2001:db8::10"}, testTimeNow.Add(1*time.Second)),
			},
			cidrs: []*networkingv1.ServiceCIDR{
				newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6),
			},
			testTime:    testTimeNow,
			expectedIPs: []string{"10.0.1.1", "2001:db8::10"},
			actions:     [][]string{{"create", "ipaddresses"}, {"create", "ipaddresses"}},
			events: []string{
				"Warning ClusterIPNotAllocated Cluster IP [IPv4]: 10.0.1.1 is not allocated; repairing",
				"Warning ClusterIPNotAllocated Cluster IP [IPv6]: 2001:db8::10 is not allocated; repairing",
			},
		},
		{
			name: "create IPAddress single stack from secondary",
			svcs: []*v1.Service{
				newServiceWithCreationTimestamp("test-svc", []string{"192.168.1.1"}, testTimeNow.Add(1*time.Second)),
			},
			cidrs: []*networkingv1.ServiceCIDR{
				newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6),
				newServiceCIDR("custom", "192.168.1.0/24", ""),
			},
			testTime:    testTimeNow,
			expectedIPs: []string{"192.168.1.1"},
			actions:     [][]string{{"create", "ipaddresses"}},
			events:      []string{"Warning ClusterIPNotAllocated Cluster IP [IPv4]: 192.168.1.1 is not allocated; repairing"},
		},
		{
			name: "reconcile IPAddress single stack wrong reference",
			svcs: []*v1.Service{
				newServiceWithCreationTimestamp("test-svc", []string{"10.0.1.1"}, testTimeNow.Add(1*time.Second)),
			},
			ipAddresses: []*networkingv1.IPAddress{
				newIPAddressWithCreationTimestamp("10.0.1.1", newService("test-svc2", []string{"10.0.1.1"}), testTimeNow),
			},
			cidrs: []*networkingv1.ServiceCIDR{
				newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6),
			},
			testTime:    testTimeNow,
			expectedIPs: []string{"10.0.1.1"},
			actions:     [][]string{{"delete", "ipaddresses"}, {"create", "ipaddresses"}},
			events:      []string{"Warning ClusterIPNotAllocated the ClusterIP [IPv4]: 10.0.1.1 for Service bar/test-svc has a wrong reference; repairing"},
		},
		{
			name: "reconcile IPAddresses dual stack",
			svcs: []*v1.Service{
				newServiceWithCreationTimestamp("test-svc", []string{"10.0.1.1", "2001:db8::10"}, testTimeNow.Add(-1*time.Second))},
			ipAddresses: []*networkingv1.IPAddress{
				newIPAddressWithCreationTimestamp("10.0.1.1", newService("test-svc2", []string{"10.0.1.1"}), testTimeNow),
				newIPAddressWithCreationTimestamp("2001:db8::10", newService("test-svc2", []string{"2001:db8::10"}), testTimeNow),
			},
			cidrs: []*networkingv1.ServiceCIDR{
				newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6),
			},
			testTime:    testTimeNow,
			expectedIPs: []string{"10.0.1.1", "2001:db8::10"},
			actions:     [][]string{{"delete", "ipaddresses"}, {"create", "ipaddresses"}, {"delete", "ipaddresses"}, {"create", "ipaddresses"}},
			events: []string{
				"Warning ClusterIPNotAllocated the ClusterIP [IPv4]: 10.0.1.1 for Service bar/test-svc has a wrong reference; repairing",
				"Warning ClusterIPNotAllocated the ClusterIP [IPv6]: 2001:db8::10 for Service bar/test-svc has a wrong reference; repairing",
			},
		},
		{
			name: "one IP out of range",
			svcs: []*v1.Service{
				newServiceWithCreationTimestamp("test-svc", []string{"192.168.1.1", "2001:db8::10"}, testTimeNow.Add(1*time.Second)),
			},
			ipAddresses: []*networkingv1.IPAddress{
				newIPAddressWithCreationTimestamp("192.168.1.1", newService("test-svc", []string{"192.168.1.1"}), testTimeNow),
				newIPAddressWithCreationTimestamp("2001:db8::10", newService("test-svc", []string{"2001:db8::10"}), testTimeNow),
			},
			cidrs: []*networkingv1.ServiceCIDR{
				newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6),
			},
			testTime:    testTimeNow,
			expectedIPs: []string{"2001:db8::10"},
			actions:     [][]string{},
			events:      []string{"Warning ClusterIPOutOfRange Cluster IP [IPv4]: 192.168.1.1 is not within any configured Service CIDR; please recreate service"},
		},
		{
			name: "one IP orphan within grace period",
			ipAddresses: []*networkingv1.IPAddress{
				newIPAddressWithCreationTimestamp("10.0.1.1", newService("test-svc", []string{"10.0.1.1"}), testTimeNow),
			},
			cidrs: []*networkingv1.ServiceCIDR{
				newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6),
			},
		},
		{
			name: "one IP orphan after grace period",
			ipAddresses: []*networkingv1.IPAddress{
				newIPAddressWithCreationTimestamp("10.0.1.1", newService("test-svc", []string{"10.0.1.1"}), testTimeNow),
			},
			cidrs: []*networkingv1.ServiceCIDR{
				newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6),
			},
			// 这里推进伪造时钟，模拟 IPAddress 创建于 70 多秒之前，
			// 也就是阻止清理 IPAddress 的宽限期已经结束。
			testTime: testTimeNow.Add(70 * time.Second),
			actions:  [][]string{{"delete", "ipaddresses"}},
			events:   []string{"Warning IPAddressNotAllocated IPAddress: 10.0.1.1 for Service bar/test-svc appears to have leaked: cleaning up"},
		},
		{
			name: "one IP out of range matching the network address",
			svcs: []*v1.Service{
				newServiceWithCreationTimestamp("test-svc", []string{"10.0.0.0"}, testTimeNow.Add(1*time.Second)),
			},
			ipAddresses: []*networkingv1.IPAddress{
				newIPAddressWithCreationTimestamp("10.0.0.0", newService("test-svc", []string{"10.0.0.0"}), testTimeNow),
			},
			cidrs: []*networkingv1.ServiceCIDR{
				newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6),
			},
			testTime:    testTimeNow,
			expectedIPs: []string{"10.0.0.0"},
			actions:     [][]string{},
			events:      []string{"Warning ClusterIPOutOfRange Cluster IP [IPv4]: 10.0.0.0 is not within any configured Service CIDR; please recreate service"},
		},
		{
			name: "one IP out of range matching the broadcast address",
			svcs: []*v1.Service{
				newServiceWithCreationTimestamp("test-svc", []string{"10.0.255.255"}, testTimeNow.Add(1*time.Second)),
			},
			ipAddresses: []*networkingv1.IPAddress{
				newIPAddressWithCreationTimestamp("10.0.255.255", newService("test-svc", []string{"10.0.255.255"}), testTimeNow),
			},
			cidrs: []*networkingv1.ServiceCIDR{
				newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6),
			},
			testTime:    testTimeNow,
			expectedIPs: []string{"10.0.255.255"},
			actions:     [][]string{},
			events:      []string{"Warning ClusterIPOutOfRange Cluster IP [IPv4]: 10.0.255.255 is not within any configured Service CIDR; please recreate service"},
		},
		{
			name: "one IPv6 out of range matching the subnet address",
			svcs: []*v1.Service{
				newServiceWithCreationTimestamp("test-svc", []string{"2001:db8::"}, testTimeNow.Add(1*time.Second)),
			},
			ipAddresses: []*networkingv1.IPAddress{
				newIPAddressWithCreationTimestamp("2001:db8::", newService("test-svc", []string{"2001:db8::"}), testTimeNow),
			},
			cidrs: []*networkingv1.ServiceCIDR{
				newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6),
			},
			testTime:    testTimeNow,
			expectedIPs: []string{"2001:db8::"},
			actions:     [][]string{},
			events:      []string{"Warning ClusterIPOutOfRange Cluster IP [IPv6]: 2001:db8:: is not within any configured Service CIDR; please recreate service"},
		},
		{
			name: "one IPv6 matching the broadcast address",
			svcs: []*v1.Service{
				newServiceWithCreationTimestamp("test-svc", []string{"2001:db8::ffff:ffff:ffff:ffff"}, testTimeNow.Add(1*time.Second)),
			},
			ipAddresses: []*networkingv1.IPAddress{
				newIPAddressWithCreationTimestamp("2001:db8::ffff:ffff:ffff:ffff", newService("test-svc", []string{"2001:db8::ffff:ffff:ffff:ffff"}), testTimeNow),
			},
			cidrs: []*networkingv1.ServiceCIDR{
				newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6),
			},
			testTime:    testTimeNow,
			expectedIPs: []string{"2001:db8::ffff:ffff:ffff:ffff"},
		},
		{
			name: "one IP orphan matching the broadcast address within grace period",
			ipAddresses: []*networkingv1.IPAddress{
				newIPAddressWithCreationTimestamp("10.0.255.255", newService("test-svc", []string{"10.0.255.255"}), testTimeNow),
			},
			cidrs: []*networkingv1.ServiceCIDR{
				newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6),
			},
			testTime: testTimeNow,
		},
		{
			name: "one IP orphan matching the broadcast address after grace period",
			ipAddresses: []*networkingv1.IPAddress{
				// 这里推进伪造时钟，模拟 IPAddress 创建于 90 多秒之前，
				// 也就是阻止清理 IPAddress 的宽限期已经结束。
				newIPAddressWithCreationTimestamp("10.0.255.255", newService("test-svc", []string{"10.0.255.255"}), testTimeNow),
			},
			cidrs: []*networkingv1.ServiceCIDR{
				newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6),
			},
			testTime: testTimeNow.Add(90 * time.Second),
			actions:  [][]string{{"delete", "ipaddresses"}},
			events:   []string{"Warning IPAddressNotAllocated IPAddress: 10.0.255.255 for Service bar/test-svc appears to have leaked: cleaning up"},
		},
		{
			name: "Two IPAddresses referencing the same service",
			svcs: []*v1.Service{
				newServiceWithCreationTimestamp("test-svc", []string{"10.0.1.1"}, testTimeNow.Add(1*time.Second)),
			},
			ipAddresses: []*networkingv1.IPAddress{
				newIPAddressWithCreationTimestamp("10.0.1.1", newService("test-svc", []string{"10.0.1.1"}), testTimeNow),
				newIPAddressWithCreationTimestamp("10.0.1.2", newService("test-svc", []string{"10.0.1.1"}), testTimeNow),
			},
			cidrs: []*networkingv1.ServiceCIDR{
				newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6),
			},
			testTime: testTimeNow,
			actions:  [][]string{{"delete", "ipaddresses"}},
			events:   []string{"Warning IPAddressWrongReference IPAddress: 10.0.1.2 for Service bar/test-svc has a wrong reference; cleaning up"},
		},
		{
			name: "Two IPAddresses referencing the same service when service create is called again and ip address is in grace period",
			svcs: []*v1.Service{
				newServiceWithCreationTimestamp("test-svc", []string{"10.0.1.1"}, testTimeNow.Add(1*time.Second)),
			},
			ipAddresses: []*networkingv1.IPAddress{
				newIPAddressWithCreationTimestamp("10.0.1.1", newService("test-svc", []string{"10.0.1.1"}), testTimeNow),
				// 确保第二个 IPAddress 对象的创建时间晚于 Service 的创建时间
				newIPAddressWithCreationTimestamp("10.0.1.2", newService("test-svc", []string{"10.0.1.1"}), testTimeNow.Add(2*time.Second)),
			},
			cidrs: []*networkingv1.ServiceCIDR{
				newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6),
			},
			// 调整测试时间，确保第二个 IPAddress 对象创建后不足 5 秒
			testTime: testTimeNow.Add(3 * time.Second),
		},
		{
			name: "Two IPAddresses referencing the same service when service create is called again and ip address is not in grace period",
			svcs: []*v1.Service{
				newServiceWithCreationTimestamp("test-svc", []string{"10.0.1.1"}, testTimeNow.Add(1*time.Second)),
			},
			ipAddresses: []*networkingv1.IPAddress{
				newIPAddressWithCreationTimestamp("10.0.1.1", newService("test-svc", []string{"10.0.1.1"}), testTimeNow),
				// 确保第二个 IPAddress 对象的创建时间晚于 Service 的创建时间
				newIPAddressWithCreationTimestamp("10.0.1.2", newService("test-svc", []string{"10.0.1.1"}), testTimeNow.Add(2*time.Second)),
			},
			cidrs: []*networkingv1.ServiceCIDR{
				newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6),
			},
			// 调整测试时间，确保第二个 IPAddress 对象创建已超过 5 秒
			testTime: testTimeNow.Add(10 * time.Second),
			actions:  [][]string{{"delete", "ipaddresses"}},
			events:   []string{"Warning IPAddressWrongReference IPAddress: 10.0.1.2 for Service bar/test-svc has a wrong reference; cleaning up"},
		},
		{
			name: "Two Services with same ClusterIP",
			svcs: []*v1.Service{
				newServiceWithCreationTimestamp("test-svc", []string{"10.0.1.1"}, testTimeNow.Add(1*time.Second)),
				newServiceWithCreationTimestamp("test-svc2", []string{"10.0.1.1"}, testTimeNow.Add(1*time.Second)),
			},
			ipAddresses: []*networkingv1.IPAddress{
				newIPAddressWithCreationTimestamp("10.0.1.1", newService("test-svc2", []string{"10.0.1.1"}), testTimeNow),
			},
			cidrs: []*networkingv1.ServiceCIDR{
				newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6),
			},
			testTime: testTimeNow,
			events:   []string{"Warning ClusterIPAlreadyAllocated Cluster IP [IPv4]:10.0.1.1 was assigned to multiple services; please recreate service"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			c, r := newFakeRepair(test.testTime)
			// 添加 CIDR
			for _, cidr := range test.cidrs {
				err := r.serviceCIDRStore.Add(cidr)
				if err != nil {
					t.Errorf("Unexpected error trying to add Service %v object: %v", cidr, err)
				}
			}

			// 为测试覆盖默认取值
			r.servicesSynced = func() bool { return true }
			r.ipAddressSynced = func() bool { return true }
			r.serviceCIDRSynced = func() bool { return true }
			recorder := events.NewFakeRecorder(100)
			r.recorder = recorder
			for _, svc := range test.svcs {
				err := r.serviceStore.Add(svc)
				if err != nil {
					t.Errorf("Unexpected error trying to add Service %v object: %v", svc, err)
				}
			}

			for _, ip := range test.ipAddresses {
				err := r.ipAddressStore.Add(ip)
				if err != nil {
					t.Errorf("Unexpected error trying to add IPAddress %s object: %v", ip, err)
				}
			}

			if err := r.runOnce(); err != nil {
				t.Fatal(err)
			}

			for _, ip := range test.expectedIPs {
				_, err := r.ipAddressLister.Get(ip)
				if err != nil {
					t.Errorf("Unexpected error trying to get IPAddress %s object: %v", ip, err)
				}
			}

			expectAction(t, c.Actions(), test.actions)
			expectEvents(t, recorder.Events, test.events)
		})
	}

}

func TestRepairIPAddress_syncIPAddress(t *testing.T) {
	tests := []struct {
		name     string
		ip       *networkingv1.IPAddress
		svc      *v1.Service
		testTime time.Time
		actions  [][]string // verb 与 resource
		wantErr  bool
	}{
		{
			name: "ExternalName Service with late IPAddress",
			ip: &networkingv1.IPAddress{
				ObjectMeta: metav1.ObjectMeta{
					Name: "10.0.1.1",
					Labels: map[string]string{
						networkingv1.LabelIPAddressFamily: string(v1.IPv4Protocol),
						networkingv1.LabelManagedBy:       ipallocator.ControllerName,
					},
					CreationTimestamp: metav1.Time{Time: testTimeNow.Add(10 * time.Second)},
				},
				Spec: networkingv1.IPAddressSpec{
					ParentRef: &networkingv1.ParentReference{
						Group:     "",
						Resource:  "services",
						Name:      "foo",
						Namespace: "bar",
					},
				},
			},
			svc:      newExternalNameService("foo", testTimeNow),
			testTime: testTimeNow.Add(2 * time.Second),
		},
		{
			name: "ExternalName Service with late IPAddress after threshold",
			ip: &networkingv1.IPAddress{
				ObjectMeta: metav1.ObjectMeta{
					Name: "10.0.1.1",
					Labels: map[string]string{
						networkingv1.LabelIPAddressFamily: string(v1.IPv4Protocol),
						networkingv1.LabelManagedBy:       ipallocator.ControllerName,
					},
					CreationTimestamp: metav1.Time{Time: testTimeNow.Add(10 * time.Second)},
				},
				Spec: networkingv1.IPAddressSpec{
					ParentRef: &networkingv1.ParentReference{
						Group:     "",
						Resource:  "services",
						Name:      "foo",
						Namespace: "bar",
					},
				},
			},
			svc:      newExternalNameService("foo", testTimeNow),
			testTime: testTimeNow.Add(71 * time.Second),
			actions:  [][]string{{"delete", "ipaddresses"}},
		},
		{
			name: "correct ipv4 address",
			ip: &networkingv1.IPAddress{
				ObjectMeta: metav1.ObjectMeta{
					Name: "10.0.1.1",
					Labels: map[string]string{
						networkingv1.LabelIPAddressFamily: string(v1.IPv4Protocol),
						networkingv1.LabelManagedBy:       ipallocator.ControllerName,
					},
					CreationTimestamp: metav1.Time{Time: testTimeNow},
				},
				Spec: networkingv1.IPAddressSpec{
					ParentRef: &networkingv1.ParentReference{
						Group:     "",
						Resource:  "services",
						Name:      "foo",
						Namespace: "bar",
					},
				},
			},
			testTime: testTimeNow,
		},
		{
			name: "correct ipv6 address",
			ip: &networkingv1.IPAddress{
				ObjectMeta: metav1.ObjectMeta{
					Name: "2001:db8::11",
					Labels: map[string]string{
						networkingv1.LabelIPAddressFamily: string(v1.IPv6Protocol),
						networkingv1.LabelManagedBy:       ipallocator.ControllerName,
					},
					CreationTimestamp: metav1.Time{Time: testTimeNow},
				},
				Spec: networkingv1.IPAddressSpec{
					ParentRef: &networkingv1.ParentReference{
						Group:     "",
						Resource:  "services",
						Name:      "foo",
						Namespace: "bar",
					},
				},
			},
			testTime: testTimeNow,
		},
		{
			name: "not managed by this controller",
			ip: &networkingv1.IPAddress{
				ObjectMeta: metav1.ObjectMeta{
					Name: "2001:db8::11",
					Labels: map[string]string{
						networkingv1.LabelIPAddressFamily: string(v1.IPv6Protocol),
						networkingv1.LabelManagedBy:       "controller-foo",
					},
					CreationTimestamp: metav1.Time{Time: testTimeNow},
				},
				Spec: networkingv1.IPAddressSpec{
					ParentRef: &networkingv1.ParentReference{
						Group:     "networking.gateway.k8s.io",
						Resource:  "gateway",
						Name:      "foo",
						Namespace: "bar",
					},
				},
			},
			testTime: testTimeNow,
		},
		{
			name: "out of range",
			ip: &networkingv1.IPAddress{
				ObjectMeta: metav1.ObjectMeta{
					Name: "fd00:db8::11",
					Labels: map[string]string{
						networkingv1.LabelIPAddressFamily: string(v1.IPv6Protocol),
						networkingv1.LabelManagedBy:       ipallocator.ControllerName,
					},
					CreationTimestamp: metav1.Time{Time: testTimeNow},
				},
				Spec: networkingv1.IPAddressSpec{
					ParentRef: &networkingv1.ParentReference{
						Group:     "",
						Resource:  "services",
						Name:      "foo",
						Namespace: "bar",
					},
				},
			},
			testTime: testTimeNow,
		},
		{
			name: "leaked ip",
			ip: &networkingv1.IPAddress{
				ObjectMeta: metav1.ObjectMeta{
					Name: "10.0.1.1",
					Labels: map[string]string{
						networkingv1.LabelIPAddressFamily: string(v1.IPv6Protocol),
						networkingv1.LabelManagedBy:       ipallocator.ControllerName,
					},
					CreationTimestamp: metav1.Time{Time: testTimeNow},
				},
				Spec: networkingv1.IPAddressSpec{
					ParentRef: &networkingv1.ParentReference{
						Group:     "",
						Resource:  "services",
						Name:      "noexist",
						Namespace: "bar",
					},
				},
			},
			// 这里推进伪造时钟，模拟 IPAddress 创建于 90 多秒之前，
			// 也就是阻止清理 IPAddress 的宽限期已经结束。
			testTime: testTimeNow.Add(90 * time.Second),
			actions:  [][]string{{"delete", "ipaddresses"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, r := newFakeRepair(tt.testTime)
			err := r.ipAddressStore.Add(tt.ip)
			if err != nil {
				t.Fatal(err)
			}
			svc := tt.svc
			if svc == nil {
				svc = newService("foo", []string{tt.ip.Name})
			}
			err = r.serviceStore.Add(svc)
			if err != nil {
				t.Fatal(err)
			}

			// 为测试覆盖默认取值
			r.servicesSynced = func() bool { return true }
			r.ipAddressSynced = func() bool { return true }
			recorder := events.NewFakeRecorder(100)
			r.recorder = recorder
			if err := r.syncIPAddress(tt.ip.Name); (err != nil) != tt.wantErr {
				t.Errorf("RepairIPAddress.syncIPAddress() error = %v, wantErr %v", err, tt.wantErr)
			}
			expectAction(t, c.Actions(), tt.actions)

		})
	}
}

func newExternalNameService(name string, timestamp time.Time) *v1.Service {
	return &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "bar",
			Name:              name,
			CreationTimestamp: metav1.Time{Time: timestamp},
		},
		Spec: v1.ServiceSpec{
			Type:         v1.ServiceTypeExternalName,
			ExternalName: "foo.bar.com",
		},
	}
}

func newIPAddressWithCreationTimestamp(name string, svc *v1.Service, timestamp time.Time) *networkingv1.IPAddress {
	ipAddress := newIPAddress(name, svc)
	ipAddress.CreationTimestamp = metav1.Time{Time: timestamp}
	return ipAddress
}

func newServiceWithCreationTimestamp(name string, ips []string, timestamp time.Time) *v1.Service {
	service := newService(name, ips)
	service.CreationTimestamp = metav1.Time{Time: timestamp}
	return service
}

func newService(name string, ips []string) *v1.Service {
	if len(ips) == 0 {
		return nil
	}
	svc := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:         "bar",
			Name:              name,
			CreationTimestamp: metav1.Time{Time: time.Now()}},
		Spec: v1.ServiceSpec{
			ClusterIP:  ips[0],
			ClusterIPs: ips,
			Type:       v1.ServiceTypeClusterIP,
		},
	}
	return svc
}

func newServiceCIDR(name, primary, secondary string) *networkingv1.ServiceCIDR {
	serviceCIDR := &networkingv1.ServiceCIDR{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: networkingv1.ServiceCIDRSpec{},
	}
	serviceCIDR.Spec.CIDRs = append(serviceCIDR.Spec.CIDRs, primary)
	if secondary != "" {
		serviceCIDR.Spec.CIDRs = append(serviceCIDR.Spec.CIDRs, secondary)
	}
	return serviceCIDR
}

func expectAction(t *testing.T, actions []k8stesting.Action, expected [][]string) {
	t.Helper()
	if len(actions) != len(expected) {
		t.Fatalf("Expected at least %d actions, got %d \ndiff: %v", len(expected), len(actions), cmp.Diff(expected, actions))
	}

	for i, action := range actions {
		verb := expected[i][0]
		if action.GetVerb() != verb {
			t.Errorf("Expected action %d verb to be %s, got %s", i, verb, action.GetVerb())
		}
		resource := expected[i][1]
		if action.GetResource().Resource != resource {
			t.Errorf("Expected action %d resource to be %s, got %s", i, resource, action.GetResource().Resource)
		}
	}
}

func expectEvents(t *testing.T, actual <-chan string, expected []string) {
	t.Helper()
	c := time.After(wait.ForeverTestTimeout)
	for _, e := range expected {
		select {
		case a := <-actual:
			if e != a {
				t.Errorf("Expected event %q, got %q", e, a)
				return
			}
		case <-c:
			t.Errorf("Expected event %q, got nothing", e)
			// 继续遍历，打印全部预期事件
		}
	}
	for {
		select {
		case a := <-actual:
			t.Errorf("Unexpected event: %q", a)
		default:
			return // 如预期，没有更多事件。
		}
	}
}

// TestRepairIPAddress_runOnce 验证一次全量协调会同时检查 Service 所需地址和由分配控制器创建的 IPAddress 孤儿对象。
func TestRepairIPAddress_runOnce(t *testing.T) {
	tests := []struct {
		name          string
		conflictCount int
		errorType     func(string) error
		expectedRuns  int
		expectedErr   bool
	}{
		{
			name:          "Retry on Conflict",
			conflictCount: 2,
			errorType: func(name string) error {
				return apierrors.NewConflict(networkingv1.Resource("ipaddresses"), name, fmt.Errorf("conflict"))
			},
			expectedRuns: 3,
			expectedErr:  false,
		},
		{
			name:          "Retry on Forbidden",
			conflictCount: 2,
			errorType: func(name string) error {
				return apierrors.NewForbidden(networkingv1.Resource("ipaddresses"), name, fmt.Errorf("forbidden"))
			},
			expectedRuns: 3,
			expectedErr:  false,
		},
		{
			name:          "No Retry on InternalError",
			conflictCount: 1, // 它会失败一次后停止
			errorType: func(name string) error {
				return apierrors.NewInternalError(fmt.Errorf("internal error"))
			},
			expectedRuns: 1,
			expectedErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, r := newFakeRepair(testTimeNow)
			// 加入一个需要修复（缺少 IPAddress）的 Service
			svc := newService("test-svc", []string{"10.0.1.1"})
			err := r.serviceStore.Add(svc)
			if err != nil {
				t.Fatalf("Unexpected error adding service: %v", err)
			}
			r.servicesSynced = func() bool { return true }
			r.ipAddressSynced = func() bool { return true }
			r.serviceCIDRSynced = func() bool { return true }

			// 加入一个默认 ServiceCIDR
			cidr := newServiceCIDR("kubernetes", serviceCIDRv4, serviceCIDRv6)
			err = r.serviceCIDRStore.Add(cidr)
			if err != nil {
				t.Fatalf("Unexpected error adding ServiceCIDR: %v", err)
			}

			// 统计 create 被调用的次数
			createCalls := 0
			client.PrependReactor("create", "ipaddresses", func(action k8stesting.Action) (bool, runtime.Object, error) {
				createCalls++
				if createCalls <= tt.conflictCount {
					return true, nil, tt.errorType("10.0.1.1")
				}
				// 透传给默认的 reactor（由它真正创建对象）
				return false, nil, nil
			})

			err = r.runOnce()
			if (err != nil) != tt.expectedErr {
				t.Errorf("runOnce() error = %v, expectedErr %v", err, tt.expectedErr)
			}

			if createCalls != tt.expectedRuns {
				t.Errorf("Expected %d create calls, got %d", tt.expectedRuns, createCalls)
			}
		})
	}
}
