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

package servicecidrs

import (
	"context"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	v1 "k8s.io/api/core/v1"
	networkingapiv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controlplane/controller/defaultservicecidr"
	"k8s.io/kubernetes/pkg/registry/core/service/ipallocator"
	"k8s.io/kubernetes/test/utils/ktesting"
	netutils "k8s.io/utils/net"
	"k8s.io/utils/ptr"
)

// testController 在真正的 Controller 之外暴露 informer 的存储对象，
// 方便测试直接预置 ServiceCIDR/IPAddress 并观察事件效果。
type testController struct {
	*Controller
	servicecidrsStore cache.Store
	ipaddressesStore  cache.Store
}

// newController 用 fake client 构造控制器，并把传入对象直接塞进 informer 存储；
// 由于 informer 未真正运行，这里把同步判断替换成永远就绪，确保 sync 不被缓存状态阻塞。
func newController(ctx context.Context, t *testing.T, cidrs []*networkingapiv1.ServiceCIDR, ips []*networkingapiv1.IPAddress) (*fake.Clientset, *testController) {
	client := fake.NewSimpleClientset()

	informerFactory := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())

	serviceCIDRInformer := informerFactory.Networking().V1().ServiceCIDRs()
	cidrStore := serviceCIDRInformer.Informer().GetStore()
	for _, obj := range cidrs {
		err := cidrStore.Add(obj)
		if err != nil {
			t.Fatal(err)
		}
	}
	ipAddressInformer := informerFactory.Networking().V1().IPAddresses()
	ipStore := ipAddressInformer.Informer().GetStore()
	for _, obj := range ips {
		err := ipStore.Add(obj)
		if err != nil {
			t.Fatal(err)
		}
	}
	controller := NewController(
		ctx,
		serviceCIDRInformer,
		ipAddressInformer,
		client)

	var alwaysReady = func() bool { return true }
	controller.serviceCIDRsSynced = alwaysReady
	controller.ipAddressSynced = alwaysReady

	return client, &testController{
		controller,
		cidrStore,
		ipStore,
	}
}

// TestControllerSync 覆盖 sync 在删除宽限期、finalizer 增删、IP 占用与 CIDR 重叠等组合下的行为。
// 断言只检查 API 动作序列，因为 fake client 的 server-side apply 行为不可靠（见注释中的 issue）。
func TestControllerSync(t *testing.T) {
	now := time.Now()

	// 刚进入删除流程、仍在宽限期内的 CIDR：此时不应移除 finalizer。
	deletingServiceCIDR := makeServiceCIDR("deleting-cidr", "192.168.0.0/24", "2001:db2::/64")
	deletingServiceCIDR.Finalizers = []string{ServiceCIDRProtectionFinalizer}
	deletingServiceCIDR.DeletionTimestamp = ptr.To[metav1.Time](metav1.Now())

	// 删除时间已超过宽限期的 CIDR：允许推进 finalizer 移除流程。
	deletedServiceCIDR := makeServiceCIDR("deleted-cidr", "192.168.0.0/24", "2001:db2::/64")
	deletedServiceCIDR.Finalizers = []string{ServiceCIDRProtectionFinalizer}
	deletedServiceCIDR.DeletionTimestamp = ptr.To[metav1.Time](metav1.NewTime(now.Add(-deletionGracePeriod - 1*time.Second)))

	testCases := []struct {
		name       string
		cidrs      []*networkingapiv1.ServiceCIDR
		ips        []*networkingapiv1.IPAddress
		cidrSynced string
		actions    [][]string // verb and resource and subresource
	}{
		{
			name: "no existing service CIDRs",
		},
		{
			name: "default service CIDR must have finalizer",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			},
			cidrSynced: defaultservicecidr.DefaultServiceCIDRName,
			actions:    [][]string{{"patch", "servicecidrs", ""}, {"patch", "servicecidrs", "status"}},
		},
		{
			name: "service CIDR must have finalizer",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR("no-finalizer", "192.168.0.0/24", "2001:db2::/64"),
			},
			cidrSynced: "no-finalizer",
			actions:    [][]string{{"patch", "servicecidrs", ""}, {"patch", "servicecidrs", "status"}},
		},
		{
			name: "service CIDR being deleted must remove the finalizer",
			cidrs: []*networkingapiv1.ServiceCIDR{
				deletedServiceCIDR,
			},
			cidrSynced: deletedServiceCIDR.Name,
			actions:    [][]string{{"patch", "servicecidrs", ""}},
		},
		{
			name: "service CIDR being deleted but within the grace period must be requeued not remove the finalizer", // TODO: assert is actually requeued
			cidrs: []*networkingapiv1.ServiceCIDR{
				deletingServiceCIDR,
			},
			cidrSynced: deletingServiceCIDR.Name,
			actions:    [][]string{},
		},
		{
			name: "service CIDR being deleted with IPv4 addresses should update the status",
			cidrs: []*networkingapiv1.ServiceCIDR{
				deletedServiceCIDR,
			},
			ips: []*networkingapiv1.IPAddress{
				makeIPAddress("192.168.0.1"),
			},
			cidrSynced: deletedServiceCIDR.Name,
			actions:    [][]string{{"patch", "servicecidrs", "status"}},
		},
		{
			name: "service CIDR being deleted and overlapping same range and IPv4 addresses should remove the finalizer",
			cidrs: []*networkingapiv1.ServiceCIDR{
				deletedServiceCIDR,
				makeServiceCIDR("overlapping", "192.168.0.0/24", "2001:db2::/64"),
			},
			ips: []*networkingapiv1.IPAddress{
				makeIPAddress("192.168.0.1"),
			},
			cidrSynced: deletedServiceCIDR.Name,
			actions:    [][]string{{"patch", "servicecidrs", ""}},
		},
		{
			name: "service CIDR being deleted and overlapping and IPv4 addresses should remove the finalizer",
			cidrs: []*networkingapiv1.ServiceCIDR{
				deletedServiceCIDR,
				makeServiceCIDR("overlapping", "192.168.0.0/16", "2001:db2::/64"),
			},
			ips: []*networkingapiv1.IPAddress{
				makeIPAddress("192.168.0.1"),
			},
			cidrSynced: deletedServiceCIDR.Name,
			actions:    [][]string{{"patch", "servicecidrs", ""}},
		},
		{
			name: "service CIDR being deleted and not overlapping and IPv4 addresses should update the status",
			cidrs: []*networkingapiv1.ServiceCIDR{
				deletedServiceCIDR,
				makeServiceCIDR("overlapping", "192.168.255.0/26", "2001:db2::/64"),
			},
			ips: []*networkingapiv1.IPAddress{
				makeIPAddress("192.168.0.1"),
			},
			cidrSynced: deletedServiceCIDR.Name,
			actions:    [][]string{{"patch", "servicecidrs", "status"}},
		},
		{
			name: "service CIDR being deleted with IPv6 addresses should update the status",
			cidrs: []*networkingapiv1.ServiceCIDR{
				deletedServiceCIDR,
			},
			ips: []*networkingapiv1.IPAddress{
				makeIPAddress("2001:db2::1"),
			},
			cidrSynced: deletedServiceCIDR.Name,
			actions:    [][]string{{"patch", "servicecidrs", "status"}},
		},
		{
			name: "service CIDR being deleted and overlapping same range and IPv6 addresses should remove the finalizer",
			cidrs: []*networkingapiv1.ServiceCIDR{
				deletedServiceCIDR,
				makeServiceCIDR("overlapping", "192.168.0.0/24", "2001:db2::/64"),
			},
			ips: []*networkingapiv1.IPAddress{
				makeIPAddress("2001:db2::1"),
			},
			cidrSynced: deletedServiceCIDR.Name,
			actions:    [][]string{{"patch", "servicecidrs", ""}},
		},
		{
			name: "service CIDR being deleted and overlapping and IPv6 addresses should remove the finalizer",
			cidrs: []*networkingapiv1.ServiceCIDR{
				deletedServiceCIDR,
				makeServiceCIDR("overlapping", "192.168.0.0/16", "2001:db2::/48"),
			},
			ips: []*networkingapiv1.IPAddress{
				makeIPAddress("2001:db2::1"),
			},
			cidrSynced: deletedServiceCIDR.Name,
			actions:    [][]string{{"patch", "servicecidrs", ""}},
		},
		{
			name: "service CIDR being deleted and not overlapping and IPv6 addresses should update the status",
			cidrs: []*networkingapiv1.ServiceCIDR{
				deletedServiceCIDR,
				makeServiceCIDR("overlapping", "192.168.255.0/26", "2001:db2:a:b::/64"),
			},
			ips: []*networkingapiv1.IPAddress{
				makeIPAddress("2001:db2::1"),
			},
			cidrSynced: deletedServiceCIDR.Name,
			actions:    [][]string{{"patch", "servicecidrs", "status"}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			client, controller := newController(tCtx, t, tc.cidrs, tc.ips)
			// fake client 对 server-side apply 支持不佳（见 issue 99953），
			// 因此忽略 sync 返回的错误，只断言实际发出的 API 动作。
			_ = controller.sync(tCtx, tc.cidrSynced)
			expectAction(t, client.Actions(), tc.actions)

		})
	}
}

// makeServiceCIDR 构造带可选主/辅 CIDR 的测试对象；secondary 为空时只设置主 CIDR。
func makeServiceCIDR(name, primary, secondary string) *networkingapiv1.ServiceCIDR {
	serviceCIDR := &networkingapiv1.ServiceCIDR{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: networkingapiv1.ServiceCIDRSpec{},
	}
	serviceCIDR.Spec.CIDRs = append(serviceCIDR.Spec.CIDRs, primary)
	if secondary != "" {
		serviceCIDR.Spec.CIDRs = append(serviceCIDR.Spec.CIDRs, secondary)
	}
	return serviceCIDR
}

// makeIPAddress 按地址字面量推断地址族标签，并打上由本控制器管理的标记，
// 使构造出的对象与真实分配记录一致。
func makeIPAddress(name string) *networkingapiv1.IPAddress {
	family := string(v1.IPv4Protocol)
	if netutils.IsIPv6String(name) {
		family = string(v1.IPv6Protocol)
	}
	return &networkingapiv1.IPAddress{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				networkingapiv1.LabelIPAddressFamily: family,
				networkingapiv1.LabelManagedBy:       ipallocator.ControllerName,
			},
		},
	}
}

// expectAction 按顺序比对 API 动作的动词、资源和子资源；数量必须完全一致，
// 因为多一次 patch（比如误改 status）本身就是需要捕获的行为错误。
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
		subresource := expected[i][2]
		if action.GetSubresource() != subresource {
			t.Errorf("Expected action %d subresource to be %s, got %s", i, subresource, action.GetSubresource())
		}
	}
}

// TestController_canDeleteCIDR 验证删除安全性：只有没有任何 IP 仍由待删 CIDR 独占、
// 且其他 CIDR 能接手这些 IP 时，才允许移除 finalizer。覆盖子网/广播地址与重叠 CIDR 场景。
func TestController_canDeleteCIDR(t *testing.T) {
	tests := []struct {
		name       string
		cidrs      []*networkingapiv1.ServiceCIDR
		ips        []*networkingapiv1.IPAddress
		cidrSynced *networkingapiv1.ServiceCIDR
		want       bool
	}{
		{
			name:       "empty",
			cidrSynced: makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			want:       true,
		},
		{
			name: "CIDR and no IPs",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			},
			cidrSynced: makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			want:       true,
		},
		{
			name: "CIDR with IPs",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			},
			ips: []*networkingapiv1.IPAddress{
				makeIPAddress("192.168.0.24"),
			},
			cidrSynced: makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			want:       false,
		},
		{
			name: "CIDR without IPs",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			},
			ips: []*networkingapiv1.IPAddress{
				makeIPAddress("192.168.1.24"),
			},
			cidrSynced: makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			want:       true,
		},
		{
			name: "CIDR with IPv4 address referencing the subnet address",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			},
			ips: []*networkingapiv1.IPAddress{
				makeIPAddress("192.168.0.0"),
			},
			cidrSynced: makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			want:       true,
		},
		{
			name: "CIDR with IPv4 address referencing the broadcast address",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			},
			ips: []*networkingapiv1.IPAddress{
				makeIPAddress("192.168.0.255"),
			},
			cidrSynced: makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			want:       true,
		},
		{
			name: "CIDR with IPv6 address referencing the broadcast address",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			},
			ips: []*networkingapiv1.IPAddress{
				makeIPAddress("2001:0db2::ffff:ffff:ffff:ffff"),
			},
			cidrSynced: makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			want:       false,
		},
		{
			name: "CIDR with same range overlapping and IPs",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
				makeServiceCIDR("overlapping", "192.168.0.0/24", "2001:db2::/64"),
			},
			ips: []*networkingapiv1.IPAddress{
				makeIPAddress("192.168.0.23"),
			},
			cidrSynced: makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			want:       true,
		},
		{
			name: "CIDR with smaller range overlapping and IPs",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
				makeServiceCIDR("overlapping", "192.168.0.0/26", "2001:db2::/64"),
			},
			ips: []*networkingapiv1.IPAddress{
				makeIPAddress("192.168.0.23"),
			},
			cidrSynced: makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			want:       true,
		},
		{
			name: "CIDR with smaller range overlapping but IPs orphan",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
				makeServiceCIDR("overlapping", "192.168.0.0/28", "2001:db2::/64"),
			},
			ips: []*networkingapiv1.IPAddress{
				makeIPAddress("192.168.0.23"),
			},
			cidrSynced: makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			want:       false,
		},
		{
			name: "CIDR with larger range overlapping and IPs",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
				makeServiceCIDR("overlapping", "192.168.0.0/16", "2001:db2::/64"),
			},
			ips: []*networkingapiv1.IPAddress{
				makeIPAddress("192.168.0.23"),
			},
			cidrSynced: makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			want:       true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			_, controller := newController(tCtx, t, tc.cidrs, tc.ips)
			got, err := controller.canDeleteCIDR(tCtx, tc.cidrSynced)
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Errorf("Controller.canDeleteCIDR() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestController_ipToCidrs 验证 IP 到所属 CIDR 的反向映射：
// 用排序后比较消除集合顺序影响，并验证子网/广播地址不被误判为占用。
func TestController_ipToCidrs(t *testing.T) {
	tests := []struct {
		name  string
		cidrs []*networkingapiv1.ServiceCIDR
		ip    *networkingapiv1.IPAddress
		want  []string
	}{
		{
			name: "empty",
			ip:   makeIPAddress("192.168.0.23"),
			want: []string{},
		}, {
			name: "one CIDR",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
				makeServiceCIDR("unrelated", "10.0.0.0/24", ""),
				makeServiceCIDR("unrelated2", "10.0.0.0/16", ""),
			},
			ip:   makeIPAddress("192.168.0.23"),
			want: []string{defaultservicecidr.DefaultServiceCIDRName},
		}, {
			name: "two equal CIDR",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
				makeServiceCIDR("overlapping", "192.168.0.0/24", "2001:db2::/96"),
				makeServiceCIDR("unrelated", "10.0.0.0/24", ""),
				makeServiceCIDR("unrelated2", "10.0.0.0/16", ""),
			},
			ip:   makeIPAddress("192.168.0.23"),
			want: []string{defaultservicecidr.DefaultServiceCIDRName, "overlapping"},
		}, {
			name: "three CIDR - two same and one larger",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
				makeServiceCIDR("overlapping", "192.168.0.0/24", "2001:db2::/64"),
				makeServiceCIDR("overlapping2", "192.168.0.0/26", "2001:db2::/96"),
				makeServiceCIDR("unrelated", "10.0.0.0/24", ""),
				makeServiceCIDR("unrelated2", "10.0.0.0/16", ""),
			},
			ip:   makeIPAddress("192.168.0.23"),
			want: []string{defaultservicecidr.DefaultServiceCIDRName, "overlapping", "overlapping2"},
		}, {
			name: "three CIDR - two same and one larger - IPv4 subnet address",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
				makeServiceCIDR("overlapping", "192.168.0.0/24", "2001:db2::/64"),
				makeServiceCIDR("overlapping2", "192.168.0.0/26", "2001:db2::/96"),
				makeServiceCIDR("unrelated", "10.0.0.0/24", ""),
				makeServiceCIDR("unrelated2", "10.0.0.0/16", ""),
			},
			ip:   makeIPAddress("192.168.0.0"),
			want: []string{},
		}, {
			name: "three CIDR - two same and one larger - IPv4 broadcast address",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
				makeServiceCIDR("overlapping", "192.168.0.0/24", "2001:db2::/64"),
				makeServiceCIDR("overlapping2", "192.168.0.0/26", "2001:db2::/96"),
				makeServiceCIDR("unrelated", "10.0.0.0/24", ""),
				makeServiceCIDR("unrelated2", "10.0.0.0/16", ""),
			},
			// 192.168.0.63 是 /26 的广播地址，不应算作 /26 内的可用 IP。
			ip:   makeIPAddress("192.168.0.63"), // broadcast for 192.168.0.0/26
			want: []string{defaultservicecidr.DefaultServiceCIDRName, "overlapping"},
		}, {
			name: "three CIDR - two same and one larger - IPv6 subnet address",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
				makeServiceCIDR("overlapping", "192.168.0.0/24", "2001:db2::/64"),
				makeServiceCIDR("overlapping2", "192.168.0.0/26", "2001:db2::/96"),
				makeServiceCIDR("unrelated", "10.0.0.0/24", ""),
				makeServiceCIDR("unrelated2", "10.0.0.0/16", ""),
			},
			ip:   makeIPAddress("2001:db2::"),
			want: []string{},
		}, {
			name: "three CIDR - two same and one larger - IPv6 broadcast address",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
				makeServiceCIDR("overlapping", "192.168.0.0/24", "2001:db2::/64"),
				makeServiceCIDR("overlapping2", "192.168.0.0/26", "2001:db2::/96"),
				makeServiceCIDR("unrelated", "10.0.0.0/24", ""),
				makeServiceCIDR("unrelated2", "10.0.0.0/16", ""),
			},
			ip:   makeIPAddress("2001:0db2::ffff:ffff:ffff:ffff"),
			want: []string{defaultservicecidr.DefaultServiceCIDRName, "overlapping"},
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			_, controller := newController(tCtx, t, tt.cidrs, nil)
			if got := controller.containingServiceCIDRs(tt.ip); !cmp.Equal(got, tt.want, cmpopts.SortSlices(func(a, b string) bool { return a < b })) {
				t.Errorf("Controller.ipToCidrs() = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestController_cidrToCidrs 验证 CIDR 之间的重叠查询，确保只返回真正重叠的 CIDR；
// 同样以排序比较抵消实现返回顺序的不确定性。
func TestController_cidrToCidrs(t *testing.T) {
	tests := []struct {
		name  string
		cidrs []*networkingapiv1.ServiceCIDR
		cidr  *networkingapiv1.ServiceCIDR
		want  []string
	}{
		{
			name: "empty",
			cidr: makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			want: []string{},
		}, {
			name: "one CIDR",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
				makeServiceCIDR("unrelated", "10.0.0.0/24", ""),
				makeServiceCIDR("unrelated2", "10.0.0.0/16", ""),
			},
			cidr: makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			want: []string{defaultservicecidr.DefaultServiceCIDRName},
		}, {
			name: "two equal CIDR",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
				makeServiceCIDR("overlapping", "192.168.0.0/24", "2001:db2::/96"),
				makeServiceCIDR("unrelated", "10.0.0.0/24", ""),
				makeServiceCIDR("unrelated2", "10.0.0.0/16", ""),
			},
			cidr: makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			want: []string{defaultservicecidr.DefaultServiceCIDRName, "overlapping"},
		}, {
			name: "three CIDR - two same and one larger",
			cidrs: []*networkingapiv1.ServiceCIDR{
				makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
				makeServiceCIDR("overlapping", "192.168.0.0/24", "2001:db2::/64"),
				makeServiceCIDR("overlapping2", "192.168.0.0/26", "2001:db2::/96"),
				makeServiceCIDR("unrelated", "10.0.0.0/24", ""),
				makeServiceCIDR("unrelated2", "10.0.0.0/16", ""),
			},
			cidr: makeServiceCIDR(defaultservicecidr.DefaultServiceCIDRName, "192.168.0.0/24", "2001:db2::/64"),
			want: []string{defaultservicecidr.DefaultServiceCIDRName, "overlapping", "overlapping2"},
		}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			_, controller := newController(tCtx, t, tt.cidrs, nil)
			if got := controller.overlappingServiceCIDRs(tt.cidr); !cmp.Equal(got, tt.want, cmpopts.SortSlices(func(a, b string) bool { return a < b })) {
				t.Errorf("Controller.cidrToCidrs() = %v, want %v", got, tt.want)
			}
		})
	}
}
