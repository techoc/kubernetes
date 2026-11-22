/*
Copyright 2019 The Kubernetes Authors.

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

package metaproxier

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/pkg/proxy"
)

// recordingProxier 实现了 proxy.Provider，并记录每次方法调用，
// 这样测试就能在无需完整 proxier 实现的情况下断言双栈分发行为。
type recordingProxier struct {
	mu sync.Mutex

	calls []string

	// syncLoopExit 非空时，会让 SyncLoop 一直阻塞到该 channel 被关闭。
	syncLoopExit <-chan struct{}
	// onSyncLoopEnter 非空时，会在记录 SyncLoop 之后、进入阻塞之前被调用。
	onSyncLoopEnter func()
}

// record 以线程安全的方式记录一次方法调用及其参数。
func (r *recordingProxier) record(method string, args ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, fmt.Sprintf("%s(%s)", method, strings.Join(args, ", ")))
}

// getCalls 返回已记录的调用并清空缓冲区，便于逐阶段断言。
func (r *recordingProxier) getCalls() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.calls))
	copy(out, r.calls)
	// 清空记录，使下一次断言只看到新一轮的调用。
	r.calls = nil
	return out
}

// Sync 记录并转发 Sync 调用。
func (r *recordingProxier) Sync() {
	r.record("Sync")
}

// SyncLoop 先记录调用，再按测试配置触发通知或阻塞。
func (r *recordingProxier) SyncLoop() {
	r.record("SyncLoop")
	if r.onSyncLoopEnter != nil {
		r.onSyncLoopEnter()
	}
	if r.syncLoopExit != nil {
		<-r.syncLoopExit
	}
}

// OnServiceAdd 记录新增 Service 的名称。
func (r *recordingProxier) OnServiceAdd(service *v1.Service) {
	r.record("OnServiceAdd", service.Name)
}

// OnServiceUpdate 记录更新前后 Service 的名称。
func (r *recordingProxier) OnServiceUpdate(oldSvc, service *v1.Service) {
	r.record("OnServiceUpdate", oldSvc.Name, service.Name)
}

// OnServiceDelete 记录删除 Service 的名称。
func (r *recordingProxier) OnServiceDelete(service *v1.Service) {
	r.record("OnServiceDelete", service.Name)
}

// OnServiceSynced 记录 Service 同步完成事件。
func (r *recordingProxier) OnServiceSynced() {
	r.record("OnServiceSynced")
}

// OnEndpointSliceAdd 记录新增 EndpointSlice 的地址类型。
func (r *recordingProxier) OnEndpointSliceAdd(endpointSlice *discovery.EndpointSlice) {
	r.record("OnEndpointSliceAdd", string(endpointSlice.AddressType))
}

// OnEndpointSliceUpdate 记录更新前后 EndpointSlice 的地址类型。
func (r *recordingProxier) OnEndpointSliceUpdate(oldEs, newEs *discovery.EndpointSlice) {
	r.record("OnEndpointSliceUpdate", string(oldEs.AddressType), string(newEs.AddressType))
}

// OnEndpointSliceDelete 记录删除 EndpointSlice 的地址类型。
func (r *recordingProxier) OnEndpointSliceDelete(endpointSlice *discovery.EndpointSlice) {
	r.record("OnEndpointSliceDelete", string(endpointSlice.AddressType))
}

// OnEndpointSlicesSynced 记录 EndpointSlice 同步完成事件。
func (r *recordingProxier) OnEndpointSlicesSynced() {
	r.record("OnEndpointSlicesSynced")
}

// OnTopologyChange 记录拓扑变更事件；本测试只关心是否被调用。
func (r *recordingProxier) OnTopologyChange(labels map[string]string) {
	r.record("OnTopologyChange")
}

// OnServiceCIDRsChanged 记录变更后的 Service CIDR 列表。
func (r *recordingProxier) OnServiceCIDRsChanged(cidrs []string) {
	r.record("OnServiceCIDRsChanged", strings.Join(cidrs, ", "))
}

// 编译期断言 recordingProxier 满足 proxy.Provider 接口。
var _ proxy.Provider = (*recordingProxier)(nil)

// TestMetaProxier_GenericCallbacksFanOutToBothProxiers 验证通用回调会广播给两个子 proxier。
func TestMetaProxier_GenericCallbacksFanOutToBothProxiers(t *testing.T) {
	ipv4 := &recordingProxier{}
	ipv6 := &recordingProxier{}
	mp := NewMetaProxier(ipv4, ipv6)

	mp.Sync()

	// 用不同名称的 Service，便于在记录结果中区分各个回调。
	svcAdd := &v1.Service{ObjectMeta: metav1.ObjectMeta{Name: "add"}}
	mp.OnServiceAdd(svcAdd)

	oldSvc := &v1.Service{ObjectMeta: metav1.ObjectMeta{Name: "old"}}
	newSvc := &v1.Service{ObjectMeta: metav1.ObjectMeta{Name: "new"}}
	mp.OnServiceUpdate(oldSvc, newSvc)

	del := &v1.Service{ObjectMeta: metav1.ObjectMeta{Name: "gone"}}
	mp.OnServiceDelete(del)

	mp.OnServiceSynced()

	mp.OnEndpointSlicesSynced()

	mp.OnTopologyChange(map[string]string{"topology.kubernetes.io/zone": "a"})
	mp.OnServiceCIDRsChanged([]string{"10.0.0.0/16", "2001:db8::/64"})

	// 期望两个子 proxier 收到完全相同的调用序列。
	want := []string{
		"Sync()",
		"OnServiceAdd(add)",
		"OnServiceUpdate(old, new)",
		"OnServiceDelete(gone)",
		"OnServiceSynced()",
		"OnEndpointSlicesSynced()",
		"OnTopologyChange()",
		"OnServiceCIDRsChanged(10.0.0.0/16, 2001:db8::/64)",
	}
	// 分别检查两个子 proxier 的调用记录，避免某一路被遗漏。
	if diff := cmp.Diff(want, ipv4.getCalls()); diff != "" {
		t.Errorf("ipv4 (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff(want, ipv6.getCalls()); diff != "" {
		t.Errorf("ipv6 (-want +got):\n%s", diff)
	}
}

// TestMetaProxier_EndpointSlice_RoutesByAddressType 验证 EndpointSlice 事件按地址类型分发。
func TestMetaProxier_EndpointSlice_RoutesByAddressType(t *testing.T) {
	ipv4 := &recordingProxier{}
	ipv6 := &recordingProxier{}
	mp := NewMetaProxier(ipv4, ipv6)

	eps4 := &discovery.EndpointSlice{AddressType: discovery.AddressTypeIPv4}
	// 构造 IPv4 与 IPv6 两种地址类型的 EndpointSlice。
	eps6 := &discovery.EndpointSlice{AddressType: discovery.AddressTypeIPv6}

	mp.OnEndpointSliceAdd(eps4)
	mp.OnEndpointSliceAdd(eps6)

	old4 := &discovery.EndpointSlice{AddressType: discovery.AddressTypeIPv4}
	new4 := &discovery.EndpointSlice{AddressType: discovery.AddressTypeIPv4}
	mp.OnEndpointSliceUpdate(old4, new4)

	// IPv4 删除事件只应转发给 IPv4 proxier。
	mp.OnEndpointSliceDelete(eps4)

	if diff := cmp.Diff([]string{
		"OnEndpointSliceAdd(IPv4)",
		"OnEndpointSliceUpdate(IPv4, IPv4)",
		"OnEndpointSliceDelete(IPv4)",
	}, ipv4.getCalls()); diff != "" {
		t.Errorf("ipv4 (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{
		"OnEndpointSliceAdd(IPv6)",
	}, ipv6.getCalls()); diff != "" {
		t.Errorf("ipv6 (-want +got):\n%s", diff)
	}
}

// TestMetaProxier_EndpointSlice_UnsupportedAddressType_NoDownstreamCalls 验证不支持的地址类型不会下发调用。
func TestMetaProxier_EndpointSlice_UnsupportedAddressType_NoDownstreamCalls(t *testing.T) {
	ipv4 := &recordingProxier{}
	ipv6 := &recordingProxier{}
	mp := NewMetaProxier(ipv4, ipv6)

	// FQDN 类型的 EndpointSlice 不属于任何 IP 族，应被忽略。
	fqdnSlice := &discovery.EndpointSlice{AddressType: discovery.AddressTypeFQDN}

	mp.OnEndpointSliceAdd(fqdnSlice)
	mp.OnEndpointSliceUpdate(fqdnSlice, fqdnSlice)
	mp.OnEndpointSliceDelete(fqdnSlice)

	// 两个子 proxier 都不应收到 FQDN 相关的调用。
	if got := ipv4.getCalls(); len(got) != 0 {
		t.Errorf("expected ipv4 no calls for FQDN slices, got %v", got)
	}
	if got := ipv6.getCalls(); len(got) != 0 {
		t.Errorf("expected ipv6 no calls for FQDN slices, got %v", got)
	}
}

// TestMetaProxier_SyncLoop_InvokesBothChildSyncLoops 验证 SyncLoop 会并发调用两个子 proxier。
func TestMetaProxier_SyncLoop_InvokesBothChildSyncLoops(t *testing.T) {
	exit := make(chan struct{})
	// 用 WaitGroup 等待两个子 SyncLoop 都进入后再放行。
	var wg sync.WaitGroup
	wg.Add(2)

	// 让两个子 SyncLoop 阻塞在同一个 exit channel 上。
	ipv4 := &recordingProxier{
		syncLoopExit:    exit,
		onSyncLoopEnter: wg.Done,
	}
	ipv6 := &recordingProxier{
		syncLoopExit:    exit,
		onSyncLoopEnter: wg.Done,
	}
	mp := NewMetaProxier(ipv4, ipv6)

	// 在独立 goroutine 中运行 SyncLoop，避免阻塞当前测试 goroutine。
	stopped := make(chan struct{})
	go func() {
		mp.SyncLoop()
		close(stopped)
	}()

	// 单独等待“两个子循环都已启动”的信号，避免与主循环退出混淆。
	bothStarted := make(chan struct{})
	go func() {
		wg.Wait()
		close(bothStarted)
	}()

	// 等待两个子 SyncLoop 启动，超时则说明没有并发调用。
	select {
	case <-bothStarted:
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatalf("timed out waiting for both child SyncLoop calls: ipv4=%v ipv6=%v", ipv4.getCalls(), ipv6.getCalls())
	}

	// 关闭 exit，放行两个阻塞中的子 SyncLoop。
	close(exit)

	// 确认子循环退出后 MetaProxier 的 SyncLoop 也能正常返回。
	select {
	case <-stopped:
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatal("SyncLoop did not return after unblocking children")
	}

	if diff := cmp.Diff([]string{"SyncLoop()"}, ipv4.getCalls()); diff != "" {
		t.Errorf("ipv4 (-want +got):\n%s", diff)
	}
	if diff := cmp.Diff([]string{"SyncLoop()"}, ipv6.getCalls()); diff != "" {
		t.Errorf("ipv6 (-want +got):\n%s", diff)
	}
}
