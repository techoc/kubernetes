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

package sync

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/kubernetes/pkg/controller/nodeipam/ipam/cidrset"
	"k8s.io/kubernetes/pkg/controller/nodeipam/ipam/test"
	netutils "k8s.io/utils/net"

	v1 "k8s.io/api/core/v1"
)

// 测试中使用的集群 CIDR 范围，用于构造 CIDR 分配与同步的场景。
var (
	_, clusterCIDRRange, _ = netutils.ParseCIDRSloppy("10.1.0.0/16")
)

// fakeEvent 记录测试中发出的事件：节点名称与事件原因。
type fakeEvent struct {
	nodeName string
	reason   string
}

// fakeAPIs 是 CIDR 同步器所依赖各接口的 fake 实现，记录调用轨迹并允许注入错误。
type fakeAPIs struct {
	aliasRange    *net.IPNet
	aliasErr      error
	addAliasErr   error
	nodeRet       *v1.Node
	nodeErr       error
	updateNodeErr error
	resyncTimeout time.Duration
	reportChan    chan struct{}

	updateNodeNetworkUnavailableErr error

	calls   []string
	events  []fakeEvent
	results []error

	logger klog.Logger
}

// Alias 返回云服务商为节点配置的 CIDR 范围（fake 实现）。
func (f *fakeAPIs) Alias(ctx context.Context, node *v1.Node) (*net.IPNet, error) {
	f.calls = append(f.calls, fmt.Sprintf("alias %v", node.Name))
	return f.aliasRange, f.aliasErr
}

// AddAlias 模拟在云服务商侧为节点添加 CIDR 别名（fake 实现）。
func (f *fakeAPIs) AddAlias(ctx context.Context, node *v1.Node, cidrRange *net.IPNet) error {
	f.calls = append(f.calls, fmt.Sprintf("addAlias %v %v", node.Name, cidrRange))
	return f.addAliasErr
}

// Node 按名称返回节点对象（fake 实现）。
func (f *fakeAPIs) Node(ctx context.Context, name string) (*v1.Node, error) {
	f.calls = append(f.calls, fmt.Sprintf("node %v", name))
	return f.nodeRet, f.nodeErr
}

// UpdateNodePodCIDR 模拟把 CIDR 写回节点的 PodCIDR 字段（fake 实现）。
func (f *fakeAPIs) UpdateNodePodCIDR(ctx context.Context, node *v1.Node, cidrRange *net.IPNet) error {
	f.calls = append(f.calls, fmt.Sprintf("updateNode %v", node))
	return f.updateNodeErr
}

// UpdateNodeNetworkUnavailable 模拟更新节点 NetworkUnavailable 状态（fake 实现）。
func (f *fakeAPIs) UpdateNodeNetworkUnavailable(nodeName string, unavailable bool) error {
	f.calls = append(f.calls, fmt.Sprintf("updateNodeNetworkUnavailable %v %v", nodeName, unavailable))
	return f.updateNodeNetworkUnavailableErr
}

// EmitNodeWarningEvent 记录一条节点 Warning 事件（fake 实现）。
func (f *fakeAPIs) EmitNodeWarningEvent(nodeName, reason, fmtStr string, args ...interface{}) {
	f.events = append(f.events, fakeEvent{nodeName, reason})
}

// ReportResult 记录同步结果，并在需要时通知等待中的测试（fake 实现）。
func (f *fakeAPIs) ReportResult(err error) {
	f.logger.V(2).Info("ReportResult", "err", err)
	f.results = append(f.results, err)
	if f.reportChan != nil {
		f.reportChan <- struct{}{}
	}
}

// ResyncTimeout 返回重新同步的超时时间；未显式设置时返回一个很大的值以免测试中触发。
func (f *fakeAPIs) ResyncTimeout() time.Duration {
	if f.resyncTimeout == 0 {
		return time.Second * 10000
	}
	return f.resyncTimeout
}

// dumpTrace 把记录下来的调用轨迹输出到测试日志。
func (f *fakeAPIs) dumpTrace() {
	for i, x := range f.calls {
		f.logger.Info("trace", "index", i, "call", x)
	}
}

// nodeWithoutCIDRRange 是尚未分配 PodCIDR 的节点，用于测试从云端同步 CIDR 的场景。
var nodeWithoutCIDRRange = &v1.Node{
	ObjectMeta: metav1.ObjectMeta{Name: "node1"},
}

// nodeWithCIDRRange 是已分配 PodCIDR 的节点，用于测试从集群读取 CIDR 的场景。
var nodeWithCIDRRange = &v1.Node{
	ObjectMeta: metav1.ObjectMeta{Name: "node1"},
	Spec:       v1.NodeSpec{PodCIDR: "10.1.1.0/24"},
}

// TestNodeSyncUpdate 覆盖不同同步模式与节点状态下，NodeSync 单次 Update 的行为，
// 校验发出的事件与是否产生错误符合预期。
func TestNodeSyncUpdate(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc string
		mode NodeSyncMode
		node *v1.Node
		fake fakeAPIs

		events    []fakeEvent
		wantError bool
	}{
		{
			desc: "validate range ==",
			mode: SyncFromCloud,
			node: nodeWithCIDRRange,
			fake: fakeAPIs{
				aliasRange: test.MustParseCIDR(nodeWithCIDRRange.Spec.PodCIDR),
			},
		},
		{
			desc:   "validate range !=",
			mode:   SyncFromCloud,
			node:   nodeWithCIDRRange,
			fake:   fakeAPIs{aliasRange: test.MustParseCIDR("192.168.0.0/24")},
			events: []fakeEvent{{"node1", "CloudCIDRAllocatorMismatch"}},
		},
		{
			desc:      "update alias from node",
			mode:      SyncFromCloud,
			node:      nodeWithCIDRRange,
			events:    []fakeEvent{{"node1", "CloudCIDRAllocatorInvalidMode"}},
			wantError: true,
		},
		{
			desc: "update alias from node",
			mode: SyncFromCluster,
			node: nodeWithCIDRRange,
			// XXX/bowei -- 待验证
		},
		{
			desc: "update node from alias",
			mode: SyncFromCloud,
			node: nodeWithoutCIDRRange,
			fake: fakeAPIs{aliasRange: test.MustParseCIDR("10.1.2.3/16")},
			// XXX/bowei -- 待验证
		},
		{
			desc:      "update node from alias",
			mode:      SyncFromCluster,
			node:      nodeWithoutCIDRRange,
			fake:      fakeAPIs{aliasRange: test.MustParseCIDR("10.1.2.3/16")},
			events:    []fakeEvent{{"node1", "CloudCIDRAllocatorInvalidMode"}},
			wantError: true,
		},
		{
			desc:      "allocate range",
			mode:      SyncFromCloud,
			node:      nodeWithoutCIDRRange,
			events:    []fakeEvent{{"node1", "CloudCIDRAllocatorInvalidMode"}},
			wantError: true,
		},
		{
			desc: "allocate range",
			mode: SyncFromCluster,
			node: nodeWithoutCIDRRange,
		},
		{
			desc: "update with node==nil",
			mode: SyncFromCluster,
			node: nil,
			fake: fakeAPIs{
				nodeRet: nodeWithCIDRRange,
			},
			wantError: false,
		},
	} {
		logger, _ := ktesting.NewTestContext(t)
		cidr, _ := cidrset.NewCIDRSet(clusterCIDRRange, 24)
		tc.fake.logger = logger
		sync := New(&tc.fake, &tc.fake, &tc.fake, tc.mode, "node1", cidr)
		doneChan := make(chan struct{})

		// 让同步循环只执行一步：启动 Loop 后送入一次更新，再关闭操作通道等待结束。
		go sync.Loop(logger, doneChan)
		sync.Update(tc.node)
		close(sync.opChan)
		<-doneChan
		tc.fake.dumpTrace()

		if !reflect.DeepEqual(tc.fake.events, tc.events) {
			t.Errorf("%v, %v; fake.events = %#v, want %#v", tc.desc, tc.mode, tc.fake.events, tc.events)
		}

		var hasError bool
		for _, r := range tc.fake.results {
			hasError = hasError || (r != nil)
		}
		if hasError != tc.wantError {
			t.Errorf("%v, %v; hasError = %t, errors = %v, want %t",
				tc.desc, tc.mode, hasError, tc.fake.events, tc.wantError)
		}
	}
}

// TestNodeSyncResync 验证在 SyncFromCluster 模式下，同步循环会按超时周期触发重新同步。
func TestNodeSyncResync(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	fake := &fakeAPIs{
		nodeRet:       nodeWithCIDRRange,
		resyncTimeout: time.Millisecond,
		// 缓冲区留 1 个位置，允许测试关闭循环期间多到达一次重新同步通知。
		reportChan: make(chan struct{}, 1),
		logger:     logger,
	}
	cidr, _ := cidrset.NewCIDRSet(clusterCIDRRange, 24)
	sync := New(fake, fake, fake, SyncFromCluster, "node1", cidr)
	doneChan := make(chan struct{})
	go sync.Loop(logger, doneChan)
	// 等待第一次重新同步完成。
	<-fake.reportChan
	// 关闭操作通道以停止循环。
	close(sync.opChan)
	// 等待循环退出。
	<-doneChan
	fake.dumpTrace()
}

// TestNodeSyncDelete 验证 Delete 事件处理：覆盖正常 CIDR、无 CIDR 以及非法 CIDR 的多种节点。
func TestNodeSyncDelete(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		desc string
		mode NodeSyncMode
		node *v1.Node
		fake fakeAPIs
	}{
		{
			desc: "delete",
			mode: SyncFromCluster,
			node: nodeWithCIDRRange,
		},
		{
			desc: "delete without CIDR range",
			mode: SyncFromCluster,
			node: nodeWithoutCIDRRange,
		},
		{
			desc: "delete with invalid CIDR range",
			mode: SyncFromCluster,
			node: &v1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "node1"},
				Spec:       v1.NodeSpec{PodCIDR: "invalid"},
			},
		},
	} {
		logger, _ := ktesting.NewTestContext(t)
		cidr, _ := cidrset.NewCIDRSet(clusterCIDRRange, 24)
		tc.fake.logger = logger
		sync := New(&tc.fake, &tc.fake, &tc.fake, tc.mode, "node1", cidr)
		doneChan := make(chan struct{})

		// 让同步循环只执行一步：启动 Loop 后送入一次更新，再关闭操作通道等待结束。
		go sync.Loop(logger, doneChan)
		sync.Delete(tc.node)
		<-doneChan
		tc.fake.dumpTrace()

		/*
			if !reflect.DeepEqual(tc.fake.events, tc.events) {
				t.Errorf("%v, %v; fake.events = %#v, want %#v", tc.desc, tc.mode, tc.fake.events, tc.events)
			}

			var hasError bool
			for _, r := range tc.fake.results {
				hasError = hasError || (r != nil)
			}
			if hasError != tc.wantError {
				t.Errorf("%v, %v; hasError = %t, errors = %v, want %t",
					tc.desc, tc.mode, hasError, tc.fake.events, tc.wantError)
			}
		*/
	}
}
