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
	"time"

	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/controller/nodeipam/ipam/cidrset"
)

const (
	// InvalidPodCIDR 记录发现节点携带无效 PodCIDR 时触发的事件。
	InvalidPodCIDR = "CloudCIDRAllocatorInvalidPodCIDR"
	// InvalidModeEvent 记录集群运行模式不正确、导致 CIDR 范围无法同步时触发的事件。
	InvalidModeEvent = "CloudCIDRAllocatorInvalidMode"
	// MismatchEvent 记录节点的 spec 中分配的 CIDR 范围与云平台中分配的不一致时触发的事件。
	MismatchEvent = "CloudCIDRAllocatorMismatch"
)

// cloudAlias 封装云平台别名（alias）相关 API。
type cloudAlias interface {
	// Alias 返回节点对应的 IP 别名。
	Alias(ctx context.Context, node *v1.Node) (*net.IPNet, error)
	// AddAlias 为节点添加一个别名。
	AddAlias(ctx context.Context, node *v1.Node, cidrRange *net.IPNet) error
}

// kubeAPI 封装本包所需的 Kubernetes API 操作。
type kubeAPI interface {
	// Node 返回指定名称的 Node 对象。
	Node(ctx context.Context, name string) (*v1.Node, error)
	// UpdateNodePodCIDR 更新 Node spec 中的 PodCIDR。
	UpdateNodePodCIDR(ctx context.Context, node *v1.Node, cidrRange *net.IPNet) error
	// UpdateNodeNetworkUnavailable 更新节点的 NetworkUnavailable 状态。
	UpdateNodeNetworkUnavailable(nodeName string, unavailable bool) error
	// EmitNodeWarningEvent 为指定节点发出一条 Warning 事件。
	EmitNodeWarningEvent(nodeName, reason, fmt string, args ...interface{})
}

// controller 封装本包所需的上层控制器回调。
type controller interface {
	// ReportResult 将最近一次同步操作的结果上报给控制器。
	ReportResult(err error)
	// ResyncTimeout 返回对节点重试同步前应等待的时间。
	ResyncTimeout() time.Duration
}

// NodeSyncMode 表示 cloud CIDR 分配器的运行模式。
type NodeSyncMode string

var (
	// SyncFromCloud 表示把云平台中的 IP 分配结果同步到节点。
	SyncFromCloud NodeSyncMode = "SyncFromCloud"
	// SyncFromCluster 表示把 Kubernetes 控制器决定的 IP 分配结果同步到云平台。
	SyncFromCluster NodeSyncMode = "SyncFromCluster"
)

// IsValidMode 判断给定模式是否为有效的同步模式。
func IsValidMode(m NodeSyncMode) bool {
	switch m {
	case SyncFromCloud:
	case SyncFromCluster:
	default:
		return false
	}
	return true
}

// NodeSync 负责同步集群中单个节点的状态。
type NodeSync struct {
	c          controller
	cloudAlias cloudAlias
	kubeAPI    kubeAPI
	mode       NodeSyncMode
	nodeName   string
	opChan     chan syncOp
	set        *cidrset.CidrSet
}

// New 为指定节点创建一个新的同步器。
func New(c controller, cloudAlias cloudAlias, kubeAPI kubeAPI, mode NodeSyncMode, nodeName string, set *cidrset.CidrSet) *NodeSync {
	return &NodeSync{
		c:          c,
		cloudAlias: cloudAlias,
		kubeAPI:    kubeAPI,
		mode:       mode,
		nodeName:   nodeName,
		opChan:     make(chan syncOp, 1),
		set:        set,
	}
}

// Loop 运行指定节点的同步循环。done 为可选通道，Loop 返回时会被关闭。
func (sync *NodeSync) Loop(logger klog.Logger, done chan struct{}) {
	logger.V(2).Info("Starting sync loop", "node", klog.KRef("", sync.nodeName))

	// 无论循环因何种原因退出，都关闭 done 通知调用方。
	defer func() {
		if done != nil {
			close(done)
		}
	}()

	timeout := sync.c.ResyncTimeout()
	delayTimer := time.NewTimer(timeout)
	logger.V(4).Info("Try to resync node later", "node", klog.KRef("", sync.nodeName), "resyncTime", timeout)

	for {
		select {
		case op, more := <-sync.opChan:
			// Delete 会关闭 opChan；通道关闭后循环即可退出，不再等待重同步。
			if !more {
				logger.V(2).Info("Stopping sync loop")
				return
			}
			sync.c.ReportResult(op.run(logger, sync))
			// 阻止旧的定时器触发：收到新操作意味着应按新的时间点重新计时。
			if !delayTimer.Stop() {
				<-delayTimer.C
			}
		case <-delayTimer.C:
			logger.V(4).Info("Running resync", "node", klog.KRef("", sync.nodeName))
			sync.c.ReportResult((&updateOp{}).run(logger, sync))
		}

		timeout := sync.c.ResyncTimeout()
		delayTimer.Reset(timeout)
		logger.V(4).Info("Try to resync node later", "node", klog.KRef("", sync.nodeName), "resyncTime", timeout)
	}
}

// Update 触发对给定节点的更新操作。若 node 为 nil，同步器会先从 API Server
// 获取节点 spec 再执行同步。此方法可被多个 goroutine 安全调用。
func (sync *NodeSync) Update(node *v1.Node) {
	sync.opChan <- &updateOp{node}
}

// Delete 执行将节点从 IPAM 状态中移除所需的同步操作。
// 此方法可被多个 goroutine 安全调用。
func (sync *NodeSync) Delete(node *v1.Node) {
	sync.opChan <- &deleteOp{node}
	close(sync.opChan)
}

// syncOp 是通用同步操作的接口。
type syncOp interface {
	// run 执行请求的同步操作。
	run(logger klog.Logger, sync *NodeSync) error
}

// updateOp 处理节点的创建与更新。
type updateOp struct {
	node *v1.Node
}

// String 返回用于日志的可读表示；node 为 nil 时仅输出操作类型。
func (op *updateOp) String() string {
	if op.node == nil {
		return fmt.Sprintf("updateOp(nil)")
	}
	return fmt.Sprintf("updateOp(%q,%v)", op.node.Name, op.node.Spec.PodCIDR)
}

// run 根据节点 spec 中的 PodCIDR 与云别名是否存在，分派到对应的处理分支。
func (op *updateOp) run(logger klog.Logger, sync *NodeSync) error {
	logger.V(3).Info("Running updateOp", "updateOp", op)

	ctx := context.Background()

	if op.node == nil {
		logger.V(3).Info("Getting node spec", "node", klog.KRef("", sync.nodeName))
		node, err := sync.kubeAPI.Node(ctx, sync.nodeName)
		if err != nil {
			logger.Error(err, "Error getting node pec", "node", klog.KRef("", sync.nodeName))
			return err
		}
		op.node = node
	}

	aliasRange, err := sync.cloudAlias.Alias(ctx, op.node)
	if err != nil {
		logger.Error(err, "Error getting cloud alias for node", "node", klog.KRef("", sync.nodeName))
		return err
	}

	// 四种组合分别对应：首次分配、从云同步到节点、从节点同步到云、以及一致性校验。
	switch {
	case op.node.Spec.PodCIDR == "" && aliasRange == nil:
		err = op.allocateRange(ctx, sync, op.node)
	case op.node.Spec.PodCIDR == "" && aliasRange != nil:
		err = op.updateNodeFromAlias(ctx, sync, op.node, aliasRange)
	case op.node.Spec.PodCIDR != "" && aliasRange == nil:
		err = op.updateAliasFromNode(ctx, sync, op.node)
	case op.node.Spec.PodCIDR != "" && aliasRange != nil:
		err = op.validateRange(ctx, sync, op.node, aliasRange)
	}

	return err
}

// validateRange 检查节点 spec 分配的范围与云别名范围是否一致。
func (op *updateOp) validateRange(ctx context.Context, sync *NodeSync, node *v1.Node, aliasRange *net.IPNet) error {
	if node.Spec.PodCIDR != aliasRange.String() {
		klog.FromContext(ctx).Error(nil, "Inconsistency detected between node PodCIDR and node alias", "podCIDR", node.Spec.PodCIDR, "alias", aliasRange)
		sync.kubeAPI.EmitNodeWarningEvent(node.Name, MismatchEvent,
			"Node.Spec.PodCIDR != cloud alias (%v != %v)", node.Spec.PodCIDR, aliasRange)
		// 这种不一致很可能是用户手工改动了 VM 别名，只能由人工介入修正。
	} else {
		klog.FromContext(ctx).V(4).Info("Node CIDR range is matches cloud assignment", "node", klog.KObj(node), "podCIDR", node.Spec.PodCIDR)
	}
	return nil
}

// updateNodeFromAlias 根据云平台分配的别名更新节点。
func (op *updateOp) updateNodeFromAlias(ctx context.Context, sync *NodeSync, node *v1.Node, aliasRange *net.IPNet) error {
	if sync.mode != SyncFromCloud {
		sync.kubeAPI.EmitNodeWarningEvent(node.Name, InvalidModeEvent,
			"Cannot sync from cloud in mode %q", sync.mode)
		return fmt.Errorf("cannot sync from cloud in mode %q", sync.mode)
	}
	logger := klog.FromContext(ctx)
	logger.V(2).Info("Updating node spec with alias range", "podCIDR", aliasRange)

	if err := sync.set.Occupy(aliasRange); err != nil {
		logger.Error(nil, "Error occupying range for node", "node", klog.KRef("", sync.nodeName), "alias", aliasRange)
		return err
	}

	if err := sync.kubeAPI.UpdateNodePodCIDR(ctx, node, aliasRange); err != nil {
		logger.Error(err, "Could not update node PodCIDR", "node", klog.KObj(node), "podCIDR", aliasRange)
		return err
	}

	logger.V(2).Info("Node PodCIDR updated", "node", klog.KObj(node), "podCIDR", aliasRange)

	if err := sync.kubeAPI.UpdateNodeNetworkUnavailable(node.Name, false); err != nil {
		logger.Error(err, "Could not update node NetworkUnavailable status to false")
		return err
	}

	logger.V(2).Info("Updated node PodCIDR from cloud alias", "node", klog.KObj(node), "alias", aliasRange)

	return nil
}

// updateAliasFromNode updates the cloud alias given the node allocation.
func (op *updateOp) updateAliasFromNode(ctx context.Context, sync *NodeSync, node *v1.Node) error {
	if sync.mode != SyncFromCluster {
		sync.kubeAPI.EmitNodeWarningEvent(
			node.Name, InvalidModeEvent, "Cannot sync to cloud in mode %q", sync.mode)
		return fmt.Errorf("cannot sync to cloud in mode %q", sync.mode)
	}

	_, aliasRange, err := netutils.ParseCIDRSloppy(node.Spec.PodCIDR)

	logger := klog.FromContext(ctx)
	if err != nil {
		logger.Error(err, "Could not parse PodCIDR for node", "node", klog.KObj(node), "podCIDR", node.Spec.PodCIDR)
		return err
	}

	if err := sync.set.Occupy(aliasRange); err != nil {
		logger.Error(nil, "Error occupying range for node", "node", klog.KRef("", sync.nodeName), "alias", aliasRange)
		return err
	}

	if err := sync.cloudAlias.AddAlias(ctx, node, aliasRange); err != nil {
		logger.Error(err, "Could not add alias for node", "node", klog.KObj(node), "alias", aliasRange)
		return err
	}

	if err := sync.kubeAPI.UpdateNodeNetworkUnavailable(node.Name, false); err != nil {
		logger.Error(err, "Could not update node NetworkUnavailable status to false")
		return err
	}

	logger.V(2).Info("Updated node cloud alias with node spec", "node", klog.KObj(node), "podCIDR", node.Spec.PodCIDR)

	return nil
}

// allocateRange allocates a new range and updates both the cloud
// platform and the node allocation.
func (op *updateOp) allocateRange(ctx context.Context, sync *NodeSync, node *v1.Node) error {
	if sync.mode != SyncFromCluster {
		sync.kubeAPI.EmitNodeWarningEvent(node.Name, InvalidModeEvent,
			"Cannot allocate CIDRs in mode %q", sync.mode)
		return fmt.Errorf("controller cannot allocate CIDRS in mode %q", sync.mode)
	}

	cidrRange, err := sync.set.AllocateNext()
	if err != nil {
		return err
	}
	// 若 AddAlias 返回硬错误，新分配的 cidrRange 会泄漏，因为它尚未持久化到任何记录中；
	// 这部分丢失的地址空间会在控制器下次重启时恢复。
	logger := klog.FromContext(ctx)
	if err := sync.cloudAlias.AddAlias(ctx, node, cidrRange); err != nil {
		logger.Error(err, "Could not add alias for node", "node", klog.KObj(node), "alias", cidrRange)
		return err
	}

	if err := sync.kubeAPI.UpdateNodePodCIDR(ctx, node, cidrRange); err != nil {
		logger.Error(err, "Could not update node PodCIDR", "node", klog.KObj(node), "podCIDR", cidrRange)
		return err
	}

	if err := sync.kubeAPI.UpdateNodeNetworkUnavailable(node.Name, false); err != nil {
		logger.Error(err, "Could not update node NetworkUnavailable status to false")
		return err
	}

	logger.V(2).Info("Allocated PodCIDR for node", "node", klog.KObj(node), "podCIDR", cidrRange)

	return nil
}

// deleteOp 处理节点删除。
type deleteOp struct {
	node *v1.Node
}

// String 返回用于日志的可读表示；node 为 nil 时仅输出操作类型。
func (op *deleteOp) String() string {
	if op.node == nil {
		return fmt.Sprintf("deleteOp(nil)")
	}
	return fmt.Sprintf("deleteOp(%q,%v)", op.node.Name, op.node.Spec.PodCIDR)
}

// run 释放已删除节点占用的 CIDR 范围；没有 PodCIDR 的节点无需处理。
func (op *deleteOp) run(logger klog.Logger, sync *NodeSync) error {
	logger.V(3).Info("Running deleteOp", "deleteOp", op)
	if op.node.Spec.PodCIDR == "" {
		logger.V(2).Info("Node was deleted, node had no PodCIDR range assigned", "node", klog.KObj(op.node))
		return nil
	}

	_, cidrRange, err := netutils.ParseCIDRSloppy(op.node.Spec.PodCIDR)
	if err != nil {
		logger.Error(err, "Deleted node has an invalid podCIDR", "node", klog.KObj(op.node), "podCIDR", op.node.Spec.PodCIDR)
		sync.kubeAPI.EmitNodeWarningEvent(op.node.Name, InvalidPodCIDR,
			"Node %q has an invalid PodCIDR: %q", op.node.Name, op.node.Spec.PodCIDR)
		return nil
	}

	sync.set.Release(cidrRange)
	logger.V(2).Info("Node was deleted, releasing CIDR range", "node", klog.KObj(op.node), "podCIDR", op.node.Spec.PodCIDR)

	return nil
}
