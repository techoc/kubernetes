/*
Copyright 2016 The Kubernetes Authors.

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

package kubelet

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	goruntime "runtime"
	"sort"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	cloudproviderapi "k8s.io/cloud-provider/api"
	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/klog/v2"
	kubeletapis "k8s.io/kubelet/pkg/apis"
	v1helper "k8s.io/kubernetes/pkg/apis/core/v1/helper"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/kubelet/nodestatus"
	volutil "k8s.io/kubernetes/pkg/volume/util"
)

// registerWithAPIServer 向集群控制面注册本节点。重复调用是安全的，
// 但不可并发调用（kl.registrationCompleted 没有加锁保护）。
func (kl *Kubelet) registerWithAPIServer(ctx context.Context) {
	if kl.registrationCompleted {
		return
	}
	logger := klog.FromContext(ctx)

	kl.nodeStartupLatencyTracker.RecordAttemptRegisterNode()

	step := 100 * time.Millisecond

	for {
		time.Sleep(step)
		step = step * 2
		if step >= 7*time.Second {
			step = 7 * time.Second
		}

		node, err := kl.initialNode(ctx)
		if err != nil {
			logger.Error(err, "Unable to construct v1.Node object for kubelet")
			continue
		}

		logger.Info("Attempting to register node", "node", klog.KObj(node))
		registered := kl.tryRegisterWithAPIServer(ctx, node)
		if registered {
			logger.Info("Successfully registered node", "node", klog.KObj(node))
			kl.registrationCompleted = true
			return
		}
	}
}

// tryRegisterWithAPIServer 尝试把给定节点注册到 API server，返回本次尝试是否成功。
// 若同名节点已存在，则协调该节点上由控制器管理的卷 attach-detach 注解值，
// 使 kubelet 当前配置与集群中的既有状态保持一致。
func (kl *Kubelet) tryRegisterWithAPIServer(ctx context.Context, node *v1.Node) bool {
	logger := klog.FromContext(ctx)
	_, err := kl.kubeClient.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
	if err == nil {
		kl.nodeStartupLatencyTracker.RecordRegisteredNewNode()
		return true
	}

	switch {
	case apierrors.IsAlreadyExists(err):
		// 节点已存在，转为协调既有节点的状态字段。
	case apierrors.IsForbidden(err):
		// 创建节点被拒绝，但节点可能已由其他途径创建；尝试读取以判断是否需要协调。
		if utilfeature.DefaultFeatureGate.Enabled(features.KubeletRegistrationGetOnExistsOnly) {
			logger.Error(err, "Unable to register node with API server, reason is forbidden", "node", klog.KObj(node))
			return false
		}
	default:
		logger.Error(err, "Unable to register node with API server", "node", klog.KObj(node))
		return false
	}

	existingNode, err := kl.kubeClient.CoreV1().Nodes().Get(ctx, string(kl.nodeName), metav1.GetOptions{})
	if err != nil {
		logger.Error(err, "Unable to register node with API server, error getting existing node", "node", klog.KObj(node))
		return false
	}

	if existingNode == nil {
		logger.Info("Unable to register node with API server, no node instance returned", "node", klog.KObj(node))
		return false
	}

	originalNode := existingNode.DeepCopy()

	logger.Info("Node was previously registered", "node", klog.KObj(node))

	// 边界情况：节点此前已注册；需要协调由控制器管理的
	// attach-detach 注解，避免 kubelet 重启后挂载策略回退。
	requiresUpdate := kl.reconcileCMADAnnotationWithExistingNode(logger, node, existingNode)
	// 各项协调相互独立，用逻辑或累加：任一字段变化都需要一次 patch。
	requiresUpdate = kl.updateDefaultLabels(node, existingNode) || requiresUpdate
	requiresUpdate = kl.reconcileExtendedResource(logger, node, existingNode) || requiresUpdate
	requiresUpdate = kl.reconcileHugePageResource(logger, node, existingNode) || requiresUpdate
	if requiresUpdate {
		if _, _, err := nodeutil.PatchNodeStatus(kl.kubeClient.CoreV1(), types.NodeName(kl.nodeName), originalNode, existingNode); err != nil {
			logger.Error(err, "Unable to reconcile node with API server,error updating node", "node", klog.KObj(node))
			return false
		}
	}

	return true
}

// reconcileHugePageResource 按页大小更新巨页 capacity/allocatable，
// 并移除节点已不再支持的巨页规格。
func (kl *Kubelet) reconcileHugePageResource(logger klog.Logger, initialNode, existingNode *v1.Node) bool {
	requiresUpdate := updateDefaultResources(initialNode, existingNode)
	supportedHugePageResources := sets.Set[string]{}

	for resourceName := range initialNode.Status.Capacity {
		if !v1helper.IsHugePageResourceName(resourceName) {
			continue
		}
		supportedHugePageResources.Insert(string(resourceName))

		initialCapacity := initialNode.Status.Capacity[resourceName]
		initialAllocatable := initialNode.Status.Allocatable[resourceName]

		capacity, resourceIsSupported := existingNode.Status.Capacity[resourceName]
		allocatable := existingNode.Status.Allocatable[resourceName]

		// 之前不支持该页大小或容量已变化时，写入新的 capacity。
		if !resourceIsSupported || capacity.Cmp(initialCapacity) != 0 {
			existingNode.Status.Capacity[resourceName] = initialCapacity.DeepCopy()
			requiresUpdate = true
		}

		// 之前不支持该页大小或可分配量已变化时，写入新的 allocatable。
		if !resourceIsSupported || allocatable.Cmp(initialAllocatable) != 0 {
			existingNode.Status.Allocatable[resourceName] = initialAllocatable.DeepCopy()
			requiresUpdate = true
		}

	}

	for resourceName := range existingNode.Status.Capacity {
		if !v1helper.IsHugePageResourceName(resourceName) {
			continue
		}

		// 已不再支持的巨页规格必须删除，否则调度器会按失效容量继续调度。
		if !supportedHugePageResources.Has(string(resourceName)) {
			delete(existingNode.Status.Capacity, resourceName)
			delete(existingNode.Status.Allocatable, resourceName)
			logger.Info("Removing huge page resource which is no longer supported", "resourceName", resourceName)
			requiresUpdate = true
		}
	}
	return requiresUpdate
}

// reconcileExtendedResource 在协调期间清零扩展资源容量：设备插件尚未重新上报前，
// 不能让调度器看到过期的可用容量。
func (kl *Kubelet) reconcileExtendedResource(logger klog.Logger, initialNode, node *v1.Node) bool {
	requiresUpdate := updateDefaultResources(initialNode, node)
	// 询问设备管理器节点是否被重建；若是，则在设备重新可用前将扩展资源清零。
	if kl.containerManager.ShouldResetExtendedResourceCapacity() {
		for k := range node.Status.Capacity {
			if v1helper.IsExtendedResourceName(k) {
				logger.Info("Zero out resource capacity in existing node", "resourceName", k, "node", klog.KObj(node))
				node.Status.Capacity[k] = *resource.NewQuantity(int64(0), resource.DecimalSI)
				node.Status.Allocatable[k] = *resource.NewQuantity(int64(0), resource.DecimalSI)
				requiresUpdate = true
			}
		}
	}
	return requiresUpdate
}

// updateDefaultResources 依据新构造的节点补齐既有节点缺失的 capacity/allocatable 映射。
func updateDefaultResources(initialNode, existingNode *v1.Node) bool {
	requiresUpdate := false
	if existingNode.Status.Capacity == nil {
		if initialNode.Status.Capacity != nil {
			existingNode.Status.Capacity = initialNode.Status.Capacity.DeepCopy()
			requiresUpdate = true
		} else {
			existingNode.Status.Capacity = make(map[v1.ResourceName]resource.Quantity)
		}
	}

	if existingNode.Status.Allocatable == nil {
		if initialNode.Status.Allocatable != nil {
			existingNode.Status.Allocatable = initialNode.Status.Allocatable.DeepCopy()
			requiresUpdate = true
		} else {
			existingNode.Status.Allocatable = make(map[v1.ResourceName]resource.Quantity)
		}
	}
	return requiresUpdate
}

// updateDefaultLabels 用 kubelet 构造的默认标签更新既有节点上的对应标签。
func (kl *Kubelet) updateDefaultLabels(initialNode, existingNode *v1.Node) bool {
	defaultLabels := []string{
		v1.LabelHostname,
		v1.LabelTopologyZone,
		v1.LabelTopologyRegion,
		v1.LabelFailureDomainBetaZone,
		v1.LabelFailureDomainBetaRegion,
		v1.LabelInstanceTypeStable,
		v1.LabelInstanceType,
		v1.LabelOSStable,
		v1.LabelArchStable,
		v1.LabelWindowsBuild,
		kubeletapis.LabelOS,
		kubeletapis.LabelArch,
	}

	needsUpdate := false
	if existingNode.Labels == nil {
		existingNode.Labels = make(map[string]string)
	}
	// 以新构造节点的值为准；空值标签直接删除，避免污染节点元数据。
	for _, label := range defaultLabels {
		if _, hasInitialValue := initialNode.Labels[label]; !hasInitialValue {
			continue
		}

		if existingNode.Labels[label] != initialNode.Labels[label] {
			existingNode.Labels[label] = initialNode.Labels[label]
			needsUpdate = true
		}

		if existingNode.Labels[label] == "" {
			delete(existingNode.Labels, label)
		}
	}

	return needsUpdate
}

// reconcileCMADAnnotationWithExistingNode 协调新构造节点与既有节点上
// 由控制器管理的 attach-detach 注解，返回既有节点是否需要更新。
func (kl *Kubelet) reconcileCMADAnnotationWithExistingNode(logger klog.Logger, node, existingNode *v1.Node) bool {
	var (
		existingCMAAnnotation    = existingNode.Annotations[volutil.ControllerManagedAttachAnnotation]
		newCMAAnnotation, newSet = node.Annotations[volutil.ControllerManagedAttachAnnotation]
	)

	if newCMAAnnotation == existingCMAAnnotation {
		return false
	}

	// 两者注解值不一致时，以 kubelet 当前配置为准更新既有节点。
	if !newSet {
		logger.Info("Controller attach-detach setting changed to false; updating existing Node")
		delete(existingNode.Annotations, volutil.ControllerManagedAttachAnnotation)
	} else {
		logger.Info("Controller attach-detach setting changed to true; updating existing Node")
		if existingNode.Annotations == nil {
			existingNode.Annotations = make(map[string]string)
		}
		existingNode.Annotations[volutil.ControllerManagedAttachAnnotation] = newCMAAnnotation
	}

	return true
}

// initialNode 构造 kubelet 的初始 v1.Node：合并节点标签、云厂商信息与 kubelet 配置。
func (kl *Kubelet) initialNode(ctx context.Context) (*v1.Node, error) {
	logger := klog.FromContext(ctx)
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(kl.nodeName),
			Labels: map[string]string{
				v1.LabelHostname:      kl.hostname,
				v1.LabelOSStable:      goruntime.GOOS,
				v1.LabelArchStable:    goruntime.GOARCH,
				kubeletapis.LabelOS:   goruntime.GOOS,
				kubeletapis.LabelArch: goruntime.GOARCH,
			},
		},
	}
	osLabels, err := getOSSpecificLabels()
	if err != nil {
		return nil, err
	}
	for label, value := range osLabels {
		node.Labels[label] = value
	}

	nodeTaints := make([]v1.Taint, len(kl.registerWithTaints))
	copy(nodeTaints, kl.registerWithTaints)

	if kl.externalCloudProvider {
		taint := v1.Taint{
			Key:    cloudproviderapi.TaintExternalCloudProvider,
			Value:  "true",
			Effect: v1.TaintEffectNoSchedule,
		}

		nodeTaints = append(nodeTaints, taint)
	}
	if len(nodeTaints) > 0 {
		node.Spec.Taints = nodeTaints
	}

	if kl.enableControllerAttachDetach {
		if node.Annotations == nil {
			node.Annotations = make(map[string]string)
		}

		logger.V(2).Info("Setting node annotation to enable volume controller attach/detach")
		node.Annotations[volutil.ControllerManagedAttachAnnotation] = "true"
	} else {
		logger.V(2).Info("Controller attach/detach is disabled for this node; Kubelet will attach and detach volumes")
	}

	// @question: 这里是否应该放到调用云厂商之后？云厂商同样会应用标签。
	// 用户显式配置的标签覆盖默认值，并记录日志以暴露覆盖行为。
	for k, v := range kl.nodeLabels {
		if cv, found := node.ObjectMeta.Labels[k]; found {
			logger.Info("the node label will overwrite default setting", "labelKey", k, "labelValue", v, "default", cv)
		}
		node.ObjectMeta.Labels[k] = v
	}

	if kl.providerID != "" {
		node.Spec.ProviderID = kl.providerID
	}

	kl.setNodeStatus(ctx, node)

	return node, nil
}

// fastNodeStatusUpdate 是 syncNodeStatus 的“轻量”版本：除最后一轮外不访问 apiserver，
// 由 fastStatusUpdateOnce 在每轮循环中调用。它与 syncNodeStatus 持有同一把锁，
// 因而可与其并发调用；返回值表示调用方循环是否应退出（即最后一轮），
// 同时会设置 kl.containerRuntimeReadyExpected。
func (kl *Kubelet) fastNodeStatusUpdate(ctx context.Context, timeout bool) (completed bool) {
	logger := klog.FromContext(ctx)
	kl.syncNodeStatusMux.Lock()
	defer func() {
		kl.syncNodeStatusMux.Unlock()

		if completed {
			// containerRuntimeReadyExpected 会被 updateRuntimeUp() 读取。
			// 该路径只会执行一次，因此不值得为它引入更细粒度的锁。
			kl.updateRuntimeMux.Lock()
			defer kl.updateRuntimeMux.Unlock()
			kl.containerRuntimeReadyExpected = true
		}
	}()

	if timeout {
		logger.Error(nil, "Node not becoming ready in time after startup")
		return true
	}

	originalNode, err := kl.GetNode(ctx)
	if err != nil {
		logger.Error(err, "Error getting the current node from lister")
		return false
	}

	readyIdx, originalNodeReady := nodeutil.GetNodeCondition(&originalNode.Status, v1.NodeReady)
	if readyIdx == -1 {
		logger.Error(nil, "Node does not have NodeReady condition", "originalNode", originalNode)
		return false
	}

	if originalNodeReady.Status == v1.ConditionTrue {
		return true
	}

	// 常规 syncNodeStatus 也会做这件事，这里提前执行是为了更早拿到容器运行时状态。
	// updateRuntimeUp 自带互斥锁，且不会递归调用 fastNodeStatusUpdate 或 syncNodeStatus。
	kl.updateRuntimeUp(ctx)

	node, changed := kl.updateNode(ctx, originalNode)

	if !changed {
		// 这里不调用 markVolumesFromNode，留给常规 syncNodeStatus 统一处理。
		return false
	}

	readyIdx, nodeReady := nodeutil.GetNodeCondition(&node.Status, v1.NodeReady)
	if readyIdx == -1 {
		logger.Error(nil, "Node does not have NodeReady condition", "node", node)
		return false
	}

	if nodeReady.Status == v1.ConditionFalse {
		return false
	}

	logger.Info("Fast updating node status as it just became ready")
	if _, err := kl.patchNodeStatus(originalNode, node); err != nil {
		// originalNode 可能已过期，但 kubelet 当前状态足以让节点变为 Ready；
		// 改用 syncNodeStatus() 从 apiserver 重新读取后再试。
		logger.Error(err, "Error updating node status, will retry with syncNodeStatus")

		// 下面先 Unlock 再 Lock 是为了允许 kl.syncNodeStatus() 获取同一把锁。
		kl.syncNodeStatusMux.Unlock()
		kl.syncNodeStatus(ctx)
		// 若在 defer 解锁前加标志位判断，这次加锁本可省略；写在显式位置更易读。
		kl.syncNodeStatusMux.Lock()
	}

	// 这里不调用 markVolumesFromNode，留给常规 syncNodeStatus 统一处理。
	return true
}

// syncNodeStatus 应由后台 goroutine 周期性调用：在状态发生变化或距上次同步
// 已超过上报周期时把节点状态同步到控制面，必要时先完成 kubelet 注册。
func (kl *Kubelet) syncNodeStatus(ctx context.Context) {
	logger := klog.FromContext(ctx)
	kl.syncNodeStatusMux.Lock()
	defer kl.syncNodeStatusMux.Unlock()

	if kl.kubeClient == nil || kl.heartbeatClient == nil {
		return
	}
	if kl.registerNode {
		// 已注册时该调用会立即返回，不会重复注册。
		kl.registerWithAPIServer(ctx)
	}
	if err := kl.updateNodeStatus(ctx); err != nil {
		logger.Error(err, "Unable to update node status")
	}
}

// updateNodeStatus 在状态变化或到达上报周期时同步节点状态，失败则按固定次数重试。
func (kl *Kubelet) updateNodeStatus(ctx context.Context) error {
	logger := klog.FromContext(ctx)
	logger.V(5).Info("Updating node status")
	for i := 0; i < nodeStatusUpdateRetry; i++ {
		if err := kl.tryUpdateNodeStatus(ctx, i); err != nil {
			// 重试计数大于 0 说明并非首次失败，触发重复心跳失败回调。
			if i > 0 && kl.onRepeatedHeartbeatFailure != nil {
				kl.onRepeatedHeartbeatFailure()
			}
			logger.Error(err, "Error updating node status, will retry")
		} else {
			return nil
		}
	}
	return fmt.Errorf("update node status exceeds retry count")
}

// tryUpdateNodeStatus 执行一次节点状态同步尝试；首次读取走本地 lister，
// 后续重试直接访问 apiserver。
func (kl *Kubelet) tryUpdateNodeStatus(ctx context.Context, tryNumber int) error {
	// 在大规模集群中，这里对 Node 对象的 GET/PUT 是 apiserver 与 etcd 的主要负载来源。
	// 为降低控制面压力，正常路径从本地 lister 读取（数据略有延迟，但延迟很小，
	// 并不会显著增加冲突）；一旦出现冲突，后续重试都直接访问 etcd。
	var originalNode *v1.Node
	var err error

	if tryNumber == 0 {
		originalNode, err = kl.nodeLister.Get(string(kl.nodeName))
	} else {
		opts := metav1.GetOptions{}
		originalNode, err = kl.heartbeatClient.CoreV1().Nodes().Get(ctx, string(kl.nodeName), opts)
	}
	if err != nil {
		return fmt.Errorf("error getting node %q: %v", kl.nodeName, err)
	}
	if originalNode == nil {
		return fmt.Errorf("nil %q node object", kl.nodeName)
	}

	node, changed := kl.updateNode(ctx, originalNode)
	shouldPatchNodeStatus := changed || kl.isUpdateStatusPeriodExpired()

	if !shouldPatchNodeStatus {
		kl.markVolumesFromNode(node)
		return nil
	}

	// shouldPatchNodeStatus 为真有三种可能：
	// 1. 节点状态发生变化；
	// 2. lastStatusReportTime 为零值导致上报周期判定为已过期（kubelet 刚重启）；
	// 3. lastStatusReportTime 非零但已过期。
	// 对情况 1、2 重新计算随机延迟，避免所有节点在同一时刻向 apiserver 上报；
	// 情况 3 说明随机间隔已经用过一轮，此时把延迟归零，后续按固定周期上报。
	if changed || kl.lastStatusReportTime.IsZero() {
		kl.delayAfterNodeStatusChange = kl.calculateDelay()
	} else {
		kl.delayAfterNodeStatusChange = 0
	}
	updatedNode, err := kl.patchNodeStatus(originalNode, node)
	if err == nil {
		kl.markVolumesFromNode(updatedNode)
	}
	return err
}

// isUpdateStatusPeriodExpired 判断距上次上报是否已超过上报周期（含随机抖动延迟）。
func (kl *Kubelet) isUpdateStatusPeriodExpired() bool {
	return kl.clock.Since(kl.lastStatusReportTime) >= kl.nodeStatusReportFrequency+kl.delayAfterNodeStatusChange
}

// calculateDelay 返回 [-0.5, 0.5) 倍上报周期的随机抖动，用于打散各节点的心跳时间。
func (kl *Kubelet) calculateDelay() time.Duration {
	return time.Duration(float64(kl.nodeStatusReportFrequency) * (-0.5 + rand.Float64()))
}

// updateNode 复制 originalNode 并在副本上执行更新逻辑，
// 返回更新后的节点以及是否有任何字段发生变化。
func (kl *Kubelet) updateNode(ctx context.Context, originalNode *v1.Node) (*v1.Node, bool) {
	logger := klog.FromContext(ctx)
	node := originalNode.DeepCopy()

	podCIDRChanged := false
	if len(node.Spec.PodCIDRs) != 0 {
		// PodCIDR 之前可能已被更新，因此不能只判断 node.Spec.PodCIDR 是否非空；
		// 还需要确认 CIDR 是否真的发生变化，以决定是否触发同步。
		var err error
		podCIDRs := strings.Join(node.Spec.PodCIDRs, ",")
		if podCIDRChanged, err = kl.updatePodCIDR(ctx, podCIDRs); err != nil {
			logger.Error(err, "Error updating pod CIDR")
		}
	}

	// OS 标签与实际运行环境不一致时修正，防止节点被错误调度。
	areRequiredLabelsNotPresent := false
	osName, osLabelExists := node.Labels[v1.LabelOSStable]
	if !osLabelExists || osName != goruntime.GOOS {
		if len(node.Labels) == 0 {
			node.Labels = make(map[string]string)
		}
		node.Labels[v1.LabelOSStable] = goruntime.GOOS
		areRequiredLabelsNotPresent = true
	}
	// arch 标签缺失或不匹配时补写，保证调度器能正确识别节点架构。
	arch, archLabelExists := node.Labels[v1.LabelArchStable]
	if !archLabelExists || arch != goruntime.GOARCH {
		if len(node.Labels) == 0 {
			node.Labels = make(map[string]string)
		}
		node.Labels[v1.LabelArchStable] = goruntime.GOARCH
		areRequiredLabelsNotPresent = true
	}

	// 每次同步都重新计算全部状态字段，setNodeStatus 会覆盖旧值。
	kl.setNodeStatus(ctx, node)

	changed := podCIDRChanged || nodeStatusHasChanged(&originalNode.Status, &node.Status) || areRequiredLabelsNotPresent
	return node, changed
}

// patchNodeStatus 以 originalNode 为基线 patch API server 上的节点对象；
// 成功时刷新 kubelet 的上报时间，并返回更新后的节点。
func (kl *Kubelet) patchNodeStatus(originalNode, node *v1.Node) (*v1.Node, error) {
	// 只有状态确有变化或到达上报周期时才会走到这里，以减少控制面写入。
	updatedNode, _, err := nodeutil.PatchNodeStatus(kl.heartbeatClient.CoreV1(), types.NodeName(kl.nodeName), originalNode, node)
	if err != nil {
		return nil, err
	}
	kl.lastStatusReportTime = kl.clock.Now()

	readyIdx, readyCondition := nodeutil.GetNodeCondition(&updatedNode.Status, v1.NodeReady)
	if readyIdx >= 0 && readyCondition.Status == v1.ConditionTrue {
		kl.nodeStartupLatencyTracker.RecordNodeReady()
	}

	return updatedNode, nil
}

// markVolumesFromNode 用节点上的 VolumesInUse 状态更新 volumeManager。
//
// 在无需上报节点状态时，用读取到的节点对象调用本方法：即使 VolumesInUse
// 列表没有增删，也必须把卷标记为 ReportedInUse，写入卷管理器的 dsw。
//
// 原因是 kubelet 重启后 dsw 会重建，卷的 ReportedInUse 初始为 false，
// 而 Node 对象中的 VolumesInUse 仍保留上一次 kubelet 实例写入的状态。
//
// 卷重新加入 dsw 后，必须依据 Node.Status 的 VolumesInUse 同步 ReportedInUse 字段。
//
// MarkVolumesAsReportedInUse() 不能直接在 dsw 内完成，因为 dsw 访问不到 Node 对象；
// 也不能放在节点状态管理器初始化时，因为那时卷可能尚未加入 dsw。
//
// 另一种调用时机是节点状态 patch 成功后，用其返回的 updatedNode 调用本方法，
// 表明这些卷已经反映在节点状态中。
func (kl *Kubelet) markVolumesFromNode(node *v1.Node) {
	// 只更新“已上报”标记，不触发新的挂载或卸载动作。
	kl.volumeManager.MarkVolumesAsReportedInUse(node.Status.VolumesInUse)
}

// nodeRefWithUID 返回 kubelet 静态节点 ObjectReference 的副本，并尽力填充 Node 的 UID。
//
// kl.nodeRef 在 kubelet 启动时（NewMainKubelet）构造一次，那时 Node 对象尚不保证
// 已存在于 apiserver，因此它有意只带 kind+name、不带 UID
// （见 https://github.com/kubernetes/kubernetes/issues/42701）。这导致 kubelet
// 记录的所有节点事件都没有 involvedObject.uid，`kubectl describe node` 等消费方
// 无法按 UID 把事件关联到节点（见 https://github.com/kubernetes/kubernetes/issues/138524）。
//
// 一旦本地缓存中能看到 Node，就解析一次 UID 并把结果缓存到 kl.cachedNodeRef。
// 该缓存不会刷新：若 Node 被删除并以新 UID 重建，而 kubelet 仍在运行，
// 事件会继续使用最初的 UID。
func (kl *Kubelet) nodeRefWithUID() *v1.ObjectReference {
	// 快路径：已缓存过带 UID 的引用，直接返回。
	if ref := kl.cachedNodeRef.Load(); ref != nil {
		return ref
	}

	// kl.nodeRef 在 NewMainKubelet 中写入一次且不再变更，因此这里无需防范并发写；
	// nil 判断只用于没有设置该字段的测试。
	if kl.nodeRef == nil {
		return nil
	}

	ref := *kl.nodeRef
	if ref.UID == "" {
		// 单机模式没有 apiserver，也就没有可解析的 Node UID。
		if kl.kubeClient == nil {
			return kl.nodeRef
		}
		node, err := kl.nodeLister.Get(string(kl.nodeName))
		if err != nil || node.UID == "" {
			// 本地缓存中尚无 UID；先返回仅含名称的引用，下次调用再重试。
			return kl.nodeRef
		}
		ref.UID = node.UID
	}

	// 缓存已解析的引用。并发解析时 CompareAndSwap 只保留先写入者，
	// 随后 Load 返回胜出的那份。
	kl.cachedNodeRef.CompareAndSwap(nil, &ref)
	return kl.cachedNodeRef.Load()
}

// recordNodeStatusEvent 为节点记录一条指定类型与消息的事件。
func (kl *Kubelet) recordNodeStatusEvent(logger klog.Logger, eventType, event string) {
	logger.V(2).Info("Recording event message for node", "node", klog.KRef("", string(kl.nodeName)), "event", event)
	kl.recorder.Eventf(kl.nodeRefWithUID(), eventType, event, "Node %s status is now: %s", kl.nodeName, event)
}

// recordEvent 为当前节点记录事件；记录器使用带 UID 的 nodeRef 作为事件对象。
func (kl *Kubelet) recordEvent(eventType, event, message string) {
	kl.recorder.Eventf(kl.nodeRefWithUID(), eventType, event, "%s", message)
}

// recordNodeSchedulableEvent 在节点可调度性发生变化时记录事件。
func (kl *Kubelet) recordNodeSchedulableEvent(ctx context.Context, node *v1.Node) error {
	logger := klog.FromContext(ctx)
	kl.lastNodeUnschedulableLock.Lock()
	defer kl.lastNodeUnschedulableLock.Unlock()
	// 仅在可调度性翻转时记录，避免周期性心跳重复产生事件。
	if kl.lastNodeUnschedulable != node.Spec.Unschedulable {
		if node.Spec.Unschedulable {
			kl.recordNodeStatusEvent(logger, v1.EventTypeNormal, events.NodeNotSchedulable)
		} else {
			kl.recordNodeStatusEvent(logger, v1.EventTypeNormal, events.NodeSchedulable)
		}
		kl.lastNodeUnschedulable = node.Spec.Unschedulable
	}
	return nil
}

// setNodeStatus 填充给定 Node 的 Status 字段，已设置的值会被覆盖。
// TODO(madhusudancs): Simplify the logic for setting node conditions and
// refactor the node status condition code out to a different file.
func (kl *Kubelet) setNodeStatus(ctx context.Context, node *v1.Node) {
	logger := klog.FromContext(ctx)
	// 单个 setter 失败只记录日志，不阻断其余字段，保证节点状态尽量可用。
	for i, f := range kl.setNodeStatusFuncs {
		logger.V(5).Info("Setting node status condition code", "position", i, "node", klog.KObj(node))
		if err := f(ctx, node); err != nil {
			logger.Error(err, "Failed to set some node status fields", "node", klog.KObj(node))
		}
	}
	// 仅在特性门控开启且确有声明特性时上报，避免向旧 apiserver 写入未知字段。
	if utilfeature.DefaultFeatureGate.Enabled(features.NodeDeclaredFeatures) && kl.nodeDeclaredFeatures != nil {
		node.Status.DeclaredFeatures = kl.nodeDeclaredFeatures
	}
}

// defaultNodeStatusFuncs 生成默认的 setNodeStatus 函数集合。
func (kl *Kubelet) defaultNodeStatusFuncs() []func(context.Context, *v1.Node) error {
	var setters []func(ctx context.Context, n *v1.Node) error
	setters = append(setters,
		nodestatus.NodeAddress(kl.nodeIPs, kl.nodeIPValidator, kl.hostname, kl.externalCloudProvider, utilnet.ResolveBindAddress),
		nodestatus.MachineInfo(string(kl.nodeName), kl.maxPods, kl.podsPerCore, kl.GetCachedMachineInfo, kl.containerManager.GetCapacity,
			kl.containerManager.GetDevicePluginResourceCapacity, kl.containerManager.GetNodeAllocatableReservation, kl.recordEvent, kl.runningInUserNS, kl.supportLocalStorageCapacityIsolation()),
		nodestatus.VersionInfo(kl.cadvisor.VersionInfo, kl.containerRuntime.Type, kl.containerRuntime.Version),
		nodestatus.DaemonEndpoints(kl.daemonEndpoints),
		nodestatus.Images(kl.nodeStatusMaxImages, kl.imageManager.GetImageList),
		nodestatus.GoRuntime(),
		nodestatus.RuntimeHandlers(kl.runtimeState.runtimeHandlers),
		nodestatus.NodeFeatures(kl.runtimeState.runtimeFeatures),
	)

	// 前半部分上报相对静态的信息（地址、容量、版本等），
	// 后半部分上报随运行时变化的条件与事件回调。
	setters = append(setters,
		nodestatus.MemoryPressureCondition(kl.clock.Now, kl.evictionManager.IsUnderMemoryPressure, kl.recordNodeStatusEvent),
		nodestatus.DiskPressureCondition(kl.clock.Now, kl.evictionManager.IsUnderDiskPressure, kl.recordNodeStatusEvent),
		nodestatus.PIDPressureCondition(kl.clock.Now, kl.evictionManager.IsUnderPIDPressure, kl.recordNodeStatusEvent),
		nodestatus.ReadyCondition(kl.clock.Now, kl.runtimeState.runtimeErrors, kl.runtimeState.networkErrors, kl.runtimeState.storageErrors,
			kl.containerManager.Status, kl.shutdownManager.ShutdownStatus, kl.recordNodeStatusEvent, kl.supportLocalStorageCapacityIsolation()),
		nodestatus.VolumesInUse(kl.volumeManager.ReconcilerStatesHasBeenSynced, kl.volumeManager.GetVolumesInUse),
		// TODO(mtaufen): I decided not to move this setter for now, since all it does is send an event
		// and record state back to the Kubelet runtime object. In the future, I'd like to isolate
		// these side-effects by decoupling the decisions to send events and partial status recording
		// from the Node setters.
		kl.recordNodeSchedulableEvent,
	)
	return setters
}

// validateNodeIP 校验给定 IP 属于本机网卡，且不是回环、组播等不可用地址。
func validateNodeIP(nodeIP net.IP) error {
	// 这些限制与 setNodeStatus() 中允许的地址类型保持一致。
	if nodeIP.To4() == nil && nodeIP.To16() == nil {
		return fmt.Errorf("nodeIP must be a valid IP address")
	}
	if nodeIP.IsLoopback() {
		return fmt.Errorf("nodeIP can't be loopback address")
	}
	if nodeIP.IsMulticast() {
		return fmt.Errorf("nodeIP can't be a multicast address")
	}
	if nodeIP.IsLinkLocalUnicast() {
		return fmt.Errorf("nodeIP can't be a link-local unicast address")
	}
	if nodeIP.IsUnspecified() {
		return fmt.Errorf("nodeIP can't be an all zeros address")
	}

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return err
	}
	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip != nil && ip.Equal(nodeIP) {
			return nil
		}
	}
	return fmt.Errorf("node IP: %q not found in the host's network interfaces", nodeIP.String())
}

// nodeStatusHasChanged 比较原始节点与当前节点的 Status，有变化则返回 true；
// 心跳时间戳不参与比较。
func nodeStatusHasChanged(originalStatus *v1.NodeStatus, status *v1.NodeStatus) bool {
	if originalStatus == nil && status == nil {
		return false
	}
	if originalStatus == nil || status == nil {
		return true
	}

	// 条件需单独比较：心跳时间每次上报都会变，不能参与整体 DeepEqual。
	if nodeConditionsHaveChanged(originalStatus.Conditions, status.Conditions) {
		return true
	}

	// 其余字段直接做语义比较。
	originalStatusCopy := originalStatus.DeepCopy()
	statusCopy := status.DeepCopy()
	originalStatusCopy.Conditions = nil
	statusCopy.Conditions = nil
	return !apiequality.Semantic.DeepEqual(originalStatusCopy, statusCopy)
}

// nodeConditionsHaveChanged 比较两组节点条件，有变化则返回 true；
// 心跳时间戳不参与比较。
func nodeConditionsHaveChanged(originalConditions []v1.NodeCondition, conditions []v1.NodeCondition) bool {
	if len(originalConditions) != len(conditions) {
		return true
	}

	originalConditionsCopy := make([]v1.NodeCondition, 0, len(originalConditions))
	originalConditionsCopy = append(originalConditionsCopy, originalConditions...)
	conditionsCopy := make([]v1.NodeCondition, 0, len(conditions))
	conditionsCopy = append(conditionsCopy, conditions...)

	// 条件顺序不保证稳定，先按类型排序再逐项比较。
	sort.SliceStable(originalConditionsCopy, func(i, j int) bool { return originalConditionsCopy[i].Type < originalConditionsCopy[j].Type })
	sort.SliceStable(conditionsCopy, func(i, j int) bool { return conditionsCopy[i].Type < conditionsCopy[j].Type })

	// LastHeartbeatTime 每次上报都会更新，比较前统一清零。
	replacedheartbeatTime := metav1.Time{}
	for i := range conditionsCopy {
		originalConditionsCopy[i].LastHeartbeatTime = replacedheartbeatTime
		conditionsCopy[i].LastHeartbeatTime = replacedheartbeatTime
		if !apiequality.Semantic.DeepEqual(&originalConditionsCopy[i], &conditionsCopy[i]) {
			return true
		}
	}
	return false
}
