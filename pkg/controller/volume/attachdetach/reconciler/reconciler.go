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

// Package reconciler 实现通过触发具体动作把期望状态与实际状态对齐的接口。
package reconciler

import (
	"context"
	"fmt"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/cache"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/metrics"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/statusupdater"
	kevents "k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/util/goroutinemap/exponentialbackoff"
	nodeutil "k8s.io/kubernetes/pkg/util/node"
	"k8s.io/kubernetes/pkg/util/taints"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/pkg/volume/util/operationexecutor"
)

// Reconciler 周期性运行协调循环，通过触发挂载/卸载操作把期望状态与实际状态对齐。
// 注意这与 kubelet 卷管理器里的 Reconciler 不同：前者服务于 attach/detach 控制器，
// 后者服务于 kubelet 卷管理器。
type Reconciler interface {
	// Run 启动周期性协调循环，检查应挂载的卷是否已挂载、应卸载的卷是否已卸载，
	// 不一致时触发相应操作来纠正。
	Run(ctx context.Context)
}

// NewReconciler 创建一个 Reconciler 实例，两次协调之间等待 loopPeriod。
// loopPeriod 是协调循环两次执行之间的间隔。
// maxWaitForUnmountDuration 是等待卷被安全卸载的最长时间，超时后仍会强制卸载，
// 以应对节点崩溃或不可用的情况；若期间卷被新 Pod 使用，则取消卸载请求并清空计时器。
func NewReconciler(
	loopPeriod time.Duration,
	maxWaitForUnmountDuration time.Duration,
	syncDuration time.Duration,
	disableReconciliationSync bool,
	disableForceDetachOnTimeout bool,
	desiredStateOfWorld cache.DesiredStateOfWorld,
	actualStateOfWorld cache.ActualStateOfWorld,
	attacherDetacher operationexecutor.OperationExecutor,
	nodeStatusUpdater statusupdater.NodeStatusUpdater,
	nodeLister corelisters.NodeLister,
	recorder record.EventRecorder) Reconciler {
	return &reconciler{
		loopPeriod:                  loopPeriod,
		maxWaitForUnmountDuration:   maxWaitForUnmountDuration,
		syncDuration:                syncDuration,
		disableReconciliationSync:   disableReconciliationSync,
		disableForceDetachOnTimeout: disableForceDetachOnTimeout,
		desiredStateOfWorld:         desiredStateOfWorld,
		actualStateOfWorld:          actualStateOfWorld,
		attacherDetacher:            attacherDetacher,
		nodeStatusUpdater:           nodeStatusUpdater,
		nodeLister:                  nodeLister,
		timeOfLastSync:              time.Now(),
		recorder:                    recorder,
	}
}

// reconciler 持有协调循环所需的缓存、执行器与配置参数。
type reconciler struct {
	loopPeriod                  time.Duration
	maxWaitForUnmountDuration   time.Duration
	syncDuration                time.Duration
	desiredStateOfWorld         cache.DesiredStateOfWorld
	actualStateOfWorld          cache.ActualStateOfWorld
	attacherDetacher            operationexecutor.OperationExecutor
	nodeStatusUpdater           statusupdater.NodeStatusUpdater
	nodeLister                  corelisters.NodeLister
	timeOfLastSync              time.Time
	disableReconciliationSync   bool
	disableForceDetachOnTimeout bool
	recorder                    record.EventRecorder
}

// Run 按 loopPeriod 周期性地调用协调函数，直到 ctx 被取消。
func (rc *reconciler) Run(ctx context.Context) {
	wait.UntilWithContext(ctx, rc.reconciliationLoopFunc(ctx), rc.loopPeriod)
}

// reconciliationLoopFunc 返回协调循环体，可通过命令行选项 disableReconciliation 关闭。
// 它周期性核对实际状态中记录的已挂载卷是否仍挂载在节点上，不一致时更新状态。
func (rc *reconciler) reconciliationLoopFunc(ctx context.Context) func(context.Context) {
	return func(ctx context.Context) {

		rc.reconcile(ctx)
		logger := klog.FromContext(ctx)
		if rc.disableReconciliationSync {
			// 命令行显式关闭时不做核对，避免对节点状态产生额外写请求。
			logger.V(5).Info("Skipping reconciling attached volumes still attached since it is disabled via the command line")
		} else if rc.syncDuration < time.Second {
			// 间隔过短会让核对请求压垮 API Server，因此小于一秒直接跳过。
			logger.V(5).Info("Skipping reconciling attached volumes still attached since it is set to less than one second via the command line")
		} else if time.Since(rc.timeOfLastSync) > rc.syncDuration {
			// 只有距上次核对超过配置间隔才执行，防止每个协调周期都重复核对。
			logger.V(5).Info("Starting reconciling attached volumes still attached")
			rc.sync()
		}
	}
}

// sync 执行一次挂载状态核对，并保证无论成功与否都刷新上次核对时间。
func (rc *reconciler) sync() {
	defer rc.updateSyncTime()
	rc.syncStates()
}

// updateSyncTime 记录本次核对时间，供下一次协调判断是否已到间隔。
func (rc *reconciler) updateSyncTime() {
	rc.timeOfLastSync = time.Now()
}

// syncStates 按节点聚合已挂载卷，交给操作执行器向节点核实实际挂载情况。
func (rc *reconciler) syncStates() {
	volumesPerNode := rc.actualStateOfWorld.GetAttachedVolumesPerNode()
	rc.attacherDetacher.VerifyVolumesAreAttached(volumesPerNode, rc.actualStateOfWorld)
}

// hasOutOfServiceTaint 判断节点是否带有 out-of-service 污点。
func (rc *reconciler) hasOutOfServiceTaint(nodeName types.NodeName) (bool, error) {
	node, err := rc.nodeLister.Get(string(nodeName))
	if err != nil {
		return false, err
	}
	return taints.TaintKeyExists(node.Spec.Taints, v1.TaintNodeOutOfService), nil
}

// nodeIsHealthy 判断节点当前是否处于健康状态。
func (rc *reconciler) nodeIsHealthy(nodeName types.NodeName) (bool, error) {
	node, err := rc.nodeLister.Get(string(nodeName))
	if err != nil {
		return false, err
	}
	return nodeutil.IsNodeReady(node), nil
}

// reconcile 执行一次完整协调：先处理应卸载的卷，再处理应挂载的卷，最后刷新节点状态。
func (rc *reconciler) reconcile(ctx context.Context) {
	// 卸载先于挂载执行：被重新调度到其他节点的 Pod 所引用的卷，必须先从原节点解绑。

	// 确保不再被需要的卷都被卸载。
	logger := klog.FromContext(ctx)
	for _, attachedVolume := range rc.actualStateOfWorld.GetAttachedVolumes() {
		if !rc.desiredStateOfWorld.VolumeExists(
			attachedVolume.VolumeName, attachedVolume.NodeName) {

			// 若已有操作在排队就直接跳过，避免重复发起卸载。
			// 这一步必须放在其他检查之前：否则其他检查通过时卷可能刚好退出 pending 状态，
			// 导致同一卷被重复触发卸载。
			// 是否允许多节点挂载会改变操作键的格式，所以分两种分支判断。
			if util.IsMultiAttachAllowed(attachedVolume.VolumeSpec) {
				if !rc.attacherDetacher.IsOperationSafeToRetry(attachedVolume.VolumeName, "" /* podName */, attachedVolume.NodeName, operationexecutor.DetachOperationName) {
					logger.V(10).Info("Operation for volume is already running or still in exponential backoff for node. Can't start detach", "node", klog.KRef("", string(attachedVolume.NodeName)), "volumeName", attachedVolume.VolumeName)
					continue
				}
			} else {
				if !rc.attacherDetacher.IsOperationSafeToRetry(attachedVolume.VolumeName, "" /* podName */, "" /* nodeName */, operationexecutor.DetachOperationName) {
					logger.V(10).Info("Operation for volume is already running or still in exponential backoff in the cluster. Can't start detach for node", "node", klog.KRef("", string(attachedVolume.NodeName)), "volumeName", attachedVolume.VolumeName)
					continue
				}
			}

			// 卸载操作会在标记完成前先更新实际状态，因此卷可能在前两次检查之间已被移除。
			// 这里再查一次实际状态，避免发出不必要的卸载请求。
			// 参见 https://github.com/kubernetes/kubernetes/issues/93902
			attachState := rc.actualStateOfWorld.GetAttachState(attachedVolume.VolumeName, attachedVolume.NodeName)
			if attachState == cache.AttachStateDetached {
				logger.V(5).Info("Volume detached--skipping", "node", klog.KRef("", string(attachedVolume.NodeName)), "volumeName", attachedVolume.VolumeName)
				continue
			}

			// 记录卸载请求时间，后续据此判断安全卸载的宽限期是否耗尽。
			elapsedTime, err := rc.actualStateOfWorld.SetDetachRequestTime(logger, attachedVolume.VolumeName, attachedVolume.NodeName)
			if err != nil {
				logger.Error(err, "Cannot trigger detach because it fails to set detach request time with error")
				continue
			}
			// 宽限期一旦耗尽，后续才可能走强制卸载分支。
			maxWaitForUnmountDurationExpired := elapsedTime > rc.maxWaitForUnmountDuration

			isHealthy, err := rc.nodeIsHealthy(attachedVolume.NodeName)
			if err != nil {
				logger.V(5).Info("Failed to get health of node",
					"node", klog.KRef("", string(attachedVolume.NodeName)),
					"err", err)
			}

			// 仅在允许超时强制卸载时，才把“宽限期耗尽”视为强制卸载条件；
			// 这里判断准确，下面才能记录正确的强制卸载指标。
			forceDetachTimeoutExpired := maxWaitForUnmountDurationExpired && !rc.disableForceDetachOnTimeout
			if maxWaitForUnmountDurationExpired && rc.disableForceDetachOnTimeout {
				logger.V(5).Info("Drain timeout expired for volume but disableForceDetachOnTimeout was set", "node", klog.KRef("", string(attachedVolume.NodeName)), "volumeName", attachedVolume.VolumeName)
			}
			forceDetach := !isHealthy && forceDetachTimeoutExpired

			hasOutOfServiceTaint, err := rc.hasOutOfServiceTaint(attachedVolume.NodeName)
			if err != nil {
				logger.V(5).Info("Failed to get taint specs for node",
					"node", klog.KRef("", string(attachedVolume.NodeName)),
					"err", err)
			}

			// 卷仍被挂载时通常跳过卸载，避免数据损坏；仅在强制卸载或节点带 out-of-service 污点时放行。
			if attachedVolume.MountedByNode && !forceDetach && !hasOutOfServiceTaint {
				logger.V(5).Info("Cannot detach volume because it is still mounted", "node", klog.KRef("", string(attachedVolume.NodeName)), "volumeName", attachedVolume.VolumeName)
				continue
			}

			// 触发卸载前先把卷从“报告为已挂载”中移除并刷新节点状态；
			// 节点状态更新失败则跳过卸载，卸载操作失败时还要把卷加回去，保证节点状态里的挂载信息准确。
			err = rc.actualStateOfWorld.RemoveVolumeFromReportAsAttached(attachedVolume.VolumeName, attachedVolume.NodeName)
			if err != nil {
				logger.V(5).Info("RemoveVolumeFromReportAsAttached failed while removing volume from node",
					"node", klog.KRef("", string(attachedVolume.NodeName)),
					"volumeName", attachedVolume.VolumeName,
					"err", err)
			}

			// 刷新节点状态，表明该卷已不再可安全挂载。
			err = rc.nodeStatusUpdater.UpdateNodeStatusForNode(logger, attachedVolume.NodeName)
			if err != nil {
				// Skip detaching this volume if unable to update node status
				logger.Error(err, "UpdateNodeStatusForNode failed while attempting to report volume as attached", "node", klog.KRef("", string(attachedVolume.NodeName)), "volumeName", attachedVolume.VolumeName)
				// 节点状态更新失败时把卷加回“报告为已挂载”，让状态更新器重新写回卷列表；
				// 此处 DetachVolume 尚未真正调用，必须自行回滚以维持每次协调的一致性。
				rc.actualStateOfWorld.AddVolumeToReportAsAttached(logger, attachedVolume.VolumeName, attachedVolume.NodeName)
				continue
			}

			// 触发卸载：正常路径要先确认可安全卸载；超时强制卸载或节点带 NoExecute 的 out-of-service 污点时跳过该检查。
			logger.V(5).Info("Starting attacherDetacher.DetachVolume", "node", klog.KRef("", string(attachedVolume.NodeName)), "volumeName", attachedVolume.VolumeName)
			if hasOutOfServiceTaint {
				logger.V(4).Info("node has out-of-service taint", "node", klog.KRef("", string(attachedVolume.NodeName)))
			}
			verifySafeToDetach := !forceDetachTimeoutExpired && !hasOutOfServiceTaint
			err = rc.attacherDetacher.DetachVolume(logger, attachedVolume.AttachedVolume, verifySafeToDetach, rc.actualStateOfWorld)
			if err == nil {
				if verifySafeToDetach { // normal detach
					logger.Info("attacherDetacher.DetachVolume started", "node", klog.KRef("", string(attachedVolume.NodeName)), "volumeName", attachedVolume.VolumeName)
				} else { // force detach
					if forceDetachTimeoutExpired {
						metrics.RecordForcedDetachMetric(metrics.ForceDetachReasonTimeout)
						logger.Info("attacherDetacher.DetachVolume started: this volume is not safe to detach, but maxWaitForUnmountDuration expired, force detaching",
							"duration", rc.maxWaitForUnmountDuration,
							"node", klog.KRef("", string(attachedVolume.NodeName)),
							"volumeName", attachedVolume.VolumeName)
					} else {
						metrics.RecordForcedDetachMetric(metrics.ForceDetachReasonOutOfService)
						logger.Info("attacherDetacher.DetachVolume started: node has out-of-service taint, force detaching",
							"node", klog.KRef("", string(attachedVolume.NodeName)),
							"volumeName", attachedVolume.VolumeName)
					}
				}
			}
			if err != nil {
				// 卸载启动失败时同样把卷加回“报告为已挂载”，供节点状态更新器写回；
				// 尽管操作生成器里也会做这件事，但 DetachVolume 可能在真正执行前就失败（如找不到卷插件），
				// 所以这里必须补一次回滚。
				rc.actualStateOfWorld.AddVolumeToReportAsAttached(logger, attachedVolume.VolumeName, attachedVolume.NodeName)

				if !exponentialbackoff.IsExponentialBackoff(err) {
					// 指数退避错误属于预期情况，不予记录；只记录真正的异常。
					logger.Error(err, "attacherDetacher.DetachVolume failed to start", "node", klog.KRef("", string(attachedVolume.NodeName)), "volumeName", attachedVolume.VolumeName)
				}
			}
		}
	}

	rc.attachDesiredVolumes(logger)

	// 协调最后统一刷新节点状态，使上述挂载/卸载变更对外可见。
	err := rc.nodeStatusUpdater.UpdateNodeStatuses(logger)
	if err != nil {
		logger.Info("UpdateNodeStatuses failed", "err", err)
	}
}

// attachDesiredVolumes 确保期望状态中待挂载的卷都已挂载，并识别多挂载冲突。
func (rc *reconciler) attachDesiredVolumes(logger klog.Logger) {
	// 确保应挂载的卷都已挂载。
	for _, volumeToAttach := range rc.desiredStateOfWorld.GetVolumesToAttach() {
		if util.IsMultiAttachAllowed(volumeToAttach.VolumeSpec) {
			// 给定卷和节点已有操作在运行时直接跳过，避免重复发起挂载。
			if rc.attacherDetacher.IsOperationPending(volumeToAttach.VolumeName, "" /* podName */, volumeToAttach.NodeName) {
				logger.V(10).Info("Operation for volume is already running for node. Can't start attach", "node", klog.KRef("", string(volumeToAttach.NodeName)), "volumeName", volumeToAttach.VolumeName)
				continue
			}
		} else {
			// 不允许多挂载时，只要该卷已有任何操作在跑就不再发起新的挂载。
			if rc.attacherDetacher.IsOperationPending(volumeToAttach.VolumeName, "" /* podName */, "" /* nodeName */) {
				logger.V(10).Info("Operation for volume is already running. Can't start attach for node", "node", klog.KRef("", string(volumeToAttach.NodeName)), "volumeNames", volumeToAttach.VolumeName)
				continue
			}
		}

		// 挂载操作会先更新实际状态再标记完成，所以必须先查待处理操作、再读挂载状态，
		// 才能保证读到的实际状态是最新的。
		// 参见 https://github.com/kubernetes/kubernetes/issues/93902
		attachState := rc.actualStateOfWorld.GetAttachState(volumeToAttach.VolumeName, volumeToAttach.NodeName)
		if attachState == cache.AttachStateAttached {
			// 卷已在目标节点上：只需重置卸载请求时间，防止之前的卸载计时继续累积。
			logger.V(10).Info("Volume attached--touching", "volume", volumeToAttach)
			rc.actualStateOfWorld.ResetDetachRequestTime(logger, volumeToAttach.VolumeName, volumeToAttach.NodeName)
			continue
		}

		if !util.IsMultiAttachAllowed(volumeToAttach.VolumeSpec) {
			nodes := rc.actualStateOfWorld.GetNodesForAttachedVolume(volumeToAttach.VolumeName)
			if len(nodes) > 0 {
				if !volumeToAttach.MultiAttachErrorReported {
					rc.reportMultiAttachError(logger, volumeToAttach, nodes)
					rc.desiredStateOfWorld.SetMultiAttachError(volumeToAttach.VolumeName, volumeToAttach.NodeName)
				}
				continue
			}
		}

		// 卷尚未挂载到目标节点：启动异步挂载操作。
		logger.V(5).Info("Starting attacherDetacher.AttachVolume", "volume", volumeToAttach)
		err := rc.attacherDetacher.AttachVolume(logger, volumeToAttach.VolumeToAttach, rc.actualStateOfWorld)
		if err == nil {
			logger.Info("attacherDetacher.AttachVolume started", "volumeName", volumeToAttach.VolumeName, "nodeName", volumeToAttach.NodeName, "scheduledPods", klog.KObjSlice(volumeToAttach.ScheduledPods))
		}
		if err != nil && !exponentialbackoff.IsExponentialBackoff(err) {
			// 指数退避错误是重试机制的一部分，属预期情况；只记录其他错误。
			logger.Error(err, "attacherDetacher.AttachVolume failed to start", "volumeName", volumeToAttach.VolumeName, "nodeName", volumeToAttach.NodeName, "scheduledPods", klog.KObjSlice(volumeToAttach.ScheduledPods))
		}
	}
}

// reportMultiAttachError 在卷应挂载到某节点却已被其他节点占用时发出事件并记录日志。
func (rc *reconciler) reportMultiAttachError(logger klog.Logger, volumeToAttach cache.VolumeToAttach, nodes []types.NodeName) {
	// 从已挂载节点列表中排除当前节点，得到真正冲突的其他节点。
	// 有些接口需要 []string、有些需要 []NodeName，因此两种形式都收集。
	// 理论上控制器不允许同一卷挂到多个节点，列表里最多只有一个元素，但用数组更稳妥。
	otherNodes := []types.NodeName{}
	otherNodesStr := []string{}
	for _, node := range nodes {
		if node != volumeToAttach.NodeName {
			otherNodes = append(otherNodes, node)
			otherNodesStr = append(otherNodesStr, string(node))
		}
	}

	// 查出占用该卷的其他节点上的 Pod，用于向用户说明是谁在阻塞挂载。
	pods := rc.desiredStateOfWorld.GetVolumePodsOnNodes(otherNodes, volumeToAttach.VolumeName)
	if len(pods) == 0 {
		// 找不到占用卷的 Pod，说明它已被删除，只需提示等待卸载即可。
		simpleMsg, _ := volumeToAttach.GenerateMsg("Waiting for detach", "Volume is already exclusively attached to one node, waiting on detach before it can be attached to another node")
		for _, pod := range volumeToAttach.ScheduledPods {
			rc.recorder.Eventf(pod, v1.EventTypeWarning, kevents.FailedAttachVolume, "%s", simpleMsg)
		}
		// 详细的节点信息只写日志给管理员，不塞进面向用户的事件。
		logger.Info("Waiting for detach: volume is already exclusively attached, waiting on detach before it can be attached to another node", "attachedTo", otherNodesStr, "volume", volumeToAttach)
		return
	}

	// 确有 Pod 在其他节点上占用该卷，通常是用户配置问题（例如 ReplicaSet 用同一 PVC 且副本数大于一），
	// 需要在事件里告知用户具体是哪些 Pod 在阻塞挂载。
	for _, scheduledPod := range volumeToAttach.ScheduledPods {
		// 每个待挂载 Pod 都要单独生成消息：它们可能位于不同命名空间，
		// 而某命名空间的用户不应看到其他命名空间里的 Pod 名称。
		localPodNames := []string{} // Names of pods in scheduledPods's namespace
		otherPods := 0              // Count of pods in other namespaces
		for _, pod := range pods {
			if pod.Namespace == scheduledPod.Namespace {
				localPodNames = append(localPodNames, pod.Name)
			} else {
				otherPods++
			}
		}

		// 同命名空间的 Pod 直接列名，其他命名空间的只报数量，避免信息越界。
		var msg string
		if len(localPodNames) > 0 {
			msg = fmt.Sprintf("Volume is already used by pod(s) %s", strings.Join(localPodNames, ", "))
			if otherPods > 0 {
				msg = fmt.Sprintf("%s and %d pod(s) in different namespaces", msg, otherPods)
			}
		} else {
			// 没有同命名空间的 Pod，只有跨命名空间的占用者。
			msg = fmt.Sprintf("Volume is already used by %d pod(s) in different namespaces", otherPods)
		}
		simpleMsg, _ := volumeToAttach.GenerateMsg("Waiting for detach", msg)
		rc.recorder.Eventf(scheduledPod, v1.EventTypeWarning, kevents.FailedAttachVolume, "%s", simpleMsg)
	}

	// 完整 Pod 列表只记录到日志，供管理员排查，事件里保持脱敏摘要。
	logger.Info("Waiting for detach: volume is in use by pods", "pods", klog.KObjSlice(pods), "attachedTo", otherNodesStr, "volume", volumeToAttach)
}
