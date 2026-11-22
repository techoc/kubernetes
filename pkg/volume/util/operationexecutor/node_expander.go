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

package operationexecutor

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	"k8s.io/kubectl/pkg/util/storage"
	kevents "k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/volume/util"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
)

// NodeExpander 负责在节点侧执行卷扩容，并处理扩容状态与错误标记。
type NodeExpander struct {
	nodeResizeOperationOpts
	kubeClient clientset.Interface
	recorder   record.EventRecorder

	// 由预检查计算得出。
	pvcStatusCap resource.Quantity
	resizeStatus v1.ClaimResourceStatus

	// 若节点扩容失败，应将本次扩容标记为不可行，使控制器按用户新请求的大小重新协调。
	markExpansionInfeasibleOnFailure bool

	// pvcAlreadyUpdated 为真表示虽然 kubelet 仍在调用 NodeExpandVolume，
	// PVC 实际已更新——可能扩容已在其他节点完成。RWX PVC 扩容时会遇到这种情况。
	pvcAlreadyUpdated bool

	// testStatus 仅供单元测试使用。
	testStatus testResponseData
}

// newNodeExpander 创建节点扩容器。
func newNodeExpander(resizeOp nodeResizeOperationOpts, client clientset.Interface, recorder record.EventRecorder) *NodeExpander {
	return &NodeExpander{
		kubeClient:              client,
		nodeResizeOperationOpts: resizeOp,
		recorder:                recorder,
	}
}

// testResponseData 仅用于单元测试中的断言。
type testResponseData struct {
	// 是否调用了底层卷驱动的扩容操作，主要用于测试。
	resizeCalledOnPlugin bool

	// kubelet 是否可认为扩容已结束。即使实际扩容并未完成，某些预检失败时
	// kubelet 也不应重试，此时同样视为结束；当然也可能是真的完成了。
	assumeResizeFinished bool
}

// runPreCheck 在扩容前做健全性检查；仅当允许继续节点扩容时返回 true。
func (ne *NodeExpander) runPreCheck() bool {
	ne.pvcStatusCap = ne.pvc.Status.Capacity[v1.ResourceStorage]

	allocatedResourceStatus := ne.pvc.Status.AllocatedResourceStatuses
	if currentStatus, ok := allocatedResourceStatus[v1.ResourceStorage]; ok {
		ne.resizeStatus = currentStatus
	}

	pvcSpecCap := ne.pvc.Spec.Resources.Requests[v1.ResourceStorage]

	// 通常节点扩容时 PV 与 PVC 规格大小应一致；若用户中途改过 PVC 且扩容以最终错误告终，
	// 应把整个节点扩容标记为不可行，交由控制器重新协调。
	if pvcSpecCap.Cmp(ne.pluginResizeOpts.NewSize) != 0 &&
		ne.actualStateOfWorld.CheckVolumeInFailedExpansionWithFinalErrors(ne.vmt.VolumeName) {
		ne.markExpansionInfeasibleOnFailure = true
	}

	if ne.pvcStatusCap.Cmp(ne.pluginResizeOpts.NewSize) >= 0 && ne.resizeStatus == "" {
		ne.pvcAlreadyUpdated = true
	}

	// 卷已扩容但为 RWX 类型、且 PVC 未标注无需节点扩容时，仍应允许节点扩容继续。
	//
	// RWX 卷的扩容必须在所有节点上执行，即便卷本身已经扩容过，故需此特例。
	if ne.pvcAlreadyUpdated &&
		storage.ContainsAccessMode(ne.pvc.Spec.AccessModes, v1.ReadWriteMany) &&
		!metav1.HasAnnotation(ne.pvc.ObjectMeta, volumetypes.NodeExpansionNotRequired) {
		return true
	}

	// 恢复特性仅在新版扩容控制器上可用。
	if ne.resizeStatus == "" {
		return false
	}

	resizeStatusVal := ne.resizeStatus

	// resizeStatus 为空、NodeExpansionInProgress 或 NodeExpansionPending 时，允许节点扩容继续。
	if resizeStatusVal == v1.PersistentVolumeClaimNodeResizePending ||
		resizeStatusVal == v1.PersistentVolumeClaimNodeResizeInProgress {
		return true
	}
	return false
}

// expandOnPlugin 在预检通过后调用卷插件执行节点扩容，并更新 PVC 状态；
// 返回是否成功、当前卷大小及错误。
func (ne *NodeExpander) expandOnPlugin() (bool, resource.Quantity, error) {
	allowExpansion := ne.runPreCheck()
	if !allowExpansion {
		if ne.pvcAlreadyUpdated {
			// PVC 已更新却走到这里，说明 ASOW 中的大小偏小，控制器已完成完整扩容，
			// 无需节点扩容；标记结束以阻止协调器重试。
			ne.testStatus = testResponseData{assumeResizeFinished: true, resizeCalledOnPlugin: false}
			return true, ne.pluginResizeOpts.NewSize, nil
		}

		klog.V(3).Infof("NodeExpandVolume is not allowed to proceed for volume %s with resizeStatus %s", ne.vmt.VolumeName, ne.resizeStatus)
		ne.testStatus = testResponseData{false /* resizeCalledOnPlugin */, true /* assumeResizeFinished */}
		return false, ne.pluginResizeOpts.OldSize, nil
	}

	var err error
	nodeName := ne.vmt.Pod.Spec.NodeName

	if !ne.pvcAlreadyUpdated {
		ne.pvc, err = util.MarkNodeExpansionInProgress(ne.pvc, ne.kubeClient)

		if err != nil {
			msg := ne.vmt.GenerateErrorDetailed("MountVolume.NodeExpandVolume failed to mark node expansion in progress: %v", err)
			klog.Error(msg.Error())
			ne.testStatus = testResponseData{}
			return false, ne.pluginResizeOpts.OldSize, err
		}
	}
	_, resizeErr := ne.volumePlugin.NodeExpand(ne.pluginResizeOpts)
	if resizeErr != nil {
		// 为支持 RWX 卷在不同节点上的扩容，预检绕过了 VolumeExpansionPendingOnNode
		// 状态检查，直接调用插件的 NodeExpandVolume。
		//
		// 但若 CSI 驱动本身不支持节点扩容，则不应视为失败。这是针对此问题的临时方案：
		// https://github.com/kubernetes/kubernetes/issues/131381.
		//
		// 其他访问模式不会走到此状态，因为 kubelet 会先等待 VolumeExpansionPendingOnNode
		// 再尝试扩容。参见上面的 runPreCheck()。
		//
		// 卷已扩容时，若驱动返回 OperationNotSupportedError，不应在节点上重试。
		if volumetypes.IsOperationNotSupportedError(resizeErr) && ne.pvcAlreadyUpdated {
			klog.V(4).InfoS(ne.vmt.GenerateMsgDetailed("MountVolume.NodeExpandVolume failed", "NodeExpandVolume not supported"), "pod", klog.KObj(ne.vmt.Pod))
			ne.testStatus = testResponseData{assumeResizeFinished: true, resizeCalledOnPlugin: false}
			return true, ne.pluginResizeOpts.NewSize, nil
		}

		if volumetypes.IsOperationFinishedError(resizeErr) {
			var markFailedError error
			ne.actualStateOfWorld.MarkVolumeExpansionFailedWithFinalError(ne.vmt.VolumeName)
			if volumetypes.IsInfeasibleError(resizeErr) || ne.markExpansionInfeasibleOnFailure {
				ne.pvc, markFailedError = util.MarkNodeExpansionInfeasible(ne.pvc, ne.kubeClient, resizeErr)
				if markFailedError != nil {
					klog.Error(ne.vmt.GenerateErrorDetailed("MountVolume.NodeExpandVolume failed to mark node expansion as failed: %v", err).Error())
				}
			} else {
				ne.pvc, markFailedError = util.MarkNodeExpansionFailedCondition(ne.pvc, ne.kubeClient, resizeErr)
				if markFailedError != nil {
					klog.Error(ne.vmt.GenerateErrorDetailed("MountVolume.NodeExpandVolume failed to mark node expansion as failed: %v", err).Error())
				}
			}
		}

		// 驱动返回 FailedPrecondition 表示本节点不应重试扩容，但扩容操作不应阻塞挂载。
		if volumetypes.IsFailedPreconditionError(resizeErr) {
			ne.actualStateOfWorld.MarkForInUseExpansionError(ne.vmt.VolumeName)
			klog.Error(ne.vmt.GenerateErrorDetailed("MountVolume.NodeExpandVolume failed with %v", resizeErr).Error())
			ne.testStatus = testResponseData{assumeResizeFinished: true, resizeCalledOnPlugin: true}
			return false, ne.pluginResizeOpts.OldSize, nil
		}
		ne.testStatus = testResponseData{assumeResizeFinished: true, resizeCalledOnPlugin: true}
		return false, ne.pluginResizeOpts.OldSize, resizeErr
	}
	simpleMsg, detailedMsg := ne.vmt.GenerateMsg("MountVolume.NodeExpandVolume succeeded", nodeName)
	ne.recorder.Eventf(ne.vmt.Pod, v1.EventTypeNormal, kevents.FileSystemResizeSuccess, "%s", simpleMsg)
	ne.recorder.Eventf(ne.pvc, v1.EventTypeNormal, kevents.FileSystemResizeSuccess, "%s", simpleMsg)
	klog.InfoS(detailedMsg, "pod", klog.KObj(ne.vmt.Pod))

	ne.testStatus = testResponseData{true /*resizeCalledOnPlugin */, true /* assumeResizeFinished */}
	// PVC 已更新则无需重复更新。
	if ne.pvcAlreadyUpdated {
		return true, ne.pluginResizeOpts.NewSize, nil
	}

	// 文件系统扩容成功，更新 PVC 容量使其与 PV 一致。
	ne.pvc, err = util.MarkNodeExpansionFinishedWithRecovery(ne.pvc, ne.pluginResizeOpts.NewSize, ne.kubeClient)
	if err != nil {
		return true, ne.pluginResizeOpts.NewSize, fmt.Errorf("mountVolume.NodeExpandVolume update pvc status failed: %w", err)
	}
	return true, ne.pluginResizeOpts.NewSize, nil
}
