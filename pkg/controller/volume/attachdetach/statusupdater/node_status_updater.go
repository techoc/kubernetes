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

// Package statusupdater 提供更新 API 对象状态的接口实现，
// 主要用于将实际状态世界中已挂载的卷写回 Node 的 VolumesAttached 字段。
package statusupdater

import (
	"fmt"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/volume/attachdetach/cache"
)

// NodeStatusUpdater 定义更新 Node 状态中 VolumesAttached 字段的操作集合。
type NodeStatusUpdater interface {
	// UpdateNodeStatuses 遍历实际状态世界中所有待上报的节点并更新其状态。
	UpdateNodeStatuses(logger klog.Logger) error
	// UpdateNodeStatusForNode 更新指定节点上的所有待处理状态变更。
	UpdateNodeStatusForNode(logger klog.Logger, nodeName types.NodeName) error
}

// NewNodeStatusUpdater 返回一个 NodeStatusUpdater 实例。
func NewNodeStatusUpdater(
	kubeClient clientset.Interface,
	nodeLister corelisters.NodeLister,
	actualStateOfWorld cache.ActualStateOfWorld) NodeStatusUpdater {
	return &nodeStatusUpdater{
		actualStateOfWorld: actualStateOfWorld,
		nodeLister:         nodeLister,
		kubeClient:         kubeClient,
	}
}

// nodeStatusUpdater 是 NodeStatusUpdater 的默认实现，依赖实际状态世界、
// Node informer 缓存与 kubeClient 完成状态回写。
type nodeStatusUpdater struct {
	kubeClient         clientset.Interface
	nodeLister         corelisters.NodeLister
	actualStateOfWorld cache.ActualStateOfWorld
}

// UpdateNodeStatuses 批量更新所有待上报节点的 VolumesAttached 状态。
// 单个节点更新失败不会中断其他节点的处理，但最终会聚合为错误以触发上层重试。
func (nsu *nodeStatusUpdater) UpdateNodeStatuses(logger klog.Logger) error {
	var nodeIssues int
	// TODO: investigate right behavior if nodeName is empty
	// kubernetes/kubernetes/issues/37777
	nodesToUpdate := nsu.actualStateOfWorld.GetVolumesToReportAttached(logger)
	for nodeName, attachedVolumes := range nodesToUpdate {
		err := nsu.processNodeVolumes(logger, nodeName, attachedVolumes)
		if err != nil {
			nodeIssues += 1
		}
	}
	if nodeIssues > 0 {
		return fmt.Errorf("unable to update %d nodes", nodeIssues)
	}
	return nil
}

// UpdateNodeStatusForNode 仅更新指定节点的状态；若该节点没有待上报变更则直接返回。
func (nsu *nodeStatusUpdater) UpdateNodeStatusForNode(logger klog.Logger, nodeName types.NodeName) error {
	needsUpdate, attachedVolumes := nsu.actualStateOfWorld.GetVolumesToReportAttachedForNode(logger, nodeName)
	if !needsUpdate {
		return nil
	}
	return nsu.processNodeVolumes(logger, nodeName, attachedVolumes)
}

// processNodeVolumes 处理单个节点的卷状态上报。
// 节点不存在时静默跳过，避免节点创建前无意义地反复重试；其他错误则重新标记待更新标志。
func (nsu *nodeStatusUpdater) processNodeVolumes(logger klog.Logger, nodeName types.NodeName, attachedVolumes []v1.AttachedVolume) error {
	nodeObj, err := nsu.nodeLister.Get(string(nodeName))
	if errors.IsNotFound(err) {
		// If node does not exist, its status cannot be updated.
		// Do nothing so that there is no retry until node is created.
		logger.V(2).Info(
			"Could not update node status. Failed to find node in NodeInformer cache", "node", klog.KRef("", string(nodeName)), "err", err)
		return nil
	} else if err != nil {
		// For all other errors, log error and reset flag statusUpdateNeeded
		// back to true to indicate this node status needs to be updated again.
		logger.V(2).Info("Error retrieving nodes from node lister", "err", err)
		nsu.actualStateOfWorld.SetNodeStatusUpdateNeeded(logger, nodeName)
		return err
	}

	err = nsu.updateNodeStatus(logger, nodeName, nodeObj, attachedVolumes)
	if errors.IsNotFound(err) {
		// If node does not exist, its status cannot be updated.
		// Do nothing so that there is no retry until node is created.
		logger.V(2).Info(
			"Could not update node status, node does not exist - skipping", "node", klog.KObj(nodeObj))
		return nil
	} else if err != nil {
		// If update node status fails, reset flag statusUpdateNeeded back to true
		// to indicate this node status needs to be updated again
		nsu.actualStateOfWorld.SetNodeStatusUpdateNeeded(logger, nodeName)

		logger.V(2).Info("Could not update node status; re-marking for update", "node", klog.KObj(nodeObj), "err", err)

		return err
	}
	return nil
}

// updateNodeStatus 通过 Patch 更新 Node 的 VolumesAttached 字段。
// 使用 DeepCopy 避免修改 informer 缓存中的对象；Patch 而非 Update 可减少冲突。
func (nsu *nodeStatusUpdater) updateNodeStatus(logger klog.Logger, nodeName types.NodeName, nodeObj *v1.Node, attachedVolumes []v1.AttachedVolume) error {
	node := nodeObj.DeepCopy()
	node.Status.VolumesAttached = attachedVolumes
	_, patchBytes, err := nodeutil.PatchNodeStatus(nsu.kubeClient.CoreV1(), nodeName, nodeObj, node)
	if err != nil {
		return err
	}

	logger.V(4).Info("Updating status for node succeeded", "node", klog.KObj(node), "patchBytes", patchBytes, "attachedVolumes", attachedVolumes)
	return nil
}
