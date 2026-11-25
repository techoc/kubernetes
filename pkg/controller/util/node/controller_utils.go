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

package node

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"

	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	utilpod "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/kubelet/util/format"
	nodepkg "k8s.io/kubernetes/pkg/util/node"

	"k8s.io/klog/v2"
)

// DeletePods 从 master 删除运行在给定节点上的所有 Pod，
// 如果有 Pod 被删除，或发现 Pod 处于待删除状态，则返回 true。
func DeletePods(ctx context.Context, kubeClient clientset.Interface, pods []*v1.Pod, recorder record.EventRecorder, nodeName, nodeUID string, daemonStore appsv1listers.DaemonSetLister) (bool, error) {
	// remaining 表示节点上仍有 Pod 存活（正在删除或待删除），调用方据此决定是否需要重试。
	remaining := false
	var updateErrList []error
	logger := klog.FromContext(ctx)

	if len(pods) > 0 {
		RecordNodeEvent(ctx, recorder, nodeName, nodeUID, v1.EventTypeNormal, "DeletingAllPods", fmt.Sprintf("Deleting all Pods from Node %v.", nodeName))
	}

	for i := range pods {
		// 防御性检查：调用方理论上只传入本节点的 Pod，但测试中可能构造出不一致的数据。
		if pods[i].Spec.NodeName != nodeName {
			continue
		}

		// 必须深拷贝：lister 返回的是共享缓存对象，直接修改会污染 informer 缓存。
		pod := pods[i].DeepCopy()
		// 在 Pod 对象上设置终止原因和消息。
		if _, err := SetPodTerminationReason(ctx, kubeClient, pod, nodeName); err != nil {
			if apierrors.IsConflict(err) {
				// 冲突说明状态被并发更新，跳过该 Pod 以免覆盖他人修改，错误稍后统一聚合返回。
				updateErrList = append(updateErrList,
					fmt.Errorf("update status failed for pod %q: %v", format.Pod(pod), err))
				continue
			}
		}
		// 即使 Pod 已被标记为删除，仍视为“仍有 Pod”，因为删除尚未完成。
		if pod.DeletionGracePeriodSeconds != nil {
			remaining = true
			continue
		}
		// 忽略 DaemonSet 管理的 Pod：它们由 DaemonSet 控制器负责，不应由节点控制器删除。
		if _, err := daemonStore.GetPodDaemonSets(pod); err == nil {
			// 无错误表示至少找到一个 DaemonSet。
			continue
		}

		logger.V(2).Info("Starting deletion of pod", "pod", klog.KObj(pod))
		recorder.Eventf(pod, v1.EventTypeNormal, "NodeControllerEviction", "Marking for deletion Pod %s from Node %s", pod.Name, nodeName)
		if err := kubeClient.CoreV1().Pods(pod.Namespace).Delete(ctx, pod.Name, metav1.DeleteOptions{}); err != nil {
			if apierrors.IsNotFound(err) {
				// NotFound 说明 Pod 已被删除，视为成功，无需处理。
				continue
			}
			return false, err
		}
		remaining = true
	}

	// 汇总状态更新冲突：只要存在冲突就返回失败，由调用方重试。
	if len(updateErrList) > 0 {
		return false, utilerrors.NewAggregate(updateErrList)
	}
	return remaining, nil
}

// SetPodTerminationReason 尝试在 Pod 状态中设置终止原因和消息，
// 将其更新到 apiserver，并在遇到错误时返回该错误。
func SetPodTerminationReason(ctx context.Context, kubeClient clientset.Interface, pod *v1.Pod, nodeName string) (*v1.Pod, error) {
	// 已经是节点不可达原因则无需重复更新，保证幂等。
	if pod.Status.Reason == nodepkg.NodeUnreachablePodReason {
		return pod, nil
	}

	pod.Status.Reason = nodepkg.NodeUnreachablePodReason
	pod.Status.Message = fmt.Sprintf(nodepkg.NodeUnreachablePodMessage, nodeName, pod.Name)

	var updatedPod *v1.Pod
	var err error
	if updatedPod, err = kubeClient.CoreV1().Pods(pod.Namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
		return nil, err
	}
	return updatedPod, nil
}

// MarkPodsNotReady 从 master 将给定节点上 Pod 的就绪状态更新为 false，
// 全部成功时返回 nil，失败时返回聚合错误。
func MarkPodsNotReady(ctx context.Context, kubeClient clientset.Interface, recorder record.EventRecorder, pods []*v1.Pod, nodeName string) error {
	logger := klog.FromContext(ctx)
	logger.V(2).Info("Update ready status of pods on node", "node", klog.KRef("", nodeName))

	// 聚合所有 Pod 的更新错误，避免首个错误就中断其余 Pod 的处理。
	errs := []error{}
	for i := range pods {
		// 防御性检查：调用方理论上只传入本节点的 Pod，但测试中可能构造出不一致的数据。
		if pods[i].Spec.NodeName != nodeName {
			continue
		}

		// 必须深拷贝：lister 返回的是共享缓存对象，直接修改会污染 informer 缓存。
		pod := pods[i].DeepCopy()
		for _, cond := range pod.Status.Conditions {
			if cond.Type != v1.PodReady {
				continue
			}

			cond.Status = v1.ConditionFalse
			if !utilpod.UpdatePodCondition(&pod.Status, &cond) {
				// 条件已是目标状态，无需提交无意义的更新。
				break
			}

			logger.V(2).Info("Updating ready status of pod to false", "pod", klog.KObj(pod))
			if _, err := kubeClient.CoreV1().Pods(pod.Namespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{}); err != nil {
				if apierrors.IsNotFound(err) {
					// NotFound 说明 Pod 已被删除，忽略即可。
					continue
				}
				logger.Info("Failed to update status for pod", "pod", klog.KObj(pod), "err", err)
				errs = append(errs, err)
			}
			// 在 UpdateStatus 成功后再记录 NodeNotReady 事件，确保 Pod 仍然存在。
			recorder.Event(pod, v1.EventTypeWarning, "NodeNotReady", "Node is not ready")
			break
		}
	}

	return utilerrors.NewAggregate(errs)
}

// RecordNodeEvent 记录与节点相关的事件。
func RecordNodeEvent(ctx context.Context, recorder record.EventRecorder, nodeName, nodeUID, eventtype, reason, event string) {
	logger := klog.FromContext(ctx)
	// 事件需要 ObjectReference；节点不属于任何命名空间，故 Namespace 留空。
	ref := &v1.ObjectReference{
		APIVersion: "v1",
		Kind:       "Node",
		Name:       nodeName,
		UID:        types.UID(nodeUID),
		Namespace:  "",
	}
	logger.V(2).Info("Recording event message for node", "event", event, "node", klog.KRef("", nodeName))
	recorder.Eventf(ref, eventtype, reason, "Node %s event: %s", nodeName, event)
}

// RecordNodeStatusChange 记录与节点状态变更相关的事件（lifecycle 与 ipam 共用）。
func RecordNodeStatusChange(logger klog.Logger, recorder record.EventRecorder, node *v1.Node, newStatus string) {
	ref := &v1.ObjectReference{
		APIVersion: "v1",
		Kind:       "Node",
		Name:       node.Name,
		UID:        node.UID,
		Namespace:  "",
	}
	logger.V(2).Info("Recording status change event message for node", "status", newStatus, "node", node.Name)
	// TODO: 这里需要事务：节点状态更新与事件记录必须同时成功或同时不生效，参见 issue #6055。
	recorder.Eventf(ref, v1.EventTypeNormal, newStatus, "Node %s status is now: %s", node.Name, newStatus)
}

// SwapNodeControllerTaint 成功时返回 true，否则返回 false。
func SwapNodeControllerTaint(ctx context.Context, kubeClient clientset.Interface, taintsToAdd, taintsToRemove []*v1.Taint, node *v1.Node) bool {
	logger := klog.FromContext(ctx)
	for _, taintToAdd := range taintsToAdd {
		now := metav1.Now()
		// 为待添加污点记录时间，NoExecute 驱逐逻辑据此计算容忍时长。
		taintToAdd.TimeAdded = &now
	}

	err := controller.AddOrUpdateTaintOnNode(ctx, kubeClient, node.Name, taintsToAdd...)
	if err != nil {
		// 添加失败立即返回，不再删除旧污点，避免状态出现半更新。
		utilruntime.HandleError(
			fmt.Errorf(
				"unable to taint %+v unresponsive Node %q: %v",
				taintsToAdd,
				node.Name,
				err))
		return false
	}
	logger.V(4).Info("Added taint to node", "taint", taintsToAdd, "node", klog.KRef("", node.Name))

	// 先添加新污点再移除旧污点，保证节点不会处于两者都缺失的中间态。
	err = controller.RemoveTaintOffNode(ctx, kubeClient, node.Name, node, taintsToRemove...)
	if err != nil {
		utilruntime.HandleError(
			fmt.Errorf(
				"unable to remove %+v unneeded taint from unresponsive Node %q: %v",
				taintsToRemove,
				node.Name,
				err))
		return false
	}
	logger.V(4).Info("Made sure that node has no taint", "node", klog.KRef("", node.Name), "taint", taintsToRemove)

	return true
}

// AddOrUpdateLabelsOnNode 更新节点上的标签，成功返回 true，失败返回 false。
func AddOrUpdateLabelsOnNode(ctx context.Context, kubeClient clientset.Interface, labelsToUpdate map[string]string, node *v1.Node) bool {
	logger := klog.FromContext(ctx)
	if err := controller.AddOrUpdateLabelsOnNode(kubeClient, node.Name, labelsToUpdate); err != nil {
		utilruntime.HandleError(
			fmt.Errorf(
				"unable to update labels %+v for Node %q: %v",
				labelsToUpdate,
				node.Name,
				err))
		return false
	}
	logger.V(4).Info("Updated labels to node", "label", labelsToUpdate, "node", klog.KRef("", node.Name))
	return true
}

// CreateAddNodeHandler 创建节点添加事件的处理函数。
func CreateAddNodeHandler(f func(node *v1.Node) error) func(obj interface{}) {
	return func(originalObj interface{}) {
		// informer 中的对象是共享缓存，回调拿到深拷贝才不会污染缓存。
		node := originalObj.(*v1.Node).DeepCopy()
		if err := f(node); err != nil {
			utilruntime.HandleError(fmt.Errorf("Error while processing Node Add: %v", err))
		}
	}
}

// CreateUpdateNodeHandler 创建节点更新事件的处理函数（lifecycle 与 ipam 共用）。
func CreateUpdateNodeHandler(f func(oldNode, newNode *v1.Node) error) func(oldObj, newObj interface{}) {
	return func(origOldObj, origNewObj interface{}) {
		// 新旧对象都来自共享缓存，回调不应持有或改动它们。
		node := origNewObj.(*v1.Node).DeepCopy()
		prevNode := origOldObj.(*v1.Node).DeepCopy()

		if err := f(prevNode, node); err != nil {
			utilruntime.HandleError(fmt.Errorf("Error while processing Node Add/Delete: %v", err))
		}
	}
}

// CreateDeleteNodeHandler 创建节点删除事件的处理函数（lifecycle 与 ipam 共用）。
func CreateDeleteNodeHandler(logger klog.Logger, f func(node *v1.Node) error) func(obj interface{}) {
	return func(originalObj interface{}) {
		originalNode, isNode := originalObj.(*v1.Node)
		// 删除事件可能丢失，informer 此时投递 tombstone（DeletedFinalStateUnknown），
		// 必须从中取出原对象，否则会漏处理删除。见 #34692。
		if !isNode {
			deletedState, ok := originalObj.(cache.DeletedFinalStateUnknown)
			if !ok {
				logger.Error(nil, "Received unexpected object", "object", originalObj)
				return
			}
			originalNode, ok = deletedState.Obj.(*v1.Node)
			if !ok {
				logger.Error(nil, "DeletedFinalStateUnknown contained non-Node object", "object", deletedState.Obj)
				return
			}
		}
		node := originalNode.DeepCopy()
		if err := f(node); err != nil {
			utilruntime.HandleError(fmt.Errorf("Error while processing Node Add/Delete: %v", err))
		}
	}
}

// GetNodeCondition 从给定状态中提取并返回指定的条件。
// 条件不存在时返回 nil 和 -1，存在时返回条件及其索引。
func GetNodeCondition(status *v1.NodeStatus, conditionType v1.NodeConditionType) (int, *v1.NodeCondition) {
	if status == nil {
		return -1, nil
	}
	for i := range status.Conditions {
		if status.Conditions[i].Type == conditionType {
			return i, &status.Conditions[i]
		}
	}
	return -1, nil
}
