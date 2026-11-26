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

// Package nodelifecycle 实现 cloud-node-lifecycle-controller：周期性比对 Kubernetes
// 节点与云侧实例，删除云侧已不存在的节点，并给已关机实例加上 shutdown 污点。
package nodelifecycle

import (
	"context"
	"errors"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	v1lister "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	cloudprovider "k8s.io/cloud-provider"
	cloudproviderapi "k8s.io/cloud-provider/api"
	cloudnodeutil "k8s.io/cloud-provider/node/helpers"
	controllersmetrics "k8s.io/component-base/metrics/prometheus/controllers"
	nodeutil "k8s.io/component-helpers/node/util"
	"k8s.io/klog/v2"
)

const (
	// 事件 reason 名称是外部可观测契约的一部分，不要重命名，否则会破坏现有告警与审计。
	deleteNodeEvent       = "DeletingNode"
	deleteNodeFailedEvent = "DeletingNodeFailed"

	// 指标标签值，用于区分云 API 调用类型；保持稳定以便 Prometheus 查询。
	instanceExistsOp   = "instance_exists"
	instanceShutdownOp = "instance_shutdown"
	instanceMetadataOp = "instance_metadata"
)

// ShutdownTaint 是加在已关机实例对应 Node 上的污点。
// 使用 NoSchedule 而非 NoExecute，避免驱逐已有 Pod——实例可能只是短暂
// 关机，保留工作负载有利于恢复后快速重新调度。
var ShutdownTaint = &v1.Taint{
	Key:    cloudproviderapi.TaintNodeShutdown,
	Effect: v1.TaintEffectNoSchedule,
}

// CloudNodeLifecycleController 负责删除或更新那些在云侧已被删除/关机的 Kubernetes 节点。
//
// 仅处理 NotReady 的节点。Ready 节点只确保移除 shutdown 污点，
// 从而把云 API 调用集中在真正需要判断的节点上。
type CloudNodeLifecycleController struct {
	kubeClient clientset.Interface
	nodeLister v1lister.NodeLister

	broadcaster record.EventBroadcaster
	recorder    record.EventRecorder

	cloud cloudprovider.Interface

	// 控制 NodeController 的检查周期，即 NodeController 多久检查一次 kubelet 上报的
	// 节点状态。
	//
	// 必须小于 controller-manager 的 nodeMonitorGracePeriod，否则节点可能在
	// 被判定为 NotReady 之前就发生了云侧状态变化，导致清理延迟。
	nodeMonitorPeriod time.Duration

	// 控制 NodeController 监控循环的 worker 数。
	concurrentNodeLifecycleSyncs int
}

// NewCloudNodeLifecycleController 构造控制器并注册指标。
//
// 构造期校验依赖与参数，把配置错误暴露在启动阶段而不是运行期；
// 云厂商必须实现 Instances 或 InstancesV2，否则无法判断实例是否存在/关机。
func NewCloudNodeLifecycleController(
	nodeInformer coreinformers.NodeInformer,
	kubeClient clientset.Interface,
	cloud cloudprovider.Interface,
	nodeMonitorPeriod time.Duration,
	concurrentNodeLifecycleSyncs int) (*CloudNodeLifecycleController, error) {

	registerMetrics()

	if kubeClient == nil {
		return nil, errors.New("kubernetes client is nil")
	}

	if cloud == nil {
		return nil, errors.New("no cloud provider provided")
	}

	_, instancesSupported := cloud.Instances()
	_, instancesV2Supported := cloud.InstancesV2()
	if !instancesSupported && !instancesV2Supported {
		return nil, errors.New("cloud provider does not support instances")
	}

	// 并发度为 0 会让 ParallelizeUntil 不执行任何节点巡检，必须显式拒绝。
	if concurrentNodeLifecycleSyncs < 1 {
		return nil, fmt.Errorf("concurrentNodeLifecycleSyncs must be >= 1, got %d", concurrentNodeLifecycleSyncs)
	}

	c := &CloudNodeLifecycleController{
		kubeClient:                   kubeClient,
		nodeLister:                   nodeInformer.Lister(),
		cloud:                        cloud,
		nodeMonitorPeriod:            nodeMonitorPeriod,
		concurrentNodeLifecycleSyncs: concurrentNodeLifecycleSyncs,
	}

	return c, nil
}

// Run 启动控制器的主循环，调用会阻塞，应通过 goroutine 启动。
func (c *CloudNodeLifecycleController) Run(ctx context.Context, controllerManagerMetrics *controllersmetrics.ControllerManagerMetrics) {
	c.broadcaster = record.NewBroadcaster(record.WithContext(ctx))
	c.recorder = c.broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "cloud-node-lifecycle-controller"})

	defer utilruntime.HandleCrash()
	controllerManagerMetrics.ControllerStarted("cloud-node-lifecycle")
	defer controllerManagerMetrics.ControllerStopped("cloud-node-lifecycle")

	// 启动事件处理管道。
	klog.Info("Sending events to api server")
	c.broadcaster.StartStructuredLogging(0)
	c.broadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: c.kubeClient.CoreV1().Events("")})
	defer c.broadcaster.Shutdown()

	// 下面的循环与 APIServer 通信，单轮最坏复杂度为 O(num_nodes)。由于相关事件发生得
	// 非常稀疏，这个开销可以接受。
	//
	// 这是全量扫描而非事件驱动——Node 删除、实例关机这类事件本身很稀疏，
	// 全量比对比维护增量状态更简单可靠；但不要在此加入高频操作。

	// 启动周期性检查，发现云 provider 侧已删除或关机的节点。
	wait.UntilWithContext(ctx, c.MonitorNodes, c.nodeMonitorPeriod)
}

// MonitorNodes 检查集群中的节点是否已被删除或关机：删除则移除 Node 对象，关机则打上关机污点。
func (c *CloudNodeLifecycleController) MonitorNodes(ctx context.Context) {
	startTime := time.Now()

	nodes, err := c.nodeLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("error listing nodes from cache: %s", err)
		return
	}

	processNode := func(piece int) {
		node := nodes[piece].DeepCopy()
		// 缺少 Ready condition 时默认按 v1.ConditionUnknown 处理，保守地走云侧检查流程，
		// 避免节点状态缺失导致实例泄漏。
		status := v1.ConditionUnknown
		if _, c := nodeutil.GetNodeCondition(&node.Status, v1.NodeReady); c != nil {
			status = c.Status
		}

		if status == v1.ConditionTrue {
			// 节点恢复后若仍带有 shutdown 污点则移除；即使污点不存在，
			// RemoveTaintOffNode 也会安全地 no-op。
			if err := cloudnodeutil.RemoveTaintOffNode(c.kubeClient, node.Name, node, ShutdownTaint); err != nil {
				klog.Errorf("error patching node taints: %v", err)
			}
			return
		}

		// 此时节点已是 NotReady，需确认它在云 provider 侧是否已不存在，不存在则删除 Node。
		exists, err := c.ensureNodeExistsByProviderID(ctx, node)
		if err != nil {
			// 云 API 失败时保持现状：无法确认实例不存在，就不能删除 Node。
			klog.Errorf("error checking if node %s exists: %v", node.Name, err)
			return
		}

		if !exists {
			// 实例已不存在，污点如何已无关紧要，直接删除节点。

			klog.V(2).Infof("deleting node since it is no longer present in cloud provider: %s", node.Name)

			ref := &v1.ObjectReference{
				APIVersion: "v1",
				Kind:       "Node",
				Name:       node.Name,
				UID:        node.UID,
				Namespace:  "",
			}

			c.recorder.Eventf(ref, v1.EventTypeNormal, deleteNodeEvent,
				"Deleting node %s because it does not exist in the cloud provider", node.Name)

			if err := c.kubeClient.CoreV1().Nodes().Delete(ctx, node.Name, metav1.DeleteOptions{}); err != nil {
				klog.Errorf("unable to delete node %q: %v", node.Name, err)
				c.recorder.Eventf(ref, v1.EventTypeWarning, deleteNodeFailedEvent,
					"Failed deleting node %s: %v", node.Name, err)
			}
		} else {
			// 节点仍然存在，需要显式检查实例状态，好让各云厂商的污点行为保持一致：
			// 当前问题是关机节点的处理不统一，并非所有云厂商都会在实例关机时从集群
			// 中删除节点（见 #46442）。部分云厂商关机后仍保留实例，因此不能依赖
			// “实例不存在”来推断关机；只有显式报告 shutdown 时才打污点。
			shutdown, err := c.shutdownInCloudProvider(ctx, node)
			if err != nil {
				klog.Errorf("error checking if node %s is shutdown: %v", node.Name, err)
				return
			}

			if shutdown {
				// 实例已关机，打上关机污点。
				if err := cloudnodeutil.AddOrUpdateTaintOnNode(c.kubeClient, node.Name, ShutdownTaint); err != nil {
					// 节点可能已被其他流程删除，属于可接受的竞态。
					klog.Errorf("failed to apply shutdown taint to node %s, it may have been deleted.", node.Name)
				}
			}
		}
	}

	workqueue.ParallelizeUntil(ctx, c.concurrentNodeLifecycleSyncs, len(nodes), processNode)

	duration := time.Since(startTime).Seconds()
	monitorNodesDuration.Observe(duration)
}

// getProviderID 返回节点的 provider ID：Node 对象上没有时，向云厂商查询。
//
// 优先使用 Node 上已固化的 ProviderID，避免不必要的云 API 调用；
// 只有未初始化节点才回查云厂商。
func (c *CloudNodeLifecycleController) getProviderID(ctx context.Context, node *v1.Node) (string, error) {
	if node.Spec.ProviderID != "" {
		return node.Spec.ProviderID, nil
	}

	if instanceV2, ok := c.cloud.InstancesV2(); ok {
		metadata, err := instanceV2.InstanceMetadata(ctx, node)
		observeInstanceOp(instanceMetadataOp, err)
		if err != nil {
			return "", err
		}
		return metadata.ProviderID, nil
	}

	providerID, err := cloudprovider.GetInstanceProviderID(ctx, c.cloud, types.NodeName(node.Name))
	observeInstanceOp(instanceMetadataOp, err)
	if err != nil {
		return "", err
	}
	return providerID, nil
}

// shutdownInCloudProvider 在节点于云提供商侧处于关机状态时返回 true。
//
// InstancesV2 直接提供 InstanceShutdown；旧版 Instances 接口通过
// providerID 查询，NotImplemented 视为“不支持关机检测”，按未关机处理以免误删。
func (c *CloudNodeLifecycleController) shutdownInCloudProvider(ctx context.Context, node *v1.Node) (bool, error) {
	if instanceV2, ok := c.cloud.InstancesV2(); ok {
		shutdown, err := instanceV2.InstanceShutdown(ctx, node)
		observeInstanceOp(instanceShutdownOp, err)
		return shutdown, err
	}

	instances, ok := c.cloud.Instances()
	if !ok {
		return false, errors.New("cloud provider does not support instances")
	}

	providerID, err := c.getProviderID(ctx, node)
	if err != nil {
		// 实例已不存在时无需再判断关机，交给删除路径处理。
		if errors.Is(err, cloudprovider.InstanceNotFound) {
			return false, nil
		}
		return false, err
	}

	shutdown, err := instances.InstanceShutdownByProviderID(ctx, providerID)
	observeInstanceOp(instanceShutdownOp, err)
	if errors.Is(err, cloudprovider.NotImplemented) {
		return false, nil
	}

	return shutdown, err
}

// ensureNodeExistsByProviderID 按 providerID 检查实例是否仍然存在。
func (c *CloudNodeLifecycleController) ensureNodeExistsByProviderID(ctx context.Context, node *v1.Node) (bool, error) {
	if instanceV2, ok := c.cloud.InstancesV2(); ok {
		exists, err := instanceV2.InstanceExists(ctx, node)
		if err == nil && !exists {
			// 把“不存在”也记为一次实例查询结果，便于在指标上与云 API 错误区分。
			observeInstanceOp(instanceExistsOp, cloudprovider.InstanceNotFound)
		} else {
			observeInstanceOp(instanceExistsOp, err)
		}
		return exists, err
	}

	instances, ok := c.cloud.Instances()
	if !ok {
		return false, errors.New("instances interface not supported in the cloud provider")
	}

	providerID, err := c.getProviderID(ctx, node)
	if err != nil {
		// InstanceNotFound 说明实例确实不存在，返回 false 让调用方删除 Node。
		if errors.Is(err, cloudprovider.InstanceNotFound) {
			return false, nil
		}
		return false, err
	}

	exists, err := instances.InstanceExistsByProviderID(ctx, providerID)
	if err == nil && !exists {
		observeInstanceOp(instanceExistsOp, cloudprovider.InstanceNotFound)
	} else {
		observeInstanceOp(instanceExistsOp, err)
	}
	return exists, err
}

// observeInstanceOp 按错误类型给云 API 调用结果打标签并计数。
// 把 NotImplemented / InstanceNotFound / 取消 / 其他错误分开统计，
// 便于判断故障是云厂商能力缺失还是临时性故障。
func observeInstanceOp(operation string, err error) {
	result := "success"
	switch {
	case errors.Is(err, cloudprovider.NotImplemented):
		result = "not_implemented"
	case errors.Is(err, cloudprovider.InstanceNotFound):
		result = "instance_not_found"
	case errors.Is(err, context.Canceled):
		result = "canceled"
	case err != nil:
		result = "error"
	}
	cloudProviderCalls.WithLabelValues(operation, result).Inc()
}
