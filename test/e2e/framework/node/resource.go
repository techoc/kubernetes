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

package node

import (
	"context"
	"encoding/json"
	"fmt"
	"k8s.io/klog/v2"
	"net"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	clientretry "k8s.io/client-go/util/retry"
	resourceapi "k8s.io/kubernetes/pkg/api/v1/resource"
	"k8s.io/kubernetes/test/e2e/framework"
	netutil "k8s.io/utils/net"
)

const (
	// poll 是轮询 pod、node 和 claim 的频率。
	poll = 2 * time.Second

	// singleCallTimeout 是单次 API 调用（如 'get' 或 'list'）的尝试时长，用于防止
	// 瞬时故障导致测试失败。
	singleCallTimeout = 5 * time.Minute

	// ssh 端口
	sshPort = "22"
)

var (
	// unreachableTaintTemplate 是 node 变为不可达时的 taint。
	// 从 pkg/controller/nodelifecycle 复制，避免引入额外依赖
	unreachableTaintTemplate = &v1.Taint{
		Key:    v1.TaintNodeUnreachable,
		Effect: v1.TaintEffectNoExecute,
	}

	// notReadyTaintTemplate 是 node 无法运行 pod 时的 taint。
	// 从 pkg/controller/nodelifecycle 复制，避免引入额外依赖
	notReadyTaintTemplate = &v1.Taint{
		Key:    v1.TaintNodeNotReady,
		Effect: v1.TaintEffectNoExecute,
	}

	// updateTaintBackOff 包含最大重试次数和两次重试之间的等待间隔。
	updateTaintBackOff = wait.Backoff{
		Steps:    5,
		Duration: 100 * time.Millisecond,
		Jitter:   1.0,
	}
)

// PodNode 是 pod-node 对，表示给定 pod 运行在哪个节点上
type PodNode struct {
	// Pod 表示 pod 名称
	Pod string
	// Node 表示 node 名称
	Node string
}

func isNodeConditionSetAsExpected(node *v1.Node, conditionType v1.NodeConditionType, wantTrue, silent bool) bool {
	// 检查 node 的 ready condition（记录所有信息）。
	for _, cond := range node.Status.Conditions {
		// 确保 condition 的 type 和 status 与预期一致。
		if cond.Type == conditionType {
			// 对于 NodeReady condition，还需要检查 Taints
			if cond.Type == v1.NodeReady {
				hasNodeControllerTaints := false
				// 对于 NodeReady，还需要检查 Taints 是否已消失
				taints := node.Spec.Taints
				for _, taint := range taints {
					if taint.MatchTaint(unreachableTaintTemplate) || taint.MatchTaint(notReadyTaintTemplate) {
						hasNodeControllerTaints = true
						break
					}
				}
				if wantTrue {
					if (cond.Status == v1.ConditionTrue) && !hasNodeControllerTaints {
						return true
					}
					msg := ""
					if !hasNodeControllerTaints {
						msg = fmt.Sprintf("Condition %s of node %s is %v instead of %t. Reason: %v, message: %v",
							conditionType, node.Name, cond.Status == v1.ConditionTrue, wantTrue, cond.Reason, cond.Message)
					} else {
						msg = fmt.Sprintf("Condition %s of node %s is %v, but Node is tainted by NodeController with %v. Failure",
							conditionType, node.Name, cond.Status == v1.ConditionTrue, taints)
					}
					if !silent {
						framework.Logf("%s", msg)
					}
					return false
				}
				// TODO: NC 的 notReady/unreachable taint 默认启用后，检查 node 是否被 taint
				if cond.Status != v1.ConditionTrue {
					return true
				}
				if !silent {
					framework.Logf("Condition %s of node %s is %v instead of %t. Reason: %v, message: %v",
						conditionType, node.Name, cond.Status == v1.ConditionTrue, wantTrue, cond.Reason, cond.Message)
				}
				return false
			}
			if (wantTrue && (cond.Status == v1.ConditionTrue)) || (!wantTrue && (cond.Status != v1.ConditionTrue)) {
				return true
			}
			if !silent {
				framework.Logf("Condition %s of node %s is %v instead of %t. Reason: %v, message: %v",
					conditionType, node.Name, cond.Status == v1.ConditionTrue, wantTrue, cond.Reason, cond.Message)
			}
			return false
		}

	}
	if !silent {
		framework.Logf("Couldn't find condition %v on node %v", conditionType, node.Name)
	}
	return false
}

// IsConditionSetAsExpected 在 node 匹配 conditionType 时返回 wantTrue，否则返回 wantTrue 的反值，并记录详细信息。
func IsConditionSetAsExpected(node *v1.Node, conditionType v1.NodeConditionType, wantTrue bool) bool {
	return isNodeConditionSetAsExpected(node, conditionType, wantTrue, false)
}

// IsConditionSetAsExpectedSilent 在 node 匹配 conditionType 时返回 wantTrue，否则返回 wantTrue 的反值。
func IsConditionSetAsExpectedSilent(node *v1.Node, conditionType v1.NodeConditionType, wantTrue bool) bool {
	return isNodeConditionSetAsExpected(node, conditionType, wantTrue, true)
}

// isConditionUnset 在给定 node 的 condition 中没有匹配给定 conditionType 时返回 true，否则返回 false。
func isConditionUnset(node *v1.Node, conditionType v1.NodeConditionType) bool {
	for _, cond := range node.Status.Conditions {
		if cond.Type == conditionType {
			return false
		}
	}
	return true
}

// Filter 原地过滤 NodeList 中的 node，移除不满足
// 给定条件的 node
func Filter(nodeList *v1.NodeList, fn func(node v1.Node) bool) {
	var l []v1.Node

	for _, node := range nodeList.Items {
		if fn(node) {
			l = append(l, node)
		}
	}
	nodeList.Items = l
}

// TotalRegistered 返回可调度 Node 的数量。
func TotalRegistered(ctx context.Context, c clientset.Interface) (int, error) {
	nodes, err := waitListSchedulableNodes(ctx, c)
	if err != nil {
		framework.Logf("Failed to list nodes: %v", err)
		return 0, err
	}
	return len(nodes.Items), nil
}

// TotalReady 返回就绪且可调度的 Node 数量。
func TotalReady(ctx context.Context, c clientset.Interface) (int, error) {
	nodes, err := waitListSchedulableNodes(ctx, c)
	if err != nil {
		framework.Logf("Failed to list nodes: %v", err)
		return 0, err
	}

	// 过滤掉未就绪的 node。
	Filter(nodes, func(node v1.Node) bool {
		return IsConditionSetAsExpected(&node, v1.NodeReady, true)
	})
	return len(nodes.Items), nil
}

// GetSSHExternalIP 返回 node 的外部 IP 拼接 ssh 端口 22
// 例如 1.2.3.4:22
func GetSSHExternalIP(node *v1.Node) (string, error) {
	framework.Logf("Getting external IP address for %s", node.Name)

	for _, a := range node.Status.Addresses {
		if a.Type == v1.NodeExternalIP && a.Address != "" {
			return net.JoinHostPort(a.Address, sshPort), nil
		}
	}
	return "", fmt.Errorf("Couldn't get the external IP of host %s with addresses %v", node.Name, node.Status.Addresses)
}

// GetSSHInternalIP 返回 node 的内部 IP 拼接 ssh 端口 22
func GetSSHInternalIP(node *v1.Node) (string, error) {
	for _, address := range node.Status.Addresses {
		if address.Type == v1.NodeInternalIP && address.Address != "" {
			return net.JoinHostPort(address.Address, sshPort), nil
		}
	}

	return "", fmt.Errorf("Couldn't get the internal IP of host %s with addresses %v", node.Name, node.Status.Addresses)
}

// FirstAddressByTypeAndFamily 返回节点列表中匹配给定类型和地址族的第一个地址
func FirstAddressByTypeAndFamily(nodelist *v1.NodeList, addrType v1.NodeAddressType, family v1.IPFamily) string {
	for _, n := range nodelist.Items {
		addresses := GetAddressesByTypeAndFamily(&n, addrType, family)
		if len(addresses) > 0 {
			return addresses[0]
		}
	}
	return ""
}

// GetAddressesByTypeAndFamily 返回给定 node 上指定 addressType 的地址列表
// 并按 IPFamily 过滤
func GetAddressesByTypeAndFamily(node *v1.Node, addressType v1.NodeAddressType, family v1.IPFamily) (ips []string) {
	for _, nodeAddress := range node.Status.Addresses {
		if nodeAddress.Type != addressType {
			continue
		}
		if nodeAddress.Address == "" {
			continue
		}
		if family == v1.IPv6Protocol && netutil.IsIPv6String(nodeAddress.Address) {
			ips = append(ips, nodeAddress.Address)
		}
		if family == v1.IPv4Protocol && !netutil.IsIPv6String(nodeAddress.Address) {
			ips = append(ips, nodeAddress.Address)
		}
	}
	return
}

// GetAddresses 返回给定 node 上指定 addressType 的地址列表
func GetAddresses(node *v1.Node, addressType v1.NodeAddressType) (ips []string) {
	for j := range node.Status.Addresses {
		nodeAddress := &node.Status.Addresses[j]
		if nodeAddress.Type == addressType && nodeAddress.Address != "" {
			ips = append(ips, nodeAddress.Address)
		}
	}
	return
}

// CollectAddresses 返回给定节点列表中指定 addressType 的地址列表
func CollectAddresses(nodes *v1.NodeList, addressType v1.NodeAddressType) []string {
	ips := []string{}
	for i := range nodes.Items {
		ips = append(ips, GetAddresses(&nodes.Items[i], addressType)...)
	}
	return ips
}

// GetPublicIps 返回 node 的公共 IP 列表。
func GetPublicIps(ctx context.Context, c clientset.Interface) ([]string, error) {
	nodes, err := GetReadySchedulableNodes(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("get schedulable and ready nodes error: %w", err)
	}
	ips := CollectAddresses(nodes, v1.NodeExternalIP)
	if len(ips) == 0 {
		// 如果没设置 ExternalIP，假设测试程序能访问 InternalIP
		ips = CollectAddresses(nodes, v1.NodeInternalIP)
	}
	return ips, nil
}

// GetReadySchedulableNodes 处理获取可工作节点的常见场景。
// 1) 必须可调度。
// 2) 必须就绪。
// 只要 1 或 2 任一不满足，多数测试都会完全忽略该节点。
// 如果没有既就绪又可调度的节点，则返回错误。
func GetReadySchedulableNodes(ctx context.Context, c clientset.Interface) (nodes *v1.NodeList, err error) {
	logger := klog.FromContext(ctx)
	nodes, err = checkWaitListSchedulableNodes(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("listing schedulable nodes error: %w", err)
	}
	Filter(nodes, func(node v1.Node) bool {
		return IsNodeSchedulable(logger, &node) && isNodeUntainted(logger, &node)
	})
	if len(nodes.Items) == 0 {
		return nil, fmt.Errorf("there are currently no ready, schedulable nodes in the cluster")
	}
	return nodes, nil
}

// GetBoundedReadySchedulableNodes 与 GetReadySchedulableNodes 类似，但最多返回
// maxNodes 个节点。用它避免测试用例在
// 大型集群上规模失控。
func GetBoundedReadySchedulableNodes(ctx context.Context, c clientset.Interface, maxNodes int) (nodes *v1.NodeList, err error) {
	nodes, err = GetReadySchedulableNodes(ctx, c)
	if err != nil {
		return nil, err
	}
	if len(nodes.Items) > maxNodes {
		shuffled := make([]v1.Node, maxNodes)
		perm := rand.Perm(len(nodes.Items))
		for i, j := range perm {
			if j < len(shuffled) {
				shuffled[j] = nodes.Items[i]
			}
		}
		nodes.Items = shuffled
	}
	return nodes, nil
}

// GetRandomReadySchedulableNode 随机选择一个可用于
// 运行 pod 的节点。没有可用节点时返回错误。
func GetRandomReadySchedulableNode(ctx context.Context, c clientset.Interface) (*v1.Node, error) {
	nodes, err := GetReadySchedulableNodes(ctx, c)
	if err != nil {
		return nil, err
	}
	return &nodes.Items[rand.Intn(len(nodes.Items))], nil
}

// GetReadyNodesIncludingTainted 返回所有就绪节点，包括被 taint 的。
// 有些场景我们关心被 taint 的节点，
// 例如 GPU 节点测试中，即使存在
// nvidia.com/gpu=present:NoSchedule taint 也在意这些节点
func GetReadyNodesIncludingTainted(ctx context.Context, c clientset.Interface) (nodes *v1.NodeList, err error) {
	logger := klog.FromContext(ctx)
	nodes, err = checkWaitListSchedulableNodes(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("listing schedulable nodes error: %w", err)
	}
	Filter(nodes, func(node v1.Node) bool {
		return IsNodeSchedulable(logger, &node)
	})
	return nodes, nil
}

// isNodeUntainted 根据节点当前的 taint 判断假 pod 能否调度到 "node" 上。
// TODO: 需要讨论是否返回 bool 和 error 类型
func isNodeUntainted(logger klog.Logger, node *v1.Node) bool {
	return isNodeUntaintedWithNonblocking(logger, node, "")
}

// isNodeUntaintedWithNonblocking 判断假 pod 能否调度到 "node" 上，
// 但允许存在非阻塞 taint 列表中的 taint。
func isNodeUntaintedWithNonblocking(logger klog.Logger, node *v1.Node, nonblockingTaints string) bool {
	// 基于逗号分隔列表简单查找非阻塞 taint。
	nonblockingTaintsMap := map[string]struct{}{}
	for _, t := range strings.Split(nonblockingTaints, ",") {
		if strings.TrimSpace(t) != "" {
			nonblockingTaintsMap[strings.TrimSpace(t)] = struct{}{}
		}
	}

	n := node
	if len(nonblockingTaintsMap) > 0 {
		nodeCopy := node.DeepCopy()
		nodeCopy.Spec.Taints = []v1.Taint{}
		for _, v := range node.Spec.Taints {
			if _, isNonblockingTaint := nonblockingTaintsMap[v.Key]; !isNonblockingTaint {
				nodeCopy.Spec.Taints = append(nodeCopy.Spec.Taints, v)
			}
		}
		n = nodeCopy
	}

	return toleratesTaintsWithNoScheduleNoExecuteEffects(logger, n.Spec.Taints, nil)
}

func toleratesTaintsWithNoScheduleNoExecuteEffects(logger klog.Logger, taints []v1.Taint, tolerations []v1.Toleration) bool {
	filteredTaints := []v1.Taint{}
	for _, taint := range taints {
		if taint.Effect == v1.TaintEffectNoExecute || taint.Effect == v1.TaintEffectNoSchedule {
			filteredTaints = append(filteredTaints, taint)
		}
	}

	toleratesTaint := func(taint v1.Taint) bool {
		for _, toleration := range tolerations {
			//TaintTolerationComparisonOperators feature gate 在 e2e 中为 false，因为该特性处于 Alpha。
			if toleration.ToleratesTaint(logger, &taint, false) {
				return true
			}
		}

		return false
	}

	for _, taint := range filteredTaints {
		if !toleratesTaint(taint) {
			return false
		}
	}

	return true
}

// IsNodeSchedulable 在以下情况返回 true：
// 1) 没有设置 "unschedulable" 字段
// 2) IsNodeReady 也返回 true
func IsNodeSchedulable(logger klog.Logger, node *v1.Node) bool {
	if node == nil {
		return false
	}

	return !node.Spec.Unschedulable && IsNodeReady(logger, node)
}

// IsNodeReady 在以下情况返回 true：
// 1) Ready condition 设为 true
// 2) NetworkUnavailable condition 未设为 true
func IsNodeReady(logger klog.Logger, node *v1.Node) bool {
	nodeReady := IsConditionSetAsExpected(node, v1.NodeReady, true)
	networkReady := isConditionUnset(node, v1.NodeNetworkUnavailable) ||
		IsConditionSetAsExpectedSilent(node, v1.NodeNetworkUnavailable, false)
	return nodeReady && networkReady
}

// isNodeSchedulableWithoutTaints 在以下情况返回 true：
// 1) 没有设置 "unschedulable" 字段
// 2) IsNodeReady 也返回 true
// 3) isNodeUntainted 也返回 true
func isNodeSchedulableWithoutTaints(logger klog.Logger, node *v1.Node) bool {
	return IsNodeSchedulable(logger, node) && isNodeUntainted(logger, node)
}

// hasNonblockingTaint 在 node 至少包含一个
// key 匹配该正则的 taint 时返回 true。
func hasNonblockingTaint(node *v1.Node, nonblockingTaints string) bool {
	if node == nil {
		return false
	}

	// 基于逗号分隔列表简单查找非阻塞 taint。
	nonblockingTaintsMap := map[string]struct{}{}
	for _, t := range strings.Split(nonblockingTaints, ",") {
		if strings.TrimSpace(t) != "" {
			nonblockingTaintsMap[strings.TrimSpace(t)] = struct{}{}
		}
	}

	for _, taint := range node.Spec.Taints {
		if _, hasNonblockingTaint := nonblockingTaintsMap[taint.Key]; hasNonblockingTaint {
			return true
		}
	}

	return false
}

// GetNodeAllocatableAndAvailableQuantities 在 resourceName 设为 v1.ResourceCPU（或 v1.ResourceMemory）时返回
//   - 第一个返回值，成功时是节点总 "CPU"（或 "Memory"）容量中可供 pod 使用的可分配部分，
//     通过从节点总 "CPU"（或 "Memory"）资源中减去为节点操作系统、Kubelet 和其他
//     系统守护进程预留的资源计算，失败时为空。
//   - 第二个返回值，成功时是节点可分配 "CPU" 容量中的可用部分，通过从
//     可分配 "CPU"（或 "Memory"）资源中减去为节点 pod 预留的资源计算，失败时为空。
//   - 第三个返回值，成功时为 nil，失败时为对应错误。
func GetNodeAllocatableAndAvailableQuantities(ctx context.Context, c clientset.Interface, n *v1.Node, resourceName v1.ResourceName) (resource.Quantity, resource.Quantity, error) {
	var nodeAllocatable, podAllocated, nodeAvailable resource.Quantity
	switch resourceName {
	case v1.ResourceCPU:
		nodeAllocatable = n.Status.Allocatable.Cpu().DeepCopy()
	case v1.ResourceMemory:
		nodeAllocatable = n.Status.Allocatable.Memory().DeepCopy()
	default:
		return resource.Quantity{}, resource.Quantity{}, fmt.Errorf("unexpected resource type %q; expected either 'CPU' or 'Memory'", resourceName)
	}

	if n.Status.Allocatable == nil {
		return resource.Quantity{}, resource.Quantity{}, fmt.Errorf("unexpected empty value of allocatable, while attempting to get node's allocatable %s", resourceName.String())
	}

	// 排除处于 Succeeded 或 Failed 状态的 pod
	selector := fmt.Sprintf("spec.nodeName=%s,status.phase!=%v,status.phase!=%v", n.Name, v1.PodSucceeded, v1.PodFailed)
	listOptions := metav1.ListOptions{FieldSelector: selector}
	podList, err := c.CoreV1().Pods(metav1.NamespaceAll).List(ctx, listOptions)
	if err != nil {
		return resource.Quantity{}, resource.Quantity{}, fmt.Errorf("error getting list of pods: %w, while attempting to get node's allocatable %s", err, resourceName.String())
	}

	for _, pod := range podList.Items {
		podRequest := resourceapi.GetResourceRequestQuantity(&pod, resourceName)
		podAllocated.Add(podRequest)
	}
	nodeAvailable = nodeAllocatable.DeepCopy()
	nodeAvailable.Sub(podAllocated)
	if nodeAvailable.Sign() < 0 {
		return resource.Quantity{}, resource.Quantity{}, fmt.Errorf("unexpected negative value of nodeAvailable %s, while attempting to get node's allocatable and available %s", nodeAvailable.String(), resourceName.String())
	}

	return nodeAllocatable, nodeAvailable, nil
}

// GetNodeHeartbeatTime 返回节点最后一次状态更新的时间戳。
func GetNodeHeartbeatTime(node *v1.Node) metav1.Time {
	for _, condition := range node.Status.Conditions {
		if condition.Type == v1.NodeReady {
			return condition.LastHeartbeatTime
		}
	}
	return metav1.Time{}
}

// PodNodePairs 返回 namespace 中所有 pod 的 podNode 对
func PodNodePairs(ctx context.Context, c clientset.Interface, ns string) ([]PodNode, error) {
	var result []PodNode

	podList, err := c.CoreV1().Pods(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return result, err
	}

	for _, pod := range podList.Items {
		result = append(result, PodNode{
			Pod:  pod.Name,
			Node: pod.Spec.NodeName,
		})
	}

	return result, nil
}

// GetClusterZones 返回从所有节点收集的 zone label 值。
func GetClusterZones(ctx context.Context, c clientset.Interface) (sets.String, error) {
	nodes, err := c.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("error getting nodes while attempting to list cluster zones: %w", err)
	}

	// 从所有节点收集 zone label 的值
	zones := sets.NewString()
	for _, node := range nodes.Items {
		if zone, found := node.Labels[v1.LabelFailureDomainBetaZone]; found {
			zones.Insert(zone)
		}

		if zone, found := node.Labels[v1.LabelTopologyZone]; found {
			zones.Insert(zone)
		}
	}
	return zones, nil
}

// GetSchedulableClusterZones 返回从所有可调度节点收集的 zone label 值。
func GetSchedulableClusterZones(ctx context.Context, c clientset.Interface) (sets.Set[string], error) {
	// GetReadySchedulableNodes 已经过滤掉了有 taint 和不可调度的节点。
	nodes, err := GetReadySchedulableNodes(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("error getting nodes while attempting to list cluster zones: %w", err)
	}

	// 从所有节点收集 zone label 的值
	zones := sets.New[string]()
	for _, node := range nodes.Items {
		if zone, found := node.Labels[v1.LabelFailureDomainBetaZone]; found {
			zones.Insert(zone)
		}

		if zone, found := node.Labels[v1.LabelTopologyZone]; found {
			zones.Insert(zone)
		}
	}
	return zones, nil
}

// CreatePodsPerNodeForSimpleApp 创建带标签的 pod。适用于不需要网络、批量创建 pod 的测试。
func CreatePodsPerNodeForSimpleApp(ctx context.Context, c clientset.Interface, namespace, appName string, podSpec func(n v1.Node) v1.PodSpec, maxCount int) map[string]string {
	nodes, err := GetBoundedReadySchedulableNodes(ctx, c, maxCount)
	// TODO 在移除 core e2e 对 node 的依赖后使用 expect.go 中的包装方法
	gomega.ExpectWithOffset(2, err).NotTo(gomega.HaveOccurred())
	podLabels := map[string]string{
		"app": appName + "-pod",
	}
	for i, node := range nodes.Items {
		framework.Logf("%v/%v : Creating container with label app=%v-pod", i, maxCount, appName)
		_, err := c.CoreV1().Pods(namespace).Create(ctx, &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:   fmt.Sprintf(appName+"-pod-%v", i),
				Labels: podLabels,
			},
			Spec: podSpec(node),
		}, metav1.CreateOptions{})
		// TODO 在移除 core e2e 对 node 的依赖后使用 expect.go 中的包装方法
		gomega.ExpectWithOffset(2, err).NotTo(gomega.HaveOccurred())
	}
	return podLabels
}

// RemoveTaintsOffNode 从给定节点移除一组 taint
// 它只是 RemoveTaintOffNode 的简单包装
func RemoveTaintsOffNode(ctx context.Context, c clientset.Interface, nodeName string, taints []v1.Taint) {
	for _, taint := range taints {
		RemoveTaintOffNode(ctx, c, nodeName, taint)
	}
}

// RemoveTaintOffNode 从给定节点移除给定 taint。
func RemoveTaintOffNode(ctx context.Context, c clientset.Interface, nodeName string, taint v1.Taint) {
	err := removeNodeTaint(ctx, c, nodeName, nil, &taint)

	// TODO 在移除 core e2e 对 node 的依赖后使用 expect.go 中的包装方法
	gomega.ExpectWithOffset(2, err).NotTo(gomega.HaveOccurred())
	verifyThatTaintIsGone(ctx, c, nodeName, &taint)
}

// AddOrUpdateTaintOnNode 向给定节点添加给定 taint 或更新 taint。
func AddOrUpdateTaintOnNode(ctx context.Context, c clientset.Interface, nodeName string, taint v1.Taint) {
	// TODO 在 core e2e framework 中移除对本包的依赖后，
	// 使用 expect.go 中的包装方法。
	err := addOrUpdateTaintOnNode(ctx, c, nodeName, &taint)
	gomega.ExpectWithOffset(2, err).NotTo(gomega.HaveOccurred())
}

// addOrUpdateTaintOnNode 向节点添加 taint。如果确实添加了，会发起 API 调用
// 更新 node；否则不发起 API 调用。有错误则返回。
// 从 pkg/controller/controller_utils.go 的 AddOrUpdateTaintOnNode() 复制
func addOrUpdateTaintOnNode(ctx context.Context, c clientset.Interface, nodeName string, taints ...*v1.Taint) error {
	if len(taints) == 0 {
		return nil
	}
	firstTry := true
	return clientretry.RetryOnConflict(updateTaintBackOff, func() error {
		var err error
		var oldNode *v1.Node
		// 先从 API server 缓存获取 node，这样更便宜。若失败
		// 则从 etcd 获取，确保拿到最新数据。
		if firstTry {
			oldNode, err = c.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{ResourceVersion: "0"})
			firstTry = false
		} else {
			oldNode, err = c.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		}
		if err != nil {
			return err
		}

		var newNode *v1.Node
		oldNodeCopy := oldNode
		updated := false
		for _, taint := range taints {
			curNewNode, ok, err := addOrUpdateTaint(oldNodeCopy, taint)
			if err != nil {
				return fmt.Errorf("failed to update taint of node")
			}
			updated = updated || ok
			newNode = curNewNode
			oldNodeCopy = curNewNode
		}
		if !updated {
			return nil
		}
		return patchNodeTaints(ctx, c, nodeName, oldNode, newNode)
	})
}

// addOrUpdateTaint 尝试向注解列表添加 taint。如有更新则返回更新后 Node 的副本和 true，
// 否则返回 false。
// 从 pkg/util/taints/taints.go 的 AddOrUpdateTaint() 复制
func addOrUpdateTaint(node *v1.Node, taint *v1.Taint) (*v1.Node, bool, error) {
	newNode := node.DeepCopy()
	nodeTaints := newNode.Spec.Taints

	var newTaints []v1.Taint
	updated := false
	for i := range nodeTaints {
		if taint.MatchTaint(&nodeTaints[i]) {
			if semantic.DeepEqual(*taint, nodeTaints[i]) {
				return newNode, false, nil
			}
			newTaints = append(newTaints, *taint)
			updated = true
			continue
		}

		newTaints = append(newTaints, nodeTaints[i])
	}

	if !updated {
		newTaints = append(newTaints, *taint)
	}

	newNode.Spec.Taints = newTaints
	return newNode, true, nil
}

// semantic 可对核心对象做语义深度比较。
// 例如：apiequality.Semantic.DeepEqual(aPod, aPodWithNonNilButEmptyMaps) == true
// 从 pkg/apis/core/helper/helpers.go 的 Semantic 复制
var semantic = conversion.EqualitiesOrDie(
	func(a, b resource.Quantity) bool {
		// 忽略格式，只关心数值是否不变。
		// TODO: 如果认为格式重要，开始比较格式应该是安全的。
		//
		// 未初始化的 quantity 等同于 0 quantity。
		return a.Cmp(b) == 0
	},
	func(a, b metav1.MicroTime) bool {
		return a.UTC() == b.UTC()
	},
	func(a, b metav1.Time) bool {
		return a.UTC() == b.UTC()
	},
	func(a, b labels.Selector) bool {
		return a.String() == b.String()
	},
	func(a, b fields.Selector) bool {
		return a.String() == b.String()
	},
)

// removeNodeTaint 用于清理临时添加到节点上的 taint，
// 目标 taint 不存在或已被移除时不会失败。
// 传入 node 时会检查是否需要操作，如果 taint 不存在则不发起
// 任何 API 调用。
func removeNodeTaint(ctx context.Context, c clientset.Interface, nodeName string, node *v1.Node, taints ...*v1.Taint) error {
	if len(taints) == 0 {
		return nil
	}
	// 短路以减少 API 调用次数。
	if node != nil {
		match := false
		for _, taint := range taints {
			if taintExists(node.Spec.Taints, taint) {
				match = true
				break
			}
		}
		if !match {
			return nil
		}
	}

	firstTry := true
	return clientretry.RetryOnConflict(updateTaintBackOff, func() error {
		var err error
		var oldNode *v1.Node
		// 先从 API server 缓存获取 node，这样更便宜。若失败
		// 则从 etcd 获取，确保拿到最新数据。
		if firstTry {
			oldNode, err = c.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{ResourceVersion: "0"})
			firstTry = false
		} else {
			oldNode, err = c.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		}
		if err != nil {
			return err
		}

		var newNode *v1.Node
		oldNodeCopy := oldNode
		updated := false
		for _, taint := range taints {
			curNewNode, ok, err := removeTaint(oldNodeCopy, taint)
			if err != nil {
				return fmt.Errorf("failed to remove taint of node")
			}
			updated = updated || ok
			newNode = curNewNode
			oldNodeCopy = curNewNode
		}
		if !updated {
			return nil
		}
		return patchNodeTaints(ctx, c, nodeName, oldNode, newNode)
	})
}

// patchNodeTaints patch 节点的 taint。
func patchNodeTaints(ctx context.Context, c clientset.Interface, nodeName string, oldNode *v1.Node, newNode *v1.Node) error {
	oldData, err := json.Marshal(oldNode)
	if err != nil {
		return fmt.Errorf("failed to marshal old node %#v for node %q: %w", oldNode, nodeName, err)
	}

	newTaints := newNode.Spec.Taints
	newNodeClone := oldNode.DeepCopy()
	newNodeClone.Spec.Taints = newTaints
	newData, err := json.Marshal(newNodeClone)
	if err != nil {
		return fmt.Errorf("failed to marshal new node %#v for node %q: %w", newNodeClone, nodeName, err)
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, v1.Node{})
	if err != nil {
		return fmt.Errorf("failed to create patch for node %q: %w", nodeName, err)
	}

	_, err = c.CoreV1().Nodes().Patch(ctx, nodeName, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{})
	return err
}

// removeTaint 尝试从注解列表移除 taint。如有更新则返回更新后 Node 的副本和 true，
// 否则返回 false。
func removeTaint(node *v1.Node, taint *v1.Taint) (*v1.Node, bool, error) {
	newNode := node.DeepCopy()
	nodeTaints := newNode.Spec.Taints
	if len(nodeTaints) == 0 {
		return newNode, false, nil
	}

	if !taintExists(nodeTaints, taint) {
		return newNode, false, nil
	}

	newTaints, _ := deleteTaint(nodeTaints, taint)
	newNode.Spec.Taints = newTaints
	return newNode, true, nil
}

// deleteTaint 移除所有 key 和 effect 与给定 taintToDelete 相同的 taint。
func deleteTaint(taints []v1.Taint, taintToDelete *v1.Taint) ([]v1.Taint, bool) {
	var newTaints []v1.Taint
	deleted := false
	for i := range taints {
		if taintToDelete.MatchTaint(&taints[i]) {
			deleted = true
			continue
		}
		newTaints = append(newTaints, taints[i])
	}
	return newTaints, deleted
}

func verifyThatTaintIsGone(ctx context.Context, c clientset.Interface, nodeName string, taint *v1.Taint) {
	ginkgo.By("verifying the node doesn't have the taint " + taint.ToString())
	nodeUpdated, err := c.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})

	// TODO 在移除 core e2e 对 node 的依赖后使用 expect.go 中的包装方法
	gomega.ExpectWithOffset(2, err).NotTo(gomega.HaveOccurred())
	if taintExists(nodeUpdated.Spec.Taints, taint) {
		framework.Fail("Failed removing taint " + taint.ToString() + " of the node " + nodeName)
	}
}
