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
	"fmt"
	"k8s.io/klog/v2"
	"regexp"
	"time"

	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

const sleepTime = 20 * time.Second

var requiredPerNodePods = []*regexp.Regexp{
	regexp.MustCompile(".*kube-proxy.*"),
	regexp.MustCompile(".*fluentd-elasticsearch.*"),
	regexp.MustCompile(".*node-problem-detector.*"),
}

// WaitForReadyNodes 在 timeout 内等待集群达到期望规模且没有 not-ready 节点。
// 集群规模指可调度 Node 的数量。
func WaitForReadyNodes(ctx context.Context, c clientset.Interface, size int, timeout time.Duration) error {
	_, err := CheckReady(ctx, c, size, timeout)
	return err
}

// WaitForTotalHealthy 检查所有已注册节点是否 ready，以及必需的系统 Pod 是否都在运行。
func WaitForTotalHealthy(ctx context.Context, c clientset.Interface, timeout time.Duration) error {
	logger := klog.FromContext(ctx)
	framework.Logf("Waiting up to %v for all nodes to be ready", timeout)

	var notReady []v1.Node
	var missingPodsPerNode map[string][]string
	err := wait.PollUntilContextTimeout(ctx, poll, timeout, true, func(ctx context.Context) (bool, error) {
		notReady = nil
		// 这里把不可调度节点一并列出也没问题。
		nodes, err := c.CoreV1().Nodes().List(ctx, metav1.ListOptions{ResourceVersion: "0"})
		if err != nil {
			return false, err
		}
		for _, node := range nodes.Items {
			if !IsConditionSetAsExpected(&node, v1.NodeReady, true) {
				notReady = append(notReady, node)
			}
		}
		pods, err := c.CoreV1().Pods(metav1.NamespaceAll).List(ctx, metav1.ListOptions{ResourceVersion: "0"})
		if err != nil {
			return false, err
		}

		systemPodsPerNode := make(map[string][]string)
		for _, pod := range pods.Items {
			if pod.Namespace == metav1.NamespaceSystem && pod.Status.Phase == v1.PodRunning {
				if pod.Spec.NodeName != "" {
					systemPodsPerNode[pod.Spec.NodeName] = append(systemPodsPerNode[pod.Spec.NodeName], pod.Name)
				}
			}
		}
		missingPodsPerNode = make(map[string][]string)
		for _, node := range nodes.Items {
			if isNodeSchedulableWithoutTaints(logger, &node) {
				for _, requiredPod := range requiredPerNodePods {
					foundRequired := false
					for _, presentPod := range systemPodsPerNode[node.Name] {
						if requiredPod.MatchString(presentPod) {
							foundRequired = true
							break
						}
					}
					if !foundRequired {
						missingPodsPerNode[node.Name] = append(missingPodsPerNode[node.Name], requiredPod.String())
					}
				}
			}
		}
		return len(notReady) == 0 && len(missingPodsPerNode) == 0, nil
	})

	if err != nil && !wait.Interrupted(err) {
		return err
	}

	if len(notReady) > 0 {
		return fmt.Errorf("Not ready nodes: %v", notReady)
	}
	if len(missingPodsPerNode) > 0 {
		return fmt.Errorf("Not running system Pods: %v", missingPodsPerNode)
	}
	return nil

}

// WaitConditionToBe 在 timeout 内检查节点 name 的 condition 状态是否匹配 wantTrue。
// wantTrue 为 true 时要求 condition 状态为 ConditionTrue；为 false 时要求
// 处于 ConditionTrue 以外的任意状态（例如 false 或 unknown）。
func WaitConditionToBe(ctx context.Context, c clientset.Interface, name string, conditionType v1.NodeConditionType, wantTrue bool, timeout time.Duration) bool {
	framework.Logf("Waiting up to %v for node %s condition %s to be %t", timeout, name, conditionType, wantTrue)
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(poll) {
		node, err := c.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			framework.Logf("Couldn't get node %s", name)
			continue
		}

		if IsConditionSetAsExpected(node, conditionType, wantTrue) {
			return true
		}
	}
	framework.Logf("Node %s didn't reach desired %s condition status (%t) within %v", name, conditionType, wantTrue, timeout)
	return false
}

// WaitForNodeToBeNotReady 在 timeout 内检查节点 name 是否 not ready
// （即 readiness condition 非 ready，例如 false 或 unknown）。
func WaitForNodeToBeNotReady(ctx context.Context, c clientset.Interface, name string, timeout time.Duration) bool {
	return WaitConditionToBe(ctx, c, name, v1.NodeReady, false, timeout)
}

// WaitForNodeToBeReady 在 timeout 内检查节点 name 是否 ready。
func WaitForNodeToBeReady(ctx context.Context, c clientset.Interface, name string, timeout time.Duration) bool {
	return WaitConditionToBe(ctx, c, name, v1.NodeReady, true, timeout)
}

func WaitForNodeSchedulable(ctx context.Context, c clientset.Interface, name string, timeout time.Duration, wantSchedulable bool) bool {
	logger := klog.FromContext(ctx)
	framework.Logf("Waiting up to %v for node %s to be schedulable: %t", timeout, name, wantSchedulable)
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(poll) {
		node, err := c.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			framework.Logf("Couldn't get node %s", name)
			continue
		}

		if IsNodeSchedulable(logger, node) == wantSchedulable {
			return true
		}
	}
	framework.Logf("Node %s didn't reach desired schedulable status (%t) within %v", name, wantSchedulable, timeout)
	return false
}

// WaitForNodeHeartbeatAfter 在 timeout 内等待节点发送时间晚于给定时间戳的下一次心跳。
//
// 要确保节点状态确实由重启后的 kubelet 上报，应在 kubelet 停机期间
// 通过 [GetNodeHeartbeatTime] 取到 after 这个值。
func WaitForNodeHeartbeatAfter(ctx context.Context, c clientset.Interface, name string, after metav1.Time, timeout time.Duration) {
	framework.Logf("Waiting up to %v for node %s to send a heartbeat after %v", timeout, name, after)
	gomega.Eventually(ctx, func() (time.Time, error) {
		node, err := c.CoreV1().Nodes().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			framework.Logf("Couldn't get node %s", name)
			return time.Time{}, err
		}
		return GetNodeHeartbeatTime(node).Time, nil
	}, timeout, poll).Should(gomega.BeTemporally(">", after.Time), "Node %s didn't send a heartbeat", name)
}

// CheckReady 在 timeout 内等待集群达到期望规模且没有 not-ready 节点。
// 集群规模指可调度 Node 的数量。
func CheckReady(ctx context.Context, c clientset.Interface, size int, timeout time.Duration) ([]v1.Node, error) {
	for start := time.Now(); time.Since(start) < timeout; time.Sleep(sleepTime) {
		nodes, err := waitListSchedulableNodes(ctx, c)
		if err != nil {
			framework.Logf("Failed to list nodes: %v", err)
			continue
		}
		numNodes := len(nodes.Items)

		// 过滤掉 not-ready 的节点。
		Filter(nodes, func(node v1.Node) bool {
			nodeReady := IsConditionSetAsExpected(&node, v1.NodeReady, true)
			networkReady := isConditionUnset(&node, v1.NodeNetworkUnavailable) || IsConditionSetAsExpected(&node, v1.NodeNetworkUnavailable, false)
			return nodeReady && networkReady
		})
		numReady := len(nodes.Items)

		if numNodes == size && numReady == size {
			framework.Logf("Cluster has reached the desired number of ready nodes %d", size)
			return nodes.Items, nil
		}
		framework.Logf("Waiting for ready nodes %d, current ready %d, not ready nodes %d", size, numReady, numNodes-numReady)
	}
	return nil, fmt.Errorf("timeout waiting %v for number of ready nodes to be %d", timeout, size)
}

// waitListSchedulableNodes 是列节点的包装，支持重试。
func waitListSchedulableNodes(ctx context.Context, c clientset.Interface) (*v1.NodeList, error) {
	var nodes *v1.NodeList
	var err error
	if wait.PollUntilContextTimeout(ctx, poll, singleCallTimeout, true, func(ctx context.Context) (bool, error) {
		nodes, err = c.CoreV1().Nodes().List(ctx, metav1.ListOptions{FieldSelector: fields.Set{
			"spec.unschedulable": "false",
		}.AsSelector().String()})
		if err != nil {
			return false, err
		}
		return true, nil
	}) != nil {
		return nodes, err
	}
	return nodes, nil
}

// checkWaitListSchedulableNodes 是列节点的包装，支持重试。
func checkWaitListSchedulableNodes(ctx context.Context, c clientset.Interface) (*v1.NodeList, error) {
	nodes, err := waitListSchedulableNodes(ctx, c)
	if err != nil {
		return nil, fmt.Errorf("error: %s. Non-retryable failure or timed out while listing nodes for e2e cluster", err)
	}
	return nodes, nil
}

// CheckReadyForTests 返回一个函数：ready 节点数超过 allowedNotReadyNodes 允许的
// 未就绪阈值时就返回 true（即用作启动测试的全局闸门）。
func CheckReadyForTests(ctx context.Context, c clientset.Interface, nonblockingTaints string, allowedNotReadyNodes, largeClusterThreshold int) func(ctx context.Context) (bool, error) {
	logger := klog.FromContext(ctx)
	attempt := 0
	return func(ctx context.Context) (bool, error) {
		if allowedNotReadyNodes == -1 {
			return true, nil
		}
		attempt++
		var nodesNotReadyYet []v1.Node
		opts := metav1.ListOptions{
			ResourceVersion: "0",
			// 统计时排除 uncordon 的节点；如果 node v2 API 去掉这个语义，TODO 重构这里。
			FieldSelector: fields.Set{"spec.unschedulable": "false"}.AsSelector().String(),
		}
		allNodes, err := c.CoreV1().Nodes().List(ctx, opts)
		if err != nil {
			var terminalListNodesErr error
			framework.Logf("Unexpected error listing nodes: %v", err)
			if attempt >= 3 {
				terminalListNodesErr = err
			}
			return false, terminalListNodesErr
		}
		for _, node := range allNodes.Items {
			if !readyForTests(logger, &node, nonblockingTaints) {
				nodesNotReadyYet = append(nodesNotReadyYet, node)
			}
		}
		// framework 允许 <TestContext.AllowedNotReadyNodes> 个节点未就绪，
		// 以容忍少量节点部署异常（集群校验也允许这种情况）。有些节点在启动时
		// 就没有正确初始化（例如某些组件没装好），永远不会 ready，
		// 所以不能指望它们变成 ready。
		//
		// 我们会记录节点不可调度的*原因*，通常是网络不可用。
		if len(nodesNotReadyYet) > 0 {
			// 大集群里每 10 次才打印一次。
			if len(nodesNotReadyYet) < largeClusterThreshold || attempt%10 == 0 {
				framework.Logf("Unschedulable nodes= %v, maximum value for starting tests= %v", len(nodesNotReadyYet), allowedNotReadyNodes)
				for _, node := range nodesNotReadyYet {
					framework.Logf("	-> Node %s [[[ Ready=%t, Network(available)=%t, Taints=%v, NonblockingTaints=%v ]]]",
						node.Name,
						IsConditionSetAsExpectedSilent(&node, v1.NodeReady, true),
						IsConditionSetAsExpectedSilent(&node, v1.NodeNetworkUnavailable, false),
						node.Spec.Taints,
						nonblockingTaints,
					)

				}
				if len(nodesNotReadyYet) > allowedNotReadyNodes {
					ready := len(allNodes.Items) - len(nodesNotReadyYet)
					remaining := len(nodesNotReadyYet) - allowedNotReadyNodes
					framework.Logf("==== node wait: %v out of %v nodes are ready, max notReady allowed %v.  Need %v more before starting.", ready, len(allNodes.Items), allowedNotReadyNodes, remaining)
				}
			}
		}
		return len(nodesNotReadyYet) <= allowedNotReadyNodes, nil
	}
}

// readyForTests 判断是否可以停止等待节点进入可测试状态。
// 默认要求节点可调度、NodeReady 且没有 taint。带有 nonblocking taints 的节点
// 允许保留这些 taint，并且在本函数中忽略其 node.Spec.Unschedulable 字段。
func readyForTests(logger klog.Logger, node *v1.Node, nonblockingTaints string) bool {
	if hasNonblockingTaint(node, nonblockingTaints) {
		// 节点带有 nonblockingTaints 中的 taint 时，只检查它是否 ready，
		// 不要求 node.Spec.Unschedulable 取任何特定值。
		if !IsNodeReady(logger, node) || !isNodeUntaintedWithNonblocking(logger, node, nonblockingTaints) {
			return false
		}
	} else {
		if !IsNodeSchedulable(logger, node) || !isNodeUntainted(logger, node) {
			return false
		}
	}
	return true
}
