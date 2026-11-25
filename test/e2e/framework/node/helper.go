/*
Copyright 2014 The Kubernetes Authors.

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
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"

	"k8s.io/kubernetes/test/e2e/framework"
	testutils "k8s.io/kubernetes/test/utils"
)

const (
	// 判定集群为"大规模"所需的最少节点数。
	largeClusterThreshold = 100
)

// WaitForAllNodesSchedulable 最多等待 timeout，直到所有节点
// （TestContext.AllowedNotReadyNodes 指定的数量除外）都变为可调度。
func WaitForAllNodesSchedulable(ctx context.Context, c clientset.Interface, timeout time.Duration) error {
	if framework.TestContext.AllowedNotReadyNodes == -1 {
		return nil
	}

	framework.Logf("Waiting up to %v for all (but %d) nodes to be schedulable", timeout, framework.TestContext.AllowedNotReadyNodes)
	return wait.PollUntilContextTimeout(
		ctx,
		30*time.Second,
		timeout,
		true,
		CheckReadyForTests(ctx, c, framework.TestContext.NonblockingTaints, framework.TestContext.AllowedNotReadyNodes, largeClusterThreshold),
	)
}

// AddOrUpdateLabelOnNode 给指定节点添加给定的 label 键值对，若已存在则更新其值。
func AddOrUpdateLabelOnNode(c clientset.Interface, nodeName string, labelKey, labelValue string) {
	framework.ExpectNoError(testutils.AddLabelsToNode(c, nodeName, map[string]string{labelKey: labelValue}))
}

// ExpectNodeHasLabel 断言指定节点具有给定的 label 键值对。
func ExpectNodeHasLabel(ctx context.Context, c clientset.Interface, nodeName string, labelKey string, labelValue string) {
	ginkgo.By("verifying the node has the label " + labelKey + " " + labelValue)
	node, err := c.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	framework.ExpectNoError(err)
	gomega.Expect(node.Labels).To(gomega.HaveKeyWithValue(labelKey, labelValue))
}

// RemoveLabelOffNode 用于清理临时添加到节点上的 label；
// 目标 label 不存在或已被移除时不会报错。
func RemoveLabelOffNode(c clientset.Interface, nodeName string, labelKey string) {
	ginkgo.By("removing the label " + labelKey + " off the node " + nodeName)
	framework.ExpectNoError(testutils.RemoveLabelOffNode(c, nodeName, []string{labelKey}))

	ginkgo.By("verifying the node doesn't have the label " + labelKey)
	framework.ExpectNoError(testutils.VerifyLabelsRemoved(c, nodeName, []string{labelKey}))
}

// ExpectNodeHasTaint 断言节点具有给定的 taint。
func ExpectNodeHasTaint(ctx context.Context, c clientset.Interface, nodeName string, taint *v1.Taint) {
	ginkgo.By("verifying the node has the taint " + taint.ToString())
	if has, err := NodeHasTaint(ctx, c, nodeName, taint); !has {
		framework.ExpectNoError(err)
		framework.Failf("Failed to find taint %s on node %s", taint.ToString(), nodeName)
	}
}

// NodeHasTaint 在节点具有给定 taint 时返回 true，否则返回 false。
func NodeHasTaint(ctx context.Context, c clientset.Interface, nodeName string, taint *v1.Taint) (bool, error) {
	node, err := c.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
	if err != nil {
		return false, err
	}

	nodeTaints := node.Spec.Taints

	if len(nodeTaints) == 0 || !taintExists(nodeTaints, taint) {
		return false, nil
	}
	return true, nil
}

// AllNodesReady 检查所有已注册节点是否就绪。将
// framework.TestContext.AllowedNotReadyNodes 设为 -1 会跳过测试后的节点就绪检查。
// TODO: 应把 AfterEach 中的 AllNodesReady 调用改为 WaitForAllNodesHealthy，
// 并找到可配置的办法，因为不能期望所有测试环境都运行默认的测试插件。
func AllNodesReady(ctx context.Context, c clientset.Interface, timeout time.Duration) error {
	if err := allNodesReady(ctx, c, timeout); err != nil {
		return fmt.Errorf("checking for ready nodes: %w", err)
	}
	return nil
}

func allNodesReady(ctx context.Context, c clientset.Interface, timeout time.Duration) error {
	if framework.TestContext.AllowedNotReadyNodes == -1 {
		return nil
	}

	framework.Logf("Waiting up to %v for all (but %d) nodes to be ready", timeout, framework.TestContext.AllowedNotReadyNodes)

	var notReady []*v1.Node
	err := wait.PollUntilContextTimeout(ctx, framework.Poll, timeout, true, func(ctx context.Context) (bool, error) {
		notReady = nil
		// 这里列出不可调度的节点也是可以的。
		nodes, err := c.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		for i := range nodes.Items {
			node := &nodes.Items[i]
			if !IsConditionSetAsExpected(node, v1.NodeReady, true) {
				notReady = append(notReady, node)
			}
		}
		// framework 允许最多 <TestContext.AllowedNotReadyNodes> 个节点未就绪，
		// 以容忍少量节点配置错误的情况（集群校验中也允许这种情况）。
		// 某些启动时未正确部署的节点永远不会就绪（例如安装失败），
		// 因此不能指望它们在任意时刻都处于就绪状态。
		return len(notReady) <= framework.TestContext.AllowedNotReadyNodes, nil
	})

	if err != nil && !wait.Interrupted(err) {
		return err
	}

	if len(notReady) > framework.TestContext.AllowedNotReadyNodes {
		msg := ""
		for _, node := range notReady {
			msg = fmt.Sprintf("%s, %s", msg, node.Name)
		}
		return fmt.Errorf("Not ready nodes: %#v", msg)
	}
	return nil
}

// taintExists 检查给定 taint 是否存在于 taint 列表中，存在返回 true，否则返回 false。
func taintExists(taints []v1.Taint, taintToFind *v1.Taint) bool {
	for _, taint := range taints {
		if taint.MatchTaint(taintToFind) {
			return true
		}
	}
	return false
}

// IsARM64 检查该 k8s Node 是否为 arm64 架构。
func IsARM64(node *v1.Node) bool {
	arch, ok := node.Labels["kubernetes.io/arch"]
	if ok {
		return arch == "arm64"
	}

	return false
}

// AddExtendedResource 向 Node 添加一项伪造的扩展资源。
func AddExtendedResource(ctx context.Context, clientSet clientset.Interface, nodeName string, extendedResourceName v1.ResourceName, extendedResourceQuantity resource.Quantity) {
	extendedResource := v1.ResourceName(extendedResourceName)

	ginkgo.By("Adding a custom resource")
	extendedResourceList := v1.ResourceList{
		extendedResource: extendedResourceQuantity,
	}

	// 不能直接把 Node 结构体序列化为 JSON：那样会清掉 node status 中此时不应改动的
	// daemonEndpoints、nodeInfo 等字段。这里用 map 绕开该问题。
	// 参见 https://github.com/kubernetes/kubernetes/issues/131229
	patchPayload, err := json.Marshal(map[string]any{
		"status": map[string]any{
			"capacity":    extendedResourceList,
			"allocatable": extendedResourceList,
		},
	})
	framework.ExpectNoError(err, "Failed to marshal node JSON")

	_, err = clientSet.CoreV1().Nodes().Patch(ctx, nodeName, types.StrategicMergePatchType, []byte(patchPayload), metav1.PatchOptions{}, "status")
	framework.ExpectNoError(err)
}

// RemoveExtendedResource 从 Node 移除一项伪造的扩展资源。
func RemoveExtendedResource(ctx context.Context, clientSet clientset.Interface, nodeName string, extendedResourceName v1.ResourceName) {
	extendedResource := v1.ResourceName(extendedResourceName)

	ginkgo.By("Removing a custom resource")
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		node, err := clientSet.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get node %s: %w", nodeName, err)
		}
		delete(node.Status.Capacity, extendedResource)
		delete(node.Status.Allocatable, extendedResource)
		_, err = clientSet.CoreV1().Nodes().UpdateStatus(ctx, node, metav1.UpdateOptions{})
		return err
	})
	framework.ExpectNoError(err)
}

func HealthCheck(url string) bool {
	insecureTransport := http.DefaultTransport.(*http.Transport).Clone()
	insecureTransport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	insecureHTTPClient := &http.Client{
		Transport: insecureTransport,
	}

	req, err := http.NewRequest(http.MethodHead, url, nil)
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", framework.TestContext.BearerToken))
	resp, err := insecureHTTPClient.Do(req)
	if err != nil {
		framework.Logf("Health check on %q failed, error=%v", url, err)
	} else if resp.StatusCode != http.StatusOK {
		framework.Logf("Health check on %q failed, status=%d", url, resp.StatusCode)
	}
	return err == nil && resp.StatusCode == http.StatusOK
}
