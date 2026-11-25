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

package benchmark

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	// createNodeRetries 定义创建节点时的重试次数。
	createNodeRetries = 5

	// pollingInterval 定义等待节点创建完成时的轮询间隔。
	pollingInterval = 2 * time.Second

	// singleCallTimeout 是单次 API 调用（如 'get' 或 'list'）的尝试时长。用于防止
	// 瞬时失败导致测试失败。
	singleCallTimeout = 5 * time.Minute
)

// NodeTemplate 负责创建一个 v1.Node 实例，
// 该实例可直接发送到 API server。
type NodeTemplate interface {
	// GetNodeTemplate 返回众多不同节点之一的节点模板。
	// 它会被多次调用，其中 index 递增，count 参数保持不变。
	// 可以使用（但不是必须）这些数字修改 node spec 的部分内容，
	// 例如指向其他对象的具名引用。
	GetNodeTemplate(index, count int) (*v1.Node, error)
}

// StaticNodeTemplate 返回一个 NodeTemplate 实现，无论 index 如何，它都返回同一个固定节点。
func StaticNodeTemplate(node *v1.Node) NodeTemplate {
	return (*staticNodeTemplate)(node)
}

type staticNodeTemplate v1.Node

// GetNodeTemplate 实现了 [NodeTemplate.GetNodeTemplate]，每次调用
// 都返回同一个节点。
func (s *staticNodeTemplate) GetNodeTemplate(index, count int) (*v1.Node, error) {
	return (*v1.Node)(s), nil
}

// IntegrationTestNodePreparer 保存测试节点准备器的配置信息。
type IntegrationTestNodePreparer struct {
	client          clientset.Interface
	countToStrategy []CountToStrategy
	nodeTemplate    NodeTemplate
}

// NewIntegrationTestNodePreparer 使用给定的 nodeTemplate 创建一个 IntegrationTestNodePreparer。
func NewIntegrationTestNodePreparer(client clientset.Interface, countToStrategy []CountToStrategy, nodeTemplate NodeTemplate) TestNodePreparer {
	return &IntegrationTestNodePreparer{
		client:          client,
		countToStrategy: countToStrategy,
		nodeTemplate:    nodeTemplate,
	}
}

// PrepareNodes 准备 countToStrategy 指定的测试节点。
func (p *IntegrationTestNodePreparer) PrepareNodes(ctx context.Context, nextNodeIndex int) error {
	numNodes := 0
	for _, v := range p.countToStrategy {
		numNodes += v.Count
	}

	klog.Infof("Making %d nodes", numNodes)

	for i := 0; i < numNodes; i++ {
		baseNode, err := p.nodeTemplate.GetNodeTemplate(i, numNodes)
		if err != nil {
			return fmt.Errorf("failed to get node template: %w", err)
		}
		for range createNodeRetries {
			// 使用常见的 kubernetes.io/hostname 标签创建节点。
			// 为此，如果希望在一个请求中完成，
			// 我们需要提前知道节点名称。
			node := baseNode.DeepCopy()
			name := node.Name
			if name == "" {
				name = node.GenerateName + rand.String(5)
				node.Name = name
			}
			if node.Labels == nil {
				node.Labels = make(map[string]string)
			}
			node.Labels["kubernetes.io/hostname"] = name
			_, err = p.client.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
			if err == nil {
				break
			}
		}
		if err != nil {
			return fmt.Errorf("creating node: %w", err)
		}
	}

	nodes, err := waitListAllNodes(ctx, p.client)
	if err != nil {
		return fmt.Errorf("listing nodes: %w", err)
	}
	index := nextNodeIndex
	for _, v := range p.countToStrategy {
		for i := 0; i < v.Count; i, index = i+1, index+1 {
			if err := DoPrepareNode(ctx, p.client, &nodes.Items[index], v.Strategy); err != nil {
				return fmt.Errorf("aborting node preparation: %w", err)
			}
		}
	}
	return nil
}

// CleanupNodes 删除现有的测试节点。
func (p *IntegrationTestNodePreparer) CleanupNodes(ctx context.Context) error {
	// TODO(#93794): 让 CleanupNodes 只清理由该
	// IntegrationTestNodePreparer 创建的节点，使其行为更直观。
	nodes, err := waitListAllNodes(ctx, p.client)
	if err != nil {
		klog.Fatalf("Error listing nodes: %v", err)
	}
	var errRet error
	for i := range nodes.Items {
		if err := p.client.CoreV1().Nodes().Delete(ctx, nodes.Items[i].Name, metav1.DeleteOptions{}); err != nil {
			klog.Errorf("Error while deleting Node: %v", err)
			errRet = err
		}
	}
	return errRet
}

// waitListAllNodes 是对列出节点操作的封装，支持重试。
func waitListAllNodes(ctx context.Context, c clientset.Interface) (*v1.NodeList, error) {
	var nodes *v1.NodeList
	var err error
	if wait.PollUntilContextTimeout(ctx, pollingInterval, singleCallTimeout, true, func(ctx context.Context) (bool, error) {
		nodes, err = c.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, err
		}
		return true, nil
	}) != nil {
		return nodes, err
	}
	return nodes, nil
}
