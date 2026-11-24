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

package registrytest

import (
	"context"
	"sync"

	"k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// NodeRegistry 是用于测试的内存 Node 注册表模拟，通过互斥锁保护节点列表和错误状态。
type NodeRegistry struct {
	Err   error
	Node  string
	Nodes api.NodeList

	sync.Mutex
}

// MakeNodeList 根据节点名称列表和统一的资源容量构造 NodeList。
func MakeNodeList(nodes []string, nodeResources api.ResourceList) *api.NodeList {
	list := api.NodeList{
		Items: make([]api.Node, len(nodes)),
	}
	for i := range nodes {
		list.Items[i].Name = nodes[i]
		list.Items[i].Status.Capacity = nodeResources
	}
	return &list
}

// SetError 设置后续节点操作统一返回的模拟错误。
func (r *NodeRegistry) SetError(err error) {
	r.Lock()
	defer r.Unlock()
	r.Err = err
}

// ListNodes 返回模拟注册表中的节点列表及当前设置的错误。
func (r *NodeRegistry) ListNodes(ctx context.Context, options *metainternalversion.ListOptions) (*api.NodeList, error) {
	r.Lock()
	defer r.Unlock()
	return &r.Nodes, r.Err
}

// CreateNode 记录新节点名称并将节点追加到模拟列表。
func (r *NodeRegistry) CreateNode(ctx context.Context, node *api.Node) error {
	r.Lock()
	defer r.Unlock()
	r.Node = node.Name
	r.Nodes.Items = append(r.Nodes.Items, *node)
	return r.Err
}

// UpdateNode 按节点名称替换模拟列表中的节点对象。
func (r *NodeRegistry) UpdateNode(ctx context.Context, node *api.Node) error {
	r.Lock()
	defer r.Unlock()
	for i, item := range r.Nodes.Items {
		if item.Name == node.Name {
			r.Nodes.Items[i] = *node
			return r.Err
		}
	}
	return r.Err
}

// GetNode 按名称读取节点；若节点不存在则返回标准 NotFound 错误。
func (r *NodeRegistry) GetNode(ctx context.Context, nodeID string, options *metav1.GetOptions) (*api.Node, error) {
	r.Lock()
	defer r.Unlock()
	if r.Err != nil {
		return nil, r.Err
	}
	for _, node := range r.Nodes.Items {
		if node.Name == nodeID {
			return &node, nil
		}
	}
	return nil, errors.NewNotFound(api.Resource("nodes"), nodeID)
}

// DeleteNode 从模拟列表中移除指定节点，并保留其余节点名称元数据。
func (r *NodeRegistry) DeleteNode(ctx context.Context, nodeID string) error {
	r.Lock()
	defer r.Unlock()
	var newList []api.Node
	for _, node := range r.Nodes.Items {
		if node.Name != nodeID {
			newList = append(newList, api.Node{ObjectMeta: metav1.ObjectMeta{Name: node.Name}})
		}
	}
	r.Nodes.Items = newList
	return r.Err
}

// WatchNodes 当前不产生事件流，仅返回预设的模拟错误。
func (r *NodeRegistry) WatchNodes(ctx context.Context, options *metainternalversion.ListOptions) (watch.Interface, error) {
	r.Lock()
	defer r.Unlock()
	return nil, r.Err
}
