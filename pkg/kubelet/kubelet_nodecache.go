/*
Copyright 2025 The Kubernetes Authors.

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

package kubelet

import (
	"context"
	"strconv"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

// GetCachedNode 根据 useCache 参数从缓存返回节点对象，
// 或向 API server 发起同步调用来获取。
func (kl *Kubelet) GetCachedNode(ctx context.Context, useCache bool) (*v1.Node, error) {
	// 按需选择缓存或同步读取。
	if useCache {
		return kl.getCachedNode(ctx)
	}
	return kl.getNodeSync(ctx)
}

// getCachedNode 比较当前缓存节点与 informer 中的节点，返回较新的那个。
func (kl *Kubelet) getCachedNode(ctx context.Context) (*v1.Node, error) {
	logger := klog.FromContext(ctx)
	informerNode, err := kl.GetNode(ctx)
	if err != nil {
		// 读取 informer 失败时退回缓存节点，避免中断。
		if kl.cachedNode != nil {
			logger.Error(err, "failed to list node; using cached node")
			return kl.cachedNode, nil
		}
		return kl.initialNode(ctx)
	}

	// 首次调用直接缓存 informer 节点。
	if kl.cachedNode == nil {
		kl.cachedNode = informerNode
		return informerNode, nil
	}

	isNewer, err := isNewer(informerNode, kl.cachedNode)
	if err != nil {
		// 出错时默认使用 informer 节点。
		logger.Error(err, "failed to check if node is newer; using informer node")
		kl.cachedNode = informerNode
	}
	if isNewer {
		kl.cachedNode = informerNode
	}
	return kl.cachedNode, nil
}

// getNodeSync 向 API server 发起同步调用获取最新节点信息，并强制刷新缓存。
func (kl *Kubelet) getNodeSync(ctx context.Context) (*v1.Node, error) {
	// 没有 API 客户端时只能使用初始节点信息。
	if kl.kubeClient == nil {
		return kl.initialNode(ctx)
	}
	node, err := kl.kubeClient.CoreV1().Nodes().Get(ctx, string(kl.nodeName), metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	// 同步结果同时更新缓存。
	kl.cachedNode = node
	return node, nil
}

// isNewer 根据 ResourceVersion 判断 informer 节点是否比缓存节点更新。
func isNewer(informerNode, cachedNode *v1.Node) (bool, error) {
	if informerNode == cachedNode {
		return true, nil
	}
	informerVersion, err := getObjVersion(informerNode)
	if err != nil {
		return false, err
	}
	cachedVersion, err := getObjVersion(cachedNode)
	if err != nil {
		return false, err
	}
	return informerVersion > cachedVersion, nil
}

// getObjVersion 解析节点的 ResourceVersion 为整数。
func getObjVersion(n *v1.Node) (int64, error) {
	objResourceVersion, err := strconv.ParseInt(n.ResourceVersion, 10, 64)
	if err != nil {
		return -1, err
	}
	return objResourceVersion, nil
}
