/*
Copyright 2020 The Kubernetes Authors.

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

package util

import (
	"context"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"k8s.io/klog/v2"
)

// SetNodeOwnerFunc 构造一个续约后处理函数，用于给 Lease 设置 Node 的 OwnerReference。
func SetNodeOwnerFunc(ctx context.Context, c clientset.Interface, nodeName string) func(lease *coordinationv1.Lease) error {
	return func(lease *coordinationv1.Lease) error {
		// 设置 owner 引用需要 Node 的 UID（注意它不同于 kubelet.nodeRef.UID）。
		// 租约刚创建时 master 与 node 可能尚未连通，因此每次续约都尝试补设 owner 引用。
		if len(lease.OwnerReferences) == 0 {
			if node, err := c.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{}); err == nil {
				lease.OwnerReferences = []metav1.OwnerReference{
					{
						APIVersion: corev1.SchemeGroupVersion.WithKind("Node").Version,
						Kind:       corev1.SchemeGroupVersion.WithKind("Node").Kind,
						Name:       nodeName,
						UID:        node.UID,
					},
				}
			} else {
				logger := klog.FromContext(ctx)
				logger.Error(err, "Failed to get node when trying to set owner ref to the node lease", "node", klog.KRef("", nodeName))
				return err
			}
		}
		return nil
	}
}
