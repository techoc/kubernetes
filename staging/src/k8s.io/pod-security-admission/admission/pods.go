/*
Copyright 2021 The Kubernetes Authors.

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

package admission

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
)

// PodListerFromClient returns a PodLister that does live lists using the provided client.
// 实时列表适合对时效性要求高的场景（如审计当前违规 Pod），代价是每次调用都打到 API Server。
func PodListerFromClient(client kubernetes.Interface) PodLister {
	return &clientPodLister{client}
}

// clientPodLister 通过客户端实时列取 Pod。
// 每次 ListPods 都同步访问 API Server，拿到的永远是最新状态。
type clientPodLister struct {
	client kubernetes.Interface
}

func (p *clientPodLister) ListPods(ctx context.Context, namespace string) ([]*corev1.Pod, error) {
	// 取 Items 的地址而非值：PodLister 接口统一返回 []*corev1.Pod，
	// 这里把切片元素转为指针以适配该契约，元素本身仍在 list 底层数组中。
	list, err := p.client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	pods := make([]*corev1.Pod, len(list.Items))
	for i := range list.Items {
		pods[i] = &list.Items[i]
	}
	return pods, nil
}

// PodListerFromInformer returns a PodLister that does cached lists using the provided lister.
// 缓存列表避免每次准入都访问 API Server，适合高频调用；代价是读到的数据可能有短暂延迟。
func PodListerFromInformer(lister corev1listers.PodLister) PodLister {
	return &informerPodLister{lister}
}

// informerPodLister 从 informer 本地缓存列取 Pod，不产生 API Server 请求。
// ctx 在此实现中未使用：底层 lister 是同步缓存查询，无法被取消。
type informerPodLister struct {
	lister corev1listers.PodLister
}

func (p *informerPodLister) ListPods(ctx context.Context, namespace string) ([]*corev1.Pod, error) {
	return p.lister.Pods(namespace).List(labels.Everything())
}
