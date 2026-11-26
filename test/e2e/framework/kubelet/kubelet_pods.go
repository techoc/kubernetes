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

package kubelet

import (
	"context"

	v1 "k8s.io/api/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
)

// GetKubeletPods 获取 kubelet 上的 Pod 列表。
func GetKubeletPods(ctx context.Context, c clientset.Interface, node string) (*v1.PodList, error) {
	return getKubeletPods(ctx, c, node, "pods")
}

// GetKubeletRunningPods 获取 kubelet 上的运行中 Pod 列表。这些 Pod
// 包含必要信息（例如 Pod/容器的 UID、name、namespace），
// 但不包含完整的 spec。
func GetKubeletRunningPods(ctx context.Context, c clientset.Interface, node string) (*v1.PodList, error) {
	return getKubeletPods(ctx, c, node, "runningpods")
}

func getKubeletPods(ctx context.Context, c clientset.Interface, node, resource string) (*v1.PodList, error) {
	result := &v1.PodList{}
	client, err := ProxyRequest(ctx, c, node, resource, framework.KubeletPort)
	if err != nil {
		return &v1.PodList{}, err
	}
	if err = client.Into(result); err != nil {
		return &v1.PodList{}, err
	}
	return result, nil
}
