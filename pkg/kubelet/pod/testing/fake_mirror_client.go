/*
Copyright 2015 The Kubernetes Authors.

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

package testing

import (
	"context"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

// FakeMirrorClient 是供测试使用的 MirrorClient 实现，记录镜像 Pod 与调用次数。
type FakeMirrorClient struct {
	// mirrorPodLock 保护以下字段。
	mirrorPodLock sync.RWMutex
	// 注意：真实的 mirror manager 不会保存镜像 Pod。
	// 该 fake 实现保存它们只是为了跟踪调用。
	// mirrorPods 是当前已知的镜像 Pod 全名集合。
	mirrorPods sets.Set[string]
	// createCounts 记录每个镜像 Pod 的创建次数。
	createCounts map[string]int
	// deleteCounts 记录每个镜像 Pod 的删除次数。
	deleteCounts map[string]int
}

// NewFakeMirrorClient 创建并初始化 FakeMirrorClient。
func NewFakeMirrorClient() *FakeMirrorClient {
	m := FakeMirrorClient{}
	m.mirrorPods = sets.New[string]()
	m.createCounts = make(map[string]int)
	m.deleteCounts = make(map[string]int)
	return &m
}

// CreateMirrorPod 记录镜像 Pod 名称与创建次数。
func (fmc *FakeMirrorClient) CreateMirrorPod(_ context.Context, pod *v1.Pod) error {
	fmc.mirrorPodLock.Lock()
	defer fmc.mirrorPodLock.Unlock()
	podFullName := kubecontainer.GetPodFullName(pod)
	fmc.mirrorPods.Insert(podFullName)
	fmc.createCounts[podFullName]++
	return nil
}

// DeleteMirrorPod 删除镜像 Pod 记录并累加删除次数。
// TODO（Robert Krawitz）：实现 UID 校验。
func (fmc *FakeMirrorClient) DeleteMirrorPod(_ context.Context, podFullName string, _ *types.UID) (bool, error) {
	fmc.mirrorPodLock.Lock()
	defer fmc.mirrorPodLock.Unlock()
	fmc.mirrorPods.Delete(podFullName)
	fmc.deleteCounts[podFullName]++
	return true, nil
}

// HasPod 返回指定镜像 Pod 是否存在。
func (fmc *FakeMirrorClient) HasPod(podFullName string) bool {
	fmc.mirrorPodLock.RLock()
	defer fmc.mirrorPodLock.RUnlock()
	return fmc.mirrorPods.Has(podFullName)
}

// NumOfPods 返回镜像 Pod 数量。
func (fmc *FakeMirrorClient) NumOfPods() int {
	fmc.mirrorPodLock.RLock()
	defer fmc.mirrorPodLock.RUnlock()
	return fmc.mirrorPods.Len()
}

// GetPods 返回所有镜像 Pod 的全名。
func (fmc *FakeMirrorClient) GetPods() []string {
	fmc.mirrorPodLock.RLock()
	defer fmc.mirrorPodLock.RUnlock()
	return sets.List(fmc.mirrorPods)
}

// GetCounts 返回指定镜像 Pod 的创建与删除次数。
func (fmc *FakeMirrorClient) GetCounts(podFullName string) (int, int) {
	fmc.mirrorPodLock.RLock()
	defer fmc.mirrorPodLock.RUnlock()
	return fmc.createCounts[podFullName], fmc.deleteCounts[podFullName]
}
