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

package kubelet

import (
	"reflect"
	"testing"
	"time"

	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestGetContainersToDeleteInPodWithFilter 验证按容器 ID 过滤时只保留指定容器，
// 删除其余符合删除条件的容器。
func TestGetContainersToDeleteInPodWithFilter(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 构造 5 个容器：4 个已退出、1 个运行中。
	pod := kubecontainer.PodStatus{
		ContainerStatuses: []*kubecontainer.Status{
			{
				ID:        kubecontainer.ContainerID{Type: "test", ID: "1"},
				Name:      "foo",
				CreatedAt: time.Now(),
				State:     kubecontainer.ContainerStateExited,
			},
			{
				ID:        kubecontainer.ContainerID{Type: "test", ID: "2"},
				Name:      "bar",
				CreatedAt: time.Now().Add(time.Second),
				State:     kubecontainer.ContainerStateExited,
			},
			{
				ID:        kubecontainer.ContainerID{Type: "test", ID: "3"},
				Name:      "bar",
				CreatedAt: time.Now().Add(2 * time.Second),
				State:     kubecontainer.ContainerStateExited,
			},
			{
				ID:        kubecontainer.ContainerID{Type: "test", ID: "4"},
				Name:      "bar",
				CreatedAt: time.Now().Add(3 * time.Second),
				State:     kubecontainer.ContainerStateExited,
			},
			{
				ID:        kubecontainer.ContainerID{Type: "test", ID: "5"},
				Name:      "bar",
				CreatedAt: time.Now().Add(4 * time.Second),
				State:     kubecontainer.ContainerStateRunning,
			},
		},
	}

	// 每个用例检查需要删除的容器集合。
	testCases := []struct {
		containersToKeep           int
		expectedContainersToDelete containerStatusbyCreatedList
	}{
		{
			0,
			[]*kubecontainer.Status{pod.ContainerStatuses[3], pod.ContainerStatuses[2], pod.ContainerStatuses[1]},
		},
		{
			1,
			[]*kubecontainer.Status{pod.ContainerStatuses[2], pod.ContainerStatuses[1]},
		},
		{
			2,
			[]*kubecontainer.Status{pod.ContainerStatuses[1]},
		},
	}

	// 过滤器为 "4" 时保留该容器，其余已退出容器按创建时间从新到旧删除。
	for _, test := range testCases {
		candidates := getContainersToDeleteInPod(logger, "4", &pod, test.containersToKeep)
		if !reflect.DeepEqual(candidates, test.expectedContainersToDelete) {
			t.Errorf("expected %v got %v", test.expectedContainersToDelete, candidates)
		}
	}
}

// TestGetContainersToDeleteInPod 验证不带过滤器时，按保留数量删除最旧的已退出容器。
func TestGetContainersToDeleteInPod(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 构造 5 个容器：4 个已退出、1 个运行中。
	pod := kubecontainer.PodStatus{
		ContainerStatuses: []*kubecontainer.Status{
			{
				ID:        kubecontainer.ContainerID{Type: "test", ID: "1"},
				Name:      "foo",
				CreatedAt: time.Now(),
				State:     kubecontainer.ContainerStateExited,
			},
			{
				ID:        kubecontainer.ContainerID{Type: "test", ID: "2"},
				Name:      "bar",
				CreatedAt: time.Now().Add(time.Second),
				State:     kubecontainer.ContainerStateExited,
			},
			{
				ID:        kubecontainer.ContainerID{Type: "test", ID: "3"},
				Name:      "bar",
				CreatedAt: time.Now().Add(2 * time.Second),
				State:     kubecontainer.ContainerStateExited,
			},
			{
				ID:        kubecontainer.ContainerID{Type: "test", ID: "4"},
				Name:      "bar",
				CreatedAt: time.Now().Add(3 * time.Second),
				State:     kubecontainer.ContainerStateExited,
			},
			{
				ID:        kubecontainer.ContainerID{Type: "test", ID: "5"},
				Name:      "bar",
				CreatedAt: time.Now().Add(4 * time.Second),
				State:     kubecontainer.ContainerStateRunning,
			},
		},
	}

	// 每个用例检查需要删除的容器集合。
	testCases := []struct {
		containersToKeep           int
		expectedContainersToDelete containerStatusbyCreatedList
	}{
		{
			0,
			[]*kubecontainer.Status{pod.ContainerStatuses[3], pod.ContainerStatuses[2], pod.ContainerStatuses[1], pod.ContainerStatuses[0]},
		},
		{
			1,
			[]*kubecontainer.Status{pod.ContainerStatuses[2], pod.ContainerStatuses[1], pod.ContainerStatuses[0]},
		},
		{
			2,
			[]*kubecontainer.Status{pod.ContainerStatuses[1], pod.ContainerStatuses[0]},
		},
	}

	// 保留数量越少，需要删除的旧容器越多。
	for _, test := range testCases {
		candidates := getContainersToDeleteInPod(logger, "", &pod, test.containersToKeep)
		if !reflect.DeepEqual(candidates, test.expectedContainersToDelete) {
			t.Errorf("expected %v got %v", test.expectedContainersToDelete, candidates)
		}
	}
}

// TestGetContainersToDeleteInPodWithNoMatch 验证过滤器不匹配任何容器时不删除任何容器。
func TestGetContainersToDeleteInPodWithNoMatch(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 构造 5 个容器：4 个已退出、1 个运行中。
	pod := kubecontainer.PodStatus{
		ContainerStatuses: []*kubecontainer.Status{
			{
				ID:        kubecontainer.ContainerID{Type: "test", ID: "1"},
				Name:      "foo",
				CreatedAt: time.Now(),
				State:     kubecontainer.ContainerStateExited,
			},
			{
				ID:        kubecontainer.ContainerID{Type: "test", ID: "2"},
				Name:      "bar",
				CreatedAt: time.Now().Add(time.Second),
				State:     kubecontainer.ContainerStateExited,
			},
			{
				ID:        kubecontainer.ContainerID{Type: "test", ID: "3"},
				Name:      "bar",
				CreatedAt: time.Now().Add(2 * time.Second),
				State:     kubecontainer.ContainerStateExited,
			},
			{
				ID:        kubecontainer.ContainerID{Type: "test", ID: "4"},
				Name:      "bar",
				CreatedAt: time.Now().Add(3 * time.Second),
				State:     kubecontainer.ContainerStateExited,
			},
			{
				ID:        kubecontainer.ContainerID{Type: "test", ID: "5"},
				Name:      "bar",
				CreatedAt: time.Now().Add(4 * time.Second),
				State:     kubecontainer.ContainerStateRunning,
			},
		},
	}

	// 过滤器 "abc" 不匹配任何容器，期望删除列表为空。
	testCases := []struct {
		filterID                   string
		expectedContainersToDelete containerStatusbyCreatedList
	}{
		{
			"abc",
			[]*kubecontainer.Status{},
		},
	}

	// 保留数量等于容器总数，因此只按过滤条件筛选。
	for _, test := range testCases {
		candidates := getContainersToDeleteInPod(logger, test.filterID, &pod, len(pod.ContainerStatuses))
		if !reflect.DeepEqual(candidates, test.expectedContainersToDelete) {
			t.Errorf("expected %v got %v", test.expectedContainersToDelete, candidates)
		}
	}
}
