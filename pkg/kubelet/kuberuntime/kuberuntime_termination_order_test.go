/*
Copyright 2023 The Kubernetes Authors.

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

package kuberuntime

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestTerminationOrderingSidecarStopAfterMain 验证 sidecar 必须等待主容器退出后才终止。
func TestTerminationOrderingSidecarStopAfterMain(t *testing.T) {
	synctest.Test(t, testTerminationOrderingSidecarStopAfterMain)
}

// testTerminationOrderingSidecarStopAfterMain 用两个 goroutine 模拟主容器与 sidecar 的退出顺序。
func testTerminationOrderingSidecarStopAfterMain(t *testing.T) {
	restartPolicy := v1.ContainerRestartPolicyAlways
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "bar",
			Namespace: "new",
		},
		Spec: v1.PodSpec{
			InitContainers: []v1.Container{
				{
					Name:            "init",
					Image:           "busybox",
					ImagePullPolicy: v1.PullIfNotPresent,
					RestartPolicy:   &restartPolicy,
				},
			},
			Containers: []v1.Container{
				{
					Name:            "main",
					Image:           "busybox",
					ImagePullPolicy: v1.PullIfNotPresent,
				},
			},
		},
	}
	to := newTerminationOrdering(pod, getContainerNames(pod))

	var wg sync.WaitGroup
	wg.Add(1)
	var sidecarWaitDelay, mainWaitDelay int64
	syncChannel := make(chan struct{})

	go func() {
		close(syncChannel) // 通知另一个 goroutine 本 goroutine 已启动。
		sidecarWaitDelay = int64(to.waitForTurn("init", 30))
		to.containerTerminated("init")
		wg.Done()
	}()

	wg.Add(1)
	go func() {
		<-syncChannel // 确保 sidecar 的等待已经开始，再让主容器退出。
		mainWaitDelay = int64(to.waitForTurn("main", 0))
		time.Sleep(1 * time.Second)
		to.containerTerminated("main")
		wg.Done()
	}()
	wg.Wait()
	// sidecar 等待一个“轮次”（主容器退出），主容器本身不等待。
	if sidecarWaitDelay != 1 {
		t.Errorf("expected sidecar to wait for main container to exit, got delay of %d", sidecarWaitDelay)
	}
	if mainWaitDelay != 0 {
		t.Errorf("expected main container to not wait to exit, got delay of %d", mainWaitDelay)
	}
}

// TestTerminationOrderingSidecarsInReverseOrder 验证多个 sidecar 按与启动相反的顺序终止。
func TestTerminationOrderingSidecarsInReverseOrder(t *testing.T) {
	synctest.Test(t, testTerminationOrderingSidecarsInReverseOrder)
}

// testTerminationOrderingSidecarsInReverseOrder 让三个 sidecar 和主容器并发等待轮次，
// 并记录各自实际等待的轮次数。
func testTerminationOrderingSidecarsInReverseOrder(t *testing.T) {
	restartPolicy := v1.ContainerRestartPolicyAlways
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "bar",
			Namespace: "new",
		},
		Spec: v1.PodSpec{
			InitContainers: []v1.Container{
				{
					Name:            "sc1",
					Image:           "busybox",
					ImagePullPolicy: v1.PullIfNotPresent,
					RestartPolicy:   &restartPolicy,
				},
				{
					Name:            "sc2",
					Image:           "busybox",
					ImagePullPolicy: v1.PullIfNotPresent,
					RestartPolicy:   &restartPolicy,
				},
				{
					Name:            "sc3",
					Image:           "busybox",
					ImagePullPolicy: v1.PullIfNotPresent,
					RestartPolicy:   &restartPolicy,
				},
			},
			Containers: []v1.Container{
				{
					Name:            "main",
					Image:           "busybox",
					ImagePullPolicy: v1.PullIfNotPresent,
				},
			},
		},
	}
	to := newTerminationOrdering(pod, getContainerNames(pod))

	var wg sync.WaitGroup
	var delays sync.Map

	waitAndExit := func(name string) {
		delay := int64(to.waitForTurn(name, 30))
		delays.Store(name, delay)
		time.Sleep(1250 * time.Millisecond)
		to.containerTerminated(name)
		wg.Done()
	}
	for _, ic := range pod.Spec.InitContainers {
		wg.Add(1)
		go waitAndExit(ic.Name)
	}
	for _, c := range pod.Spec.Containers {
		wg.Add(1)
		go waitAndExit(c.Name)
	}

	// 等待所有模拟容器退出。
	wg.Wait()

	getDelay := func(name string) int64 {
		delay, ok := delays.Load(name)
		if !ok {
			t.Errorf("unable to find delay for container %s", name)
		}
		return delay.(int64)
	}

	for _, tc := range []struct {
		containerName string
		expectedDelay int64
	}{
		// sidecar 应按逆序退出：sc1 需要等主容器、sc3、sc2 共 3 轮。
		{
			containerName: "sc1",
			expectedDelay: 3,
		},
		// sc2 需要等主容器和 sc3 共 2 轮。
		{
			containerName: "sc2",
			expectedDelay: 2,
		},
		// sc3 只需等主容器 1 轮。
		{
			containerName: "sc3",
			expectedDelay: 1,
		},
		// 主容器无需等待，延迟为 0。
		{
			containerName: "main",
			expectedDelay: 0,
		},
	} {
		if got := getDelay(tc.containerName); got != tc.expectedDelay {
			t.Errorf("expected delay for container %s = %d, got %d", tc.containerName, tc.expectedDelay, got)
		}
	}
}

// TestTerminationOrderingObeysGrace 验证等待时间受 Pod 宽限期上限约束。
func TestTerminationOrderingObeysGrace(t *testing.T) {
	synctest.Test(t, testTerminationOrderingObeysGrace)
}

// testTerminationOrderingObeysGrace 用 2 秒宽限期模拟无法等到所有轮次完成的情况。
func testTerminationOrderingObeysGrace(t *testing.T) {
	restartPolicy := v1.ContainerRestartPolicyAlways
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			UID:       "12345678",
			Name:      "bar",
			Namespace: "new",
		},
		Spec: v1.PodSpec{
			InitContainers: []v1.Container{
				{
					Name:            "sc1",
					Image:           "busybox",
					ImagePullPolicy: v1.PullIfNotPresent,
					RestartPolicy:   &restartPolicy,
				},
				{
					Name:            "sc2",
					Image:           "busybox",
					ImagePullPolicy: v1.PullIfNotPresent,
					RestartPolicy:   &restartPolicy,
				},
				{
					Name:            "sc3",
					Image:           "busybox",
					ImagePullPolicy: v1.PullIfNotPresent,
					RestartPolicy:   &restartPolicy,
				},
			},
			Containers: []v1.Container{
				{
					Name:            "main",
					Image:           "busybox",
					ImagePullPolicy: v1.PullIfNotPresent,
				},
			},
		},
	}
	to := newTerminationOrdering(pod, getContainerNames(pod))

	var wg sync.WaitGroup
	var delays sync.Map

	waitAndExit := func(name string) {
		// 宽限期只有 2 秒，不足以让所有 sidecar 依次完成等待。
		delay := int64(to.waitForTurn(name, 2))
		delays.Store(name, delay)
		time.Sleep(1 * time.Second)
		to.containerTerminated(name)
		wg.Done()
	}
	for _, ic := range pod.Spec.InitContainers {
		wg.Add(1)
		go waitAndExit(ic.Name)
	}
	for _, c := range pod.Spec.Containers {
		wg.Add(1)
		go waitAndExit(c.Name)
	}

	// 等待所有模拟容器退出。
	wg.Wait()

	getDelay := func(name string) int64 {
		delay, ok := delays.Load(name)
		if !ok {
			t.Errorf("unable to find delay for container %s", name)
		}
		return delay.(int64)
	}

	for _, tc := range []struct {
		containerName string
		expectedDelay int64
	}{
		{
			containerName: "sc1",
			// 整体宽限期限制了这里的等待时长。
			expectedDelay: 2,
		},
		{
			containerName: "sc2",
			expectedDelay: 2,
		},
		{
			containerName: "sc3",
			expectedDelay: 1,
		},
		{
			containerName: "main",
			expectedDelay: 0,
		},
	} {
		if got := getDelay(tc.containerName); got != tc.expectedDelay {
			t.Errorf("expected delay for container %s = %d, got %d", tc.containerName, tc.expectedDelay, got)
		}
	}
}

// getContainerNames 按启动顺序返回 init 容器与普通容器名称。
func getContainerNames(p *v1.Pod) []string {
	var running []string
	for _, ic := range p.Spec.InitContainers {
		running = append(running, ic.Name)
	}
	for _, c := range p.Spec.Containers {
		running = append(running, c.Name)
	}
	return running
}
