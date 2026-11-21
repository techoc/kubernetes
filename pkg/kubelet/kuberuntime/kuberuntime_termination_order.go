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
	"time"

	v1 "k8s.io/api/core/v1"

	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
)

// terminationOrdering 用于约束 sidecar 容器的终止顺序。
// 它在 sidecar 之间建立依赖，使 Pod 终止流程可以等待宽限期结束，
// 或等待所有依赖容器终止完成。
type terminationOrdering struct {
	// terminated 是从容器名到通道的映射；通道关闭表示该容器已终止。
	terminated map[string]chan struct{}
	// prereqs 是从容器名到通道列表的映射，容器必须等待这些通道关闭
	// 以保证终止顺序。
	prereqs map[string][]chan struct{}

	// lock 保护 terminated 与 prereqs 的并发访问。
	lock sync.Mutex
}

// newTerminationOrdering 根据 Pod 规格和当前运行中的容器构造终止顺序约束。
func newTerminationOrdering(pod *v1.Pod, runningContainerNames []string) *terminationOrdering {
	to := &terminationOrdering{
		prereqs:    map[string][]chan struct{}{},
		terminated: map[string]chan struct{}{},
	}

	// 建立运行中容器集合，用于判断哪些容器需要等待。
	runningContainers := map[string]struct{}{}
	for _, name := range runningContainerNames {
		runningContainers[name] = struct{}{}
	}

	var mainContainerChannels []chan struct{}
	// sidecar 需要等待主容器退出，因此为每个主容器创建一个通道供其等待。
	for _, c := range pod.Spec.Containers {
		channel := make(chan struct{})
		to.terminated[c.Name] = channel
		mainContainerChannels = append(mainContainerChannels, channel)

		// 非运行中的容器直接预先关闭通道，避免其他容器无谓等待。
		if _, isRunning := runningContainers[c.Name]; !isRunning {
			close(channel)
		}
	}

	// 逆序遍历 InitContainer，使 sidecar 按声明的逆序依次终止。
	var previousSidecarName string
	for i := range pod.Spec.InitContainers {
		// 按逆序获取 InitContainer。
		ic := pod.Spec.InitContainers[len(pod.Spec.InitContainers)-i-1]

		channel := make(chan struct{})
		to.terminated[ic.Name] = channel

		// 非运行中的容器直接预先关闭通道，避免其他容器无谓等待。
		if _, isRunning := runningContainers[ic.Name]; !isRunning {
			close(channel)
		}

		if podutil.IsRestartableInitContainer(&ic) {
			// sidecar 需要等待所有主容器退出。
			to.prereqs[ic.Name] = append(to.prereqs[ic.Name], mainContainerChannels...)

			// 若存在后声明的 sidecar，本容器需要等它先终止。
			if previousSidecarName != "" {
				to.prereqs[ic.Name] = append(to.prereqs[ic.Name], to.terminated[previousSidecarName])
			}
			previousSidecarName = ic.Name
		}
	}
	return to
}

// waitForTurn 等待指定容器可以开始终止，最多等到给定宽限期。
// gracePeriod = 0 时不等待。返回实际等待的秒数。
func (o *terminationOrdering) waitForTurn(name string, gracePeriod int64) float64 {
	// 没有宽限期时无需等待。
	if gracePeriod <= 0 {
		return 0
	}

	// 记录等待开始时间，用于返回实际等待时长。
	start := time.Now()
	remainingGrace := time.NewTimer(time.Duration(gracePeriod) * time.Second)

	for _, c := range o.prereqs[name] {
		select {
		case <-c:
		case <-remainingGrace.C:
			// 宽限期已到，立即返回。
			return time.Since(start).Seconds()
		}
	}

	return time.Since(start).Seconds()
}

// containerTerminated 应在指定容器退出后调用，用于通知等待者。
func (o *terminationOrdering) containerTerminated(name string) {
	o.lock.Lock()
	defer o.lock.Unlock()
	// 关闭通道通知等待者，并删除记录避免重复关闭。
	if ch, ok := o.terminated[name]; ok {
		close(ch)
		delete(o.terminated, name)
	}
}
