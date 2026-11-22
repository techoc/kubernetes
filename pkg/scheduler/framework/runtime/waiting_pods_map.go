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

package runtime

import (
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	fwk "k8s.io/kube-scheduler/framework"
)

// waitingPodsMap 是线程安全的映射，维护处于 permit 阶段等待的 Pod。
type waitingPodsMap struct {
	pods map[types.UID]*waitingPod
	mu   sync.RWMutex
}

// NewWaitingPodsMap 返回一个新的 waitingPodsMap。
func NewWaitingPodsMap() *waitingPodsMap {
	return &waitingPodsMap{
		pods: make(map[types.UID]*waitingPod),
	}
}

// add 把一个 WaitingPod 加入映射。
func (m *waitingPodsMap) add(wp *waitingPod) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pods[wp.GetPod().UID] = wp
}

// remove 按 UID 从映射中移除一个 WaitingPod。
func (m *waitingPodsMap) remove(uid types.UID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pods, uid)
}

// get 按 UID 从映射中取一个 WaitingPod，不存在时返回 nil。
func (m *waitingPodsMap) get(uid types.UID) *waitingPod {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.pods[uid]
}

// iterate 持有读锁遍历所有 WaitingPod，对每个元素调用 callback。
// 回调中不应再调用本映射的写方法，以免死锁。
func (m *waitingPodsMap) iterate(callback func(fwk.WaitingPod)) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, v := range m.pods {
		callback(v)
	}
}

// waitingPod 表示在 permit 阶段等待各插件放行的 Pod。
type waitingPod struct {
	pod            *v1.Pod
	pendingPlugins map[string]*time.Timer
	s              chan *fwk.Status
	mu             sync.RWMutex
	done           bool
}

var _ fwk.WaitingPod = &waitingPod{}

// newWaitingPod 根据每个 permit 插件的最长等待时间创建一个新的 waitingPod。
func newWaitingPod(pod *v1.Pod, pluginsMaxWaitTime map[string]time.Duration) *waitingPod {
	wp := &waitingPod{
		pod: pod,
		// Allow() 和 Reject() 必须是非阻塞的，因此用非阻塞发送。
		// 缓冲区大小为 1，避免接收方晚于发送方就绪时把唯一的状态通知丢掉。
		s: make(chan *fwk.Status, 1),
	}

	wp.pendingPlugins = make(map[string]*time.Timer, len(pluginsMaxWaitTime))
	// time.AfterFunc 触发的 Reject 会遍历 pendingPlugins，先持锁保证定时器回调
	// 只能在初始化完成后执行，避免读到半构造的状态。
	wp.mu.Lock()
	defer wp.mu.Unlock()
	for plugin, waitTime := range pluginsMaxWaitTime {
		wp.pendingPlugins[plugin] = time.AfterFunc(waitTime, func() {
			msg := fmt.Sprintf("rejected due to timeout after waiting %v at plugin %v",
				waitTime, plugin)
			wp.Reject(plugin, msg)
		})
	}

	return wp
}

// GetPod 返回等待中的 Pod 引用。
func (w *waitingPod) GetPod() *v1.Pod {
	return w.pod
}

// GetPendingPlugins 返回尚未放行的 permit 插件名字列表。
func (w *waitingPod) GetPendingPlugins() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	plugins := make([]string, 0, len(w.pendingPlugins))
	for p := range w.pendingPlugins {
		plugins = append(plugins, p)
	}

	return plugins
}

// Allow 表示插件 pluginName 同意调度该等待中的 Pod。
// 当它是最后一个未放行的插件时，会发送成功信号解除 Pod 的等待。
func (w *waitingPod) Allow(pluginName string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if timer, exist := w.pendingPlugins[pluginName]; exist {
		timer.Stop()
		delete(w.pendingPlugins, pluginName)
	}

	// 还有插件未放行，此时不能提前发出成功信号。
	if len(w.pendingPlugins) != 0 {
		return
	}

	// 用 select+default 实现非阻塞发送：缓冲区满时直接跳过，避免卡住插件。
	select {
	case w.s <- fwk.NewStatus(fwk.Success, ""):
	default:
	}
	w.done = true
}

// Reject 判定等待中的 Pod 不可调度。
func (w *waitingPod) Reject(pluginName, msg string) bool {
	return w.stopWithStatus(fwk.Unschedulable, pluginName, msg)
}

// Preempt 表示等待中的 Pod 被抢占。与 Reject 不同，它不把 Pod 标记为不可调度，
// 因此 Pod 还有机会被重新调度。
func (w *waitingPod) Preempt(pluginName, msg string) bool {
	return w.stopWithStatus(fwk.Error, pluginName, msg)
}

// stopWithStatus 停止所有待处理定时器并向等待方发送给定的状态。
// 返回 true 表示本次调用首次结束了等待，重复调用返回 false。
func (w *waitingPod) stopWithStatus(status fwk.Code, pluginName, msg string) bool {
	w.mu.RLock()
	defer w.mu.RUnlock()
	for _, timer := range w.pendingPlugins {
		timer.Stop()
	}

	// 非阻塞发送：状态缓冲区已满说明等待方已有结论，不必覆盖。
	select {
	case w.s <- fwk.NewStatus(status, msg).WithPlugin(pluginName):
	default:
	}
	if w.done {
		return false
	}
	w.done = true
	return true
}
