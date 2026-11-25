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

package util

import (
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
)

// TriggerTimeTracker 记录每个 Service 最近一次的 Pod/Service 触发时间，
// 用于计算 EndpointsLastChangeTriggerTime 注解。
// 同一对象在两次连续同步之间多次变化时，计算结果可能不准确。修复需要大幅重写
// endpoint(slice) controller 与 Informer 框架；此类情况较少，当前接受该限制并给出近似值。
type TriggerTimeTracker struct {
	// 以 ServiceKey 为键保存最近一次同步时观察到的状态。
	ServiceStates map[ServiceKey]ServiceState

	// 保护 ServiceStates 映射的互斥锁。
	mutex sync.Mutex
}

// NewTriggerTimeTracker 创建一个空的 TriggerTimeTracker。
func NewTriggerTimeTracker() *TriggerTimeTracker {
	return &TriggerTimeTracker{
		// 初始化 Service 状态映射。
		ServiceStates: make(map[ServiceKey]ServiceState),
	}
}

// ServiceKey 由命名空间和名称组成，唯一标识一个 Service。
type ServiceKey struct {
	// Namespace 与 Name 共同构成 Service 的唯一标识。
	Namespace, Name string
}

// ServiceState 保存 tracker 已知的某个 Service 的最近触发时间。
type ServiceState struct {
	// 最近一次观察到的 Service 触发时间。
	lastServiceTriggerTime time.Time
	// 以 Pod 名称为键记录最近一次观察到的 Pod 触发时间。
	lastPodTriggerTimes map[string]time.Time
}

// ComputeEndpointLastChangeTriggerTime 更新 Service 状态并返回应写入注解的时间；
// 返回零值表示不应输出该注解。
// 本方法仅在不同 key 并发调用时安全；同一 key 不得并发调用。
// 当前 endpoint(slice) controller 满足该契约。
func (t *TriggerTimeTracker) ComputeEndpointLastChangeTriggerTime(
	namespace string, service *v1.Service, pods []*v1.Pod) time.Time {

	// 用命名空间和 Service 名称构造键。
	key := ServiceKey{Namespace: namespace, Name: service.Name}
	// 同一键不会被并发调用，只需短暂加锁读取状态。
	t.mutex.Lock()
	// 读取旧状态并判断该 Service 是否已知。
	state, wasKnown := t.ServiceStates[key]
	t.mutex.Unlock()

	// 函数返回前用 defer 将最新状态写回映射。
	defer func() {
		t.mutex.Lock()
		t.ServiceStates[key] = state
		t.mutex.Unlock()
	}()

	// 记录自上次同步以来发生变化的最小触发时间。
	var minChangedTriggerTime time.Time
	podTriggerTimes := make(map[string]time.Time)
	// 先收集本次所有 Pod 的触发时间。
	for _, pod := range pods {
		// 遍历所有相关的 Pod。
		if podTriggerTime := getPodTriggerTime(pod); !podTriggerTime.IsZero() {
			// 只处理带有有效 Ready 转换时间的 Pod。
			podTriggerTimes[pod.Name] = podTriggerTime
			if podTriggerTime.After(state.lastPodTriggerTimes[pod.Name]) {
				// Pod 触发时间变晚说明出现了新的触发事件。
				minChangedTriggerTime = min(minChangedTriggerTime, podTriggerTime)
				// 取所有变化触发时间中的最小值。
			}
		}
	}
	serviceTriggerTime := getServiceTriggerTime(service)
	// Service 的触发时间取自其创建时间。
	if serviceTriggerTime.After(state.lastServiceTriggerTime) {
		// Service 触发时间变晚时同样纳入最小值计算。
		minChangedTriggerTime = min(minChangedTriggerTime, serviceTriggerTime)
	}

	state.lastPodTriggerTimes = podTriggerTimes
	// 用本次观察结果覆盖旧状态。
	state.lastServiceTriggerTime = serviceTriggerTime

	if !wasKnown {
		// 新 Service 首次同步时直接返回其创建时间。
		return service.CreationTimestamp.Time
	}

	// 普通更新返回变化触发时间中的最小值（无变化则为零值）。
	return minChangedTriggerTime
}

// DeleteService 删除指定 Service 在本 tracker 中保存的状态。
func (t *TriggerTimeTracker) DeleteService(namespace, name string) {
	key := ServiceKey{Namespace: namespace, Name: name}
	// 构造键并加锁删除。
	t.mutex.Lock()
	defer t.mutex.Unlock()
	delete(t.ServiceStates, key)
}

// getPodTriggerTime 返回导致 endpoint 变化的 Pod Ready 条件最近转换时间。
func getPodTriggerTime(pod *v1.Pod) (triggerTime time.Time) {
	if readyCondition := getPodReadyCondition(&pod.Status); readyCondition != nil {
		// 从 PodStatus 中取出 Ready 条件的转换时间；不存在时返回零值。
		triggerTime = readyCondition.LastTransitionTime.Time
	}
	return triggerTime
}

// getServiceTriggerTime 返回 Service 的创建时间作为其触发时间。
func getServiceTriggerTime(service *v1.Service) (triggerTime time.Time) {
	return service.CreationTimestamp.Time
}

// min 返回两个时间的较小值；currentMin 为零值时直接返回 newValue。
func min(currentMin, newValue time.Time) time.Time {
	if currentMin.IsZero() || newValue.Before(currentMin) {
		// 零值表示尚未设置，此时以 newValue 为准。
		return newValue
	}
	return currentMin
}
