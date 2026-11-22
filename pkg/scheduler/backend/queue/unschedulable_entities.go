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

package queue

import (
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
)

// unschedulableEntities 保存暂时无法调度的 Pod 和 PodGroup。
type unschedulableEntities struct {
	// entityInfoMap 以实体的 key 为索引，保存对应的 QueuedEntityInfo。
	entityInfoMap map[string]framework.QueuedEntityInfo
	keyFunc       func(framework.QueuedEntityInfo) string
	// 两个 recorder 分别统计不可调度队列中两类实体的数量：
	// unschedulableRecorder 统计普通不可调度实体，gatedRecorder 统计被调度门禁阻塞的实体。
	// 分开计数便于观察门禁解除前后的队列变化。
	unschedulableRecorder, gatedRecorder metrics.MetricRecorder
}

// newUnschedulableEntities 初始化一个 unschedulableEntities 对象。
func newUnschedulableEntities(unschedulableRecorder, gatedRecorder metrics.MetricRecorder) *unschedulableEntities {
	return &unschedulableEntities{
		entityInfoMap:         make(map[string]framework.QueuedEntityInfo),
		keyFunc:               queuedEntityKeyFunc,
		unschedulableRecorder: unschedulableRecorder,
		gatedRecorder:         gatedRecorder,
	}
}

// updateMetricsOnStateChange 处理实体在 Gated 和 Unschedulable 状态间切换时的指标记账。
func (u *unschedulableEntities) updateMetricsOnStateChange(gatedBefore, isGated bool, entity framework.QueuedEntityInfo) {
	if gatedBefore == isGated {
		return
	}

	if gatedBefore {
		// 状态切换：Gated -> Ungated，门禁解除后归入普通不可调度计数。
		u.gatedRecorder.Remove(entity)
		u.unschedulableRecorder.Add(entity)
	} else {
		// 状态切换：Ungated -> Gated，被门禁阻塞后改由 gated 计数。
		u.gatedRecorder.Add(entity)
		u.unschedulableRecorder.Remove(entity)
	}
}

// addOrUpdate 把实体加入不可调度的 entityInfoMap。
// event 标记触发加入的事件，用于指标记录。
// strategy 是实体原先的排队策略，据此判断它是从其他队列迁入还是全新加入，
// 从而正确维护流入实体的指标。
func (u *unschedulableEntities) addOrUpdate(entity framework.QueuedEntityInfo, gatedBefore bool, event string, strategy *queueingStrategy) {
	entityKey := u.keyFunc(entity)
	if _, exists := u.entityInfoMap[entityKey]; exists {
		u.updateMetricsOnStateChange(gatedBefore, entity.Gated(), entity)
	} else {
		if entity.Gated() {
			u.gatedRecorder.Add(entity)
		} else {
			u.unschedulableRecorder.Add(entity)
		}
		recordIncomingEntitiesMetrics("unschedulable", entity, event, strategy)
	}
	u.entityInfoMap[entityKey] = entity
}

// delete 从不可调度的 entityInfoMap 中删除实体。
// gated 参数指明实体当时的状态，用于确定该减少哪个计数器。
func (u *unschedulableEntities) delete(entity framework.QueuedEntityInfo, gated bool) {
	entityKey := u.keyFunc(entity)
	if _, exists := u.entityInfoMap[entityKey]; exists {
		if gated {
			u.gatedRecorder.Remove(entity)
		} else {
			u.unschedulableRecorder.Remove(entity)
		}
	}
	delete(u.entityInfoMap, entityKey)
}

// get 查找 map 中 key 相同的实体并返回其 QueuedEntityInfo，找不到则返回 nil。
func (u *unschedulableEntities) get(entity framework.QueuedEntityInfo) framework.QueuedEntityInfo {
	entityKey := u.keyFunc(entity)
	if entity, exists := u.entityInfoMap[entityKey]; exists {
		return entity
	}
	return nil
}

// clear 清空不可调度的 entityInfoMap 及对应的指标记录。
func (u *unschedulableEntities) clear() {
	u.entityInfoMap = make(map[string]framework.QueuedEntityInfo)
	u.unschedulableRecorder.Clear()
	u.gatedRecorder.Clear()
}
