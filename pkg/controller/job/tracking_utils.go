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

package job

import (
	"fmt"
	"slices"
	"sync"

	batch "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/controller/job/metrics"
)

// uidSetKeyFunc 从 uidSet 中解析出键，供 cache.Store 按 job key 索引。
// 传入非 uidSet 对象时返回错误，避免静默写入错误类型的条目。
var uidSetKeyFunc = func(obj interface{}) (string, error) {
	if u, ok := obj.(*uidSet); ok {
		return u.key, nil
	}
	return "", fmt.Errorf("could not find key for obj %#v", obj)
}

// uidSet 保存一个 job key 及对应的 UID 集合，
// uidTrackingExpectations 用它记录已经观察到的、以及仍在等待删除跟踪 finalizer 的 Pod UID。
type uidSet struct {
	sync.RWMutex
	set sets.Set[types.UID]
	key string
}

// uidTrackingExpectations 跟踪控制器的“预期”：仍在等待其跟踪 finalizer 被删除的 Pod UID。
// 通过比较预期与 informer 实际观察到的事件，控制器可判断相关事件是否已全部到齐。
type uidTrackingExpectations struct {
	store cache.Store
}

// getSet 按 controllerKey 从 store 中取出 uidSet，不存在或查询出错时返回 nil。
// 这是内部便捷方法，不直接暴露 UID 集合；返回的集合并非线程安全，
// 所有修改都必须在持有 uidSet 自身锁的情况下进行。
func (u *uidTrackingExpectations) getSet(controllerKey string) *uidSet {
	if obj, exists, err := u.store.GetByKey(controllerKey); err == nil && exists {
		return obj.(*uidSet)
	}
	return nil
}

// getExpectedUIDs 返回指定 job 仍在等待 finalizer 删除的 UID 快照。
// 采用克隆而非直接返回内部集合，防止调用方在锁外修改导致数据竞争。
func (u *uidTrackingExpectations) getExpectedUIDs(controllerKey string) sets.Set[types.UID] {
	uids := u.getSet(controllerKey)
	if uids == nil {
		return nil
	}
	uids.RLock()
	set := uids.set.Clone()
	uids.RUnlock()
	return set
}

// expectFinalizersRemoved 针对给定 jobKey 记录 deleteKeys 对应的删除预期。
// 每个 job 拥有独立的 uidSet 及其锁，因此不同 job key 之间可并发操作。
func (u *uidTrackingExpectations) expectFinalizersRemoved(logger klog.Logger, jobKey string, deletedKeys []types.UID) error {
	logger.V(4).Info("Expecting tracking finalizers removed", "key", jobKey, "podUIDs", deletedKeys)

	uids := u.getSet(jobKey)
	if uids == nil {
		// 首次为该 job 记录预期时创建 uidSet；store.Add 失败要向上返回，
		// 否则预期会丢失，控制器可能误认为事件已到齐而过早继续处理。
		uids = &uidSet{
			key: jobKey,
			set: sets.New[types.UID](),
		}
		if err := u.store.Add(uids); err != nil {
			return err
		}
	}
	uids.Lock()
	uids.set.Insert(deletedKeys...)
	uids.Unlock()
	return nil
}

// finalizerRemovalObserved 记录某个 Pod UID 的跟踪 finalizer 已被移除。
// 只有命中预期集合时才记日志并删除条目；对未预期的事件保持静默，
// 避免陈旧或重复事件干扰预期计数。
func (u *uidTrackingExpectations) finalizerRemovalObserved(logger klog.Logger, jobKey string, deleteKey types.UID) {
	uids := u.getSet(jobKey)
	if uids != nil {
		uids.Lock()
		if uids.set.Has(deleteKey) {
			logger.V(4).Info("Observed tracking finalizer removed", "key", jobKey, "podUID", deleteKey)
			uids.set.Delete(deleteKey)
		}
		uids.Unlock()
	}
}

// deleteExpectations 删除指定 job 的整个 UID 预期集合。
// job 结束后不再需要跟踪 finalizer，清理可防止 store 随 job 数量无限增长；
// 删除失败只记日志，因为此时继续等待预期也不会带来正确结果。
func (u *uidTrackingExpectations) deleteExpectations(logger klog.Logger, jobKey string) {
	set := u.getSet(jobKey)
	if set != nil {
		if err := u.store.Delete(set); err != nil {
			logger.Error(err, "Could not delete tracking annotation UID expectations", "key", jobKey)
		}
	}
}

// newUIDTrackingExpectations 创建一个以 uidSetKeyFunc 为键函数的 UID 预期跟踪器。
func newUIDTrackingExpectations() *uidTrackingExpectations {
	return &uidTrackingExpectations{store: cache.NewStore(uidSetKeyFunc)}
}

// hasJobTrackingFinalizer 判断 Pod 是否带有 Job 的跟踪 finalizer。
func hasJobTrackingFinalizer(pod *v1.Pod) bool {
	return slices.Contains(pod.Finalizers, batch.JobTrackingFinalizer)
}

// recordFinishedPodWithTrackingFinalizer 在 Pod 进入/退出“已终止且带跟踪 finalizer”状态时
// 更新对应计数器。仅当该状态发生翻转时才计数，避免每次 informer 重放事件都重复累加。
func recordFinishedPodWithTrackingFinalizer(oldPod, newPod *v1.Pod) {
	was := isFinishedPodWithTrackingFinalizer(oldPod)
	is := isFinishedPodWithTrackingFinalizer(newPod)
	if was == is {
		return
	}
	var event = metrics.Delete
	if is {
		event = metrics.Add
	}
	metrics.TerminatedPodsTrackingFinalizerTotal.WithLabelValues(event).Inc()
}

// isFinishedPodWithTrackingFinalizer 判断 Pod 是否已成功或失败终止且仍带跟踪 finalizer。
// nil Pod 返回 false，便于安全地传入 informer 的旧对象。
func isFinishedPodWithTrackingFinalizer(pod *v1.Pod) bool {
	if pod == nil {
		return false
	}
	return (pod.Status.Phase == v1.PodFailed || pod.Status.Phase == v1.PodSucceeded) && hasJobTrackingFinalizer(pod)
}
