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

package runtime

import (
	"context"
	"errors"
	"sync"

	"k8s.io/apimachinery/pkg/types"
	fwk "k8s.io/kube-scheduler/framework"
)

// podsInPreBindMap 保存正处于 preBind 阶段的 Pod，线程安全。
type podsInPreBindMap struct {
	pods map[types.UID]*podInPreBind
	mu   sync.RWMutex
}

// NewPodsInPreBindMap 创建一个空的 podsInPreBindMap。
func NewPodsInPreBindMap() *podsInPreBindMap {
	return &podsInPreBindMap{
		pods: make(map[types.UID]*podInPreBind),
	}
}

// get 返回指定 UID 对应的 Pod，不存在时返回 nil。
func (pbm *podsInPreBindMap) get(uid types.UID) *podInPreBind {
	pbm.mu.RLock()
	defer pbm.mu.RUnlock()
	return pbm.pods[uid]
}

// add 将 Pod 加入 map，若已存在则覆盖。
func (pbm *podsInPreBindMap) add(uid types.UID, cancel context.CancelCauseFunc) {
	pbm.mu.Lock()
	defer pbm.mu.Unlock()
	pbm.pods[uid] = &podInPreBind{cancel: cancel}
}

// remove 从 map 中移除指定 UID 的 Pod。
func (pbm *podsInPreBindMap) remove(uid types.UID) {
	pbm.mu.Lock()
	defer pbm.mu.Unlock()
	delete(pbm.pods, uid)
}

var _ fwk.PodInPreBind = &podInPreBind{}

// podInPreBind 描述一个处于 preBind 阶段、尚未真正发起 Bind 调用的 Pod。
type podInPreBind struct {
	finished bool
	canceled bool
	cancel   context.CancelCauseFunc
	mu       sync.Mutex
}

// CancelPod 取消指定 Pod 运行 preBind 阶段所使用的 context。
// 一旦 preBind 完成（MarkPrebound）便无法再取消。
func (bp *podInPreBind) CancelPod(message string) bool {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if bp.finished {
		return false
	}
	if !bp.canceled {
		bp.cancel(errors.New(message))
	}
	bp.canceled = true
	return true
}

// MarkPrebound 标记 Pod 已完成绑定周期的 preBind 阶段，
// 此后该绑定周期无法再被取消。
func (bp *podInPreBind) MarkPrebound() bool {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if bp.canceled {
		return false
	}
	bp.finished = true
	return true
}
