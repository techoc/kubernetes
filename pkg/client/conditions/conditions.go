/*
Copyright 2014 The Kubernetes Authors.

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

package conditions

import (
	"fmt"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
)

// ErrPodCompleted 由 PodRunning 或 PodContainerRunning 返回，用于表明
// Pod 已经进入完成状态（成功或失败）。
var ErrPodCompleted = fmt.Errorf("pod ran to completion")

// PodRunning 判断 Pod 是否正在运行：
//   - 返回 true 表示 Pod 已进入 Running 状态；
//   - 返回 false 表示 Pod 尚未进入 Running 状态；
//   - 返回 ErrPodCompleted 表示 Pod 已经运行结束（成功或失败）；
//   - 其他异常情况返回相应错误。
func PodRunning(event watch.Event) (bool, error) {
	switch event.Type {
	// 收到删除事件说明被监听的 Pod 已消失，视作 NotFound 交由调用方处理，
	// 避免调用方一直等待一个永远不会再出现的对象。
	case watch.Deleted:
		return false, errors.NewNotFound(schema.GroupResource{Resource: "pods"}, "")
	}
	switch t := event.Object.(type) {
	case *v1.Pod:
		switch t.Status.Phase {
		case v1.PodRunning:
			return true, nil
		// 终态相位（成功/失败）不可能再变为 Running，须单独告知调用方，
		// 使其能与"尚未开始运行"的普通 false 区分开来。
		case v1.PodFailed, v1.PodSucceeded:
			return false, ErrPodCompleted
		}
	}
	return false, nil
}

// PodCompleted 判断 Pod 是否已经运行结束：
//   - 返回 true 表示 Pod 已进入 Failed 或 Succeeded 终态；
//   - 返回 false 表示 Pod 尚未结束（可能仍在运行或未开始）；
//   - 其他异常情况（如对象被删除）返回相应错误。
func PodCompleted(event watch.Event) (bool, error) {
	switch event.Type {
	// 与 PodRunning 一致：删除事件意味着对象不复存在，返回 NotFound
	// 让调用方及时停止等待，而不是无限期地监听该 Pod。
	case watch.Deleted:
		return false, errors.NewNotFound(schema.GroupResource{Resource: "pods"}, "")
	}
	switch t := event.Object.(type) {
	case *v1.Pod:
		switch t.Status.Phase {
		// 只有成功与失败是终态，其余相位均视为尚未完成。
		case v1.PodFailed, v1.PodSucceeded:
			return true, nil
		}
	}
	return false, nil
}
