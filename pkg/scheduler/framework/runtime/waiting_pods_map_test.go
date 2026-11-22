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
	"testing"
	"time"

	st "k8s.io/kubernetes/pkg/scheduler/testing"
)

// TestWaitingPodReturnFalseOnAllowed 校验 Pod 被放行后，抢占与拒绝操作都不应成功。
func TestWaitingPodReturnFalseOnAllowed(t *testing.T) {
	tests := []struct {
		name       string
		action     func(wp *waitingPod) bool
		actionName string
	}{
		{
			name: "Preempt returns false on allowed pod",
			action: func(wp *waitingPod) bool {
				return wp.Preempt("preemption-plugin", "preempted")
			},
			actionName: "Preempt",
		},
		{
			name: "Reject returns false on allowed pod",
			action: func(wp *waitingPod) bool {
				return wp.Reject("reject-plugin", "rejected")
			},
			actionName: "Reject",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pod := st.MakePod().Name("test-pod").UID("test-uid").Obj()
			wp := newWaitingPod(pod, map[string]time.Duration{"plugin1": 10 * time.Second})

			// 1. 模拟插件放行：一旦 Allow，Pod 的最终信号就已确定。
			wp.Allow("plugin1")

			// 2. 执行待测动作：放行后再抢占或拒绝都应无效。
			if success := tt.action(wp); success {
				t.Errorf("Expected %s to return false (failed), but it returned true (success)", tt.actionName)
			}

			// 3. 校验 Pod 收到的信号仍是 Allow 产生的 Success，未被后续动作改写。
			select {
			case status := <-wp.s:
				if !status.IsSuccess() {
					t.Errorf("Expected Pod to stay Allowed (Success), but got status: %v", status)
				}
			default:
				t.Fatal("No status received")
			}
		})
	}
}

// TestWaitingPodMultipleActions 校验放行信号被消费后，后续拒绝与抢占均告失败。
func TestWaitingPodMultipleActions(t *testing.T) {
	pod := st.MakePod().Name("test-pod").UID("test-uid").Obj()
	wp := newWaitingPod(pod, map[string]time.Duration{"plugin1": 10 * time.Second})
	startWaitOnPermit := make(chan struct{})
	endWaitOnPermit := make(chan struct{})

	// 用 goroutine 模拟 WaitOnPermit 消费信号，确认状态何时被读取。
	go func() {
		close(startWaitOnPermit)
		<-wp.s
		close(endWaitOnPermit)
	}()

	<-startWaitOnPermit
	// 1. 插件放行，信号应被 WaitOnPermit 消费，随后通道不再有接收者。
	wp.Allow("plugin1")
	<-endWaitOnPermit

	// 2. 此时再拒绝：没有接收者，且状态已终结，应返回失败。
	res := wp.Reject("plugin2", "rejected")
	if res {
		t.Fatalf("Expected reject to fail, but it succeeded")
	}

	// 3. 同理，抢占也不应成功。
	res = wp.Preempt("preemption-plugin", "preempted")
	if res {
		t.Fatalf("Expected preempt to fail, but it succeeded")
	}
}
