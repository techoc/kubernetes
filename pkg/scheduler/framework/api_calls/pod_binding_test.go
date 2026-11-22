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

package apicalls

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/klog/v2/ktesting"
)

// TestPodBindingCall_Execute 验证 Execute 通过客户端的 pods/binding 子资源提交绑定请求，
// 且发送的 Binding 对象与构造时一致。
func TestPodBindingCall_Execute(t *testing.T) {
	// 测试上下文携带 logger，失败时可输出带调用链的日志。
	_, ctx := ktesting.NewTestContext(t)
	// 构造期望发送的绑定对象：将 ns/pod 绑定到 node。
	binding := &v1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod",
			Namespace: "ns",
		},
		Target: v1.ObjectReference{
			Name: "node",
		},
	}

	// 用 fake 客户端拦截 create 动作，避免依赖真实 API Server。
	client := fake.NewClientset()
	// 记录 reactor 是否真的处理过 binding 子资源；防止测试空跑还显示通过。
	bound := false
	// 前置 reactor 优先于默认 reactor，返回 handled 可阻止请求落到 fake tracker。
	client.PrependReactor("create", "pods", func(action clienttesting.Action) (bool, runtime.Object, error) {
		createAction := action.(clienttesting.CreateActionImpl)
		// 同一资源上可能有其他 create（如事件），只关心 binding 子资源。
		if createAction.Subresource != "binding" {
			return false, nil, nil
		}
		bound = true

		// 用完整对象比对，而非只查名字，确保字段没有被丢失或改写。
		gotBinding := createAction.GetObject().(*v1.Binding)
		if diff := cmp.Diff(binding, gotBinding); diff != "" {
			t.Errorf("Execute() sent incorrect binding object (-want,+got):\n%s", diff)
		}
		return true, nil, nil
	})

	call := NewPodBindingCall(binding)
	// 正常路径下不应返回错误；返回错误说明调用链中有环节失败。
	if err := call.Execute(ctx, client); err != nil {
		t.Fatalf("Execute() returned unexpected error: %v", err)
	}
	// 没有触发 binding 请求，上面的断言就都无从谈起。
	if !bound {
		t.Error("Expected binding API to be called")
	}
}
