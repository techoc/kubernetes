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

package testing

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestOriginalObjectCaptured 校验 Fake 在调用 reactor 前会深拷贝 Action 中的对象：
// reactor 对对象的修改只体现在返回值上，不应污染已记录到 f.actions 的原始动作。
//
// 这一不变量很重要：测试经常在 Invokes 返回后检查记录的动作，
// 若两者共享同一对象，就无法区分“客户端请求的内容”和“reactor 伪造的响应”。
func TestOriginalObjectCaptured(t *testing.T) {
	// 该 ReactionFunc 设置资源的 SelfLink。
	const testSelfLink = "some-value"
	reactors := []ReactionFunc{
		func(action Action) (bool, runtime.Object, error) {
			createAction := action.(CreateActionImpl)
			accessor, err := meta.Accessor(createAction.Object)
			if err != nil {
				return false, nil, err
			}

			// 设置资源上的任意字段。
			accessor.SetSelfLink(testSelfLink)

			return true, createAction.Object, nil
		},
	}

	// 用测试用 reactor 创建 Fake。
	f := &Fake{}
	for _, r := range reactors {
		f.AddReactor("", "", r)
	}

	// 构造测试资源。
	testResource := schema.GroupVersionResource{Group: "", Version: "test_version", Resource: "test_kind"}
	testObj := getArbitraryResource(testResource, "test_name", "test_namespace")

	// 创建 fake CreateAction。
	action := CreateActionImpl{
		Object: testObj,
	}

	// 执行反应链。
	ret, err := f.Invokes(action, nil)
	assert.NoError(t, err, "running Invokes failed")

	// 为返回的资源取元数据访问器。
	accessor, err := meta.Accessor(ret)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 校验返回的资源确实被 ReactionFunc 修改过。
	if accessor.GetSelfLink() != testSelfLink {
		t.Errorf("expected resource returned by Invokes to be modified by the ReactionFunc")
	}
	// 验证只执行了一个 action。
	if len(f.actions) != 1 {
		t.Errorf("expected 1 action to be executed")
		t.FailNow()
	}
	// 检查记录的 action 未被反应链修改。
	createAction := f.actions[0].(CreateActionImpl)
	accessor, err = meta.Accessor(createAction.Object)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if accessor.GetSelfLink() != "" {
		t.Errorf("expected Action recorded to not be modified by ReactionFunc but it was")
	}
}

// TestReactorChangesPersisted 校验 reactor 链内部的修改会依次传递：
// 第一个 reactor 写入的字段，第二个 reactor 必须能看到。
//
// 与 TestOriginalObjectCaptured 的区别在于这里的修改发生在 Invokes 内部的副本上，
// 因而可以在链中传播；但记录到 f.actions 的动作仍然保持原样。
func TestReactorChangesPersisted(t *testing.T) {
	// 该 ReactionFunc 设置资源的 SelfLink。
	const testSelfLink = "some-value"
	reactors := []ReactionFunc{
		func(action Action) (bool, runtime.Object, error) {
			createAction := action.(CreateActionImpl)
			accessor, err := meta.Accessor(createAction.Object)
			if err != nil {
				return false, nil, err
			}

			// 设置资源上的任意字段。
			accessor.SetSelfLink(testSelfLink)

			return false, createAction.Object, nil
		},
		func(action Action) (bool, runtime.Object, error) {
			createAction := action.(CreateActionImpl)
			accessor, err := meta.Accessor(createAction.Object)
			if err != nil {
				return false, nil, err
			}

			// 确认 selfLink 已被设为 testSelfLink。
			if accessor.GetSelfLink() != testSelfLink {
				t.Errorf("expected resource passed to second reactor to be modified by first reactor")
			}

			return true, createAction.Object, nil
		},
	}

	// 用测试用 reactor 创建 Fake。
	f := &Fake{}
	for _, r := range reactors {
		f.AddReactor("", "", r)
	}

	// 构造测试资源。
	testResource := schema.GroupVersionResource{Group: "", Version: "test_version", Resource: "test_kind"}
	testObj := getArbitraryResource(testResource, "test_name", "test_namespace")

	// 创建 fake CreateAction。
	action := CreateActionImpl{
		Object: testObj,
	}

	// 执行反应链。
	ret, err := f.Invokes(action, nil)
	assert.NoError(t, err, "running Invokes failed")

	// 为返回的资源取元数据访问器。
	accessor, err := meta.Accessor(ret)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 校验返回的资源确实被 ReactionFunc 修改过。
	if accessor.GetSelfLink() != testSelfLink {
		t.Errorf("expected resource returned by Invokes to be modified by the ReactionFunc")
	}
	// 验证只执行了一个 action。
	if len(f.actions) != 1 {
		t.Errorf("expected 1 action to be executed")
		t.FailNow()
	}
	// 检查记录的 action 未被反应链修改。
	createAction := f.actions[0].(CreateActionImpl)
	accessor, err = meta.Accessor(createAction.Object)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if accessor.GetSelfLink() != "" {
		t.Errorf("expected Action recorded to not be modified by ReactionFunc but it was")
	}
}
