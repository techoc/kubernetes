/*
Copyright 2015 The Kubernetes Authors.

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

package admission

import (
	"context"
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// FakeHandler 是可配置接受/拒绝行为的假准入插件，用于验证链式调用顺序与短路逻辑。
// admitCalled/validateCalled 记录被调用的阶段，测试据此断言“准入阶段不会触发校验阶段”等不变量。
type FakeHandler struct {
	*Handler
	name                     string
	admit, admitCalled       bool
	validate, validateCalled bool
}

// Admit 记录本次调用，并根据 admit 开关决定接受或拒绝。
func (h *FakeHandler) Admit(ctx context.Context, a Attributes, o ObjectInterfaces) (err error) {
	h.admitCalled = true
	if h.admit {
		return nil
	}
	return fmt.Errorf("Don't admit")
}

// Validate 记录本次调用，并根据 validate 开关决定接受或拒绝。
func (h *FakeHandler) Validate(ctx context.Context, a Attributes, o ObjectInterfaces) (err error) {
	h.validateCalled = true
	if h.validate {
		return nil
	}
	return fmt.Errorf("Don't validate")
}

// makeHandler 构造一个同时处理 admit 与 validate、且行为一致的假插件；
// ops 指定它声明支持的 Operation，未声明的操作会被链直接跳过。
func makeHandler(name string, accept bool, ops ...Operation) *FakeHandler {
	return &FakeHandler{
		name:     name,
		admit:    accept,
		validate: accept,
		Handler:  NewHandler(ops...),
	}
}

// TestAdmitAndValidate 验证链式准入的几条核心语义：
// 只调用声明支持该 Operation 的插件；某一插件拒绝后立即短路，后续插件不再被调用；
// 且 Admit 与 Validate 两个阶段互不串扰。
func TestAdmitAndValidate(t *testing.T) {
	sysns := metav1.NamespaceSystem
	otherns := "default"
	// calls 精确描述每个用例中“应当被调用的插件”，用来检测漏调用与多调用。
	tests := []struct {
		name      string
		ns        string
		operation Operation
		options   runtime.Object
		chain     chainAdmissionHandler
		accept    bool
		calls     map[string]bool
	}{
		{
			// 所有插件都支持 Create，全部按序执行并接受。
			name:      "all accept",
			ns:        sysns,
			operation: Create,
			options:   &metav1.CreateOptions{},
			chain: []Interface{
				makeHandler("a", true, Update, Delete, Create),
				makeHandler("b", true, Delete, Create),
				makeHandler("c", true, Create),
			},
			calls:  map[string]bool{"a": true, "b": true, "c": true},
			accept: true,
		},
		{
			// b 不声明 Create，应被忽略；a、c 正常执行。
			name:      "ignore handler",
			ns:        otherns,
			operation: Create,
			options:   &metav1.CreateOptions{},
			chain: []Interface{
				makeHandler("a", true, Update, Delete, Create),
				makeHandler("b", false, Delete),
				makeHandler("c", true, Create),
			},
			calls:  map[string]bool{"a": true, "c": true},
			accept: true,
		},
		{
			// 没有插件声明 Connect，链为空，直接放行。
			name:      "ignore all",
			ns:        sysns,
			operation: Connect,
			options:   nil,
			chain: []Interface{
				makeHandler("a", true, Update, Delete, Create),
				makeHandler("b", false, Delete),
				makeHandler("c", true, Create),
			},
			calls:  map[string]bool{},
			accept: true,
		},
		{
			// b 拒绝 Delete 后短路，c 本就不支持 Delete，因此 c 不应被调用。
			name:      "reject one",
			ns:        otherns,
			operation: Delete,
			options:   &metav1.DeleteOptions{},
			chain: []Interface{
				makeHandler("a", true, Update, Delete, Create),
				makeHandler("b", false, Delete),
				makeHandler("c", true, Create),
			},
			calls:  map[string]bool{"a": true, "b": true},
			accept: false,
		},
	}
	for _, test := range tests {
		t.Logf("testcase = %s", test.name)
		// call admit and check that validate was not called at all
		// 第一阶段：只跑 Admit，同时确认没有任何插件误触发 Validate。
		err := test.chain.Admit(context.TODO(), NewAttributesRecord(nil, nil, schema.GroupVersionKind{}, test.ns, "", schema.GroupVersionResource{}, "", test.operation, test.options, false, nil), nil)
		accepted := (err == nil)
		if accepted != test.accept {
			t.Errorf("unexpected result of admit call: %v", accepted)
		}
		for _, h := range test.chain {
			fake := h.(*FakeHandler)
			_, shouldBeCalled := test.calls[fake.name]
			if shouldBeCalled != fake.admitCalled {
				t.Errorf("admit handler %s not called as expected: %v", fake.name, fake.admitCalled)
				continue
			}
			if fake.validateCalled {
				t.Errorf("validate handler %s called during admit", fake.name)
			}

			// reset value for validation test
			fake.admitCalled = false
		}

		// call validate and check that admit was not called at all
		// 第二阶段：只跑 Validate，同样确认 Admit 未被误触发。
		err = test.chain.Validate(context.TODO(), NewAttributesRecord(nil, nil, schema.GroupVersionKind{}, test.ns, "", schema.GroupVersionResource{}, "", test.operation, test.options, false, nil), nil)
		accepted = (err == nil)
		if accepted != test.accept {
			t.Errorf("unexpected result of validate call: %v\n", accepted)
		}
		for _, h := range test.chain {
			fake := h.(*FakeHandler)

			_, shouldBeCalled := test.calls[fake.name]
			if shouldBeCalled != fake.validateCalled {
				t.Errorf("validate handler %s not called as expected: %v", fake.name, fake.validateCalled)
				continue
			}

			if fake.admitCalled {
				t.Errorf("mutating handler unexpectedly called: %s", fake.name)
			}
		}
	}
}

// TestHandles 验证 chainAdmissionHandler.Handles 的聚合语义：
// 只要有任一插件声明支持该 Operation 即为 true。
func TestHandles(t *testing.T) {
	chain := chainAdmissionHandler{
		makeHandler("a", true, Update, Delete, Create),
		makeHandler("b", true, Delete, Create),
		makeHandler("c", true, Create),
	}

	tests := []struct {
		name      string
		operation Operation
		chain     chainAdmissionHandler
		expected  bool
	}{
		{
			name:      "all handle",
			operation: Create,
			expected:  true,
		},
		{
			name:      "none handle",
			operation: Connect,
			expected:  false,
		},
		{
			name:      "some handle",
			operation: Delete,
			expected:  true,
		},
	}
	for _, test := range tests {
		handles := chain.Handles(test.operation)
		if handles != test.expected {
			t.Errorf("Unexpected handles result. Expected: %v. Actual: %v", test.expected, handles)
		}
	}
}
