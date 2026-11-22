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

// Package testing 提供准入插件的测试辅助工具，目前主要是幂等性
// （reinvocation）检查器。注意包名 testing 与标准库同名，导入时通常
// 需要别名。
package testing

import (
	"context"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/admission"
)

// WithReinvocationTesting wraps a mutating admission handler and reinvokes it each time Admit is
// called. It checks the admission output object and reports a test error if the admission handler
// performs non-idempotent mutatations to the object.
//
// 背景：apiserver 在其他插件变更对象后会再次调用 mutating 插件，非幂等的
// 变更会导致结果不可预测，因此用该包装器在单测中强制暴露这类 bug。
func WithReinvocationTesting(t *testing.T, admission admission.MutationInterface) admission.MutationInterface {
	return &reinvoker{t, admission}
}

// reinvoker 持有原始插件与测试句柄。
type reinvoker struct {
	t         *testing.T
	admission admission.MutationInterface
}

// Admit reinvokes the admission handler and reports a test error if the admission handler performs
// non-idempotent mutatations to the admission object.
//
// 两次调用之间会把对象替换为 DeepCopy 后的副本，确保插件不能依赖指针
// 身份或对象内部缓存，模拟真实 reinvocation 时的“往返拷贝”语义。
func (r *reinvoker) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	r.t.Helper()
	outputs := []runtime.Object{}
	for i := 0; i < 2; i++ {
		err := r.admission.Admit(ctx, a, o)
		if err != nil {
			return err
		}
		if a.GetObject() != nil {
			// keep a copy of the output for subsequent idempotency checking
			outputs = append(outputs, a.GetObject().DeepCopyObject())
			// replace a.GetObject() with a copy of itself to make sure admission is safe to reinvoke with a round-tripped copy (no pointer comparisons are done)
			if deepCopyInto, ok := reflect.TypeOf(a.GetObject()).MethodByName("DeepCopyInto"); ok {
				deepCopyInto.Func.Call([]reflect.Value{
					reflect.ValueOf(a.GetObject().DeepCopyObject()),
					reflect.ValueOf(a.GetObject()),
				})
			}
		}
	}
	for i := 1; i < len(outputs); i++ {
		// 用 Semantic.DeepEqual 而非 reflect.DeepEqual：Kubernetes 对象中
		// 零值与非 nil 空集合在语义上等价。
		if !apiequality.Semantic.DeepEqual(outputs[0], outputs[i]) {
			r.t.Errorf("expected mutating admission plugin to be idempontent, but got different results on reinvocation, diff:\n%s", cmp.Diff(outputs[0], outputs[i]))
		}
	}
	return nil
}

// Handles will return true if any of the admission andler handlers handle the given operation.
// 直接委托给被包装插件，保证包装器对调用方完全透明。
func (r *reinvoker) Handles(operation admission.Operation) bool {
	r.t.Helper()
	return r.admission.Handles(operation)
}
