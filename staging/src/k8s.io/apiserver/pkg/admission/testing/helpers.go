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

// WithReinvocationTesting 包装一个 mutating 准入处理器，每次调用 Admit 时都会重复调用它，
// 并比对准入输出的对象：如果处理器对对象做了非幂等的变更，就上报测试错误。
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

// Admit 会重复调用准入处理器，若处理器对准入对象做了非幂等的变更则上报测试错误。
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
			// 保留一份输出副本，供后续幂等性比对使用
			outputs = append(outputs, a.GetObject().DeepCopyObject())
			// 用对象自身的副本替换 a.GetObject()，确保重复准入调用对「往返拷贝」后的对象依然安全（不做指针比较）
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

// Handles 只要被包装的准入处理器中有一个能处理该操作就返回 true，这里直接委托，
// 保证包装器对调用方完全透明。
func (r *reinvoker) Handles(operation admission.Operation) bool {
	r.t.Helper()
	return r.admission.Handles(operation)
}
