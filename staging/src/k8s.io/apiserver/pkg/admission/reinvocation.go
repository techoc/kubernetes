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

package admission

import "context"

// newReinvocationHandler 创建一个包装给定准入链的 handler，
// 并按各 webhook 的重入策略在需要时重新调用该链。
//
// 重入只发生在变更阶段；后续 webhook 可能修改了前序 webhook 关心的字段，
// 因此需要给它们一次基于最终对象的重新判断机会。
func newReinvocationHandler(admissionChain Interface) Interface {
	return &reinvoker{admissionChain}
}

// reinvoker 包装整条准入链，至多触发一次额外调用。
//
// 只重入一次（由 SetIsReinvoke 保证），防止互相修改的 webhook 之间
// 无限来回震荡；这是有意的收敛保证。
type reinvoker struct {
	admissionChain Interface
}

// Admit 使用被包装的准入链执行准入控制检查，并根据重入策略在需要时重新调用准入链。
// 插件应当检查准入属性的重入上下文是否符合其重入策略来决定是否重跑，
// 并在执行变更时更新重入上下文。
//
// 首次 Admit 出错立即返回；只有成功且上下文请求重入时才执行第二次。
// 第二次调用覆盖整条链，树内插件也会被重跑，因此它们必须保证幂等。
func (r *reinvoker) Admit(ctx context.Context, a Attributes, o ObjectInterfaces) error {
	if mutator, ok := r.admissionChain.(MutationInterface); ok {
		err := mutator.Admit(ctx, a, o)
		if err != nil {
			return err
		}
		s := a.GetReinvocationContext()
		if s.ShouldReinvoke() {
			// 先标记再调用：即便第二次调用中再次设置请求，也不会形成第三次调用。
			s.SetIsReinvoke()
			// 第二次调用 Admit 会重新调用所有树内插件，
			// 以及根据重入策略需要重入的 webhook 插件。
			return mutator.Admit(ctx, a, o)
		}
	}
	return nil
}

// Validate 使用被包装的准入链执行准入控制检查，并在遇到第一个错误时立即返回。
//
// 校验阶段不做重入——校验只读对象，不会引入需要重新判断的变更。
func (r *reinvoker) Validate(ctx context.Context, a Attributes, o ObjectInterfaces) error {
	if validator, ok := r.admissionChain.(ValidationInterface); ok {
		return validator.Validate(ctx, a, o)
	}
	return nil
}

// Handles 在准入链中任意 handler 处理给定操作时返回 true。
//
// 直接委托，包装层不改变操作支持集合。
func (r *reinvoker) Handles(operation Operation) bool {
	return r.admissionChain.Handles(operation)
}
