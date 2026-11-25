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

package admission

import "context"

// chainAdmissionHandler 是一个准入 handler，按准入链顺序执行准入控制。
//
// 链式处理器按顺序执行各插件，任一插件返回错误即短路。顺序因此具有语义：
// 变更类插件先于校验类插件运行，使后续插件看到已变更的对象。
type chainAdmissionHandler []Interface

// NewChainHandler 用一组 handler 创建链式 handler，供测试使用。
func NewChainHandler(handlers ...Interface) chainAdmissionHandler {
	return chainAdmissionHandler(handlers)
}

// Admit 使用 handler 链执行准入控制检查，并在遇到第一个错误时立即返回。
//
// 只有实现了 MutationInterface 的插件参与变更；校验类插件在此阶段被跳过。
// 跳过 Handles 不匹配的操作，避免插件为不关心的操作付出代价。
func (admissionHandler chainAdmissionHandler) Admit(ctx context.Context, a Attributes, o ObjectInterfaces) error {
	for _, handler := range admissionHandler {
		if !handler.Handles(a.GetOperation()) {
			continue
		}
		if mutator, ok := handler.(MutationInterface); ok {
			err := mutator.Admit(ctx, a, o)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Validate 使用 handler 链执行准入控制检查，并在遇到第一个错误时立即返回。
//
// 与 Admit 对称：这里只调用 ValidationInterface，变更已经在前一阶段完成。
// 立即返回首个错误，保证拒绝原因确定、不依赖后续插件的副作用。
func (admissionHandler chainAdmissionHandler) Validate(ctx context.Context, a Attributes, o ObjectInterfaces) error {
	for _, handler := range admissionHandler {
		if !handler.Handles(a.GetOperation()) {
			continue
		}
		if validator, ok := handler.(ValidationInterface); ok {
			err := validator.Validate(ctx, a, o)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Handles 在任意 handler 处理给定操作时返回 true。
//
// 只要有一个插件关心该操作就返回 true，使上层不会因链中混有不关心的插件而跳过整条链。
func (admissionHandler chainAdmissionHandler) Handles(operation Operation) bool {
	for _, handler := range admissionHandler {
		if handler.Handles(operation) {
			return true
		}
	}
	return false
}
