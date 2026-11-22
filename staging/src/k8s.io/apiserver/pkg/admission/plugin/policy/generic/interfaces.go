/*
Copyright 2024 The Kubernetes Authors.

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

// Package generic 定义动态准入插件（webhook 与 admission policy）共享的
// 抽象接口：Hook、Source、Dispatcher 与 Evaluator。具体插件只需提供这些
// 接口的实现，调度、同步与分发骨架由通用代码复用。
package generic

import (
	"context"

	"k8s.io/apiserver/pkg/admission"
)

// Hook represents a dynamic admission hook. The hook may be a webhook or a
// policy. For webhook, the Hook may describe how to contact the endpoint, expected
// cert, etc. For policies, the hook may describe a compiled policy-binding pair.
//
// Hook 表示一个动态准入钩子：既可能是 webhook，也可能是策略（policy）。
// 该接口刻意留空，是因为匹配逻辑的输入（规则、objectSelector、
// namespaceSelector、matchConditions）由各实现自行解释，通用层只负责
// 通过泛型参数把它们串起来。
type Hook interface {
	// All hooks are expected to contain zero or more match conditions, object
	// selectors, namespace selectors to help the dispatcher decide when to apply
	// the hook.
	//
	// Methods of matching logic is applied are specific to the hook and left up
	// to the implementation.
	//
	// 设计约定：所有 hook 都应携带零个或多个匹配条件、对象选择器和命名空间
	// 选择器，供调度器判断是否执行；具体匹配语义由实现决定，因此不在此处
	// 固化方法签名。
}

// Source can list dynamic admission plugins.
//
// Source 提供当前生效的 hook 列表，并负责与后端配置（如
// ValidatingAdmissionPolicy 资源或 webhook 配置）保持同步。
type Source[H Hook] interface {
	// Hooks returns the list of currently known admission hooks.
	//
	// 返回当前已知的 hook 快照。调用方不得依赖返回切片的可变性或顺序稳定性。
	Hooks() []H

	// Run the source. This method should be called only once at startup.
	//
	// Run 启动后台同步循环（例如 informer），仅应在启动时调用一次。
	Run(ctx context.Context) error

	// HasSynced returns true if the source has completed its initial sync.
	//
	// 初次同步完成前不应认为 hook 列表已可靠，否则可能漏掉本应生效的
	// 准入检查。
	HasSynced() bool
}

// Dispatcher dispatches evaluates an admission request against the currently
// active hooks returned by the source.
//
// Dispatcher 针对 Source 返回的一组 hook 评估一次准入请求，决定放行、
// 变更或拒绝。
type Dispatcher[H Hook] interface {
	// Start the dispatcher. This method should be called only once at startup.
	//
	// Start 启动后台任务（如编译 CEL 表达式缓存），仅应在启动时调用一次。
	Start(ctx context.Context) error

	// Dispatch a request to the policies. Dispatcher may choose not to
	// call a hook, either because the rules of the hook does not match, or
	// the namespaceSelector or the objectSelector of the hook does not
	// match. A non-nil error means the request is rejected.
	//
	// Dispatch 将请求分发给 hook 逐一评估。实现可因规则、namespaceSelector
	// 或 objectSelector 不匹配而跳过某个 hook；返回非 nil error 表示请求
	// 被拒绝，调用方必须将其转换为对应的 API 错误。
	Dispatch(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces, hooks []H) error
}

// An evaluator represents a compiled CEL expression that can be evaluated a
// given a set of inputs used by the generic PolicyHook for Mutating and
// ValidatingAdmissionPolicy.
// Mutating and Validating may have different forms of evaluators
//
// Evaluator 表示已编译的 CEL 表达式。Mutating 与 Validating 策略的输入和
// 副作用模型不同，因此接口本身不定义方法，由各自的实现扩展。
type Evaluator interface {
}
