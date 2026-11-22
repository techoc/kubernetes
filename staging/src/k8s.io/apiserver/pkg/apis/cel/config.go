/*
Copyright 2023 The Kubernetes Authors.

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

// Package cel 定义 CEL 编译与求值的全局成本参数。
// 这些数值是 API 兼容性的一部分：调整它们会改变哪些表达式被视为“过于昂贵”，
// 从而影响已存对象的重新校验结果，因此必须按 KEP 流程谨慎变更。
package cel

const (
	// PerCallLimit specify the actual cost limit per CEL validation call
	// current PerCallLimit gives roughly 0.1 second for each expression validation call
	//
	// 单次 CEL 调用的硬上限，防止单个表达式耗尽 CPU。
	PerCallLimit = 1000000

	// RuntimeCELCostBudget is the overall cost budget for runtime CEL validation cost per ValidatingAdmissionPolicyBinding or CustomResource
	// current RuntimeCELCostBudget gives roughly 1 seconds for the validation
	//
	// 一次准入/对象校验所有表达式共享的总预算。
	RuntimeCELCostBudget = 10000000

	// RuntimeCELCostBudgetMatchConditions is the overall cost budget for runtime CEL validation cost on matchConditions per object with matchConditions
	// this is per webhook for validatingwebhookconfigurations and mutatingwebhookconfigurations or per ValidatingAdmissionPolicyBinding
	// current RuntimeCELCostBudgetMatchConditions gives roughly 1/4 seconds for the validation
	//
	// matchConditions 的预算单独计算，避免条件求值挤占真正的校验预算。
	RuntimeCELCostBudgetMatchConditions = 2500000

	// CheckFrequency configures the number of iterations within a comprehension to evaluate
	// before checking whether the function evaluation has been interrupted
	//
	// 每执行若干次迭代检查一次 context 取消，兼顾开销与响应性。
	CheckFrequency = 100

	// MaxRequestSizeBytes is the maximum size of a request to the API server
	// TODO(DangerOnTheRanger): wire in MaxRequestBodyBytes from apiserver/pkg/server/options/server_run_options.go to make this configurable
	// Note that even if server_run_options.go becomes configurable in the future, this cost constant should be fixed and it should be the max allowed request size for the server
	//
	// 成本估算假设的请求大小上限；用于推导字符串/列表遍历成本的上界。
	MaxRequestSizeBytes = int64(3 * 1024 * 1024)

	// MaxEvaluatedMessageExpressionSizeBytes represents the largest-allowable string generated
	// by a messageExpression field
	//
	// messageExpression 的输出会写入 API 错误响应，限制长度避免放大响应体。
	MaxEvaluatedMessageExpressionSizeBytes = 5 * 1024
)
