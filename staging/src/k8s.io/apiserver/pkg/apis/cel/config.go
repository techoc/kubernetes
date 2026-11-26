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
	// PerCallLimit 指定单次 CEL 校验调用的实际成本上限。
	// 当前取值约为每次表达式校验 0.1 秒。
	//
	// 单次 CEL 调用的硬上限，防止单个表达式耗尽 CPU。
	PerCallLimit = 1000000

	// RuntimeCELCostBudget 是每次 ValidatingAdmissionPolicyBinding 或 CustomResource 校验的运行时 CEL 总成本预算。
	// 当前取值约为校验 1 秒。
	//
	// 一次准入/对象校验所有表达式共享的总预算。
	RuntimeCELCostBudget = 10000000

	// RuntimeCELCostBudgetMatchConditions 是每个带 matchConditions 对象在 matchConditions 上的运行时 CEL 总成本预算。
	// 对 validatingwebhookconfigurations 和 mutatingwebhookconfigurations 按 webhook 计算，对 ValidatingAdmissionPolicyBinding 按绑定计算。
	// 当前取值约为校验 1/4 秒。
	//
	// matchConditions 的预算单独计算，避免条件求值挤占真正的校验预算。
	RuntimeCELCostBudgetMatchConditions = 2500000

	// CheckFrequency 配置在检查函数求值是否被中断之前，需要在推导式中执行的迭代次数。
	// 每隔若干次迭代检查一次取消状态，避免每次迭代都付出检查开销。
	//
	// 每执行若干次迭代检查一次 context 取消，兼顾开销与响应性。
	CheckFrequency = 100

	// MaxRequestSizeBytes 是发送到 API server 的请求的最大尺寸。
	// TODO(DangerOnTheRanger): 接入 apiserver/pkg/server/options/server_run_options.go 中的 MaxRequestBodyBytes，使其可配置。
	// 注意：即便将来 server_run_options.go 变为可配置，本成本常量也应保持固定，并作为服务端允许的最大请求尺寸。
	//
	// 成本估算假设的请求大小上限；用于推导字符串/列表遍历成本的上界。
	MaxRequestSizeBytes = int64(3 * 1024 * 1024)

	// MaxEvaluatedMessageExpressionSizeBytes 表示 messageExpression 字段允许生成的最大字符串长度。
	// 限制生成消息长度，避免错误响应被撑大。
	//
	// messageExpression 的输出会写入 API 错误响应，限制长度避免放大响应体。
	MaxEvaluatedMessageExpressionSizeBytes = 5 * 1024
)
