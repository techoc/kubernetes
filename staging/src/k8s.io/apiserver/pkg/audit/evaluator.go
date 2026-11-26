/*
Copyright 2021 The Kubernetes Authors.

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

// Package audit 定义审计策略评估接口：把请求的授权属性映射为最终生效的
// 审计级别、需省略的阶段以及是否记录 managedFields。
package audit

import (
	"k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

// RequestAuditConfig 是某个请求最终生效的审计配置。PolicyRuleEvaluator 依据
// 授权属性评估审计策略，返回适用于该请求的配置。
//
// RequestAuditConfig 是某个请求最终生效的审计配置。它是一次策略评估的
// 结果快照，调用方应在请求生命周期内复用它，避免重复评估。
type RequestAuditConfig struct {
	// 请求使用的审计级别（None/Metadata/Request/RequestResponse）。
	Level audit.Level

	// 需要从审计中省略的阶段，例如 RequestReceived；用于降低敏感或
	// 高噪声阶段的记录量。
	OmitStages []audit.Stage

	// 是否把请求与响应体中的 managed fields 从 API 审计日志中剔除。
	// 它们体积大且由服务端维护，通常对审计价值低。
	OmitManagedFields bool
}

// PolicyRuleEvaluator 暴露策略规则的评估接口。
//
// PolicyRuleEvaluator 将审计策略规则编译后的形式暴露给请求处理链路，
// 使 HTTP 层无需了解策略文件格式。
type PolicyRuleEvaluator interface {
	// EvaluatePolicyRule 依据给定的授权属性评估 apiserver 的审计策略，
	// 返回适用于该请求的审计配置。
	//
	// 根据请求的授权属性（用户、动词、资源、命名空间等）评估策略。
	// 实现必须是只读且可并发调用的，因为每个请求都会调用一次。
	EvaluatePolicyRule(authorizer.Attributes) RequestAuditConfig
}
