/*
Copyright 2022 The Kubernetes Authors.

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

package validating

import (
	"net/http"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PolicyDecisionAction 是策略最终对请求采取的动作。
type PolicyDecisionAction string

const (
	// ActionAdmit 表示放行。
	ActionAdmit PolicyDecisionAction = "admit"
	// ActionDeny 表示拒绝。
	ActionDeny PolicyDecisionAction = "deny"
)

// PolicyDecisionEvaluation 记录决策的来源：正常通过、出错还是显式拒绝。
// 与 Action 分开是为了在 deny 时区分“表达式返回 false”和“表达式求值失败”。
type PolicyDecisionEvaluation string

const (
	EvalAdmit PolicyDecisionEvaluation = "admit"
	EvalError PolicyDecisionEvaluation = "error"
	EvalDeny  PolicyDecisionEvaluation = "deny"
)

// PolicyDecision contains the action determined from a cel evaluation along with metadata such as message, reason and duration
//
// 该结构会出现在审计与拒绝响应中，字段语义需保持稳定。
type PolicyDecision struct {
	// Action 最终动作。
	Action PolicyDecisionAction
	// Evaluation 决策来源（通过/出错/拒绝）。
	Evaluation PolicyDecisionEvaluation
	// Message 面向用户的说明。
	Message string
	// Reason 映射为 status 的 reason，进而决定 HTTP 状态码。
	Reason metav1.StatusReason
	// Elapsed CEL 求值耗时，用于预算与性能观测。
	Elapsed time.Duration
}

// PolicyAuditAnnotationAction 描述一条审计注解的处理结果。
type PolicyAuditAnnotationAction string

const (
	// AuditAnnotationActionPublish indicates that the audit annotation should be
	// published with the audit event.
	AuditAnnotationActionPublish PolicyAuditAnnotationAction = "publish"
	// AuditAnnotationActionError indicates that the valueExpression resulted
	// in an error.
	AuditAnnotationActionError PolicyAuditAnnotationAction = "error"
	// AuditAnnotationActionExclude indicates that the audit annotation should be excluded
	// because the valueExpression evaluated to null, or because FailurePolicy is Ignore
	// and the expression failed with a parse error, type check error, or runtime error.
	AuditAnnotationActionExclude PolicyAuditAnnotationAction = "exclude"
)

// PolicyAuditAnnotation 是一条待写入审计事件的注解及其求值结果。
type PolicyAuditAnnotation struct {
	Key     string
	Value   string
	Elapsed time.Duration
	Action  PolicyAuditAnnotationAction
	Error   string
}

// reasonToCode 把策略 reason 映射为 HTTP 状态码。
// 只允许 API 层可设置的 reason 进入这里，未知值兜底为 422，避免产生误导性的 5xx。
func reasonToCode(r metav1.StatusReason) int32 {
	switch r {
	case metav1.StatusReasonForbidden:
		return http.StatusForbidden
	case metav1.StatusReasonUnauthorized:
		return http.StatusUnauthorized
	case metav1.StatusReasonRequestEntityTooLarge:
		return http.StatusRequestEntityTooLarge
	case metav1.StatusReasonInvalid:
		return http.StatusUnprocessableEntity
	default:
		// It should not reach here since we only allow above reason to be set from API level
		return http.StatusUnprocessableEntity
	}
}
