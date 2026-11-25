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
	"context"

	celgo "github.com/google/cel-go/cel"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/plugin/cel"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

// 编译期断言：ValidationCondition 必须满足 CEL 表达式访问器接口。
var _ cel.ExpressionAccessor = &ValidationCondition{}

// ValidationCondition 包含编译、求值并校验一个 CEL 表达式所需的输入。
//
// 对应 ValidatingAdmissionPolicy 中的 validation 条目；表达式必须返回 bool。
type ValidationCondition struct {
	Expression string
	Message    string
	Reason     *metav1.StatusReason
}

// GetExpression 返回待编译/求值的 CEL 源码。
func (v *ValidationCondition) GetExpression() string {
	return v.Expression
}

// ReturnTypes 限定校验表达式只能返回 bool，便于编译期类型检查。
func (v *ValidationCondition) ReturnTypes() []*celgo.Type {
	return []*celgo.Type{celgo.BoolType}
}

// AuditAnnotationCondition 包含编译、求值并发布一个 CEL 审计注解所需的输入。
//
// 值表达式允许返回 null：null 表示“不发布”，因此返回类型集合包含 NullType。
type AuditAnnotationCondition struct {
	Key             string
	ValueExpression string
}

// GetExpression 返回注解值表达式的 CEL 源码。
func (v *AuditAnnotationCondition) GetExpression() string {
	return v.ValueExpression
}

// ReturnTypes 限定注解值表达式返回 string 或 null。
func (v *AuditAnnotationCondition) ReturnTypes() []*celgo.Type {
	return []*celgo.Type{celgo.StringType, celgo.NullType}
}

// Variable 是用于组合复用的具名表达式。
//
// 变量用于在多个校验/注解表达式间复用中间结果，避免重复计算。
type Variable struct {
	Name       string
	Expression string
}

// GetExpression 返回变量定义的 CEL 源码。
func (v *Variable) GetExpression() string {
	return v.Expression
}

// ReturnTypes 变量不限制具体类型，接受 any/dyn。
func (v *Variable) ReturnTypes() []*celgo.Type {
	return []*celgo.Type{celgo.AnyType, celgo.DynType}
}

// GetName 返回变量在 CEL 环境中的绑定名。
func (v *Variable) GetName() string {
	return v.Name
}

// ValidateResult 定义 Validator.Validate 操作的结果。
type ValidateResult struct {
	// Decisions 给出校验结论以及该决策的详细信息。
	Decisions []PolicyDecision
	// AuditAnnotations 指定本次校验需要记录的审计注解。
	AuditAnnotations []PolicyAuditAnnotation
}

// Validator 包含把 ValidationEvaluation 转换为 PolicyDecision 的逻辑。
type Validator interface {
	// Validate 用于把 CEL 求值结果转换为决策。
	// runtimeCELCostBudget 仅为测试目的而加；调用方应始终使用
	// k8s.io/apiserver/pkg/apis/cel/config.go 中的常量 RuntimeCELCostBudget 作为入参。
	Validate(ctx context.Context, matchedResource schema.GroupVersionResource, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, namespace *corev1.Namespace, runtimeCELCostBudget int64, authz authorizer.UnconditionalAuthorizer) ValidateResult
	// CompileError 返回 CEL 编译阶段的错误；编译成功时返回 nil。
	//
	// 编译失败时 Validate 无法给出可靠结果，调用方必须先检查此方法再决定如何处理请求。
	CompileError() error
}
