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

package matchconditions

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

// MatchResult 汇总 matchConditions 的求值结论。
// Error 非空时由调用方按 failurePolicy 决定放行还是拒绝；
// FailedConditionName 仅用于诊断信息与审计，便于定位是哪条条件失败。
type MatchResult struct {
	Matches             bool
	Error               error
	FailedConditionName string
}

// Matcher 包含把 Evaluations 转换为是否匹配的布尔结论的逻辑。
type Matcher interface {
	// Match 用于把 CEL 求值结果转换为决策。
	// 多个条件之间是逻辑与：任一条件为 false 即整体不匹配；
	// 任一条件求值出错则立即返回错误，不再继续求值后续条件。
	Match(ctx context.Context, versionedAttr *admission.VersionedAttributes, versionedParams runtime.Object, authz authorizer.UnconditionalAuthorizer) MatchResult
}
