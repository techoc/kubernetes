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

package validation

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	celgo "github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/operators"
	exprpb "google.golang.org/genproto/googleapis/api/expr/v1alpha1"

	v1 "k8s.io/api/authorization/v1"
	"k8s.io/api/authorization/v1beta1"
	"k8s.io/apimachinery/pkg/util/sets"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	api "k8s.io/apiserver/pkg/apis/apiserver"
	authenticationcel "k8s.io/apiserver/pkg/authentication/cel"
	authorizationcel "k8s.io/apiserver/pkg/authorization/cel"
	"k8s.io/apiserver/pkg/cel"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/util/cert"
)

// ValidateAuthenticationConfiguration 校验给定的 AuthenticationConfiguration。
//
// 校验分两类：结构约束（字段互斥、重复项、数量上限）与 CEL 表达式编译。
// disallowedIssuers 来自启动参数，防止动态配置的 issuer 与内置认证器（如 service account）
// 重叠而被用来伪造身份。
func ValidateAuthenticationConfiguration(compiler authenticationcel.Compiler, c *api.AuthenticationConfiguration, disallowedIssuers []string) field.ErrorList {
	root := field.NewPath("jwt")
	var allErrs field.ErrorList

	// 允许配置 0 个认证器：支持 apiserver 初始无认证器、
	// 后续再通过动态配置添加认证器的场景。

	// 64 是硬上限，避免恶意配置放大每个请求的认证开销。
	if len(c.JWT) > 64 {
		allErrs = append(allErrs, field.TooMany(root, len(c.JWT), 64))
		return allErrs
	}

	seenIssuers := sets.New[string]()
	seenDiscoveryURLs := sets.New[string]()
	for i, a := range c.JWT {
		fldPath := root.Index(i)
		_, errs := validateJWTAuthenticator(compiler, a, fldPath, sets.New(disallowedIssuers...), utilfeature.DefaultFeatureGate.Enabled(features.StructuredAuthenticationConfigurationEgressSelector))
		allErrs = append(allErrs, errs...)

		// issuer URL 重复会让同一 token 被多个认证器处理，语义不明确，直接拒绝。
		if seenIssuers.Has(a.Issuer.URL) {
			allErrs = append(allErrs, field.Duplicate(fldPath.Child("issuer").Child("url"), a.Issuer.URL))
		}
		seenIssuers.Insert(a.Issuer.URL)

		if len(a.Issuer.DiscoveryURL) > 0 {
			if seenDiscoveryURLs.Has(a.Issuer.DiscoveryURL) {
				allErrs = append(allErrs, field.Duplicate(fldPath.Child("issuer").Child("discoveryURL"), a.Issuer.DiscoveryURL))
			}
			seenDiscoveryURLs.Insert(a.Issuer.DiscoveryURL)
		}
	}

	// 匿名认证的 conditions 只在 enabled=true 时才有意义，否则配置意图自相矛盾。
	if c.Anonymous != nil {
		if !c.Anonymous.Enabled && len(c.Anonymous.Conditions) > 0 {
			allErrs = append(allErrs, field.Invalid(field.NewPath("anonymous", "conditions"), c.Anonymous.Conditions, "enabled should be set to true when conditions are defined"))
		}
	}

	return allErrs
}

// CompileAndValidateJWTAuthenticator 校验给定的 JWTAuthenticator，返回包含已编译 CEL 表达式
// （用于 claim 映射与校验规则）的 CELMapper。导出供 oidc 包使用。
//
// 返回的 CELMapper 可在请求路径上复用，避免每次认证都重新编译表达式。
func CompileAndValidateJWTAuthenticator(compiler authenticationcel.Compiler, authenticator api.JWTAuthenticator, disallowedIssuers []string) (authenticationcel.CELMapper, field.ErrorList) {
	return validateJWTAuthenticator(compiler, authenticator, nil, sets.New(disallowedIssuers...), utilfeature.DefaultFeatureGate.Enabled(features.StructuredAuthenticationConfigurationEgressSelector))
}

// validateJWTAuthenticator 是单个 JWT 认证器的校验主干：
// issuer → claimValidationRules → claimMappings → userValidationRules。
// 顺序有意义：claim 映射的编译结果决定后续 email/email_verified 一致性检查。
func validateJWTAuthenticator(compiler authenticationcel.Compiler, authenticator api.JWTAuthenticator, fldPath *field.Path, disallowedIssuers sets.Set[string], structuredAuthnEgressSelectorFeatureEnabled bool) (authenticationcel.CELMapper, field.ErrorList) {
	var allErrs field.ErrorList

	state := &validationState{}

	allErrs = append(allErrs, validateIssuer(authenticator.Issuer, disallowedIssuers, fldPath.Child("issuer"), structuredAuthnEgressSelectorFeatureEnabled)...)
	allErrs = append(allErrs, validateClaimValidationRules(compiler, state, authenticator.ClaimValidationRules, fldPath.Child("claimValidationRules"))...)
	allErrs = append(allErrs, validateClaimMappings(compiler, state, authenticator.ClaimMappings, fldPath.Child("claimMappings"))...)
	allErrs = append(allErrs, validateUserValidationRules(compiler, state, authenticator.UserValidationRules, fldPath.Child("userValidationRules"))...)

	return state.mapper, allErrs
}

// validationState 在一次校验中累积编译产物与跨字段依赖标记。
//
// usesEmailClaim / usesEmailVerifiedClaim 用于检查：一旦用 email 作为用户名，
// 就必须同时校验 email_verified，否则未验证邮箱可冒充任意用户。
type validationState struct {
	mapper                 authenticationcel.CELMapper
	usesEmailClaim         bool
	usesEmailVerifiedClaim bool
}

// validateIssuer 校验 issuer 块的各个字段。
func validateIssuer(issuer api.Issuer, disallowedIssuers sets.Set[string], fldPath *field.Path, structuredAuthnEgressSelectorFeatureEnabled bool) field.ErrorList {
	var allErrs field.ErrorList

	allErrs = append(allErrs, validateIssuerURL(issuer.URL, disallowedIssuers, fldPath.Child("url"))...)
	allErrs = append(allErrs, validateIssuerDiscoveryURL(issuer.URL, issuer.DiscoveryURL, fldPath.Child("discoveryURL"))...)
	allErrs = append(allErrs, validateAudiences(issuer.Audiences, issuer.AudienceMatchPolicy, fldPath.Child("audiences"), fldPath.Child("audienceMatchPolicy"))...)
	allErrs = append(allErrs, validateCertificateAuthority(issuer.CertificateAuthority, fldPath.Child("certificateAuthority"))...)
	allErrs = append(allErrs, validateEgressSelector(issuer.EgressSelectorType, fldPath.Child("egressSelectorType"), structuredAuthnEgressSelectorFeatureEnabled)...)

	return allErrs
}

// validateIssuerURL 要求 issuer URL 非空，再做通用 URL 校验。
func validateIssuerURL(issuerURL string, disallowedIssuers sets.Set[string], fldPath *field.Path) field.ErrorList {
	if len(issuerURL) == 0 {
		return field.ErrorList{field.Required(fldPath, "")}
	}

	return validateURL(issuerURL, disallowedIssuers, fldPath)
}

// validateIssuerDiscoveryURL 校验可选的 discoveryURL：不能与 issuer URL 相同，
// 否则等价于没有覆盖，且容易让运维误以为已切换发现端点。
func validateIssuerDiscoveryURL(issuerURL, issuerDiscoveryURL string, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if len(issuerDiscoveryURL) == 0 {
		return nil
	}

	if len(issuerURL) > 0 && strings.TrimRight(issuerURL, "/") == strings.TrimRight(issuerDiscoveryURL, "/") {
		allErrs = append(allErrs, field.Invalid(fldPath, issuerDiscoveryURL, "discoveryURL must be different from URL"))
	}

	// discoveryURL 不是 issuer URL，无需按禁用 issuer 列表校验。
	allErrs = append(allErrs, validateURL(issuerDiscoveryURL, nil, fldPath)...)
	return allErrs
}

// validateURL 是 issuer 类 URL 的通用校验：必须 https、不得带凭据/查询串/片段。
// 禁止 userinfo 与 query 是为了避免凭据泄漏和缓存键歧义。
func validateURL(issuerURL string, disallowedIssuers sets.Set[string], fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if disallowedIssuers.Has(issuerURL) {
		allErrs = append(allErrs, field.Invalid(fldPath, issuerURL, fmt.Sprintf("URL must not overlap with disallowed issuers: %s", sets.List(disallowedIssuers))))
	}

	u, err := url.Parse(issuerURL)
	if err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, issuerURL, err.Error()))
		return allErrs
	}
	if u.Scheme != "https" {
		allErrs = append(allErrs, field.Invalid(fldPath, issuerURL, "URL scheme must be https"))
	}
	if u.User != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, issuerURL, "URL must not contain a username or password"))
	}
	if len(u.RawQuery) > 0 {
		allErrs = append(allErrs, field.Invalid(fldPath, issuerURL, "URL must not contain a query"))
	}
	if len(u.Fragment) > 0 {
		allErrs = append(allErrs, field.Invalid(fldPath, issuerURL, "URL must not contain a fragment"))
	}

	return allErrs
}

// validateAudiences 校验 audience 列表及匹配策略：
// 多 audience 必须显式声明 MatchAny，单 audience 则不允许声明其他策略。
func validateAudiences(audiences []string, audienceMatchPolicy api.AudienceMatchPolicyType, fldPath, audienceMatchPolicyFldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if len(audiences) == 0 {
		allErrs = append(allErrs, field.Required(fldPath, fmt.Sprintf(atLeastOneRequiredErrFmt, fldPath)))
		return allErrs
	}

	seenAudiences := sets.NewString()
	for i, audience := range audiences {
		fldPath := fldPath.Index(i)
		if len(audience) == 0 {
			allErrs = append(allErrs, field.Required(fldPath, ""))
		}
		if seenAudiences.Has(audience) {
			allErrs = append(allErrs, field.Duplicate(fldPath, audience))
		}
		seenAudiences.Insert(audience)
	}

	if len(audiences) > 1 && audienceMatchPolicy != api.AudienceMatchPolicyMatchAny {
		allErrs = append(allErrs, field.Invalid(audienceMatchPolicyFldPath, audienceMatchPolicy, "audienceMatchPolicy must be MatchAny for multiple audiences"))
	}
	if len(audiences) == 1 && (len(audienceMatchPolicy) > 0 && audienceMatchPolicy != api.AudienceMatchPolicyMatchAny) {
		allErrs = append(allErrs, field.Invalid(audienceMatchPolicyFldPath, audienceMatchPolicy, "audienceMatchPolicy must be empty or MatchAny for single audience"))
	}

	return allErrs
}

// validateCertificateAuthority 校验 PEM 格式的 CA 证书能否解析为证书池；
// 错误信息中省略具体内容，避免把证书材料写进 API 错误响应。
func validateCertificateAuthority(certificateAuthority string, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	if len(certificateAuthority) == 0 {
		return allErrs
	}
	_, err := cert.NewPoolFromBytes([]byte(certificateAuthority))
	if err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, "<omitted>", err.Error()))
	}

	return allErrs
}

// validateEgressSelector 校验 egress 选择器：特性门控关闭时该字段不可用，
// 且只允许 controlplane 与 cluster 两种取值。
func validateEgressSelector(selectorType api.EgressSelectorType, fldPath *field.Path, structuredAuthnEgressSelectorFeatureEnabled bool) field.ErrorList {
	var allErrs field.ErrorList

	if len(selectorType) == 0 {
		return allErrs
	}

	if !structuredAuthnEgressSelectorFeatureEnabled {
		allErrs = append(allErrs, field.Invalid(fldPath, selectorType, "egress selector is not supported when StructuredAuthenticationConfigurationEgressSelector feature gate is disabled"))
	}

	switch selectorType {
	case api.EgressSelectorControlPlane, api.EgressSelectorCluster:
		// valid
	default:
		allErrs = append(allErrs, field.Invalid(fldPath, selectorType, "egress selector must be either controlplane or cluster"))
	}

	return allErrs
}

// validateClaimValidationRules 校验 claim 校验规则：claim 与 expression 二选一且互斥，
// 并对 expression 去重后编译，把结果写入 state.mapper 供运行时复用。
func validateClaimValidationRules(compiler authenticationcel.Compiler, state *validationState, rules []api.ClaimValidationRule, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	seenClaims := sets.NewString()
	seenExpressions := sets.NewString()
	var compilationResults []authenticationcel.CompilationResult

	for i, rule := range rules {
		fldPath := fldPath.Index(i)

		switch {
		case len(rule.Claim) > 0 && len(rule.Expression) > 0:
			allErrs = append(allErrs, field.Invalid(fldPath, rule.Claim, "claim and expression can't both be set"))
		case len(rule.Claim) == 0 && len(rule.Expression) == 0:
			allErrs = append(allErrs, field.Required(fldPath, "claim or expression is required"))
		case len(rule.Claim) > 0:
			// message 仅对 expression 形式有意义，claim 形式下的自定义信息无法生效。
			if len(rule.Message) > 0 {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("message"), rule.Message, "may not be specified when claim is set"))
			}
			if seenClaims.Has(rule.Claim) {
				allErrs = append(allErrs, field.Duplicate(fldPath.Child("claim"), rule.Claim))
			}
			seenClaims.Insert(rule.Claim)
		case len(rule.Expression) > 0:
			// requiredValue 只适用于 claim 形式。
			if len(rule.RequiredValue) > 0 {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("requiredValue"), rule.RequiredValue, "may not be specified when expression is set"))
			}
			if seenExpressions.Has(rule.Expression) {
				allErrs = append(allErrs, field.Duplicate(fldPath.Child("expression"), rule.Expression))
				continue
			}
			seenExpressions.Insert(rule.Expression)

			compilationResult, err := compileClaimsCELExpression(compiler, &authenticationcel.ClaimValidationCondition{
				Expression: rule.Expression,
				Message:    rule.Message,
			}, fldPath.Child("expression"))

			if err != nil {
				allErrs = append(allErrs, err)
				continue
			}
			if compilationResult != nil {
				compilationResults = append(compilationResults, *compilationResult)
			}
		}
	}

	if len(compilationResults) > 0 {
		state.mapper.ClaimValidationRules = authenticationcel.NewClaimsMapper(compilationResults)
		state.usesEmailVerifiedClaim = state.usesEmailVerifiedClaim || anyUsesEmailVerifiedClaim(compilationResults)
	}

	return allErrs
}

// validateClaimMappings 校验 username/groups/uid/extra 四类 claim 映射，
// 并在最后执行 email 与 email_verified 的联动检查。
func validateClaimMappings(compiler authenticationcel.Compiler, state *validationState, m api.ClaimMappings, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList

	compilationResult, err := validatePrefixClaimOrExpression(compiler, m.Username, fldPath.Child("username"), true)
	if err != nil {
		allErrs = append(allErrs, err...)
	} else if compilationResult != nil {
		// 记录 username 是否直接来自 email，用于后续强制 email_verified 校验。
		state.usesEmailClaim = state.usesEmailClaim || usesEmailClaim(compilationResult.AST)
		state.usesEmailVerifiedClaim = state.usesEmailVerifiedClaim || usesEmailVerifiedClaim(compilationResult.AST)
		state.mapper.Username = authenticationcel.NewClaimsMapper([]authenticationcel.CompilationResult{*compilationResult})
	}

	// groups 是可选项，因此 claimOrExpressionRequired=false。
	compilationResult, err = validatePrefixClaimOrExpression(compiler, m.Groups, fldPath.Child("groups"), false)
	if err != nil {
		allErrs = append(allErrs, err...)
	} else if compilationResult != nil {
		state.mapper.Groups = authenticationcel.NewClaimsMapper([]authenticationcel.CompilationResult{*compilationResult})
	}

	switch {
	case len(m.UID.Claim) > 0 && len(m.UID.Expression) > 0:
		allErrs = append(allErrs, field.Invalid(fldPath.Child("uid"), "", "claim and expression can't both be set"))
	case len(m.UID.Expression) > 0:
		compilationResult, err := compileClaimsCELExpression(compiler, &authenticationcel.ClaimMappingExpression{
			Expression: m.UID.Expression,
		}, fldPath.Child("uid").Child("expression"))

		if err != nil {
			allErrs = append(allErrs, err)
		} else if compilationResult != nil {
			state.mapper.UID = authenticationcel.NewClaimsMapper([]authenticationcel.CompilationResult{*compilationResult})
		}
	}

	var extraCompilationResults []authenticationcel.CompilationResult
	seenExtraKeys := sets.NewString()

	for i, mapping := range m.Extra {
		fldPath := fldPath.Child("extra").Index(i)
		// 额外属性键应带上使用它的认证器（或认证器/授权器对）的域名前缀，
		// 例如用 "example.org/foo" 而不是 "foo"。
		// xref: https://github.com/kubernetes/kubernetes/blob/3825e206cb162a7ad7431a5bdf6a065ae8422cf7/staging/src/k8s.io/apiserver/pkg/authentication/user/user.go#L31-L41
		// IsDomainPrefixedPath 校验键非空且带域名前缀。
		//
		// 额外属性键必须带域名前缀，避免不同认证器互相覆盖同名属性。
		allErrs = append(allErrs, utilvalidation.IsDomainPrefixedPath(fldPath.Child("key"), mapping.Key)...)
		if mapping.Key != strings.ToLower(mapping.Key) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("key"), mapping.Key, "must be lowercase"))
		}

		// k8s.io / kubernetes.io 前缀保留给 Kubernetes 自身，防止伪装成系统属性。
		if isKubernetesDomainPrefix(mapping.Key) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("key"), mapping.Key, "k8s.io, kubernetes.io and their subdomains are reserved for Kubernetes use"))
		}

		if seenExtraKeys.Has(mapping.Key) {
			allErrs = append(allErrs, field.Duplicate(fldPath.Child("key"), mapping.Key))
			continue
		}
		seenExtraKeys.Insert(mapping.Key)

		if len(mapping.ValueExpression) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("valueExpression"), ""))
			continue
		}

		compilationResult, err := compileClaimsCELExpression(compiler, &authenticationcel.ExtraMappingExpression{
			Key:        mapping.Key,
			Expression: mapping.ValueExpression,
		}, fldPath.Child("valueExpression"))

		if err != nil {
			allErrs = append(allErrs, err)
			continue
		}

		if compilationResult != nil {
			extraCompilationResults = append(extraCompilationResults, *compilationResult)
		}
	}

	if len(extraCompilationResults) > 0 {
		state.mapper.Extra = authenticationcel.NewClaimsMapper(extraCompilationResults)
		state.usesEmailVerifiedClaim = state.usesEmailVerifiedClaim || anyUsesEmailVerifiedClaim(extraCompilationResults)
	}

	// 安全不变式：username 取自 email 时，配置中必须在某处校验 email_verified，
	// 否则未验证的邮箱可被用来冒充他人。
	if state.usesEmailClaim && !state.usesEmailVerifiedClaim {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("username", "expression"), m.Username.Expression,
			"claims.email_verified must be used in claimMappings.username.expression or claimMappings.extra[*].valueExpression or claimValidationRules[*].expression when claims.email is used in claimMappings.username.expression"))
	}

	return allErrs
}

// isKubernetesDomainPrefix 判断键的域名前缀是否属于 Kubernetes 保留域。
func isKubernetesDomainPrefix(key string) bool {
	domainPrefix := getDomainPrefix(key)
	if domainPrefix == "kubernetes.io" || strings.HasSuffix(domainPrefix, ".kubernetes.io") {
		return true
	}
	if domainPrefix == "k8s.io" || strings.HasSuffix(domainPrefix, ".k8s.io") {
		return true
	}
	return false
}

// getDomainPrefix 取出 "domain/path" 形式的域名部分，没有斜杠则返回空串。
func getDomainPrefix(key string) string {
	if parts := strings.SplitN(key, "/", 2); len(parts) == 2 {
		return parts[0]
	}
	return ""
}

// usesEmailClaim 判断 AST 中是否出现 claims.email 选择表达式。
func usesEmailClaim(ast *celgo.Ast) bool {
	return hasSelectExp(ast.Expr(), "claims", "email")
}

// anyUsesEmailVerifiedClaim 在多个编译结果中查找是否使用 claims.email_verified。
func anyUsesEmailVerifiedClaim(results []authenticationcel.CompilationResult) bool {
	for _, result := range results {
		if usesEmailVerifiedClaim(result.AST) {
			return true
		}
	}
	return false
}

// usesEmailVerifiedClaim 判断 AST 中是否出现 claims.email_verified。
func usesEmailVerifiedClaim(ast *celgo.Ast) bool {
	return hasSelectExp(ast.Expr(), "claims", "email_verified")
}

// hasSelectExp 递归遍历 CEL AST，查找 "<operand>.<field>" 形式的字段访问。
//
// 同时识别 select 表达式与可选链 optSelect("claims", "email_verified")，
// 因为 CEL 编译器可能把可选访问表示成函数调用。任一子树命中即返回 true。
func hasSelectExp(exp *exprpb.Expr, operand, field string) bool {
	if exp == nil {
		return false
	}
	switch e := exp.ExprKind.(type) {
	case *exprpb.Expr_ConstExpr,
		*exprpb.Expr_IdentExpr:
		return false
	case *exprpb.Expr_SelectExpr:
		s := e.SelectExpr
		if s == nil {
			return false
		}
		if isIdentOperand(s.Operand, operand) && s.Field == field {
			return true
		}
		return hasSelectExp(s.Operand, operand, field)
	case *exprpb.Expr_CallExpr:
		c := e.CallExpr
		if c == nil {
			return false
		}
		// 可选访问 claims.?email_verified 会被编码成 optSelect(claims, "email_verified")。
		if c.Target == nil && c.Function == operators.OptSelect && len(c.Args) == 2 &&
			isIdentOperand(c.Args[0], operand) && isConstField(c.Args[1], field) {
			return true
		}
		for _, arg := range c.Args {
			if hasSelectExp(arg, operand, field) {
				return true
			}
		}
		return hasSelectExp(c.Target, operand, field)
	case *exprpb.Expr_ListExpr:
		l := e.ListExpr
		if l == nil {
			return false
		}
		for _, element := range l.Elements {
			if hasSelectExp(element, operand, field) {
				return true
			}
		}
		return false
	case *exprpb.Expr_StructExpr:
		s := e.StructExpr
		if s == nil {
			return false
		}
		for _, entry := range s.Entries {
			if hasSelectExp(entry.GetMapKey(), operand, field) {
				return true
			}
			if hasSelectExp(entry.Value, operand, field) {
				return true
			}
		}
		return false
	case *exprpb.Expr_ComprehensionExpr:
		c := e.ComprehensionExpr
		if c == nil {
			return false
		}
		return hasSelectExp(c.IterRange, operand, field) ||
			hasSelectExp(c.AccuInit, operand, field) ||
			hasSelectExp(c.LoopCondition, operand, field) ||
			hasSelectExp(c.LoopStep, operand, field) ||
			hasSelectExp(c.Result, operand, field)
	default:
		return false
	}
}

// isIdentOperand 判断表达式是否为指定名字的标识符。
func isIdentOperand(exp *exprpb.Expr, operand string) bool {
	if len(operand) == 0 {
		return false // sanity check against default values
	}
	id := exp.GetIdentExpr() // does not panic even if exp is nil
	return id != nil && id.Name == operand
}

// isConstField 判断表达式是否为指定字符串常量。
func isConstField(exp *exprpb.Expr, field string) bool {
	if len(field) == 0 {
		return false // sanity check against default values
	}
	c := exp.GetConstExpr()                        // does not panic even if exp is nil
	return c != nil && c.GetStringValue() == field // does not panic even if c is not a string
}

// validatePrefixClaimOrExpression 校验 "claim + prefix" 或 "expression" 二选一的映射写法，
// 并返回 expression 形式的编译结果。
// claimOrExpressionRequired 用于区分必填（username）与可选（groups）字段。
func validatePrefixClaimOrExpression(compiler authenticationcel.Compiler, mapping api.PrefixedClaimOrExpression, fldPath *field.Path, claimOrExpressionRequired bool) (*authenticationcel.CompilationResult, field.ErrorList) {
	var allErrs field.ErrorList

	var compilationResult *authenticationcel.CompilationResult
	switch {
	case len(mapping.Expression) > 0 && len(mapping.Claim) > 0:
		allErrs = append(allErrs, field.Invalid(fldPath, "", "claim and expression can't both be set"))
	case len(mapping.Expression) == 0 && len(mapping.Claim) == 0 && claimOrExpressionRequired:
		allErrs = append(allErrs, field.Required(fldPath, "claim or expression is required"))
	case len(mapping.Expression) > 0:
		var err *field.Error

		// prefix 与 expression 互斥：expression 自带完整的取值逻辑。
		if mapping.Prefix != nil {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("prefix"), *mapping.Prefix, "may not be specified when expression is set"))
		}
		compilationResult, err = compileClaimsCELExpression(compiler, &authenticationcel.ClaimMappingExpression{
			Expression: mapping.Expression,
		}, fldPath.Child("expression"))

		if err != nil {
			allErrs = append(allErrs, err)
		}

	case len(mapping.Claim) > 0:
		// prefix 必须显式设置（可为空串），以区分“漏写”与“故意不加前缀”。
		if mapping.Prefix == nil {
			allErrs = append(allErrs, field.Required(fldPath.Child("prefix"), "prefix is required when claim is set. It can be set to an empty string to disable prefixing"))
		}
	}

	return compilationResult, allErrs
}

// validateUserValidationRules 校验用户信息校验规则：expression 必填且去重，
// 编译结果用于在认证后对 user.Info 做最终裁决。
func validateUserValidationRules(compiler authenticationcel.Compiler, state *validationState, rules []api.UserValidationRule, fldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	var compilationResults []authenticationcel.CompilationResult

	seenExpressions := sets.NewString()
	for i, rule := range rules {
		fldPath := fldPath.Index(i)

		if len(rule.Expression) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("expression"), ""))
			continue
		}

		if seenExpressions.Has(rule.Expression) {
			allErrs = append(allErrs, field.Duplicate(fldPath.Child("expression"), rule.Expression))
			continue
		}
		seenExpressions.Insert(rule.Expression)

		compilationResult, err := compileUserCELExpression(compiler, &authenticationcel.UserValidationCondition{
			Expression: rule.Expression,
			Message:    rule.Message,
		}, fldPath.Child("expression"))

		if err != nil {
			allErrs = append(allErrs, err)
			continue
		}

		if compilationResult != nil {
			compilationResults = append(compilationResults, *compilationResult)
		}
	}

	if len(compilationResults) > 0 {
		state.mapper.UserValidationRules = authenticationcel.NewUserMapper(compilationResults)
	}

	return allErrs
}

// compileClaimsCELExpression 编译 claim 相关表达式，并把 CEL 错误转换为字段错误。
func compileClaimsCELExpression(compiler authenticationcel.Compiler, expression authenticationcel.ExpressionAccessor, fldPath *field.Path) (*authenticationcel.CompilationResult, *field.Error) {
	compilationResult, err := compiler.CompileClaimsExpression(expression)
	if err != nil {
		return nil, convertCELErrorToValidationError(fldPath, expression.GetExpression(), err)
	}
	return &compilationResult, nil
}

// compileUserCELExpression 编译 user 相关表达式，并把 CEL 错误转换为字段错误。
func compileUserCELExpression(compiler authenticationcel.Compiler, expression authenticationcel.ExpressionAccessor, fldPath *field.Path) (*authenticationcel.CompilationResult, *field.Error) {
	compilationResult, err := compiler.CompileUserExpression(expression)
	if err != nil {
		return nil, convertCELErrorToValidationError(fldPath, expression.GetExpression(), err)
	}
	return &compilationResult, nil
}

// ValidateAuthorizationConfiguration 校验给定的 AuthorizationConfiguration。
//
// knownTypes 限定可用的 authorizer 类型；repeatableTypes 允许同类多个实例
// （例如多个 webhook），其余类型在同一配置中重复即报错。
func ValidateAuthorizationConfiguration(compiler authorizationcel.Compiler, fldPath *field.Path, c *api.AuthorizationConfiguration, knownTypes sets.Set[string], repeatableTypes sets.Set[string]) field.ErrorList {
	allErrs := field.ErrorList{}

	if len(c.Authorizers) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("authorizers"), "at least one authorization mode must be defined"))
	}

	seenAuthorizerTypes := sets.NewString()
	seenAuthorizerNames := sets.NewString()
	for i, a := range c.Authorizers {
		fldPath := fldPath.Child("authorizers").Index(i)
		aType := string(a.Type)
		if aType == "" {
			allErrs = append(allErrs, field.Required(fldPath.Child("type"), ""))
			continue
		}
		if !knownTypes.Has(aType) {
			allErrs = append(allErrs, field.NotSupported(fldPath.Child("type"), aType, sets.List(knownTypes)))
			continue
		}
		// 不可重复的类型出现多次会让授权链语义含混，直接拒绝。
		if seenAuthorizerTypes.Has(aType) && !repeatableTypes.Has(aType) {
			allErrs = append(allErrs, field.Duplicate(fldPath.Child("type"), aType))
			continue
		}
		seenAuthorizerTypes.Insert(aType)

		// name 既用于日志/审计定位，也作为同类 authorizer 之间的唯一标识。
		if len(a.Name) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("name"), ""))
		} else if seenAuthorizerNames.Has(a.Name) {
			allErrs = append(allErrs, field.Duplicate(fldPath.Child("name"), a.Name))
		} else if errs := utilvalidation.IsDNS1123Subdomain(a.Name); len(errs) != 0 {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("name"), a.Name, fmt.Sprintf("authorizer name is invalid: %s", strings.Join(errs, ", "))))
		}
		seenAuthorizerNames.Insert(a.Name)

		switch a.Type {
		case api.TypeWebhook:
			if a.Webhook == nil {
				allErrs = append(allErrs, field.Required(fldPath.Child("webhook"), "required when type=Webhook"))
				continue
			}
			allErrs = append(allErrs, ValidateWebhookConfiguration(compiler, fldPath, a.Webhook)...)
		default:
			// 非 webhook 类型不允许携带 webhook 配置，避免出现“看似生效实则被忽略”的配置。
			if a.Webhook != nil {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("webhook"), "non-null", "may only be specified when type=Webhook"))
			}
		}
	}

	return allErrs
}

// ValidateWebhookConfiguration 校验授权 webhook 的必填字段与取值范围。
//
// timeout 上限 30s 与内置 webhook 超时保持一致，避免授权链路拖垮请求；
// kubeConfigFile 必须是已存在的绝对路径，因为该路径由 apiserver 进程直接打开。
func ValidateWebhookConfiguration(compiler authorizationcel.Compiler, fldPath *field.Path, c *api.WebhookConfiguration) field.ErrorList {
	allErrs := field.ErrorList{}

	if c.Timeout.Duration == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("timeout"), ""))
	} else if c.Timeout.Duration > 30*time.Second || c.Timeout.Duration < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("timeout"), c.Timeout.Duration.String(), "must be > 0s and <= 30s"))
	}

	if c.AuthorizedTTL.Duration == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("authorizedTTL"), ""))
	} else if c.AuthorizedTTL.Duration < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("authorizedTTL"), c.AuthorizedTTL.Duration.String(), "must be > 0s"))
	}

	if c.UnauthorizedTTL.Duration == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("unauthorizedTTL"), ""))
	} else if c.UnauthorizedTTL.Duration < 0 {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("unauthorizedTTL"), c.UnauthorizedTTL.Duration.String(), "must be > 0s"))
	}

	switch c.SubjectAccessReviewVersion {
	case "":
		allErrs = append(allErrs, field.Required(fldPath.Child("subjectAccessReviewVersion"), ""))
	case "v1":
		// 编译期引用具体类型，确保该版本在当前 scheme 中确实可用。
		_ = &v1.SubjectAccessReview{}
	case "v1beta1":
		_ = &v1beta1.SubjectAccessReview{}
	default:
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("subjectAccessReviewVersion"), c.SubjectAccessReviewVersion, []string{"v1", "v1beta1"}))
	}

	switch c.MatchConditionSubjectAccessReviewVersion {
	case "":
		// 仅在配置了 matchConditions 时该字段才必需。
		if len(c.MatchConditions) > 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("matchConditionSubjectAccessReviewVersion"), "required if match conditions are specified"))
		}
	case "v1":
		_ = &v1.SubjectAccessReview{}
	default:
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("matchConditionSubjectAccessReviewVersion"), c.MatchConditionSubjectAccessReviewVersion, []string{"v1"}))
	}

	switch c.FailurePolicy {
	case "":
		allErrs = append(allErrs, field.Required(fldPath.Child("failurePolicy"), ""))
	case api.FailurePolicyNoOpinion, api.FailurePolicyDeny:
		// 合法取值：NoOpinion 让后续 authorizer 继续判断，Deny 直接拒绝。
	default:
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("failurePolicy"), c.FailurePolicy, []string{"NoOpinion", "Deny"}))
	}

	switch c.ConnectionInfo.Type {
	case "":
		allErrs = append(allErrs, field.Required(fldPath.Child("connectionInfo", "type"), ""))
	case api.AuthorizationWebhookConnectionInfoTypeInCluster:
		// 集群内连接不需要 kubeconfig，设置即视为配置错误。
		if c.ConnectionInfo.KubeConfigFile != nil {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("connectionInfo", "kubeConfigFile"), *c.ConnectionInfo.KubeConfigFile, "can only be set when type=KubeConfigFile"))
		}
	case api.AuthorizationWebhookConnectionInfoTypeKubeConfigFile:
		// 依次检查：存在、绝对路径、可读、是普通文件。
		if c.ConnectionInfo.KubeConfigFile == nil || *c.ConnectionInfo.KubeConfigFile == "" {
			allErrs = append(allErrs, field.Required(fldPath.Child("connectionInfo", "kubeConfigFile"), ""))
		} else if !filepath.IsAbs(*c.ConnectionInfo.KubeConfigFile) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("connectionInfo", "kubeConfigFile"), *c.ConnectionInfo.KubeConfigFile, "must be an absolute path"))
		} else if info, err := os.Stat(*c.ConnectionInfo.KubeConfigFile); err != nil {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("connectionInfo", "kubeConfigFile"), *c.ConnectionInfo.KubeConfigFile, fmt.Sprintf("error loading file: %v", err)))
		} else if !info.Mode().IsRegular() {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("connectionInfo", "kubeConfigFile"), *c.ConnectionInfo.KubeConfigFile, "must be a regular file"))
		}
	default:
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("connectionInfo", "type"), c.ConnectionInfo, []string{api.AuthorizationWebhookConnectionInfoTypeInCluster, api.AuthorizationWebhookConnectionInfoTypeKubeConfigFile}))
	}

	_, errs := compileMatchConditions(compiler, c.MatchConditions, fldPath)
	allErrs = append(allErrs, errs...)

	return allErrs
}

// ValidateAndCompileMatchConditions 校验给定 Webhook 的 matchConditions，导出供 authz 包使用。
func ValidateAndCompileMatchConditions(compiler authorizationcel.Compiler, matchConditions []api.WebhookMatchCondition) (*authorizationcel.CELMatcher, field.ErrorList) {
	return compileMatchConditions(compiler, matchConditions, nil)
}

// compileMatchConditions 编译 matchConditions，并汇总是否需要 field/label selector 的信息。
// 上限 64 条与 mutating/validating webhook 的约束保持一致。
func compileMatchConditions(compiler authorizationcel.Compiler, matchConditions []api.WebhookMatchCondition, fldPath *field.Path) (*authorizationcel.CELMatcher, field.ErrorList) {
	var allErrs field.ErrorList
	if len(matchConditions) > 64 {
		allErrs = append(allErrs, field.TooMany(fldPath.Child("matchConditions"), len(matchConditions), 64))
		return nil, allErrs
	}

	seenExpressions := sets.NewString()
	var compilationResults []authorizationcel.CompilationResult
	var usesFieldSelector, usesLabelSelector bool

	for i, condition := range matchConditions {
		fldPath := fldPath.Child("matchConditions").Index(i).Child("expression")
		// 空白表达式视为未填写，避免部署后才发现条件恒为真。
		if len(strings.TrimSpace(condition.Expression)) == 0 {
			allErrs = append(allErrs, field.Required(fldPath, ""))
			continue
		}
		if seenExpressions.Has(condition.Expression) {
			allErrs = append(allErrs, field.Duplicate(fldPath, condition.Expression))
			continue
		}
		seenExpressions.Insert(condition.Expression)
		compilationResult, err := compileMatchConditionsExpression(fldPath, compiler, condition.Expression)
		if err != nil {
			allErrs = append(allErrs, err)
			continue
		}
		compilationResults = append(compilationResults, compilationResult)
		// 任一条件使用了 selector，整个 matcher 就需要为求值准备相应数据。
		usesFieldSelector = usesFieldSelector || compilationResult.UsesFieldSelector
		usesLabelSelector = usesLabelSelector || compilationResult.UsesLabelSelector
	}
	if len(compilationResults) == 0 {
		return nil, allErrs
	}
	return &authorizationcel.CELMatcher{
		CompilationResults: compilationResults,
		UsesFieldSelector:  usesFieldSelector,
		UsesLabelSelector:  usesLabelSelector,
	}, allErrs
}

// compileMatchConditionsExpression 编译单条 matchCondition 表达式。
func compileMatchConditionsExpression(fldPath *field.Path, compiler authorizationcel.Compiler, expression string) (authorizationcel.CompilationResult, *field.Error) {
	authzExpression := &authorizationcel.SubjectAccessReviewMatchCondition{
		Expression: expression,
	}
	compilationResult, err := compiler.CompileCELExpression(authzExpression)
	if err != nil {
		return compilationResult, convertCELErrorToValidationError(fldPath, authzExpression.GetExpression(), err)
	}
	return compilationResult, nil
}

// convertCELErrorToValidationError 把 CEL 编译错误映射为对应的 field.Error 类型，
// 使 API 返回的 reason 能区分“字段缺失”“值非法”与“服务端内部错误”。
func convertCELErrorToValidationError(fldPath *field.Path, expression string, err error) *field.Error {
	var celErr *cel.Error
	if errors.As(err, &celErr) {
		switch celErr.Type {
		case cel.ErrorTypeRequired:
			return field.Required(fldPath, celErr.Detail)
		case cel.ErrorTypeInvalid:
			return field.Invalid(fldPath, expression, celErr.Detail)
		default:
			return field.InternalError(fldPath, celErr)
		}
	}
	return field.InternalError(fldPath, fmt.Errorf("error is not cel error: %w", err))
}
