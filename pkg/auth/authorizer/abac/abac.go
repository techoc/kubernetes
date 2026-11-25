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

// Package abac 使用基于属性的访问控制方案
// 对 Kubernetes API 请求做授权：策略以文件形式提供，逐行描述允许的属性组合。
package abac

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/kubernetes/pkg/apis/abac"

	// 空导入 latest API，触发 init 副作用（把最新版本的类型/转换注册进 scheme）。
	_ "k8s.io/kubernetes/pkg/apis/abac/latest"
	v0 "k8s.io/kubernetes/pkg/apis/abac/v0"
)

// policyLoadError 携带策略文件加载失败的位置信息，
// 便于在报错时指出具体是哪个文件、哪一行出了问题。
type policyLoadError struct {
	path string
	line int
	data []byte
	err  error
}

// Error 返回策略加载错误；能定位到行号时附带原始行，便于排查语法问题。
func (p policyLoadError) Error() string {
	// line >= 0 表示错误定位到了某一行，把原始行内容一并输出方便排查。
	if p.line >= 0 {
		return fmt.Sprintf("error reading policy file %s, line %d: %s: %v", p.path, p.line, string(p.data), p.err)
	}
	return fmt.Sprintf("error reading policy file %s: %v", p.path, p.err)
}

// 编译期断言：PolicyList 同时满足 Authorizer 和 RuleResolver 接口。
var _ = authorizer.Authorizer(PolicyList{})
var _ = authorizer.RuleResolver(PolicyList{})

// PolicyList 是 Policy 列表，一个元素即一条 ABAC 策略。
type PolicyList []*abac.Policy

// NewFromFile 尝试从给定文件加载策略列表。
//
// TODO: 未来改为通过 API 创建策略并存入 REST storage。
func NewFromFile(path string) (PolicyList, error) {
	// 文件格式为每行一个 map：这样便于文件拼接、在文件中写注释，
	// 并且可以精确定位出错的行号。
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	pl := make(PolicyList, 0)

	decoder := abac.Codecs.UniversalDecoder()

	i := 0
	unversionedLines := 0
	for scanner.Scan() {
		i++
		p := &abac.Policy{}
		b := scanner.Bytes()

		// 跳过注释行和空行。
		trimmed := strings.TrimSpace(string(b))
		if len(trimmed) == 0 || strings.HasPrefix(trimmed, "#") {
			continue
		}

		decodedObj, _, err := decoder.Decode(b, nil, nil)
		if err != nil {
			// 缺少 version/kind 或类型未注册，说明是旧的无版本格式，
			// 走下面的兼容迁移路径；其他解码错误直接失败。
			if !(runtime.IsMissingVersion(err) || runtime.IsMissingKind(err) || runtime.IsNotRegisteredError(err)) {
				return nil, policyLoadError{path, i, b, err}
			}
			unversionedLines++
			// 把无版本的旧策略对象解码到 v0 类型，再转换成内部类型。
			oldPolicy := &v0.Policy{}
			if err := runtime.DecodeInto(decoder, b, oldPolicy); err != nil {
				return nil, policyLoadError{path, i, b, err}
			}
			if err := abac.Scheme.Convert(oldPolicy, p, nil); err != nil {
				return nil, policyLoadError{path, i, b, err}
			}
			pl = append(pl, p)
			continue
		}

		decodedPolicy, ok := decodedObj.(*abac.Policy)
		if !ok {
			return nil, policyLoadError{path, i, b, fmt.Errorf("unrecognized object: %#v", decodedObj)}
		}
		pl = append(pl, decodedPolicy)
	}

	if unversionedLines > 0 {
		// 提示用户旧格式仍被兼容但已不推荐，引导其迁移到带版本的格式。
		klog.Warningf("Policy file %s contained unversioned rules. See docs/admin/authorization.md#abac-mode for ABAC file format details.", path)
	}

	if err := scanner.Err(); err != nil {
		return nil, policyLoadError{path, -1, nil, err}
	}
	return pl, nil
}

// matches 判断一条策略是否匹配请求的全部属性条件。
func matches(p abac.Policy, a authorizer.Attributes) bool {
	if subjectMatches(p, a.GetUser()) {
		if verbMatches(p, a) {
			// 资源请求和非资源请求互斥，一条策略最多匹配其中一种。
			if resourceMatches(p, a) {
				return true
			}
			if nonResourceMatches(p, a) {
				return true
			}
		}
	}
	return false
}

// subjectMatches 判断策略中指定的用户和组属性是否匹配请求的用户信息。
func subjectMatches(p abac.Policy, user user.Info) bool {
	matched := false

	if user == nil {
		return false
	}
	username := user.GetName()
	groups := user.GetGroups()

	// 策略指定了用户时必须匹配："*" 匹配所有，否则精确匹配用户名。
	if len(p.Spec.User) > 0 {
		if p.Spec.User == "*" {
			matched = true
		} else {
			matched = p.Spec.User == username
			if !matched {
				return false
			}
		}
	}

	// 策略指定了组时必须匹配："*" 匹配所有，否则用户所在组中至少有一个命中。
	if len(p.Spec.Group) > 0 {
		if p.Spec.Group == "*" {
			matched = true
		} else {
			matched = false
			for _, group := range groups {
				if p.Spec.Group == group {
					matched = true
					break
				}
			}
			if !matched {
				return false
			}
		}
	}

	// 用户和组都没指定时也返回 false：空策略不授予任何权限。
	return matched
}

// verbMatches 检查请求的操作是否被策略允许。
func verbMatches(p abac.Policy, a authorizer.Attributes) bool {
	// TODO: 按具体 verb 做匹配。

	// 所有策略都允许只读请求。
	if a.IsReadOnly() {
		return true
	}

	// 非只读请求只有在策略本身不是 readonly 时才允许。
	if !p.Spec.Readonly {
		return true
	}

	return false
}

// nonResourceMatches 判断策略是否匹配一个非资源请求（如 /healthz）。
func nonResourceMatches(p abac.Policy, a authorizer.Attributes) bool {
	// 只有非资源请求才可能匹配非资源策略。
	if !a.IsResourceRequest() {
		// 通配符匹配所有路径。
		if p.Spec.NonResourcePath == "*" {
			return true
		}
		// 精确匹配。
		if p.Spec.NonResourcePath == a.GetPath() {
			return true
		}
		// 以 * 结尾时按子路径前缀匹配，例如 "/api/*" 匹配 "/api/v1"。
		if strings.HasSuffix(p.Spec.NonResourcePath, "*") && strings.HasPrefix(a.GetPath(), strings.TrimRight(p.Spec.NonResourcePath, "*")) {
			return true
		}
	}
	return false
}

// resourceMatches 判断策略是否匹配一个资源请求。
func resourceMatches(p abac.Policy, a authorizer.Attributes) bool {
	// 只有资源请求才可能匹配资源策略。
	if a.IsResourceRequest() {
		// 命名空间、资源、API 组依次比较，每项是 "*" 或与请求值相等即通过。
		if p.Spec.Namespace == "*" || p.Spec.Namespace == a.GetNamespace() {
			if p.Spec.Resource == "*" || p.Spec.Resource == a.GetResource() {
				if p.Spec.APIGroup == "*" || p.Spec.APIGroup == a.GetAPIGroup() {
					return true
				}
			}
		}
	}
	return false
}

// Authorize 实现 authorizer.Authorize：任意一条策略匹配即放行。
func (pl PolicyList) Authorize(ctx context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
	for _, p := range pl {
		if matches(*p, a) {
			return authorizer.DecisionAllow, "", nil
		}
	}
	// 没有策略匹配时返回 NoOpinion，把决定权交给授权链中的下一个 authorizer。
	return authorizer.DecisionNoOpinion, "No policy matched.", nil
	// TODO: 测量中等规模策略文件下匹配耗时与其他步骤（编解码）的对比，
	// 只有确有需要时才加入缓存。
}

// ConditionsAwareAuthorize 不支持条件判断，直接把 Authorize 的决策转换后返回。
func (pl PolicyList) ConditionsAwareAuthorize(ctx context.Context, a authorizer.Attributes) authorizer.ConditionsAwareDecision {
	return authorizer.ConditionsAwareDecisionFromParts(pl.Authorize(ctx, a))
}

// EvaluateConditions 不受该 authorizer 支持，调用即返回“不支持条件求值”错误。
func (PolicyList) EvaluateConditions(_ context.Context, _ authorizer.ConditionsAwareDecision, _ authorizer.ConditionsData) (authorizer.Decision, string, error) {
	return authorizer.DecisionDeny, "", authorizer.ErrorConditionEvaluationNotSupported
}

// RulesFor 返回给定用户和命名空间下该策略列表对应的资源规则与非资源规则。
func (pl PolicyList) RulesFor(ctx context.Context, user user.Info, namespace string) ([]authorizer.ResourceRuleInfo, []authorizer.NonResourceRuleInfo, bool, error) {
	var (
		resourceRules    []authorizer.ResourceRuleInfo
		nonResourceRules []authorizer.NonResourceRuleInfo
	)

	for _, p := range pl {
		// 只收集主体匹配、且命名空间匹配的策略。
		if subjectMatches(*p, user) {
			if p.Spec.Namespace == "*" || p.Spec.Namespace == namespace {
				// 资源规则与非资源规则分别构造，一条策略可能同时产生两类规则。
				if len(p.Spec.Resource) > 0 {
					r := authorizer.DefaultResourceRuleInfo{
						Verbs:     getVerbs(p.Spec.Readonly),
						APIGroups: []string{p.Spec.APIGroup},
						Resources: []string{p.Spec.Resource},
					}
					var resourceRule authorizer.ResourceRuleInfo = &r
					resourceRules = append(resourceRules, resourceRule)
				}
				if len(p.Spec.NonResourcePath) > 0 {
					r := authorizer.DefaultNonResourceRuleInfo{
						Verbs:           getVerbs(p.Spec.Readonly),
						NonResourceURLs: []string{p.Spec.NonResourcePath},
					}
					var nonResourceRule authorizer.NonResourceRuleInfo = &r
					nonResourceRules = append(nonResourceRules, nonResourceRule)
				}
			}
		}
	}
	// ABAC 不支持校验规则（第三返回值始终为 false）。
	return resourceRules, nonResourceRules, false, nil
}

// getVerbs 把 readonly 标记转换成对应的操作集合：
// 只读策略只允许 get/list/watch，非只读策略允许全部操作。
func getVerbs(isReadOnly bool) []string {
	if isReadOnly {
		return []string{"get", "list", "watch"}
	}
	return []string{"*"}
}
