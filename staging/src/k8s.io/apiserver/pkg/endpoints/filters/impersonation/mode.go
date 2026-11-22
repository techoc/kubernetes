/*
Copyright 2025 The Kubernetes Authors.

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

package impersonation

import (
	"context"
	"fmt"
	"slices"
	"strings"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/validation"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/authentication/serviceaccount"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
)

// impersonatedUserInfo 是某个模式成功模拟后的结果。
//
// 中文说明：constraint 记录「允许此次模拟的动词」，用于审计与错误信息；
// 它不参与授权判定，只用于解释结果来源。
type impersonatedUserInfo struct {
	user       user.Info
	constraint string // the verb used in impersonationModeState.check that allowed this user to be impersonated
}

// impersonationMode is a type that represents a specific impersonation mode
// it checks if a requester is allowed to make an API request (the attributes) while impersonating a user (the wantedUser)
// a mode may return a cached result if it supports caching (using the input cache key if appropriate)
// a nil impersonatedUserInfo is returned if the mode does not support impersonating the wantedUser
//
// 中文说明：每个模式负责两件事——先判断「该模式是否适用于本次请求」，再做授权检查。
// 返回 nil 表示不适用（继续尝试下一个模式），返回错误表示明确拒绝。
// 缓存由各模式自行决定；key 由调用方给出，但模式可以按需改造或忽略。
type impersonationMode interface {
	check(ctx context.Context, key *impersonationCacheKey, wantedUser *user.DefaultInfo, attributes authorizer.Attributes) (*impersonatedUserInfo, error)

	// all methods below are only used in unit tests
	verbForTests() string
	cachesForTests() (outer, inner *impersonationCache)
}

// constrainedImpersonationModeFilter is a function that defines if a specific constrained impersonation mode
// supports the requestor impersonating the wantedUser.  It serves as a sudo authorization check for the mode.
//
// 中文说明：过滤在授权之前执行，且不查缓存——因为缓存键不一定与 wantedUser+attributes
// 一一对应，必须先确认模式适用，否则可能命中为其他输入准备的缓存项。
type constrainedImpersonationModeFilter func(wantedUser *user.DefaultInfo, requestor user.Info) bool

// allImpersonationModes 返回按优先级排列的全部模式。
// 中文说明：顺序即尝试顺序；associated-node 放在最前，因为它对节点代理有专门优化，
// 且其语义最窄，先匹配可避免落入更宽泛的模式。
func allImpersonationModes(a authorizer.UnconditionalAuthorizer) []impersonationMode {
	return []impersonationMode{
		associatedNodeImpersonationMode(a),
		arbitraryNodeImpersonationMode(a),
		serviceAccountImpersonationMode(a),
		userInfoImpersonationMode(a),
		legacyImpersonationMode(a),
	}
}

// associatedNodeImpersonationMode allows a requestor service account to impersonate the node that it is
// associated with.  this is by far the most complex impersonation mode because it caches successful
// impersonation attempts in a way that results in a high cache hit ratio even when the same service account
// is used across different pods running on different nodes (i.e. a node agent running as a daemonset).
// only the username can be specified by the requester.  all other fields in user.Info are controlled by the API server.
//
// 中文说明：缓存键刻意抹去节点名与 extra 值（用 "system:node:*" 作占位），
// 使同一 ServiceAccount 在所有节点上共享缓存；安全性由前置的「节点关联校验」保证——
// 只有确认该 SA 的 bound node 与目标节点一致时才会走到这里。
func associatedNodeImpersonationMode(a authorizer.UnconditionalAuthorizer) impersonationMode {
	// we wrap the authorizer so that we can override the requestor service account's extra values
	// and the node name used in the authorization check.  this makes our authorization checks match
	// the exact semantics of our cache key which prevents unexpected privilege escalation on a cache
	// hit.  see the comment below for the cache key details.
	wrappedAuthorizer := authorizer.AuthorizerFunc(func(ctx context.Context, attributes authorizer.Attributes) (authorizer.Decision, string, error) {
		// we use checkAuthorization instead of directly calling the authorizer so we can
		// make the error message line up with the actual attributes authorized against
		if err := checkAuthorization(ctx, a, &associatedNodeImpersonationAttributes{Attributes: attributes}); err != nil {
			return authorizer.DecisionDeny, "", err
		}
		return authorizer.DecisionAllow, "", nil
	})
	mode := newConstrainedImpersonationMode(wrappedAuthorizer, "associated-node",
		func(wantedUser *user.DefaultInfo, requestor user.Info) bool {
			wantedNodeName := wantedUser.Name
			return onlyUsernameSet(wantedUser) && requesterAssociatedWithRequestedNodeUsername(requestor, wantedNodeName)
		},
	)
	return &associatedNodeImpersonationCheck{mode: mode}
}

// associatedNodeImpersonationCheck 在通用模式之上做「结果回填」：
// 缓存可能来自任意节点，因此取回后必须把用户名替换为本次请求的目标节点名。
type associatedNodeImpersonationCheck struct {
	mode impersonationMode
}

// check 使用静态缓存键执行检查，再把结果中的节点名修正为目标节点。
// 中文说明：忽略传入 key 是有意为之——见下方注释，缓存语义需要跨节点共享。
func (a *associatedNodeImpersonationCheck) check(ctx context.Context, _ *impersonationCacheKey, wantedUser *user.DefaultInfo, attributes authorizer.Attributes) (*impersonatedUserInfo, error) {
	wantedNodeName := wantedUser.Name
	// ignore the input cache key because the cache semantics for associated-node require custom logic.
	// we know that by the time this key is used, the filter has already verified that the requestor is
	// a service account with a node ref that matches the node it is trying to impersonate.
	// the actual node being impersonated is not relevant to the cache key, so we just use a static wantedUser.
	// we wrap the attributes so that we can drop the requestor service account's extra values.
	// this results in the cache key being the same for the same service account across all nodes.
	// this is only safe because of the aforementioned filter running before the cache lookup happens.
	key := &impersonationCacheKey{wantedUser: &user.DefaultInfo{Name: "system:node:*"}, attributes: &associatedNodeImpersonationAttributes{Attributes: attributes}}
	impersonatedNodeWithMaybeIncorrectUsername, err := a.mode.check(ctx, key, wantedUser, attributes)
	if err != nil || impersonatedNodeWithMaybeIncorrectUsername == nil {
		return nil, err
	}
	// at this point, we know that we have a successful associated-node impersonation.
	// the value could have come from the cache and thus could be for any node, so we wrap the result
	// here so that the username matches the node associated with the requestor service account.
	return &impersonatedUserInfo{
		user: &associatedNodeImpersonationWantedUserInfo{
			Info: impersonatedNodeWithMaybeIncorrectUsername.user,
			name: wantedNodeName,
		},
		constraint: impersonatedNodeWithMaybeIncorrectUsername.constraint,
	}, nil
}

// verbForTests 暴露模式动词，仅用于测试断言。
func (a *associatedNodeImpersonationCheck) verbForTests() string {
	return a.mode.verbForTests()
}

// cachesForTests 暴露内外两层缓存，仅用于测试。
func (a *associatedNodeImpersonationCheck) cachesForTests() (*impersonationCache, *impersonationCache) {
	return a.mode.cachesForTests()
}

// associatedNodeImpersonationAttributes 包装属性，用于在授权时替换 requestor 的 extra。
type associatedNodeImpersonationAttributes struct {
	authorizer.Attributes
}

// GetUser 返回替换 extra 后的 requestor，使授权结果与缓存键语义一致。
func (a *associatedNodeImpersonationAttributes) GetUser() user.Info {
	return &associatedNodeImpersonationRequestorUserInfo{Info: a.Attributes.GetUser()}
}

// GetName 在 associated-node 动词下返回 "*"。
// 中文说明：因为缓存键忽略节点名，授权检查必须对「所有节点名」都成立，
// 否则会出现「授权了 A 节点、却命中 B 节点的缓存」这种越权。
func (a *associatedNodeImpersonationAttributes) GetName() string {
	if a.GetVerb() == "impersonate:associated-node" {
		return "*" // our cache key ignores the node name, so our authorization check needs to be valid for all node names
	}
	return a.Attributes.GetName()
}

// associatedNodeImpersonationRequestorUserInfo 抹去 requestor 的 extra 值。
type associatedNodeImpersonationRequestorUserInfo struct {
	user.Info
}

// GetExtra 只保留 extra 的键名集合，丢弃具体值。
// 中文说明：节点名等值会让缓存键随节点变化，导致命中率极低；
// 保留键名是为了让不同类型的 bound object ref 产生不同缓存键，维持隔离。
// 完整 extra 仍会出现在审计日志中。
func (a *associatedNodeImpersonationRequestorUserInfo) GetExtra() map[string][]string {
	return map[string][]string{
		// we know the requestor is a service account with a node ref that matches the node it is trying to impersonate
		// basically all the extra values would cause cache misses (for example, the node name itself)
		// so we drop all the extra values but keep the associated key names
		// the authorizer can trust that we have performed the node association check correctly
		// the audit log will still contain the full requestor extra fields
		// different bound object ref types can result in different extra keys, and thus a different cache key
		"authentication.kubernetes.io/associated-node-keys": sets.StringKeySet(a.Info.GetExtra()).List(),
	}
}

// associatedNodeImpersonationWantedUserInfo 在缓存结果之上覆盖用户名。
type associatedNodeImpersonationWantedUserInfo struct {
	user.Info
	name string
}

// GetName 返回本次请求实际要模拟的节点名，而非缓存中记录的节点名。
func (a *associatedNodeImpersonationWantedUserInfo) GetName() string {
	return a.name
}

// arbitraryNodeImpersonationMode implements constrained impersonation for nodes.
// Only the username can be specified by the requester.  All other fields in user.Info are controlled by the API server.
func arbitraryNodeImpersonationMode(a authorizer.UnconditionalAuthorizer) impersonationMode {
	return newConstrainedImpersonationMode(a, "arbitrary-node",
		func(wantedUser *user.DefaultInfo, _ user.Info) bool {
			if !onlyUsernameSet(wantedUser) {
				return false
			}
			_, ok := isNodeUsername(wantedUser.Name)
			return ok
		},
	)
}

// serviceAccountImpersonationMode implements constrained impersonation for service accounts.
// Only the username can be specified by the requester.  All other fields in user.Info are controlled by the API server.
func serviceAccountImpersonationMode(a authorizer.UnconditionalAuthorizer) impersonationMode {
	return newConstrainedImpersonationMode(a, "serviceaccount",
		func(wantedUser *user.DefaultInfo, _ user.Info) bool {
			if !onlyUsernameSet(wantedUser) {
				return false
			}
			_, _, ok := isServiceAccountUsername(wantedUser.Name)
			return ok
		},
	)
}

// userInfoImpersonationMode implements constrained impersonation for non-node and non-service account users.
// Unlike the other constrained impersonation modes, it supports impersonating all fields of user.Info.
//
// 中文说明：排除节点与 ServiceAccount 用户名，是因为它们已有专门模式且
// 其组/extra 由 API Server 固定生成；若允许在此模式模拟，会绕过这些固定语义。
func userInfoImpersonationMode(a authorizer.UnconditionalAuthorizer) impersonationMode {
	return newConstrainedImpersonationMode(a, "user-info",
		func(wantedUser *user.DefaultInfo, _ user.Info) bool {
			// nodes and service accounts cannot be impersonated in this mode.
			// the user-info bucket is reserved for the "other" users, that is,
			// users that do not have an explicit schema defined by Kube.
			if _, ok := isNodeUsername(wantedUser.Name); ok {
				return false
			}
			if _, _, ok := isServiceAccountUsername(wantedUser.Name); ok {
				return false
			}
			return true
		},
	)
}

// legacyImpersonationMode is a complete reimplementation of the original impersonation mode that has
// existed in kube since v1.3.  The behavior is expected to be identical to the original implementation.
//
// 中文说明：保留原语义（使用 impersonate 动词、无外层缓存、无额外校验），
// 以保证升级后旧客户端的授权结果不变。
func legacyImpersonationMode(a authorizer.UnconditionalAuthorizer) impersonationMode {
	return &legacyImpersonationCheck{m: newImpersonationModeState(a, "impersonate", false)}
}

// legacyImpersonationCheck 是 legacy 模式的薄封装。
type legacyImpersonationCheck struct {
	m *impersonationModeState
}

// check 直接委托给主授权检查。
func (l *legacyImpersonationCheck) check(ctx context.Context, key *impersonationCacheKey, wantedUser *user.DefaultInfo, attributes authorizer.Attributes) (*impersonatedUserInfo, error) {
	requestor := attributes.GetUser()
	return l.m.check(ctx, key, wantedUser, requestor)
}

// verbForTests 返回 legacy 动词 "impersonate"。
func (l *legacyImpersonationCheck) verbForTests() string {
	return l.m.verb
}

// cachesForTests 返回空的外层缓存与（未使用的）内层缓存。
func (l *legacyImpersonationCheck) cachesForTests() (*impersonationCache, *impersonationCache) {
	// legacy impersonation has no outer layer so just return an empty cache
	// though an inner cache is present, it is unused
	return newImpersonationCache(false), l.m.cache
}

// newConstrainedImpersonationMode 构造受限模式：动词为 impersonate:<mode>，启用缓存。
func newConstrainedImpersonationMode(a authorizer.UnconditionalAuthorizer, mode string, filter constrainedImpersonationModeFilter) impersonationMode {
	return &constrainedImpersonationModeState{
		state:      newImpersonationModeState(a, "impersonate:"+mode, true),
		cache:      newImpersonationCache(false),
		authorizer: a,
		mode:       mode,
		filter:     filter,
	}
}

// constrainedImpersonationModeState implements the secondary authorization check via impersonate-on:<mode>:<verb> to
// determine if the requestor is authorized to perform the specific verb when impersonating the wantedUser via mode.
// if this check succeeds, the primary authorization checks are run, see impersonationModeState for details.
// if the mode's filter does not match the inputs, the impersonation automatically fails and returns a nil impersonatedUserInfo.
//
// 中文说明：两层授权——先检查「是否允许在此模式下执行该动词」（sudo 检查），
// 再检查「是否允许模拟该用户的具体字段」。外层缓存覆盖整体结果，命中率高时省去两轮检查。
type constrainedImpersonationModeState struct {
	state *impersonationModeState
	// this outer cache covers the overall impersonation for this mode, i.e. a cache hit here short-circuits all checks
	// skipAttributes is false, i.e. this cache depends on the request being made, not just the user being impersonated by the requestor
	// it is expected to have a low hit ratio because the requestor is unlikely to make the same request multiple times in a short period
	cache      *impersonationCache
	authorizer authorizer.UnconditionalAuthorizer
	mode       string
	filter     constrainedImpersonationModeFilter
}

// check 依次执行过滤、外层缓存、sudo 授权、主授权与缓存写入。
func (c *constrainedImpersonationModeState) check(ctx context.Context, key *impersonationCacheKey, wantedUser *user.DefaultInfo, attributes authorizer.Attributes) (*impersonatedUserInfo, error) {
	requestor := attributes.GetUser()
	// we must call the filter before doing anything because this serves as a sudo authorization check to say "does this mode even apply?"
	// also the cache key is not always a direct match with wantedUser+attributes, so again, we must call the filter first
	if !c.filter(wantedUser, requestor) {
		return nil, nil
	}

	if impersonatedUser := c.cache.get(key); impersonatedUser != nil {
		return impersonatedUser, nil
	}

	if err := checkAuthorization(ctx, c.authorizer, &impersonateOnAttributes{mode: c.mode, Attributes: attributes}); err != nil {
		return nil, err
	}

	impersonatedUser, err := c.state.check(ctx, key, wantedUser, requestor)
	if err != nil || impersonatedUser == nil {
		return nil, err
	}
	c.cache.set(key, impersonatedUser)
	return impersonatedUser, nil
}

// verbForTests 返回受限模式的动词。
func (c *constrainedImpersonationModeState) verbForTests() string {
	return c.state.verb
}

// cachesForTests 返回外层与内层缓存，仅用于测试。
func (c *constrainedImpersonationModeState) cachesForTests() (*impersonationCache, *impersonationCache) {
	return c.cache, c.state.cache
}

// impersonationModeState implements the primary authorization checks via the impersonate:<mode> verb for constrained
// impersonation and the impersonate verb for legacy impersonation.  each field that is set in the wantedUser
// results in one or more authorization checks to determine if the requestor has access to impersonate that value.
//
// 中文说明：内层缓存只取决于「被模拟的用户」，与具体请求无关（skipAttributes=true），
// 因此同一用户被反复模拟时命中率很高。legacy 模式不启用该缓存以保持行为不变。
type impersonationModeState struct {
	authorizer                 authorizer.UnconditionalAuthorizer
	verb                       string
	isConstrainedImpersonation bool

	usernameAndGroupGV schema.GroupVersion
	constraint         string

	// this inner cache covers the checks related to the specific fields set in wantedUser
	// skipAttributes is true, i.e. this cache only depends on the user being impersonated by the requestor
	// it is expected to have a high hit ratio because the requestor may impersonate the same user for many different requests
	cache *impersonationCache
}

// newImpersonationModeState 根据是否为受限模式选择 API 组与约束标识。
// 中文说明：受限模式使用 authentication.k8s.io/v1 的虚拟资源（users/groups/uids），
// legacy 模式沿用 core/v1 的历史语义，两者不能混用，否则旧策略会失效。
func newImpersonationModeState(a authorizer.UnconditionalAuthorizer, verb string, isConstrainedImpersonation bool) *impersonationModeState {
	usernameAndGroupGV := authenticationv1.SchemeGroupVersion
	constraint := verb
	if !isConstrainedImpersonation {
		usernameAndGroupGV = corev1.SchemeGroupVersion
		constraint = ""
	}
	return &impersonationModeState{
		authorizer:                 a,
		verb:                       verb,
		isConstrainedImpersonation: isConstrainedImpersonation,

		usernameAndGroupGV: usernameAndGroupGV,
		constraint:         constraint,
		cache:              newImpersonationCache(true),
	}
}

// check 逐字段授权并补齐系统组。
// 中文说明：字段检查顺序为用户名 → UID → 组 → extra；任一失败即拒绝。
// 最后根据匿名与否补齐 system:unauthenticated / system:authenticated，
// 使模拟出的用户与真实认证用户的组集合一致。
func (m *impersonationModeState) check(ctx context.Context, key *impersonationCacheKey, wantedUser *user.DefaultInfo, requestor user.Info) (*impersonatedUserInfo, error) {
	// we only use caching in constrained impersonation mode to avoid any behavioral changes with legacy impersonation
	if m.isConstrainedImpersonation {
		if impersonatedUser := m.cache.get(key); impersonatedUser != nil {
			return impersonatedUser, nil
		}
	}

	actualUser := *wantedUser

	if err := m.authorizeUsername(ctx, requestor, wantedUser.Name, wantedUser.Groups, &actualUser); err != nil {
		return nil, err
	}

	if err := m.authorizeUID(ctx, requestor, wantedUser.UID); err != nil {
		return nil, err
	}

	if err := m.authorizeGroups(ctx, requestor, wantedUser.Groups); err != nil {
		return nil, err
	}

	if err := m.authorizeExtra(ctx, requestor, wantedUser.Extra); err != nil {
		return nil, err
	}

	if actualUser.Name == user.Anonymous {
		ensureGroup(&actualUser, user.AllUnauthenticated)
	} else if !slices.Contains(actualUser.Groups, user.AllUnauthenticated) {
		ensureGroup(&actualUser, user.AllAuthenticated)
	}

	impersonatedUser := &impersonatedUserInfo{user: &actualUser, constraint: m.constraint}
	if m.isConstrainedImpersonation {
		m.cache.set(key, impersonatedUser)
	}
	return impersonatedUser, nil
}

// authorizeUsername 对用户名执行授权，并在受限模式下规范化节点/ServiceAccount 的属性。
// 中文说明：受限模式要求节点与 SA 不得同时指定组（其组由系统固定生成）；
// 若未指定组，则填入对应的固定组，保证模拟结果可预测且不可被调用方篡改。
func (m *impersonationModeState) authorizeUsername(ctx context.Context, requestor user.Info, username string, wantedUserGroups []string, actualUser *user.DefaultInfo) error {
	usernameAttributes := impersonationAttributes(requestor, m.usernameAndGroupGV, m.verb, "users", username)

	if m.isConstrainedImpersonation {
		if name, ok := isNodeUsername(username); ok {
			usernameAttributes.Resource = "nodes"
			usernameAttributes.Name = name

			// this should be impossible to reach but check just in case
			if len(wantedUserGroups) != 0 {
				return responsewriters.ForbiddenStatusError(usernameAttributes, fmt.Sprintf("when impersonating a node, cannot impersonate groups %q", wantedUserGroups))
			}

			actualUser.Groups = []string{user.NodesGroup} // all nodes have a fixed group list in constrained impersonation
		}
	}

	if namespace, name, ok := isServiceAccountUsername(username); ok {
		usernameAttributes.Resource = "serviceaccounts"
		usernameAttributes.Namespace = namespace
		usernameAttributes.Name = name

		// this should be impossible to reach but check just in case
		if m.isConstrainedImpersonation && len(wantedUserGroups) != 0 {
			return responsewriters.ForbiddenStatusError(usernameAttributes, fmt.Sprintf("when impersonating a service account, cannot impersonate groups %q", wantedUserGroups))
		}

		if len(wantedUserGroups) == 0 {
			// if groups are not specified for a service account, we know the groups because it is a fixed mapping.  Add them
			actualUser.Groups = serviceaccount.MakeGroupNames(namespace)
		}
	}

	return checkAuthorization(ctx, m.authorizer, usernameAttributes)
}

// authorizeUID 对 UID 执行授权；UID 为空时跳过。
func (m *impersonationModeState) authorizeUID(ctx context.Context, requestor user.Info, uid string) error {
	if len(uid) == 0 {
		return nil
	}
	uidAttributes := impersonationAttributes(requestor, authenticationv1.SchemeGroupVersion, m.verb, "uids", uid)
	return checkAuthorization(ctx, m.authorizer, uidAttributes)
}

// manyAuthorizationChecksInLoop is an arbitrary value used in constrained impersonation modes to decide if they
// should try to perform a single wildcard authorization check before making many individual checks in a loop.
//
// 中文说明：当组/extra 数量较多时，先尝试一次通配授权，可把 N 次授权压缩为 1 次；
// 4 是经验阈值，平衡「多一次可能失败的检查」与「循环开销」。
const manyAuthorizationChecksInLoop = 4

// authorizeGroups 对组列表执行授权。
// 中文说明：受限模式额外禁止模拟空字符串组与 system:masters——后者等价于提权。
// 数量达到阈值时先尝试通配授权。
func (m *impersonationModeState) authorizeGroups(ctx context.Context, requestor user.Info, groups []string) error {
	if len(groups) == 0 {
		return nil
	}

	groupAttributes := impersonationAttributes(requestor, m.usernameAndGroupGV, m.verb, "groups", "")

	// perform extra sanity checks that would be backwards incompatible with legacy impersonation
	if m.isConstrainedImpersonation {
		if slices.Contains(groups, "") {
			return responsewriters.ForbiddenStatusError(groupAttributes, "impersonating the empty string group is not allowed")
		}
		if slices.Contains(groups, user.SystemPrivilegedGroup) {
			groupAttributes.Name = user.SystemPrivilegedGroup
			return responsewriters.ForbiddenStatusError(groupAttributes, "impersonating the system:masters group is not allowed")
		}
	}

	// if the requestor is trying to impersonate many groups at once, see if they are authorized to impersonate all groups
	// we only do this in constrained impersonation mode to avoid any behavioral changes with legacy impersonation
	if m.isConstrainedImpersonation && len(groups) >= manyAuthorizationChecksInLoop {
		groupAttributes.Name = "*"
		if err := checkAuthorization(ctx, m.authorizer, groupAttributes); err == nil {
			return nil
		}
	}

	for _, group := range groups {
		groupAttributes.Name = group
		if err := checkAuthorization(ctx, m.authorizer, groupAttributes); err != nil {
			return err
		}
	}

	return nil
}

// authorizeExtra 对 user.Info.Extra 执行授权。
// 中文说明：extra 的键作为子资源、值作为对象名参与授权，因此策略可以精确到
// 「允许模拟某个键的某个值」。受限模式会先校验键格式与值非空，
// 防止畸形 extra 进入下游或被用于构造歧义键。
func (m *impersonationModeState) authorizeExtra(ctx context.Context, requestor user.Info, extra map[string][]string) error {
	if len(extra) == 0 {
		return nil
	}

	extraAttributes := impersonationAttributes(requestor, authenticationv1.SchemeGroupVersion, m.verb, "userextras", "")

	// perform extra sanity checks that would be backwards incompatible with legacy impersonation
	if m.isConstrainedImpersonation {
		if err := validateExtra(extra); err != nil {
			return responsewriters.ForbiddenStatusError(extraAttributes, err.Error())
		}
	}

	// if the requestor is trying to impersonate many extras at once, see if they are authorized to impersonate all extras
	// we only do this in constrained impersonation mode to avoid any behavioral changes with legacy impersonation
	if m.isConstrainedImpersonation && isLargeExtra(extra) {
		extraAttributes.Subresource = "*"
		extraAttributes.Name = "*"
		if err := checkAuthorization(ctx, m.authorizer, extraAttributes); err == nil {
			return nil
		}
	}

	for key, values := range extra {
		extraAttributes.Subresource = key
		for _, value := range values {
			extraAttributes.Name = value
			if err := checkAuthorization(ctx, m.authorizer, extraAttributes); err != nil {
				return err
			}
		}
	}

	return nil
}

// validateExtra 校验 extra 的键与值格式。
// 中文说明：键必须是小写的域名前缀路径（与 user.Info 约定一致），
// 空键、空值、空字符串值一律拒绝；这些约束在 legacy 模式下无法强制执行，
// 因此只在受限模式生效，以免破坏兼容性。
func validateExtra(extra map[string][]string) error {
	fp := field.NewPath("extra", "key")
	for key, values := range extra {
		if len(key) == 0 {
			return fmt.Errorf("impersonating the empty string key in extra is not allowed")
		}
		if err := utilvalidation.IsDomainPrefixedPath(fp, key).ToAggregate(); err != nil {
			return fmt.Errorf("impersonating an invalid key in extra is not allowed: %w", err)
		}
		if key != strings.ToLower(key) {
			return fmt.Errorf("impersonating a non-lowercase key in extra is not allowed: %q", key)
		}
		if len(values) == 0 {
			return fmt.Errorf("impersonating empty values in extra is not allowed")
		}
		if slices.Contains(values, "") {
			return fmt.Errorf("impersonating the empty string value in extra is not allowed")
		}
	}
	return nil
}

// isLargeExtra 判断 extra 的键值总数是否达到通配授权阈值。
func isLargeExtra(extra map[string][]string) bool {
	if len(extra) >= manyAuthorizationChecksInLoop {
		return true
	}
	var count int
	for _, values := range extra {
		count += len(values)
		if count >= manyAuthorizationChecksInLoop {
			return true
		}
	}
	return false
}

// impersonationAttributes 构造一次模拟授权检查的属性记录。
func impersonationAttributes(requestor user.Info, gv schema.GroupVersion, verb, resource, name string) authorizer.AttributesRecord {
	return authorizer.AttributesRecord{
		User:            requestor,
		Verb:            verb,
		APIGroup:        gv.Group,
		APIVersion:      gv.Version,
		Resource:        resource,
		Name:            name,
		ResourceRequest: true,
	}
}

// impersonateOnAttributes is a simple wrapper that updates the verb of the attributes to impersonate-on:<mode>:<verb>
// This allows the expression of "a subject can perform this verb while using this impersonation mode"
//
// 中文说明：通过改写在授权器看来是「另一个动词」，策略作者无需理解模拟实现细节，
// 即可表达「某主体可在该模式下执行某动词」。
type impersonateOnAttributes struct {
	mode string
	authorizer.Attributes
}

// GetVerb 返回 impersonate-on:<mode>:<verb> 形式的复合动词。
func (i *impersonateOnAttributes) GetVerb() string {
	return "impersonate-on:" + i.mode + ":" + i.Attributes.GetVerb()
}

// checkAuthorization 调用授权器并把结果转换为错误。
// 中文说明：先看 Decision 再看 err——RBAC 等授权器可能在求值出错时仍返回 Allow，
// 此时应以 Decision 为准。已是 Forbidden 的错误不再二次包装，保留原始信息。
func checkAuthorization(ctx context.Context, a authorizer.UnconditionalAuthorizer, attributes authorizer.Attributes) error {
	authorized, reason, err := a.Authorize(ctx, attributes)

	// an authorizer like RBAC could encounter evaluation errors and still allow the request, so authorizer decision is checked before error here.
	if authorized == authorizer.DecisionAllow {
		return nil
	}

	// if the authorizer gave us a forbidden error, do not wrap it again
	if errors.IsForbidden(err) {
		return err
	}

	msg := reason
	switch {
	case err != nil && len(reason) > 0:
		msg = fmt.Sprintf("%v: %s", err, reason)
	case err != nil:
		msg = err.Error()
	}

	return responsewriters.ForbiddenStatusError(attributes, msg)
}

// ensureGroup 在组列表中补齐指定组；若已存在则不动。
// 中文说明：复制后再追加，避免修改调用方传入的底层数组，
// 否则可能污染其他请求共享的切片。
func ensureGroup(u *user.DefaultInfo, group string) {
	if slices.Contains(u.Groups, group) {
		return
	}

	// do not mutate a slice that we did not create
	groups := make([]string, 0, len(u.Groups)+1)
	groups = append(groups, u.Groups...)
	groups = append(groups, group)
	u.Groups = groups
}

// isServiceAccountUsername 解析 system:serviceaccount:<ns>:<name> 形式的用户名。
func isServiceAccountUsername(username string) (namespace, name string, ok bool) {
	namespace, name, err := serviceaccount.SplitUsername(username)
	return namespace, name, err == nil
}

// matches the real ValidateNodeName from k8s.io/kubernetes/pkg/apis/core/validation
// which we are not allowed to import here
//
// 中文说明：staging 不能依赖 k8s.io/kubernetes，因此复用同一底层校验函数，
// 保持与真实 Node 名称规则一致。
var validateNodeName = validation.NameIsDNSSubdomain

// isNodeUsername 判断用户名是否为 system:node:<name> 且 name 合法。
func isNodeUsername(username string) (string, bool) {
	const nodeUsernamePrefix = "system:node:"
	if !strings.HasPrefix(username, nodeUsernamePrefix) {
		return "", false
	}
	name := strings.TrimPrefix(username, nodeUsernamePrefix)
	if len(validateNodeName(name, false)) != 0 {
		return "", false
	}
	return name, true
}

// requesterAssociatedWithRequestedNodeUsername 判断 requestor 是否为
// 「bound node 与目标节点一致」的 ServiceAccount。
// 中文说明：这是 associated-node 模式的安全前提；只有它为真，
// 跨节点共享缓存才是安全的。
func requesterAssociatedWithRequestedNodeUsername(requestor user.Info, username string) bool {
	nodeName, ok := isNodeUsername(username)
	if !ok {
		return false
	}
	if _, _, ok := isServiceAccountUsername(requestor.GetName()); !ok {
		return false
	}
	return getExtraValue(requestor, serviceaccount.NodeNameKey) == nodeName
}

// getExtraValue 返回 extra 中某键的唯一值；缺失或多值时返回空串。
func getExtraValue(u user.Info, key string) string {
	values := u.GetExtra()[key]
	if len(values) != 1 {
		return ""
	}
	return values[0]
}

// onlyUsernameSet 判断用户是否只设置了用户名（无 UID、组、extra）。
// 中文说明：受限的节点/SA 模式只允许指定用户名，其余字段由 API Server 生成；
// 该判断是这些模式的准入条件。
func onlyUsernameSet(u user.Info) bool {
	return len(u.GetUID()) == 0 && len(u.GetGroups()) == 0 && len(u.GetExtra()) == 0
}
