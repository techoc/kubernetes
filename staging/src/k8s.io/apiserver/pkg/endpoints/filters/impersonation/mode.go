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
// constraint 记录「允许此次模拟的动词」，用于审计与错误信息；
// 它不参与授权判定，只用于解释结果来源。
type impersonatedUserInfo struct {
	user       user.Info
	constraint string // impersonationModeState.check 中允许该用户被模拟的那个动词。
}

// impersonationMode 表示一种具体的模拟模式：它检查请求方在模拟某个用户（wantedUser）时
// 是否被允许发起某个 API 请求（attributes）。若模式支持缓存，可能返回缓存结果
// （必要时使用传入的缓存键）。若该模式不支持模拟该 wantedUser，则返回 nil 的 impersonatedUserInfo。
//
// 每个模式负责两件事——先判断「该模式是否适用于本次请求」，再做授权检查。
// 返回 nil 表示不适用（继续尝试下一个模式），返回错误表示明确拒绝。
// 缓存由各模式自行决定；key 由调用方给出，但模式可以按需改造或忽略。
type impersonationMode interface {
	check(ctx context.Context, key *impersonationCacheKey, wantedUser *user.DefaultInfo, attributes authorizer.Attributes) (*impersonatedUserInfo, error)

	// 以下方法仅用于单元测试。
	verbForTests() string
	cachesForTests() (outer, inner *impersonationCache)
}

// constrainedImpersonationModeFilter 用于判断某个受约束的模拟模式是否支持
// requestor 模拟 wantedUser，它同时充当该模式的 sudo 授权检查。
//
// 过滤在授权之前执行，且不查缓存——因为缓存键不一定与 wantedUser+attributes
// 一一对应，必须先确认模式适用，否则可能命中为其他输入准备的缓存项。
type constrainedImpersonationModeFilter func(wantedUser *user.DefaultInfo, requestor user.Info) bool

// allImpersonationModes 返回按优先级排列的全部模式。
// 顺序即尝试顺序；associated-node 放在最前，因为它对节点代理有专门优化，
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

// associatedNodeImpersonationMode 允许 requestor ServiceAccount 模拟与它关联的节点。
// 这是最复杂的一种模拟模式：它对成功的模拟结果做缓存，使得同一个 ServiceAccount
// 在不同节点上的不同 Pod 中使用时（例如以 DaemonSet 运行的节点代理）仍能保持很高的缓存命中率。
// 请求方只能指定用户名，user.Info 的其余字段都由 API Server 控制。
//
// 缓存键刻意抹去节点名与 extra 值（用 "system:node:*" 作占位），
// 使同一 ServiceAccount 在所有节点上共享缓存；安全性由前置的「节点关联校验」保证——
// 只有确认该 SA 的 bound node 与目标节点一致时才会走到这里。
func associatedNodeImpersonationMode(a authorizer.UnconditionalAuthorizer) impersonationMode {
	// 我们包装 authorizer，以便覆盖 requestor ServiceAccount 的 extra 取值
	// 以及授权检查中使用的节点名。这样我们的授权检查与缓存键的语义
	// 精确一致，从而避免缓存命中时出现意外的权限提升。
	// 缓存键的细节见下方注释。
	wrappedAuthorizer := authorizer.AuthorizerFunc(func(ctx context.Context, attributes authorizer.Attributes) (authorizer.Decision, string, error) {
		// 这里使用 checkAuthorization 而不是直接调用 authorizer，
		// 以便让错误信息与实际用于授权的 attributes 保持一致。
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
// 忽略传入 key 是有意为之——见下方注释，缓存语义需要跨节点共享。
func (a *associatedNodeImpersonationCheck) check(ctx context.Context, _ *impersonationCacheKey, wantedUser *user.DefaultInfo, attributes authorizer.Attributes) (*impersonatedUserInfo, error) {
	wantedNodeName := wantedUser.Name
	// 忽略传入的缓存键，因为 associated-node 的缓存语义需要自定义逻辑。
	// 能用到这个键时，filter 已经确认 requestor 是一个 ServiceAccount，
	// 且其 node ref 与要模拟的节点匹配。
	// 实际被模拟的节点与缓存键无关，因此这里只使用一个固定的 wantedUser。
	// 我们包装 attributes，以便丢弃 requestor ServiceAccount 的 extra 取值。
	// 这样同一 ServiceAccount 在所有节点上的缓存键都相同。
	// 只有依赖前述“filter 先于缓存查询执行”这一前提，这样做才是安全的。
	key := &impersonationCacheKey{wantedUser: &user.DefaultInfo{Name: "system:node:*"}, attributes: &associatedNodeImpersonationAttributes{Attributes: attributes}}
	impersonatedNodeWithMaybeIncorrectUsername, err := a.mode.check(ctx, key, wantedUser, attributes)
	if err != nil || impersonatedNodeWithMaybeIncorrectUsername == nil {
		return nil, err
	}
	// 走到这里说明 associated-node 模拟已成功。
	// 结果可能来自缓存，因此可能对应任意节点，所以这里再包装一次结果，
	// 使用户名与 requestor ServiceAccount 关联的节点一致。
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
// 因为缓存键忽略节点名，授权检查必须对「所有节点名」都成立，
// 否则会出现「授权了 A 节点、却命中 B 节点的缓存」这种越权。
func (a *associatedNodeImpersonationAttributes) GetName() string {
	if a.GetVerb() == "impersonate:associated-node" {
		return "*" // 缓存键忽略节点名，因此授权检查必须对所有节点名都成立
	}
	return a.Attributes.GetName()
}

// associatedNodeImpersonationRequestorUserInfo 抹去 requestor 的 extra 值。
type associatedNodeImpersonationRequestorUserInfo struct {
	user.Info
}

// GetExtra 只保留 extra 的键名集合，丢弃具体值。
// 节点名等值会让缓存键随节点变化，导致命中率极低；
// 保留键名是为了让不同类型的 bound object ref 产生不同缓存键，维持隔离。
// 完整 extra 仍会出现在审计日志中。
func (a *associatedNodeImpersonationRequestorUserInfo) GetExtra() map[string][]string {
	return map[string][]string{
		// 此处已知 requestor 是 ServiceAccount，且其 node ref 与要模拟的节点匹配。
		// 基本上所有 extra 的取值都会造成缓存未命中（例如节点名本身），
		// 因此丢弃全部 extra 取值但保留其键名。
		// authorizer 可以相信我们已经正确完成了节点关联校验。
		// 审计日志中仍会保留 requestor 完整的 extra 字段。
		// 不同类型的 bound object ref 会产生不同的 extra 键，从而得到不同的缓存键。
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

// arbitraryNodeImpersonationMode 为节点实现受约束的模拟。
// 请求方只能指定用户名，user.Info 的其余字段都由 API Server 控制。
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

// serviceAccountImpersonationMode 为 ServiceAccount 实现受约束的模拟。
// 请求方只能指定用户名，user.Info 的其余字段都由 API Server 控制。
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

// userInfoImpersonationMode 为非节点、非 ServiceAccount 的用户实现受约束的模拟。
// 与其他受约束模式不同，它支持模拟 user.Info 的全部字段。
//
// 排除节点与 ServiceAccount 用户名，是因为它们已有专门模式且
// 其组/extra 由 API Server 固定生成；若允许在此模式模拟，会绕过这些固定语义。
func userInfoImpersonationMode(a authorizer.UnconditionalAuthorizer) impersonationMode {
	return newConstrainedImpersonationMode(a, "user-info",
		func(wantedUser *user.DefaultInfo, _ user.Info) bool {
			// 在该模式下不能模拟节点和 ServiceAccount。
			// user-info 这个“桶”专门留给其他用户，即那些
			// Kubernetes 没有为其定义显式 schema 的用户。
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

// legacyImpersonationMode 完全重新实现了 kube 自 v1.3 起就存在的原始模拟模式。
// 其行为被期望与原实现保持一致。
//
// 保留原语义（使用 impersonate 动词、无外层缓存、无额外校验），
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
	// legacy 模拟没有外层，直接返回空缓存；
	// 虽有内层缓存，但并未使用。
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

// constrainedImpersonationModeState 通过 impersonate:<mode> 动词实现次级授权检查：
// 判断 requestor 是否在模拟 wantedUser 时被授权执行这个具体动词。
// 该检查通过后，才会执行主授权检查（见 impersonationModeState）。
// 若模式的 filter 与输入不匹配，模拟直接失败并返回 nil。
//
// 两层授权——先检查「是否允许在此模式下执行该动词」（sudo 检查），
// 再检查「是否允许模拟该用户的具体字段」。外层缓存覆盖整体结果，命中率高时省去两轮检查。
type constrainedImpersonationModeState struct {
	state *impersonationModeState
	// 这层外层缓存覆盖该模式的整体模拟结果：命中这里就意味着后续
	// skipAttributes 为 false，即该缓存取决于所发出的请求，而不仅是被模拟的用户；
	// 预期命中率较低，因为 requestor 很少用完全相同的方式重复同一请求。
	cache      *impersonationCache
	authorizer authorizer.UnconditionalAuthorizer
	mode       string
	filter     constrainedImpersonationModeFilter
}

// check 依次执行过滤、外层缓存、sudo 授权、主授权与缓存写入。
func (c *constrainedImpersonationModeState) check(ctx context.Context, key *impersonationCacheKey, wantedUser *user.DefaultInfo, attributes authorizer.Attributes) (*impersonatedUserInfo, error) {
	requestor := attributes.GetUser()
	// 必须先调用 filter，因为它同时充当 sudo 授权检查；
	// 而且缓存键并不总是与 wantedUser+attributes 直接对应，所以同样必须先调用它。
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

// impersonationModeState 通过 impersonate:<mode> 动词实现主授权检查，
// 对 legacy 模拟则使用 impersonate 动词。wantedUser 中每设置一个字段，
// 都会触发一次或多次授权检查，以判断 requestor 是否有权模拟该字段。
//
// 内层缓存只取决于「被模拟的用户」，与具体请求无关（skipAttributes=true），
// 因此同一用户被反复模拟时命中率很高。legacy 模式不启用该缓存以保持行为不变。
type impersonationModeState struct {
	authorizer                 authorizer.UnconditionalAuthorizer
	verb                       string
	isConstrainedImpersonation bool

	usernameAndGroupGV schema.GroupVersion
	constraint         string

	// 这层内层缓存覆盖 wantedUser 中各具体字段相关的检查。
	// skipAttributes 为 true，即该缓存只取决于 requestor 要模拟的那个用户，
	// 与请求内容无关。预期命中率很高，因为 requestor 常会针对不同请求模拟同一个用户。
	cache *impersonationCache
}

// newImpersonationModeState 根据是否为受限模式选择 API 组与约束标识。
// 受限模式使用 authentication.k8s.io/v1 的虚拟资源（users/groups/uids），
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
// 字段检查顺序为用户名 → UID → 组 → extra；任一失败即拒绝。
// 最后根据匿名与否补齐 system:unauthenticated / system:authenticated，
// 使模拟出的用户与真实认证用户的组集合一致。
func (m *impersonationModeState) check(ctx context.Context, key *impersonationCacheKey, wantedUser *user.DefaultInfo, requestor user.Info) (*impersonatedUserInfo, error) {
	// 仅在 constrained 模拟模式下使用缓存，以免给 legacy 模拟模式带来任何行为变化。
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
// 受限模式要求节点与 SA 不得同时指定组（其组由系统固定生成）；
// 若未指定组，则填入对应的固定组，保证模拟结果可预测且不可被调用方篡改。
func (m *impersonationModeState) authorizeUsername(ctx context.Context, requestor user.Info, username string, wantedUserGroups []string, actualUser *user.DefaultInfo) error {
	usernameAttributes := impersonationAttributes(requestor, m.usernameAndGroupGV, m.verb, "users", username)

	if m.isConstrainedImpersonation {
		if name, ok := isNodeUsername(username); ok {
			usernameAttributes.Resource = "nodes"
			usernameAttributes.Name = name

			// 理论上不可能走到这里，但仍做检查。
			if len(wantedUserGroups) != 0 {
				return responsewriters.ForbiddenStatusError(usernameAttributes, fmt.Sprintf("when impersonating a node, cannot impersonate groups %q", wantedUserGroups))
			}

			actualUser.Groups = []string{user.NodesGroup} // constrained 模式下所有节点都有固定的组列表。
		}
	}

	if namespace, name, ok := isServiceAccountUsername(username); ok {
		usernameAttributes.Resource = "serviceaccounts"
		usernameAttributes.Namespace = namespace
		usernameAttributes.Name = name

		// 理论上不可能走到这里，但仍做检查。
		if m.isConstrainedImpersonation && len(wantedUserGroups) != 0 {
			return responsewriters.ForbiddenStatusError(usernameAttributes, fmt.Sprintf("when impersonating a service account, cannot impersonate groups %q", wantedUserGroups))
		}

		if len(wantedUserGroups) == 0 {
			// 若未给 ServiceAccount 指定组，我们已知其组是固定映射，因此可以自行构造。
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

// manyAuthorizationChecksInLoop 是 constrained 模式中使用的一个任意阈值：
// 当待检查的条目多于该值时，先尝试一次通配授权检查，而不是一一检查。
//
// 当组/extra 数量较多时，先尝试一次通配授权，可把 N 次授权压缩为 1 次；
// 4 是经验阈值，平衡「多一次可能失败的检查」与「循环开销」。
const manyAuthorizationChecksInLoop = 4

// authorizeGroups 对组列表执行授权。
// 受限模式额外禁止模拟空字符串组与 system:masters——后者等价于提权。
// 数量达到阈值时先尝试通配授权。
func (m *impersonationModeState) authorizeGroups(ctx context.Context, requestor user.Info, groups []string) error {
	if len(groups) == 0 {
		return nil
	}

	groupAttributes := impersonationAttributes(requestor, m.usernameAndGroupGV, m.verb, "groups", "")

	// 执行那些与 legacy 模拟不向后兼容的额外安全检查。
	if m.isConstrainedImpersonation {
		if slices.Contains(groups, "") {
			return responsewriters.ForbiddenStatusError(groupAttributes, "impersonating the empty string group is not allowed")
		}
		if slices.Contains(groups, user.SystemPrivilegedGroup) {
			groupAttributes.Name = user.SystemPrivilegedGroup
			return responsewriters.ForbiddenStatusError(groupAttributes, "impersonating the system:masters group is not allowed")
		}
	}

	// 若 requestor 一次性模拟多个组，检查其是否被授权模拟这些组。
	// 只在 constrained 模式下这么做，以避免改变 legacy 模式的行为。
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
// extra 的键作为子资源、值作为对象名参与授权，因此策略可以精确到
// 「允许模拟某个键的某个值」。受限模式会先校验键格式与值非空，
// 防止畸形 extra 进入下游或被用于构造歧义键。
func (m *impersonationModeState) authorizeExtra(ctx context.Context, requestor user.Info, extra map[string][]string) error {
	if len(extra) == 0 {
		return nil
	}

	extraAttributes := impersonationAttributes(requestor, authenticationv1.SchemeGroupVersion, m.verb, "userextras", "")

	// 执行那些与 legacy 模拟不向后兼容的额外安全检查。
	if m.isConstrainedImpersonation {
		if err := validateExtra(extra); err != nil {
			return responsewriters.ForbiddenStatusError(extraAttributes, err.Error())
		}
	}

	// 若 requestor 一次性模拟多个 extra，检查其是否被授权模拟这些 extra。
	// 只在 constrained 模式下这么做，以避免改变 legacy 模式的行为。
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
// 键必须是小写的域名前缀路径（与 user.Info 约定一致），
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

// impersonateOnAttributes 是一个简单的包装，把属性的动词改成 impersonate:<mode>，
// 从而表达“某主体可以在使用该模拟模式的前提下执行这个动词”。
//
// 通过改写在授权器看来是「另一个动词」，策略作者无需理解模拟实现细节，
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
// 先看 Decision 再看 err——RBAC 等授权器可能在求值出错时仍返回 Allow，
// 此时应以 Decision 为准。已是 Forbidden 的错误不再二次包装，保留原始信息。
func checkAuthorization(ctx context.Context, a authorizer.UnconditionalAuthorizer, attributes authorizer.Attributes) error {
	authorized, reason, err := a.Authorize(ctx, attributes)

	// 像 RBAC 这样的 authorizer 可能在遇到评估错误的同时仍放行请求，
	// 因此这里先检查授权决定，再检查错误。
	if authorized == authorizer.DecisionAllow {
		return nil
	}

	// 若 authorizer 给出的本来就是 forbidden 错误，不要再包一层。
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
// 复制后再追加，避免修改调用方传入的底层数组，
// 否则可能污染其他请求共享的切片。
func ensureGroup(u *user.DefaultInfo, group string) {
	if slices.Contains(u.Groups, group) {
		return
	}

	// 不要修改并非由我们创建的 slice。
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

// 这里的实现与 k8s.io/kubernetes/pkg/apis/core/validation 中真实的 ValidateNodeName 保持一致，
// 而那个包在本处不允许被导入。
//
// staging 不能依赖 k8s.io/kubernetes，因此复用同一底层校验函数，
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
// 这是 associated-node 模式的安全前提；只有它为真，
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
// 受限的节点/SA 模式只允许指定用户名，其余字段由 API Server 生成；
// 该判断是这些模式的准入条件。
func onlyUsernameSet(u user.Info) bool {
	return len(u.GetUID()) == 0 && len(u.GetGroups()) == 0 && len(u.GetExtra()) == 0
}
