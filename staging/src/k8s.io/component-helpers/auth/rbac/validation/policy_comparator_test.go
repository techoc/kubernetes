/*
Copyright 2016 The Kubernetes Authors.

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
	"reflect"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
)

// escalationTest 把 owner 与 servant 两组规则以及期望的“覆盖/未覆盖”结果放在一起，
// 所有用例都通过 test 方法跑同一套断言，便于集中维护授权提升判定语义。
type escalationTest struct {
	ownerRules   []rbacv1.PolicyRule // 授权方（能力更强）的规则集合，即“覆盖者”。
	servantRules []rbacv1.PolicyRule // 被授权方的规则集合，即“被覆盖者”。

	expectedCovered        bool                // 期望的整体覆盖结论。
	expectedUncoveredRules []rbacv1.PolicyRule // 期望返回的、未被覆盖的最小规则列表。
}

// 最基础的覆盖：分组、动词、资源完全相同，说明覆盖判定不是靠名字比较，
// 而是按规则三元组逐项比对，后续用例都在这个基线上演变。
func TestCoversExactMatch(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"get"}, Resources: []string{"builds"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"get"}, Resources: []string{"builds"}},
		},

		expectedCovered:        true,
		expectedUncoveredRules: []rbacv1.PolicyRule{},
	}.test(t)
}

// 子资源通配符 */scale 应当覆盖具体资源的 /scale：
// 这是“授权者用通配写法、被授权者用具体写法”不会造成权限提升的前提。
func TestCoversSubresourceWildcard(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"get"}, Resources: []string{"*/scale"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"get"}, Resources: []string{"foo/scale"}},
		},

		expectedCovered:        true,
		expectedUncoveredRules: []rbacv1.PolicyRule{},
	}.test(t)
}

// 分散在多条规定里的权限合起来覆盖一条复合规则：覆盖是按并集判断的，
// 否则用户把一条大规则拆成几条等价小规则就会被误判为提权。
func TestCoversMultipleRulesCoveringSingleRule(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"delete"}, Resources: []string{"deployments"}},
			{APIGroups: []string{"v1"}, Verbs: []string{"delete"}, Resources: []string{"builds"}},
			{APIGroups: []string{"v1"}, Verbs: []string{"update"}, Resources: []string{"builds", "deployments"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"delete", "update"}, Resources: []string{"builds", "deployments"}},
		},

		expectedCovered:        true,
		expectedUncoveredRules: []rbacv1.PolicyRule{},
	}.test(t)

}

// 反向的场景：一条规则里列出多个 API 组，由多条单组规则共同覆盖，
// 确保比较时会把 APIGroups 展开成笛卡尔积而不是整段字符串比较。
func TestCoversMultipleAPIGroupsCoveringSingleRule(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"group1"}, Verbs: []string{"delete"}, Resources: []string{"deployments"}},
			{APIGroups: []string{"group1"}, Verbs: []string{"delete"}, Resources: []string{"builds"}},
			{APIGroups: []string{"group1"}, Verbs: []string{"update"}, Resources: []string{"builds", "deployments"}},
			{APIGroups: []string{"group2"}, Verbs: []string{"delete"}, Resources: []string{"deployments"}},
			{APIGroups: []string{"group2"}, Verbs: []string{"delete"}, Resources: []string{"builds"}},
			{APIGroups: []string{"group2"}, Verbs: []string{"update"}, Resources: []string{"builds", "deployments"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"group1", "group2"}, Verbs: []string{"delete", "update"}, Resources: []string{"builds", "deployments"}},
		},

		expectedCovered:        true,
		expectedUncoveredRules: []rbacv1.PolicyRule{},
	}.test(t)

}

// 一条多组规则覆盖多条单组规则，验证覆盖关系在两个方向上都能成立，
// 避免实现只对“多拆少”生效。
func TestCoversSingleAPIGroupsCoveringMultiple(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"group1", "group2"}, Verbs: []string{"delete", "update"}, Resources: []string{"builds", "deployments"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"group1"}, Verbs: []string{"delete"}, Resources: []string{"deployments"}},
			{APIGroups: []string{"group1"}, Verbs: []string{"delete"}, Resources: []string{"builds"}},
			{APIGroups: []string{"group1"}, Verbs: []string{"update"}, Resources: []string{"builds", "deployments"}},
			{APIGroups: []string{"group2"}, Verbs: []string{"delete"}, Resources: []string{"deployments"}},
			{APIGroups: []string{"group2"}, Verbs: []string{"delete"}, Resources: []string{"builds"}},
			{APIGroups: []string{"group2"}, Verbs: []string{"update"}, Resources: []string{"builds", "deployments"}},
		},

		expectedCovered:        true,
		expectedUncoveredRules: []rbacv1.PolicyRule{},
	}.test(t)

}

// 覆盖判定必须精确到“动词 x 资源”的组合：仅因为每个动词和资源各自出现过，
// 就认为组合权限也被覆盖，会漏掉真实存在的提权。
func TestCoversMultipleRulesMissingSingleVerbResourceCombination(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"delete", "update"}, Resources: []string{"builds", "deployments"}},
			{APIGroups: []string{"v1"}, Verbs: []string{"delete"}, Resources: []string{"pods"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"delete", "update"}, Resources: []string{"builds", "deployments", "pods"}},
		},

		expectedCovered: false,
		expectedUncoveredRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"update"}, Resources: []string{"pods"}},
		},
	}.test(t)
}

// 星号 API 组覆盖枚举组：这是通配符的基本语义，
// 也是防止“把 * 换成若干具体组名就能绕过限制”的关键判定。
func TestCoversAPIGroupStarCoveringMultiple(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"*"}, Verbs: []string{"get"}, Resources: []string{"roles"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"group1", "group2"}, Verbs: []string{"get"}, Resources: []string{"roles"}},
		},

		expectedCovered:        true,
		expectedUncoveredRules: []rbacv1.PolicyRule{},
	}.test(t)
}

// 枚举组不能覆盖星号组：即使列尽当前已知的组，也无法约束未来新增的组，
// 所以这种方向必须判为未覆盖，并把星号规则原样报出来。
func TestCoversEnumerationNotCoveringAPIGroupStar(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"dummy-group"}, Verbs: []string{"get"}, Resources: []string{"roles"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"*"}, Verbs: []string{"get"}, Resources: []string{"roles"}},
		},

		expectedCovered: false,
		expectedUncoveredRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"*"}, Verbs: []string{"get"}, Resources: []string{"roles"}},
		},
	}.test(t)
}

// 两边都是星号时也必须覆盖，防止通配符比较因“相等但算特殊值”被误判。
func TestCoversAPIGroupStarCoveringStar(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"*"}, Verbs: []string{"get"}, Resources: []string{"roles"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"*"}, Verbs: []string{"get"}, Resources: []string{"roles"}},
		},

		expectedCovered:        true,
		expectedUncoveredRules: []rbacv1.PolicyRule{},
	}.test(t)
}

// 动词星号覆盖枚举动词，语义与 API 组星号一致。
func TestCoversVerbStarCoveringMultiple(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"*"}, Resources: []string{"roles"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"watch", "list"}, Resources: []string{"roles"}},
		},

		expectedCovered:        true,
		expectedUncoveredRules: []rbacv1.PolicyRule{},
	}.test(t)
}

// 即使把常见动词全部列出来，也不能覆盖星号：RBAC 允许自定义子资源动词，
// 枚举永远无法穷举，所以必须保守地判为未覆盖。
func TestCoversEnumerationNotCoveringVerbStar(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"get", "list", "watch", "create", "update", "delete", "exec"}, Resources: []string{"roles"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"*"}, Resources: []string{"roles"}},
		},

		expectedCovered: false,
		expectedUncoveredRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"*"}, Resources: []string{"roles"}},
		},
	}.test(t)
}

// 两边都是动词星号时视为覆盖，和 API 组、资源的星号用例形成对称检查。
func TestCoversVerbStarCoveringStar(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"*"}, Resources: []string{"roles"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"*"}, Resources: []string{"roles"}},
		},

		expectedCovered:        true,
		expectedUncoveredRules: []rbacv1.PolicyRule{},
	}.test(t)
}

// 资源星号覆盖带冒号的扩展资源名（如 resourcegroup:deployments），
// 确认比较逻辑按完整资源字符串匹配，而不是只认内建资源类型。
func TestCoversResourceStarCoveringMultiple(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"get"}, Resources: []string{"*"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"get"}, Resources: []string{"resourcegroup:deployments"}},
		},

		expectedCovered:        true,
		expectedUncoveredRules: []rbacv1.PolicyRule{},
	}.test(t)
}

// 枚举资源不能覆盖星号，理由同动词：未来新增资源不该自动落进被授权范围。
func TestCoversEnumerationNotCoveringResourceStar(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"get"}, Resources: []string{"roles", "resourcegroup:deployments"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"get"}, Resources: []string{"*"}},
		},

		expectedCovered: false,
		expectedUncoveredRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"get"}, Resources: []string{"*"}},
		},
	}.test(t)
}

// 两边都是资源星号时视为覆盖，补齐三种星号维度的对称用例。
func TestCoversResourceStarCoveringStar(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"get"}, Resources: []string{"*"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"get"}, Resources: []string{"*"}},
		},

		expectedCovered:        true,
		expectedUncoveredRules: []rbacv1.PolicyRule{},
	}.test(t)
}

// ResourceNames 为空表示不限定名称，因此覆盖列出的具体名称；
// 若把空列表误当作“没有权限”，会拒绝掉大量合法的授权继承。
func TestCoversResourceNameEmptyCoveringMultiple(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"get"}, Resources: []string{"pods"}, ResourceNames: []string{}},
		},
		servantRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"get"}, Resources: []string{"pods"}, ResourceNames: []string{"foo", "bar"}},
		},

		expectedCovered:        true,
		expectedUncoveredRules: []rbacv1.PolicyRule{},
	}.test(t)
}

// 反过来，列出的名称不能覆盖“所有名称”：被授权者拿到的是全集，
// 未覆盖时结果里要去掉 ResourceNames 字段，表示缺失的是整个资源级权限。
func TestCoversEnumerationNotCoveringResourceNameEmpty(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"get"}, Resources: []string{"pods"}, ResourceNames: []string{"foo", "bar"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"get"}, Resources: []string{"pods"}, ResourceNames: []string{}},
		},

		expectedCovered: false,
		expectedUncoveredRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"get"}, Resources: []string{"pods"}},
		},
	}.test(t)
}

// 非资源 URL 和普通资源走相似的覆盖逻辑，但字段来源是 NonResourceURLs；
// 用一个最简用例确认比较器确实读取了这个字段。
func TestCoversNonResourceURLs(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{NonResourceURLs: []string{"/apis"}, Verbs: []string{"*"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{NonResourceURLs: []string{"/apis"}, Verbs: []string{"*"}},
		},

		expectedCovered:        true,
		expectedUncoveredRules: []rbacv1.PolicyRule{},
	}.test(t)
}

// 非资源 URL 的星号覆盖所有路径，等同于资源维度的通配符。
func TestCoversNonResourceURLsStar(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{NonResourceURLs: []string{"*"}, Verbs: []string{"*"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{NonResourceURLs: []string{"/apis", "/apis/v1", "/"}, Verbs: []string{"*"}},
		},

		expectedCovered:        true,
		expectedUncoveredRules: []rbacv1.PolicyRule{},
	}.test(t)
}

// /apis/* 只覆盖该前缀“下面”的路径，不覆盖前缀本身：
// 前缀节点和子节点是两个独立的 URL，不能因为字符串像就被算进去。
func TestCoversNonResourceURLsStarAfterPrefixDoesntCover(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{NonResourceURLs: []string{"/apis/*"}, Verbs: []string{"*"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{NonResourceURLs: []string{"/apis", "/apis/v1"}, Verbs: []string{"get"}},
		},

		expectedCovered: false,
		expectedUncoveredRules: []rbacv1.PolicyRule{
			{NonResourceURLs: []string{"/apis"}, Verbs: []string{"get"}},
		},
	}.test(t)
}

// 与上一个用例互补：同一前缀下的子孙路径应被 /apis/* 覆盖，
// 且比较要考虑多层级，而非只看直接子节点。
func TestCoversNonResourceURLsStarAfterPrefix(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{NonResourceURLs: []string{"/apis/*"}, Verbs: []string{"*"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{NonResourceURLs: []string{"/apis/v1/foo", "/apis/v1"}, Verbs: []string{"get"}},
		},

		expectedCovered:        true,
		expectedUncoveredRules: []rbacv1.PolicyRule{},
	}.test(t)
}

// 一条规则可以同时含资源字段和非资源 URL 字段；两边一致时不应因为
// 比较器只跑了其中一条路径就误判，这里确认两组字段都被纳入判断。
func TestCoversNonResourceURLsWithOtherFields(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"get"}, Resources: []string{"builds"}, NonResourceURLs: []string{"/apis"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"get"}, Resources: []string{"builds"}, NonResourceURLs: []string{"/apis"}},
		},

		expectedCovered:        true,
		expectedUncoveredRules: []rbacv1.PolicyRule{},
	}.test(t)
}

// 被授权者比授权者多出 NonResourceURLs 时必须判为未覆盖，
// 且未覆盖结果只保留多出来的那部分，方便直接向用户指出哪段权限有问题。
func TestCoversNonResourceURLsWithOtherFieldsFailure(t *testing.T) {
	escalationTest{
		ownerRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"get"}, Resources: []string{"builds"}},
		},
		servantRules: []rbacv1.PolicyRule{
			{APIGroups: []string{"v1"}, Verbs: []string{"get"}, Resources: []string{"builds"}, NonResourceURLs: []string{"/apis"}},
		},

		expectedCovered:        false,
		expectedUncoveredRules: []rbacv1.PolicyRule{{NonResourceURLs: []string{"/apis"}, Verbs: []string{"get"}}},
	}.test(t)
}

// test 执行该用例的公共断言：调用一次 Covers，同时核对覆盖结论和未覆盖规则列表。
func (test escalationTest) test(t *testing.T) {
	// 所有用例共用这一处断言，保证“是否覆盖”和“未覆盖规则列表”的结论
	// 始终来自同一次 Covers 调用，不会因分别调用而产生口径差异。
	actualCovered, actualUncoveredRules := Covers(test.ownerRules, test.servantRules)

	if actualCovered != test.expectedCovered {
		t.Errorf("expected %v, but got %v", test.expectedCovered, actualCovered)
	}

	if !rulesMatch(test.expectedUncoveredRules, actualUncoveredRules) {
		t.Errorf("expected %v, but got %v", test.expectedUncoveredRules, actualUncoveredRules)
	}
}

// rulesMatch 按集合语义比较规则列表：Covers 不保证输出顺序，
// 所以这里只要求每条期望规则都能在实际结果里找到对应项。
func rulesMatch(expectedRules, actualRules []rbacv1.PolicyRule) bool {
	if len(expectedRules) != len(actualRules) {
		return false
	}

	for _, expectedRule := range expectedRules {
		found := false
		for _, actualRule := range actualRules {
			if reflect.DeepEqual(expectedRule, actualRule) {
				found = true
				break
			}
		}

		if !found {
			return false
		}
	}

	return true
}

// TestNonResourceURLCovers 单独锁死 URL 通配符的匹配边界：
// 末尾的 /* 只匹配后代，裸 * 匹配所有路径，中间的 * 不做跨层级匹配。
// 这些规则一改就会直接影响授权提升判定的结果。
func TestNonResourceURLCovers(t *testing.T) {
	// 每个用例中 owner 是授权方的 URL，requested 是被授权方请求的 URL，want 是期望的覆盖结论。
	tests := []struct {
		owner     string // 授权方 URL，末尾的 * 表示通配
		requested string // 被授权方请求的 URL
		want      bool   // 期望 owner 是否覆盖 requested
	}{
		// 裸 * 匹配所有路径，包括空串和根路径。
		{"*", "", true},
		{"*", "/", true},
		{"*", "/api", true},
		// /* 只覆盖前缀下的后代路径，不覆盖前缀本身（此处前缀节点为空串）。
		{"/*", "", false},
		{"/*", "/", true},
		{"/*", "/foo", true},
		// 精确相等才覆盖；无包含关系的路径互不覆盖，多级路径也不能反向匹配其祖先。
		{"/api", "/api", true},
		{"/apis", "/api", false},
		{"/api/v1", "/api", false},
		{"/api/v1", "/api/v1", true},
		// 带 /* 的前缀覆盖其后代，但不覆盖前缀节点本身。
		{"/api/*", "/api/v1", true},
		{"/api/*", "/api", false},
		// 中间的 * 不构成跨层级通配，多段通配写法也无法匹配中间缺层的路径。
		{"/api/*/*", "/api/v1", false},
		{"/*/v1/*", "/api/v1", false},
	}

	for _, tc := range tests {
		got := nonResourceURLCovers(tc.owner, tc.requested)
		if got != tc.want {
			t.Errorf("nonResourceURLCovers(%q, %q): want=(%t), got=(%t)", tc.owner, tc.requested, tc.want, got)
		}
	}
}
