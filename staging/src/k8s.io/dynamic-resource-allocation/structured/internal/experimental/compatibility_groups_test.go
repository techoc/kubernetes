/*
Copyright The Kubernetes Authors.

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

package experimental

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
)

// TestCompatibilityGroupSet 验证 nil 列表与空列表都生成 nil 集合，即“空切片等同于 nil”。
// 输入分别为 nil、[] 和 ["mig"]，断言前两者返回 nil，后者返回只含 "mig" 的集合。
func TestCompatibilityGroupSet(t *testing.T) {
	// 输入 nil：期望返回 nil 集合。
	if got := compatibilityGroupSet(nil); got != nil {
		t.Errorf("compatibilityGroupSet(nil) = %v, want nil", got)
	}
	// 输入空切片：期望同样返回 nil 集合。
	if got := compatibilityGroupSet([]string{}); got != nil {
		t.Errorf("compatibilityGroupSet([]) = %v, want nil", got)
	}
	// 输入 ["mig"]：期望返回含单一元素 "mig" 的集合。
	if got := compatibilityGroupSet([]string{"mig"}); got.Len() != 1 || !got.Has("mig") {
		t.Errorf("compatibilityGroupSet([mig]) = %v, want {mig}", got)
	}
}

// TestCompatibilityGroupIntersectionAdmits 针对已有交集状态，测试单个设备的准入判定。
// 表驱动用例覆盖空交集、无组（nil）、单组、多组以及严格的 nil 匹配规则：
// intersection 为已有状态，candidate 为候选设备的组集合，want 为期望的准入结果。
func TestCompatibilityGroupIntersectionAdmits(t *testing.T) {
	// 辅助函数：把可变参数组名转成集合，无参数时返回 nil 集合代表“无兼容组”。
	set := func(groups ...string) sets.Set[string] {
		if len(groups) == 0 {
			return nil
		}
		return sets.New[string](groups...)
	}

	// 表驱动：intersection 初始状态、candidate 候选组、want 期望准入结果。
	for name, tc := range map[string]struct {
		intersection compatibilityGroupIntersection
		candidate    sets.Set[string]
		want         bool
	}{
		// 空交集接纳有组设备：尚未放置任何成员，任何候选都可接纳。
		"empty-intersection-admits-grouped": {
			intersection: compatibilityGroupIntersection{},
			candidate:    set("mig"),
			want:         true,
		},
		// 空交集接纳无组设备：同样因为还没有成员。
		"empty-intersection-admits-nil": {
			intersection: compatibilityGroupIntersection{},
			candidate:    set(),
			want:         true,
		},
		// 已有成员无组 + 候选无组（nil 对 nil）：严格 nil 匹配，准入。
		"no-group-members-admit-no-group-candidate (nil-vs-nil)": {
			intersection: compatibilityGroupIntersection{}.add(set()),
			candidate:    set(),
			want:         true,
		},
		// 已有成员无组 + 候选有组（nil 对集合）：不兼容，拒绝。
		"no-group-members-reject-grouped-candidate (nil-vs-set)": {
			intersection: compatibilityGroupIntersection{}.add(set()),
			candidate:    set("mig"),
			want:         false,
		},
		// 已有成员有组 + 候选无组（集合对 nil）：不兼容，拒绝。
		"grouped-members-reject-no-group-candidate (set-vs-nil)": {
			intersection: compatibilityGroupIntersection{}.add(set("mig")),
			candidate:    set(),
			want:         false,
		},
		// 已有成员有组 + 候选组重叠：准入。
		"grouped-members-admit-overlapping-candidate (set-vs-set)": {
			intersection: compatibilityGroupIntersection{}.add(set("mig")),
			candidate:    set("mig"),
			want:         true,
		},
		// 已有成员有组 + 候选组不相交：拒绝。
		"grouped-members-reject-disjoint-candidate": {
			intersection: compatibilityGroupIntersection{}.add(set("mig")),
			candidate:    set("vgpu"),
			want:         false,
		},
		// 多个组：候选与交集有公共组 foobar，准入。
		"multi-group-candidate-overlaps": {
			intersection: compatibilityGroupIntersection{}.add(set("foo", "foobar")),
			candidate:    set("bar", "foobar"),
			want:         true,
		},
		// 多个组：候选与交集无公共组，拒绝。
		"multi-group-candidate-disjoint": {
			intersection: compatibilityGroupIntersection{}.add(set("foo", "foobar")),
			candidate:    set("baz"),
			want:         false,
		},
	} {
		t.Run(name, func(t *testing.T) {
			// 断言 admits() 的返回值与期望值一致。
			if got := tc.intersection.admits(tc.candidate); got != tc.want {
				t.Errorf("admits() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestCompatibilityGroupIntersectionRolling 验证设备依次加入时的滚动交集行为，对应 KEP 示例 4：
// 一个设备可属于多个组，且随着设备加入交集不断收窄。输入为依次加入的 {foo,foobar}、
// {bar,foobar}，断言交集最终收窄为 {foobar}，随后无公共组的 {baz} 被拒绝，而仍共享 foobar
// 的 {qux,foobar} 被接纳。
func TestCompatibilityGroupIntersectionRolling(t *testing.T) {
	// 辅助函数：把组名可变参数转成集合。
	set := func(groups ...string) sets.Set[string] { return sets.New[string](groups...) }

	// 从空交集开始：foo 设备 {foo, foobar} 被接纳并作为交集初值。
	i := compatibilityGroupIntersection{}
	if !i.admits(set("foo", "foobar")) {
		t.Fatal("expected empty intersection to admit {foo, foobar}")
	}
	i = i.add(set("foo", "foobar"))

	// bar 设备 {bar, foobar} 与初值共享 foobar，被接纳，交集收窄为 {foobar}。
	if !i.admits(set("bar", "foobar")) {
		t.Fatal("expected {foo, foobar} to admit {bar, foobar} via foobar")
	}
	i = i.add(set("bar", "foobar"))
	// 断言滚动交集只剩 foobar。
	if i.groups.Len() != 1 || !i.groups.Has("foobar") {
		t.Fatalf("expected rolling intersection {foobar}, got %v", i.groups)
	}

	// baz 设备 {baz} 与收窄后的交集无公共组，应被拒绝。
	if i.admits(set("baz")) {
		t.Error("expected narrowed intersection {foobar} to reject {baz}")
	}
	// 另一个共享 foobar 的设备仍被接纳。
	if !i.admits(set("qux", "foobar")) {
		t.Error("expected narrowed intersection {foobar} to admit {qux, foobar}")
	}
}

// TestCompatibilityGroupIntersectionNoGroupsStacking 验证任意数量的无组设备可以自由叠加，
// 但一旦存在无组设备，有组设备就会被拒绝（反之亦然）。输入为连续加入 3 个 nil（无组）设备，
// 断言每次都被接纳，随后声明 "mig" 组的有组设备被拒绝。
func TestCompatibilityGroupIntersectionNoGroupsStacking(t *testing.T) {
	i := compatibilityGroupIntersection{}
	// 连续加入 3 个无组设备，每次都应被接纳并折叠进交集。
	for n := range 3 {
		if !i.admits(nil) {
			t.Fatalf("no-group device #%d should be admitted", n)
		}
		i = i.add(nil)
	}
	// 已有无组设备后，有组设备必须被拒绝。
	if i.admits(sets.New[string]("mig")) {
		t.Error("grouped device must be rejected once no-group devices are present")
	}
}
