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

package statefulset

import (
	"os"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"sigs.k8s.io/json"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
)

// TestStatefulSetCompatibility 验证跨版本兼容性：用 1.33/1.34 版本的 StatefulSet
// 及其 ControllerRevision 固定样本，调用 getStatefulSetRevisions 计算当前/更新 revision，
// 断言结果均与最后一个（最新）revision 完全一致，即历史版本不会触发无谓的修订变更。
func TestStatefulSetCompatibility(t *testing.T) {
	// 加载两个版本的固定样本；每个用例使用 DeepCopy 防止用例间共享状态被相互污染。
	set133 := &appsv1.StatefulSet{}
	set134 := &appsv1.StatefulSet{}
	rev133 := &appsv1.ControllerRevision{}
	rev134 := &appsv1.ControllerRevision{}
	load(t, "compatibility_set_1.33.0.json", set133)
	load(t, "compatibility_set_1.34.0.json", set134)
	load(t, "compatibility_revision_1.33.0.json", rev133)
	load(t, "compatibility_revision_1.34.0.json", rev134)

	testcases := []struct {
		name      string
		set       *appsv1.StatefulSet
		revisions []*appsv1.ControllerRevision
	}{
		{
			name:      "1.33 set, 1.33 rev",
			set:       set133.DeepCopy(),
			revisions: []*appsv1.ControllerRevision{rev133.DeepCopy()},
		},
		{
			name:      "1.34 set, 1.34 rev",
			set:       set134.DeepCopy(),
			revisions: []*appsv1.ControllerRevision{rev134.DeepCopy()},
		},
		{
			name:      "1.34 set, 1.33+1.34 rev",
			set:       set134.DeepCopy(),
			revisions: []*appsv1.ControllerRevision{rev133.DeepCopy(), rev134.DeepCopy()},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// revisions 按版本升序排列，最后一个即为最新 revision，作为期望结果。
			latestRev := tc.revisions[len(tc.revisions)-1]
			client := fake.NewClientset(tc.set)
			_, _, ssc := setupController(client)
			currentRev, updateRev, _, err := ssc.(*defaultStatefulSetControl).getStatefulSetRevisions(tc.set, tc.revisions)
			if err != nil {
				t.Fatal(err)
			}
			// currentRev 与 updateRev 都必须等于最新 revision：
			// 否则说明跨版本样本被误判为模板变更，会触发非预期的滚动更新。
			if !reflect.DeepEqual(currentRev, latestRev) {
				t.Fatalf("expected no change from latestRev, got %s", cmp.Diff(latestRev, currentRev))
			}
			if !reflect.DeepEqual(updateRev, latestRev) {
				t.Fatalf("expected no change from latestRev, got %s", cmp.Diff(latestRev, updateRev))
			}
		})
	}
}

// BenchmarkStatefulSetCompatibility 对 getStatefulSetRevisions 做基准测试，
// 用例与 TestStatefulSetCompatibility 相同，用于衡量跨版本 revision 计算的开销。
func BenchmarkStatefulSetCompatibility(b *testing.B) {
	set133 := &appsv1.StatefulSet{}
	set134 := &appsv1.StatefulSet{}
	rev133 := &appsv1.ControllerRevision{}
	rev134 := &appsv1.ControllerRevision{}
	load(b, "compatibility_set_1.33.0.json", set133)
	load(b, "compatibility_set_1.34.0.json", set134)
	load(b, "compatibility_revision_1.33.0.json", rev133)
	load(b, "compatibility_revision_1.34.0.json", rev134)

	testcases := []struct {
		name      string
		set       *appsv1.StatefulSet
		revisions []*appsv1.ControllerRevision
	}{
		{
			name:      "1.33 set, 1.33 rev",
			set:       set133.DeepCopy(),
			revisions: []*appsv1.ControllerRevision{rev133.DeepCopy()},
		},
		{
			name:      "1.34 set, 1.34 rev",
			set:       set134.DeepCopy(),
			revisions: []*appsv1.ControllerRevision{rev134.DeepCopy()},
		},
		{
			name:      "1.34 set, 1.33+1.34 rev",
			set:       set134.DeepCopy(),
			revisions: []*appsv1.ControllerRevision{rev133.DeepCopy(), rev134.DeepCopy()},
		},
	}

	for _, tc := range testcases {
		b.Run(tc.name, func(b *testing.B) {
			latestRev := tc.revisions[len(tc.revisions)-1]
			client := fake.NewClientset(tc.set)
			// 控制器只搭建一次，循环内仅执行被测函数，避免 setup 开销污染基准结果。
			_, _, ssc := setupController(client)
			for i := 0; i < b.N; i++ {
				currentRev, updateRev, _, err := ssc.(*defaultStatefulSetControl).getStatefulSetRevisions(tc.set, tc.revisions)
				if err != nil {
					b.Fatal(err)
				}
				if !reflect.DeepEqual(currentRev, latestRev) {
					b.Fatalf("expected no change from latestRev, got %s", cmp.Diff(latestRev, currentRev))
				}
				if !reflect.DeepEqual(updateRev, latestRev) {
					b.Fatalf("expected no change from latestRev, got %s", cmp.Diff(latestRev, updateRev))
				}
			}
		})
	}
}

// load 从 testdata 目录读取 JSON 文件并严格反序列化到 object。
// 使用 UnmarshalStrict 可捕获样本中与目标类型定义不一致的字段；
// 最后按从 etcd 读取的场景执行 defaulting，保证样本与真实 API 返回一致。
func load(t testing.TB, filename string, object runtime.Object) {
	data, err := os.ReadFile("testdata/" + filename)
	if err != nil {
		t.Fatal(err)
	}
	if strictErrs, err := json.UnmarshalStrict(data, object); err != nil {
		t.Fatal(err)
	} else if len(strictErrs) > 0 {
		t.Fatal(strictErrs)
	}
	// 与从 etcd 读取时一样应用默认值，避免样本缺少默认字段导致断言失真。
	legacyscheme.Scheme.Default(object)
}
