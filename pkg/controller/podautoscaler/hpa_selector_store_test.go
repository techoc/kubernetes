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

package podautoscaler

import (
	"fmt"
	"testing"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/kubernetes/pkg/controller/util/selectors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHPASelectorStorePutIfAbsent 验证入队阶段使用的 PutIfAbsent 不会覆盖协调阶段更新的值。
// 若已被占用就保持原值，避免入队时写入的旧选择器覆盖协调阶段的最新结果。
func TestHPASelectorStorePutIfAbsent(t *testing.T) {
	store := newHPASelectorStore()
	key := selectors.Key{Name: "hpa-1", Namespace: "ns-1"}
	sel1, _ := labels.Parse("app=hakuna")
	sel2, _ := labels.Parse("app=matata")

	// 首次插入应当成功。
	assert.True(t, store.PutIfAbsent("ns-1", key, sel1))

	// 键已存在时应当无操作，不能覆盖原选择器。
	assert.False(t, store.PutIfAbsent("ns-1", key, sel2))

	// 通过匹配结果确认原选择器仍然生效。
	pods := []*v1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "ns-1", Labels: map[string]string{"app": "hakuna"}}},
	}
	assert.Len(t, store.HPAsMatchingPods("ns-1", pods, 0), 1)

	pods2 := []*v1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "ns-1", Labels: map[string]string{"app": "matata"}}},
	}
	assert.Empty(t, store.HPAsMatchingPods("ns-1", pods2, 0))
}

// TestHPASelectorStorePutIfPresent 验证协调阶段使用的 PutIfPresent 不会误加入已从存储中删除的 HPA。
// 仅在键已存在时更新，防止删除后残留的更新请求把 HPA 重新带回来。
func TestHPASelectorStorePutIfPresent(t *testing.T) {
	store := newHPASelectorStore()
	key := selectors.Key{Name: "hpa-1", Namespace: "ns-1"}
	sel1, _ := labels.Parse("app=hakuna")
	sel2, _ := labels.Parse("app=matata")

	// 对不存在的键更新应当无操作。
	assert.False(t, store.PutIfPresent("ns-1", key, sel2))

	// 先插入再更新，用于验证更新确实生效。
	store.PutIfAbsent("ns-1", key, sel1)
	assert.True(t, store.PutIfPresent("ns-1", key, sel2))

	// 选择器已更新，旧标签的 Pod 不再匹配。
	pods := []*v1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "ns-1", Labels: map[string]string{"app": "hakuna"}}},
	}
	assert.Empty(t, store.HPAsMatchingPods("ns-1", pods, 0))

	// 新标签的 Pod 应当匹配到更新后的选择器。
	pods2 := []*v1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "ns-1", Labels: map[string]string{"app": "matata"}}},
	}
	assert.Len(t, store.HPAsMatchingPods("ns-1", pods2, 0), 1)
}

// TestHPASelectorStoreDeleteCleansUpNamespace 验证删除 HPA 时不会遗留空的命名空间条目。
// 只有命名空间内最后一个 HPA 被删除时，对应的命名空间条目才应被清理。
func TestHPASelectorStoreDeleteCleansUpNamespace(t *testing.T) {
	store := newHPASelectorStore()
	sel, _ := labels.Parse("app=hakuna")

	key1 := selectors.Key{Name: "hpa-1", Namespace: "ns-1"}
	key2 := selectors.Key{Name: "hpa-2", Namespace: "ns-1"}
	store.PutIfAbsent("ns-1", key1, sel)
	store.PutIfAbsent("ns-1", key2, sel)

	// 删除第一个 HPA 后仍有残留，命名空间条目必须保留。
	store.Delete("ns-1", key1)
	store.mu.RLock()
	require.Contains(t, store.namespaces, "ns-1")
	store.mu.RUnlock()

	// 删除最后一个 HPA，命名空间条目应被移除。
	store.Delete("ns-1", key2)
	store.mu.RLock()
	require.NotContains(t, store.namespaces, "ns-1")
	store.mu.RUnlock()
}

// TestHPASelectorStoreHPAsMatchingPodsLimit 验证匹配时不会遍历整个 selectorStore。
// 达到 limit 后立即提前返回，既限制结果数量也避免不必要的扫描。
func TestHPASelectorStoreHPAsMatchingPodsLimit(t *testing.T) {
	store := newHPASelectorStore()

	// 两个 HPA 都选择 "app=hakuna"，用于观察数量限制效果。
	sel, _ := labels.Parse("app=hakuna")
	store.PutIfAbsent("ns-1", selectors.Key{Name: "hpa-1", Namespace: "ns-1"}, sel)
	store.PutIfAbsent("ns-1", selectors.Key{Name: "hpa-2", Namespace: "ns-1"}, sel)

	pods := []*v1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "ns-1", Labels: map[string]string{"app": "hakuna"}}},
	}

	// limit 为 0 表示不限制，返回全部匹配结果。
	assert.Len(t, store.HPAsMatchingPods("ns-1", pods, 0), 2)

	// limit 为 2 时恰好返回 2 个，验证提前退出不会少算。
	assert.Len(t, store.HPAsMatchingPods("ns-1", pods, 2), 2)

	// limit 为 1 时只返回 1 个，验证数量限制生效。
	assert.Len(t, store.HPAsMatchingPods("ns-1", pods, 1), 1)
}

// TestHPASelectorStoreHPAsMatchingPodsNamespaceIsolation 验证匹配时不会遍历所有命名空间下的 HPA。
// 查询只会命中目标命名空间，其他命名空间的 HPA 不可见。
func TestHPASelectorStoreHPAsMatchingPodsNamespaceIsolation(t *testing.T) {
	store := newHPASelectorStore()
	sel, _ := labels.Parse("app=hakuna")

	store.PutIfAbsent("ns-1", selectors.Key{Name: "hpa-1", Namespace: "ns-1"}, sel)
	store.PutIfAbsent("ns-2", selectors.Key{Name: "hpa-2", Namespace: "ns-2"}, sel)

	pods := []*v1.Pod{
		{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "ns-1", Labels: map[string]string{"app": "hakuna"}}},
	}

	// 只返回被查询命名空间中的 HPA。
	result := store.HPAsMatchingPods("ns-1", pods, 0)
	assert.Len(t, result, 1)
	assert.Equal(t, "hpa-1", result[0].Name)

	// 查询不存在的命名空间时，ns-1 的 Pod 不会匹配到任何 HPA。
	assert.Empty(t, store.HPAsMatchingPods("ns-3", pods, 0))
}

// BenchmarkHPAsMatchingPodsParallel 基准测试新版 hpaSelectorStore 在并发匹配场景下的性能。
func BenchmarkHPAsMatchingPodsParallel(b *testing.B) {
	benchmarkMatchingPodsParallel(b, newHPASelectorStore())
}

// TODO: Remove once HPAOptimizedSelectorStore graduates to GA and the legacy
// BiMultimap tracker is removed.
// BenchmarkHPAsMatchingPodsParallelBiMultimap 基准测试旧版 BiMultimap 跟踪器，用于与新版做性能对比。
func BenchmarkHPAsMatchingPodsParallelBiMultimap(b *testing.B) {
	benchmarkMatchingPodsParallel(b, newBiMultimapSelectorTracker())
}

// benchmarkMatchingPodsParallel 以多组 HPA/Pod 规模并发运行匹配基准，供不同实现横向比较。
// 通过 RunParallel 和 ReportAllocs 同时观察吞吐量与分配开销。
func benchmarkMatchingPodsParallel(b *testing.B, tracker hpaSelectorTracker) {
	scenarios := []struct {
		name     string
		hpaCount int
		podCount int
	}{
		{name: "100hpas_10pods", hpaCount: 100, podCount: 10},
		{name: "1000hpas_10pods", hpaCount: 1000, podCount: 10},
		{name: "1000hpas_100pods", hpaCount: 1000, podCount: 100},
		{name: "10000hpas_100pods", hpaCount: 10000, podCount: 100},
	}

	const namespace = "hakuna"
	for _, s := range scenarios {
		b.Run(s.name, func(b *testing.B) {
			for i := 0; i < s.hpaCount; i++ {
				key := selectors.Key{Name: fmt.Sprintf("hpa-%d", i), Namespace: namespace}
				sel, _ := labels.Parse(fmt.Sprintf("app=matata-%d", i))
				tracker.PutIfAbsent(namespace, key, sel)
			}

			// 所有 Pod 的标签都匹配第一个 HPA 的选择器，使匹配路径稳定可比较。
			pods := make([]*v1.Pod, 0, s.podCount)
			for i := 0; i < s.podCount; i++ {
				pods = append(pods, &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("pod-%d", i),
						Namespace: namespace,
						Labels:    map[string]string{"app": "matata-0"},
					},
				})
			}

			b.ResetTimer()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					tracker.HPAsMatchingPods(namespace, pods, 2)
				}
			})
		})
	}
}
