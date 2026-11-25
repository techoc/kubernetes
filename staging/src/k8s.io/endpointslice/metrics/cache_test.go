/*
Copyright 2019 The Kubernetes Authors.

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

package metrics

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	endpointsliceutil "k8s.io/endpointslice/util"
	"k8s.io/utils/ptr"
)

// TestNumEndpointsAndSlices 验证缓存中聚合的端点数、实际与期望切片数会随服务缓存的写入、替换和删除而正确更新。
func TestNumEndpointsAndSlices(t *testing.T) {
	c := NewCache(int32(100))

	// 构造两种端口组合：仅 80 端口，以及 80+443 端口。
	pmKey80443 := endpointsliceutil.NewPortMapKey([]discovery.EndpointPort{{Port: ptr.To[int32](80)}, {Port: ptr.To[int32](443)}})
	pmKey80 := endpointsliceutil.NewPortMapKey([]discovery.EndpointPort{{Port: ptr.To[int32](80)}})

	// 高效缓存：每个端口组合的端点都集中在 1 个切片中，共 80 个端点、2 个实际切片、2 个期望切片。
	spCacheEfficient := NewServicePortCache()
	spCacheEfficient.Set(pmKey80, EfficiencyInfo{Endpoints: 45, Slices: 1})
	spCacheEfficient.Set(pmKey80443, EfficiencyInfo{Endpoints: 35, Slices: 1})

	// 低效缓存：端点被分散到多个切片中，共 30 个端点、13 个实际切片、2 个期望切片。
	spCacheInefficient := NewServicePortCache()
	spCacheInefficient.Set(pmKey80, EfficiencyInfo{Endpoints: 12, Slices: 5})
	spCacheInefficient.Set(pmKey80443, EfficiencyInfo{Endpoints: 18, Slices: 8})

	// svc1 写入低效缓存：断言期望切片 2、实际切片 13、端点 30。
	c.UpdateServicePortCache(types.NamespacedName{Namespace: "ns1", Name: "svc1"}, spCacheInefficient)
	expectNumEndpointsAndSlices(t, c, 2, 13, 30)

	// svc2 写入高效缓存：累加后期望切片 4、实际切片 15、端点 110。
	c.UpdateServicePortCache(types.NamespacedName{Namespace: "ns1", Name: "svc2"}, spCacheEfficient)
	expectNumEndpointsAndSlices(t, c, 4, 15, 110)

	// svc3 写入低效缓存：继续累加到期望切片 6、实际切片 28、端点 140。
	c.UpdateServicePortCache(types.NamespacedName{Namespace: "ns1", Name: "svc3"}, spCacheInefficient)
	expectNumEndpointsAndSlices(t, c, 6, 28, 140)

	// 用高效缓存覆盖 svc1：断言缓存以差值方式扣除了旧缓存的贡献（期望 6、实际 17、端点 190）。
	c.UpdateServicePortCache(types.NamespacedName{Namespace: "ns1", Name: "svc1"}, spCacheEfficient)
	expectNumEndpointsAndSlices(t, c, 6, 17, 190)

	// 删除 svc3：断言其统计值被完整扣减（期望 4、实际 4、端点 160）。
	c.DeleteService(types.NamespacedName{Namespace: "ns1", Name: "svc3"})
	expectNumEndpointsAndSlices(t, c, 4, 4, 160)
}

// TestPlaceHolderSlice 验证没有端点时仍然会保留一个占位切片，因此期望切片数至少为 1。
func TestPlaceHolderSlice(t *testing.T) {
	c := NewCache(int32(100))

	pmKey80443 := endpointsliceutil.NewPortMapKey([]discovery.EndpointPort{{Port: ptr.To[int32](80)}, {Port: ptr.To[int32](443)}})
	pmKey80 := endpointsliceutil.NewPortMapKey([]discovery.EndpointPort{{Port: ptr.To[int32](80)}})

	// 两个端口组合都只有切片而没有端点，用于触发占位切片逻辑。
	sp := NewServicePortCache()
	sp.Set(pmKey80, EfficiencyInfo{Endpoints: 0, Slices: 1})
	sp.Set(pmKey80443, EfficiencyInfo{Endpoints: 0, Slices: 1})

	// 断言：期望切片为 1（占位），实际切片为 2，端点数为 0。
	c.UpdateServicePortCache(types.NamespacedName{Namespace: "ns1", Name: "svc1"}, sp)
	expectNumEndpointsAndSlices(t, c, 1, 2, 0)
}

// expectNumEndpointsAndSlices 断言缓存中三个聚合值与期望值一致。
func expectNumEndpointsAndSlices(t *testing.T, c *Cache, desired int, actual int, numEndpoints int) {
	t.Helper()
	if c.numSlicesDesired != desired {
		t.Errorf("Expected numSlicesDesired to be %d, got %d", desired, c.numSlicesDesired)
	}
	if c.numSlicesActual != actual {
		t.Errorf("Expected numSlicesActual to be %d, got %d", actual, c.numSlicesActual)
	}
	if c.numEndpoints != numEndpoints {
		t.Errorf("Expected numEndpoints to be %d, got %d", numEndpoints, c.numEndpoints)
	}
}

// TestCache_ServicesByTrafficDistribution 验证 Cache.servicesByTrafficDistribution 字段在各种增删与切换场景下的变化。
func TestCache_ServicesByTrafficDistribution(t *testing.T) {
	cache := NewCache(0)

	// 准备四个分属不同命名空间的 Service，作为后续变更的输入。
	service1 := types.NamespacedName{Namespace: "ns1", Name: "service1"}
	service2 := types.NamespacedName{Namespace: "ns1", Name: "service2"}
	service3 := types.NamespacedName{Namespace: "ns2", Name: "service3"}
	service4 := types.NamespacedName{Namespace: "ns3", Name: "service4"}

	// 定义断言辅助函数：比较期望的分组映射与缓存中的实际映射，不一致则失败。
	mustHaveServicesByTrafficDistribution := func(wantServicesByTrafficDistribution map[string]map[types.NamespacedName]bool, desc string) {
		t.Helper()
		gotServicesByTrafficDistribution := cache.servicesByTrafficDistribution
		if diff := cmp.Diff(wantServicesByTrafficDistribution, gotServicesByTrafficDistribution); diff != "" {
			t.Fatalf("UpdateTrafficDistributionForService(%v) resulted in unexpected diff for cache.servicesByTrafficDistribution; (-want, +got)\n%v", desc, diff)
		}
	}

	// 依次执行变更并断言：每种 trafficDistribution 取值对应一个 Service 集合。

	// 场景 1：service1 开始使用 PreferClose，断言它被加入对应集合。
	desc := "service1 starts using trafficDistribution=PreferClose"
	cache.UpdateTrafficDistributionForService(service1, ptr.To(corev1.ServiceTrafficDistributionPreferClose))
	mustHaveServicesByTrafficDistribution(map[string]map[types.NamespacedName]bool{
		corev1.ServiceTrafficDistributionPreferClose: {service1: true},
	}, desc)

	// 场景 2：重复相同更新，断言操作幂等，集合不产生变化。
	desc = "service1 starts using trafficDistribution=PreferClose, retries of similar mutation should be idempotent"
	cache.UpdateTrafficDistributionForService(service1, ptr.To(corev1.ServiceTrafficDistributionPreferClose))
	mustHaveServicesByTrafficDistribution(map[string]map[types.NamespacedName]bool{ // 无增量
		corev1.ServiceTrafficDistributionPreferClose: {service1: true},
	}, desc)

	// 场景 3：service2 也使用 PreferClose，断言集合中出现两个 Service。
	desc = "service2 starts using trafficDistribution=PreferClose"
	cache.UpdateTrafficDistributionForService(service2, ptr.To(corev1.ServiceTrafficDistributionPreferClose))
	mustHaveServicesByTrafficDistribution(map[string]map[types.NamespacedName]bool{
		corev1.ServiceTrafficDistributionPreferClose: {service1: true, service2: true}, // 增量
	}, desc)

	// 场景 4：service3 使用自定义的 FutureValue，断言新增了一个分类集合。
	desc = "service3 starts using trafficDistribution=FutureValue"
	cache.UpdateTrafficDistributionForService(service3, ptr.To("FutureValue"))
	mustHaveServicesByTrafficDistribution(map[string]map[types.NamespacedName]bool{
		corev1.ServiceTrafficDistributionPreferClose: {service1: true, service2: true},
		"FutureValue": {service3: true}, // 增量
	}, desc)

	// 场景 5：service4 的 trafficDistribution 为 nil，断言既不新增分类也不影响已有分类。
	desc = "service4 starts using trafficDistribution=nil"
	cache.UpdateTrafficDistributionForService(service4, nil)
	mustHaveServicesByTrafficDistribution(map[string]map[types.NamespacedName]bool{ // 无增量
		corev1.ServiceTrafficDistributionPreferClose: {service1: true, service2: true},
		"FutureValue": {service3: true},
	}, desc)

	// 场景 6：service2 从 PreferClose 切换到 AnotherFutureValue，断言旧集合移除、新集合加入。
	desc = "service2 transitions trafficDistribution: PreferClose -> AnotherFutureValue"
	cache.UpdateTrafficDistributionForService(service2, ptr.To("AnotherFutureValue"))
	mustHaveServicesByTrafficDistribution(map[string]map[types.NamespacedName]bool{
		corev1.ServiceTrafficDistributionPreferClose: {service1: true}, // 增量
		"FutureValue":        {service3: true},
		"AnotherFutureValue": {service2: true}, // 增量
	}, desc)

	// 场景 7：删除 service3，断言它从 FutureValue 集合中移除且集合被清空。
	desc = "service3 gets deleted"
	cache.DeleteService(service3)
	mustHaveServicesByTrafficDistribution(map[string]map[types.NamespacedName]bool{
		corev1.ServiceTrafficDistributionPreferClose: {service1: true},
		"FutureValue":        {}, // 增量
		"AnotherFutureValue": {service2: true},
	}, desc)

	// 场景 8：service1 从 PreferClose 切换到 nil，断言旧集合被清空。
	desc = "service1 transitions trafficDistribution: PreferClose -> nil"
	cache.UpdateTrafficDistributionForService(service1, nil)
	mustHaveServicesByTrafficDistribution(map[string]map[types.NamespacedName]bool{
		corev1.ServiceTrafficDistributionPreferClose: {}, // 增量
		"FutureValue":        {},
		"AnotherFutureValue": {service2: true},
	}, desc)

	// 场景 9：service2 从 AnotherFutureValue 切换到 nil，断言最后一个非空集合也被清空。
	desc = "service2 transitions trafficDistribution: AnotherFutureValue -> nil"
	cache.UpdateTrafficDistributionForService(service2, nil)
	mustHaveServicesByTrafficDistribution(map[string]map[types.NamespacedName]bool{
		corev1.ServiceTrafficDistributionPreferClose: {},
		"FutureValue":        {},
		"AnotherFutureValue": {}, // 增量
	}, desc)

}

// benchmarkUpdateServicePortCache 是基准测试辅助函数：先预热 num 个 Service，再测量新增 Service 时的缓存更新开销。
func benchmarkUpdateServicePortCache(b *testing.B, num int) {
	c := NewCache(int32(100))
	ns := "benchmark"
	// 准备两个端口组合，并构造含两种端口统计的共享 ServicePortCache 作为输入。
	httpKey := endpointsliceutil.NewPortMapKey([]discovery.EndpointPort{{Port: ptr.To[int32](80)}})
	httpsKey := endpointsliceutil.NewPortMapKey([]discovery.EndpointPort{{Port: ptr.To[int32](443)}})
	spCache := &ServicePortCache{items: map[endpointsliceutil.PortMapKey]EfficiencyInfo{
		httpKey: {
			Endpoints: 182,
			Slices:    2,
		},
		httpsKey: {
			Endpoints: 356,
			Slices:    4,
		},
	}}

	// 预热阶段：写入 num 个 Service，使缓存规模达到待基准测试的水平。
	for i := 0; i < num; i++ {
		nName := types.NamespacedName{Namespace: ns, Name: fmt.Sprintf("service-%d", i)}
		c.UpdateServicePortCache(nName, spCache)
	}

	// 重置计时器后，仅测量循环内新增 Service 的缓存更新耗时。
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nName := types.NamespacedName{Namespace: ns, Name: fmt.Sprintf("bench-%d", i)}
		c.UpdateServicePortCache(nName, spCache)
	}
}

// BenchmarkUpdateServicePortCache100 在缓存已有 100 个 Service 时测量 UpdateServicePortCache 的性能。
func BenchmarkUpdateServicePortCache100(b *testing.B) {
	benchmarkUpdateServicePortCache(b, 100)
}

// BenchmarkUpdateServicePortCache1000 在缓存已有 1000 个 Service 时测量 UpdateServicePortCache 的性能。
func BenchmarkUpdateServicePortCache1000(b *testing.B) {
	benchmarkUpdateServicePortCache(b, 1000)
}

// BenchmarkUpdateServicePortCache10000 在缓存已有 10000 个 Service 时测量 UpdateServicePortCache 的性能。
func BenchmarkUpdateServicePortCache10000(b *testing.B) {
	benchmarkUpdateServicePortCache(b, 10000)
}

// BenchmarkUpdateServicePortCache100000 在缓存已有 100000 个 Service 时测量 UpdateServicePortCache 的性能。
func BenchmarkUpdateServicePortCache100000(b *testing.B) {
	benchmarkUpdateServicePortCache(b, 100000)
}
