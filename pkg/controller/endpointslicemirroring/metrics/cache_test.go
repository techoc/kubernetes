/*
Copyright 2020 The Kubernetes Authors.

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
	"testing"

	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	endpointsliceutil "k8s.io/endpointslice/util"
)

// TestNumEndpointsAndSlices 验证缓存能在多次更新与删除后正确累计
// 期望/实际的 EndpointSlice 数量以及 Endpoint 总数。
func TestNumEndpointsAndSlices(t *testing.T) {
	c := NewCache(int32(100))

	// 端口组合是缓存的键；80 与 443 两种组合用于模拟同一服务多端口的场景。
	p80 := int32(80)
	p443 := int32(443)

	pmKey80443 := endpointsliceutil.NewPortMapKey([]discovery.EndpointPort{{Port: &p80}, {Port: &p443}})
	pmKey80 := endpointsliceutil.NewPortMapKey([]discovery.EndpointPort{{Port: &p80}})

	// 高效缓存：每个端口映射仅用 1 个 slice，用于验证差值计算中的期望值。
	spCacheEfficient := NewEndpointPortCache()
	spCacheEfficient.Set(pmKey80, EfficiencyInfo{Endpoints: 45, Slices: 1})
	spCacheEfficient.Set(pmKey80443, EfficiencyInfo{Endpoints: 35, Slices: 1})

	// 低效缓存：同样的端口映射被拆成多个 slice，用于验证实际值高于期望值的场景。
	spCacheInefficient := NewEndpointPortCache()
	spCacheInefficient.Set(pmKey80, EfficiencyInfo{Endpoints: 12, Slices: 5})
	spCacheInefficient.Set(pmKey80443, EfficiencyInfo{Endpoints: 18, Slices: 8})

	c.UpdateEndpointPortCache(types.NamespacedName{Namespace: "ns1", Name: "svc1"}, spCacheInefficient)
	expectNumEndpointsAndSlices(t, c, 2, 13, 30)

	c.UpdateEndpointPortCache(types.NamespacedName{Namespace: "ns1", Name: "svc2"}, spCacheEfficient)
	expectNumEndpointsAndSlices(t, c, 4, 15, 110)

	c.UpdateEndpointPortCache(types.NamespacedName{Namespace: "ns1", Name: "svc3"}, spCacheInefficient)
	expectNumEndpointsAndSlices(t, c, 6, 28, 140)

	// 同一服务再次更新为高效缓存：期望值不变，实际 slice 数应下降。
	c.UpdateEndpointPortCache(types.NamespacedName{Namespace: "ns1", Name: "svc1"}, spCacheEfficient)
	expectNumEndpointsAndSlices(t, c, 6, 17, 190)

	// 删除服务时其贡献必须从所有累计值中扣除，否则指标会持续偏高。
	c.DeleteEndpoints(types.NamespacedName{Namespace: "ns1", Name: "svc3"})
	expectNumEndpointsAndSlices(t, c, 4, 4, 160)
}

// expectNumEndpointsAndSlices 校验缓存聚合后的期望/实际 slice 数与端点总数。
func expectNumEndpointsAndSlices(t *testing.T, c *Cache, desired int, actual int, numEndpoints int) {
	t.Helper()
	mUpdate := c.desiredAndActualSlices()
	if mUpdate.desired != desired {
		t.Errorf("Expected numEndpointSlices to be %d, got %d", desired, mUpdate.desired)
	}
	if mUpdate.actual != actual {
		t.Errorf("Expected desiredEndpointSlices to be %d, got %d", actual, mUpdate.actual)
	}
	if c.numEndpoints != numEndpoints {
		t.Errorf("Expected numEndpoints to be %d, got %d", numEndpoints, c.numEndpoints)
	}
}
