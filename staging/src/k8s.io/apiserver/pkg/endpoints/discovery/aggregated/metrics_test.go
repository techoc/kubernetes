/*
Copyright 2022 The Kubernetes Authors.

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

package aggregated_test

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	discoveryendpoint "k8s.io/apiserver/pkg/endpoints/discovery/aggregated"
	genericfeatures "k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
)

// formatExpectedMetrics 生成期望的聚合计数文本；计数为 0 时不输出任何行，
// 因为 CounterVec 在未被观测前不会出现在 gather 结果中。
func formatExpectedMetrics(aggregationCount int) io.Reader {
	expected := ``
	if aggregationCount > 0 {
		expected = expected + `# HELP aggregator_discovery_aggregation_count_total [ALPHA] Counter of number of times discovery was aggregated
# TYPE aggregator_discovery_aggregation_count_total counter
aggregator_discovery_aggregation_count_total %d
`
	}
	args := []any{}
	if aggregationCount > 0 {
		args = append(args, aggregationCount)
	}
	return strings.NewReader(fmt.Sprintf(expected, args...))
}

// TestBasicMetrics 验证聚合结果被缓存：连续两次请求只触发一次聚合。
func TestBasicMetrics(t *testing.T) {
	legacyregistry.Reset()
	manager := discoveryendpoint.NewResourceManager("apis")

	apis := fuzzAPIGroups(1, 3, 10)
	manager.SetGroups(apis.Items)

	interests := []string{"aggregator_discovery_aggregation_count_total"}

	_, _, _ = fetchPath(manager, "application/json", discoveryPath, "")
	// 单次 fetch 应触发聚合并递增 regeneration 计数器。
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, formatExpectedMetrics(1), interests...); err != nil {
		t.Fatal(err)
	}
	_, _, _ = fetchPath(manager, "application/json", discoveryPath, "")
	// 后续 fetch 不应重新聚合 discovery。
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, formatExpectedMetrics(1), interests...); err != nil {
		t.Fatal(err)
	}
}

// TestMetricsModified 验证发现内容变化后会重新聚合，计数递增。
func TestMetricsModified(t *testing.T) {
	legacyregistry.Reset()
	manager := discoveryendpoint.NewResourceManager("apis")

	apis := fuzzAPIGroups(1, 3, 10)
	manager.SetGroups(apis.Items)

	interests := []string{"aggregator_discovery_aggregation_count_total"}

	_, _, _ = fetchPath(manager, "application/json", discoveryPath, "")
	// 单次 fetch 应触发聚合并递增 regeneration 计数器。
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, formatExpectedMetrics(1), interests...); err != nil {
		t.Fatal(err)
	}

	// 更新 discovery 文档。
	manager.SetGroups(fuzzAPIGroups(1, 3, 10).Items)
	_, _, _ = fetchPath(manager, "application/json", discoveryPath, "")
	// 若 discovery 内容发生变化，则应重新聚合。
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, formatExpectedMetrics(2), interests...); err != nil {
		t.Fatal(err)
	}
}

// TestPeerAggregatedDiscoveryMetrics 验证 peer 聚合发现的三类指标：
// 缓存命中、缓存未命中（需重新跨节点聚合）与 no-peer 请求计数。
func TestPeerAggregatedDiscoveryMetrics(t *testing.T) {
	legacyregistry.Reset()
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, genericfeatures.UnknownVersionInteroperabilityProxy, true)
	manager := discoveryendpoint.NewResourceManager("apis")
	localGroup := newAPIGroup("local.example.com", "v1", "local-resource")
	manager.AddGroupVersion(localGroup.Name, localGroup.Versions[0])
	peerProvider := &mockPeerDiscoveryProvider{
		resources: map[string][]apidiscoveryv2.APIGroupDiscovery{
			"peer-server-1": {
				newAPIGroup("peer.example.com", "v2", "peer-resource"),
			},
		},
	}
	peerAggregatedDiscoveryManager := discoveryendpoint.NewPeerAggregatedDiscoveryHandler("test-server", manager, peerProvider, "apis")
	wrapped := discoveryendpoint.WrapAggregatedDiscoveryToHandler(manager, manager, peerAggregatedDiscoveryManager)

	_ = legacyregistry.Register(discoveryendpoint.PeerAggregatedCacheHitsCounter)
	_ = legacyregistry.Register(discoveryendpoint.PeerAggregatedCacheMissesCounter)
	_ = legacyregistry.Register(discoveryendpoint.NoPeerDiscoveryRequestCounter)

	// 发起 3 次 peer 聚合请求。
	// 前两次命中缓存，第三次带 as=APIGroupDiscoveryList 的请求仍走同一缓存。
	fetchPath(wrapped, "application/json", "/apis", "")
	fetchPath(wrapped, "application/json;profile=foo", "/apis", "")
	fetchPath(wrapped, "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList", "/apis", "")

	// 发起 2 次无 peer 的 discovery 请求。
	// 每次 InvalidateCache 后再次请求，确保计数不被缓存吞掉。
	fetchPath(wrapped, "application/json;profile=nopeer", "/apis", "")
	peerAggregatedDiscoveryManager.InvalidateCache()
	fetchPath(wrapped, "application/json;profile=nopeer", "/apis", "")
	peerAggregatedDiscoveryManager.InvalidateCache()

	cacheHitsTotalMetric := `
# HELP aggregator_discovery_peer_aggregated_cache_hits_total [ALPHA] Counter of number of times discovery was served from peer-aggregated cache
# TYPE aggregator_discovery_peer_aggregated_cache_hits_total counter
aggregator_discovery_peer_aggregated_cache_hits_total 2
`

	cacheMissesTotalMetric := `
# HELP aggregator_discovery_peer_aggregated_cache_misses_total [ALPHA] Counter of number of times discovery was aggregated across all API servers
# TYPE aggregator_discovery_peer_aggregated_cache_misses_total counter
aggregator_discovery_peer_aggregated_cache_misses_total 1
`

	noPeerDiscoveryTotalMetric := `
# HELP aggregator_discovery_nopeer_requests_total [ALPHA] Counter of number of times no-peer (non peer-aggregated) discovery was requested
# TYPE aggregator_discovery_nopeer_requests_total counter
aggregator_discovery_nopeer_requests_total 2
`

	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(cacheHitsTotalMetric), "aggregator_discovery_peer_aggregated_cache_hits_total"); err != nil {
		t.Errorf("unexpected metrics output: %v", err)
	}

	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(cacheMissesTotalMetric), "aggregator_discovery_peer_aggregated_cache_misses_total"); err != nil {
		t.Errorf("unexpected metrics output: %v", err)
	}

	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(noPeerDiscoveryTotalMetric), "aggregator_discovery_nopeer_requests_total"); err != nil {
		t.Errorf("unexpected metrics output: %v", err)
	}
}
