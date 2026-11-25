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

package aggregated_test

import (
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime/schema"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	discoveryendpoint "k8s.io/apiserver/pkg/endpoints/discovery/aggregated"
	genericfeatures "k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
)

// TestPeerAggregatedDiscovery 验证 peer 聚合发现的启用条件与 profile 选择：
// 特性门控关闭、请求 nopeer、或 provider 为 nil 时只返回本地组；否则合并 peer 组。
func TestPeerAggregatedDiscovery(t *testing.T) {
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

	testCases := []struct {
		name                          string
		enablePeerAggregatedDiscovery bool
		acceptHeader                  string
		peerProvider                  discoveryendpoint.PeerDiscoveryProvider
		wantGroupNames                []string
	}{
		{
			name:                          "Peer aggregated discovery disabled (should get local discovery)",
			enablePeerAggregatedDiscovery: false,
			acceptHeader:                  "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList;profile=nopeer",
			wantGroupNames:                []string{"local.example.com"},
		},
		{
			name:                          "Request without profile (should default to peer-aggregated)",
			enablePeerAggregatedDiscovery: true,
			acceptHeader:                  "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList",
			peerProvider:                  peerProvider,
			wantGroupNames:                []string{"local.example.com", "peer.example.com"},
		},
		{
			name:                          "Request with unknown profile (should default to peer-aggregated)",
			enablePeerAggregatedDiscovery: true,
			acceptHeader:                  "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList;profile=foo",
			peerProvider:                  peerProvider,
			wantGroupNames:                []string{"local.example.com", "peer.example.com"},
		},
		{
			name:                          "Request with profile=nopeer (should get no-peer discovery)",
			enablePeerAggregatedDiscovery: true,
			acceptHeader:                  "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList;profile=nopeer",
			peerProvider:                  peerProvider,
			wantGroupNames:                []string{"local.example.com"},
		},
		{
			name:                          "Peer provider nil (should get local discovery)",
			enablePeerAggregatedDiscovery: true,
			acceptHeader:                  "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList",
			peerProvider:                  nil,
			wantGroupNames:                []string{"local.example.com"},
		},
		{
			name:                          "Multiple peer resources (should return all)",
			enablePeerAggregatedDiscovery: true,
			acceptHeader:                  "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList",
			peerProvider: &mockPeerDiscoveryProvider{
				resources: map[string][]apidiscoveryv2.APIGroupDiscovery{
					"peer-server-1": {
						newAPIGroup("peer.example.com", "v2", "peer-resource"),
					},
					"peer-server-2": {
						newAPIGroup("peer2.example.com", "v1", "peer2-resource"),
					},
				},
			},
			wantGroupNames: []string{
				"local.example.com", "peer.example.com", "peer2.example.com",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, genericfeatures.UnknownVersionInteroperabilityProxy, tc.enablePeerAggregatedDiscovery)

			peerAggregatedDiscoveryManager := discoveryendpoint.NewPeerAggregatedDiscoveryHandler("test-server", manager, tc.peerProvider, "apis")
			wrapped := discoveryendpoint.WrapAggregatedDiscoveryToHandler(manager, manager, peerAggregatedDiscoveryManager)

			resp, _, decoded := fetchPath(wrapped, tc.acceptHeader, "/apis", "")
			require.Equal(t, http.StatusOK, resp.StatusCode)
			require.NotNil(t, decoded)
			gotGroupNames := make([]string, 0, len(decoded.Items))
			for _, group := range decoded.Items {
				gotGroupNames = append(gotGroupNames, group.Name)
			}
			assert.ElementsMatch(t, tc.wantGroupNames, gotGroupNames, "group names mismatch")
		})
	}
}

// TestPeerAggregatedDiscovery_ETagHandling 验证缓存失效后 ETag 随 peer 资源
// 变化而更新，未变化时保持稳定。
func TestPeerAggregatedDiscovery_ETagHandling(t *testing.T) {
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

	// First request, get ETag
	rr1 := &responseRecorder{header: make(http.Header)}
	req1, _ := http.NewRequest(http.MethodGet, "/apis", nil)
	req1.Header.Set("Accept", "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList")
	wrapped.ServeHTTP(rr1, req1)
	etag1 := rr1.header.Get("ETag")
	assert.NotEmpty(t, etag1, "ETag should be set on first response")

	// 第二次请求，ETag 应保持不变（缓存命中）
	rr2 := &responseRecorder{header: make(http.Header)}
	req2, _ := http.NewRequest(http.MethodGet, "/apis", nil)
	req2.Header.Set("Accept", "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList")
	wrapped.ServeHTTP(rr2, req2)
	etag2 := rr2.header.Get("ETag")
	assert.Equal(t, etag1, etag2, "ETag should be the same for cached response")

	// 使缓存失效并添加新的 peer 资源
	peerAggregatedDiscoveryManager.InvalidateCache()
	gvr := schema.GroupVersionResource{Group: "peer.example.com", Version: "v2", Resource: "peer-resource2"}
	peerProvider.addResource("peer-server-3", gvr, newAPIGroup(gvr.Group, gvr.Version, "peer-resource2"))

	// 第三次请求，ETag 应发生变化
	rr3 := &responseRecorder{header: make(http.Header)}
	req3, _ := http.NewRequest(http.MethodGet, "/apis", nil)
	req3.Header.Set("Accept", "application/json;g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList")
	wrapped.ServeHTTP(rr3, req3)
	etag3 := rr3.header.Get("ETag")
	assert.NotEmpty(t, etag3, "ETag should be set after resource change")
	assert.NotEqual(t, etag1, etag3, "ETag should change when resources change")
}

// TestFetchFromCache 验证从缓存组装文档时本地与 peer 资源的组合结果。
func TestFetchFromCache(t *testing.T) {
	testCases := []struct {
		name         string
		localGroups  []apidiscoveryv2.APIGroupDiscovery
		peerProvider discoveryendpoint.PeerDiscoveryProvider
		wantGroups   []string
	}{
		{
			name: "local only - no peer provider",
			localGroups: []apidiscoveryv2.APIGroupDiscovery{
				newAPIGroup("local.example.com", "v1", "local-resource"),
			},
			peerProvider: nil,
			wantGroups:   []string{"local.example.com"},
		},
		{
			name: "local and peer resources",
			localGroups: []apidiscoveryv2.APIGroupDiscovery{
				newAPIGroup("local.example.com", "v1", "local-resource"),
			},
			peerProvider: &mockPeerDiscoveryProvider{
				resources: map[string][]apidiscoveryv2.APIGroupDiscovery{
					"peer-server-1": {
						newAPIGroup("peer1.example.com", "v1", "peer1-resource"),
					},
					"peer-server-2": {
						newAPIGroup("peer2.example.com", "v1", "peer2-resource"),
					},
				},
			},
			wantGroups: []string{"local.example.com", "peer1.example.com", "peer2.example.com"},
		},
		{
			name:        "no local resources, only peer",
			localGroups: []apidiscoveryv2.APIGroupDiscovery{},
			peerProvider: &mockPeerDiscoveryProvider{
				resources: map[string][]apidiscoveryv2.APIGroupDiscovery{
					"peer-server-1": {
						newAPIGroup("peer.example.com", "v1", "peer-resource"),
					},
				},
			},
			wantGroups: []string{"peer.example.com"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			manager := discoveryendpoint.NewResourceManager("apis")
			for _, group := range tc.localGroups {
				for _, version := range group.Versions {
					manager.AddGroupVersion(group.Name, version)
				}
			}

			response, etag := discoveryendpoint.TestFetchFromCache("test-server", manager, tc.peerProvider)
			gotGroupNames := make([]string, 0, len(response.Items))
			for _, group := range response.Items {
				gotGroupNames = append(gotGroupNames, group.Name)
			}

			assert.ElementsMatch(t, tc.wantGroups, gotGroupNames, "group names mismatch")
			assert.NotEmpty(t, etag, "ETag should be set")
		})
	}
}

// TestMergeResources 覆盖 mergeResources 的各类合并场景：同 g/v 增加资源、
// 同组增加版本、全新组、顺序差异、verbs/subresources 合并，以及无变化时的
// 短路返回（返回原切片而非新切片）。
func TestMergeResources(t *testing.T) {
	r1 := apidiscoveryv2.APIResourceDiscovery{Resource: "r1"}
	r2 := apidiscoveryv2.APIResourceDiscovery{Resource: "r2"}

	v1r1 := apidiscoveryv2.APIVersionDiscovery{Version: "v1", Resources: []apidiscoveryv2.APIResourceDiscovery{r1}}
	v1r2 := apidiscoveryv2.APIVersionDiscovery{Version: "v1", Resources: []apidiscoveryv2.APIResourceDiscovery{r2}}
	v2r1 := apidiscoveryv2.APIVersionDiscovery{Version: "v2", Resources: []apidiscoveryv2.APIResourceDiscovery{r1}}

	serverA := "serverA"
	serverB := "serverB"

	// Local versions/resources
	g1v1r1 := apidiscoveryv2.APIGroupDiscovery{
		ObjectMeta: metav1.ObjectMeta{Name: "g1", UID: "g1-uid"},
		Versions:   []apidiscoveryv2.APIVersionDiscovery{v1r1},
	}
	g2v1r1 := apidiscoveryv2.APIGroupDiscovery{
		ObjectMeta: metav1.ObjectMeta{Name: "g2", UID: "g2-uid"},
		Versions:   []apidiscoveryv2.APIVersionDiscovery{v1r1},
	}

	// Peer versions/resources
	g1v1r2 := apidiscoveryv2.APIGroupDiscovery{
		ObjectMeta: metav1.ObjectMeta{Name: "g1", UID: "g1-peer-uid"}, // Different UID
		Versions:   []apidiscoveryv2.APIVersionDiscovery{v1r2},
	}
	g1v2r1 := apidiscoveryv2.APIGroupDiscovery{
		ObjectMeta: metav1.ObjectMeta{Name: "g1", UID: "g1-peer-uid"},
		Versions:   []apidiscoveryv2.APIVersionDiscovery{v2r1},
	}

	// g1v1r1 + g1v1r2 = g1v1(r1,r2)
	g1v1r1r2Merged := apidiscoveryv2.APIGroupDiscovery{
		// Meta 复制自排序后 serverIDs 列表中的第一个 server
		ObjectMeta: metav1.ObjectMeta{Name: "g1", UID: "g1-uid"}, // Meta from g1v1r1
		Versions: []apidiscoveryv2.APIVersionDiscovery{
			{Version: "v1", Resources: []apidiscoveryv2.APIResourceDiscovery{r1, r2}},
		},
	}

	// g1v1r1 + g1v2r1 = g1(v1r1, v2r1)
	g1v1r1v2r1Merged := apidiscoveryv2.APIGroupDiscovery{
		ObjectMeta: metav1.ObjectMeta{Name: "g1", UID: "g1-uid"}, // Meta from g1v1r1
		Versions: []apidiscoveryv2.APIVersionDiscovery{
			{Version: "v1", Resources: []apidiscoveryv2.APIResourceDiscovery{r1}},
			{Version: "v2", Resources: []apidiscoveryv2.APIResourceDiscovery{r1}},
		},
	}

	// verbs test data
	rVerbs1 := apidiscoveryv2.APIResourceDiscovery{Resource: "pods", Verbs: []string{"get", "list"}}
	rVerbs2 := apidiscoveryv2.APIResourceDiscovery{Resource: "pods", Verbs: []string{"get", "watch"}}
	rVerbsMerged := apidiscoveryv2.APIResourceDiscovery{Resource: "pods", Verbs: []string{"get", "list", "watch"}} // Verbs are sorted by helper

	vVerbs1 := apidiscoveryv2.APIVersionDiscovery{Version: "v1", Resources: []apidiscoveryv2.APIResourceDiscovery{rVerbs1}}
	vVerbs2 := apidiscoveryv2.APIVersionDiscovery{Version: "v1", Resources: []apidiscoveryv2.APIResourceDiscovery{rVerbs2}}
	vVerbsMerged := apidiscoveryv2.APIVersionDiscovery{Version: "v1", Resources: []apidiscoveryv2.APIResourceDiscovery{rVerbsMerged}}

	gVerbs1 := apidiscoveryv2.APIGroupDiscovery{
		ObjectMeta: metav1.ObjectMeta{Name: "g1", UID: "g1-uid-a"},
		Versions:   []apidiscoveryv2.APIVersionDiscovery{vVerbs1},
	}
	gVerbs2 := apidiscoveryv2.APIGroupDiscovery{
		ObjectMeta: metav1.ObjectMeta{Name: "g1", UID: "g1-uid-b"},
		Versions:   []apidiscoveryv2.APIVersionDiscovery{vVerbs2},
	}
	gVerbsMerged := apidiscoveryv2.APIGroupDiscovery{
		ObjectMeta: metav1.ObjectMeta{Name: "g1", UID: "g1-uid-a"}, // Metadata from serverA (alphabetical first)
		Versions:   []apidiscoveryv2.APIVersionDiscovery{vVerbsMerged},
	}

	// subresource test data
	rSub1 := apidiscoveryv2.APIResourceDiscovery{Resource: "pods", Subresources: []apidiscoveryv2.APISubresourceDiscovery{{Subresource: "status"}}}
	rSub2 := apidiscoveryv2.APIResourceDiscovery{Resource: "pods", Subresources: []apidiscoveryv2.APISubresourceDiscovery{{Subresource: "log"}}}
	rSubMerged := apidiscoveryv2.APIResourceDiscovery{Resource: "pods", Subresources: []apidiscoveryv2.APISubresourceDiscovery{{Subresource: "log"}, {Subresource: "status"}}} // Sorted by helper

	vSub1 := apidiscoveryv2.APIVersionDiscovery{Version: "v1", Resources: []apidiscoveryv2.APIResourceDiscovery{rSub1}}
	vSub2 := apidiscoveryv2.APIVersionDiscovery{Version: "v1", Resources: []apidiscoveryv2.APIResourceDiscovery{rSub2}}
	vSubMerged := apidiscoveryv2.APIVersionDiscovery{Version: "v1", Resources: []apidiscoveryv2.APIResourceDiscovery{rSubMerged}}

	gSub1 := apidiscoveryv2.APIGroupDiscovery{
		ObjectMeta: metav1.ObjectMeta{Name: "g1", UID: "g1-uid-a"},
		Versions:   []apidiscoveryv2.APIVersionDiscovery{vSub1},
	}
	gSub2 := apidiscoveryv2.APIGroupDiscovery{
		ObjectMeta: metav1.ObjectMeta{Name: "g1", UID: "g1-uid-b"},
		Versions:   []apidiscoveryv2.APIVersionDiscovery{vSub2},
	}
	gSubMerged := apidiscoveryv2.APIGroupDiscovery{
		ObjectMeta: metav1.ObjectMeta{Name: "g1", UID: "g1-uid-a"}, // Metadata from serverA
		Versions:   []apidiscoveryv2.APIVersionDiscovery{vSubMerged},
	}

	tests := []struct {
		name                 string
		localServerID        string
		localGroups          []apidiscoveryv2.APIGroupDiscovery
		peerGroupDiscovery   map[string][]apidiscoveryv2.APIGroupDiscovery
		wantResult           []apidiscoveryv2.APIGroupDiscovery
		validateShortCircuit bool
	}{
		{
			name:                 "no peers, just local resources",
			localServerID:        serverA,
			localGroups:          []apidiscoveryv2.APIGroupDiscovery{g1v1r1, g2v1r1},
			peerGroupDiscovery:   map[string][]apidiscoveryv2.APIGroupDiscovery{},
			wantResult:           []apidiscoveryv2.APIGroupDiscovery{g1v1r1, g2v1r1},
			validateShortCircuit: true,
		},
		{
			name:                 "peer adds no new g/v/r",
			localServerID:        serverA,
			localGroups:          []apidiscoveryv2.APIGroupDiscovery{g1v1r1, g2v1r1},
			peerGroupDiscovery:   map[string][]apidiscoveryv2.APIGroupDiscovery{"serverB": {g1v1r1, g2v1r1}},
			wantResult:           []apidiscoveryv2.APIGroupDiscovery{g1v1r1, g2v1r1},
			validateShortCircuit: true,
		},
		{
			name:               "peer adds a new resource to existing g/v",
			localServerID:      serverA,
			localGroups:        []apidiscoveryv2.APIGroupDiscovery{g1v1r1},
			peerGroupDiscovery: map[string][]apidiscoveryv2.APIGroupDiscovery{"serverB": {g1v1r2}},
			wantResult:         []apidiscoveryv2.APIGroupDiscovery{g1v1r1r2Merged},
		},
		{
			name:               "peer adds a new version to existing group",
			localServerID:      serverA,
			localGroups:        []apidiscoveryv2.APIGroupDiscovery{g1v1r1},
			peerGroupDiscovery: map[string][]apidiscoveryv2.APIGroupDiscovery{"serverB": {g1v2r1}},
			wantResult:         []apidiscoveryv2.APIGroupDiscovery{g1v1r1v2r1Merged},
		},
		{
			name:               "peer adds a completely new group",
			localServerID:      serverA,
			localGroups:        []apidiscoveryv2.APIGroupDiscovery{g1v1r1},
			peerGroupDiscovery: map[string][]apidiscoveryv2.APIGroupDiscovery{"serverB": {g2v1r1}},
			wantResult:         []apidiscoveryv2.APIGroupDiscovery{g1v1r1, g2v1r1},
		},
		{
			name:               "peer has same data but different group order",
			localServerID:      serverA,
			localGroups:        []apidiscoveryv2.APIGroupDiscovery{g1v1r1, g2v1r1},                         // g1, g2
			peerGroupDiscovery: map[string][]apidiscoveryv2.APIGroupDiscovery{"serverB": {g2v1r1, g1v1r1}}, // g2, g1
			wantResult:         []apidiscoveryv2.APIGroupDiscovery{g1v1r1, g2v1r1},                         // Final order is [g1, g2] due to topo-sort
		},
		{
			name:               "deterministic merge (Server A's view)",
			localServerID:      serverA,
			localGroups:        []apidiscoveryv2.APIGroupDiscovery{g1v1r1},                         // A has g1
			peerGroupDiscovery: map[string][]apidiscoveryv2.APIGroupDiscovery{"serverB": {g2v1r1}}, // B has g2
			wantResult:         []apidiscoveryv2.APIGroupDiscovery{g1v1r1, g2v1r1},                 // Sorted order is [g1, g2]
		},
		{
			name:               "deterministic merge (Server B's view)",
			localServerID:      serverB,
			localGroups:        []apidiscoveryv2.APIGroupDiscovery{g2v1r1},                         // B has g2
			peerGroupDiscovery: map[string][]apidiscoveryv2.APIGroupDiscovery{"serverA": {g1v1r1}}, // A has g1
			wantResult:         []apidiscoveryv2.APIGroupDiscovery{g1v1r1, g2v1r1},                 // Sorted order is [g1, g2]
		},
		{
			name:               "peer adds a new verb to existing resource (triggers Case 2)",
			localServerID:      serverA,
			localGroups:        []apidiscoveryv2.APIGroupDiscovery{gVerbs1},
			peerGroupDiscovery: map[string][]apidiscoveryv2.APIGroupDiscovery{serverB: {gVerbs2}},
			wantResult:         []apidiscoveryv2.APIGroupDiscovery{gVerbsMerged},
		},
		{
			name:               "peer adds a new subresource to existing resource (triggers Case 2)",
			localServerID:      serverA,
			localGroups:        []apidiscoveryv2.APIGroupDiscovery{gSub1},
			peerGroupDiscovery: map[string][]apidiscoveryv2.APIGroupDiscovery{serverB: {gSub2}},
			wantResult:         []apidiscoveryv2.APIGroupDiscovery{gSubMerged},
		},
		{
			name:               "empty local, non-empty peer (triggers Case 3)",
			localServerID:      serverA,
			localGroups:        []apidiscoveryv2.APIGroupDiscovery{},
			peerGroupDiscovery: map[string][]apidiscoveryv2.APIGroupDiscovery{serverB: {g1v1r1}},
			wantResult:         []apidiscoveryv2.APIGroupDiscovery{g1v1r1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := discoveryendpoint.TestMergeResources(tt.localServerID, tt.localGroups, tt.peerGroupDiscovery)
			if !reflect.DeepEqual(result, tt.wantResult) {
				t.Errorf("mergeResources() mismatch (-want +got):\n%s", cmp.Diff(tt.wantResult, result))
			}

			// 按需专门测试短路（short-circuit）情形
			if tt.validateShortCircuit {
				// reflect.ValueOf(slice).Pointer() 给出底层数组的地址。
				// 若短路生效，结果 slice 应与 localGroups 是*完全相同*的
				// slice（同一指针），而不只是 DeepEqual 相等。
				if reflect.ValueOf(result).Pointer() != reflect.ValueOf(tt.localGroups).Pointer() {
					t.Errorf("Short-circuit failed: function returned a new slice instead of the original localGroups")
				}
			}
		})
	}
}

// mockPeerDiscoveryProvider 是固定返回预置资源的 PeerDiscoveryProvider。
type mockPeerDiscoveryProvider struct {
	resources map[string][]apidiscoveryv2.APIGroupDiscovery
}

// responseRecorder 是一个最小化的 http.ResponseWriter，用于测试状态码与响应头
type responseRecorder struct {
	header     http.Header
	statusCode int
	body       strings.Builder
}

func (r *responseRecorder) Header() http.Header         { return r.header }
func (r *responseRecorder) Write(b []byte) (int, error) { return r.body.Write(b) }
func (r *responseRecorder) WriteHeader(statusCode int)  { r.statusCode = statusCode }

func (m *mockPeerDiscoveryProvider) GetPeerResources() map[string][]apidiscoveryv2.APIGroupDiscovery {
	return m.resources
}

// addResource 向指定 server 追加一个组发现结果，必要时初始化 map。
func (m *mockPeerDiscoveryProvider) addResource(serverID string, gvr schema.GroupVersionResource, groupDiscovery apidiscoveryv2.APIGroupDiscovery) {
	if m.resources == nil {
		m.resources = make(map[string][]apidiscoveryv2.APIGroupDiscovery)
	}

	if _, exists := m.resources[serverID]; !exists {
		m.resources[serverID] = []apidiscoveryv2.APIGroupDiscovery{}
	}
	m.resources[serverID] = append(m.resources[serverID], groupDiscovery)
}

// newAPIGroup 构造只含一个版本、一个资源的组发现对象。
func newAPIGroup(groupName, versionName, resourceName string) apidiscoveryv2.APIGroupDiscovery {
	return apidiscoveryv2.APIGroupDiscovery{
		ObjectMeta: metav1.ObjectMeta{Name: groupName},
		Versions: []apidiscoveryv2.APIVersionDiscovery{
			{
				Version: versionName,
				Resources: []apidiscoveryv2.APIResourceDiscovery{
					{Resource: resourceName},
				},
			},
		},
	}
}
