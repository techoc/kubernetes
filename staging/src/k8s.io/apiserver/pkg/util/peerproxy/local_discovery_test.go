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

// Package peerproxy 实现 apiserver 之间的对等代理（peer proxying）：
// 当某资源只存在于部分 apiserver 时，把请求转发给拥有该资源的 peer，
// 以支持多版本/多组件的滚动升级。本测试文件聚焦本地发现缓存的判定逻辑。
package peerproxy

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// TestShouldServeLocally_Table 验证 shouldServeLocally 的三态判定：
// 缓存明确为 true 才本地服务；明确为 false、缺失或缓存为空都必须返回 false，
// 即宁可交给 peer 处理，也不要错误地本地返回 404。
func TestShouldServeLocally_Table(t *testing.T) {
	testCases := []struct {
		name  string
		cache map[schema.GroupVersionResource]bool
		gvr   schema.GroupVersionResource
		want  bool
	}{
		{
			name:  "resource present and true",
			cache: map[schema.GroupVersionResource]bool{{Group: "foo", Version: "v1", Resource: "bars"}: true},
			gvr:   schema.GroupVersionResource{Group: "foo", Version: "v1", Resource: "bars"},
			want:  true,
		},
		{
			name:  "resource present and false",
			cache: map[schema.GroupVersionResource]bool{{Group: "foo", Version: "v1", Resource: "bars"}: false},
			gvr:   schema.GroupVersionResource{Group: "foo", Version: "v1", Resource: "bars"},
			want:  false,
		},
		{
			name:  "resource not present",
			cache: map[schema.GroupVersionResource]bool{{Group: "foo", Version: "v1", Resource: "bars"}: true},
			gvr:   schema.GroupVersionResource{Group: "foo", Version: "v1", Resource: "baz"},
			want:  false,
		},
		{
			name:  "empty cache",
			cache: map[schema.GroupVersionResource]bool{},
			gvr:   schema.GroupVersionResource{Group: "foo", Version: "v1", Resource: "bars"},
			want:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 直接注入缓存快照，绕过真实的发现信息同步。
			h := &peerProxyHandler{}
			h.localDiscoveryInfoCache.Store(tc.cache)
			got := h.shouldServeLocally(tc.gvr)
			if got != tc.want {
				t.Errorf("shouldServeLocally(%v) = %v, want %v", tc.gvr, got, tc.want)
			}
		})
	}
}
