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

package cache

import (
	"testing"

	"k8s.io/client-go/util/watchlist"
)

// fakeWatchListClient 模拟客户端对 WatchList 语义的支持情况。
type fakeWatchListClient struct {
	unSupportedWatchListSemantics bool
}

// IsWatchListSemanticsUnSupported 返回预设值，供包装函数探测。
func (f fakeWatchListClient) IsWatchListSemanticsUnSupported() bool {
	return f.unSupportedWatchListSemantics
}

// TestToListWatcherWithWatchListSemantics 验证包装后的 ListWatch 能正确反映
// 客户端的 WatchList 能力；未实现该接口的客户端按“支持”处理。
func TestToListWatcherWithWatchListSemantics(t *testing.T) {
	scenarios := []struct {
		name                                string
		client                              any
		expectUnSupportedWatchListSemantics bool
	}{
		{
			name:                                "client which doesn't implement the interface supports WatchList semantics",
			client:                              nil,
			expectUnSupportedWatchListSemantics: false,
		},
		{
			name:                                "client does not support WatchList semantics",
			client:                              fakeWatchListClient{unSupportedWatchListSemantics: true},
			expectUnSupportedWatchListSemantics: true,
		},
		{
			name:                                "client supports WatchList semantics",
			client:                              fakeWatchListClient{unSupportedWatchListSemantics: false},
			expectUnSupportedWatchListSemantics: false,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			target := ToListWatcherWithWatchListSemantics(&ListWatch{}, scenario.client)

			if got := watchlist.DoesClientNotSupportWatchListSemantics(target); got != scenario.expectUnSupportedWatchListSemantics {
				t.Fatalf("DoesClientNotSupportWatchListSemantics returned: %v, want: %v", got, scenario.expectUnSupportedWatchListSemantics)
			}
		})
	}
}
