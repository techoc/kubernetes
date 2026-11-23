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

package client

import (
	"testing"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/util/watchlist"
)

// TestDoesClientSupportWatchListSemantics 验证包装客户端的 WatchList 语义判定：
// 真实 kube 客户端应被判定为支持（返回 false），fake 客户端被判定为不支持（返回 true）。
func TestDoesClientSupportWatchListSemantics(t *testing.T) {
	// 表结构字段：name 为场景名，clientSet 为底层客户端，
	// expected... 为期望的“不支持 WatchList”判定结果。
	scenarios := []struct {
		name                                           string
		clientSet                                      kubernetes.Interface
		expectedDoesClientNotSupportWatchListSemantics bool
	}{
		// 场景一：真实客户端，期望支持 WatchList，即“不支持”为 false。
		{
			name:      "DR with real kube client supports WatchList semantics",
			clientSet: kubernetes.NewForConfigOrDie(&restclient.Config{}),
			expectedDoesClientNotSupportWatchListSemantics: false,
		},
		// 场景二：fake 客户端，期望不支持 WatchList，即“不支持”为 true。
		{
			name:      "DR with fake kube client does NOT support WatchList semantics",
			clientSet: fake.NewClientset(),
			expectedDoesClientNotSupportWatchListSemantics: true,
		},
	}

	for _, scenario := range scenarios {
		// 每个场景作为独立子测试执行。
		t.Run(scenario.name, func(t *testing.T) {
			// 用给定客户端集合构造被测包装客户端。
			target := New(scenario.clientSet)
			// 调用 client-go 工具函数获得实际判定。
			actual := watchlist.DoesClientNotSupportWatchListSemantics(target)
			// 断言实际判定与期望一致。
			if actual != scenario.expectedDoesClientNotSupportWatchListSemantics {
				t.Fatalf("watchlist.DoesClientNotSupportWatchListSemantics, got: %v, want: %v", actual, scenario.expectedDoesClientNotSupportWatchListSemantics)
			}
		})
	}
}
