/*
Copyright 2024 The Kubernetes Authors.

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

package watchlist

import (
	"testing"

	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/utils/ptr"
)

// supportsWLS 伪造“支持 WatchList 语义”的客户端：该方法返回 false 表示未声明不支持。
type supportsWLS struct{}

func (supportsWLS) IsWatchListSemanticsUnSupported() bool { return false }

// doesNotSupportWLS 伪造显式声明不支持 WatchList 语义的客户端（例如旧版本服务端或代理）。
type doesNotSupportWLS struct{}

func (doesNotSupportWLS) IsWatchListSemanticsUnSupported() bool { return true }

// TestDoesClientNotSupportWatchListSemantics 覆盖接口探测的四种情况：返回 false/true、
// 未实现该可选接口、以及 nil 客户端。未实现与 nil 都按“支持”处理，以保持向后兼容。
func TestDoesClientNotSupportWatchListSemantics(t *testing.T) {
	scenarios := []struct {
		name                                string
		client                              any
		expectUnSupportedWatchListSemantics bool
	}{
		{
			name:                                "client implements the unSupportedWatchListSemantics interface and returns false",
			client:                              supportsWLS{},
			expectUnSupportedWatchListSemantics: false,
		},
		{
			name:                                "client implements the unSupportedWatchListSemantics interface and returns true",
			client:                              doesNotSupportWLS{},
			expectUnSupportedWatchListSemantics: true,
		},
		{
			name:                                "client does not implement the unSupportedWatchListSemantics interface",
			client:                              struct{}{},
			expectUnSupportedWatchListSemantics: false,
		},
		{
			name:                                "nil client",
			client:                              nil,
			expectUnSupportedWatchListSemantics: false,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			got := DoesClientNotSupportWatchListSemantics(scenario.client)
			if got != scenario.expectUnSupportedWatchListSemantics {
				t.Errorf("DoesClientNotSupportWatchListSemantics returned: %v, want: %v", got, scenario.expectUnSupportedWatchListSemantics)
			}
		})
	}
}

// TestPrepareWatchListOptionsFromListOptions 测试以下场景：
//
// +--------------------------+-----------------+---------+-----------------+
// |   ResourceVersionMatch   | ResourceVersion |  Limit  |  Continuation   |
// +--------------------------+-----------------+---------+-----------------+
// | unset/NotOlderThan/Exact | unset/0/100     | unset/4 | unset/FakeToken |
// +--------------------------+-----------------+---------+-----------------+
//
// 能否改写为 watch-list 请求由 feature gate 与这四个字段共同决定：
//   - feature gate 关闭：一律不改写。
//   - 已带 Continue：说明调用方处在分页 list 过程中，不能替换语义。
//   - ResourceVersionMatch=Exact 且指定了 ResourceVersion：exact 语义与 watch-list 不兼容。
//   - 其余组合（含 NotOlderThan）都可安全改写，并把 RV 原样带过去。
func TestPrepareWatchListOptionsFromListOptions(t *testing.T) {
	scenarios := []struct {
		name              string
		listOptions       metav1.ListOptions
		enableWatchListFG bool

		expectToPrepareWatchListOptions bool
		expectedWatchListOptions        metav1.ListOptions
	}{

		{
			name:                            "can't enable watch list for: WatchListClient=off, RVM=unset, RV=unset, Limit=unset, Continuation=unset",
			enableWatchListFG:               false,
			expectToPrepareWatchListOptions: false,
		},
		//		+----------------------+-----------------+-------+--------------+
		//		| ResourceVersionMatch | ResourceVersion | Limit | Continuation |
		//		+----------------------+-----------------+-------+--------------+
		//		| unset                | unset           | unset | unset        |
		//		| unset                | 0               | unset | unset        |
		//		| unset                | 100             | unset | unset        |
		//		| unset                | 0               | 4     | unset        |
		//		| unset                | 0               | unset | FakeToken    |
		//		+----------------------+-----------------+-------+--------------+
		{
			name:                            "can enable watch list for: RVM=unset, RV=unset, Limit=unset, Continuation=unset",
			enableWatchListFG:               true,
			expectToPrepareWatchListOptions: true,
			expectedWatchListOptions:        expectedWatchListOptionsFor(""),
		},
		{
			name:                            "can enable watch list for: RVM=unset, RV=0, Limit=unset, Continuation=unset",
			listOptions:                     metav1.ListOptions{ResourceVersion: "0"},
			enableWatchListFG:               true,
			expectToPrepareWatchListOptions: true,
			expectedWatchListOptions:        expectedWatchListOptionsFor("0"),
		},
		{
			name:                            "can enable watch list for: RVM=unset, RV=100, Limit=unset, Continuation=unset",
			listOptions:                     metav1.ListOptions{ResourceVersion: "100"},
			enableWatchListFG:               true,
			expectToPrepareWatchListOptions: true,
			expectedWatchListOptions:        expectedWatchListOptionsFor("100"),
		},
		// legacy 前缀的用例来自旧版 client-go 的调用路径：即便带 Limit 也允许改写，
		// 因为 watch-list 本身不使用分页。
		{
			name:                            "legacy: can enable watch list for: RVM=unset, RV=0, Limit=4,  Continuation=unset",
			listOptions:                     metav1.ListOptions{ResourceVersion: "0", Limit: 4},
			enableWatchListFG:               true,
			expectToPrepareWatchListOptions: true,
			expectedWatchListOptions:        expectedWatchListOptionsFor("0"),
		},
		{
			name:                            "can't enable watch list for: RVM=unset, RV=0, Limit=unset, Continuation=FakeToken",
			listOptions:                     metav1.ListOptions{ResourceVersion: "0", Continue: "FakeToken"},
			enableWatchListFG:               true,
			expectToPrepareWatchListOptions: false,
		},
		//		+----------------------+-----------------+-------+--------------+
		//		| ResourceVersionMatch | ResourceVersion | Limit | Continuation |
		//		+----------------------+-----------------+-------+--------------+
		//		| NotOlderThan         | unset           | unset | unset        |
		//		| NotOlderThan         | 0               | unset | unset        |
		//		| NotOlderThan         | 100             | unset | unset        |
		//		| NotOlderThan         | 0               | 4     | unset        |
		//		| NotOlderThan         | 0               | unset | FakeToken    |
		//		+----------------------+-----------------+-------+--------------+
		{
			name:                            "can't enable watch list for: RVM=NotOlderThan, RV=unset, Limit=unset, Continuation=unset",
			listOptions:                     metav1.ListOptions{ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan},
			enableWatchListFG:               true,
			expectToPrepareWatchListOptions: false,
		},
		{
			name:                            "can enable watch list for: RVM=NotOlderThan, RV=0, Limit=unset, Continuation=unset",
			listOptions:                     metav1.ListOptions{ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan, ResourceVersion: "0"},
			enableWatchListFG:               true,
			expectToPrepareWatchListOptions: true,
			expectedWatchListOptions:        expectedWatchListOptionsFor("0"),
		},
		{
			name:                            "can enable watch list for: RVM=NotOlderThan, RV=100, Limit=unset, Continuation=unset",
			listOptions:                     metav1.ListOptions{ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan, ResourceVersion: "100"},
			enableWatchListFG:               true,
			expectToPrepareWatchListOptions: true,
			expectedWatchListOptions:        expectedWatchListOptionsFor("100"),
		},
		{
			name:                            "legacy: can enable watch list for: RVM=NotOlderThan, RV=0, Limit=4, Continuation=unset",
			listOptions:                     metav1.ListOptions{ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan, ResourceVersion: "0", Limit: 4},
			enableWatchListFG:               true,
			expectToPrepareWatchListOptions: true,
			expectedWatchListOptions:        expectedWatchListOptionsFor("0"),
		},
		{
			name:                            "can't enable watch list for: RVM=NotOlderThan, RV=0, Limit=unset, Continuation=FakeToken",
			listOptions:                     metav1.ListOptions{ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan, ResourceVersion: "0", Continue: "FakeToken"},
			enableWatchListFG:               true,
			expectToPrepareWatchListOptions: false,
		},

		//		+----------------------+-----------------+-------+--------------+
		//		| ResourceVersionMatch | ResourceVersion | Limit | Continuation |
		//		+----------------------+-----------------+-------+--------------+
		//		| Exact                | unset           | unset | unset        |
		//		| Exact                | 0               | unset | unset        |
		//		| Exact                | 100             | unset | unset        |
		//		| Exact                | 0               | 4     | unset        |
		//		| Exact                | 0               | unset | FakeToken    |
		//		+----------------------+-----------------+-------+--------------+
		{
			name:                            "can't enable watch list for: RVM=Exact, RV=unset, Limit=unset, Continuation=unset",
			listOptions:                     metav1.ListOptions{ResourceVersionMatch: metav1.ResourceVersionMatchExact},
			enableWatchListFG:               true,
			expectToPrepareWatchListOptions: false,
		},
		{
			name:                            "can enable watch list for: RVM=Exact, RV=0, Limit=unset, Continuation=unset",
			listOptions:                     metav1.ListOptions{ResourceVersionMatch: metav1.ResourceVersionMatchExact, ResourceVersion: "0"},
			enableWatchListFG:               true,
			expectToPrepareWatchListOptions: false,
		},
		{
			name:                            "can enable watch list for: RVM=Exact, RV=100, Limit=unset, Continuation=unset",
			listOptions:                     metav1.ListOptions{ResourceVersionMatch: metav1.ResourceVersionMatchExact, ResourceVersion: "100"},
			enableWatchListFG:               true,
			expectToPrepareWatchListOptions: false,
		},
		{
			name:                            "can't enable watch list for: RVM=Exact, RV=0, Limit=4,  Continuation=unset",
			listOptions:                     metav1.ListOptions{ResourceVersionMatch: metav1.ResourceVersionMatchExact, ResourceVersion: "0", Limit: 4},
			enableWatchListFG:               true,
			expectToPrepareWatchListOptions: false,
		},
		{
			name:                            "can't enable watch list for: RVM=Exact, RV=0, Limit=unset, Continuation=FakeToken",
			listOptions:                     metav1.ListOptions{ResourceVersionMatch: metav1.ResourceVersionMatchExact, ResourceVersion: "0", Continue: "FakeToken"},
			enableWatchListFG:               true,
			expectToPrepareWatchListOptions: false,
		},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, scenario.enableWatchListFG)

			watchListOptions, hasWatchListOptionsPrepared, err := PrepareWatchListOptionsFromListOptions(scenario.listOptions)

			require.NoError(t, err)
			require.Equal(t, scenario.expectToPrepareWatchListOptions, hasWatchListOptionsPrepared)
			require.Equal(t, scenario.expectedWatchListOptions, watchListOptions)
		})
	}
}

// expectedWatchListOptionsFor 构造 watch-list 请求的预期选项：它把普通 list 转换为
// “带 NotOlderThan 语义的 watch，并要求服务端发送初始事件与 bookmark”。
func expectedWatchListOptionsFor(rv string) metav1.ListOptions {
	var watchListOptions metav1.ListOptions

	watchListOptions.ResourceVersion = rv
	watchListOptions.ResourceVersionMatch = metav1.ResourceVersionMatchNotOlderThan
	watchListOptions.Watch = true
	watchListOptions.AllowWatchBookmarks = true
	watchListOptions.SendInitialEvents = ptr.To(true)

	return watchListOptions
}
