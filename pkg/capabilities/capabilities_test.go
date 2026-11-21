/*
Copyright 2014 The Kubernetes Authors.

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

package capabilities

import (
	"reflect"
	"testing"
)

// TestGet 验证 Get 在两种场景下的行为：
// 未初始化时返回安全的默认能力集；Initialize 后返回所设置的能力集。
func TestGet(t *testing.T) {
	// 每个测试结束后重置全局状态，避免污染其他用例。
	defer ResetForTest()
	defaultCap := Capabilities{
		AllowPrivileged: false,
		PrivilegedSources: PrivilegedSources{
			HostNetworkSources: []string{},
			HostPIDSources:     []string{},
			HostIPCSources:     []string{},
		},
	}

	res := Get()
	if !reflect.DeepEqual(defaultCap, res) {
		t.Fatalf("expected Capabilities: %#v, got a non-default: %#v", defaultCap, res)
	}

	// 重新初始化后，Get 应反映出显式设置的能力。
	cap := Capabilities{
		PrivilegedSources: PrivilegedSources{
			HostNetworkSources: []string{"A", "B"},
		},
	}
	ResetForTest()
	Initialize(cap)

	res = Get()
	if !reflect.DeepEqual(cap, res) {
		t.Fatalf("expected Capabilities: %#v , got a different: %#v", cap, res)
	}
}

// TestSetup 表驱动验证 Setup 的便捷封装：
// 传入的 allowPrivileged 与带宽限制应原样落到全局能力配置中。
func TestSetup(t *testing.T) {
	defer ResetForTest()
	testCases := []struct {
		name                     string
		allowPrivileged          bool
		perConnectionBytesPerSec int64
		expectedCapabilities     Capabilities
	}{
		{
			name:                     "AllowPrivileged true with bandwidth limit",
			allowPrivileged:          true,
			perConnectionBytesPerSec: 1024,
			expectedCapabilities: Capabilities{
				AllowPrivileged:                        true,
				PerConnectionBandwidthLimitBytesPerSec: 1024,
			},
		},
		{
			name:                     "AllowPrivileged false with higher bandwidth limit",
			allowPrivileged:          false,
			perConnectionBytesPerSec: 2048,
			expectedCapabilities: Capabilities{
				AllowPrivileged:                        false,
				PerConnectionBandwidthLimitBytesPerSec: 2048,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 每个用例前重置，因为 Initialize 每个进程只生效一次。
			ResetForTest()

			Setup(tc.allowPrivileged, tc.perConnectionBytesPerSec)
			res := Get()
			if !reflect.DeepEqual(tc.expectedCapabilities, res) {
				t.Fatalf("expected Capabilities: %#v, got: %#v", tc.expectedCapabilities, res)
			}
		})
	}
}
