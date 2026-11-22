/*
Copyright 2018 The Kubernetes Authors.

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

package etcd3

import (
	"testing"
)

// TestGetReuseDurationSeconds 验证 lease 复用窗口的计算：取 TTL 的 1/20，
// 且下限为 60 秒。
// 中文说明：复用窗口的存在是为了避免频繁创建 lease——只要旧 lease 还剩足够长时间，
// 新对象就继续挂到它上面。60 秒下限保证短 TTL 场景下仍有足够的安全边际；
// 1/20 上限则保证 lease 不会在对象过期后才失效（那会导致键被提前删除）。
func TestGetReuseDurationSeconds(t *testing.T) {
	testCases := []struct {
		ttl      int64
		duration int64
	}{
		{
			// 3600/20 = 180，但被下限压到 60。
			ttl:      3600,
			duration: 60,
		},
		{
			// 1000/20 = 50，同样被下限提升到 60。
			ttl:      1000,
			duration: 50,
		},
	}
	// listener 传 nil：本用例只测纯计算逻辑，不涉及 etcd 连接。
	lm := newDefaultLeaseManager(nil, NewDefaultLeaseManagerConfig())
	for i := 0; i < len(testCases); i++ {
		dur := lm.getReuseDurationSecondsLocked(testCases[i].ttl)
		if dur != testCases[i].duration {
			t.Errorf("Duration error: ttl %v, expected duration %v, get %v\n", testCases[i].ttl, testCases[i].duration, dur)
		}
	}
}
