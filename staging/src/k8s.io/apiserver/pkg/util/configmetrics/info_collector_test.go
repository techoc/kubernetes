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

package configmetrics

import (
	"testing"
)

// assertHashesEqual 比较期望与实际的哈希切片。
func assertHashesEqual(t *testing.T, expected, actual []string) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Errorf("expected %d hashes, got %d", len(expected), len(actual))
		return
	}
	for i, hash := range actual {
		if hash != expected[i] {
			t.Errorf("expected hash %q at index %d, got %q", expected[i], i, hash)
		}
	}
}

// TestAtomicHashProvider 验证 AtomicHashProvider 的状态转换：
// 初始为空；SetHashes 后按固定顺序返回 [apiserverIDHash, configHash]；
// 可重复更新；SetHashes() 无参时清空。
func TestAtomicHashProvider(t *testing.T) {
	provider := NewAtomicHashProvider()

	apiServerIDHash := "sha256:abc123"
	configHash := "sha256:hash1"

	// 初始状态：尚未设置任何哈希
	hashes := provider.GetCurrentHashes()
	assertHashesEqual(t, []string{}, hashes)

	// 设置哈希
	provider.SetHashes(apiServerIDHash, configHash)
	hashes = provider.GetCurrentHashes()
	assertHashesEqual(t, []string{apiServerIDHash, configHash}, hashes)

	// 更新配置哈希（apiserver 标识保持不变）
	newConfigHash := "sha256:hash2"
	provider.SetHashes(apiServerIDHash, newConfigHash)
	hashes = provider.GetCurrentHashes()
	assertHashesEqual(t, []string{apiServerIDHash, newConfigHash}, hashes)

	// 清空哈希
	provider.SetHashes()
	hashes = provider.GetCurrentHashes()
	assertHashesEqual(t, []string{}, hashes)
}
