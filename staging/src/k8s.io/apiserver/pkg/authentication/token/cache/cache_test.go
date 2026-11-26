/*
Copyright 2017 The Kubernetes Authors.

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
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/google/uuid"

	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/utils/clock"
)

// TestSimpleCache 对单分片缓存跑通用行为测试。
func TestSimpleCache(t *testing.T) {
	testCache(newSimpleCache(clock.RealClock{}), t)
}

// 注意：该 benchmark 的性能特征可能与生产环境不同。
// 修改 SimpleCache 时，应分别在有并发和无并发的情况下运行测试，以了解影响。
// 本工具用于在隔离环境下测试与度量缓存的高并发表现，并非评估 Kubernetes 对缓存的使用方式。
//
// BenchmarkCacheContentions 对比单分片与 32 分片缓存在高并发下的争用差异，
// 并覆盖不同 key 数量（256/4096/65536），用来判断分片是否值得引入。
func BenchmarkCacheContentions(b *testing.B) {
	for _, numKeys := range []int{1 << 8, 1 << 12, 1 << 16} {
		b.Run(fmt.Sprintf("Simple/keys=%d", numKeys), func(b *testing.B) {
			benchmarkCache(newSimpleCache(clock.RealClock{}), b, numKeys)
		})
		b.Run(fmt.Sprintf("Striped/keys=%d", numKeys), func(b *testing.B) {
			benchmarkCache(newStripedCache(32, fnvHashFunc, func() cache { return newSimpleCache(clock.RealClock{}) }), b, numKeys)
		})
	}
}

// TestStripedCache 让分片缓存跑同一套行为测试，确认分片不改变语义。
func TestStripedCache(t *testing.T) {
	testCache(newStripedCache(32, fnvHashFunc, func() cache { return newSimpleCache(clock.RealClock{}) }), t)
}

// benchmarkCache 在随机 key 上做 get/remove/set 混合操作，模拟真实令牌校验的访问模式：
// 命中后立即删除（一次性使用），未命中则写入，从而持续制造竞争。
func benchmarkCache(cache cache, b *testing.B, numKeys int) {
	keys := []string{}
	for i := 0; i < numKeys; i++ {
		key := uuid.New().String()
		keys = append(keys, key)
	}

	b.ResetTimer()

	// 高并行度用于放大锁争用，使分片收益显现。
	b.SetParallelism(500)
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := keys[rand.Intn(numKeys)]
			_, ok := cache.get(key)
			if ok {
				cache.remove(key)
			} else {
				cache.set(key, &cacheRecord{}, time.Second)
			}
		}
	})
}

// testCache 验证缓存的四条基本语义：未命中、写入、覆盖、删除。
func testCache(cache cache, t *testing.T) {
	if result, ok := cache.get("foo"); ok || result != nil {
		t.Errorf("Expected null, false, got %#v, %v", result, ok)
	}

	record1 := &cacheRecord{resp: &authenticator.Response{User: &user.DefaultInfo{Name: "bob"}}}
	record2 := &cacheRecord{resp: &authenticator.Response{User: &user.DefaultInfo{Name: "alice"}}}

	// 为空时，记录会被存储
	cache.set("foo", record1, time.Hour)
	if result, ok := cache.get("foo"); !ok || result != record1 {
		t.Errorf("Expected %#v, true, got %#v, %v", record1, result, ok)
	}

	// 较新的记录覆盖旧记录
	// 后写覆盖先写，保证刷新令牌后旧身份不会残留。
	cache.set("foo", record2, time.Hour)
	if result, ok := cache.get("foo"); !ok || result != record2 {
		t.Errorf("Expected %#v, true, got %#v, %v", record2, result, ok)
	}

	// 移除当前值会一并移除
	cache.remove("foo")
	if result, ok := cache.get("foo"); ok || result != nil {
		t.Errorf("Expected null, false, got %#v, %v", result, ok)
	}
}
