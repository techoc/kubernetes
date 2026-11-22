/*
Copyright 2019 The Kubernetes Authors.

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
	"context"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	testingclock "k8s.io/utils/clock/testing"
)

// TestExpiringCache 验证缓存的基本读写语义：未命中返回 nil,false；写入后可读；
// 新值覆盖旧值；删除后不再命中。
func TestExpiringCache(t *testing.T) {
	cache := NewExpiring()

	if result, ok := cache.Get("foo"); ok || result != nil {
		t.Errorf("Expected null, false, got %#v, %v", result, ok)
	}

	record1 := "bob"
	record2 := "alice"

	// 为空时写入记录
	cache.Set("foo", record1, time.Hour)
	if result, ok := cache.Get("foo"); !ok || result != record1 {
		t.Errorf("Expected %#v, true, got %#v, %v", record1, result, ok)
	}

	// 较新的记录会覆盖旧记录
	cache.Set("foo", record2, time.Hour)
	if result, ok := cache.Get("foo"); !ok || result != record2 {
		t.Errorf("Expected %#v, true, got %#v, %v", record2, result, ok)
	}

	// 删除当前值
	cache.Delete("foo")
	if result, ok := cache.Get("foo"); ok || result != nil {
		t.Errorf("Expected null, false, got %#v, %v", result, ok)
	}
}

// TestExpiration 用 FakeClock 验证 TTL 过期、重新 Set 重置 TTL、清理与重置竞争时
// 不误删新条目（generation 机制），以及 AllowExpiredGet 对过期项的读取语义。
func TestExpiration(t *testing.T) {
	fc := &testingclock.FakeClock{}
	c := NewExpiringWithClock(fc)

	c.Set("a", "a", time.Second)

	fc.Step(500 * time.Millisecond)
	if _, ok := c.Get("a"); !ok {
		t.Fatalf("we should have found a key")
	}

	fc.Step(time.Second)
	if _, ok := c.Get("a"); ok {
		t.Fatalf("we should not have found a key")
	}

	c.Set("a", "a", time.Second)

	fc.Step(500 * time.Millisecond)
	if _, ok := c.Get("a"); !ok {
		t.Fatalf("we should have found a key")
	}

	// 重新 Set 应重启 TTL
	c.Set("a", "a", time.Second)

	fc.Step(750 * time.Millisecond)
	if _, ok := c.Get("a"); !ok {
		t.Fatalf("we should have found a key")
	}

	// 模拟 reset 与 cleanup 之间的竞争：断言清理不会删掉已被重置的键。
	// 提升 generation 并延后 expiry，等价于并发 Set 已经发生。
	c.Set("a", "a", time.Second)

	e := c.cache["a"]
	e.generation++
	e.expiry = e.expiry.Add(1 * time.Second)
	c.cache["a"] = e

	fc.Step(1 * time.Second)
	if _, ok := c.Get("a"); !ok {
		t.Fatalf("we should have found a key")
	}

	// 验证 AllowExpiredGet 开关下读取过期键的行为
	c.Set("b", "b", time.Second)
	fc.Step(2 * time.Second)
	if _, ok := c.Get("b"); ok {
		t.Fatalf("we should not have found b key")
	}
	if count := c.Len(); count != 2 { // b 仍在缓存中
		t.Errorf("expected two items got: %d", count)
	}
	c.AllowExpiredGet = true
	if _, ok := c.Get("b"); !ok {
		t.Fatalf("we should have found b key")
	}
	if count := c.Len(); count != 2 { // b 仍在缓存中
		t.Errorf("expected two items got: %d", count)
	}
	c.Set("c", "c", time.Second)      // 写入一个无关键以触发 gc
	if count := c.Len(); count != 2 { // 现在缓存中只剩 a 和 c
		t.Errorf("expected two items got: %d", count)
	}
	if _, ok := c.Get("b"); ok {
		t.Fatalf("we should not have found b key")
	}
	if _, ok := c.Get("a"); !ok {
		t.Fatalf("we should have found a key")
	}
	if _, ok := c.Get("c"); !ok {
		t.Fatalf("we should have found c key")
	}
}

// TestGarbageCollection 验证 gc(now) 只清除已过期条目，并在同一次清理中处理多个过期项；
// 条目写入顺序与过期时间无关。
func TestGarbageCollection(t *testing.T) {
	fc := &testingclock.FakeClock{}

	type entry struct {
		key, val string
		ttl      time.Duration
	}

	tests := []struct {
		name string
		now  time.Time
		set  []entry
		want map[string]string
	}{
		{
			name: "two entries just set",
			now:  fc.Now().Add(0 * time.Second),
			set: []entry{
				{"a", "aa", 1 * time.Second},
				{"b", "bb", 2 * time.Second},
			},
			want: map[string]string{
				"a": "aa",
				"b": "bb",
			},
		},
		{
			name: "first entry expired now",
			now:  fc.Now().Add(1 * time.Second),
			set: []entry{
				{"a", "aa", 1 * time.Second},
				{"b", "bb", 2 * time.Second},
			},
			want: map[string]string{
				"b": "bb",
			},
		},
		{
			name: "first entry expired half a second ago",
			now:  fc.Now().Add(1500 * time.Millisecond),
			set: []entry{
				{"a", "aa", 1 * time.Second},
				{"b", "bb", 2 * time.Second},
			},
			want: map[string]string{
				"b": "bb",
			},
		},
		{
			name: "three entries weird order",
			now:  fc.Now().Add(1 * time.Second),
			set: []entry{
				{"c", "cc", 3 * time.Second},
				{"a", "aa", 1 * time.Second},
				{"b", "bb", 2 * time.Second},
			},
			want: map[string]string{
				"b": "bb",
				"c": "cc",
			},
		},
		{
			name: "expire multiple entries in one cycle",
			now:  fc.Now().Add(2500 * time.Millisecond),
			set: []entry{
				{"a", "aa", 1 * time.Second},
				{"b", "bb", 2 * time.Second},
				{"c", "cc", 3 * time.Second},
			},
			want: map[string]string{
				"c": "cc",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := NewExpiringWithClock(fc)
			for _, e := range test.set {
				c.Set(e.key, e.val, e.ttl)
			}

			c.gc(test.now)

			for k, want := range test.want {
				got, ok := c.Get(k)
				if !ok {
					t.Errorf("expected cache to have entry for key=%q but found none", k)
					continue
				}
				if got != want {
					t.Errorf("unexpected value for key=%q: got=%q, want=%q", k, got, want)
				}
			}
			if got, want := c.Len(), len(test.want); got != want {
				t.Errorf("unexpected cache size: got=%d, want=%d", got, want)
			}
		})
	}
}

// BenchmarkExpiringCacheContention 在不同淘汰概率下测量并发读写与删除的争用情况。
func BenchmarkExpiringCacheContention(b *testing.B) {
	b.Run("evict_probablility=100%", func(b *testing.B) {
		benchmarkExpiringCacheContention(b, 1)
	})
	b.Run("evict_probablility=10%", func(b *testing.B) {
		benchmarkExpiringCacheContention(b, 0.1)
	})
	b.Run("evict_probablility=1%", func(b *testing.B) {
		benchmarkExpiringCacheContention(b, 0.01)
	})
}

// benchmarkExpiringCacheContention 用大量键并行执行 Get/Delete/Set，模拟真实高争用场景。
func benchmarkExpiringCacheContention(b *testing.B, prob float64) {
	const numKeys = 1 << 16
	cache := NewExpiring()

	keys := []string{}
	for i := 0; i < numKeys; i++ {
		key := uuid.New().String()
		keys = append(keys, key)
	}

	b.ResetTimer()

	b.SetParallelism(256)
	b.RunParallel(func(pb *testing.PB) {
		rand := rand.New(rand.NewSource(rand.Int63()))
		for pb.Next() {
			i := rand.Int31()
			key := keys[i%numKeys]
			_, ok := cache.Get(key)
			if ok {
				// 用采样到的 i 的低位决定是否淘汰，使并发下各操作比例可控。
				if rand.Float64() < prob {
					cache.Delete(key)
				}
			} else {
				cache.Set(key, struct{}{}, 50*time.Millisecond)
			}
		}
	})
}

// TestStressExpiringCache 在 5 秒内以 256 个 goroutine 并发读写带 50ms TTL 的缓存，
// 然后用一次 Set 触发 GC，断言过期条目都被清除、只余触发键。
func TestStressExpiringCache(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	const numKeys = 1 << 16
	cache := NewExpiring()

	keys := []string{}
	for i := 0; i < numKeys; i++ {
		key := uuid.New().String()
		keys = append(keys, key)
	}

	var wg sync.WaitGroup
	for i := 0; i < 256; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rand := rand.New(rand.NewSource(rand.Int63()))
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				key := keys[rand.Intn(numKeys)]
				if _, ok := cache.Get(key); !ok {
					cache.Set(key, struct{}{}, 50*time.Millisecond)
				}
			}
		}()
	}

	wg.Wait()

	// 通过一次 Set 触发 GC，并检查缓存大小。
	time.Sleep(60 * time.Millisecond)
	cache.Set("trigger", "gc", time.Second)
	if cache.Len() != 1 {
		t.Errorf("unexpected cache size: got=%d, want=1", cache.Len())
	}
}
