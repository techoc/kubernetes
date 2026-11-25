/*
Copyright 2016 The Kubernetes Authors.

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
	"time"

	"github.com/google/go-cmp/cmp"
	testingclock "k8s.io/utils/clock/testing"
)

// expectEntry 断言 key 在缓存中存在且值等于 value。
func expectEntry(t *testing.T, c *LRUExpireCache, key interface{}, value interface{}) {
	t.Helper()
	result, ok := c.Get(key)
	if !ok || result != value {
		t.Errorf("Expected cache[%v]: %v, got %v", key, value, result)
	}
}

// expectNotEntry 断言 key 不存在（或已过期）于缓存中。
func expectNotEntry(t *testing.T, c *LRUExpireCache, key interface{}) {
	t.Helper()
	if result, ok := c.Get(key); ok {
		t.Errorf("Expected cache[%v] to be empty, got %v", key, result)
	}
}

// 注意：Get() 会改变淘汰列表，因此应先检查 key，再检查具体条目。
// assertKeys 在比较键序列前不进任何 Get 调用：Get 会改变 LRU 驱逐顺序，
// 从而影响 Keys() 的返回顺序。
func assertKeys(t *testing.T, gotKeys, wantKeys []interface{}) {
	t.Helper()
	if diff := cmp.Diff(gotKeys, wantKeys); diff != "" {
		t.Errorf("Wrong result for keys: diff (-got +want):\n%s", diff)
	}
}

// TestSimpleGet 验证写入后能按键取回值。
func TestSimpleGet(t *testing.T) {
	c := NewLRUExpireCache(10)
	c.Add("long-lived", "12345", 10*time.Hour)

	assertKeys(t, c.Keys(), []interface{}{"long-lived"})

	expectEntry(t, c, "long-lived", "12345")
}

// TestSimpleRemove 验证 Remove 后键从缓存中消失。
func TestSimpleRemove(t *testing.T) {
	c := NewLRUExpireCache(10)
	c.Add("long-lived", "12345", 10*time.Hour)
	c.Remove("long-lived")

	assertKeys(t, c.Keys(), []interface{}{})

	expectNotEntry(t, c, "long-lived")
}

// TestSimpleRemoveAll 验证按谓词批量删除：只移除命中项，其余保留。
func TestSimpleRemoveAll(t *testing.T) {
	c := NewLRUExpireCache(10)
	c.Add("long-lived", "12345", 10*time.Hour)
	c.Add("other-long-lived", "12345", 10*time.Hour)
	c.RemoveAll(func(k any) bool {
		return k.(string) == "long-lived"
	})

	assertKeys(t, c.Keys(), []any{"other-long-lived"})

	expectNotEntry(t, c, "long-lived")
	expectEntry(t, c, "other-long-lived", "12345")
}

// TestExpiredGet 用假时钟推进时间使条目过期，验证过期条目既取不到也不出现在 Keys 中。
func TestExpiredGet(t *testing.T) {
	fakeClock := testingclock.NewFakeClock(time.Now())
	c := NewLRUExpireCacheWithClock(10, fakeClock)
	c.Add("short-lived", "12345", 1*time.Millisecond)
	//确保条目已过期
	fakeClock.Step(2 * time.Millisecond)

	//Keys() 不应返回已过期的 key。
	assertKeys(t, c.Keys(), []interface{}{})

	expectNotEntry(t, c, "short-lived")
}

// TestLRUOverflow 验证超过容量时淘汰最久未使用的条目（elem1）。
func TestLRUOverflow(t *testing.T) {
	c := NewLRUExpireCache(4)
	c.Add("elem1", "1", 10*time.Hour)
	c.Add("elem2", "2", 10*time.Hour)
	c.Add("elem3", "3", 10*time.Hour)
	c.Add("elem4", "4", 10*time.Hour)
	c.Add("elem5", "5", 10*time.Hour)

	assertKeys(t, c.Keys(), []interface{}{"elem2", "elem3", "elem4", "elem5"})

	expectNotEntry(t, c, "elem1")
	expectEntry(t, c, "elem2", "2")
	expectEntry(t, c, "elem3", "3")
	expectEntry(t, c, "elem4", "4")
	expectEntry(t, c, "elem5", "5")
}

// TestAddBringsToFront 验证重复 Add 会把条目提升为最近使用，
// 从而在后续溢出时被淘汰的是更早的 elem2。
func TestAddBringsToFront(t *testing.T) {
	c := NewLRUExpireCache(4)
	c.Add("elem1", "1", 10*time.Hour)
	c.Add("elem2", "2", 10*time.Hour)
	c.Add("elem3", "3", 10*time.Hour)
	c.Add("elem4", "4", 10*time.Hour)

	c.Add("elem1", "1-new", 10*time.Hour)

	c.Add("elem5", "5", 10*time.Hour)

	assertKeys(t, c.Keys(), []interface{}{"elem3", "elem4", "elem1", "elem5"})

	expectNotEntry(t, c, "elem2")
	expectEntry(t, c, "elem1", "1-new")
	expectEntry(t, c, "elem3", "3")
	expectEntry(t, c, "elem4", "4")
	expectEntry(t, c, "elem5", "5")
}

// TestGetBringsToFront 验证读取也会提升条目的最近使用顺序。
func TestGetBringsToFront(t *testing.T) {
	c := NewLRUExpireCache(4)
	c.Add("elem1", "1", 10*time.Hour)
	c.Add("elem2", "2", 10*time.Hour)
	c.Add("elem3", "3", 10*time.Hour)
	c.Add("elem4", "4", 10*time.Hour)

	c.Get("elem1")

	c.Add("elem5", "5", 10*time.Hour)

	assertKeys(t, c.Keys(), []interface{}{"elem3", "elem4", "elem1", "elem5"})

	expectNotEntry(t, c, "elem2")
	expectEntry(t, c, "elem1", "1")
	expectEntry(t, c, "elem3", "3")
	expectEntry(t, c, "elem4", "4")
	expectEntry(t, c, "elem5", "5")
}

// TestLRUKeys 验证 Keys 按写入（最近使用）顺序返回。
func TestLRUKeys(t *testing.T) {
	c := NewLRUExpireCache(4)
	c.Add("elem1", "1", 10*time.Hour)
	c.Add("elem2", "2", 10*time.Hour)
	c.Add("elem3", "3", 10*time.Hour)
	c.Add("elem4", "4", 10*time.Hour)

	assertKeys(t, c.Keys(), []interface{}{"elem1", "elem2", "elem3", "elem4"})
}
