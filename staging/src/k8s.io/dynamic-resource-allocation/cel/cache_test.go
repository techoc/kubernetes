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

package cel

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCacheSemantic 验证缓存的核心语义：编译成功的结果会被复用、
// 编译失败的结果不入缓存、容量超限时按 LRU 淘汰，以及成本估算保持关闭。
func TestCacheSemantic(t *testing.T) {
	// Cache two entries.
	//
	// Entries are comparable structs with pointers inside. Each
	// compilation leads to different pointers, so the entries can be
	// compared by value to figure out whether an entry was cached or
	// compiled anew.
	// 用容量为 2 的缓存测试；结果可比较且含指针，比较两次结果是否相等即可判断是否命中缓存。
	cache := NewCache(2, Features{})

	// Successful compilations get cached.
	// 输入 "true"：编译成功，两次调用应返回完全相同的结果。
	resultTrue := cache.GetOrCompile("true")
	require.Nil(t, resultTrue.Error)
	resultTrueAgain := cache.GetOrCompile("true")
	// 断言结果相等，证明第二次命中缓存而非重新编译。
	if resultTrue != resultTrueAgain {
		t.Fatal("result of compiling `true` should have been cached")
	}

	// Unsuccessful ones don't.
	// 输入不存在的变量：编译失败，失败结果不应写入缓存。
	resultFailed := cache.GetOrCompile("no-such-variable")
	require.NotNil(t, resultFailed.Error)
	resultFailedAgain := cache.GetOrCompile("no-such-variable")
	// 两次失败结果应不相等，证明每次都重新编译。
	if resultFailed == resultFailedAgain {
		t.Fatal("result of compiling `no-such-variable` should not have been cached")
	}

	// The cache can hold a second result.
	// 输入 "false"：缓存第二个成功结果，并确认 "true" 仍然命中缓存。
	resultFalse := cache.GetOrCompile("false")
	require.Nil(t, resultFalse.Error)
	resultFalseAgain := cache.GetOrCompile("false")
	if resultFalse != resultFalseAgain {
		t.Fatal("result of compiling `false` should have been cached")
	}
	resultTrueAgain = cache.GetOrCompile("true")
	if resultTrue != resultTrueAgain {
		t.Fatal("result of compiling `true` should still have been cached")
	}

	// A third result pushes out the least recently used one.
	// 输入 "false && true"：第三个结果应把最近最少使用的 "false" 淘汰。
	resultOther := cache.GetOrCompile("false && true")
	require.Nil(t, resultFalse.Error)
	resultOtherAgain := cache.GetOrCompile("false && true")
	if resultOther != resultOtherAgain {
		t.Fatal("result of compiling `false && true` should have been cached")
	}
	resultFalseAgain = cache.GetOrCompile("false")
	// 被淘汰后重新编译，结果指针应与之前不同。
	if resultFalse == resultFalseAgain {
		t.Fatal("result of compiling `false` should have been evicted from the cache")
	}

	// Cost estimation must be off (not needed by scheduler).
	// 断言关闭成本估算时 MaxCost 为 math.MaxUint64。
	if resultFalseAgain.MaxCost != math.MaxUint64 {
		t.Error("cost estimation should have been disabled, was enabled")
	}
}

// TestCacheConcurrency 用 10 个并发 worker 查询不同表达式，
// 借助 go test -race 检测缓存读写是否加锁；每个结果都应无错误。
func TestCacheConcurrency(t *testing.T) {
	// There's no guarantee that concurrent use of the cache would really
	// trigger the race detector in `go test -race`, but in practice
	// it does when not using the cacheMutex.
	//
	// The compileMutex ony affects performance and thus cannot be tested
	// without benchmarking.
	// 本测试只覆盖数据竞争，编译去重属于性能特性，无法通过普通测试验证。
	numWorkers := 10

	cache := NewCache(2, Features{})
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	for i := 0; i < numWorkers; i++ {
		// 每个 worker 编译一个各不相同的表达式，避免键锁串行化。
		go func(i int) {
			defer wg.Done()
			result := cache.GetOrCompile(fmt.Sprintf("%d == %d", i, i))
			// 断言并发编译的结果均没有错误。
			assert.Nil(t, result.Error)
		}(i)
	}
	// 等待所有 worker 完成。
	wg.Wait()
}

// Test that compiling the same expression under different scopes (selector vs.
// derived attribute) isolates the results in different cache keyspaces,
// preventing collisions
//
// TestCacheIsolation 验证选择器与派生属性使用不同的缓存键空间：
// 同一表达式 "42" 在派生属性下成功、在选择器下失败，且两者互不污染。
func TestCacheIsolation(t *testing.T) {
	cache := NewCache(10, Features{})

	// 1. Compiling "42" as a derived attribute must succeed, as derived attributes
	// are allowed to evaluate to integers.
	// 派生属性允许整型结果，因此 "42" 应编译成功。
	resultDerived := cache.GetOrCompileDerivedAttribute("42")
	require.Nil(t, resultDerived.Error)

	// 2. Compiling "42" as a selector must fail, as selectors are strictly
	// restricted to boolean results.
	// 选择器要求布尔结果，"42" 应编译失败，且不污染派生命名空间。
	resultSelector := cache.GetOrCompile("42")
	require.NotNil(t, resultSelector.Error)

	// 3. Re-compiling as a derived attribute must return the cached success entry,
	// proving that the failed selector compilation did not pollute or overwrite
	// the derived namespace.
	// 再次按派生属性编译，应命中第一次的成功结果，证明失败的选择器编译没有覆盖它。
	resultDerivedAgain := cache.GetOrCompileDerivedAttribute("42")
	require.Nil(t, resultDerivedAgain.Error)
	if resultDerived != resultDerivedAgain {
		t.Fatal("successful derived attribute compilation should have been cached")
	}
}
