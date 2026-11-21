/*
Copyright 2015 The Kubernetes Authors.

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

package container

import (
	"fmt"
	"strconv"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/types"
)

// newTestCache 返回测试用的底层 cache 实现。
func newTestCache() *cache {
	c := NewCache()
	return c.(*cache)
}

// TestCacheNotInitialized 验证全局时间戳未设置时查询总是返回 nil。
func TestCacheNotInitialized(t *testing.T) {
	cache := newTestCache()
	// 全局时间戳未设置时，任何查询都返回 nil。
	d := cache.getIfNewerThan(types.UID("1234"), time.Time{})
	assert.Nil(t, d, "should return nil since cache is not initialized")
}

// getTestPodIDAndStatus 生成唯一 Pod ID 和指定容器数量的状态对象。
func getTestPodIDAndStatus(numContainers int) (types.UID, *PodStatus) {
	id := types.UID(strconv.FormatInt(time.Now().UnixNano(), 10))
	name := fmt.Sprintf("cache-foo-%s", string(id))
	namespace := "ns"
	var status *PodStatus
	if numContainers > 0 {
		status = &PodStatus{ID: id, Name: name, Namespace: namespace}
	} else {
		status = &PodStatus{ID: id}
	}
	for i := 0; i < numContainers; i++ {
		status.ContainerStatuses = append(status.ContainerStatuses, &Status{Name: strconv.Itoa(i)})
	}
	return id, status
}

// TestGetIfNewerThanWhenPodExists 验证 Pod 存在时 getIfNewerThan 的判定：
// 全局时间戳或 Pod 条目修改时间任一更新即视为有新数据。
func TestGetIfNewerThanWhenPodExists(t *testing.T) {
	cache := newTestCache()
	timestamp := time.Now()

	cases := []struct {
		cacheTime time.Time
		modified  time.Time
		observed  time.Time
		expected  bool
	}{
		{
			// 全局时间戳和 Pod 修改时间都晚于查询时间。
			cacheTime: timestamp.Add(time.Second),
			modified:  timestamp,
			expected:  true,
		},
		{
			// 全局时间戳较新、Pod 修改时间较旧：条目虽未变化，但整体缓存已更新，仍视为有新数据。
			cacheTime: timestamp.Add(time.Second),
			modified:  timestamp.Add(-time.Second * 10),
			expected:  true,
		},
		{
			// 全局时间戳较旧、Pod 修改时间较新：该条目本身已更新，返回新数据。
			cacheTime: timestamp.Add(-time.Second),
			modified:  timestamp.Add(time.Second * 3),
			expected:  true,
		},
		{
			// 全局时间戳和 Pod 修改时间都早于查询时间，没有新数据。
			cacheTime: timestamp.Add(-time.Second),
			modified:  timestamp.Add(-time.Second),
			expected:  false,
		},
	}
	for i, c := range cases {
		podID, status := getTestPodIDAndStatus(2)
		cache.UpdateTime(c.cacheTime)
		cache.Set(podID, status, nil, c.modified)
		d := cache.getIfNewerThan(podID, timestamp)
		assert.Equal(t, c.expected, d != nil, "test[%d]", i)
	}
}

// TestGetPodNewerThanWhenPodDoesNotExist 验证 Pod 不存在时只看全局时间戳。
func TestGetPodNewerThanWhenPodDoesNotExist(t *testing.T) {
	cache := newTestCache()
	cacheTime := time.Now()
	cache.UpdateTime(cacheTime)
	podID := types.UID("1234")

	cases := []struct {
		timestamp time.Time
		expected  bool
	}{
		{
			timestamp: cacheTime.Add(-time.Second),
			expected:  true,
		},
		{
			timestamp: cacheTime.Add(time.Second),
			expected:  false,
		},
	}
	for i, c := range cases {
		d := cache.getIfNewerThan(podID, c.timestamp)
		assert.Equal(t, c.expected, d != nil, "test[%d]", i)
	}
}

// TestCacheSetAndGet 验证写入的状态和错误能原样读回。
func TestCacheSetAndGet(t *testing.T) {
	cache := NewCache()
	cases := []struct {
		numContainers int
		error         error
	}{
		{numContainers: 3, error: nil},
		{numContainers: 2, error: fmt.Errorf("unable to get status")},
		{numContainers: 0, error: nil},
	}
	for i, c := range cases {
		podID, status := getTestPodIDAndStatus(c.numContainers)
		cache.Set(podID, status, c.error, time.Time{})
		// 读回缓存中的状态和错误，确认与写入一致。
		actualStatus, actualErr := cache.Get(podID)
		assert.Equal(t, status, actualStatus, "test[%d]", i)
		assert.Equal(t, c.error, actualErr, "test[%d]", i)
	}
}

// TestCacheGetPodDoesNotExist 验证查询不存在的 Pod 时返回仅带 ID 的空状态。
func TestCacheGetPodDoesNotExist(t *testing.T) {
	cache := NewCache()
	podID, status := getTestPodIDAndStatus(0)
	// Pod 不在缓存中时，返回只填充了 ID 的状态对象。
	actualStatus, actualErr := cache.Get(podID)
	assert.Equal(t, status, actualStatus)
	assert.NoError(t, actualErr)
}

// TestDelete 验证删除后查询返回空状态。
func TestDelete(t *testing.T) {
	cache := &cache{pods: map[types.UID]*data{}}
	// 先写入一个 Pod 状态。
	podID, status := getTestPodIDAndStatus(3)
	cache.Set(podID, status, nil, time.Time{})
	actualStatus, actualErr := cache.Get(podID)
	assert.Equal(t, status, actualStatus)
	assert.NoError(t, actualErr)
	// 删除后再次查询应得到空状态。
	cache.Delete(podID)
	expectedStatus := &PodStatus{ID: podID}
	actualStatus, actualErr = cache.Get(podID)
	assert.Equal(t, expectedStatus, actualStatus)
	assert.NoError(t, actualErr)
}

// verifyNotification 断言通知 channel 是否符合预期，并排空已收到的通知。
func verifyNotification(t *testing.T, ch chan *data, expectNotification bool) {
	if expectNotification {
		assert.NotEmpty(t, ch, "Did not receive notification")
	} else {
		assert.Empty(t, ch, "Should not have triggered the notification")
	}
	// 排空 channel，避免影响下一次断言。
	for i := 0; i < len(ch); i++ {
		<-ch
	}
}

// TestRegisterNotification 验证订阅后 Set 与全局时间戳更新都会触发通知。
func TestRegisterNotification(t *testing.T) {
	cache := newTestCache()
	cacheTime := time.Now()
	cache.UpdateTime(cacheTime)

	podID, status := getTestPodIDAndStatus(1)
	ch := cache.subscribe(podID, cacheTime.Add(time.Second))
	verifyNotification(t, ch, false)
	cache.Set(podID, status, nil, cacheTime.Add(time.Second))
	// Set 操作应触发通知。
	verifyNotification(t, ch, true)

	podID, _ = getTestPodIDAndStatus(1)

	ch = cache.subscribe(podID, cacheTime.Add(time.Second))
	verifyNotification(t, ch, false)
	cache.UpdateTime(cacheTime.Add(time.Second * 2))
	// 全局时间戳推进也应触发通知。
	verifyNotification(t, ch, true)
}

// TestGetNewerThan 验证 GetNewerThan 会阻塞到出现比 minTime 更新的数据，
// 覆盖 Set、SetObservedTime、UpdateTime 以及 Pod 不存在等场景。
func TestGetNewerThan(t *testing.T) {
	podID := types.UID("test-pod")
	status := &PodStatus{ID: podID}

	cases := []struct {
		name      string
		podExists bool

		action func(*cache, time.Time)
	}{
		{
			name:      "UnblockBySet",
			podExists: true,
			action:    func(c *cache, timestamp time.Time) { c.Set(podID, status, nil, timestamp) },
		},
		{
			name:      "UnblockBySetObservedTime",
			podExists: true,
			action:    func(c *cache, timestamp time.Time) { c.SetObservedTime(podID, timestamp) },
		},
		{
			name:      "UnblockByUpdateTimeExists",
			podExists: true,
			action:    func(c *cache, timestamp time.Time) { c.UpdateTime(timestamp) },
		},
		{
			name:      "Missing:UnblockBySet",
			podExists: false,
			action:    func(c *cache, timestamp time.Time) { c.Set(podID, status, nil, timestamp) },
		},
		{
			name:      "Missing:UnblockByUpdateTime",
			podExists: false,
			action:    func(c *cache, timestamp time.Time) { c.UpdateTime(timestamp) },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				initial := time.Now()
				minTime := initial.Add(10 * time.Second)

				c := newTestCache()
				c.UpdateTime(initial)

				if tc.podExists {
					c.Set(podID, status, nil, initial)
					c.SetObservedTime(podID, initial)
				}

				// 首次调用 GetNewerThan 应阻塞，直到出现更新。
				resCh := make(chan *PodStatus, 1)
				go func() {
					s, err := c.GetNewerThan(podID, minTime)
					assert.NoError(t, err)
					resCh <- s
				}()

				requireBlocked(t, resCh, "Initial")

				// 动作时间早于 minTime 时仍应阻塞。
				tc.action(c, minTime.Add(-time.Second))
				requireBlocked(t, resCh, "Before minTime")

				// 动作时间晚于 minTime 时应解除阻塞。
				tc.action(c, minTime.Add(time.Second))
				r := requireUnblocked(t, resCh, "After minTime")
				assert.Equal(t, podID, r.ID)

				// 后续调用应立即返回，不再阻塞。
				resCh2 := make(chan *PodStatus, 1)
				go func() {
					s, err := c.GetNewerThan(podID, minTime)
					assert.NoError(t, err)
					resCh2 <- s
				}()
				r = requireUnblocked(t, resCh2, "Already unblocked")
				assert.Equal(t, podID, r.ID)
			})
		})
	}
}

// requireBlocked 断言在 synctest 时间推进前 channel 不会返回结果。
func requireBlocked[T any](t *testing.T, ch <-chan T, msg string) {
	t.Helper()
	synctest.Wait()
	select {
	case r := <-ch:
		t.Fatalf("%s: receive should have blocked, but got: %v", msg, r)
	default:
		// 没有收到数据，符合预期。
	}
}

// requireUnblocked 断言 channel 已返回结果并返回该值。
func requireUnblocked[T any](t *testing.T, ch <-chan T, msg string) (received T) {
	t.Helper()
	synctest.Wait()
	select {
	case r := <-ch:
		return r
	default:
		t.Fatalf("%s: receive should not have been blocked", msg)
		return
	}
}
