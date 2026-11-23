/*
Copyright The Kubernetes Authors.

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

package resourceslice

import (
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
)

// TestFilterSliceWatchByPoolName_FiltersByPoolName 验证 filterSliceWatchByPoolName 只转发目标 pool 的 ResourceSlice 事件。
func TestFilterSliceWatchByPoolName_FiltersByPoolName(t *testing.T) {
	// 使用 synctest 在虚拟时间下运行，避免时序抖动。
	synctest.Test(t, testFilterSliceWatchByPoolNameFiltersByPoolName)
}

// testFilterSliceWatchByPoolNameFiltersByPoolName 是上述测试的实体：注入混合事件并断言过滤结果。
func testFilterSliceWatchByPoolNameFiltersByPoolName(t *testing.T) {
	// 目标 pool 为 pool-a。
	poolName := "pool-a"

	// 构造带缓冲的 fake watch 作为事件源。
	source := watch.NewFakeWithOptions(watch.FakeOptions{ChannelSize: 5})
	defer source.Stop()

	// 用目标 pool 名创建包装 watcher。
	wrapped := filterSliceWatchByPoolName(t.Context(), source, poolName)
	defer wrapped.Stop()

	// 输入：两个目标 pool 事件、一个非目标 pool 事件，随后是书签与错误事件。
	source.Add(resourceSliceForPool("pool-a", "slice-a"))
	source.Add(resourceSliceForPool("pool-b", "slice-b"))
	source.Add(resourceSliceForPool("pool-a", "slice-a2"))
	source.Action(watch.Bookmark, &metav1.PartialObjectMetadata{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}})
	// 错误事件应被放行。
	source.Error(&metav1.Status{Status: metav1.StatusFailure})

	// 断言：只收到 pool-a 的两个 Added 事件，且名称与 pool 字段正确。
	event1 := <-wrapped.ResultChan()
	require.Equal(t, watch.Added, event1.Type)
	require.Equal(t, "slice-a", event1.Object.(*resourceapi.ResourceSlice).Name)
	require.Equal(t, poolName, event1.Object.(*resourceapi.ResourceSlice).Spec.Pool.Name)

	event2 := <-wrapped.ResultChan()
	require.Equal(t, watch.Added, event2.Type)
	require.Equal(t, "slice-a2", event2.Object.(*resourceapi.ResourceSlice).Name)

	// 断言：书签与错误事件被放行。
	bookmark := <-wrapped.ResultChan()
	require.Equal(t, watch.Bookmark, bookmark.Type)

	errEvent := <-wrapped.ResultChan()
	require.Equal(t, watch.Error, errEvent.Type)
}

// TestWrapWatcher_NilMatchPassesAll 验证 match 为 nil 时所有事件都被转发。
func TestWrapWatcher_NilMatchPassesAll(t *testing.T) {
	// 使用 synctest 运行。
	synctest.Test(t, testWrapWatcherNilMatchPassesAll)
}

// testWrapWatcherNilMatchPassesAll 是上述测试的实体。
func testWrapWatcherNilMatchPassesAll(t *testing.T) {
	// 构造容量为 2 的 fake watch。
	source := watch.NewFakeWithOptions(watch.FakeOptions{ChannelSize: 2})
	defer source.Stop()

	// match 传 nil，期望全部事件通过。
	wrapped := newWrapWatcher(t.Context(), source, nil)
	defer wrapped.Stop()

	// 输入：分别属于 pool-a 与 pool-b 的两个事件。
	source.Add(resourceSliceForPool("pool-a", "slice-a"))
	source.Add(resourceSliceForPool("pool-b", "slice-b"))

	// 断言：两个事件按顺序全部收到。
	event1 := <-wrapped.ResultChan()
	require.Equal(t, "slice-a", event1.Object.(*resourceapi.ResourceSlice).Name)

	event2 := <-wrapped.ResultChan()
	require.Equal(t, "slice-b", event2.Object.(*resourceapi.ResourceSlice).Name)
}

// TestWrapWatcher_StopClosesResultChan 验证 Stop 会关闭结果通道。
func TestWrapWatcher_StopClosesResultChan(t *testing.T) {
	// 使用 synctest 运行。
	synctest.Test(t, testWrapWatcherStopClosesResultChan)
}

// testWrapWatcherStopClosesResultChan 是上述测试的实体。
func testWrapWatcherStopClosesResultChan(t *testing.T) {
	// 构造容量为 1 的 fake watch。
	source := watch.NewFakeWithOptions(watch.FakeOptions{ChannelSize: 1})
	wrapped := newWrapWatcher(t.Context(), source, nil)
	defer wrapped.Stop()

	// 输入：先发送一个事件并确认能收到，随后调用 Stop。
	source.Add(resourceSliceForPool("pool-a", "slice-a"))

	// 断言：通道打开时 ok 为 true。
	_, ok := <-wrapped.ResultChan()
	require.True(t, ok)

	// 调用 Stop 停止 watcher。
	wrapped.Stop()

	// 断言：Stop 后通道关闭，ok 为 false。
	_, ok = <-wrapped.ResultChan()
	require.False(t, ok)
}

// resourceSliceForPool 构造一个仅设置名称与 pool 名的测试用 ResourceSlice。
func resourceSliceForPool(poolName, name string) *resourceapi.ResourceSlice {
	return &resourceapi.ResourceSlice{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: resourceapi.ResourceSliceSpec{
			Driver: "test-driver",
			Pool:   resourceapi.ResourcePool{Name: poolName},
		},
	}
}
