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

package etcd3

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

// TestStatsCache 验证资源大小估算缓存对 UpdateKey/Update/DeleteKey 的处理，
// 以及基于 getKeys 返回的键集合重新计算平均值的行为。
//
// 关键不变量：估算值始终是"当前所有键对应值大小之和 / 键数量"，且快照
// （Update）只应把 revision 更旧的条目回放进来，不能覆盖更新的写入。
func TestStatsCache(t *testing.T) {
	ctx := t.Context()
	store := newResourceSizeEstimator("/prefix", func(ctx context.Context) ([]string, error) { return []string{}, nil })
	defer store.Close()

	// 初始没有键时平均值为 0。
	stats, err := store.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), stats.EstimatedAverageObjectSizeBytes)

	// 单个 10 字节对象：平均值 10。
	store.UpdateKey(&mvccpb.KeyValue{Key: []byte("/prefix/foo1"), Value: []byte("0123456789"), ModRevision: 2})
	store.getKeys = func(ctx context.Context) ([]string, error) { return []string{"/prefix/foo1"}, nil }
	stats, err = store.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(10), stats.EstimatedAverageObjectSizeBytes)

	// 加入 20 字节对象：平均 (10+20)/2 = 15。
	store.UpdateKey(&mvccpb.KeyValue{Key: []byte("/prefix/foo2"), Value: []byte("01234567890123456789"), ModRevision: 3})
	store.getKeys = func(ctx context.Context) ([]string, error) { return []string{"/prefix/foo1", "/prefix/foo2"}, nil }
	stats, err = store.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(15), stats.EstimatedAverageObjectSizeBytes)

	// foo1 被更新为 30 字节：平均 (30+20)/2 = 25。
	store.UpdateKey(&mvccpb.KeyValue{Key: []byte("/prefix/foo1"), Value: []byte("012345678901234567890123456789"), ModRevision: 4})
	stats, err = store.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(25), stats.EstimatedAverageObjectSizeBytes)

	// foo2 缩小为 10 字节：平均 (30+10)/2 = 20。
	store.UpdateKey(&mvccpb.KeyValue{Key: []byte("/prefix/foo2"), Value: []byte("0123456789"), ModRevision: 5})
	stats, err = store.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(20), stats.EstimatedAverageObjectSizeBytes)

	// 删除 foo1 后只剩 10 字节对象。
	store.DeleteKey(&mvccpb.KeyValue{Key: []byte("/prefix/foo1"), ModRevision: 6})
	store.getKeys = func(ctx context.Context) ([]string, error) { return []string{"/prefix/foo2"}, nil }
	stats, err = store.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(10), stats.EstimatedAverageObjectSizeBytes)

	// Snapshot revision from revision 3
	// 用 revision 3 的快照覆盖：旧条目不得覆盖已删除/更新的状态。
	store.Update([]*mvccpb.KeyValue{
		{Key: []byte("/prefix/foo1"), Value: []byte("0123456789"), ModRevision: 2},
		{Key: []byte("/prefix/foo2"), Value: []byte("01234567890123456789"), ModRevision: 3},
	})
	stats, err = store.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(10), stats.EstimatedAverageObjectSizeBytes)

	// Replay from revision 2
	// 从 revision 2 开始重放增量事件，平均值随事件推进而更新。
	store.UpdateKey(&mvccpb.KeyValue{Key: []byte("/prefix/foo1"), Value: []byte("0123456789"), ModRevision: 2})
	stats, err = store.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(10), stats.EstimatedAverageObjectSizeBytes)

	store.UpdateKey(&mvccpb.KeyValue{Key: []byte("/prefix/foo2"), Value: []byte("01234567890123456789"), ModRevision: 3})
	stats, err = store.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(10), stats.EstimatedAverageObjectSizeBytes)

	store.UpdateKey(&mvccpb.KeyValue{Key: []byte("/prefix/foo1"), Value: []byte("012345678901234567890123456789"), ModRevision: 4})
	stats, err = store.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(10), stats.EstimatedAverageObjectSizeBytes)

	store.UpdateKey(&mvccpb.KeyValue{Key: []byte("/prefix/foo2"), Value: []byte("0123456789"), ModRevision: 5})
	stats, err = store.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(10), stats.EstimatedAverageObjectSizeBytes)

	store.DeleteKey(&mvccpb.KeyValue{Key: []byte("/prefix/foo1"), ModRevision: 6})
	stats, err = store.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(10), stats.EstimatedAverageObjectSizeBytes)

	// 再次删除同一键（幂等）后键集合为空，平均值回到 0。
	store.DeleteKey(&mvccpb.KeyValue{Key: []byte("/prefix/foo1"), ModRevision: 7})
	store.getKeys = func(ctx context.Context) ([]string, error) { return []string{}, nil }
	stats, err = store.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), stats.EstimatedAverageObjectSizeBytes)

	// Old snapshot might restore old revision if keys were recreated
	// 键被重建后，旧快照可能把已删除的旧 revision 重新带入，这里确认其影响。
	store.getKeys = func(ctx context.Context) ([]string, error) { return []string{"foo1", "foo2"}, nil }
	store.Update([]*mvccpb.KeyValue{
		{Key: []byte("/prefix/foo1"), Value: []byte("0123456789"), ModRevision: 2},
		{Key: []byte("/prefix/foo2"), Value: []byte("01234567890123456789"), ModRevision: 3},
	})
	stats, err = store.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(15), stats.EstimatedAverageObjectSizeBytes)

	// Cleanup if keys were deleted
	// 键集合为空时，即使缓存中仍有条目，平均值也必须为 0。
	store.getKeys = func(ctx context.Context) ([]string, error) { return []string{}, nil }
	stats, err = store.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), stats.EstimatedAverageObjectSizeBytes)
}
