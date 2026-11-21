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

package consistency

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
)

// TestOwnerRecord_WroteAt 测试 ownerRecord.WroteAt 只保留每个资源上观察到的最高版本号。
// 乱序到达的旧版本不能覆盖较新的版本，否则会误判写操作尚未同步。
func TestOwnerRecord_WroteAt(t *testing.T) {
	uid := types.UID("owner-uid-1")
	or := newOwnerRecord(uid)
	assert.Equal(t, uid, or.ownerUID)
	require.NotNil(t, or.versions)

	grPod := schema.GroupResource{Group: "", Resource: "pods"}
	grDs := schema.GroupResource{Group: "apps", Resource: "daemonsets"}

	// 首次写入：建立该资源的版本记录。
	or.WroteAt(grPod, "5")
	assert.Equal(t, "5", or.versions[grPod])

	// 第二次写入：版本号更高，应当更新记录。
	or.WroteAt(grPod, "10")
	assert.Equal(t, "10", or.versions[grPod])

	// 第三次写入：版本号更低，属于乱序到达，应当被忽略。
	or.WroteAt(grPod, "8")
	assert.Equal(t, "10", or.versions[grPod])

	// 写入另一个资源：不同资源的版本互相独立，不能彼此覆盖。
	or.WroteAt(grDs, "1")
	assert.Equal(t, "1", or.versions[grDs])
	assert.Equal(t, "10", or.versions[grPod], "Pod version should be unchanged")
}

// TestOwnerRecord_IsReady 测试 EnsureReady 依据读版本号是否追平写版本号来判断就绪状态。
// 覆盖无写、无读、读落后、读相等、读超前以及多资源部分就绪等场景。
func TestOwnerRecord_IsReady(t *testing.T) {
	uid := types.UID("owner-uid-1")
	or := newOwnerRecord(uid)
	grPod := schema.GroupResource{Group: "", Resource: "pods"}
	grDs := schema.GroupResource{Group: "apps", Resource: "daemonsets"}
	podStore := cache.NewStore(cache.MetaNamespaceKeyFunc)
	dsStore := cache.NewStore(cache.MetaNamespaceKeyFunc)
	resourceStores := map[schema.GroupResource]LastSyncRVGetter{
		grPod: podStore,
		grDs:  dsStore,
	}

	store := NewConsistencyStore(resourceStores)

	// 场景 1：没有任何写操作，自然处于就绪状态。
	require.NoError(t, or.EnsureReady(store), "Should be ready if no writes are recorded")

	// 记录一次写操作。
	or.WroteAt(grPod, "10")

	// 场景 2：有写但没有读，读无法确认写是否已同步，因此仍保持就绪。
	require.NoError(t, or.EnsureReady(store), "Should stay ready if write exists and no read")

	// 添加一个落后于写版本的读。
	podStore.Bookmark("5")

	// 场景 3：读版本落后于写版本，说明写尚未被观察到，未就绪。
	require.Error(t, or.EnsureReady(store), "Not ready if read < write")

	// 添加一个与写版本相等的读。
	podStore.Bookmark("10")

	// 场景 4：读版本等于写版本，写已被观察到，就绪。
	require.NoError(t, or.EnsureReady(store), "Ready if read == write")

	// 添加一个高于写版本的读。
	podStore.Bookmark("15")

	// 场景 5：读版本高于写版本，同样说明写已被观察到，就绪。
	require.NoError(t, or.EnsureReady(store), "Ready if read > write")

	// 再写入第二个资源，并给出一个落后的读。
	or.WroteAt(grDs, "100")
	dsStore.Bookmark("50")

	// 场景 6：多个资源中只有一个就绪，整体仍未就绪。
	require.Error(t, or.EnsureReady(store), "Not ready if one of multiple writes is not ready (no read)")

	// 让第二个资源的读追平写。
	dsStore.Bookmark("100")

	// 场景 7：所有资源都就绪，整体才就绪。
	require.NoError(t, or.EnsureReady(store), "Ready if all writes are ready")
}

// TestConsistencyStore_New 测试 NewConsistencyStore 会初始化写入记录表，即使传入的资源存储为空。
func TestConsistencyStore_New(t *testing.T) {
	store := NewConsistencyStore(nil)
	require.NotNil(t, store)
	require.NotNil(t, store.writes)
	assert.Empty(t, store.writes)
}

// TestConsistencyStore_EnsureWrittenRecord 测试 ensureWrittenRecord 的创建、复用与替换语义。
// 当 UID 变化时旧记录必须被整体替换，避免旧版本的写污染新记录。
func TestConsistencyStore_EnsureWrittenRecord(t *testing.T) {
	store := NewConsistencyStore(nil)
	owner := types.NamespacedName{Name: "owner1"}
	uid1 := types.UID("uid-1")
	uid2 := types.UID("uid-2")

	// 首次调用：创建记录并存入 map。
	r1 := store.ensureWrittenRecord(owner, uid1)
	require.NotNil(t, r1)
	assert.Equal(t, uid1, r1.ownerUID)
	assert.Same(t, r1, store.writes[owner])

	// UID 相同：复用已有记录，便于持续累加版本信息。
	r2 := store.ensureWrittenRecord(owner, uid1)
	assert.Same(t, r1, r2, "Should return existing record for same UID")

	// UID 不同：说明对象被重建，必须换成全新记录。
	r3 := store.ensureWrittenRecord(owner, uid2)
	require.NotNil(t, r3)
	assert.NotSame(t, r1, r3, "Should be a new record")
	assert.Equal(t, uid2, r3.ownerUID)
	assert.Same(t, r3, store.writes[owner], "New record should replace old one in map")
	assert.Empty(t, r3.versions, "New record should be empty")

	// 验证旧记录已脱离 map，后续写入不会传播到新记录。
	grPod := schema.GroupResource{Group: "", Resource: "pods"}
	// 只写旧记录
	r1.WroteAt(grPod, "10")
	assert.Empty(t, r3.versions, "Write to old record should not affect new record")
}

// TestConsistencyStore_EnsureWrittenRecord_Concurrent 测试 ensureWrittenRecord 在并发调用下的正确性。
// 通过大量 goroutine 竞争，验证同一 UID 只产生一条记录，且 UID 切换后也只有一条新记录。
func TestConsistencyStore_EnsureWrittenRecord_Concurrent(t *testing.T) {
	store := NewConsistencyStore(nil)
	owner := types.NamespacedName{Name: "owner1"}
	uid1 := types.UID("uid-1")
	uid2 := types.UID("uid-2")

	wg := sync.WaitGroup{}
	numGoroutines := 50

	// 相同 UID 并发创建：所有 goroutine 必须拿到同一条记录。
	var firstRecord *ownerRecord
	var once sync.Once
	for range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := store.ensureWrittenRecord(owner, uid1)
			assert.Equal(t, uid1, r.ownerUID)
			once.Do(func() {
				firstRecord = r
			})
			assert.Same(t, firstRecord, r)
		}()
	}
	wg.Wait()
	require.NotNil(t, firstRecord)
	assert.Len(t, store.writes, 1)

	// 新 UID 并发替换：所有 goroutine 必须拿到同一条替换记录。
	var replacementRecord *ownerRecord
	var replaceOnce sync.Once
	for range numGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := store.ensureWrittenRecord(owner, uid2)
			assert.Equal(t, uid2, r.ownerUID)
			replaceOnce.Do(func() {
				replacementRecord = r
			})
			assert.Same(t, replacementRecord, r)
		}()
	}
	wg.Wait()
	require.NotNil(t, replacementRecord)
	assert.Len(t, store.writes, 1)
	assert.Same(t, replacementRecord, store.writes[owner])
	assert.NotSame(t, firstRecord, replacementRecord)
}

// TestConsistencyStore_WroteAt 测试 WroteAt 会为 owner 建立记录并更新对应资源的版本。
func TestConsistencyStore_WroteAt(t *testing.T) {
	store := NewConsistencyStore(nil)
	owner := types.NamespacedName{Name: "owner1"}
	uid1 := types.UID("uid-1")
	grPod := schema.GroupResource{Group: "", Resource: "pods"}

	store.WroteAt(owner, uid1, grPod, "10")

	record := store.getWrittenRecord(owner)
	require.NotNil(t, record)
	assert.Equal(t, uid1, record.ownerUID)

	assert.Equal(t, "10", record.versions[grPod])

	// 再次写入：同一记录中的版本应当被更新。
	store.WroteAt(owner, uid1, grPod, "20")
	assert.Equal(t, "20", record.versions[grPod])
}

// TestConsistencyStore_Clear 测试 Clear 只在 UID 匹配时删除记录，空 UID 视为强制清理。
func TestConsistencyStore_Clear(t *testing.T) {
	store := NewConsistencyStore(nil)
	owner1 := types.NamespacedName{Name: "owner1"}
	owner2 := types.NamespacedName{Name: "owner2"}
	uid1 := types.UID("uid-1")
	uid2 := types.UID("uid-2")

	// 准备两条不同 owner 的记录。
	r1 := store.ensureWrittenRecord(owner1, uid1)
	r2 := store.ensureWrittenRecord(owner2, uid2)
	require.Len(t, store.writes, 2)

	// 清理不存在的 owner：不应影响任何记录。
	store.Clear(types.NamespacedName{Name: "non-existent"}, uid1)
	assert.Len(t, store.writes, 2)

	// UID 不匹配：说明是旧 owner 的清理请求，不能误删新记录。
	store.Clear(owner1, uid2)
	assert.Len(t, store.writes, 2, "Should not clear with wrong UID")
	assert.Same(t, r1, store.writes[owner1])

	// UID 匹配：正常清理，且不影响其他 owner。
	store.Clear(owner1, uid1)
	assert.Len(t, store.writes, 1, "Should clear with correct UID")
	assert.Nil(t, store.writes[owner1])
	assert.Same(t, r2, store.writes[owner2], "Other record should remain")

	// 重新添加 owner1 的记录，供空 UID 场景使用。
	store.ensureWrittenRecord(owner1, uid1)
	require.Len(t, store.writes, 2)

	// 空 UID：表示无法确认版本，保守起见直接清理记录。
	store.Clear(owner1, "")
	assert.Len(t, store.writes, 1, "Should clear with empty UID")
	assert.Nil(t, store.writes[owner1])
	assert.Same(t, r2, store.writes[owner2])
}

// TestConsistencyStore_IsReady 测试 ConsistencyStore.EnsureReady 的就绪判断及就绪后记录的清理。
// 一旦读版本追平写版本，写记录即可安全删除，无需继续跟踪该 owner。
func TestConsistencyStore_IsReady(t *testing.T) {
	owner1 := types.NamespacedName{Name: "owner1"}
	uid1 := types.UID("uid-1")
	grPod := schema.GroupResource{Group: "", Resource: "pods"}
	podStore := cache.NewStore(cache.MetaNamespaceKeyFunc)
	resourceStores := map[schema.GroupResource]LastSyncRVGetter{
		grPod: podStore,
	}

	store := NewConsistencyStore(resourceStores)

	// 场景 1：没有记录，无需等待，直接就绪。
	require.NoError(t, store.EnsureReady(owner1), "Ready if no record exists")

	// 添加一个初始读版本，并写入一个更高的版本。
	podStore.Bookmark("5")
	store.WroteAt(owner1, uid1, grPod, "10")

	// 场景 2：读版本落后于写版本，未就绪。
	require.Error(t, store.EnsureReady(owner1), "Not ready if read < write")

	// 读版本追平写版本。
	podStore.Bookmark("10")

	// 场景 3：读版本等于写版本，就绪。
	require.NoError(t, store.EnsureReady(owner1), "Ready if read == write")

	// 读版本超过写版本。
	podStore.Bookmark("15")

	// 场景 4：读版本高于写版本，仍然就绪。
	require.NoError(t, store.EnsureReady(owner1), "Ready if read > write")

	// 读版本已经高于最新写版本，后续不再需要跟踪该记录，验证其已被删除。
	// 只要读版本始终领先，就无需继续保留写记录。
	assert.Nil(t, store.getWrittenRecord(owner1), "Written record should no longer exist")
}
