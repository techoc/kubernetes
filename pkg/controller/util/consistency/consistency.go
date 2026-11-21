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
	"fmt"
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/resourceversion"
)

// ConsistencyStore 记录各属主写入过的资源版本，并允许在读取侧缓存追上写入版本后再继续处理，
// 避免因 informer 缓存滞后而使用陈旧数据做出错误决策。
type ConsistencyStore interface {
	// WroteAt 记录某个属主对指定资源写入时的资源版本（RV）。
	WroteAt(owner types.NamespacedName, ownerUID types.UID, resource schema.GroupResource, rv string)
	// Clear 在 UID 匹配时清除属主记录；ownerUID 为空表示无条件清除，
	// 用于属主被删除后放弃等待其同步。
	Clear(owner types.NamespacedName, ownerUID types.UID)
	// EnsureReady 检查缓存中观测到的版本是否不旧于所有已记录写入版本；
	// 尚未追上则返回错误，调用方应等待后重试。
	EnsureReady(owner types.NamespacedName) error
}

// ConsistencyError 指出导致一致性检查失败的 GroupResource 及读写双方的资源版本，
// 便于调用方定位具体是哪一类资源的缓存滞后。
type ConsistencyError struct {
	ReadRV        string
	WroteRV       string
	GroupResource schema.GroupResource
}

// Error 返回可直接用于日志的错误描述。
func (c *ConsistencyError) Error() string {
	return fmt.Sprintf("read version: %s is not as new as written version: %s for group resource %s", c.ReadRV, c.WroteRV, c.GroupResource.String())
}

// 编译期断言：RealConsistencyStore 实现 ConsistencyStore 接口。
var _ ConsistencyStore = &RealConsistencyStore{}

// LastSyncRVGetter 由各类 informer store 实现，返回其本地缓存最近一次同步到的资源版本。
type LastSyncRVGetter interface {
	LastStoreSyncResourceVersion() string
}

// RealConsistencyStore 以属主为键保存各资源的最新写入版本，并对照 store 的读取版本判断缓存是否就绪。
type RealConsistencyStore struct {
	// writesLock 保护 writes 映射本身的增删查。
	// 映射中的 ownerRecord 各自用自己的锁保护内部版本数据，因此此处锁的粒度仅到映射结构。
	writesLock sync.RWMutex
	// writes 是属主到其记录的映射。
	writes map[types.NamespacedName]*ownerRecord

	stores map[schema.GroupResource]LastSyncRVGetter
}

// NewConsistencyStore 用给定的 GroupResource -> store 映射构造一致性存储；
// 该映射用于在 EnsureReady 时读取各类资源缓存的最新版本。
func NewConsistencyStore(stores map[schema.GroupResource]LastSyncRVGetter) *RealConsistencyStore {
	return &RealConsistencyStore{
		writes: map[types.NamespacedName]*ownerRecord{},
		stores: stores,
	}
}

// getWrittenRecord returns the record for the given owner, or nil if no record exists.
func (c *RealConsistencyStore) getWrittenRecord(owner types.NamespacedName) *ownerRecord {
	c.writesLock.RLock()
	defer c.writesLock.RUnlock()
	return c.writes[owner]
}

// ensureWrittenRecord 返回属主对应的记录：不存在则新建，UID 变化则整体替换，
// 防止属主被重建（同名但 UID 不同）后沿用旧记录造成错误的一致性判断。
func (c *RealConsistencyStore) ensureWrittenRecord(owner types.NamespacedName, ownerUID types.UID) *ownerRecord {
	// 快路径：读锁下若记录已存在且 UID 一致，直接复用。
	if record := c.getWrittenRecord(owner); record != nil && record.ownerUID == ownerUID {
		return record
	}

	// 慢路径：加写锁后必须再次检查，因为两次检查之间可能有其他 goroutine 创建或替换了记录。
	c.writesLock.Lock()
	defer c.writesLock.Unlock()
	if record := c.writes[owner]; record != nil && record.ownerUID == ownerUID {
		return record
	}
	// 未命中（或 UID 不同）时，按指定 UID 创建全新记录。
	record := newOwnerRecord(ownerUID)
	c.writes[owner] = record
	return record
}

// WroteAt 更新属主对某资源的写入版本，仅当新 RV 更新时才记录。
func (c *RealConsistencyStore) WroteAt(owner types.NamespacedName, ownerUID types.UID, resource schema.GroupResource, rv string) {
	c.ensureWrittenRecord(owner, ownerUID).WroteAt(resource, rv)
}

// Clear 删除属主记录，前提是 UID 匹配（或调用方明确传入空 UID 表示无条件删除）。
func (c *RealConsistencyStore) Clear(owner types.NamespacedName, ownerUID types.UID) {
	// 被删除的属主通常已有记录，因此跳过针对"不存在"的快路径检查，直接加写锁处理。
	c.writesLock.Lock()
	defer c.writesLock.Unlock()
	if record := c.writes[owner]; record != nil && (len(ownerUID) == 0 || record.ownerUID == ownerUID) {
		delete(c.writes, owner)
	}
}

// EnsureReady 在读取缓存版本不旧于所有已记录写入版本时返回 nil，否则返回 ConsistencyError。
// 注意：不能与同一属主的 WroteAt 并发调用——该约束由调用方保证，
// 否则在检查完成后记录被更新，可能误判为"已就绪"。
func (c *RealConsistencyStore) EnsureReady(owner types.NamespacedName) error {
	record := c.getWrittenRecord(owner)
	if record == nil {
		return nil
	}
	// 检查通过后清除记录：该属主的本次写入已被缓存追上，无需再等待。
	err := record.EnsureReady(c)
	if err == nil {
		c.Clear(owner, record.ownerUID)
		return nil
	}
	return err
}

// ownerRecord 保存单个属主对各资源写入的最新 RV。
// ownerUID 创建后不可变；versions 由自身锁保护，可与映射级别的锁独立并发。
type ownerRecord struct {
	// ownerUID 一经创建不可修改，因为它是判断记录是否仍属于当前属主的唯一依据。
	ownerUID types.UID

	versionsLock sync.Mutex
	versions     map[schema.GroupResource]string
}

// newOwnerRecord 创建一个空的属主记录。
func newOwnerRecord(ownerUID types.UID) *ownerRecord {
	return &ownerRecord{ownerUID: ownerUID, versions: map[schema.GroupResource]string{}}
}

// WroteAt 仅当传入 RV 是该资源目前见过的最大值时才更新记录，
// 避免乱序到达的旧写入把版本回退。
func (w *ownerRecord) WroteAt(resource schema.GroupResource, rv string) {
	w.versionsLock.Lock()
	defer w.versionsLock.Unlock()
	if _, ok := w.versions[resource]; !ok {
		w.versions[resource] = rv
		return
	}
	// RV 是逻辑时钟，不能按字符串大小比较，必须使用专门的 CompareResourceVersion。
	cmp, err := resourceversion.CompareResourceVersion(w.versions[resource], rv)
	if err == nil && cmp >= 0 {
		return
	}
	// 新 RV 更大（或无法比较的错误被忽略）时覆盖旧值。
	w.versions[resource] = rv
}

// EnsureReady 逐资源比较写入版本与 store 的读取版本，
// 任一资源读取版本落后就返回 ConsistencyError。
func (w *ownerRecord) EnsureReady(c *RealConsistencyStore) error {
	w.versionsLock.Lock()
	defer w.versionsLock.Unlock()
	for gr, wroteRV := range w.versions {
		store, exists := c.stores[gr]
		// 没有为对应 GroupResource 注册 store 时无从判断，跳过而非阻塞同步。
		if !exists || store == nil {
			continue
		}
		readRV := store.LastStoreSyncResourceVersion()
		// 读取版本为空说明 LastStoreSyncResourceVersion 特性未启用，同样跳过检查。
		if readRV == "" {
			continue
		}
		i, err := resourceversion.CompareResourceVersion(wroteRV, readRV)
		// RV 比较失败说明数据本身有问题，不能据此判定"就绪"；
		// 为避免永久阻塞同步，这里选择跳过该项而非返回错误。
		if err != nil {
			continue
		}
		// 写入版本大于读取版本，说明缓存尚未追上，本次检查失败。
		if i > 0 {
			// read version is not as new as owner version, not ready
			return &ConsistencyError{
				WroteRV:       wroteRV,
				ReadRV:        readRV,
				GroupResource: gr,
			}
		}
	}
	return nil
}

// NoopConsistencyStore 不记录任何状态，始终认为缓存已就绪，
// 用于一致性检查特性门未启用的场景。
type NoopConsistencyStore struct{}

// 编译期断言：NoopConsistencyStore 实现 ConsistencyStore 接口。
var _ ConsistencyStore = &NoopConsistencyStore{}

// WroteAt 是空实现：不记录任何写入版本。
func (*NoopConsistencyStore) WroteAt(owner types.NamespacedName, ownerUID types.UID, resource schema.GroupResource, rv string) {
}

// ReadAt 是空实现，仅用于满足相关调用约定。
func (*NoopConsistencyStore) ReadAt(resource schema.GroupResource, rv string) {}

// Clear 是空实现：没有状态需要清理。
func (*NoopConsistencyStore) Clear(owner types.NamespacedName, ownerUID types.UID) {}

// EnsureReady 始终返回 nil，表示缓存永远就绪。
func (*NoopConsistencyStore) EnsureReady(owner types.NamespacedName) error {
	return nil
}

// NewNoopConsistencyStore 返回一个空实现的一致性存储。
func NewNoopConsistencyStore() *NoopConsistencyStore {
	return &NoopConsistencyStore{}
}
