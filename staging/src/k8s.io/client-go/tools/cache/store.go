/*
Copyright 2014 The Kubernetes Authors.

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
	"errors"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Store 是通用的对象存储与处理接口。Store 维护从字符串 key 到累积对象的
// map，并提供把给定对象添加/更新/删除到给定 key 当前关联累积对象的操作。
// Store 还知道如何从给定对象提取 key，因此很多操作只需传入对象。
//
// 在最简单的 Store 实现中，每个累积对象就是最后传入的对象，Delete 之后为空，
// 因此 Store 的行为就是简单存储。
//
// Reflector 知道如何 watch 服务端并更新 Store。本包提供了 Store 的多种实现。
type Store interface {

	// Add 把给定对象加入其 key 关联的累积对象
	Add(obj interface{}) error

	// Update 用给定对象更新其 key 关联的累积对象
	Update(obj interface{}) error

	// Delete 从给定对象 key 关联的累积对象中删除该对象
	Delete(obj interface{}) error

	// List 返回当前所有非空累积对象的列表
	List() []interface{}

	// ListKeys 返回当前所有关联非空累积对象的 key
	ListKeys() []string

	// LastStoreSyncResourceVersion 返回 store 见过的最新 resource version，
	// 即写入 store 的对象中被观察到的最大 resource version。
	LastStoreSyncResourceVersion() string

	// Bookmark 记录传入的新的 resource version，
	// 用于获取 store 的最新 resource version。
	//
	// bookmark 事件不携带对象，仅推进 resourceVersion，用于无变更时维持 watch 进度。
	Bookmark(rv string)

	// Get 返回给定对象 key 关联的累积对象
	Get(obj interface{}) (item interface{}, exists bool, err error)

	// GetByKey 返回给定 key 关联的累积对象
	GetByKey(key string) (item interface{}, exists bool, err error)

	// Replace 会删除 store 的内容，改用给定的列表。Store 接管该列表的所有权，
	// 调用此函数后不应再引用它。
	Replace([]interface{}, string) error

	// Resync 在本接口的语境下没有意义，但在一些有额外行为的实现中
	// （如 DeltaFIFO）是有含义的。
	Resync() error
}

// TransactionType 定义事务操作的类型，用于表示对象正在被添加、更新还是删除。
type TransactionType string

const (
	TransactionTypeAdd    TransactionType = "Add"
	TransactionTypeUpdate TransactionType = "Update"
	TransactionTypeDelete TransactionType = "Delete"
)

// Transaction 表示流程中的单个操作或事件，携带与该事务关联的通用对象
// Object，以及表示事务类型的 Type。
type Transaction struct {
	Object interface{}
	Type   TransactionType
}

// TransactionStore 是一次性提交多步操作的可选能力。
//
// 依赖它的调用方可在高竞争场景下把多次修改合并到一次加锁中，
// 避免分批写入时被其他写者穿插而无法推进。
type TransactionStore interface {
	// Transaction 允许在一次加锁内完成多个操作，确保高竞争下仍能推进。
	Transaction(txns ...Transaction) *TransactionError
}

var _ error = &TransactionError{}

// TransactionError 汇总批量事务中失败的操作，同时给出成功操作的原始下标。
type TransactionError struct {
	SuccessfulIndices []int
	TotalTransactions int
	Errors            []error
}

func (t *TransactionError) Error() string {
	return fmt.Sprintf("failed to execute (%d/%d) transactions failed due to: %v",
		t.TotalTransactions-len(t.SuccessfulIndices), t.TotalTransactions, t.Errors)
}

// KeyFunc 知道如何从对象生成 key，实现应当是确定性的。
type KeyFunc func(obj interface{}) (string, error)

// KeyError 在 KeyFunc 出错时返回，其中包含出问题的对象。
type KeyError struct {
	Obj interface{}
	Err error
}

// Error 返回人类可读的错误描述。
func (k KeyError) Error() string {
	return fmt.Sprintf("couldn't create key for object %+v: %v", k.Obj, k.Err)
}

// Unwrap 实现 errors.Unwrap。
func (k KeyError) Unwrap() error {
	return k.Err
}

// ExplicitKey：如果手头只有对象的 key 而没有对象本身，可以把它传给
// MetaNamespaceKeyFunc。
type ExplicitKey string

// MetaNamespaceKeyFunc 是默认便捷 KeyFunc，能为实现 meta.Interface 的
// API 对象生成 key。格式为 <namespace>/<name>；<namespace> 为空时只用 <name>。
//
// 需要结构化结果的调用方可以使用 ObjectToName 或 MetaObjectToName。
// 注意：那不会是想给 Store 取 key 的调用方，因为 Store 的 key 必须是字符串。
//
// TODO 也许哪天？：给 Store 换一种 key 方案
func MetaNamespaceKeyFunc(obj interface{}) (string, error) {
	// ExplicitKey 允许调用方在只有 key、没有对象时仍走统一入口。
	if key, ok := obj.(ExplicitKey); ok {
		return string(key), nil
	}
	objName, err := ObjectToName(obj)
	if err != nil {
		return "", err
	}
	return objName.String(), nil
}

// ObjectToName 返回给定对象的结构化名称，前提是该对象可以被视作 metav1.Object。
func ObjectToName(obj interface{}) (ObjectName, error) {
	meta, err := meta.Accessor(obj)
	if err != nil {
		return ObjectName{}, fmt.Errorf("object has no meta: %v", err)
	}
	return MetaObjectToName(meta), nil
}

// MetaObjectToName 返回给定对象的结构化名称。
func MetaObjectToName(obj metav1.Object) ObjectName {
	// 集群级对象没有命名空间，ObjectName.Namespace 留空以与命名空间对象区分。
	if len(obj.GetNamespace()) > 0 {
		return ObjectName{Namespace: obj.GetNamespace(), Name: obj.GetName()}
	}
	return ObjectName{Namespace: "", Name: obj.GetName()}
}

// SplitMetaNamespaceKey 解析出 MetaNamespaceKeyFunc 编码进 key 的
// namespace 和 name。
//
// TODO: 用 key 结构体取代字符串 key，从而免去这种打包/解包。
func SplitMetaNamespaceKey(key string) (namespace, name string, err error) {
	parts := strings.Split(key, "/")
	switch len(parts) {
	case 1:
		// 只有 name，没有 namespace
		return "", parts[0], nil
	case 2:
		// namespace 和 name
		return parts[0], parts[1], nil
	}

	// 多于两段说明 key 被拼接错误，返回明确错误而非猜测。
	return "", "", fmt.Errorf("unexpected key format: %q", key)
}

// `*cache` 基于 ThreadSafeStore 和关联的 KeyFunc 实现 Indexer。
type cache struct {
	// cacheStorage 承担缓存的线程安全责任
	cacheStorage ThreadSafeStore
	// keyFunc 为存入和取出的对象生成 key，必须是确定性的。
	keyFunc KeyFunc
	// 每个放入缓存的对象都会调用 transformer。
	transformer TransformFunc
	// identifier 用于在指标中标识该 store。
	identifier InformerNameAndResource
	// metrics 是该 store 的指标提供者。
	metrics InformerMetricsProvider
}

var _ Store = &cache{}

// Transaction 把一批操作交给底层 ThreadSafeStore 原子执行。
//
// key 计算失败的条目会被跳过并记录，成功计算的条目即使整体返回错误也已生效，
// 调用方需按 SuccessfulIndices 判断实际结果。
func (c *cache) Transaction(txns ...Transaction) *TransactionError {
	txnStore, ok := c.cacheStorage.(ThreadSafeStoreWithTransaction)
	if !ok {
		return &TransactionError{
			TotalTransactions: len(txns),
			Errors: []error{
				errors.New("transaction not supported"),
			},
		}
	}
	keyedTxns := make([]ThreadSafeStoreTransaction, 0, len(txns))
	successfulIndices := make([]int, 0, len(txns))
	errs := make([]error, 0)
	for i := range txns {
		txn := txns[i]
		key, err := c.keyFunc(txn.Object)
		if err != nil {
			errs = append(errs, KeyError{txn.Object, err})
			continue
		}
		successfulIndices = append(successfulIndices, i)
		keyedTxns = append(keyedTxns, ThreadSafeStoreTransaction{txn, key})
	}
	txnStore.Transaction(keyedTxns...)
	if len(errs) > 0 {
		return &TransactionError{
			SuccessfulIndices: successfulIndices,
			TotalTransactions: len(txns),
			Errors:            errs,
		}
	}
	return nil
}

// Add 把条目插入缓存。
func (c *cache) Add(obj interface{}) error {
	key, err := c.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	// transformer 在写入前改写对象（如裁剪字段），失败则拒绝写入以保持缓存一致。
	if c.transformer != nil {
		obj, err = c.transformer(obj)
		if err != nil {
			return fmt.Errorf("transforming: %w", err)
		}
	}
	c.cacheStorage.Add(key, obj)
	return nil
}

// Update 把缓存中的条目更新为最新状态。
func (c *cache) Update(obj interface{}) error {
	key, err := c.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	if c.transformer != nil {
		obj, err = c.transformer(obj)
		if err != nil {
			return fmt.Errorf("transforming: %w", err)
		}
	}
	c.cacheStorage.Update(key, obj)
	return nil
}

// Delete 从缓存中移除条目。
func (c *cache) Delete(obj interface{}) error {
	key, err := c.keyFunc(obj)
	if err != nil {
		return KeyError{obj, err}
	}
	// 传入完整对象供底层实现（如指标）识别被删除的是哪一个。
	c.cacheStorage.DeleteWithObject(key, obj)
	return nil
}

// List 返回全部条目的列表。只要把所有条目视为不可变对象，List 就完全线程安全。
func (c *cache) List() []interface{} {
	return c.cacheStorage.List()
}

// ListKeys 返回当前缓存中所有对象的 key。
func (c *cache) ListKeys() []string {
	return c.cacheStorage.ListKeys()
}

func (c *cache) LastStoreSyncResourceVersion() string {
	return c.cacheStorage.LastStoreSyncResourceVersion()
}

func (c *cache) Bookmark(rv string) {
	c.cacheStorage.Bookmark(rv)
}

// GetIndexers 返回缓存的索引器表。
func (c *cache) GetIndexers() Indexers {
	return c.cacheStorage.GetIndexers()
}

// Index 返回匹配索引函数的条目列表。只要把所有条目视为不可变对象，Index 就是线程安全的。
func (c *cache) Index(indexName string, obj interface{}) ([]interface{}, error) {
	return c.cacheStorage.Index(indexName, obj)
}

// IndexKeys 返回在指定索引下包含给定索引值的已存储对象的存储键，
// 返回的键可直接传给 GetByKey()。
func (c *cache) IndexKeys(indexName, indexedValue string) ([]string, error) {
	return c.cacheStorage.IndexKeys(indexName, indexedValue)
}

// ListIndexFuncValues 返回某个索引函数生成的全部索引值。
func (c *cache) ListIndexFuncValues(indexName string) []string {
	return c.cacheStorage.ListIndexFuncValues(indexName)
}

// ByIndex 返回在指定索引下包含给定索引值的已存储对象。
func (c *cache) ByIndex(indexName, indexedValue string) ([]interface{}, error) {
	return c.cacheStorage.ByIndex(indexName, indexedValue)
}

func (c *cache) AddIndexers(newIndexers Indexers) error {
	return c.cacheStorage.AddIndexers(newIndexers)
}

// Get 返回请求的条目；不存在时 exists=false。只要把所有条目视为不可变对象，
// Get 就完全线程安全。
func (c *cache) Get(obj interface{}) (item interface{}, exists bool, err error) {
	key, err := c.keyFunc(obj)
	if err != nil {
		return nil, false, KeyError{obj, err}
	}
	return c.GetByKey(key)
}

// GetByKey 返回请求的条目；不存在时 exists=false。只要把所有条目视为不可变对象，
// GetByKey 就完全线程安全。
func (c *cache) GetByKey(key string) (item interface{}, exists bool, err error) {
	item, exists = c.cacheStorage.Get(key)
	return item, exists, nil
}

// Replace 会删除 'c' 的内容，改用给定的列表。'c' 接管该列表的所有权，
// 调用此函数后不应再引用它。
func (c *cache) Replace(list []interface{}, resourceVersion string) error {
	items := make(map[string]interface{}, len(list))
	for _, item := range list {
		key, err := c.keyFunc(item)
		if err != nil {
			return KeyError{item, err}
		}

		if c.transformer != nil {
			item, err = c.transformer(item)
			if err != nil {
				return fmt.Errorf("transforming: %w", err)
			}
		}
		items[key] = item
	}
	// 先构造完整映射再一次性替换，避免中途错误留下部分更新的缓存。
	c.cacheStorage.Replace(items, resourceVersion)
	return nil
}

// Resync 对该类型没有意义
func (c *cache) Resync() error {
	return nil
}

// StoreOption 用于在构造 Store/Indexer 时注入可选行为。
type StoreOption = func(*cache)

// WithTransformer 设置写入前的对象转换函数。
func WithTransformer(transformer TransformFunc) StoreOption {
	return func(c *cache) {
		c.transformer = transformer
	}
}

// WithStoreMetrics 绑定指标标识；metrics 为 nil 时使用全局默认提供者。
func WithStoreMetrics(identifier InformerNameAndResource, metrics InformerMetricsProvider) StoreOption {
	return func(c *cache) {
		c.identifier = identifier
		if metrics == nil {
			metrics = globalInformerMetricsProvider
		}
		c.metrics = metrics
	}
}

// NewStore 返回一个仅用 map 和锁简单实现的 Store。
func NewStore(keyFunc KeyFunc, opts ...StoreOption) Store {
	c := &cache{
		keyFunc: keyFunc,
	}
	for _, opt := range opts {
		opt(c)
	}
	threadSafeOpts := []ThreadSafeStoreOption{}
	if c.metrics != nil {
		threadSafeOpts = append(threadSafeOpts, WithThreadSafeStoreMetrics(c.identifier, c.metrics))
	}
	c.cacheStorage = NewThreadSafeStore(Indexers{}, Indices{}, threadSafeOpts...)
	return c
}

// NewIndexer 返回一个仅用 map 和锁简单实现的 Indexer。
func NewIndexer(keyFunc KeyFunc, indexers Indexers, opts ...StoreOption) Indexer {
	c := &cache{
		keyFunc: keyFunc,
	}
	for _, opt := range opts {
		opt(c)
	}
	threadSafeOpts := []ThreadSafeStoreOption{}
	if c.metrics != nil {
		threadSafeOpts = append(threadSafeOpts, WithThreadSafeStoreMetrics(c.identifier, c.metrics))
	}
	c.cacheStorage = NewThreadSafeStore(indexers, Indices{}, threadSafeOpts...)
	return c
}
