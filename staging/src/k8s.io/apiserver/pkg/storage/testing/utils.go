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

package testing

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/api/meta"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/apis/example"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/value"
)

// CreateObjList 把对象数组逐个写入存储，并把写入后的对象（含 ResourceVersion）
// 写回 items，方便调用方继续断言。
func CreateObjList(prefix string, helper storage.Interface, items []runtime.Object) error {
	for i := range items {
		obj := items[i]
		meta, err := meta.Accessor(obj)
		if err != nil {
			return err
		}
		err = helper.Create(context.Background(), path.Join(prefix, meta.GetName()), obj, obj, 0)
		if err != nil {
			return err
		}
		items[i] = obj
	}
	return nil
}

// CreateList 从 list 中取出元素写入存储，再把更新后的元素放回 list。
func CreateList(prefix string, helper storage.Interface, list runtime.Object) error {
	items, err := meta.ExtractList(list)
	if err != nil {
		return err
	}
	err = CreateObjList(prefix, helper, items)
	if err != nil {
		return err
	}
	return meta.SetList(list, items)
}

// DeepEqualSafePodSpec 返回一个适合做 deep-equal 比较的 example.PodSpec。
// 只包含可稳定比较的字段，避免默认值或指针差异干扰断言。
func DeepEqualSafePodSpec() example.PodSpec {
	grace := int64(30)
	return example.PodSpec{
		RestartPolicy:                 "Always",
		TerminationGracePeriodSeconds: &grace,
		SchedulerName:                 "default-scheduler",
	}
}

// computePodKey 计算 example.Pod 在测试存储中的 key。
func computePodKey(obj *example.Pod) string {
	return fmt.Sprintf("/pods/%s/%s", obj.Namespace, obj.Name)
}

// testPropagateStore 把对象写入存储，自动生成 key，并返回 key 与写入后的对象。
// 写入前先删除同 key 的旧数据，确保测试从干净状态开始。
func testPropagateStore(ctx context.Context, t *testing.T, store storage.Interface, obj *example.Pod) (string, *example.Pod) {
	// 用 key 初始化存储并取得写入结果用于返回。
	key := computePodKey(obj)

	// 清理可能残留的旧对象。
	err := store.Delete(ctx, key, &example.Pod{}, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{})
	if err != nil && !storage.IsNotFound(err) {
		t.Fatalf("Cleanup failed: %v", err)
	}
	setOutput := &example.Pod{}
	if err := store.Create(ctx, key, obj, setOutput, 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	return key, setOutput
}

// expectNoDiff 断言期望值与实际值相等；失败时优先输出 cmp.Diff 便于定位。
func expectNoDiff(t *testing.T, msg string, expected, actual interface{}) {
	t.Helper()
	if !reflect.DeepEqual(expected, actual) {
		if diff := cmp.Diff(expected, actual); diff != "" {
			t.Errorf("%s: %s", msg, diff)
		} else {
			t.Errorf("%s:\nexpected: %#v\ngot: %#v", msg, expected, actual)
		}
	}
}

// ExpectContains 断言 got 与 expectedList 中某一项相等。
func ExpectContains(t *testing.T, msg string, expectedList []interface{}, got interface{}) {
	t.Helper()
	for _, expected := range expectedList {
		if reflect.DeepEqual(expected, got) {
			return
		}
	}
	if len(expectedList) == 0 {
		t.Errorf("%s: empty expectedList", msg)
		return
	}
	if diff := cmp.Diff(expectedList[0], got); diff != "" {
		t.Errorf("%s: differs from all items, with first: %s", msg, diff)
	} else {
		t.Errorf("%s: differs from all items, first: %#v\ngot: %#v", msg, expectedList[0], got)
	}
}

// dummyPrefix 是构造 continue token 时使用的前缀，模拟真实存储的 key 前缀。
const dummyPrefix = "adapter"

// encodeContinueOrDie 构造 continue token，失败直接 panic（仅用于测试）。
func encodeContinueOrDie(key string, resourceVersion int64) string {
	token, err := storage.EncodeContinue(dummyPrefix+key, dummyPrefix, resourceVersion)
	if err != nil {
		panic(err)
	}
	return token
}

// testCheckEventType 等待下一个事件并断言其类型。
func testCheckEventType(t *testing.T, w watch.Interface, expectEventType watch.EventType) {
	select {
	case res := <-w.ResultChan():
		if res.Type != expectEventType {
			t.Errorf("event type want=%v, get=%v", expectEventType, res.Type)
		}
	case <-time.After(wait.ForeverTestTimeout):
		t.Errorf("time out after waiting %v on ResultChan", wait.ForeverTestTimeout)
	}
}

// testCheckResult 等待下一个事件并断言其内容。
func testCheckResult(t *testing.T, w watch.Interface, expectEvent watch.Event) {
	testCheckResultFunc(t, w, func(actualEvent watch.Event) {
		expectNoDiff(t, "incorrect event", expectEvent, actualEvent)
	})
}

// testCheckResultFunc 取出下一个事件，解开 cacher 的包装层后交给 check 断言。
func testCheckResultFunc(t *testing.T, w watch.Interface, check func(actualEvent watch.Event)) {
	select {
	case res := <-w.ResultChan():
		obj := res.Object
		// cacher 返回的可能是 CacheableObject，需要取出真正的对象再比较。
		if co, ok := obj.(runtime.CacheableObject); ok {
			res.Object = co.GetObject()
		}
		// 记录时间的事件在比较前需要解包，否则时间字段会干扰断言。
		if withRecordTime, ok := res.Object.(storage.WatchEventWithRecordTime); ok {
			res.Object = withRecordTime.Unwrap()
		}
		check(res)
	case <-time.After(wait.ForeverTestTimeout):
		t.Errorf("time out after waiting %v on ResultChan", wait.ForeverTestTimeout)
	}
}

// testCheckResultWithIgnoreFunc 依次接收事件，跳过 ignore 判定为可忽略的事件，
// 并与 expectedEvents 逐项比较。
func testCheckResultWithIgnoreFunc(t *testing.T, w watch.Interface, expectedEvents []watch.Event, ignore func(watch.Event) bool) {
	checkIndex := 0
	for {
		select {
		case event := <-w.ResultChan():
			obj := event.Object
			if co, ok := obj.(runtime.CacheableObject); ok {
				event.Object = co.GetObject()
			}
			if withRecordTime, ok := event.Object.(storage.WatchEventWithRecordTime); ok {
				event.Object = withRecordTime.Unwrap()
			}
			if ignore != nil && ignore(event) {
				continue
			}
			if checkIndex < len(expectedEvents) {
				expectNoDiff(t, "incorrect event", expectedEvents[checkIndex], event)
				checkIndex++
			} else {
				t.Fatalf("cannot receive correct event, expect no event, but get a event: %+v", event)
			}
		case <-time.After(150 * time.Millisecond):
			// 强制等待 150ms 以接收包括 bookmark 在内的 watch 事件。
			// 无法保证 150ms 内收到所有 bookmark 事件，
			// 但超时值过大会导致包级测试超时。
			if checkIndex < len(expectedEvents) {
				t.Fatalf("cannot receive enough events within specific time, rest expected events: %+v", expectedEvents[checkIndex:])
			}
			return
		}
	}
}

// testCheckStop 断言 watch 通道已关闭。
func testCheckStop(t *testing.T, w watch.Interface) {
	select {
	case e, ok := <-w.ResultChan():
		if ok {
			var obj string
			switch e.Object.(type) {
			case *example.Pod:
				obj = e.Object.(*example.Pod).Name
			case *v1.Status:
				obj = e.Object.(*v1.Status).Message
			}
			t.Errorf("ResultChan should have been closed. Event: %s. Object: %s", e.Type, obj)
		}
	case <-time.After(wait.ForeverTestTimeout):
		t.Errorf("time out after waiting 1s on ResultChan")
	}
}

// TestCheckResultsInStrictOrder 按顺序逐个断言事件。
func TestCheckResultsInStrictOrder(t *testing.T, w watch.Interface, expectedEvents []watch.Event) {
	for _, expectedEvent := range expectedEvents {
		testCheckResult(t, w, expectedEvent)
	}
}

// TestCheckNoMoreResultsWithIgnoreFunc 断言在 ignore 之外没有更多事件。
func TestCheckNoMoreResultsWithIgnoreFunc(t *testing.T, w watch.Interface, ignore func(watch.Event) bool) {
	select {
	case e := <-w.ResultChan():
		if ignore == nil || !ignore(e) {
			t.Errorf("Unexpected: %#v event received, expected no events", e)
		}
	// 这里刻意使用较短的超时以加快测试。
	case <-time.After(100 * time.Millisecond):
		return
	}
}

// toInterfaceSlice 把泛型切片转换为 []interface{}，便于与测试期望值比较。
func toInterfaceSlice[T any](s []T) []interface{} {
	result := make([]interface{}, len(s))
	for i, v := range s {
		result[i] = v
	}
	return result
}

// resourceVersionNotOlderThan 返回一个校验 resourceVersion 的函数：
// 早于 sentinel 的逻辑时间报错，等于或新于 sentinel 的不报错。
func resourceVersionNotOlderThan(sentinel string) func(string) error {
	return func(resourceVersion string) error {
		objectVersioner := storage.APIObjectVersioner{}
		actualRV, err := objectVersioner.ParseResourceVersion(resourceVersion)
		if err != nil {
			return err
		}
		expectedRV, err := objectVersioner.ParseResourceVersion(sentinel)
		if err != nil {
			return err
		}
		if actualRV < expectedRV {
			return fmt.Errorf("expected a resourceVersion no smaller than than %d, but got %d", expectedRV, actualRV)
		}
		return nil
	}
}

// StorageInjectingListErrors 为前 N 次 GetList 调用注入错误，
// 用于验证调用方在瞬时失败后的重试行为。
type StorageInjectingListErrors struct {
	storage.Interface

	lock   sync.Mutex
	Errors int
}

// GetList 在前 Errors 次调用中返回注入错误，之后透传到底层存储。
func (s *StorageInjectingListErrors) GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error {
	err := func() error {
		s.lock.Lock()
		defer s.lock.Unlock()
		if s.Errors > 0 {
			s.Errors--
			return fmt.Errorf("injected error")
		}
		return nil
	}()
	if err != nil {
		return err
	}
	return s.Interface.GetList(ctx, key, opts, listObj)
}

// ErrorsConsumed 报告注入的错误是否已全部消耗。
func (s *StorageInjectingListErrors) ErrorsConsumed() (bool, error) {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.Errors == 0, nil
}

// PrefixTransformer 在数据进出存储时加上并校验固定前缀，
// 用于测试加密/编码 transformer 链的前缀处理逻辑。
type PrefixTransformer struct {
	prefix []byte
	stale  bool
	err    error
	reads  uint64
}

// NewPrefixTransformer 创建带指定前缀的 transformer；stale 表示读取时报告数据已过期。
func NewPrefixTransformer(prefix []byte, stale bool) *PrefixTransformer {
	return &PrefixTransformer{
		prefix: prefix,
		stale:  stale,
	}
}

// TransformFromStorage 校验前缀并去掉前缀；reads 用原子操作计数，供并发测试断言。
func (p *PrefixTransformer) TransformFromStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, bool, error) {
	atomic.AddUint64(&p.reads, 1)
	if dataCtx == nil {
		panic("no context provided")
	}
	if !bytes.HasPrefix(data, p.prefix) {
		return nil, false, fmt.Errorf("value does not have expected prefix %q: %s,", p.prefix, string(data))
	}
	return bytes.TrimPrefix(data, p.prefix), p.stale, p.err
}

// TransformToStorage 给非空数据加上前缀；空数据保持为空。
func (p *PrefixTransformer) TransformToStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, error) {
	if dataCtx == nil {
		panic("no context provided")
	}
	if len(data) > 0 {
		return append(append([]byte{}, p.prefix...), data...), p.err
	}
	return data, p.err
}

// GetReadsAndReset 原子地读取并清零读取计数。
func (p *PrefixTransformer) GetReadsAndReset() uint64 {
	return atomic.SwapUint64(&p.reads, 0)
}

// reproducingTransformer 是仅用于测试一致性的 transformer。
// 它在 TransformFromStorage 时创建预定义对象，从而能在读操作过程中
// 精确地注入新对象，用于复现缓存与存储之间的一致性场景。
type reproducingTransformer struct {
	wrapped value.Transformer
	store   storage.Interface

	index      uint32
	nextObject func(uint32) (string, *example.Pod)
}

// TransformFromStorage 先注入下一个对象，再委托给被包装的 transformer。
func (rt *reproducingTransformer) TransformFromStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, bool, error) {
	if err := rt.createObject(ctx); err != nil {
		return nil, false, err
	}
	return rt.wrapped.TransformFromStorage(ctx, data, dataCtx)
}

// TransformToStorage 直接透传。
func (rt *reproducingTransformer) TransformToStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, error) {
	return rt.wrapped.TransformToStorage(ctx, data, dataCtx)
}

// createObject 按递增序号生成并写入下一个对象。
func (rt *reproducingTransformer) createObject(ctx context.Context) error {
	key, obj := rt.nextObject(atomic.AddUint32(&rt.index, 1))
	out := &example.Pod{}
	return rt.store.Create(ctx, key, obj, out, 0)
}

// failingTransformer 是仅用于测试的 transformer：从存储读取时总是失败。
type failingTransformer struct {
}

// TransformFromStorage 始终返回错误。
func (ft *failingTransformer) TransformFromStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, bool, error) {
	return nil, false, fmt.Errorf("failed transformation")
}

// TransformToStorage 原样返回数据。
func (ft *failingTransformer) TransformToStorage(ctx context.Context, data []byte, dataCtx value.Context) ([]byte, error) {
	return data, nil
}

// sortablePodList 为 example.Pod 切片提供按 key 排序的能力，
// 使不保证顺序的存储返回结果也能稳定比较。
type sortablePodList []example.Pod

// Len 实现 sort.Interface。
func (s sortablePodList) Len() int {
	return len(s)
}

// Less 按 computePodKey 的字典序比较。
func (s sortablePodList) Less(i, j int) bool {
	return computePodKey(&s[i]) < computePodKey(&s[j])
}

// Swap 实现 sort.Interface。
func (s sortablePodList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// CreatePodPredicate 构造针对 example.Pod 的选择谓词。
func CreatePodPredicate(field fields.Selector, namespaceScoped bool, indexField []string) storage.SelectionPredicate {
	return storage.SelectionPredicate{
		Label:       labels.Everything(),
		Field:       field,
		GetAttrs:    determinePodGetAttrFunc(namespaceScoped, indexField),
		IndexFields: indexField,
	}
}

// determinePodGetAttrFunc 根据作用域与是否使用索引字段选择属性提取函数。
func determinePodGetAttrFunc(namespaceScoped bool, indexField []string) storage.AttrFunc {
	if indexField != nil {
		if namespaceScoped {
			return namespacedScopedNodeNameAttrFunc
		}
		return clusterScopedNodeNameAttrFunc
	}
	if namespaceScoped {
		return storage.DefaultNamespaceScopedAttr
	}
	return storage.DefaultClusterScopedAttr
}

// namespacedScopedNodeNameAttrFunc 提取命名空间级 Pod 的索引字段。
func namespacedScopedNodeNameAttrFunc(obj runtime.Object) (labels.Set, fields.Set, error) {
	pod := obj.(*example.Pod)
	return nil, fields.Set{
		"spec.nodeName":      pod.Spec.NodeName,
		"metadata.name":      pod.ObjectMeta.Name,
		"metadata.namespace": pod.ObjectMeta.Namespace,
	}, nil
}

// clusterScopedNodeNameAttrFunc 提取集群级 Pod 的索引字段。
func clusterScopedNodeNameAttrFunc(obj runtime.Object) (labels.Set, fields.Set, error) {
	pod := obj.(*example.Pod)
	return nil, fields.Set{
		"spec.nodeName": pod.Spec.NodeName,
		"metadata.name": pod.ObjectMeta.Name,
	}, nil
}

// mustAtoi 把字符串转为 int，失败直接 panic；仅用于测试中写死的数字。
func mustAtoi(str string) int {
	result, err := strconv.Atoi(str)
	if err != nil {
		panic(err)
	}
	return result
}
