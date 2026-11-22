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

package testing

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/apis/example"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/value"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/utils/ptr"
)

// KeyValidation 用于在写入成功后由测试套件回调，校验 key 在底层存储中的实际状态。
// 不同后端（etcd3、watchcache）的检查方式不同，因此由调用方注入。
type KeyValidation func(ctx context.Context, t *testing.T, key string)

// RunTestCreate 校验 Create 的基本语义：新建对象返回非空 resourceVersion，
// 且禁止客户端在创建时预设 resourceVersion（否则并发写会绕过版本控制）。
func RunTestCreate(ctx context.Context, t *testing.T, store storage.Interface, validation KeyValidation) {
	tests := []struct {
		name          string
		inputObj      *example.Pod
		expectedError error
	}{{
		name:     "successful create",
		inputObj: &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}},
	}, {
		name:          "create with ResourceVersion set",
		inputObj:      &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "bar", Namespace: "test-ns", ResourceVersion: "1"}},
		expectedError: storage.ErrResourceVersionSetOnCreate,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := &example.Pod{} // reset
			// verify that kv pair is empty before set
			// 先确认 key 为空，避免上一次运行的残留数据掩盖 Create 的问题
			key := computePodKey(tt.inputObj)
			if err := store.Get(ctx, key, storage.GetOptions{}, out); !storage.IsNotFound(err) {
				t.Fatalf("expecting empty result on key %s, got %v", key, err)
			}

			err := store.Create(ctx, key, tt.inputObj, out, 0)
			if !errors.Is(err, tt.expectedError) {
				t.Errorf("expecting error %v, but get: %v", tt.expectedError, err)
			}
			if err != nil {
				return
			}
			// basic tests of the output
			// 输出对象必须带回写入的名字，并且被分配了新的 resourceVersion
			if tt.inputObj.ObjectMeta.Name != out.ObjectMeta.Name {
				t.Errorf("pod name want=%s, get=%s", tt.inputObj.ObjectMeta.Name, out.ObjectMeta.Name)
			}
			if out.ResourceVersion == "" {
				t.Errorf("output should have non-empty resource version")
			}
			validation(ctx, t, key)
		})
	}
}

// RunTestCreateWithTTL 校验带 TTL 的创建：租约到期后对象应被清除，watcher 收到 Deleted 事件。
func RunTestCreateWithTTL(ctx context.Context, t *testing.T, store storage.Interface) {
	input := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}}
	out := &example.Pod{}

	key := computePodKey(input)
	if err := store.Create(ctx, key, input, out, 1); err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	w, err := store.Watch(ctx, key, storage.ListOptions{ResourceVersion: out.ResourceVersion, Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	testCheckEventType(t, w, watch.Deleted)
}

// RunTestCreateWithKeyExist 校验对已存在的 key 再次 Create 会返回 AlreadyExists，
// 而不是静默覆盖——覆盖会绕过 resourceVersion 的乐观并发控制。
func RunTestCreateWithKeyExist(ctx context.Context, t *testing.T, store storage.Interface) {
	obj := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}}
	key, _ := testPropagateStore(ctx, t, store, obj)
	out := &example.Pod{}

	err := store.Create(ctx, key, obj, out, 0)
	if err == nil || !storage.IsExist(err) {
		t.Errorf("expecting key exists error, but get: %s", err)
	}
}

// RunTestGet 覆盖 Get 的 resourceVersion 语义：不指定 RV 时读取最新值，
// 指定 RV 时按 NotOlderThan 处理（可能返回更新的版本），RV=0 表示不保证一致性。
func RunTestGet(ctx context.Context, t *testing.T, store storage.Interface) {
	// create an object to test
	// 创建一个对象，随后更新一次，以便测试按精确 resourceVersion 读取
	key, createdObj := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}})
	// update the object once to allow get by exact resource version to be tested
	// 更新一次，使 createdObj 的 resourceVersion 落后于最新值
	updateObj := createdObj.DeepCopy()
	updateObj.Annotations = map[string]string{"test-annotation": "1"}
	storedObj := &example.Pod{}
	err := store.GuaranteedUpdate(ctx, key, storedObj, true, nil,
		func(_ runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			ttl := uint64(1)
			return updateObj, &ttl, nil
		}, nil)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	// create an additional object to increment the resource version for pods above the resource version of the foo object
	// 再创建一个对象，把全局 RV 推高到 foo 的 RV 之上，用于验证"请求 RV 落后于最新"的场景
	secondObj := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "bar", Namespace: "test-ns"}}
	lastUpdatedObj := &example.Pod{}
	if err := store.Create(ctx, computePodKey(secondObj), secondObj, lastUpdatedObj, 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	currentRV, _ := strconv.Atoi(storedObj.ResourceVersion)
	lastUpdatedCurrentRV, _ := strconv.Atoi(lastUpdatedObj.ResourceVersion)

	// TODO(jpbetz): Add exact test cases
	// 注意：这里的用例故意只断言"可接受的输出集合"而非精确值，
	// 因为 Get 的 NotOlderThan 语义允许返回比请求更新的版本。
	tests := []struct {
		name                 string
		key                  string
		ignoreNotFound       bool
		expectNotFoundErr    bool
		expectRVTooLarge     bool
		expectedOut          *example.Pod
		expectedAlternatives []*example.Pod
		rv                   string
	}{{
		name:              "get existing",
		key:               key,
		ignoreNotFound:    false,
		expectNotFoundErr: false,
		expectedOut:       storedObj,
	}, {
		// For RV=0 arbitrarily old version is allowed, including from the moment
		// when the object didn't yet exist.
		// As a result, we allow it by setting ignoreNotFound and allowing an empty
		// object in expectedOut.
		// RV=0 表示"任意旧版本"，因此允许对象尚不存在时的空结果。
		name:                 "resource version 0",
		key:                  key,
		ignoreNotFound:       true,
		expectedAlternatives: []*example.Pod{{}, createdObj, storedObj},
		rv:                   "0",
	}, {
		// Given that Get with set ResourceVersion is effectively always
		// NotOlderThan semantic, both versions of object are allowed.
		// 带 RV 的 Get 实际是 NotOlderThan 语义，所以旧版本和新版本都算合法结果。
		name:                 "object created resource version",
		key:                  key,
		expectedAlternatives: []*example.Pod{createdObj, storedObj},
		rv:                   createdObj.ResourceVersion,
	}, {
		name:        "current object resource version, match=NotOlderThan",
		key:         key,
		expectedOut: storedObj,
		rv:          fmt.Sprintf("%d", currentRV),
	}, {
		name:        "latest resource version",
		key:         key,
		expectedOut: storedObj,
		rv:          fmt.Sprintf("%d", lastUpdatedCurrentRV),
	}, {
		name:             "too high resource version",
		key:              key,
		expectRVTooLarge: true,
		rv:               strconv.FormatInt(math.MaxInt64, 10),
	}, {
		name:              "get non-existing",
		key:               "/pods/non-existing",
		ignoreNotFound:    false,
		expectNotFoundErr: true,
	}, {
		name:              "get non-existing, ignore not found",
		key:               "/pods/non-existing",
		ignoreNotFound:    true,
		expectNotFoundErr: false,
		expectedOut:       &example.Pod{},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// For some asynchronous implementations of storage interface (in particular watchcache),
			// certain requests may impact result of further requests. As an example, if we first
			// ensure that watchcache is synchronized up to ResourceVersion X (using Get/List requests
			// with NotOlderThan semantic), the further requests (even specifying earlier resource
			// version) will also return the result synchronized to at least ResourceVersion X.
			// By parallelizing test cases we ensure that the order in which test cases are defined
			// doesn't automatically preclude some scenarios from happening.
			// 并行化可让用例之间不因定义顺序而互相"预热"，避免掩盖异步实现的时序问题。
			t.Parallel()

			out := &example.Pod{}
			err := store.Get(ctx, tt.key, storage.GetOptions{IgnoreNotFound: tt.ignoreNotFound, ResourceVersion: tt.rv}, out)
			if tt.expectNotFoundErr {
				if err == nil || !storage.IsNotFound(err) {
					t.Errorf("expecting not found error, but get: %v", err)
				}
				return
			}
			if tt.expectRVTooLarge {
				if err == nil || !storage.IsTooLargeResourceVersion(err) {
					t.Errorf("expecting resource version too high error, but get: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if tt.expectedAlternatives == nil {
				expectNoDiff(t, fmt.Sprintf("%s: incorrect pod", tt.name), tt.expectedOut, out)
			} else {
				ExpectContains(t, fmt.Sprintf("%s: incorrect pod", tt.name), toInterfaceSlice(tt.expectedAlternatives), out)
			}
		})
	}
}

// RunTestUnconditionalDelete 校验不带前置条件的 Delete：返回被删对象的最终状态，
// 且其 resourceVersion 必须比删除前更新（删除本身也消耗一次版本号）。
func RunTestUnconditionalDelete(ctx context.Context, t *testing.T, store storage.Interface) {
	key, storedObj := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}})

	tests := []struct {
		name              string
		key               string
		expectedObj       *example.Pod
		expectNotFoundErr bool
	}{{
		name:              "existing key",
		key:               key,
		expectedObj:       storedObj,
		expectNotFoundErr: false,
	}, {
		name:              "non-existing key",
		key:               "/pods/non-existing",
		expectedObj:       nil,
		expectNotFoundErr: true,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := &example.Pod{} // reset
			err := store.Delete(ctx, tt.key, out, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{})
			if tt.expectNotFoundErr {
				if err == nil || !storage.IsNotFound(err) {
					t.Errorf("expecting not found error, but get: %s", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Delete failed: %v", err)
			}
			// We expect the resource version of the returned object to be
			// updated compared to the last existing object.
			if storedObj.ResourceVersion == out.ResourceVersion {
				t.Errorf("expecting resource version to be updated, but get: %s", out.ResourceVersion)
			}
			out.ResourceVersion = storedObj.ResourceVersion
			expectNoDiff(t, "incorrect pod:", tt.expectedObj, out)
		})
	}
}

// RunTestConditionalDelete 校验 UID 前置条件：UID 不匹配时必须返回 Invalid 错误，
// 防止删除掉同名但已被重建的对象。
func RunTestConditionalDelete(ctx context.Context, t *testing.T, store storage.Interface) {
	obj := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns", UID: "A"}}
	key, storedObj := testPropagateStore(ctx, t, store, obj)

	tests := []struct {
		name                string
		precondition        *storage.Preconditions
		expectInvalidObjErr bool
	}{{
		name:                "UID match",
		precondition:        storage.NewUIDPreconditions("A"),
		expectInvalidObjErr: false,
	}, {
		name:                "UID mismatch",
		precondition:        storage.NewUIDPreconditions("B"),
		expectInvalidObjErr: true,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := &example.Pod{}
			err := store.Delete(ctx, key, out, tt.precondition, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{})
			if tt.expectInvalidObjErr {
				if err == nil || !storage.IsInvalidObj(err) {
					t.Errorf("expecting invalid UID error, but get: %s", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Delete failed: %v", err)
			}
			// We expect the resource version of the returned object to be
			// updated compared to the last existing object.
			if storedObj.ResourceVersion == out.ResourceVersion {
				t.Errorf("expecting resource version to be updated, but get: %s", out.ResourceVersion)
			}
			out.ResourceVersion = storedObj.ResourceVersion
			expectNoDiff(t, "incorrect pod:", storedObj, out)
			obj := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns", UID: "A"}}
			key, storedObj = testPropagateStore(ctx, t, store, obj)
		})
	}
}

// The following set of Delete tests are testing the logic of adding `suggestion`
// as a parameter with probably value of the current state.
// Introducing it for GuaranteedUpdate cause a number of issues, so we're addressing
// all of those upfront by adding appropriate tests:
// - https://github.com/kubernetes/kubernetes/pull/35415
//   [DONE] Lack of tests originally - added TestDeleteWithSuggestion.
// - https://github.com/kubernetes/kubernetes/pull/40664
//   [DONE] Irrelevant for delete, as Delete doesn't write data (nor compare it).
// - https://github.com/kubernetes/kubernetes/pull/47703
//   [DONE] Irrelevant for delete, because Delete doesn't persist data.
// - https://github.com/kubernetes/kubernetes/pull/48394/
//   [DONE] Irrelevant for delete, because Delete doesn't compare data.
// - https://github.com/kubernetes/kubernetes/pull/43152
//   [DONE] Added TestDeleteWithSuggestionAndConflict
// - https://github.com/kubernetes/kubernetes/pull/54780
//   [DONE] Irrelevant for delete, because Delete doesn't compare data.
// - https://github.com/kubernetes/kubernetes/pull/58375
//   [DONE] Irrelevant for delete, because Delete doesn't compare data.
// - https://github.com/kubernetes/kubernetes/pull/77619
//   [DONE] Added TestValidateDeletionWithSuggestion for corresponding delete checks.
// - https://github.com/kubernetes/kubernetes/pull/78713
//   [DONE] Bug was in getState function which is shared with the new code.
// - https://github.com/kubernetes/kubernetes/pull/78713
//   [DONE] Added TestPreconditionalDeleteWithSuggestion

// RunTestDeleteWithSuggestion 校验 Delete 接受 suggestion 参数但不因此改变行为。
func RunTestDeleteWithSuggestion(ctx context.Context, t *testing.T, store storage.Interface) {
	key, originalPod := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "name", Namespace: "test-ns"}})

	out := &example.Pod{}
	if err := store.Delete(ctx, key, out, nil, storage.ValidateAllObjectFunc, originalPod, storage.DeleteOptions{}); err != nil {
		t.Errorf("Unexpected failure during deletion: %v", err)
	}

	if err := store.Get(ctx, key, storage.GetOptions{}, &example.Pod{}); !storage.IsNotFound(err) {
		t.Errorf("Unexpected error on reading object: %v", err)
	}
}

// RunTestDeleteWithSuggestionAndConflict 校验 suggestion 已过期（期间发生过更新）时，
// Delete 仍应基于最新版本完成删除，而不是报冲突。
func RunTestDeleteWithSuggestionAndConflict(ctx context.Context, t *testing.T, store storage.Interface) {
	key, originalPod := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "name", Namespace: "test-ns"}})

	// First update, so originalPod is outdated.
	// 先做一次更新，使 originalPod 相对存储中的当前版本已经过期。
	updatedPod := &example.Pod{}
	if err := store.GuaranteedUpdate(ctx, key, updatedPod, false, nil,
		storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
			pod := obj.(*example.Pod)
			pod.ObjectMeta.Labels = map[string]string{"foo": "bar"}
			return pod, nil
		}), nil); err != nil {
		t.Errorf("Unexpected failure during updated: %v", err)
	}

	out := &example.Pod{}
	if err := store.Delete(ctx, key, out, nil, storage.ValidateAllObjectFunc, originalPod, storage.DeleteOptions{}); err != nil {
		t.Errorf("Unexpected failure during deletion: %v", err)
	}

	if err := store.Get(ctx, key, storage.GetOptions{}, &example.Pod{}); !storage.IsNotFound(err) {
		t.Errorf("Unexpected error on reading object: %v", err)
	}
	updatedPod.ObjectMeta.ResourceVersion = out.ObjectMeta.ResourceVersion
	expectNoDiff(t, "incorrect pod:", updatedPod, out)
}

// RunTestDeleteWithConflict tests the case when another conflicting update happened before the delete completed.
// 删除的 validate 阶段发生并发更新时，实现应重新读取并重试，最终删除成功且返回更新后的对象。
func RunTestDeleteWithConflict(ctx context.Context, t *testing.T, store storage.Interface) {
	key, _ := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "name", Namespace: "test-ns"}})

	// First update, so originalPod is outdated.
	// 先更新一次，让 originalPod 过期；随后在 Delete 的 validate 回调中再注入一次并发更新。
	updatedPod := &example.Pod{}
	validateCount := 0
	updateCount := 0
	// Simulate a conflicting update in the middle of delete.
	// 模拟删除执行期间发生的并发更新：validate 会被重试，第二次起不再注入更新。
	validateAllWithUpdate := func(_ context.Context, _ runtime.Object) error {
		validateCount++
		if validateCount > 1 {
			return nil
		}
		if err := store.GuaranteedUpdate(ctx, key, updatedPod, false, nil,
			storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
				pod := obj.(*example.Pod)
				pod.ObjectMeta.Labels = map[string]string{"foo": "bar"}
				return pod, nil
			}), nil); err != nil {
			t.Errorf("Unexpected failure during updated: %v", err)
		}
		updateCount++
		return nil
	}

	out := &example.Pod{}
	if err := store.Delete(ctx, key, out, nil, validateAllWithUpdate, nil, storage.DeleteOptions{}); err != nil {
		t.Errorf("Unexpected failure during deletion: %v", err)
	}

	if validateCount != 2 {
		t.Errorf("Expect validateCount = %d, but got %d", 2, validateCount)
	}
	if updateCount != 1 {
		t.Errorf("Expect updateCount = %d, but got %d", 1, updateCount)
	}

	if err := store.Get(ctx, key, storage.GetOptions{}, &example.Pod{}); !storage.IsNotFound(err) {
		t.Errorf("Unexpected error on reading object: %v", err)
	}
	updatedPod.ObjectMeta.ResourceVersion = out.ObjectMeta.ResourceVersion
	expectNoDiff(t, "incorrect pod:", updatedPod, out)
}

// RunTestDeleteWithSuggestionOfDeletedObject 校验对象已被删除后，再用过期的 suggestion
// 发起删除会得到 NotFound，而不是成功或内部错误（幂等删除的边界）。
func RunTestDeleteWithSuggestionOfDeletedObject(ctx context.Context, t *testing.T, store storage.Interface) {
	key, originalPod := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "name", Namespace: "test-ns"}})

	// First delete, so originalPod is outdated.
	// 先删除一次，使 originalPod 成为过期数据。
	deletedPod := &example.Pod{}
	if err := store.Delete(ctx, key, deletedPod, nil, storage.ValidateAllObjectFunc, originalPod, storage.DeleteOptions{}); err != nil {
		t.Errorf("Unexpected failure during deletion: %v", err)
	}

	// Now try deleting with stale object.
	out := &example.Pod{}
	if err := store.Delete(ctx, key, out, nil, storage.ValidateAllObjectFunc, originalPod, storage.DeleteOptions{}); !storage.IsNotFound(err) {
		t.Errorf("Unexpected error during deletion: %v, expected not-found", err)
	}
}

// RunTestValidateDeletionWithSuggestion 校验 validateDeletion 回调的调用次数：
// 建议对象过期时允许重试一次，但实现也可以直接忽略 suggestion 只调用一次。
func RunTestValidateDeletionWithSuggestion(ctx context.Context, t *testing.T, store storage.Interface) {
	key, originalPod := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "name", Namespace: "test-ns"}})

	// Check that validaing fresh object fails is called once and fails.
	// 校验函数直接返回错误时，删除必须立即失败且只调用一次。
	validationCalls := 0
	validationError := fmt.Errorf("validation error")
	validateNothing := func(_ context.Context, _ runtime.Object) error {
		validationCalls++
		return validationError
	}
	out := &example.Pod{}
	// nolint:errorlint // not changing the level of assertion
	if err := store.Delete(ctx, key, out, nil, validateNothing, originalPod, storage.DeleteOptions{}); err != validationError {
		t.Errorf("Unexpected failure during deletion: %v", err)
	}
	if validationCalls != 1 {
		t.Errorf("validate function should have been called once, called %d", validationCalls)
	}

	// First update, so originalPod is outdated.
	updatedPod := &example.Pod{}
	if err := store.GuaranteedUpdate(ctx, key, updatedPod, false, nil,
		storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
			pod := obj.(*example.Pod)
			pod.ObjectMeta.Labels = map[string]string{"foo": "bar"}
			return pod, nil
		}), nil); err != nil {
		t.Errorf("Unexpected failure during updated: %v", err)
	}

	calls := 0
	validateFresh := func(_ context.Context, obj runtime.Object) error {
		calls++
		pod := obj.(*example.Pod)
		if pod.ObjectMeta.Labels == nil || pod.ObjectMeta.Labels["foo"] != "bar" {
			return fmt.Errorf("stale object")
		}
		return nil
	}

	if err := store.Delete(ctx, key, out, nil, validateFresh, originalPod, storage.DeleteOptions{}); err != nil {
		t.Errorf("Unexpected failure during deletion: %v", err)
	}

	// Implementations of the storage interface are allowed to ignore the suggestion,
	// in which case just one validation call is possible.
	// 实现可以忽略 suggestion，此时只调用一次校验也是合法的，因此用上界而非精确值断言。
	if calls > 2 {
		t.Errorf("validate function should have been called at most twice, called %d", calls)
	}

	if err := store.Get(ctx, key, storage.GetOptions{}, &example.Pod{}); !storage.IsNotFound(err) {
		t.Errorf("Unexpected error on reading object: %v", err)
	}
}

// RunTestValidateDeletionWithOnlySuggestionValid tests the case of delete with validateDeletion function,
// when the suggested cachedExistingObject passes the validate function while the current version does not pass the validate function.
// 即只有过期缓存能通过校验：此时必须拒绝删除，并且不得真的删掉当前对象。
func RunTestValidateDeletionWithOnlySuggestionValid(ctx context.Context, t *testing.T, store storage.Interface) {
	key, originalPod := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "name", Namespace: "test-ns", Labels: map[string]string{"foo": "bar"}}})

	// Check that validaing fresh object fails is called once and fails.
	// 校验函数直接报错时，删除应立即失败且只调用一次。
	validationCalls := 0
	validationError := fmt.Errorf("validation error")
	validateNothing := func(_ context.Context, _ runtime.Object) error {
		validationCalls++
		return validationError
	}
	out := &example.Pod{}
	// nolint:errorlint // not changing the level of assertion
	if err := store.Delete(ctx, key, out, nil, validateNothing, originalPod, storage.DeleteOptions{}); err != validationError {
		t.Errorf("Unexpected failure during deletion: %v", err)
	}
	if validationCalls != 1 {
		t.Errorf("validate function should have been called once, called %d", validationCalls)
	}

	// First update, so originalPod is outdated.
	updatedPod := &example.Pod{}
	if err := store.GuaranteedUpdate(ctx, key, updatedPod, false, nil,
		storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
			pod := obj.(*example.Pod)
			pod.ObjectMeta.Labels = map[string]string{"foo": "barbar"}
			return pod, nil
		}), nil); err != nil {
		t.Errorf("Unexpected failure during updated: %v", err)
	}

	calls := 0
	validateFresh := func(_ context.Context, obj runtime.Object) error {
		calls++
		pod := obj.(*example.Pod)
		if pod.ObjectMeta.Labels == nil || pod.ObjectMeta.Labels["foo"] != "bar" {
			return fmt.Errorf("stale object")
		}
		return nil
	}

	err := store.Delete(ctx, key, out, nil, validateFresh, originalPod, storage.DeleteOptions{})
	if err == nil || err.Error() != "stale object" {
		t.Errorf("expecting stale object error, but get: %s", err)
	}

	// Implementations of the storage interface are allowed to ignore the suggestion,
	// in which case just one validation call is possible.
	// 同上：允许只调用一次，断言取上界。
	if calls > 2 {
		t.Errorf("validate function should have been called at most twice, called %d", calls)
	}

	if err = store.Get(ctx, key, storage.GetOptions{}, out); err != nil {
		t.Errorf("Unexpected error on reading object: %v", err)
	}
	expectNoDiff(t, "incorrect pod:", updatedPod, out)
}

// RunTestPreconditionalDeleteWithSuggestion 校验 UID 前置条件与 suggestion 组合使用：
// suggestion 过期不影响基于最新 UID 的删除成功。
func RunTestPreconditionalDeleteWithSuggestion(ctx context.Context, t *testing.T, store storage.Interface) {
	key, originalPod := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "name", Namespace: "test-ns"}})

	// First update, so originalPod is outdated.
	// 先更新一次（同时修改 UID），让 originalPod 与 UID 前置条件都指向旧状态。
	updatedPod := &example.Pod{}
	if err := store.GuaranteedUpdate(ctx, key, updatedPod, false, nil,
		storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
			pod := obj.(*example.Pod)
			pod.ObjectMeta.UID = "myUID"
			return pod, nil
		}), nil); err != nil {
		t.Errorf("Unexpected failure during updated: %v", err)
	}

	prec := storage.NewUIDPreconditions("myUID")

	out := &example.Pod{}
	if err := store.Delete(ctx, key, out, prec, storage.ValidateAllObjectFunc, originalPod, storage.DeleteOptions{}); err != nil {
		t.Errorf("Unexpected failure during deletion: %v", err)
	}

	if err := store.Get(ctx, key, storage.GetOptions{}, &example.Pod{}); !storage.IsNotFound(err) {
		t.Errorf("Unexpected error on reading object: %v", err)
	}
}

// RunTestPreconditionalDeleteWithOnlySuggestionPass tests the case of delete with preconditions,
// when the suggested cachedExistingObject passes the preconditions while the current version does not pass the preconditions.
// 前置条件只对过期 suggestion 成立时，删除必须失败，绝不能误删当前版本。
func RunTestPreconditionalDeleteWithOnlySuggestionPass(ctx context.Context, t *testing.T, store storage.Interface) {
	key, originalPod := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "name", Namespace: "test-ns", UID: "myUID"}})

	// First update, so originalPod is outdated.
	// 更新 UID，使原始对象的前置条件只对过期数据成立。
	updatedPod := &example.Pod{}
	if err := store.GuaranteedUpdate(ctx, key, updatedPod, false, nil,
		storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
			pod := obj.(*example.Pod)
			pod.ObjectMeta.UID = "otherUID"
			return pod, nil
		}), nil); err != nil {
		t.Errorf("Unexpected failure during updated: %v", err)
	}

	prec := storage.NewUIDPreconditions("myUID")
	// Although originalPod passes the precondition, its delete would fail due to conflict.
	// The 2nd try with updatedPod would fail the precondition.
	out := &example.Pod{}
	err := store.Delete(ctx, key, out, prec, storage.ValidateAllObjectFunc, originalPod, storage.DeleteOptions{})
	if err == nil || !storage.IsInvalidObj(err) {
		t.Errorf("expecting invalid UID error, but get: %s", err)
	}

	if err = store.Get(ctx, key, storage.GetOptions{}, out); err != nil {
		t.Errorf("Unexpected error on reading object: %v", err)
	}
	expectNoDiff(t, "incorrect pod:", updatedPod, out)
}

// RunTestList 校验 List 的核心契约：resourceVersion 与 resourceVersionMatch 的组合语义、
// 分页/continue、前缀递归展开，以及 watchcache 是否按预期回源 etcd。
// watchCacheEnabled 为 true 时会额外比对 recorder 记录的 etcd 请求，确保缓存层没有多做/少做查询。
func RunTestList(ctx context.Context, t *testing.T, store storage.Interface, compact Compaction, watchCacheEnabled bool, recorder *KubernetesRecorder) {
	initialRV, createdPods, updatedPod, err := seedMultiLevelData(ctx, store)
	if err != nil {
		t.Fatal(err)
	}

	list := &example.PodList{}
	storageOpts := storage.ListOptions{
		// Ensure we're listing from "now".
		// 空 RV 表示一致读，取当前最新版本作为后续 continue token 的基准。
		ResourceVersion: "",
		Predicate:       storage.Everything,
		Recursive:       true,
	}
	if err := store.GetList(ctx, "/pods/second", storageOpts, list); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	continueRV := mustParseResourceVersion(t, list.ResourceVersion)
	// 预先构造一个指向 second/foo 的 continue token，用于测试"客户端自带 token"的路径。
	secondContinuation, err := storage.EncodeContinue("/pods/second/foo", "/pods/second/", int64(continueRV))
	if err != nil {
		t.Fatal(err)
	}

	getAttrs := func(obj runtime.Object) (labels.Set, fields.Set, error) {
		pod := obj.(*example.Pod)
		return nil, fields.Set{"metadata.name": pod.Name, "spec.nodeName": pod.Spec.NodeName}, nil
	}
	// Compact and increase RV to test consistent List.
	// 压缩早期版本，用于验证过旧的 RV/continue token 会被明确拒绝而不是静默返回错误数据。
	compact(ctx, t, createdPods[0].ResourceVersion)
	currentRV := fmt.Sprintf("%d", continueRV+1)

	// 测试表字段说明：expectRV 要求响应原样回显请求 RV（match=Exact 时必须）；
	// expectRVFunc 用于允许"不早于"某版本的模糊断言；expectCacherRequestsToEtcd
	// 描述 watchcache 打开时预期下推到 etcd 的请求序列。
	tests := []struct {
		name                       string
		rv                         string
		rvMatch                    metav1.ResourceVersionMatch
		prefix                     string
		pred                       storage.SelectionPredicate
		ignoreForWatchCache        bool
		expectedOut                []example.Pod
		expectedAlternatives       [][]example.Pod
		expectContinue             bool
		expectContinueExact        string
		expectedRemainingItemCount *int64
		expectError                bool
		expectRVTooLarge           bool
		expectRVTooOld             bool
		expectContinueTooOld       bool
		expectRV                   string
		expectRVFunc               func(string) error
		expectCacherRequestsToEtcd func() []RecordedList
	}{
		{
			name:        "rejects invalid resource version",
			prefix:      "/pods/",
			pred:        storage.Everything,
			rv:          "abc",
			expectError: true,
		},
		{
			name:   "rejects resource version and continue token",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label:    labels.Everything(),
				Field:    fields.Everything(),
				Limit:    1,
				Continue: secondContinuation,
			},
			rv:          "1",
			expectError: true,
		},
		{
			name:             "rejects resource version set too high",
			prefix:           "/pods/",
			pred:             storage.Everything,
			rv:               strconv.FormatInt(math.MaxInt64, 10),
			expectRVTooLarge: true,
		},
		{
			name:        "test List on existing key",
			prefix:      "/pods/first/",
			pred:        storage.Everything,
			expectedOut: []example.Pod{*updatedPod},
			expectCacherRequestsToEtcd: func() []RecordedList {
				return nil
			},
		},
		{
			name:                 "test List on existing key with resource version set to 0",
			prefix:               "/pods/first/",
			pred:                 storage.Everything,
			expectedAlternatives: [][]example.Pod{{}, {*updatedPod}},
			rv:                   "0",
		},
		{
			name:        "test List on existing key with resource version set before update, match=Exact",
			prefix:      "/pods/first/",
			pred:        storage.Everything,
			expectedOut: []example.Pod{*createdPods[0]},
			rv:          createdPods[0].ResourceVersion,
			rvMatch:     metav1.ResourceVersionMatchExact,
			expectRV:    createdPods[0].ResourceVersion,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/first/",
						Revision: mustParseResourceVersion(t, createdPods[0].ResourceVersion),
					},
				}
			},
		},
		{
			name:        "test List on existing key with resource version set before creation, match=Exact",
			prefix:      "/pods/second/",
			pred:        storage.Everything,
			expectedOut: []example.Pod{},
			rv:          createdPods[0].ResourceVersion,
			rvMatch:     metav1.ResourceVersionMatchExact,
			expectRV:    createdPods[0].ResourceVersion,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/second/",
						Revision: mustParseResourceVersion(t, createdPods[0].ResourceVersion),
					},
				}
			},
		},
		{
			name:        "test List on existing key with resource version set after creation, match=Exact",
			prefix:      "/pods/second/",
			pred:        storage.Everything,
			expectedOut: []example.Pod{*createdPods[1]},
			rv:          createdPods[1].ResourceVersion,
			rvMatch:     metav1.ResourceVersionMatchExact,
			expectRV:    createdPods[1].ResourceVersion,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/second/",
						Revision: mustParseResourceVersion(t, createdPods[1].ResourceVersion),
					},
				}
			},
		},
		{
			name:           "test List on existing key with resource version set before first write, match=Exact",
			prefix:         "/pods/first/",
			pred:           storage.Everything,
			rv:             initialRV,
			rvMatch:        metav1.ResourceVersionMatchExact,
			expectRVTooOld: true,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/first/",
						Revision: mustParseResourceVersion(t, initialRV),
					},
				}
			},
		},
		{
			name:                 "test List on existing key with resource version set to 0, match=NotOlderThan",
			prefix:               "/pods/first/",
			pred:                 storage.Everything,
			expectedAlternatives: [][]example.Pod{{}, {*updatedPod}},
			rv:                   "0",
			rvMatch:              metav1.ResourceVersionMatchNotOlderThan,
		},
		{
			name:        "test List on existing key with resource version set to 0, match=Invalid",
			prefix:      "/pods/first/",
			pred:        storage.Everything,
			rv:          "0",
			rvMatch:     "Invalid",
			expectError: true,
		},
		{
			name:                 "test List on existing key with resource version set before first write, match=NotOlderThan",
			prefix:               "/pods/first/",
			pred:                 storage.Everything,
			expectedAlternatives: [][]example.Pod{{}, {*updatedPod}},
			rv:                   initialRV,
			rvMatch:              metav1.ResourceVersionMatchNotOlderThan,
		},
		{
			name:        "test List on existing key with resource version set before first write, match=Invalid",
			prefix:      "/pods/first/",
			pred:        storage.Everything,
			rv:          initialRV,
			rvMatch:     "Invalid",
			expectError: true,
		},
		{
			name:        "test List on existing key with resource version set to current resource version",
			prefix:      "/pods/first/",
			pred:        storage.Everything,
			expectedOut: []example.Pod{*updatedPod},
			rv:          list.ResourceVersion,
		},
		{
			name:        "test List on existing key with resource version set to current resource version, match=Exact",
			prefix:      "/pods/first/",
			pred:        storage.Everything,
			expectedOut: []example.Pod{*updatedPod},
			rv:          list.ResourceVersion,
			rvMatch:     metav1.ResourceVersionMatchExact,
			expectRV:    list.ResourceVersion,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/first/",
						Revision: int64(continueRV),
					},
				}
			},
		},
		{
			name:        "test List on existing key with resource version set to current resource version, match=NotOlderThan",
			prefix:      "/pods/first/",
			pred:        storage.Everything,
			expectedOut: []example.Pod{*updatedPod},
			rv:          list.ResourceVersion,
			rvMatch:     metav1.ResourceVersionMatchNotOlderThan,
		},
		{
			name:        "test List on non-existing key",
			prefix:      "/pods/non-existing/",
			pred:        storage.Everything,
			expectedOut: []example.Pod{},
			expectCacherRequestsToEtcd: func() []RecordedList {
				return nil
			},
		},
		{
			name:   "test List with pod name matching",
			prefix: "/pods/first/",
			pred: storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.ParseSelectorOrDie("metadata.name!=bar"),
			},
			expectedOut: []example.Pod{},
			expectCacherRequestsToEtcd: func() []RecordedList {
				return nil
			},
		},
		{
			name:   "test List with pod name matching with resource version set to current resource version, match=NotOlderThan",
			prefix: "/pods/first/",
			pred: storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.ParseSelectorOrDie("metadata.name!=bar"),
			},
			expectedOut: []example.Pod{},
			rv:          list.ResourceVersion,
			rvMatch:     metav1.ResourceVersionMatchNotOlderThan,
		},
		{
			name:   "test List with limit",
			prefix: "/pods/second/",
			pred: storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.Everything(),
				Limit: 1,
			},
			expectedOut:                []example.Pod{*createdPods[1]},
			expectRV:                   currentRV,
			expectContinue:             true,
			expectContinueExact:        encodeContinueOrDie(createdPods[1].Name+"\x00", int64(mustAtoi(currentRV))),
			expectedRemainingItemCount: ptr.To[int64](1),
			expectCacherRequestsToEtcd: func() []RecordedList {
				return nil
			},
		},
		{
			name:   "test List with limit at current resource version",
			prefix: "/pods/second/",
			pred: storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.Everything(),
				Limit: 1,
			},
			expectedOut:                []example.Pod{*createdPods[1]},
			expectContinue:             true,
			expectContinueExact:        encodeContinueOrDie(createdPods[1].Name+"\x00", int64(mustAtoi(list.ResourceVersion))),
			expectedRemainingItemCount: ptr.To[int64](1),
			rv:                         list.ResourceVersion,
			expectRV:                   list.ResourceVersion,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/second/",
						Revision: int64(continueRV),
						Limit:    1,
					},
				}
			},
		},
		{
			name:   "test List with limit at current resource version and match=Exact",
			prefix: "/pods/second/",
			pred: storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.Everything(),
				Limit: 1,
			},
			expectedOut:                []example.Pod{*createdPods[1]},
			expectContinue:             true,
			expectContinueExact:        encodeContinueOrDie(createdPods[1].Name+"\x00", int64(mustAtoi(list.ResourceVersion))),
			expectedRemainingItemCount: ptr.To[int64](1),
			rv:                         list.ResourceVersion,
			rvMatch:                    metav1.ResourceVersionMatchExact,
			expectRV:                   list.ResourceVersion,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/second/",
						Revision: int64(continueRV),
						Limit:    1,
					},
				}
			},
		},
		{
			name:   "test List with limit at current resource version and match=NotOlderThan",
			prefix: "/pods/second/",
			pred: storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.Everything(),
				Limit: 1,
			},
			expectedOut:                []example.Pod{*createdPods[1]},
			expectContinue:             true,
			expectedRemainingItemCount: ptr.To[int64](1),
			rv:                         list.ResourceVersion,
			rvMatch:                    metav1.ResourceVersionMatchNotOlderThan,
			expectRVFunc:               resourceVersionNotOlderThan(list.ResourceVersion),
		},
		{
			name:   "test List with limit at resource version 0",
			prefix: "/pods/second/",
			pred: storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.Everything(),
				Limit: 1,
			},
			// TODO(#108003): As of now, watchcache is deliberately ignoring
			// limit if RV=0 is specified, returning whole list of objects.
			// While this should eventually get fixed, for now we're explicitly
			// ignoring this testcase for watchcache.
			ignoreForWatchCache:        true,
			expectedOut:                []example.Pod{*createdPods[1]},
			expectContinue:             true,
			expectedRemainingItemCount: ptr.To[int64](1),
			rv:                         "0",
			expectRVFunc:               resourceVersionNotOlderThan(list.ResourceVersion),
		},
		{
			name:   "test List with limit at resource version 0 match=NotOlderThan",
			prefix: "/pods/second/",
			pred: storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.Everything(),
				Limit: 1,
			},
			// TODO(#108003): As of now, watchcache is deliberately ignoring
			// limit if RV=0 is specified, returning whole list of objects.
			// While this should eventually get fixed, for now we're explicitly
			// ignoring this testcase for watchcache.
			ignoreForWatchCache:        true,
			expectedOut:                []example.Pod{*createdPods[1]},
			expectContinue:             true,
			expectedRemainingItemCount: ptr.To[int64](1),
			rv:                         "0",
			rvMatch:                    metav1.ResourceVersionMatchNotOlderThan,
			expectRVFunc:               resourceVersionNotOlderThan(list.ResourceVersion),
		},
		{
			name:   "test List with limit at resource version before created and match=Exact",
			prefix: "/pods/second/",
			pred: storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.Everything(),
				Limit: 1,
			},
			expectedOut:    []example.Pod{},
			expectContinue: false,
			rv:             createdPods[0].ResourceVersion,
			rvMatch:        metav1.ResourceVersionMatchExact,
			expectRV:       createdPods[0].ResourceVersion,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/second/",
						Revision: mustParseResourceVersion(t, createdPods[0].ResourceVersion),
						Limit:    1,
					},
				}
			},
		},
		{
			name:   "test List with limit at resource version after created and match=Exact",
			prefix: "/pods/second/",
			pred: storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.Everything(),
				Limit: 1,
			},
			expectedOut:    []example.Pod{*createdPods[1]},
			expectContinue: false,
			rv:             createdPods[1].ResourceVersion,
			rvMatch:        metav1.ResourceVersionMatchExact,
			expectRV:       createdPods[1].ResourceVersion,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/second/",
						Revision: mustParseResourceVersion(t, createdPods[1].ResourceVersion),
						Limit:    1,
					},
				}
			},
		},
		{
			name:   "test List with pregenerated continue token",
			prefix: "/pods/second/",
			pred: storage.SelectionPredicate{
				Label:    labels.Everything(),
				Field:    fields.Everything(),
				Limit:    1,
				Continue: secondContinuation,
			},
			expectedOut: []example.Pod{*createdPods[2]},
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/second/foo",
						Revision: int64(continueRV),
						Limit:    1,
					},
				}
			},
		},
		{
			name:   "ignores resource version 0 for List with pregenerated continue token",
			prefix: "/pods/second/",
			pred: storage.SelectionPredicate{
				Label:    labels.Everything(),
				Field:    fields.Everything(),
				Limit:    1,
				Continue: secondContinuation,
			},
			rv:          "0",
			expectedOut: []example.Pod{*createdPods[2]},
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/second/foo",
						Revision: int64(continueRV),
						Limit:    1,
					},
				}
			},
		},
		{
			name:        "test List with multiple levels of directories and expect flattened result",
			prefix:      "/pods/second/",
			pred:        storage.Everything,
			expectedOut: []example.Pod{*createdPods[1], *createdPods[2]},
			expectCacherRequestsToEtcd: func() []RecordedList {
				return nil
			},
		},
		{
			name:        "test List with multiple levels of directories and expect flattened result with current resource version and match=NotOlderThan",
			prefix:      "/pods/second/",
			pred:        storage.Everything,
			rv:          list.ResourceVersion,
			rvMatch:     metav1.ResourceVersionMatchNotOlderThan,
			expectedOut: []example.Pod{*createdPods[1], *createdPods[2]},
		},
		{
			name:   "test List with filter returning only one item, ensure only a single page returned",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Field: fields.OneTermEqualSelector("metadata.name", "barfoo"),
				Label: labels.Everything(),
				Limit: 1,
			},
			expectedOut:    []example.Pod{*createdPods[3]},
			expectContinue: true,
			expectCacherRequestsToEtcd: func() []RecordedList {
				return nil
			},
		},
		{
			name:   "test List with filter returning only one item, ensure only a single page returned with current resource version and match=NotOlderThan",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Field: fields.OneTermEqualSelector("metadata.name", "barfoo"),
				Label: labels.Everything(),
				Limit: 1,
			},
			rv:             list.ResourceVersion,
			rvMatch:        metav1.ResourceVersionMatchNotOlderThan,
			expectedOut:    []example.Pod{*createdPods[3]},
			expectRVFunc:   resourceVersionNotOlderThan(list.ResourceVersion),
			expectContinue: true,
		},
		{
			name:   "test List with filter returning only one item, covers the entire list",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Field: fields.OneTermEqualSelector("metadata.name", "barfoo"),
				Label: labels.Everything(),
				Limit: 2,
			},
			expectedOut:    []example.Pod{*createdPods[3]},
			expectContinue: false,
			expectCacherRequestsToEtcd: func() []RecordedList {
				return nil
			},
		},
		{
			name:   "test List with filter returning only one item, covers the entire list with current resource version and match=NotOlderThan",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Field: fields.OneTermEqualSelector("metadata.name", "barfoo"),
				Label: labels.Everything(),
				Limit: 2,
			},
			rv:             list.ResourceVersion,
			rvMatch:        metav1.ResourceVersionMatchNotOlderThan,
			expectedOut:    []example.Pod{*createdPods[3]},
			expectRVFunc:   resourceVersionNotOlderThan(list.ResourceVersion),
			expectContinue: false,
		},
		{
			name:   "test List with filter returning only one item, covers the entire list, with resource version 0",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Field: fields.OneTermEqualSelector("metadata.name", "barfoo"),
				Label: labels.Everything(),
				Limit: 2,
			},
			rv:                   "0",
			expectedAlternatives: [][]example.Pod{{}, {*createdPods[3]}},
			expectContinue:       false,
		},
		{
			name:   "test List with filter returning two items, more pages possible",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Field: fields.OneTermEqualSelector("metadata.name", "bar"),
				Label: labels.Everything(),
				Limit: 2,
			},
			expectContinue: true,
			expectedOut:    []example.Pod{*updatedPod, *createdPods[1]},
			expectCacherRequestsToEtcd: func() []RecordedList {
				return nil
			},
		},
		{
			name:   "test List with filter returning two items, more pages possible with current resource version and match=NotOlderThan",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Field: fields.OneTermEqualSelector("metadata.name", "bar"),
				Label: labels.Everything(),
				Limit: 2,
			},
			rv:             list.ResourceVersion,
			rvMatch:        metav1.ResourceVersionMatchNotOlderThan,
			expectContinue: true,
			expectedOut:    []example.Pod{*updatedPod, *createdPods[1]},
		},
		{
			name:   "filter returns two items split across multiple pages",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Field: fields.OneTermEqualSelector("metadata.name", "foo"),
				Label: labels.Everything(),
				Limit: 2,
			},
			expectedOut: []example.Pod{*createdPods[2], *createdPods[4]},
			expectCacherRequestsToEtcd: func() []RecordedList {
				return nil
			},
		},
		{
			name:   "filter returns two items split across multiple pages with current resource version and match=NotOlderThan",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Field: fields.OneTermEqualSelector("metadata.name", "foo"),
				Label: labels.Everything(),
				Limit: 2,
			},
			rv:          list.ResourceVersion,
			rvMatch:     metav1.ResourceVersionMatchNotOlderThan,
			expectedOut: []example.Pod{*createdPods[2], *createdPods[4]},
		},
		{
			name:   "filter returns one item for last page, ends on last item, not full",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Field:    fields.OneTermEqualSelector("metadata.name", "foo"),
				Label:    labels.Everything(),
				Limit:    2,
				Continue: encodeContinueOrDie("third/barfoo", int64(continueRV)),
			},
			expectedOut: []example.Pod{*createdPods[4]},
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/third/barfoo",
						Revision: int64(continueRV),
						Limit:    2,
					},
				}
			},
		},
		{
			name:   "filter returns one item for last page, starts on last item, full",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Field:    fields.OneTermEqualSelector("metadata.name", "foo"),
				Label:    labels.Everything(),
				Limit:    1,
				Continue: encodeContinueOrDie("third/barfoo", int64(continueRV)),
			},
			expectedOut: []example.Pod{*createdPods[4]},
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/third/barfoo",
						Revision: int64(continueRV),
						Limit:    1,
					},
					{
						Key:      "/registry/pods/third/barfoo\x00",
						Revision: int64(continueRV),
						Limit:    2,
					},
				}
			},
		},
		{
			name:   "filter returns one item for last page, starts on last item, partial page",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Field:    fields.OneTermEqualSelector("metadata.name", "foo"),
				Label:    labels.Everything(),
				Limit:    2,
				Continue: encodeContinueOrDie("third/barfoo", int64(continueRV)),
			},
			expectedOut: []example.Pod{*createdPods[4]},
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/third/barfoo",
						Revision: int64(continueRV),
						Limit:    2,
					},
				}
			},
		},
		{
			name:   "filter returns two items, page size equal to total list size",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Field: fields.OneTermEqualSelector("metadata.name", "foo"),
				Label: labels.Everything(),
				Limit: 5,
			},
			expectedOut: []example.Pod{*createdPods[2], *createdPods[4]},
			expectCacherRequestsToEtcd: func() []RecordedList {
				return nil
			},
		},
		{
			name:   "filter returns two items, page size equal to total list size with current resource version and match=NotOlderThan",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Field: fields.OneTermEqualSelector("metadata.name", "foo"),
				Label: labels.Everything(),
				Limit: 5,
			},
			rv:          list.ResourceVersion,
			rvMatch:     metav1.ResourceVersionMatchNotOlderThan,
			expectedOut: []example.Pod{*createdPods[2], *createdPods[4]},
		},
		{
			name:   "filter returns one item, page size equal to total list size",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Field: fields.OneTermEqualSelector("metadata.name", "barfoo"),
				Label: labels.Everything(),
				Limit: 5,
			},
			expectedOut: []example.Pod{*createdPods[3]},
			expectCacherRequestsToEtcd: func() []RecordedList {
				return nil
			},
		},
		{
			name:   "filter returns one item, page size equal to total list size with current resource version and match=NotOlderThan",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Field: fields.OneTermEqualSelector("metadata.name", "barfoo"),
				Label: labels.Everything(),
				Limit: 5,
			},
			rv:          list.ResourceVersion,
			rvMatch:     metav1.ResourceVersionMatchNotOlderThan,
			expectedOut: []example.Pod{*createdPods[3]},
		},
		{
			name:        "list all items",
			prefix:      "/pods/",
			pred:        storage.Everything,
			expectedOut: []example.Pod{*updatedPod, *createdPods[1], *createdPods[2], *createdPods[3], *createdPods[4]},
			expectCacherRequestsToEtcd: func() []RecordedList {
				return nil
			},
		},
		{
			name:        "list all items with current resource version and match=NotOlderThan",
			prefix:      "/pods/",
			pred:        storage.Everything,
			rv:          list.ResourceVersion,
			rvMatch:     metav1.ResourceVersionMatchNotOlderThan,
			expectedOut: []example.Pod{*updatedPod, *createdPods[1], *createdPods[2], *createdPods[3], *createdPods[4]},
		},
		{
			name:   "verify list returns updated version of object; filter returns one item, page size equal to total list size with current resource version and match=NotOlderThan",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Field: fields.OneTermEqualSelector("spec.nodeName", "fakeNode"),
				Label: labels.Everything(),
				Limit: 5,
			},
			rv:          list.ResourceVersion,
			rvMatch:     metav1.ResourceVersionMatchNotOlderThan,
			expectedOut: []example.Pod{*updatedPod},
		},
		{
			name:   "verify list does not return deleted object; filter for deleted object, page size equal to total list size with current resource version and match=NotOlderThan",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Field: fields.OneTermEqualSelector("metadata.name", "baz"),
				Label: labels.Everything(),
				Limit: 5,
			},
			rv:          list.ResourceVersion,
			rvMatch:     metav1.ResourceVersionMatchNotOlderThan,
			expectedOut: []example.Pod{},
		},
		{
			name:        "test consistent List",
			prefix:      "/pods/empty",
			pred:        storage.Everything,
			rv:          "",
			expectRV:    currentRV,
			expectedOut: []example.Pod{},
			expectCacherRequestsToEtcd: func() []RecordedList {
				return nil
			},
		},
		{
			name:         "test non-consistent List",
			prefix:       "/pods/empty",
			pred:         storage.Everything,
			rv:           "0",
			expectRVFunc: resourceVersionNotOlderThan(initialRV),
			expectedOut:  []example.Pod{},
		},
		// match=Exact
		// 以下一组用例验证 match=Exact 的历史快照语义：返回的是"该 RV 时刻"的完整列表，
		// 因此随着写入次数增加，期望结果逐条累积。
		{
			name:           "test List with resource version set before first write, match=Exact",
			prefix:         "/pods/",
			pred:           storage.Everything,
			rv:             initialRV,
			rvMatch:        metav1.ResourceVersionMatchExact,
			expectRVTooOld: true,
		},
		{
			name:        "test List with resource version of first write, match=Exact",
			prefix:      "/pods/",
			pred:        storage.Everything,
			expectedOut: []example.Pod{*createdPods[0]},
			rv:          createdPods[0].ResourceVersion,
			rvMatch:     metav1.ResourceVersionMatchExact,
			expectRV:    createdPods[0].ResourceVersion,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/",
						Revision: 2,
					},
				}
			},
		},
		{
			name:        "test List with resource version of second write, match=Exact",
			prefix:      "/pods/",
			pred:        storage.Everything,
			expectedOut: []example.Pod{*createdPods[0], *createdPods[1]},
			rv:          createdPods[1].ResourceVersion,
			rvMatch:     metav1.ResourceVersionMatchExact,
			expectRV:    createdPods[1].ResourceVersion,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/",
						Revision: 3,
					},
				}
			},
		},
		{
			name:        "test List with resource version of third write, match=Exact",
			prefix:      "/pods/",
			pred:        storage.Everything,
			expectedOut: []example.Pod{*createdPods[0], *createdPods[1], *createdPods[2]},
			rv:          createdPods[2].ResourceVersion,
			rvMatch:     metav1.ResourceVersionMatchExact,
			expectRV:    createdPods[2].ResourceVersion,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/",
						Revision: 4,
					},
				}
			},
		},
		{
			name:        "test List with resource version of fourth write, match=Exact",
			prefix:      "/pods/",
			pred:        storage.Everything,
			expectedOut: []example.Pod{*createdPods[0], *createdPods[1], *createdPods[2], *createdPods[3]},
			rv:          createdPods[3].ResourceVersion,
			rvMatch:     metav1.ResourceVersionMatchExact,
			expectRV:    createdPods[3].ResourceVersion,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/",
						Revision: 5,
					},
				}
			},
		},
		{
			name:        "test List with resource version of fifth write, match=Exact",
			prefix:      "/pods/",
			pred:        storage.Everything,
			expectedOut: []example.Pod{*createdPods[0], *createdPods[1], *createdPods[2], *createdPods[3], *createdPods[4]},
			rv:          createdPods[4].ResourceVersion,
			rvMatch:     metav1.ResourceVersionMatchExact,
			expectRV:    createdPods[4].ResourceVersion,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/",
						Revision: 6,
					},
				}
			},
		},
		{
			name:        "test List with resource version of six write, match=Exact",
			prefix:      "/pods/",
			pred:        storage.Everything,
			expectedOut: []example.Pod{*createdPods[0], *createdPods[1], *createdPods[2], *createdPods[3], *createdPods[4], *createdPods[5]},
			rv:          createdPods[5].ResourceVersion,
			rvMatch:     metav1.ResourceVersionMatchExact,
			expectRV:    createdPods[5].ResourceVersion,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/",
						Revision: 7,
					},
				}
			},
		},
		{
			name:        "test List with resource version of seventh write, match=Exact",
			prefix:      "/pods/",
			pred:        storage.Everything,
			expectedOut: []example.Pod{*updatedPod, *createdPods[1], *createdPods[2], *createdPods[3], *createdPods[4], *createdPods[5]},
			rv:          updatedPod.ResourceVersion,
			rvMatch:     metav1.ResourceVersionMatchExact,
			expectRV:    updatedPod.ResourceVersion,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/",
						Revision: 8,
					},
				}
			},
		},
		{
			name:        "test List with resource version of eight write, match=Exact",
			prefix:      "/pods/",
			pred:        storage.Everything,
			expectedOut: []example.Pod{*updatedPod, *createdPods[1], *createdPods[2], *createdPods[3], *createdPods[4]},
			rv:          fmt.Sprint(continueRV),
			rvMatch:     metav1.ResourceVersionMatchExact,
			expectRV:    fmt.Sprint(continueRV),
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/",
						Revision: int64(continueRV),
					},
				}
			},
		},
		{
			name:        "test List with resource version after writes, match=Exact",
			prefix:      "/pods/",
			pred:        storage.Everything,
			expectedOut: []example.Pod{*updatedPod, *createdPods[1], *createdPods[2], *createdPods[3], *createdPods[4]},
			rv:          fmt.Sprint(continueRV + 1),
			rvMatch:     metav1.ResourceVersionMatchExact,
			expectRV:    fmt.Sprint(continueRV + 1),
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/",
						Revision: int64(continueRV) + 1,
					},
				}
			},
		},
		{
			name:             "test List with future resource version, match=Exact",
			prefix:           "/pods/",
			pred:             storage.Everything,
			rv:               fmt.Sprint(continueRV + 2),
			rvMatch:          metav1.ResourceVersionMatchExact,
			expectRVTooLarge: true,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/",
						Revision: int64(continueRV) + 2,
					},
				}
			},
		},
		// limit, match=Exact
		// 带 limit 的 Exact 列表需要在指定 RV 的快照上分页，continue token 里的 RV 必须与该快照一致。
		{
			name:   "test List with limit, resource version of second write, match=Exact",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.Everything(),
				Limit: 1,
			},
			expectedOut:                []example.Pod{*createdPods[0]},
			rv:                         createdPods[1].ResourceVersion,
			expectContinue:             true,
			expectContinueExact:        encodeContinueOrDie(createdPods[0].Namespace+"/"+createdPods[0].Name+"\x00", int64(mustAtoi(createdPods[1].ResourceVersion))),
			rvMatch:                    metav1.ResourceVersionMatchExact,
			expectRV:                   createdPods[1].ResourceVersion,
			expectedRemainingItemCount: ptr.To[int64](1),
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/",
						Revision: mustParseResourceVersion(t, createdPods[1].ResourceVersion),
						Limit:    1,
					},
				}
			},
		},
		{
			name:   "test List with limit, resource version of third write, match=Exact",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.Everything(),
				Limit: 2,
			},
			rv:                         createdPods[2].ResourceVersion,
			rvMatch:                    metav1.ResourceVersionMatchExact,
			expectedOut:                []example.Pod{*createdPods[0], *createdPods[1]},
			expectContinue:             true,
			expectContinueExact:        encodeContinueOrDie(createdPods[1].Namespace+"/"+createdPods[1].Name+"\x00", int64(mustAtoi(createdPods[2].ResourceVersion))),
			expectRV:                   createdPods[2].ResourceVersion,
			expectedRemainingItemCount: ptr.To[int64](1),
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/",
						Revision: mustParseResourceVersion(t, createdPods[2].ResourceVersion),
						Limit:    2,
					},
				}
			},
		},
		{
			name:   "test List with limit, resource version of fourth write, match=Exact",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.Everything(),
				Limit: 4,
			},
			rv:          createdPods[3].ResourceVersion,
			rvMatch:     metav1.ResourceVersionMatchExact,
			expectedOut: []example.Pod{*createdPods[0], *createdPods[1], *createdPods[2], *createdPods[3]},
			expectRV:    createdPods[3].ResourceVersion,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/",
						Revision: mustParseResourceVersion(t, createdPods[3].ResourceVersion),
						Limit:    4,
					},
				}
			},
		},
		{
			name:   "test List with limit, resource version of fifth write, match=Exact",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.Everything(),
				Limit: 1,
			},
			rv:                         createdPods[4].ResourceVersion,
			rvMatch:                    metav1.ResourceVersionMatchExact,
			expectedOut:                []example.Pod{*createdPods[0]},
			expectRV:                   createdPods[4].ResourceVersion,
			expectContinue:             true,
			expectContinueExact:        encodeContinueOrDie(createdPods[0].Namespace+"/"+createdPods[0].Name+"\x00", int64(mustAtoi(createdPods[4].ResourceVersion))),
			expectedRemainingItemCount: ptr.To[int64](4),
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/",
						Revision: mustParseResourceVersion(t, createdPods[4].ResourceVersion),
						Limit:    1,
					},
				}
			},
		},
		{
			name:   "test List with limit, resource version of six write, match=Exact",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.Everything(),
				Limit: 2,
			},
			rv:                         createdPods[5].ResourceVersion,
			rvMatch:                    metav1.ResourceVersionMatchExact,
			expectedOut:                []example.Pod{*createdPods[0], *createdPods[1]},
			expectRV:                   createdPods[5].ResourceVersion,
			expectContinue:             true,
			expectContinueExact:        encodeContinueOrDie(createdPods[1].Namespace+"/"+createdPods[1].Name+"\x00", int64(mustAtoi(createdPods[5].ResourceVersion))),
			expectedRemainingItemCount: ptr.To[int64](4),
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/",
						Revision: mustParseResourceVersion(t, createdPods[5].ResourceVersion),
						Limit:    2,
					},
				}
			},
		},
		{
			name:   "test List with limit, resource version of seventh write, match=Exact",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.Everything(),
				Limit: 4,
			},
			rv:                         updatedPod.ResourceVersion,
			rvMatch:                    metav1.ResourceVersionMatchExact,
			expectedOut:                []example.Pod{*updatedPod, *createdPods[1], *createdPods[5], *createdPods[2]},
			expectRV:                   updatedPod.ResourceVersion,
			expectContinue:             true,
			expectContinueExact:        encodeContinueOrDie(createdPods[2].Namespace+"/"+createdPods[2].Name+"\x00", int64(mustAtoi(updatedPod.ResourceVersion))),
			expectedRemainingItemCount: ptr.To[int64](2),
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/",
						Revision: mustParseResourceVersion(t, updatedPod.ResourceVersion),
						Limit:    4,
					},
				}
			},
		},
		{
			name:   "test List with limit, resource version of eight write, match=Exact",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.Everything(),
				Limit: 8,
			},
			expectedOut: []example.Pod{*updatedPod, *createdPods[1], *createdPods[2], *createdPods[3], *createdPods[4]},
			rv:          fmt.Sprint(continueRV),
			rvMatch:     metav1.ResourceVersionMatchExact,
			expectRV:    fmt.Sprint(continueRV),
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/",
						Revision: int64(continueRV),
						Limit:    8,
					},
				}
			},
		},
		{
			name:   "test List with limit, resource version after writes, match=Exact",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.Everything(),
				Limit: 1,
			},
			rv:                         fmt.Sprint(continueRV + 1),
			rvMatch:                    metav1.ResourceVersionMatchExact,
			expectedOut:                []example.Pod{*updatedPod},
			expectRV:                   fmt.Sprint(continueRV + 1),
			expectContinue:             true,
			expectContinueExact:        encodeContinueOrDie(updatedPod.Namespace+"/"+updatedPod.Name+"\x00", int64(continueRV+1)),
			expectedRemainingItemCount: ptr.To[int64](4),
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/",
						Revision: int64(continueRV) + 1,
						Limit:    1,
					},
				}
			},
		},
		// Continue
		// continue token 中携带起始 key 与 RV；RV 早于压缩点时必须报"continue 过旧"，
		// 否则分页会基于已被清理的历史数据，产生重复或遗漏。
		{
			name:   "test List with continue, resource version before first write",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label:    labels.Everything(),
				Field:    fields.Everything(),
				Limit:    1,
				Continue: encodeContinueOrDie(createdPods[0].Namespace+"/"+createdPods[0].Name+"\x00", mustParseResourceVersion(t, initialRV)),
			},
			expectContinueTooOld: true,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/",
						Revision: mustParseResourceVersion(t, initialRV),
						Limit:    4,
					},
				}
			},
		},
		{
			name:   "test List with continue, resource version of first write",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label:    labels.Everything(),
				Field:    fields.Everything(),
				Limit:    1,
				Continue: encodeContinueOrDie(createdPods[0].Namespace+"/"+createdPods[0].Name+"\x00", int64(mustAtoi(createdPods[0].ResourceVersion))),
			},
			expectedOut: []example.Pod{},
			expectRV:    createdPods[0].ResourceVersion,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/first/bar\x00",
						Revision: mustParseResourceVersion(t, createdPods[0].ResourceVersion),
						Limit:    1,
					},
				}
			},
		},
		{
			name:   "test List with continue, resource version of second write",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label:    labels.Everything(),
				Field:    fields.Everything(),
				Limit:    1,
				Continue: encodeContinueOrDie(createdPods[0].Namespace+"/"+createdPods[0].Name+"\x00", int64(mustAtoi(createdPods[1].ResourceVersion))),
			},
			expectedOut: []example.Pod{*createdPods[1]},
			expectRV:    createdPods[1].ResourceVersion,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/first/bar\x00",
						Revision: mustParseResourceVersion(t, createdPods[1].ResourceVersion),
						Limit:    1,
					},
				}
			},
		},
		{
			name:   "test List with continue, resource version of third write",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label:    labels.Everything(),
				Field:    fields.Everything(),
				Limit:    2,
				Continue: encodeContinueOrDie(createdPods[1].Namespace+"/"+createdPods[1].Name+"\x00", int64(mustAtoi(createdPods[2].ResourceVersion))),
			},
			expectedOut: []example.Pod{*createdPods[2]},
			expectRV:    createdPods[2].ResourceVersion,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/second/bar\x00",
						Revision: mustParseResourceVersion(t, createdPods[2].ResourceVersion),
						Limit:    2,
					},
				}
			},
		},
		{
			name:   "test List with continue, resource version of fifth write",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label:    labels.Everything(),
				Field:    fields.Everything(),
				Limit:    1,
				Continue: encodeContinueOrDie(createdPods[0].Namespace+"/"+createdPods[0].Name+"\x00", int64(mustAtoi(createdPods[4].ResourceVersion))),
			},
			expectedOut:                []example.Pod{*createdPods[1]},
			expectRV:                   createdPods[4].ResourceVersion,
			expectContinue:             true,
			expectContinueExact:        encodeContinueOrDie(createdPods[1].Namespace+"/"+createdPods[1].Name+"\x00", int64(mustAtoi(createdPods[4].ResourceVersion))),
			expectedRemainingItemCount: ptr.To[int64](3),
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/first/bar\x00",
						Revision: mustParseResourceVersion(t, createdPods[4].ResourceVersion),
						Limit:    1,
					},
				}
			},
		},
		{
			name:   "test List with continue, resource version of six write",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label:    labels.Everything(),
				Field:    fields.Everything(),
				Limit:    2,
				Continue: encodeContinueOrDie(createdPods[1].Namespace+"/"+createdPods[1].Name+"\x00", int64(mustAtoi(createdPods[5].ResourceVersion))),
			},
			expectedOut:                []example.Pod{*createdPods[5], *createdPods[2]},
			expectRV:                   createdPods[5].ResourceVersion,
			expectContinue:             true,
			expectContinueExact:        encodeContinueOrDie(createdPods[2].Namespace+"/"+createdPods[2].Name+"\x00", int64(mustAtoi(createdPods[5].ResourceVersion))),
			expectedRemainingItemCount: ptr.To[int64](2),
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/second/bar\x00",
						Revision: mustParseResourceVersion(t, createdPods[5].ResourceVersion),
						Limit:    2,
					},
				}
			},
		},
		{
			name:   "test List with continue, resource version of seventh write",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label:    labels.Everything(),
				Field:    fields.Everything(),
				Limit:    4,
				Continue: encodeContinueOrDie(createdPods[2].Namespace+"/"+createdPods[2].Name+"\x00", int64(mustAtoi(updatedPod.ResourceVersion))),
			},
			expectedOut: []example.Pod{*createdPods[3], *createdPods[4]},
			expectRV:    updatedPod.ResourceVersion,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/second/foo\x00",
						Revision: mustParseResourceVersion(t, updatedPod.ResourceVersion),
						Limit:    4,
					},
				}
			},
		},
		{
			name:   "test List with continue, resource version after writes",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label:    labels.Everything(),
				Field:    fields.Everything(),
				Limit:    1,
				Continue: encodeContinueOrDie(updatedPod.Namespace+"/"+updatedPod.Name+"\x00", int64(continueRV+1)),
			},
			expectedOut:                []example.Pod{*createdPods[1]},
			expectRV:                   fmt.Sprint(continueRV + 1),
			expectContinue:             true,
			expectContinueExact:        encodeContinueOrDie(createdPods[1].Namespace+"/"+createdPods[1].Name+"\x00", int64(continueRV+1)),
			expectedRemainingItemCount: ptr.To[int64](3),
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:      "/registry/pods/first/bar\x00",
						Revision: int64(continueRV) + 1,
						Limit:    1,
					},
				}
			},
		},
		{
			name:   "test List with continue from second pod, negative resource version gives consistent read",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label:    labels.Everything(),
				Field:    fields.Everything(),
				Continue: encodeContinueOrDie(createdPods[0].Namespace+"/"+createdPods[0].Name+"\x00", -1),
			},
			expectedOut: []example.Pod{*createdPods[1], *createdPods[2], *createdPods[3], *createdPods[4]},
			expectRV:    currentRV,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key: "/registry/pods/first/bar\x00",
					},
				}
			},
		},
		{
			name:   "test List with continue from second pod and limit, negative resource version gives consistent read",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label:    labels.Everything(),
				Field:    fields.Everything(),
				Limit:    2,
				Continue: encodeContinueOrDie(createdPods[0].Namespace+"/"+createdPods[0].Name+"\x00", -1),
			},
			expectedOut:                []example.Pod{*createdPods[1], *createdPods[2]},
			expectContinue:             true,
			expectContinueExact:        encodeContinueOrDie(createdPods[2].Namespace+"/"+createdPods[2].Name+"\x00", int64(continueRV+1)),
			expectRV:                   currentRV,
			expectedRemainingItemCount: ptr.To[int64](2),
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key:   "/registry/pods/first/bar\x00",
						Limit: 2,
					},
				}
			},
		},
		{
			name:   "test List with continue from third pod, negative resource version gives consistent read",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label:    labels.Everything(),
				Field:    fields.Everything(),
				Continue: encodeContinueOrDie(createdPods[2].Namespace+"/"+createdPods[2].Name+"\x00", -1),
			},
			expectedOut: []example.Pod{*createdPods[3], *createdPods[4]},
			expectRV:    currentRV,
			expectCacherRequestsToEtcd: func() []RecordedList {
				if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
					return nil
				}
				return []RecordedList{
					{
						Key: "/registry/pods/second/foo\x00",
					},
				}
			},
		},
		{
			name:   "test List with continue from empty fails",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label:    labels.Everything(),
				Field:    fields.Everything(),
				Continue: encodeContinueOrDie("", int64(continueRV)),
			},
			expectError: true,
		},
		{
			name:   "test List with continue from first pod, empty resource version fails",
			prefix: "/pods/",
			pred: storage.SelectionPredicate{
				Label:    labels.Everything(),
				Field:    fields.Everything(),
				Continue: encodeContinueOrDie(createdPods[0].Namespace+"/"+createdPods[0].Name+"\x00", 0),
			},
			expectError: true,
		},
		{
			name:   "test List with negative rv fails",
			prefix: "/pods/",
			rv:     "-1",
			pred: storage.SelectionPredicate{
				Label: labels.Everything(),
				Field: fields.Everything(),
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// For some asynchronous implementations of storage interface (in particular watchcache),
			// certain requests may impact result of further requests. As an example, if we first
			// ensure that watchcache is synchronized up to ResourceVersion X (using Get/List requests
			// with NotOlderThan semantic), the further requests (even specifying earlier resource
			// version) will also return the result synchronized to at least ResourceVersion X.
			// By parallelizing test cases we ensure that the order in which test cases are defined
			// doesn't automatically preclude some scenarios from happening.
			t.Parallel()

			if watchCacheEnabled && tt.ignoreForWatchCache {
				t.Skip()
			}

			if tt.pred.GetAttrs == nil {
				tt.pred.GetAttrs = getAttrs
			}

			out := &example.PodList{}
			storageOpts := storage.ListOptions{
				ResourceVersion:      tt.rv,
				ResourceVersionMatch: tt.rvMatch,
				Predicate:            tt.pred,
				Recursive:            true,
			}
			recorderKey := t.Name()
			listCtx := context.WithValue(ctx, RecorderContextKey, recorderKey)
			err := store.GetList(listCtx, tt.prefix, storageOpts, out)
			if tt.expectRVTooLarge {
				if !storage.IsTooLargeResourceVersion(err) {
					t.Fatalf("expecting resource version too high error, but get: %v", err)
				}
				return
			}
			if tt.expectRVTooOld {
				if err == nil || !strings.Contains(err.Error(), "The resourceVersion for the provided list is too old") {
					t.Fatalf("expecting resource version too old error, but get: %v", err)
				}
				return
			}
			if tt.expectContinueTooOld {
				if err == nil || !strings.Contains(err.Error(), "The provided continue parameter is too old to display a consistent list result") {
					t.Fatalf("expecting continue too old error, but get: %v", err)
				}
				return
			}

			if err != nil {
				if !tt.expectError {
					t.Fatalf("GetList failed: %v", err)
				}
				return
			}
			if tt.expectError {
				t.Fatalf("expected error but got none")
			}
			if (len(out.Continue) > 0) != tt.expectContinue {
				t.Errorf("unexpected continue token: %q", out.Continue)
			}

			if len(tt.expectContinueExact) > 0 {
				ExpectContinueMatches(t, tt.expectContinueExact, out.Continue)
			}

			// If a client requests an exact resource version, it must be echoed back to them.
			// match=Exact 时响应 RV 必须原样回显请求值，客户端据此继续分页。
			if tt.expectRV != "" {
				if tt.expectRV != out.ResourceVersion {
					t.Errorf("resourceVersion in list response want=%s, got=%s", tt.expectRV, out.ResourceVersion)
				}
			}
			if tt.expectRVFunc != nil {
				if err := tt.expectRVFunc(out.ResourceVersion); err != nil {
					t.Errorf("resourceVersion in list response invalid: %v", err)
				}
			}

			if tt.expectedAlternatives == nil {
				sort.Sort(sortablePodList(tt.expectedOut))
				expectNoDiff(t, "incorrect list pods", tt.expectedOut, out.Items)
			} else {
				ExpectContains(t, "incorrect list pods", toInterfaceSlice(tt.expectedAlternatives), out.Items)
			}
			if !cmp.Equal(tt.expectedRemainingItemCount, out.RemainingItemCount) {
				t.Fatalf("unexpected remainingItemCount, diff: %s", cmp.Diff(tt.expectedRemainingItemCount, out.RemainingItemCount))
			}
			if watchCacheEnabled {
				var expectedListRequests []RecordedList
				if tt.expectCacherRequestsToEtcd != nil {
					expectedListRequests = tt.expectCacherRequestsToEtcd()
				}
				gotListRequests := recorder.ListRequestForKey(recorderKey)
				if !cmp.Equal(expectedListRequests, gotListRequests) {
					t.Fatalf("unexpected etcd list requests, diff: %s", cmp.Diff(expectedListRequests, gotListRequests))
				}
			}
		})
	}
}

// ExpectContinueMatches 比较两个 continue token；不相等时解码 base64 后再报错，
// 便于定位 token 内容（起始 key + RV）的差异，而不是只看到两串乱码。
func ExpectContinueMatches(t *testing.T, expect, got string) {
	t.Helper()
	if expect == got {
		return
	}
	expectDecoded, err := base64.RawURLEncoding.DecodeString(expect)
	if err != nil {
		t.Errorf("expected continue token: %q, got: %q", expect, got)
		return
	}
	gotDecoded, err := base64.RawURLEncoding.DecodeString(got)
	if err != nil {
		t.Errorf("expected continue token: %q, got: %q", expect, got)
		return
	}
	t.Errorf("expected continue token: %s, got: %s", expectDecoded, gotDecoded)
}

// RunTestDeleteWithConflictAndMissingExpectedTransformOrDecodeError verifies that, when
// ExpectTransformOrDecodeError is true, the delete will fail if a concurrent write makes it
// possible to decode the object.
// 该选项的语义是"我预期对象读不出来"；一旦对象变得可读，就说明前提不成立，
// 继续删除可能掩盖数据损坏，因此必须返回 Invalid 错误。
func RunTestDeleteWithConflictAndMissingExpectedTransformOrDecodeError(ctx context.Context, t testing.TB, store storage.Interface, setFailing func(bool)) {
	obj := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}}
	key := computePodKey(obj)
	out := &example.Pod{}
	if err := store.Create(ctx, key, obj, out, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	// During validation (immediately before executing the optimistic delete transaction),
	// simulate a concurrent update that both increases the current revision and allows the next
	// TransformFromStorage/Decode to succeed.
	// 在乐观删除事务提交前注入并发写：既推高版本号，又让后续解码恢复成功。
	validateCount := 0
	injectUpdate := func(_ context.Context, _ runtime.Object) error {
		validateCount++
		if validateCount > 1 {
			return nil
		}

		setFailing(false)

		return store.GuaranteedUpdate(ctx, key, &example.Pod{}, false, nil,
			storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
				pod := obj.(*example.Pod)
				pod.Labels = map[string]string{"updated": "true"}
				return pod, nil
			}), nil)
	}

	// Initially, TransformFromStorage or Decode should fail.
	// 初始状态下对象不可读，这是本次删除的前提条件。
	setFailing(true)

	err := store.Delete(ctx, key, &example.Pod{}, nil, injectUpdate, nil,
		storage.DeleteOptions{ExpectTransformOrDecodeError: true})
	if !storage.IsInvalidObj(err) {
		t.Fatalf("Expected Delete to return an 'Invalid Object' error: %v", err)
	}
}

// RunTestDeleteExpectedTransformOrDecodeError tests that an object that cannot be transformed or
// decoded from storage can be deleted using the ExpectTransformOrDecodeError option.
// 这是"卡住的坏对象"的唯一清理途径：管理员显式声明预期不可读后允许删除。
func RunTestDeleteExpectedTransformOrDecodeError(ctx context.Context, t testing.TB, store storage.Interface, setFailing func(bool)) {
	obj := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}}
	key := computePodKey(obj)
	out := &example.Pod{}
	if err := store.Create(ctx, key, obj, out, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	setFailing(true)
	err := store.Delete(ctx, key, &example.Pod{}, nil, storage.ValidateAllObjectFunc, nil,
		storage.DeleteOptions{ExpectTransformOrDecodeError: true})
	if err != nil {
		t.Fatal(err)
	}

	if err := store.Get(ctx, key, storage.GetOptions{}, &example.Pod{}); !storage.IsNotFound(err) {
		t.Errorf("Unexpected error on reading object: %v", err)
	}
}

// RunTestDeleteWithSuggestionAndMissingExpectedTransformOrDecodeError verifies that, when
// ExpectTransformOrDecodeError is true, passing a suggested object does not bypass transformation
// or decoding of the serialized object in storage.
// 关键不变量：suggestion 只是缓存副本，绝不能替代对存储中真实字节的解码校验，
// 否则客户端可用任意建议对象删掉任意 key。
func RunTestDeleteWithSuggestionAndMissingExpectedTransformOrDecodeError(ctx context.Context, t testing.TB, store storage.Interface) {
	obj := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}}
	key := computePodKey(obj)
	out := &example.Pod{}
	if err := store.Create(ctx, key, obj, out, 0); err != nil {
		t.Fatalf("Create: %v", err)
	}

	err := store.Delete(ctx, key, &example.Pod{}, nil, storage.ValidateAllObjectFunc, out,
		storage.DeleteOptions{ExpectTransformOrDecodeError: true})
	if !storage.IsInvalidObj(err) {
		t.Fatalf("Expected Delete to return an 'Invalid Object' error: %v", err)
	}
}

// RunTestConsistentList 校验"一致读"对缓存的副作用：不带 RV 的 List 必须返回一致 RV，
// 并（在 watchcache 实现中）把缓存推进到该 RV；此后 RV=0 的读也只能返回不早于该点的数据。
// listFromCacheSnapshot 为 true 时，负 continue RV 也会触发缓存同步。
func RunTestConsistentList(ctx context.Context, t *testing.T, store storage.Interface, increaseRV IncreaseRVFunc, cacheEnabled, consistentReadsSupported, listFromCacheSnapshot bool) {
	outPod := &example.Pod{}
	inPod := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "foo"}}
	err := store.Create(ctx, computePodKey(inPod), inPod, outPod, 0)
	require.NoError(t, err)
	lastResourceWriteRV, err := strconv.Atoi(outPod.ResourceVersion)
	require.NoError(t, err)

	// 人为推进一次全局 RV，使"一致读 RV"与"最后一次写入 RV"可区分。
	increaseRV(ctx, t)
	consistentRV := lastResourceWriteRV + 1
	cacheSyncRV := 0

	tcs := []struct {
		name               string
		requestRV          string
		continueToken      string
		validateResponseRV func(*testing.T, int)
	}{
		{
			// 一致读之前，缓存可能还没观察到最新写入；etcd 则永远指向最新 RV。
			name:      "Non-consistent list before consistent read",
			requestRV: "0",
			validateResponseRV: func(t *testing.T, rv int) {
				if cacheEnabled {
					// Cache might not yet observed write
					assert.LessOrEqual(t, rv, lastResourceWriteRV)
				} else {
					// Etcd should always be up to date with consistent RV
					assert.Equal(t, consistentRV, rv)
				}
			},
		},
		{
			// 不带 RV 的 List 必须落到最新一致点，并记录该点供后续用例比对。
			name:      "LIST without RV returns consistent RV",
			requestRV: "",
			validateResponseRV: func(t *testing.T, rv int) {
				assert.Equal(t, consistentRV, rv)
				cacheSyncRV = rv
			},
		},
		{
			// 负 continue RV 表示"从最新一致点继续"，同样必须返回一致 RV。
			name:          "List with negative continue RV returns consistent RV",
			continueToken: encodeContinueOrDie("/pods/a", -1),
			validateResponseRV: func(t *testing.T, rv int) {
				assert.Equal(t, consistentRV, rv)
				if listFromCacheSnapshot {
					cacheSyncRV = rv
				}
			},
		},
		{
			// 一致读是否把缓存推到 cacheSyncRV，取决于实现是否支持一致读。
			name:      "Non-consistent request after consistent read",
			requestRV: "0",
			validateResponseRV: func(t *testing.T, rv int) {
				if cacheEnabled {
					if consistentReadsSupported {
						// Consistent read will sync cache
						assert.Equal(t, cacheSyncRV, rv)
					} else {
						// Without consisten reads cache is not synced
						assert.LessOrEqual(t, rv, lastResourceWriteRV)
					}
				} else {
					// Etcd always points to newest RV
					assert.Equal(t, consistentRV, rv)
				}
			},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			out := &example.PodList{}
			opts := storage.ListOptions{
				Recursive:       true,
				ResourceVersion: tc.requestRV,
				Predicate: storage.SelectionPredicate{
					Label:    labels.Everything(),
					Field:    fields.Everything(),
					Continue: tc.continueToken,
				},
			}
			err = store.GetList(ctx, "/pods/empty", opts, out)
			require.NoError(t, err)

			parsedOutRV, err := strconv.Atoi(out.ResourceVersion)
			require.NoError(t, err)
			tc.validateResponseRV(t, parsedOutRV)
		})
		// Update RV on each read to test multiple reads for consistent RV.
		// 每轮读后都推进 RV，确保后续用例验证的是"新的"一致点而非上一轮残留。
		increaseRV(ctx, t)
		consistentRV++
	}
}

// seedMultiLevelData creates a set of keys with a multi-level structure, returning a resourceVersion
// from before any were created along with the full set of objects that were persisted
// created 按创建顺序返回（含随后被删除的 baz），便于测试用例按下标引用。长度固定为 6。
func seedMultiLevelData(ctx context.Context, store storage.Interface) (initialRV string, created []*example.Pod, updated *example.Pod, err error) {
	// Setup storage with the following structure:
	//  /
	//   - first/
	//  |         - bar
	//  |
	//   - second/
	//  |         - bar
	//  |         - foo
	//  |         - [deleted] baz
	//  |
	//   - third/
	//  |         - barfoo
	//  |         - foo
	barFirst := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "first", Name: "bar"}}
	barSecond := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "second", Name: "bar"}}
	fooSecond := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "second", Name: "foo"}}
	bazSecond := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "second", Name: "baz"}}
	barfooThird := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "third", Name: "barfoo"}}
	fooThird := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "third", Name: "foo"}}

	preset := []struct {
		key       string
		obj       *example.Pod
		storedObj *example.Pod
	}{
		{
			key: computePodKey(barFirst),
			obj: barFirst,
		},
		{
			key: computePodKey(barSecond),
			obj: barSecond,
		},
		{
			key: computePodKey(fooSecond),
			obj: fooSecond,
		},
		{
			key: computePodKey(barfooThird),
			obj: barfooThird,
		},
		{
			key: computePodKey(fooThird),
			obj: fooThird,
		},
		{
			key: computePodKey(bazSecond),
			obj: bazSecond,
		},
	}

	// we want to figure out the resourceVersion before we create anything
	// 先记录"任何写入之前"的 RV，作为 match=Exact 请求过旧版本时的基准。
	initialList := &example.PodList{}
	if err := store.GetList(ctx, "/pods/", storage.ListOptions{Predicate: storage.Everything, Recursive: true}, initialList); err != nil {
		return "", nil, nil, fmt.Errorf("failed to determine starting resourceVersion: %w", err)
	}
	initialRV = initialList.ResourceVersion

	for i, ps := range preset {
		preset[i].storedObj = &example.Pod{}
		err := store.Create(ctx, ps.key, ps.obj, preset[i].storedObj, 0)
		if err != nil {
			return "", nil, nil, fmt.Errorf("failed to create object: %w", err)
		}
	}

	// For barFirst, we first create it with key /pods/first/bar and then we update
	// it by changing its spec.nodeName. The point of doing this is to be able to
	// test that if a pod with key /pods/first/bar is in fact returned, the returned
	// pod is the updated one (i.e. with spec.nodeName changed).
	updated = &example.Pod{}
	if err := store.GuaranteedUpdate(ctx, computePodKey(barFirst), updated, true, nil,
		func(input runtime.Object, _ storage.ResponseMeta) (output runtime.Object, ttl *uint64, err error) {
			pod := input.(*example.Pod).DeepCopy()
			pod.Spec.NodeName = "fakeNode"
			return pod, nil, nil
		}, nil); err != nil {
		return "", nil, nil, fmt.Errorf("failed to update object: %w", err)
	}

	// We now delete bazSecond provided it has been created first. We do this to enable
	// testing cases that had an object exist initially and then was deleted and how this
	// would be reflected in responses of different calls.
	// 删除 baz 是为了覆盖"曾存在后又被删除"的对象在 List/Get 中的表现。
	storedObj := &example.Pod{}
	if err := store.Delete(ctx, computePodKey(bazSecond), storedObj, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{}); err != nil {
		return "", nil, nil, fmt.Errorf("failed to delete object: %w", err)
	}

	for _, item := range preset {
		created = append(created, item.storedObj)
	}
	return initialRV, created, updated, nil
}

// RunTestGetListNonRecursive 校验非递归 List（Recursive=false）只作用于单个 key，
// 并且仍然遵守 resourceVersion / resourceVersionMatch 的组合语义。
func RunTestGetListNonRecursive(ctx context.Context, t *testing.T, increaseRV IncreaseRVFunc, store storage.Interface) {
	key, prevStoredObj := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}})
	prevRV, _ := strconv.Atoi(prevStoredObj.ResourceVersion)

	// 更新一次，制造"前一版本"与"当前版本"两个可区分的快照。
	storedObj := &example.Pod{}
	if err := store.GuaranteedUpdate(ctx, key, storedObj, false, nil,
		func(_ runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			newPod := prevStoredObj.DeepCopy()
			newPod.Annotations = map[string]string{"version": "second"}
			return newPod, nil, nil
		}, nil); err != nil {
		t.Fatalf("update failed: %v", err)
	}
	objRV, _ := strconv.Atoi(storedObj.ResourceVersion)
	// Increase RV to test consistent List.
	// 再推高一次全局 RV，确保"一致读"不会退化成直接返回最后一次写入的版本。
	increaseRV(ctx, t)

	// 注意：exact 与 notOlderThan 对同一 RV 的期望不同——exact 必须返回该 RV 的快照，
	// notOlderThan 则允许返回更新的版本，故用 expectedAlternatives 表达多解。
	tests := []struct {
		name                 string
		key                  string
		pred                 storage.SelectionPredicate
		expectedOut          []example.Pod
		expectedAlternatives [][]example.Pod
		rv                   string
		rvMatch              metav1.ResourceVersionMatch
		expectRVTooLarge     bool
	}{{
		name:        "existing key",
		key:         key,
		pred:        storage.Everything,
		expectedOut: []example.Pod{*storedObj},
	}, {
		name:                 "existing key, resourceVersion=0",
		key:                  key,
		pred:                 storage.Everything,
		expectedAlternatives: [][]example.Pod{{}, {*prevStoredObj}, {*storedObj}},
		rv:                   "0",
	}, {
		name:                 "existing key, resourceVersion=0, resourceVersionMatch=notOlderThan",
		key:                  key,
		pred:                 storage.Everything,
		expectedAlternatives: [][]example.Pod{{}, {*prevStoredObj}, {*storedObj}},
		rv:                   "0",
		rvMatch:              metav1.ResourceVersionMatchNotOlderThan,
	}, {
		name:        "existing key, resourceVersion=current",
		key:         key,
		pred:        storage.Everything,
		expectedOut: []example.Pod{*storedObj},
		rv:          fmt.Sprintf("%d", objRV),
	}, {
		name:        "existing key, resourceVersion=current, resourceVersionMatch=notOlderThan",
		key:         key,
		pred:        storage.Everything,
		expectedOut: []example.Pod{*storedObj},
		rv:          fmt.Sprintf("%d", objRV),
		rvMatch:     metav1.ResourceVersionMatchNotOlderThan,
	}, {
		name:                 "existing key, resourceVersion=previous, resourceVersionMatch=notOlderThan",
		key:                  key,
		pred:                 storage.Everything,
		expectedAlternatives: [][]example.Pod{{*prevStoredObj}, {*storedObj}},
		rv:                   fmt.Sprintf("%d", prevRV),
		rvMatch:              metav1.ResourceVersionMatchNotOlderThan,
	}, {
		name:        "existing key, resourceVersion=current, resourceVersionMatch=exact",
		key:         key,
		pred:        storage.Everything,
		expectedOut: []example.Pod{*storedObj},
		rv:          fmt.Sprintf("%d", objRV),
		rvMatch:     metav1.ResourceVersionMatchExact,
	}, {
		name:        "existing key, resourceVersion=previous, resourceVersionMatch=exact",
		key:         key,
		pred:        storage.Everything,
		expectedOut: []example.Pod{*prevStoredObj},
		rv:          fmt.Sprintf("%d", prevRV),
		rvMatch:     metav1.ResourceVersionMatchExact,
	}, {
		name:             "existing key, resourceVersion=too high",
		key:              key,
		pred:             storage.Everything,
		expectedOut:      []example.Pod{*storedObj},
		rv:               strconv.FormatInt(math.MaxInt64, 10),
		expectRVTooLarge: true,
	}, {
		name:        "non-existing key",
		key:         "/pods/non-existing",
		pred:        storage.Everything,
		expectedOut: []example.Pod{},
	}, {
		name: "with matching pod name",
		key:  "/pods/non-existing",
		pred: storage.SelectionPredicate{
			Label: labels.Everything(),
			Field: fields.ParseSelectorOrDie("metadata.name!=" + storedObj.Name),
			GetAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
				pod := obj.(*example.Pod)
				return nil, fields.Set{"metadata.name": pod.Name}, nil
			},
		},
		expectedOut: []example.Pod{},
	}, {
		name: "existing key, resourceVersion=current, with not matching pod name",
		key:  key,
		pred: storage.SelectionPredicate{
			Label: labels.Everything(),
			Field: fields.ParseSelectorOrDie("metadata.name!=" + storedObj.Name),
			GetAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
				pod := obj.(*example.Pod)
				return nil, fields.Set{"metadata.name": pod.Name}, nil
			},
		},
		expectedOut: []example.Pod{},
		rv:          fmt.Sprintf("%d", objRV),
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// For some asynchronous implementations of storage interface (in particular watchcache),
			// certain requests may impact result of further requests. As an example, if we first
			// ensure that watchcache is synchronized up to ResourceVersion X (using Get/List requests
			// with NotOlderThan semantic), the further requests (even specifying earlier resource
			// version) will also return the result synchronized to at least ResourceVersion X.
			// By parallelizing test cases we ensure that the order in which test cases are defined
			// doesn't automatically preclude some scenarios from happening.
			// 并行化避免用例按定义顺序互相预热缓存。
			t.Parallel()

			out := &example.PodList{}
			storageOpts := storage.ListOptions{
				ResourceVersion:      tt.rv,
				ResourceVersionMatch: tt.rvMatch,
				Predicate:            tt.pred,
				Recursive:            false,
			}
			err := store.GetList(ctx, tt.key, storageOpts, out)

			if tt.expectRVTooLarge {
				if err == nil || !storage.IsTooLargeResourceVersion(err) {
					t.Errorf("%s: expecting resource version too high error, but get: %s", tt.name, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("GetList failed: %v", err)
			}
			if len(out.ResourceVersion) == 0 {
				t.Errorf("%s: unset resourceVersion", tt.name)
			}

			if tt.expectedAlternatives == nil {
				expectNoDiff(t, "incorrect list pods", tt.expectedOut, out.Items)
			} else {
				ExpectContains(t, "incorrect list pods", toInterfaceSlice(tt.expectedAlternatives), out.Items)
			}
		})
	}
}

// RunTestGetListRecursivePrefix tests how recursive parameter works for object keys that are prefixes of each other.
// 关键点：foo 是 foobar 的前缀，非递归 List 必须按"key 边界"精确匹配，
// 不能把 /pods/test-ns/foo 当成 /pods/test-ns/foobar 的父路径。
func RunTestGetListRecursivePrefix(ctx context.Context, t *testing.T, store storage.Interface) {
	fooKey, fooObj := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}})
	fooBarKey, fooBarObj := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foobar", Namespace: "test-ns"}})
	_, otherNamespaceObj := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "baz", Namespace: "test-ns2"}})
	lastRev := otherNamespaceObj.ResourceVersion

	tests := []struct {
		name        string
		key         string
		recursive   bool
		expectedOut []example.Pod
	}{
		{
			name:        "NonRecursive on resource prefix doesn't return any objects",
			key:         "/pods/",
			recursive:   false,
			expectedOut: []example.Pod{},
		},
		{
			name:        "Recursive on resource prefix returns all objects",
			key:         "/pods/",
			recursive:   true,
			expectedOut: []example.Pod{*fooObj, *fooBarObj, *otherNamespaceObj},
		},
		{
			name:        "NonRecursive on namespace prefix doesn't return any objects",
			key:         "/pods/test-ns/",
			recursive:   false,
			expectedOut: []example.Pod{},
		},
		{
			name:        "Recursive on resource prefix returns objects in the namespace",
			key:         "/pods/test-ns/",
			recursive:   true,
			expectedOut: []example.Pod{*fooObj, *fooBarObj},
		},
		{
			name:        "NonRecursive on object key (prefix) returns object and no other objects with the same prefix",
			key:         fooKey,
			recursive:   false,
			expectedOut: []example.Pod{*fooObj},
		},
		{
			name:        "Recursive on object key (prefix) doesn't return anything",
			key:         fooKey,
			recursive:   true,
			expectedOut: []example.Pod{},
		},
		{
			name:        "NonRecursive on object key (no-prefix) return object",
			key:         fooBarKey,
			recursive:   false,
			expectedOut: []example.Pod{*fooBarObj},
		},
		{
			name:        "Recursive on object key (no-prefix) doesn't return anything",
			key:         fooBarKey,
			recursive:   true,
			expectedOut: []example.Pod{},
		},
	}

	listTypes := []struct {
		name            string
		ResourceVersion string
		Match           metav1.ResourceVersionMatch
	}{
		{
			name:            "Exact",
			ResourceVersion: lastRev,
			Match:           metav1.ResourceVersionMatchExact,
		},
		{
			name:            "Consistent",
			ResourceVersion: "",
		},
		{
			name:            "NotOlderThan",
			ResourceVersion: "0",
			Match:           metav1.ResourceVersionMatchNotOlderThan,
		},
	}

	for _, listType := range listTypes {
		t.Run(listType.name, func(t *testing.T) {
			// 同一组前缀语义必须在 Exact / 一致读 / NotOlderThan 三种模式下保持一致。
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					out := &example.PodList{}
					storageOpts := storage.ListOptions{
						ResourceVersion:      listType.ResourceVersion,
						ResourceVersionMatch: listType.Match,
						Recursive:            tt.recursive,
						Predicate:            storage.Everything,
					}
					err := store.GetList(ctx, tt.key, storageOpts, out)
					if err != nil {
						t.Fatalf("GetList failed: %v", err)
					}
					expectNoDiff(t, "incorrect list pods", tt.expectedOut, out.Items)
				})
			}
		})
	}
}

// errorInjector decides, per object storage key, whether transformation should
// fail and with which error. Returning nil delegates to default behavior.
// 注入点放在 transformer 而非存储层，这样无需改动后端即可模拟"损坏对象"。
type errorInjector func(key string) error

// idOf extracts the numeric suffix from an object key like ".../foo-000002" -> 2.
// 用例按 id 决定哪些对象损坏，因此需要从 key 反解出编号。
func idOf(t *testing.T, key string) int {
	t.Helper()
	name := key[strings.LastIndex(key, "/")+1:]
	n, err := strconv.Atoi(strings.TrimPrefix(name, "foo-"))
	if err != nil {
		t.Fatalf("failed to parse the object id from key %q: %v", key, err)
	}
	return n
}

// errInjectingTransformer wraps an existing transformer and lets an errorInjector
// fail transformation for selected keys on read. Writes (TransformToStorage)
// delegate to the embedded transformer.
// 仅拦截读取路径：写入必须继续成功，否则无法先准备好"坏数据"。
type errInjectingTransformer struct {
	value.Transformer
	inject errorInjector
}

// TransformFromStorage 在真正解码前按 key 注入错误；dataCtx 的 AuthenticatedData
// 携带对象 key，是唯一能定位"哪个对象"的信息来源。
func (t *errInjectingTransformer) TransformFromStorage(ctx context.Context, data []byte, dataCtx value.Context) (out []byte, stale bool, err error) {
	if injErr := t.inject(string(dataCtx.AuthenticatedData())); injErr != nil {
		return nil, false, injErr
	}
	return t.Transformer.TransformFromStorage(ctx, data, dataCtx)
}

// newErrInjectingModifier 把 errorInjector 包装成 TransformerModifier，
// 供 store.UpdateTransformer 在测试期间临时替换 transformer。
func newErrInjectingModifier(inject errorInjector) TransformerModifier {
	return func(orig value.Transformer) value.Transformer {
		return &errInjectingTransformer{Transformer: orig, inject: inject}
	}
}

// RunTestGetListWithErrorAggregation tests aggregation of errors while the list operation is in progress
// 校验错误聚合：损坏对象（corruptErr）应被收集进 API Status 的 causes，
// 而其他错误必须立即中止；聚合数量有硬上限（100），超限后追加哨兵条目。
func RunTestGetListWithErrorAggregation(ctx context.Context, t *testing.T, store InterfaceWithTransformerOverride, corruptErr error) {
	// the test creates n objects and assigns each object unique id i ie. 1 <= i <= n
	objNameFn := func(id int) string {
		// pad with leading zeros so that for i, j where i < j, object
		// foo-{j} follows object foo-{i} in lexicographical order
		return fmt.Sprintf("foo-%06d", id)
	}
	keyFn := func(prefix string, id int) string {
		return fmt.Sprintf("%s%s", prefix, objNameFn(id))
	}

	// errUnexpected is a non-corrupt transform error. The aggregator treats it as
	// fatal and aborts the list, unlike corruptErr which is collected.
	// 这是"聚合只针对损坏对象"这一不变量的判别器。
	errUnexpected := errors.New("bits flipped")

	tests := []struct {
		name string
		// number of Pod objects to be created when the test starts, the
		// objects are named as "foo-{i}" where 1 <= i <= n
		n int
		// inject decides, per object key, whether transformation fails and with
		// which error. Returning nil means the object transforms normally.
		inject func(t *testing.T, key string) error
		// verifies the result from GetList
		// list: result of GetList operation is saved into list
		// err: error returned from GetList
		verifier func(t *testing.T, prefix string, list *example.PodList, err error)
	}{
		{
			name: "first error is an unexpected error, no aggregation expected",
			// we initially create n=7 objects with ids {1, 2, 3 ... 7}, they are put in the following disjoint sets:
			// - good: {1, 3, 5, 7}, these objects will never become corrupt
			// - unexpected: {2}, this object is marked to yield an unexpected error (not corruptObjErr)
			// - corrupt: {4, 6}, these objects are marked to become corrupt
			n: 7,
			inject: func(t *testing.T, key string) error {
				switch id := idOf(t, key); {
				case id == 2:
					return errUnexpected
				case id%2 == 0:
					return corruptErr
				default:
					return nil
				}
			},
			// the following sequence of events are expected to occur in order while retrieving the n objects:
			//
			// -- |- 1: no error, successfully decoded
			//    |- 2: unexpected error, GetList aborts
			//
			//  a) GetList encounters an unexpected (not corruptObjErr)
			//  error while retrieving {2} and immediately aborts
			//  b) GetList successfully decodes {1}
			verifier: func(t *testing.T, _ string, list *example.PodList, err error) {
				// a) the error returned from GetList should be a bare
				// storage.InternalError, not wrapped — proves no aggregation
				// nolint:errorlint // the aggregator should return the error as is
				if _, ok := err.(storage.InternalError); !ok {
					t.Fatalf("expected the error to be %T, but got: %#v", storage.InternalError{}, err)
				}

				// b) GetList should successfully decode object 1 from the good set
				if want, got := 1, len(list.Items); want != got {
					t.Fatalf("expected the list to have %d item(s), but got: %d", want, got)
				}
				if want, got := objNameFn(1), list.Items[0].Name; want != got {
					t.Errorf("expected an object name of %q, but got: %q", want, got)
				}
			},
		},
		{
			name: "feature enabled, should aggregate corrupt object errors",

			// we initially create n=7 objects with ids {1, 2, 3 ... 7}, they are put in the following disjoint sets:
			// - good: {1, 3, 5, 7}, these objects will never become corrupt
			// - corrupt: {2, 4, 6}, these objects are marked to become corrupt
			n: 7,
			inject: func(t *testing.T, key string) error {
				if idOf(t, key)%2 == 0 {
					return corruptErr
				}
				return nil
			},
			// while retrieving the n objects, we expect the following from GetList:
			//  a) GetList encounters corruptObjErr while retrieving objects in the corrupt set
			//  b) GetList successfully decodes object(s) in the good set
			verifier: func(t *testing.T, prefix string, list *example.PodList, listErr error) {
				// the error returned from GetList should be an API status object
				var statusGot apierrors.APIStatus
				if !errors.As(listErr, &statusGot) {
					t.Fatalf("expected an API status error object, but got: %v", listErr)
				}

				details := statusGot.Status().Details
				corrupt := []string{keyFn(prefix, 2), keyFn(prefix, 4), keyFn(prefix, 6)}
				// a) all the corruptObjErr errors should be aggregated
				if details == nil || len(details.Causes) != len(corrupt) {
					t.Fatalf("expected the API status to include the corrupt object errors, but got: %v", details)
				}
				for i, key := range corrupt {
					if want, got := key, details.Causes[i].Field; want != got {
						t.Errorf("expected an object name of %q, but got: %q", want, got)
					}
				}

				// b) GetList should successfully decode all objects in the good set
				good := []int{1, 3, 5, 7}
				if want, got := len(good), len(list.Items); want != got {
					t.Fatalf("expected the list to have %d item(s), but got: %d", want, got)
				}
				for i, id := range good {
					if want, got := objNameFn(id), list.Items[i].Name; want != got {
						t.Errorf("expected an object name of %q, but got: %q", want, got)
					}
				}
			},
		},
		{
			name: "feature enabled, aggregation should abort as soon as it encounters an unexpected error",

			// we initially create n=7 objects with ids {1, 2, 3 ... 7}, they are put in the following disjoint sets:
			// - good: {1, 3, 5, 7}, these objects will never become corrupt
			// - unexpected: {4}, this object is marked to yield an unexpected error (not corruptObjErr)
			// - corrupt: {2, 6}, these objects are marked to become corrupt
			n: 7,
			inject: func(t *testing.T, key string) error {
				switch id := idOf(t, key); {
				case id == 4:
					return errUnexpected
				case id%2 == 0:
					return corruptErr
				default:
					return nil
				}
			},
			// the following sequence of events are expected to occur in order while retrieving the n objects:
			//
			// --|- 1: no error, successfully decoded
			//   |- 2: yields an expected corruptObjErr, GetList aggregates this error
			//   |- 3: no error, successfully decoded
			//   |- 4: unexpected error, GetList aborts
			//
			//  a) GetList encounters a corruptObjErr error while retrieving {2} in the corrupt set
			//  b) GetList encounters an unexpected (not corruptObjErr) error while
			//  retrieving {4} in the unexpected set, and immediately aborts
			//  c) GetList successfully decodes {1,3} in the good set before it aborts
			verifier: func(t *testing.T, prefix string, list *example.PodList, err error) {
				// the error returned from GetList should be an API status object
				var statusGot apierrors.APIStatus
				if !errors.As(err, &statusGot) {
					t.Fatalf("expected an API status error object, but got: %v", err)
				}

				details := statusGot.Status().Details
				// a) the corrupt error from {2} plus the unexpected abort error from {4}
				// should both be surfaced as causes
				if details == nil || len(details.Causes) != 2 {
					t.Fatalf("expected 2 causes (1 corrupt + 1 abort), but got: %v", details)
				}
				if want, got := keyFn(prefix, 2), details.Causes[0].Field; want != got {
					t.Errorf("expected corrupt object key %q, but got: %q", want, got)
				}
				if want, got := metav1.CauseTypeUnexpectedServerResponse, details.Causes[1].Type; want != got {
					t.Errorf("expected abort cause type %q, but got: %q", want, got)
				}

				// b) verify that GetList successfully decodes objects before the abort
				ids := []int{1, 3}
				if want, got := len(ids), len(list.Items); want != got {
					t.Errorf("expected GetList to successfully decode %d object(s), but got: %d", want, got)
				}
				for i, id := range ids {
					if want, got := objNameFn(id), list.Items[i].Name; want != got {
						t.Errorf("expected an object name of %q, but got: %q", want, got)
					}
				}
			},
		},
		{
			name: "feature enabled, error aggregation should not exceed the maximum limit",

			// aggregation limit is currently hard coded to 100
			// we initially create n=210 objects with ids {1, 2, 3 ... 210}, they are put in the following disjoint sets:
			// - good: {1, 3, 5 ... 195, 197, 199, ... 207, 209}, these 105 objects will never become corrupt
			// - corrupt: {2, 4, 6 ... 196, 198, 200, ... 208, 210}, these 105 objects are marked to become corrupt
			n: 210,
			inject: func(t *testing.T, key string) error {
				if idOf(t, key)%2 == 0 {
					return corruptErr
				}
				return nil
			},
			// while listing the n objects, we expect the following:
			//  a) GetList continues to aggregate the corruptObjErr errors
			//  until it reaches the maximum limit, and then it immediately aborts
			//  b) GetList successfully decodes the first 100 objects in the good set into list
			verifier: func(t *testing.T, _ string, list *example.PodList, err error) {
				// must match the aggregation limit that etcd3 configures via
				// corruptObjErrAggregatorFactory when the feature is enabled
				limit := 100
				var statusGot apierrors.APIStatus
				if !errors.As(err, &statusGot) {
					t.Fatalf("expected an API status error object, but got: %v", err)
				}
				details := statusGot.Status().Details
				// a) the returned error should contain limit+1 causes:
				// limit corrupt errors + the sentinel "too many" entry
				if want := limit + 1; details == nil || len(details.Causes) != want {
					t.Fatalf("expected %d causes (limit + sentinel), but got: %d", want, len(details.Causes))
				}
				want := metav1.StatusCause{
					Type:    metav1.CauseTypeTooMany,
					Message: "too many errors, the list is truncated",
				}
				if got := details.Causes[limit]; !cmp.Equal(want, got) {
					t.Errorf("expected the sentinel error, diff: %s", cmp.Diff(want, got))
				}

				// b) before the limit is hit and GetList aborts, it successfully decodes 100 objects
				if want, got := limit, len(list.Items); want != got {
					t.Errorf("expected GetList to successfully decode %d object(s), but got: %d", want, got)
				}
			},
		},
		{
			name: "feature enabled, there are exactly 100 (error aggregation limit) corrupt errors",

			// aggregation limit is currently hard coded to 100
			// we initially create n=100 objects with ids {1, 2, 3 ... 100}, they are put in the following disjoint sets:
			// - good: {}, all the objects are marked to become corrupt
			// - corrupt: {1, 2, 3 ... 99, 100}, these objects are marked to become corrupt
			n:      100,
			inject: func(t *testing.T, key string) error { return corruptErr },
			//  while listing the n objects, we expect the following:
			//  a) GetList continues to aggregate the corruptObjErr errors
			//  until it reaches the maximum limit, and then it immediately aborts
			//  b) GetList successfully decodes zero objects
			verifier: func(t *testing.T, _ string, list *example.PodList, err error) {
				// must match the aggregation limit that etcd3 configures via
				// corruptObjErrAggregatorFactory when the feature is enabled
				limit := 100
				var statusGot apierrors.APIStatus
				if !errors.As(err, &statusGot) {
					t.Fatalf("expected an API status error object, but got: %v", err)
				}
				// a) the returned error should contain limit+1 causes:
				// limit corrupt errors + the sentinel "too many" entry
				details := statusGot.Status().Details
				if want := limit + 1; details == nil || len(details.Causes) != want {
					t.Fatalf("expected %d causes (limit + sentinel), but got: %d", want, len(details.Causes))
				}
				want := metav1.StatusCause{
					Type:    metav1.CauseTypeTooMany,
					Message: "too many errors, the list is truncated",
				}
				if got := details.Causes[limit]; !cmp.Equal(want, got) {
					t.Errorf("expected the sentinel error, diff: %s", cmp.Diff(want, got))
				}

				// b) all objects are corrupt, so GetList should decode 0 objects
				if want, got := 0, len(list.Items); want != got {
					t.Errorf("expected GetList to successfully decode %d object(s), but got: %d", want, got)
				}
			},
		},
	}

	for i, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ns := fmt.Sprintf("ns-%d", i)
			prefix := fmt.Sprintf("/pods/%s/", ns)

			// Step 1: add N objects to the store foo-{1 ... n}
			// we construct the names of the objects in a way that ensures
			// that the order of creation is also the lexicographical
			// order by which etcd List operation returns the objects
			// 零填充保证 id 顺序与字典序一致，从而遍历顺序可预测。
			for j := 1; j <= test.n; j++ {
				obj := &example.Pod{ObjectMeta: metav1.ObjectMeta{
					Name:      objNameFn(j),
					Namespace: ns,
				}}
				if err := store.Create(ctx, computePodKey(obj), obj, &example.Pod{}, 0); err != nil {
					t.Fatalf("Create failed: %v", err)
				}
			}

			// step 2: list the N objects, we expect no error
			// 先做一次基线 List，确认损坏注入前数据完整。
			out := &example.PodList{}
			storageOpts := storage.ListOptions{
				Predicate: storage.Everything,
				Recursive: true,
			}
			err := store.GetList(ctx, prefix, storageOpts, out)
			if err != nil {
				t.Fatalf("GetList failed with unexpected error: %v", err)
			}
			if want, got := test.n, len(out.Items); want != got {
				t.Fatalf("Expected length: %d, but got: %d", want, got)
			}

			// step 3: change the transformer so the marked objects appear corrupt
			// 用 defer 恢复，避免损坏状态泄漏到后续子测试。
			revertTransformer := store.UpdateTransformer(newErrInjectingModifier(func(key string) error {
				return test.inject(t, key)
			}))
			defer revertTransformer()

			// step 4: invoke GetList again, this time it should encounter the corrupt object(s)
			out = &example.PodList{}
			err = store.GetList(ctx, prefix, storageOpts, out)
			if err == nil {
				t.Fatalf("Expected GetList to return error")
			}

			// step 5: verify what we expect from GetList
			test.verifier(t, prefix, out, err)
		})
	}
}

// RunTestGetListWithoutErrorAggregation tests that when the AllowUnsafeMalformedObjectDeletion
// feature is disabled, GetList maintains backward compatibility by aborting on the first error.
// 兼容性要求：未启用特性时行为必须与旧版本完全一致，返回裸 InternalError 而非聚合的 Status。
func RunTestGetListWithoutErrorAggregation(ctx context.Context, t *testing.T, store InterfaceWithTransformerOverride, corruptErr error) {
	prefix := "/pods/ns/"
	objNameFn := func(id int) string {
		return fmt.Sprintf("foo-%06d", id)
	}
	keyFn := func(id int) string {
		return fmt.Sprintf("%s%s", prefix, objNameFn(id))
	}

	// Step 1: create 7 objects, where even-numbered ones are corrupt
	// - good: {1, 3, 5, 7}
	// - corrupt: {2, 4, 6}
	n := 7
	for i := 1; i <= n; i++ {
		obj := &example.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:      objNameFn(i),
			Namespace: "ns",
		}}
		if err := store.Create(ctx, computePodKey(obj), obj, &example.Pod{}, 0); err != nil {
			t.Fatalf("Create failed: %v", err)
		}
	}

	// Step 2: list all objects, expect no error before corruption
	out := &example.PodList{}
	storageOpts := storage.ListOptions{
		Predicate: storage.Everything,
		Recursive: true,
	}
	err := store.GetList(ctx, prefix, storageOpts, out)
	if err != nil {
		t.Fatalf("GetList failed with unexpected error: %v", err)
	}
	if want, got := n, len(out.Items); want != got {
		t.Fatalf("Expected length: %d, but got: %d", want, got)
	}

	// Step 3: corrupt the transformer so even-numbered objects fail to decode
	revertTransformer := store.UpdateTransformer(newErrInjectingModifier(func(key string) error {
		if idOf(t, key)%2 == 0 {
			return corruptErr
		}
		return nil
	}))
	defer revertTransformer()

	// Step 4: list again, expect GetList to abort on the first corrupt object
	out = &example.PodList{}
	err = store.GetList(ctx, prefix, storageOpts, out)
	if err == nil {
		t.Fatalf("Expected GetList to return error")
	}

	// Verify: the error should be a bare storage.InternalError (no aggregation)
	// nolint:errorlint // the aggregator should return the error as is
	intErr, ok := err.(storage.InternalError)
	if !ok {
		t.Fatalf("expected the error to be %T, but got: %#v", storage.InternalError{}, err)
	}
	if want, got := fmt.Sprintf(`unable to transform key "%s"`, keyFn(2)), intErr.Error(); !strings.HasPrefix(got, want) {
		t.Errorf("expected the error to start with %q, but got: %v", want, got)
	}

	// Verify: GetList successfully decodes {1} before aborting
	if want, got := 1, len(out.Items); want != got {
		t.Errorf("expected the list to have %d item(s), but got: %d", want, got)
	}
	if want, got := objNameFn(1), out.Items[0].Name; want != got {
		t.Errorf("expected an object name of %q, but got: %q", want, got)
	}
}

// CallsValidation 由调用方提供，用于观察一次 List 的页大小与实际处理的底层对象数；
// 二者不相等时通常意味着过滤或分页放大了扫描量。
type CallsValidation func(t *testing.T, pageSize, estimatedProcessedObjects uint64)

// RunTestListContinuation 校验分页语义：limit 只在一致读下生效，
// 且翻页时必须沿用首页返回的 RV，否则不同页可能来自不同快照。
func RunTestListContinuation(ctx context.Context, t *testing.T, store storage.Interface, validation CallsValidation) {
	// Setup storage with the following structure:
	//  /
	//   - first/
	//  |         - bar
	//  |
	//   - second/
	//  |         - bar
	//  |         - foo
	barFirst := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "first", Name: "bar"}}
	barSecond := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "second", Name: "bar"}}
	fooSecond := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "second", Name: "foo"}}
	preset := []struct {
		key       string
		obj       *example.Pod
		storedObj *example.Pod
	}{
		{
			key: computePodKey(barFirst),
			obj: barFirst,
		},
		{
			key: computePodKey(barSecond),
			obj: barSecond,
		},
		{
			key: computePodKey(fooSecond),
			obj: fooSecond,
		},
	}

	var currentRV string
	for i, ps := range preset {
		preset[i].storedObj = &example.Pod{}
		err := store.Create(ctx, ps.key, ps.obj, preset[i].storedObj, 0)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
		currentRV = preset[i].storedObj.ResourceVersion
	}

	// test continuations
	out := &example.PodList{}
	pred := func(limit int64, continueValue string) storage.SelectionPredicate {
		return storage.SelectionPredicate{
			Limit:    limit,
			Continue: continueValue,
			Label:    labels.Everything(),
			Field:    fields.Everything(),
			GetAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
				pod := obj.(*example.Pod)
				return nil, fields.Set{"metadata.name": pod.Name}, nil
			},
		}
	}
	options := storage.ListOptions{
		// Limit is ignored when ResourceVersion is set to 0.
		// Set it to consistent read.
		ResourceVersion: "",
		Predicate:       pred(1, ""),
		Recursive:       true,
	}
	if err := store.GetList(ctx, "/pods/", options, out); err != nil {
		t.Fatalf("Unable to get initial list: %v", err)
	}
	if len(out.Continue) == 0 {
		t.Fatalf("No continuation token set")
	}
	expectNoDiff(t, "incorrect first page", []example.Pod{*preset[0].storedObj}, out.Items)
	if out.ResourceVersion != currentRV {
		t.Errorf("Expect output.ResourceVersion = %s, but got %s", currentRV, out.ResourceVersion)
	}
	if validation != nil {
		validation(t, 1, 1)
	}

	continueFromSecondItem := out.Continue

	// no limit, should get two items
	out = &example.PodList{}
	options = storage.ListOptions{
		// ResourceVersion should be unset when setting continuation token.
		ResourceVersion: "",
		Predicate:       pred(0, continueFromSecondItem),
		Recursive:       true,
	}
	if err := store.GetList(ctx, "/pods/", options, out); err != nil {
		t.Fatalf("Unable to get second page: %v", err)
	}
	if len(out.Continue) != 0 {
		t.Fatalf("Unexpected continuation token set")
	}
	key, rv, err := storage.DecodeContinue(continueFromSecondItem, "/pods/")
	t.Logf("continue token was %d %s %v", rv, key, err)
	expectNoDiff(t, "incorrect second page", []example.Pod{*preset[1].storedObj, *preset[2].storedObj}, out.Items)
	if out.ResourceVersion != currentRV {
		t.Errorf("Expect output.ResourceVersion = %s, but got %s", currentRV, out.ResourceVersion)
	}
	if validation != nil {
		validation(t, 0, 2)
	}

	// limit, should get two more pages
	out = &example.PodList{}
	options = storage.ListOptions{
		// ResourceVersion should be unset when setting continuation token.
		ResourceVersion: "",
		Predicate:       pred(1, continueFromSecondItem),
		Recursive:       true,
	}
	if err := store.GetList(ctx, "/pods/", options, out); err != nil {
		t.Fatalf("Unable to get second page: %v", err)
	}
	if len(out.Continue) == 0 {
		t.Fatalf("No continuation token set")
	}
	expectNoDiff(t, "incorrect second page", []example.Pod{*preset[1].storedObj}, out.Items)
	if out.ResourceVersion != currentRV {
		t.Errorf("Expect output.ResourceVersion = %s, but got %s", currentRV, out.ResourceVersion)
	}
	if validation != nil {
		validation(t, 1, 1)
	}

	continueFromThirdItem := out.Continue

	out = &example.PodList{}
	options = storage.ListOptions{
		// ResourceVersion should be unset when setting continuation token.
		ResourceVersion: "",
		Predicate:       pred(1, continueFromThirdItem),
		Recursive:       true,
	}
	if err := store.GetList(ctx, "/pods/", options, out); err != nil {
		t.Fatalf("Unable to get second page: %v", err)
	}
	if len(out.Continue) != 0 {
		t.Fatalf("Unexpected continuation token set")
	}
	expectNoDiff(t, "incorrect third page", []example.Pod{*preset[2].storedObj}, out.Items)
	if out.ResourceVersion != currentRV {
		t.Errorf("Expect output.ResourceVersion = %s, but got %s", currentRV, out.ResourceVersion)
	}
	if validation != nil {
		validation(t, 1, 1)
	}
}

// RunTestListPaginationRareObject 校验"稀有对象"分页：1000 个对象中只有 1 个匹配，
// 实现必须扫描整个列表才能确定没有下一页，且 validation 能观察到实际扫描量。
func RunTestListPaginationRareObject(ctx context.Context, t *testing.T, store storage.Interface, validation CallsValidation) {
	podCount := 1000
	var pods []*example.Pod
	for i := 0; i < podCount; i++ {
		obj := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d", i)}}
		key := computePodKey(obj)
		storedObj := &example.Pod{}
		err := store.Create(ctx, key, obj, storedObj, 0)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
		pods = append(pods, storedObj)
	}

	out := &example.PodList{}
	options := storage.ListOptions{
		Predicate: storage.SelectionPredicate{
			Limit: 1,
			Label: labels.Everything(),
			Field: fields.OneTermEqualSelector("metadata.name", "pod-999"),
			GetAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
				pod := obj.(*example.Pod)
				return nil, fields.Set{"metadata.name": pod.Name}, nil
			},
		},
		Recursive: true,
	}
	if err := store.GetList(ctx, "/pods/", options, out); err != nil {
		t.Fatalf("Unable to get initial list: %v", err)
	}
	if len(out.Continue) != 0 {
		t.Errorf("Unexpected continuation token set")
	}
	if len(out.Items) != 1 || !reflect.DeepEqual(&out.Items[0], pods[999]) {
		t.Fatalf("Unexpected first page: %#v", out.Items)
	}
	if validation != nil {
		validation(t, 1, uint64(podCount))
	}
}

// RunTestListContinuationWithFilter 校验过滤 + 分页：limit 统计的是"匹配后"的对象数，
// 但底层可能需要多读若干条来凑够一页，validation 的第二个参数即反映这种放大。
func RunTestListContinuationWithFilter(ctx context.Context, t *testing.T, store storage.Interface, validation CallsValidation) {
	foo1 := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "1", Name: "foo"}}
	bar2 := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "2", Name: "bar"}} // this should not match
	foo3 := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "3", Name: "foo"}}
	foo4 := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "4", Name: "foo"}}
	preset := []struct {
		key       string
		obj       *example.Pod
		storedObj *example.Pod
	}{
		{
			key: computePodKey(foo1),
			obj: foo1,
		},
		{
			key: computePodKey(bar2),
			obj: bar2,
		},
		{
			key: computePodKey(foo3),
			obj: foo3,
		},
		{
			key: computePodKey(foo4),
			obj: foo4,
		},
	}

	var currentRV string
	for i, ps := range preset {
		preset[i].storedObj = &example.Pod{}
		err := store.Create(ctx, ps.key, ps.obj, preset[i].storedObj, 0)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
		currentRV = preset[i].storedObj.ResourceVersion
	}

	// the first list call should try to get 2 items from etcd (and only those items should be returned)
	// the field selector should result in it reading 3 items via the transformer
	// the chunking should result in 2 etcd Gets
	// there should be a continueValue because there is more data
	out := &example.PodList{}
	pred := func(limit int64, continueValue string) storage.SelectionPredicate {
		return storage.SelectionPredicate{
			Limit:    limit,
			Continue: continueValue,
			Label:    labels.Everything(),
			Field:    fields.OneTermNotEqualSelector("metadata.name", "bar"),
			GetAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
				pod := obj.(*example.Pod)
				return nil, fields.Set{"metadata.name": pod.Name}, nil
			},
		}
	}
	options := storage.ListOptions{
		// Limit is ignored when ResourceVersion is set to 0.
		// Set it to consistent read.
		ResourceVersion: "",
		Predicate:       pred(2, ""),
		Recursive:       true,
	}
	if err := store.GetList(ctx, "/pods/", options, out); err != nil {
		t.Errorf("Unable to get initial list: %v", err)
	}
	if len(out.Continue) == 0 {
		t.Errorf("No continuation token set")
	}
	expectNoDiff(t, "incorrect first page", []example.Pod{*preset[0].storedObj, *preset[2].storedObj}, out.Items)
	if out.ResourceVersion != currentRV {
		t.Errorf("Expect output.ResourceVersion = %s, but got %s", currentRV, out.ResourceVersion)
	}
	if validation != nil {
		validation(t, 2, 3)
	}

	// the rest of the test does not make sense if the previous call failed
	if t.Failed() {
		return
	}

	cont := out.Continue

	// the second list call should try to get 2 more items from etcd
	// but since there is only one item left, that is all we should get with no continueValue
	// both read counters should be incremented for the singular calls they make in this case
	out = &example.PodList{}
	options = storage.ListOptions{
		// ResourceVersion should be unset when setting continuation token.
		ResourceVersion: "",
		Predicate:       pred(2, cont),
		Recursive:       true,
	}
	if err := store.GetList(ctx, "/pods/", options, out); err != nil {
		t.Errorf("Unable to get second page: %v", err)
	}
	if len(out.Continue) != 0 {
		t.Errorf("Unexpected continuation token set")
	}
	expectNoDiff(t, "incorrect second page", []example.Pod{*preset[3].storedObj}, out.Items)
	if out.ResourceVersion != currentRV {
		t.Errorf("Expect output.ResourceVersion = %s, but got %s", currentRV, out.ResourceVersion)
	}
	if validation != nil {
		validation(t, 2, 1)
	}
}

// Compaction 由调用方提供，用于把存储的历史版本压缩到给定 RV；
// 套件借此验证过期 RV / continue token 的错误处理。
type Compaction func(ctx context.Context, t *testing.T, resourceVersion string)

// IncreaseRVFunc 由调用方提供，用于推进存储的全局 RV 而不写入对象，
// 以便区分"写入产生的 RV"与"当前一致点 RV"。
type IncreaseRVFunc func(ctx context.Context, t *testing.T) (revision int64)

// RunTestListInconsistentContinuation 校验 continue token 过期后的恢复路径：
// 实现应返回 API Status 并在其中附带一个"不一致但可用"的新 token，
// 客户端据此可以继续翻页，而不是从头开始。
func RunTestListInconsistentContinuation(ctx context.Context, t *testing.T, store storage.Interface, compaction Compaction) {
	// Setup storage with the following structure:
	//  /
	//   - first/
	//  |         - bar
	//  |
	//   - second/
	//  |         - bar
	//  |         - foo
	barFirst := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "first", Name: "bar"}}
	barSecond := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "second", Name: "bar"}}
	fooSecond := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "second", Name: "foo"}}
	preset := []struct {
		key       string
		obj       *example.Pod
		storedObj *example.Pod
	}{
		{
			key: computePodKey(barFirst),
			obj: barFirst,
		},
		{
			key: computePodKey(barSecond),
			obj: barSecond,
		},
		{
			key: computePodKey(fooSecond),
			obj: fooSecond,
		},
	}
	for i, ps := range preset {
		preset[i].storedObj = &example.Pod{}
		err := store.Create(ctx, ps.key, ps.obj, preset[i].storedObj, 0)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	pred := func(limit int64, continueValue string) storage.SelectionPredicate {
		return storage.SelectionPredicate{
			Limit:    limit,
			Continue: continueValue,
			Label:    labels.Everything(),
			Field:    fields.Everything(),
			GetAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
				pod := obj.(*example.Pod)
				return nil, fields.Set{"metadata.name": pod.Name}, nil
			},
		}
	}

	out := &example.PodList{}
	options := storage.ListOptions{
		ResourceVersion: "0",
		Predicate:       pred(1, ""),
		Recursive:       true,
	}
	if err := store.GetList(ctx, "/pods/", options, out); err != nil {
		t.Fatalf("Unable to get initial list: %v", err)
	}
	if len(out.Continue) == 0 {
		t.Fatalf("No continuation token set")
	}
	expectNoDiff(t, "incorrect first page", []example.Pod{*preset[0].storedObj}, out.Items)

	continueFromSecondItem := out.Continue

	// update /second/bar
	oldName := preset[2].obj.Name
	newPod := &example.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: oldName,
			Labels: map[string]string{
				"state": "new",
			},
		},
	}
	if err := store.GuaranteedUpdate(ctx, preset[2].key, preset[2].storedObj, false, nil,
		func(_ runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			return newPod, nil, nil
		}, newPod); err != nil {
		t.Fatalf("update failed: %v", err)
	}

	// compact to latest revision.
	// 压缩到最新版本，使旧 continue token 中携带的 RV 落在压缩点之前。
	lastRVString := preset[2].storedObj.ResourceVersion
	compaction(ctx, t, lastRVString)

	// The old continue token should have expired
	options = storage.ListOptions{
		ResourceVersion: "0",
		Predicate:       pred(0, continueFromSecondItem),
		Recursive:       true,
	}
	err := store.GetList(ctx, "/pods/", options, out)
	if err == nil {
		t.Fatalf("unexpected no error")
	}
	if !strings.Contains(err.Error(), "The provided continue parameter is too old ") {
		t.Fatalf("unexpected error message %v", err)
	}
	status, ok := err.(apierrors.APIStatus)
	if !ok {
		t.Fatalf("expect error of implements the APIStatus interface, got %v", reflect.TypeOf(err))
	}
	inconsistentContinueFromSecondItem := status.Status().ListMeta.Continue
	if len(inconsistentContinueFromSecondItem) == 0 {
		t.Fatalf("expect non-empty continue token")
	}

	out = &example.PodList{}
	options = storage.ListOptions{
		ResourceVersion: "0",
		Predicate:       pred(1, inconsistentContinueFromSecondItem),
		Recursive:       true,
	}
	if err := store.GetList(ctx, "/pods/", options, out); err != nil {
		t.Fatalf("Unable to get second page: %v", err)
	}
	if len(out.Continue) == 0 {
		t.Fatalf("No continuation token set")
	}
	validateResourceVersion := resourceVersionNotOlderThan(lastRVString)
	expectNoDiff(t, "incorrect second page", []example.Pod{*preset[1].storedObj}, out.Items)
	if err := validateResourceVersion(out.ResourceVersion); err != nil {
		t.Fatal(err)
	}
	continueFromThirdItem := out.Continue
	resolvedResourceVersionFromThirdItem := out.ResourceVersion
	out = &example.PodList{}
	options = storage.ListOptions{
		ResourceVersion: "0",
		Predicate:       pred(1, continueFromThirdItem),
		Recursive:       true,
	}
	if err := store.GetList(ctx, "/pods/", options, out); err != nil {
		t.Fatalf("Unable to get second page: %v", err)
	}
	if len(out.Continue) != 0 {
		t.Fatalf("Unexpected continuation token set")
	}
	expectNoDiff(t, "incorrect third page", []example.Pod{*preset[2].storedObj}, out.Items)
	if out.ResourceVersion != resolvedResourceVersionFromThirdItem {
		t.Fatalf("Expected list resource version to be %s, got %s", resolvedResourceVersionFromThirdItem, out.ResourceVersion)
	}
}

// PrefixTransformerModifier 用于替换实现内部的 PrefixTransformer，
// 返回的 transformer 由实现负责安装，调用方不直接持有它。
type PrefixTransformerModifier func(*PrefixTransformer) value.Transformer

// InterfaceWithPrefixTransformer 是支持替换 PrefixTransformer 的存储实现，
// 仅供需要观察"前缀 + 数据"字节形态的测试使用（如 staleness、序列化归一化）。
type InterfaceWithPrefixTransformer interface {
	storage.Interface

	UpdatePrefixTransformer(PrefixTransformerModifier) func()
}

// TransformerModifier 用于在不关心 PrefixTransformer 细节时替换 transformer。
type TransformerModifier func(value.Transformer) value.Transformer

// InterfaceWithTransformerOverride 允许测试临时替换 transformer 并返回恢复函数，
// 用于模拟对象损坏或加解密失败。
type InterfaceWithTransformerOverride interface {
	storage.Interface
	UpdateTransformer(TransformerModifier) func()
}

// RunTestListResourceVersionMatch 校验 List 在 match=Exact 下返回的是可复现的快照：
// 用响应中的 RV 再发一次 Exact 列表，结果必须完全一致；NotOlderThan 同理。
func RunTestListResourceVersionMatch(ctx context.Context, t *testing.T, store InterfaceWithPrefixTransformer) {
	nextPod := func(index uint32) (string, *example.Pod) {
		obj := &example.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: fmt.Sprintf("pod-%d", index),
				Labels: map[string]string{
					"even": strconv.FormatBool(index%2 == 0),
				},
			},
		}
		return computePodKey(obj), obj
	}

	transformer := &reproducingTransformer{
		store:      store,
		nextObject: nextPod,
	}

	revertTransformer := store.UpdatePrefixTransformer(
		func(previousTransformer *PrefixTransformer) value.Transformer {
			transformer.wrapped = previousTransformer
			return transformer
		})
	defer revertTransformer()

	for i := 0; i < 5; i++ {
		if err := transformer.createObject(ctx); err != nil {
			t.Fatalf("failed to create object: %v", err)
		}
	}

	getAttrs := func(obj runtime.Object) (labels.Set, fields.Set, error) {
		pod, ok := obj.(*example.Pod)
		if !ok {
			return nil, nil, fmt.Errorf("invalid object")
		}
		return labels.Set(pod.Labels), nil, nil
	}
	predicate := storage.SelectionPredicate{
		Label:    labels.Set{"even": "true"}.AsSelector(),
		GetAttrs: getAttrs,
		Limit:    4,
	}

	result1 := example.PodList{}
	options := storage.ListOptions{
		Predicate: predicate,
		Recursive: true,
	}
	if err := store.GetList(ctx, "/pods/", options, &result1); err != nil {
		t.Fatalf("failed to list objects: %v", err)
	}

	// List objects from the returned resource version.
	// 用首页返回的 RV 做 Exact 读，结果必须与首次列表逐字节一致。
	options = storage.ListOptions{
		Predicate:            predicate,
		ResourceVersion:      result1.ResourceVersion,
		ResourceVersionMatch: metav1.ResourceVersionMatchExact,
		Recursive:            true,
	}

	result2 := example.PodList{}
	if err := store.GetList(ctx, "/pods/", options, &result2); err != nil {
		t.Fatalf("failed to list objects: %v", err)
	}

	expectNoDiff(t, "incorrect lists", result1, result2)

	// Now also verify the  ResourceVersionMatchNotOlderThan.
	// 再用 NotOlderThan 读一次，然后把响应 RV 作为 Exact 基准复读，验证其自洽性。
	options.ResourceVersionMatch = metav1.ResourceVersionMatchNotOlderThan

	result3 := example.PodList{}
	if err := store.GetList(ctx, "/pods/", options, &result3); err != nil {
		t.Fatalf("failed to list objects: %v", err)
	}

	options.ResourceVersion = result3.ResourceVersion
	options.ResourceVersionMatch = metav1.ResourceVersionMatchExact

	result4 := example.PodList{}
	if err := store.GetList(ctx, "/pods/", options, &result4); err != nil {
		t.Fatalf("failed to list objects: %v", err)
	}

	expectNoDiff(t, "incorrect lists", result3, result4)
}

// RunTestGuaranteedUpdate 覆盖 GuaranteedUpdate 的关键语义：
// ignoreNotFound 的两种行为、UID 前置条件、内容未变时短路不写（避免无意义的 RV 递增），
// 以及 transformer 报告 stale 时必须强制重写。
func RunTestGuaranteedUpdate(ctx context.Context, t *testing.T, store InterfaceWithPrefixTransformer, validation KeyValidation) {
	inputObj := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns", UID: "A"}}
	key := computePodKey(inputObj)

	tests := []struct {
		name                string
		key                 string
		ignoreNotFound      bool
		precondition        *storage.Preconditions
		expectNotFoundErr   bool
		expectInvalidObjErr bool
		expectNoUpdate      bool
		transformStale      bool
		hasSelfLink         bool
	}{{
		name:                "non-existing key, ignoreNotFound=false",
		key:                 "/pods/non-existing",
		ignoreNotFound:      false,
		precondition:        nil,
		expectNotFoundErr:   true,
		expectInvalidObjErr: false,
		expectNoUpdate:      false,
	}, {
		name:                "non-existing key, ignoreNotFound=true",
		key:                 "/pods/non-existing",
		ignoreNotFound:      true,
		precondition:        nil,
		expectNotFoundErr:   false,
		expectInvalidObjErr: false,
		expectNoUpdate:      false,
	}, {
		name:                "existing key",
		key:                 key,
		ignoreNotFound:      false,
		precondition:        nil,
		expectNotFoundErr:   false,
		expectInvalidObjErr: false,
		expectNoUpdate:      false,
	}, {
		name:                "same data",
		key:                 key,
		ignoreNotFound:      false,
		precondition:        nil,
		expectNotFoundErr:   false,
		expectInvalidObjErr: false,
		expectNoUpdate:      true,
	}, {
		name:                "same data, a selfLink",
		key:                 key,
		ignoreNotFound:      false,
		precondition:        nil,
		expectNotFoundErr:   false,
		expectInvalidObjErr: false,
		expectNoUpdate:      true,
		hasSelfLink:         true,
	}, {
		name:                "same data, stale",
		key:                 key,
		ignoreNotFound:      false,
		precondition:        nil,
		expectNotFoundErr:   false,
		expectInvalidObjErr: false,
		expectNoUpdate:      false,
		transformStale:      true,
	}, {
		name:                "UID match",
		key:                 key,
		ignoreNotFound:      false,
		precondition:        storage.NewUIDPreconditions("A"),
		expectNotFoundErr:   false,
		expectInvalidObjErr: false,
		expectNoUpdate:      true,
	}, {
		name:                "UID mismatch",
		key:                 key,
		ignoreNotFound:      false,
		precondition:        storage.NewUIDPreconditions("B"),
		expectNotFoundErr:   false,
		expectInvalidObjErr: true,
		expectNoUpdate:      true,
	}}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, storeObj := testPropagateStore(ctx, t, store, inputObj)

			out := &example.Pod{}
			// 每个用例用不同的 annotation 值区分写入内容；expectNoUpdate 时置空，
			// 让 update func 返回与已存内容完全相同的对象，以触发短路逻辑。
			annotations := map[string]string{"version": fmt.Sprintf("%d", i)}
			if tt.expectNoUpdate {
				annotations = nil
			}

			// 模拟 transformer 认为数据陈旧（如加密 key 轮转），此时即使内容相同也必须重写。
			if tt.transformStale {
				revertTransformer := store.UpdatePrefixTransformer(
					func(transformer *PrefixTransformer) value.Transformer {
						transformer.stale = true
						return transformer
					})
				defer revertTransformer()
			}

			version := storeObj.ResourceVersion
			err := store.GuaranteedUpdate(ctx, tt.key, out, tt.ignoreNotFound, tt.precondition,
				storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
					// ignoreNotFound 且 key 不存在时，实现应传入零值对象而非报错。
					if tt.expectNotFoundErr && tt.ignoreNotFound {
						if pod := obj.(*example.Pod); pod.Name != "" {
							t.Errorf("%s: expecting zero value, but get=%#v", tt.name, pod)
						}
					}
					pod := *storeObj
					if tt.hasSelfLink {
						pod.SelfLink = "testlink"
					}
					pod.Annotations = annotations
					return &pod, nil
				}), nil)

			if tt.expectNotFoundErr {
				if err == nil || !storage.IsNotFound(err) {
					t.Errorf("%s: expecting not found error, but get: %v", tt.name, err)
				}
				return
			}
			if tt.expectInvalidObjErr {
				if err == nil || !storage.IsInvalidObj(err) {
					t.Errorf("%s: expecting invalid UID error, but get: %s", tt.name, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("%s: GuaranteedUpdate failed: %v", tt.name, err)
			}
			if !reflect.DeepEqual(out.ObjectMeta.Annotations, annotations) {
				t.Errorf("%s: pod annotations want=%s, get=%s", tt.name, annotations, out.ObjectMeta.Annotations)
			}
			if out.SelfLink != "" {
				t.Errorf("%s: selfLink should not be set", tt.name)
			}

			// verify that kv pair is not empty after set and that the underlying data matches expectations
			// 通过后端特定的 validation 检查存储中的原始字节，而不只信任返回对象。
			validation(ctx, t, key)

			// 版本号是"是否真正写入"的最终判据：短路时不得递增 RV。
			switch tt.expectNoUpdate {
			case true:
				if version != out.ResourceVersion {
					t.Errorf("%s: expect no version change, before=%s, after=%s", tt.name, version, out.ResourceVersion)
				}
			case false:
				if version == out.ResourceVersion {
					t.Errorf("%s: expect version change, but get the same version=%s", tt.name, version)
				}
			}
		})
	}
}

// RunTestGuaranteedUpdateWithTTL 校验更新时携带 TTL 会让对象过期并产生 Deleted 事件。
func RunTestGuaranteedUpdateWithTTL(ctx context.Context, t *testing.T, store storage.Interface) {
	input := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}}
	key := computePodKey(input)

	out := &example.Pod{}
	err := store.GuaranteedUpdate(ctx, key, out, true, nil,
		func(_ runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			ttl := uint64(1)
			return input, &ttl, nil
		}, nil)
	if err != nil {
		t.Fatalf("Create failed: %v", err)
	}

	w, err := store.Watch(ctx, key, storage.ListOptions{ResourceVersion: out.ResourceVersion, Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	testCheckEventType(t, w, watch.Deleted)
}

// RunTestGuaranteedUpdateChecksStoredData 校验"短路"判断基于存储中的序列化字节，
// 而非解码后的对象：字节不一致（如历史遗留的空格）或 transformer 报告 stale 时都必须重写。
func RunTestGuaranteedUpdateChecksStoredData(ctx context.Context, t *testing.T, store InterfaceWithPrefixTransformer) {
	input := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}}
	key := computePodKey(input)

	// serialize input into etcd with data that would be normalized by a write -
	// in this case, leading whitespace
	revertTransformer := store.UpdatePrefixTransformer(
		func(transformer *PrefixTransformer) value.Transformer {
			transformer.prefix = []byte(string(transformer.prefix) + " ")
			return transformer
		})
	_, initial := testPropagateStore(ctx, t, store, input)
	revertTransformer()

	// this update should write the canonical value to etcd because the new serialization differs
	// from the stored serialization
	// 第一次更新：存储中是非规范字节，必须重写为规范形式。
	input.ResourceVersion = initial.ResourceVersion
	out := &example.Pod{}
	err := store.GuaranteedUpdate(ctx, key, out, true, nil,
		func(_ runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			return input, nil, nil
		}, input)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if out.ResourceVersion == initial.ResourceVersion {
		t.Errorf("guaranteed update should have updated the serialized data, got %#v", out)
	}

	lastVersion := out.ResourceVersion

	// this update should not write to etcd because the input matches the stored data
	// 第二次更新：内容与规范字节完全一致，应短路且 RV 不变。
	input = out
	out = &example.Pod{}
	err = store.GuaranteedUpdate(ctx, key, out, true, nil,
		func(_ runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			return input, nil, nil
		}, input)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if out.ResourceVersion != lastVersion {
		t.Errorf("guaranteed update should have short-circuited write, got %#v", out)
	}

	revertTransformer = store.UpdatePrefixTransformer(
		func(transformer *PrefixTransformer) value.Transformer {
			transformer.stale = true
			return transformer
		})
	defer revertTransformer()

	// this update should write to etcd because the transformer reported stale
	// 第三次更新：字节相同但 transformer 声明陈旧，必须强制重写（加密轮转场景）。
	err = store.GuaranteedUpdate(ctx, key, out, true, nil,
		func(_ runtime.Object, _ storage.ResponseMeta) (runtime.Object, *uint64, error) {
			return input, nil, nil
		}, input)
	if err != nil {
		t.Fatalf("Update failed: %v", err)
	}
	if out.ResourceVersion == lastVersion {
		t.Errorf("guaranteed update should have written to etcd when transformer reported stale, got %#v", out)
	}
}

// RunTestGuaranteedUpdateWithConflict 用两个并发 GuaranteedUpdate 强制冲突：
// 后提交的一方必须重新读取最新值并再次调用 update 函数，而不是覆盖对方的写入。
func RunTestGuaranteedUpdateWithConflict(ctx context.Context, t *testing.T, store storage.Interface) {
	key, _ := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}})

	// 两个 WaitGroup 交叉握手：保证双方都进入 update 函数后再继续，使冲突必然发生。
	errChan := make(chan error, 1)
	var firstToFinish sync.WaitGroup
	var secondToEnter sync.WaitGroup
	firstToFinish.Add(1)
	secondToEnter.Add(1)

	go func() {
		err := store.GuaranteedUpdate(ctx, key, &example.Pod{}, false, nil,
			storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
				pod := obj.(*example.Pod)
				pod.Name = "foo-1"
				secondToEnter.Wait()
				return pod, nil
			}), nil)
		firstToFinish.Done()
		errChan <- err
	}()

	updateCount := 0
	err := store.GuaranteedUpdate(ctx, key, &example.Pod{}, false, nil,
		storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
			if updateCount == 0 {
				secondToEnter.Done()
				firstToFinish.Wait()
			}
			updateCount++
			pod := obj.(*example.Pod)
			pod.Name = "foo-2"
			return pod, nil
		}), nil)
	if err != nil {
		t.Fatalf("Second GuaranteedUpdate error %#v", err)
	}
	if err := <-errChan; err != nil {
		t.Fatalf("First GuaranteedUpdate error %#v", err)
	}

	if updateCount != 2 {
		t.Errorf("Should have conflict and called update func twice")
	}
}

// RunTestGuaranteedUpdateWithSuggestionAndConflict 校验 suggestion 与冲突重试的交互：
// 过期 suggestion 触发冲突后应回源读取再重试；而当前 suggestion 下 update 函数返回错误时
// 不得重试（否则会放大 admission 拒绝的代价）。
func RunTestGuaranteedUpdateWithSuggestionAndConflict(ctx context.Context, t *testing.T, store storage.Interface) {
	key, originalPod := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}})

	// First, update without a suggestion so originalPod is outdated
	updatedPod := &example.Pod{}
	err := store.GuaranteedUpdate(ctx, key, updatedPod, false, nil,
		storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
			pod := obj.(*example.Pod)
			pod.Generation = 2
			return pod, nil
		}),
		nil,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Second, update using the outdated originalPod as the suggestion. Return a conflict error when
	// passed originalPod, and make sure that SimpleUpdate is called a second time after a live lookup
	// with the value of updatedPod.
	sawConflict := false
	updatedPod2 := &example.Pod{}
	err = store.GuaranteedUpdate(ctx, key, updatedPod2, false, nil,
		storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
			pod := obj.(*example.Pod)
			if pod.Generation != 2 {
				if sawConflict {
					t.Fatalf("unexpected second conflict")
				}
				sawConflict = true
				// simulated stale object - return a conflict
				return nil, apierrors.NewConflict(example.SchemeGroupVersion.WithResource("pods").GroupResource(), "name", errors.New("foo"))
			}
			pod.Generation = 3
			return pod, nil
		}),
		originalPod,
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if updatedPod2.Generation != 3 {
		t.Errorf("unexpected pod generation: %d", updatedPod2.Generation)
	}

	// Third, update using a current version as the suggestion.
	// Return an error and make sure that SimpleUpdate is NOT called a second time,
	// since the live lookup shows the suggestion was already up to date.
	attempts := 0
	updatedPod3 := &example.Pod{}
	err = store.GuaranteedUpdate(ctx, key, updatedPod3, false, nil,
		storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
			pod := obj.(*example.Pod)
			if pod.Generation != updatedPod2.Generation || pod.ResourceVersion != updatedPod2.ResourceVersion {
				t.Logf("stale object (rv=%s), expected rv=%s", pod.ResourceVersion, updatedPod2.ResourceVersion)
			}
			attempts++
			return nil, fmt.Errorf("validation or admission error")
		}),
		updatedPod2,
	)
	if err == nil {
		t.Fatalf("expected error, got none")
	}
	// Implementations of the storage interface are allowed to ignore the suggestion,
	// in which case two attempts are possible.
	// 允许实现忽略 suggestion，因此用上界断言。
	if attempts > 2 {
		t.Errorf("update function should have been called at most twice, called %d", attempts)
	}
}

// RunTestTransformationFailure 校验单个坏对象不会"污染"整个存储：
// List/Get/Update/Delete 都应返回 InternalError，但其他 key 仍可正常访问。
func RunTestTransformationFailure(ctx context.Context, t *testing.T, store InterfaceWithPrefixTransformer) {
	barFirst := &example.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "first", Name: "bar"},
		Spec:       DeepEqualSafePodSpec(),
	}
	bazSecond := &example.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "second", Name: "baz"},
		Spec:       DeepEqualSafePodSpec(),
	}

	preset := []struct {
		key       string
		obj       *example.Pod
		storedObj *example.Pod
	}{{
		key: computePodKey(barFirst),
		obj: barFirst,
	}, {
		key: computePodKey(bazSecond),
		obj: bazSecond,
	}}
	for i, ps := range preset[:1] {
		preset[i].storedObj = &example.Pod{}
		err := store.Create(ctx, ps.key, ps.obj, preset[:1][i].storedObj, 0)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}

	// create a second resource with an invalid prefix
	// 用一个不匹配的前缀写入第二个对象，模拟"存储中的字节无法被当前 transformer 解开"。
	revertTransformer := store.UpdatePrefixTransformer(
		func(transformer *PrefixTransformer) value.Transformer {
			return NewPrefixTransformer([]byte("otherprefix!"), false)
		})
	for i, ps := range preset[1:] {
		preset[1:][i].storedObj = &example.Pod{}
		err := store.Create(ctx, ps.key, ps.obj, preset[1:][i].storedObj, 0)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
	}
	revertTransformer()

	// List should fail
	var got example.PodList
	storageOpts := storage.ListOptions{
		Predicate: storage.Everything,
		Recursive: true,
	}
	if err := store.GetList(ctx, "/pods/", storageOpts, &got); !storage.IsInternalError(err) {
		t.Errorf("Unexpected error %v", err)
	}

	// Get should fail
	if err := store.Get(ctx, preset[1].key, storage.GetOptions{}, &example.Pod{}); !storage.IsInternalError(err) {
		t.Errorf("Unexpected error: %v", err)
	}

	updateFunc := func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
		return input, nil, nil
	}
	// GuaranteedUpdate without suggestion should return an error
	if err := store.GuaranteedUpdate(ctx, preset[1].key, &example.Pod{}, false, nil, updateFunc, nil); !storage.IsInternalError(err) {
		t.Errorf("Unexpected error: %v", err)
	}
	// GuaranteedUpdate with suggestion should return an error if we don't change the object
	if err := store.GuaranteedUpdate(ctx, preset[1].key, &example.Pod{}, false, nil, updateFunc, preset[1].obj); err == nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Delete fails with internal error.
	if err := store.Delete(ctx, preset[1].key, &example.Pod{}, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{}); !storage.IsInternalError(err) {
		t.Errorf("Unexpected error: %v", err)
	}
	if err := store.Get(ctx, preset[1].key, storage.GetOptions{}, &example.Pod{}); !storage.IsInternalError(err) {
		t.Errorf("Unexpected error: %v", err)
	}
}

// RunTestStats 校验 Stats 的计数与平均大小估算：对象数随增删变化，
// 平均大小按编码 + 转换后的字节数计算；sizeEnabled 为 false 时大小字段必须恒为 0。
func RunTestStats(ctx context.Context, t *testing.T, store storage.Interface, codec runtime.Codec, transformer value.Transformer, sizeEnabled bool) {
	assertStats(t, store, sizeEnabled, storage.Stats{ObjectCount: 0, EstimatedAverageObjectSizeBytes: 0})

	foo := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo"}}
	fooKey := computePodKey(foo)
	if err := store.Create(ctx, fooKey, foo, nil, 0); err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	fooSize := objectSize(t, codec, foo, transformer)
	assertStats(t, store, sizeEnabled, storage.Stats{ObjectCount: 1, EstimatedAverageObjectSizeBytes: fooSize})

	bar := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "bar"}}
	barKey := computePodKey(bar)
	if err := store.Create(ctx, barKey, bar, nil, 0); err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	barSize := objectSize(t, codec, bar, transformer)
	assertStats(t, store, sizeEnabled, storage.Stats{ObjectCount: 2, EstimatedAverageObjectSizeBytes: (fooSize + barSize) / 2})

	if err := store.GuaranteedUpdate(ctx, barKey, bar, false, nil,
		storage.SimpleUpdate(func(obj runtime.Object) (runtime.Object, error) {
			pod := obj.(*example.Pod)
			pod.Labels = map[string]string{"foo": "bar"}
			return pod, nil
		}), nil); err != nil {
		t.Errorf("Update failed: %v", err)
	}
	// ResourceVerson is not stored.
	// resourceVersion 是元数据，不参与大小估算，因此清空后再计算。
	bar.ResourceVersion = ""
	barSize = objectSize(t, codec, bar, transformer)
	assertStats(t, store, sizeEnabled, storage.Stats{ObjectCount: 2, EstimatedAverageObjectSizeBytes: (fooSize + barSize) / 2})

	if err := store.Delete(ctx, fooKey, foo, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{}); err != nil {
		t.Errorf("Delete failed: %v", err)
	}
	assertStats(t, store, sizeEnabled, storage.Stats{ObjectCount: 1, EstimatedAverageObjectSizeBytes: barSize})

	if err := store.Delete(ctx, fooKey, foo, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{}); err == nil {
		t.Errorf("Delete expected to fail")
	}
	assertStats(t, store, sizeEnabled, storage.Stats{ObjectCount: 1, EstimatedAverageObjectSizeBytes: barSize})

	if err := store.Delete(ctx, barKey, bar, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{}); err != nil {
		t.Errorf("Delete failed: %v", err)
	}
	assertStats(t, store, sizeEnabled, storage.Stats{ObjectCount: 0, EstimatedAverageObjectSizeBytes: 0})
}

// assertStats 先做一次一致读，确保异步实现（watchcache）的统计已追上最新状态，
// 再比对期望值；sizeEnabled 为 false 时忽略大小字段。
func assertStats(t *testing.T, store storage.Interface, sizeEnabled bool, expectStats storage.Stats) {
	t.Helper()
	// Execute consistent LIST to refresh state of cache.
	err := store.GetList(t.Context(), "/pods/", storage.ListOptions{Recursive: true, Predicate: storage.Everything}, &example.PodList{})
	if err != nil {
		t.Fatalf("GetList failed: %v", err)
	}
	stats, err := store.Stats(t.Context())
	if err != nil {
		t.Fatalf("store.Stats failed: %v", err)
	}

	if !sizeEnabled {
		expectStats.EstimatedAverageObjectSizeBytes = 0
	}
	if expectStats != stats {
		t.Errorf("store.Stats: expected %+v but got %+v", expectStats, stats)
	}
}

// objectSize 计算对象落盘后的字节数：先编码，再经过 transformer（可能加密/加前缀），
// 与 Stats 的估算口径保持一致。
func objectSize(t *testing.T, codec runtime.Codec, obj runtime.Object, transformer value.Transformer) int64 {
	data, err := runtime.Encode(codec, obj)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}
	data, err = transformer.TransformToStorage(t.Context(), data, value.DefaultContext{})
	if err != nil {
		t.Fatalf("Transform failed: %v", err)
	}
	return int64(len(data))
}

// RunTestListPaging 校验分页期间的新写入不会"插队"到已翻过的页里：
// 翻页过程中创建 test-5，最终结果仍必须是创建时的 4 个对象。
func RunTestListPaging(ctx context.Context, t *testing.T, store storage.Interface) {
	out := &example.Pod{}
	for i := 0; i < 4; i++ {
		name := fmt.Sprintf("test-%d", i)
		pod := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "paging"}}
		key := computePodKey(pod)
		if err := store.Create(ctx, key, pod, out, 0); err != nil {
			t.Fatal(err)
		}
	}

	var names []string
	opts := storage.ListOptions{
		Recursive: true,
		Predicate: storage.SelectionPredicate{
			Label: labels.Everything(),
			Field: fields.Everything(),
			Limit: 1,
		},
	}
	calls := 0
	for {
		calls++
		listOut := &example.PodList{}
		err := store.GetList(ctx, "/pods/", opts, listOut)
		if err != nil {
			t.Fatalf("Unexpected error %s", err)
		}
		for _, item := range listOut.Items {
			names = append(names, item.Name)
		}
		if listOut.Continue == "" {
			break
		}
		if calls == 2 {
			// 在翻页中途插入新对象：continue token 固定的 RV 快照应使其不可见。
			name := "test-5"
			pod := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "paging"}}
			key := computePodKey(pod)
			if err := store.Create(ctx, key, pod, out, 0); err != nil {
				t.Fatal(err)
			}
		}
		opts.Predicate.Continue = listOut.Continue
	}
	if calls != 4 {
		t.Errorf("unexpected list invocations: %d", calls)
	}
	if !reflect.DeepEqual(names, []string{"test-0", "test-1", "test-2", "test-3"}) {
		t.Errorf("unexpected items: %#v", names)
	}
}

// RunTestNamespaceScopedList 覆盖命名空间范围与字段选择器的组合矩阵：
// 重点验证 metadata.namespace 选择器与请求路径中的 namespace 前缀同时存在时的取交集行为，
// 以及带索引字段（spec.nodeName）时过滤仍与全量扫描结果一致。
func RunTestNamespaceScopedList(ctx context.Context, t *testing.T, store storage.Interface) {
	tests := []struct {
		name               string
		requestedNamespace string
		recursive          bool
		indexFields        []string
		fieldSelector      func(namespace string) fields.Selector
		inputPods          func(namespace string) []example.Pod
		expectPods         func(namespace string) []example.Pod
	}{
		{
			name:          "request without namespace, without field selector",
			recursive:     true,
			fieldSelector: func(namespace string) fields.Selector { return fields.Everything() },
			inputPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", "ns1"),
					*baseNamespacedPod("foo2", "ns2"),
				}
			},
			expectPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", "ns1"),
					*baseNamespacedPod("foo2", "ns2"),
				}
			},
		},
		{
			name:      "request without namespace, field selector with metadata.namespace",
			recursive: true,
			fieldSelector: func(namespace string) fields.Selector {
				return fields.SelectorFromSet(fields.Set{"metadata.namespace": namespace + "ns1"})
			},
			inputPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", namespace+"ns1"),
					*baseNamespacedPod("foo1", namespace+"ns2"),
				}
			},
			expectPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", namespace+"ns1"),
				}
			},
		},
		{
			name:      "request without namespace, field selector with spec.nodename",
			recursive: true,
			fieldSelector: func(namespace string) fields.Selector {
				return fields.ParseSelectorOrDie("spec.nodeName=bar1")
			},
			indexFields: []string{"spec.nodeName"},
			inputPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPodAssigned("foo1", namespace+"ns1", "bar1"),
					*baseNamespacedPodAssigned("foo2", namespace+"ns2", "bar2"),
				}
			},
			expectPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPodAssigned("foo1", namespace+"ns1", "bar1"),
				}
			},
		},
		{
			name:          "request without namespace, field selector with spec.nodename to filter out",
			recursive:     true,
			fieldSelector: func(namespace string) fields.Selector { return fields.ParseSelectorOrDie("spec.nodeName!=bar1") },
			indexFields:   []string{"spec.nodeName"},
			inputPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPodAssigned("foo1", namespace+"ns1", "bar1"),
					*baseNamespacedPodAssigned("foo2", namespace+"ns2", "bar2"),
				}
			},
			expectPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPodAssigned("foo2", namespace+"ns2", "bar2"),
				}
			},
		},
		{
			name:               "request with namespace, without field selector",
			requestedNamespace: "ns1",
			recursive:          true,
			fieldSelector:      func(namespace string) fields.Selector { return fields.Everything() },
			inputPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", namespace+"ns1"),
					*baseNamespacedPod("foo1", namespace+"ns2"),
					*baseNamespacedPod("foo2", namespace+"ns1"),
				}
			},
			expectPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", namespace+"ns1"),
					*baseNamespacedPod("foo2", namespace+"ns1"),
				}
			},
		},
		{
			name:               "request with namespace, field selector with matched metadata.namespace",
			requestedNamespace: "ns1",
			recursive:          true,
			fieldSelector: func(namespace string) fields.Selector {
				return fields.SelectorFromSet(fields.Set{"metadata.namespace": namespace + "ns1"})
			},
			inputPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", namespace+"ns1"),
					*baseNamespacedPod("foo1", namespace+"ns2"),
				}
			},
			expectPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", namespace+"ns1"),
				}
			},
		},
		{
			name:               "request with namespace, field selector with non-matched metadata.namespace",
			requestedNamespace: "ns1",
			recursive:          true,
			fieldSelector: func(namespace string) fields.Selector {
				return fields.SelectorFromSet(fields.Set{"metadata.namespace": namespace + "ns2"})
			},
			inputPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", namespace+"ns1"),
					*baseNamespacedPod("foo1", namespace+"ns2"),
					*baseNamespacedPodUpdated("foo2", namespace+"ns1"),
					*baseNamespacedPodUpdated("foo2", namespace+"ns2"),
				}
			},
			expectPods: func(namespace string) []example.Pod { return []example.Pod{} },
		},
		{
			name:               "request with namespace, field selector with spec.nodename",
			requestedNamespace: "ns1",
			recursive:          true,
			fieldSelector:      func(namespace string) fields.Selector { return fields.ParseSelectorOrDie("spec.nodeName=bar2") },
			indexFields:        []string{"spec.nodeName"},
			inputPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPodAssigned("foo1", namespace+"ns1", "bar1"),
					*baseNamespacedPodAssigned("foo1", namespace+"ns2", "bar2"),
					*baseNamespacedPodAssigned("foo2", namespace+"ns1", "bar2"),
				}
			},
			expectPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPodAssigned("foo2", namespace+"ns1", "bar2"),
				}
			},
		},
		{
			name:               "request with namespace, field selector with spec.nodename to filter out",
			requestedNamespace: "ns2",
			recursive:          true,
			fieldSelector:      func(namespace string) fields.Selector { return fields.ParseSelectorOrDie("spec.nodeName!=bar1") },
			indexFields:        []string{"spec.nodeName"},
			inputPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", namespace+"ns1"),
					*baseNamespacedPod("foo1", namespace+"ns2"),
					*baseNamespacedPodAssigned("foo3", namespace+"ns2", "bar1"),
					*baseNamespacedPodAssigned("foo2", namespace+"ns2", "bar2"),
				}
			},
			expectPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", namespace+"ns2"),
					*baseNamespacedPodAssigned("foo2", namespace+"ns2", "bar2"),
				}
			},
		},
		{
			name:          "request without namespace, field selector with metadata.name",
			recursive:     true,
			fieldSelector: func(namespace string) fields.Selector { return fields.ParseSelectorOrDie("metadata.name=foo1") },
			inputPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", namespace+"ns1"),
					*baseNamespacedPod("foo1", namespace+"ns2"),
					*baseNamespacedPod("foo2", namespace+"ns1"),
				}
			},
			expectPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", namespace+"ns1"),
					*baseNamespacedPod("foo1", namespace+"ns2"),
				}
			},
		},
		{
			name:      "request without namespace, field selector with metadata.name and metadata.namespace",
			recursive: true,
			fieldSelector: func(namespace string) fields.Selector {
				return fields.SelectorFromSet(fields.Set{
					"metadata.name":      "foo1",
					"metadata.namespace": namespace + "ns1",
				})
			},
			inputPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", namespace+"ns1"),
					*baseNamespacedPod("foo2", namespace+"ns1"),
					*baseNamespacedPod("foo1", namespace+"ns2"),
				}
			},
			expectPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", namespace+"ns1"),
				}
			},
		},
		{
			name:      "request without namespace, field selector with metadata.name and spec.nodeName",
			recursive: true,
			fieldSelector: func(namespace string) fields.Selector {
				return fields.SelectorFromSet(fields.Set{
					"metadata.name": "foo1",
					"spec.nodeName": "bar1",
				})
			},
			indexFields: []string{"spec.nodeName"},
			inputPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPodAssigned("foo1", namespace+"ns1", "bar1"),
				}
			},
			expectPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPodAssigned("foo1", namespace+"ns1", "bar1"),
				}
			},
		},
		{
			name:      "request without namespace, field selector with metadata.name, and with spec.nodeName to filter out watch",
			recursive: true,
			fieldSelector: func(namespace string) fields.Selector {
				return fields.AndSelectors(
					fields.ParseSelectorOrDie("spec.nodeName!=bar1"),
					fields.SelectorFromSet(fields.Set{"metadata.name": "foo1"}),
				)
			},
			indexFields: []string{"spec.nodeName"},
			inputPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", namespace+"ns1"),
					*baseNamespacedPod("foo2", namespace+"ns1"),
					*baseNamespacedPodAssigned("foo1", namespace+"ns2", "bar1"),
					*baseNamespacedPodAssigned("foo2", namespace+"ns2", "bar1"),
					*baseNamespacedPodAssigned("foo1", namespace+"ns3", "bar2"),
					*baseNamespacedPodAssigned("foo2", namespace+"ns3", "bar2"),
				}
			},
			expectPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", namespace+"ns1"),
					*baseNamespacedPodAssigned("foo1", namespace+"ns3", "bar2"),
				}
			},
		},
		{
			name:               "request with namespace, with field selector metadata.name",
			requestedNamespace: "ns1",
			fieldSelector:      func(namespace string) fields.Selector { return fields.ParseSelectorOrDie("metadata.name=foo1") },
			inputPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", namespace+"ns1"),
					*baseNamespacedPod("foo2", namespace+"ns1"),
					*baseNamespacedPod("foo1", namespace+"ns2"),
					*baseNamespacedPod("foo2", namespace+"ns2"),
				}
			},
			expectPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", namespace+"ns1"),
				}
			},
		},
		{
			name:               "request with namespace, with field selector metadata.name and metadata.namespace",
			requestedNamespace: "ns1",
			fieldSelector: func(namespace string) fields.Selector {
				return fields.SelectorFromSet(fields.Set{
					"metadata.name":      "foo1",
					"metadata.namespace": namespace + "ns1",
				})
			},
			inputPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", namespace+"ns1"),
					*baseNamespacedPod("foo2", namespace+"ns1"),
					*baseNamespacedPod("foo1", namespace+"ns2"),
					*baseNamespacedPod("foo2", namespace+"ns2"),
				}
			},
			expectPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", namespace+"ns1"),
				}
			},
		},
		{
			name:               "request with namespace, with field selector metadata.name, metadata.namespace and spec.nodename",
			requestedNamespace: "ns2",
			fieldSelector: func(namespace string) fields.Selector {
				return fields.SelectorFromSet(fields.Set{
					"metadata.name":      "foo2",
					"metadata.namespace": namespace + "ns2",
					"spec.nodeName":      "bar1",
				})
			},
			indexFields: []string{"spec.nodeName"},
			inputPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", namespace+"ns1"),
					*baseNamespacedPod("foo2", namespace+"ns1"),
					*baseNamespacedPod("foo1", namespace+"ns2"),
					*baseNamespacedPodAssigned("foo2", namespace+"ns2", "bar1"),
					*baseNamespacedPodAssigned("foo1", namespace+"ns3", "bar1"),
					*baseNamespacedPodAssigned("foo2", namespace+"ns3", "bar1"),
				}
			},
			expectPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPodAssigned("foo2", namespace+"ns2", "bar1"),
				}
			},
		},
		{
			name:               "request with namespace, with field selector metadata.name, metadata.namespace, and with spec.nodename to filter out",
			requestedNamespace: "ns2",
			fieldSelector: func(namespace string) fields.Selector {
				return fields.AndSelectors(
					fields.ParseSelectorOrDie("spec.nodeName!=bar2"),
					fields.SelectorFromSet(fields.Set{"metadata.name": "foo1", "metadata.namespace": namespace + "ns2"}),
				)
			},
			indexFields: []string{"spec.nodeName"},
			inputPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPod("foo1", namespace+"ns1"),
					*baseNamespacedPod("foo2", namespace+"ns1"),
					*baseNamespacedPodAssigned("foo1", namespace+"ns2", "bar1"),
					*baseNamespacedPodAssigned("foo2", namespace+"ns2", "bar2"),
					*baseNamespacedPodAssigned("foo1", namespace+"ns3", "bar2"),
				}
			},
			expectPods: func(namespace string) []example.Pod {
				return []example.Pod{
					*baseNamespacedPodAssigned("foo1", namespace+"ns2", "bar1"),
				}
			},
		},
	}
	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			podNames := map[string]struct{}{}
			namespace := fmt.Sprintf("t%d-", i)
			for _, pod := range tt.inputPods(namespace) {
				out := &example.Pod{}
				key := computePodKey(&pod)
				podNames[key] = struct{}{}
				err := store.Create(ctx, key, &pod, out, 0)
				if err != nil {
					t.Fatalf("GuaranteedUpdate failed: %v", err)
				}
			}
			opts := storage.ListOptions{
				ResourceVersion: "",
				Predicate:       CreatePodPredicate(tt.fieldSelector(namespace), true, tt.indexFields),
				Recursive:       true,
			}
			listOut := &example.PodList{}
			path := "/pods/"
			if tt.requestedNamespace != "" {
				path += namespace + tt.requestedNamespace
			}
			if err := store.GetList(ctx, path, opts, listOut); err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			i := 0
			for _, pod := range listOut.Items {
				if _, found := podNames[computePodKey(&pod)]; !found {
					continue
				}
				pod.ResourceVersion = ""
				listOut.Items[i] = pod
				i++
			}
			listOut.Items = listOut.Items[:i]

			expectNoDiff(t, "incorrect list pods", tt.expectPods(namespace), listOut.Items)
		})
	}
}

// RunTestCompactRevision 校验 CompactRevision 与 Exact List 的联动：
// 压缩点之前的 RV 必须报"too old"，且 CompactRevision() 要反映最新压缩点。
func RunTestCompactRevision(ctx context.Context, t *testing.T, store storage.Interface, increaseRV IncreaseRVFunc, compact Compaction) {
	listOut := &example.PodList{}
	if err := store.GetList(ctx, "/pods/", storage.ListOptions{
		Predicate: storage.Everything,
		Recursive: true,
	}, listOut); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	currentRV, err := store.Versioner().ParseResourceVersion(listOut.ResourceVersion)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	compactedRV := store.CompactRevision()
	// 当前 RV 尚未被压缩，否则 Exact 读会立刻失败，后续步骤失去意义。
	if compactedRV >= int64(currentRV) {
		t.Fatalf("Expected current RV not be compacted, current: %d, compacted: %d", currentRV, compactedRV)
	}
	if err := store.GetList(ctx, "/pods/", storage.ListOptions{
		Predicate:            storage.Everything,
		Recursive:            true,
		ResourceVersion:      listOut.ResourceVersion,
		ResourceVersionMatch: metav1.ResourceVersionMatchExact,
	}, listOut); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	increaseRV(ctx, t)
	// 压缩到当前 RV 之后：上面保存的 listOut.ResourceVersion 由此变得过旧。
	expectCompactedRV := currentRV + 1
	compact(ctx, t, fmt.Sprintf("%d", expectCompactedRV))
	if err := store.GetList(ctx, "/pods/", storage.ListOptions{
		Predicate:            storage.Everything,
		Recursive:            true,
		ResourceVersion:      listOut.ResourceVersion,
		ResourceVersionMatch: metav1.ResourceVersionMatchExact,
	}, listOut); err == nil || !strings.Contains(err.Error(), "The resourceVersion for the provided list is too old") {
		t.Errorf(`Expected "The resourceVersion for the provided list is too old", but got error: %v`, err)
	}
	compactedRV = store.CompactRevision()
	if compactedRV != int64(expectCompactedRV) {
		t.Errorf("CompactRevision()=%d, expected: %d", store.CompactRevision(), expectCompactedRV)
	}
}

// RunTestKeySchema 校验 key 格式的前置校验：空 key、"."/".."、缺少资源前缀等
// 必须在触达后端之前就被拒绝，避免把非法路径当作合法 key 写入。
func RunTestKeySchema(ctx context.Context, t *testing.T, store storage.Interface) {
	// Create：非法 key 一律拒绝；"/pods/" 这类"资源前缀"本身以及其下的单段/两段 key 合法。
	createObj := &example.Pod{}
	createOut := &example.Pod{}
	require.ErrorContains(t, store.Create(ctx, "", createObj, createOut, 0), "empty key")
	require.ErrorContains(t, store.Create(ctx, "/", createObj, createOut, 0), "empty key")
	require.ErrorContains(t, store.Create(ctx, ".", createObj, createOut, 0), "invalid key")
	require.ErrorContains(t, store.Create(ctx, "..", createObj, createOut, 0), "invalid key")
	require.ErrorContains(t, store.Create(ctx, "pods", createObj, createOut, 0), "lacks resource prefix")
	require.ErrorContains(t, store.Create(ctx, "/pods", createObj, createOut, 0), "lacks resource prefix")
	require.ErrorContains(t, store.Create(ctx, "/pods.apps", createObj, createOut, 0), "lacks resource prefix")
	require.ErrorContains(t, store.Create(ctx, "/foo/", createObj, createOut, 0), "lacks resource prefix")
	require.NoError(t, store.Create(ctx, "/pods/", createObj, createOut, 0))
	require.NoError(t, store.Create(ctx, "/pods/name", createObj, createOut, 0))
	require.NoError(t, store.Create(ctx, "/pods/namespace", createObj, createOut, 0))
	require.NoError(t, store.Create(ctx, "/pods/namespace/name", createObj, createOut, 0))

	// GetList：注意 "/pods"（无尾斜杠）仅在非递归时非法，递归列表允许用它作为前缀根。
	listOut := &example.PodList{}
	recursiveListOpts := storage.ListOptions{Predicate: storage.Everything, Recursive: true}
	nonRecursiveListOpts := storage.ListOptions{Predicate: storage.Everything, Recursive: false}
	require.ErrorContains(t, store.GetList(ctx, "", recursiveListOpts, listOut), "empty key")
	require.ErrorContains(t, store.GetList(ctx, "/", recursiveListOpts, listOut), "empty key")
	require.ErrorContains(t, store.GetList(ctx, ".", recursiveListOpts, listOut), "invalid key")
	require.ErrorContains(t, store.GetList(ctx, "..", recursiveListOpts, listOut), "invalid key")
	require.ErrorContains(t, store.GetList(ctx, "pods", recursiveListOpts, listOut), "lacks resource prefix")
	require.ErrorContains(t, store.GetList(ctx, "/pods.apps", recursiveListOpts, listOut), "lacks resource prefix")
	require.ErrorContains(t, store.GetList(ctx, "/foo/", recursiveListOpts, listOut), "lacks resource prefix")
	require.ErrorContains(t, store.GetList(ctx, "/pods", nonRecursiveListOpts, listOut), "lacks resource prefix")
	require.NoError(t, store.GetList(ctx, "/pods", recursiveListOpts, listOut))
	require.NoError(t, store.GetList(ctx, "/pods/", recursiveListOpts, listOut))
	require.NoError(t, store.GetList(ctx, "/pods/namespace", recursiveListOpts, listOut))
	require.NoError(t, store.GetList(ctx, "/pods/namespace/name", recursiveListOpts, listOut))

	// Get：与 List 相同的 key 校验规则。
	getOut := &example.Pod{}
	getOpts := storage.GetOptions{}
	require.ErrorContains(t, store.Get(ctx, "", getOpts, getOut), "empty key")
	require.ErrorContains(t, store.Get(ctx, "/", getOpts, getOut), "empty key")
	require.ErrorContains(t, store.Get(ctx, ".", getOpts, getOut), "invalid key")
	require.ErrorContains(t, store.Get(ctx, "..", getOpts, getOut), "invalid key")
	require.ErrorContains(t, store.Get(ctx, "pods", getOpts, getOut), "lacks resource prefix")
	require.ErrorContains(t, store.Get(ctx, "/pods", getOpts, getOut), "lacks resource prefix")
	require.ErrorContains(t, store.Get(ctx, "/pods.apps", getOpts, getOut), "lacks resource prefix")
	require.ErrorContains(t, store.Get(ctx, "/foo/", getOpts, getOut), "lacks resource prefix")
	require.NoError(t, store.Get(ctx, "/pods/", getOpts, getOut))
	require.NoError(t, store.Get(ctx, "/pods/namespace", getOpts, getOut))
	require.NoError(t, store.Get(ctx, "/pods/namespace/name", getOpts, getOut))

	// Watch：非法 key 必须在建立 watcher 之前失败；合法 key 返回可 Stop 的 watcher。
	_, err := store.Watch(ctx, "", recursiveListOpts)
	require.ErrorContains(t, err, "empty key")
	_, err = store.Watch(ctx, "/", recursiveListOpts)
	require.ErrorContains(t, err, "empty key")
	_, err = store.Watch(ctx, ".", recursiveListOpts)
	require.ErrorContains(t, err, "invalid key")
	_, err = store.Watch(ctx, "..", recursiveListOpts)
	require.ErrorContains(t, err, "invalid key")
	_, err = store.Watch(ctx, "pods", recursiveListOpts)
	require.ErrorContains(t, err, "lacks resource prefix")
	_, err = store.Watch(ctx, "/pods.apps", recursiveListOpts)
	require.ErrorContains(t, err, "lacks resource prefix")
	_, err = store.Watch(ctx, "/foo/", recursiveListOpts)
	require.ErrorContains(t, err, "lacks resource prefix")
	_, err = store.Watch(ctx, "/pods", nonRecursiveListOpts)
	require.ErrorContains(t, err, "lacks resource prefix")
	w, err := store.Watch(ctx, "/pods", recursiveListOpts)
	require.NoError(t, err)
	w.Stop()
	require.NoError(t, err)
	w.Stop()
	w, err = store.Watch(ctx, "/pods/namespace", recursiveListOpts)
	require.NoError(t, err)
	w.Stop()
	w, err = store.Watch(ctx, "/pods/namespace/name", recursiveListOpts)
	require.NoError(t, err)
	w.Stop()

	// GuaranteedUpdate：校验在调用 update func 之前完成，避免副作用发生在非法 key 上。
	updateIn := &example.Pod{}
	updateOut := &example.Pod{}
	updateFunc := func(input runtime.Object, res storage.ResponseMeta) (output runtime.Object, ttl *uint64, err error) {
		return updateIn, nil, nil
	}
	require.ErrorContains(t, store.GuaranteedUpdate(ctx, "", updateOut, false, nil, updateFunc, nil), "empty key")
	require.ErrorContains(t, store.GuaranteedUpdate(ctx, "/", updateOut, false, nil, updateFunc, nil), "empty key")
	require.ErrorContains(t, store.GuaranteedUpdate(ctx, ".", updateOut, false, nil, updateFunc, nil), "invalid key")
	require.ErrorContains(t, store.GuaranteedUpdate(ctx, "..", updateOut, false, nil, updateFunc, nil), "invalid key")
	require.ErrorContains(t, store.GuaranteedUpdate(ctx, "pods", updateOut, false, nil, updateFunc, nil), "lacks resource prefix")
	require.ErrorContains(t, store.GuaranteedUpdate(ctx, "/pods", updateOut, false, nil, updateFunc, nil), "lacks resource prefix")
	require.ErrorContains(t, store.GuaranteedUpdate(ctx, "/pods.apps", updateOut, false, nil, updateFunc, nil), "lacks resource prefix")
	require.ErrorContains(t, store.GuaranteedUpdate(ctx, "/foo/", updateOut, false, nil, updateFunc, nil), "lacks resource prefix")
	require.NoError(t, store.GuaranteedUpdate(ctx, "/pods/", updateOut, false, nil, updateFunc, nil))
	require.NoError(t, store.GuaranteedUpdate(ctx, "/pods/namespace", updateOut, false, nil, updateFunc, nil))
	require.NoError(t, store.GuaranteedUpdate(ctx, "/pods/namespace/name", updateOut, false, nil, updateFunc, nil))

	// Delete：注意这里用 "/foo"（无尾斜杠）验证资源前缀校验对删除路径同样生效。
	deleteOut := &example.Pod{}
	deleteFunc := func(ctx context.Context, obj runtime.Object) error {
		return nil
	}
	deleteOpts := storage.DeleteOptions{}
	require.ErrorContains(t, store.Delete(ctx, "", deleteOut, nil, deleteFunc, nil, deleteOpts), "empty key")
	require.ErrorContains(t, store.Delete(ctx, "/", deleteOut, nil, deleteFunc, nil, deleteOpts), "empty key")
	require.ErrorContains(t, store.Delete(ctx, ".", deleteOut, nil, deleteFunc, nil, deleteOpts), "invalid key")
	require.ErrorContains(t, store.Delete(ctx, "..", deleteOut, nil, deleteFunc, nil, deleteOpts), "invalid key")
	require.ErrorContains(t, store.Delete(ctx, "pods", deleteOut, nil, deleteFunc, nil, deleteOpts), "lacks resource prefix")
	require.ErrorContains(t, store.Delete(ctx, "/pods", deleteOut, nil, deleteFunc, nil, deleteOpts), "lacks resource prefix")
	require.ErrorContains(t, store.Delete(ctx, "/pods.apps", deleteOut, nil, deleteFunc, nil, deleteOpts), "lacks resource prefix")
	require.ErrorContains(t, store.Delete(ctx, "/foo", deleteOut, nil, deleteFunc, nil, deleteOpts), "lacks resource prefix")
	require.NoError(t, store.Delete(ctx, "/pods/", deleteOut, nil, deleteFunc, nil, deleteOpts))
	require.NoError(t, store.Delete(ctx, "/pods/namespace", deleteOut, nil, deleteFunc, nil, deleteOpts))
	require.NoError(t, store.Delete(ctx, "/pods/namespace/name", deleteOut, nil, deleteFunc, nil, deleteOpts))
}

// mustParseResourceVersion 用 storage.APIObjectVersioner 解析 RV；
// 解析失败直接 Fatal，因为后续所有断言都依赖一个可比较的整数版本号。
func mustParseResourceVersion(t *testing.T, resourceVersion string) int64 {
	t.Helper()
	versioner := storage.APIObjectVersioner{}
	rv, err := versioner.ParseResourceVersion(resourceVersion)
	if err != nil {
		t.Fatal(err)
	}
	return int64(rv)
}
