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

package pullmanager

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"reflect"
	"slices"
	"strings"
	"testing"

	"k8s.io/klog/v2"
	kubeletconfiginternal "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// testPullRecordsAccessor 是 PullRecordsAccessor 的内存实现，供测试注入错误与检查写入结果。
// nextGetWriteError 同时作用于读和写，便于用单一开关模拟 delegate 故障。
type testPullRecordsAccessor struct {
	intents         map[string]*kubeletconfiginternal.ImagePullIntent
	intentListError error

	pulledRecords          map[string]*kubeletconfiginternal.ImagePulledRecord
	pulledRecordsListError error

	nextGetWriteError error
}

// ListImagePullIntents 返回按镜像名排序的 intent 列表；排序让测试断言不依赖 map 遍历顺序。
func (a *testPullRecordsAccessor) ListImagePullIntents() ([]*kubeletconfiginternal.ImagePullIntent, error) {
	ret := make([]*kubeletconfiginternal.ImagePullIntent, 0, len(a.intents))
	for _, intent := range a.intents {
		ret = append(ret, intent)
	}

	slices.SortFunc(ret, pullIntentsCmp)
	return ret, a.intentListError
}

// ListImagePulledRecords 返回按 ImageRef 排序的记录列表，理由同上。
func (a *testPullRecordsAccessor) ListImagePulledRecords() ([]*kubeletconfiginternal.ImagePulledRecord, error) {
	ret := make([]*kubeletconfiginternal.ImagePulledRecord, 0, len(a.pulledRecords))
	for _, record := range a.pulledRecords {
		ret = append(ret, record)
	}

	slices.SortFunc(ret, pulledRecordsCmp)
	return ret, a.pulledRecordsListError
}

// ImagePullIntentExists 报告 intent 是否存在；注入错误时优先返回错误，模拟 delegate 读取失败。
func (a *testPullRecordsAccessor) ImagePullIntentExists(image string) (bool, error) {
	if a.nextGetWriteError != nil {
		return false, a.nextGetWriteError
	}
	_, exists := a.intents[image]

	return exists, nil
}

// GetImagePulledRecord 返回记录及其是否存在；即便出错也保留 exists，便于测试断言缓存层的行为。
func (a *testPullRecordsAccessor) GetImagePulledRecord(imageRef string) (*kubeletconfiginternal.ImagePulledRecord, bool, error) {
	record, exists := a.pulledRecords[imageRef]
	if a.nextGetWriteError != nil {
		return nil, exists, a.nextGetWriteError
	}
	return record, exists, nil
}

// WriteImagePullIntent 写入 intent；注入错误时直接返回且不修改内存状态，模拟写入失败。
func (a *testPullRecordsAccessor) WriteImagePullIntent(logger klog.Logger, image string) error {
	if a.nextGetWriteError != nil {
		return a.nextGetWriteError
	}

	a.intents[image] = &kubeletconfiginternal.ImagePullIntent{
		Image: image,
	}
	return nil
}

// WriteImagePulledRecord 按 ImageRef 写入记录，语义同 WriteImagePullIntent。
func (a *testPullRecordsAccessor) WriteImagePulledRecord(logger klog.Logger, rec *kubeletconfiginternal.ImagePulledRecord) error {
	if a.nextGetWriteError != nil {
		return a.nextGetWriteError
	}

	a.pulledRecords[rec.ImageRef] = rec
	return nil
}

// DeleteImagePullIntent 删除 intent；注入错误时不删除，用于验证缓存层的回滚/保留逻辑。
func (a *testPullRecordsAccessor) DeleteImagePullIntent(logger klog.Logger, image string) error {
	if a.nextGetWriteError != nil {
		return a.nextGetWriteError
	}

	delete(a.intents, image)
	return nil
}

// DeleteImagePulledRecord 按 ImageRef 删除记录，语义同 DeleteImagePullIntent。
func (a *testPullRecordsAccessor) DeleteImagePulledRecord(logger klog.Logger, imageRef string) error {
	if a.nextGetWriteError != nil {
		return a.nextGetWriteError
	}

	delete(a.pulledRecords, imageRef)
	return nil
}

// withIntentsListError 让后续 ListImagePullIntents 返回指定错误。
func (a *testPullRecordsAccessor) withIntentsListError(err error) *testPullRecordsAccessor {
	a.intentListError = err
	return a
}

// withPulledRecordsListError 让后续 ListImagePulledRecords 返回指定错误。
func (a *testPullRecordsAccessor) withPulledRecordsListError(err error) *testPullRecordsAccessor {
	a.pulledRecordsListError = err
	return a
}

// withNextGetWriteError 让下一次读或写操作失败。
func (a *testPullRecordsAccessor) withNextGetWriteError(err error) *testPullRecordsAccessor {
	a.nextGetWriteError = err
	return a
}

// newTestPullRecordsAccessor 用切片构造内存 accessor，内部转成以镜像名/ImageRef 为键的 map。
func newTestPullRecordsAccessor(
	pullIntents []*kubeletconfiginternal.ImagePullIntent,
	pulledRecords []*kubeletconfiginternal.ImagePulledRecord,
) *testPullRecordsAccessor {
	cachedPullIntents := make(map[string]*kubeletconfiginternal.ImagePullIntent)
	for _, intent := range pullIntents {
		cachedPullIntents[intent.Image] = intent
	}

	cachedPullRecords := make(map[string]*kubeletconfiginternal.ImagePulledRecord)
	for _, record := range pulledRecords {
		cachedPullRecords[record.ImageRef] = record
	}

	return &testPullRecordsAccessor{
		intents:       cachedPullIntents,
		pulledRecords: cachedPullRecords,
	}
}

// recordingPullRecordsAccessor 记录被调用的方法名，用于断言缓存层是否写穿透到 delegate。
type recordingPullRecordsAccessor struct {
	delegate      PullRecordsAccessor
	methodsCalled []string
}

// ListImagePullIntents 记录调用后委托给真实 delegate。
func (a *recordingPullRecordsAccessor) ListImagePullIntents() ([]*kubeletconfiginternal.ImagePullIntent, error) {
	a.methodsCalled = append(a.methodsCalled, "ListImagePullIntents")
	return a.delegate.ListImagePullIntents()
}

// ListImagePulledRecords 记录调用后委托给真实 delegate。
func (a *recordingPullRecordsAccessor) ListImagePulledRecords() ([]*kubeletconfiginternal.ImagePulledRecord, error) {
	a.methodsCalled = append(a.methodsCalled, "ListImagePulledRecords")
	return a.delegate.ListImagePulledRecords()
}

// ImagePullIntentExists 记录调用后委托给真实 delegate。
func (a *recordingPullRecordsAccessor) ImagePullIntentExists(image string) (bool, error) {
	a.methodsCalled = append(a.methodsCalled, "ImagePullIntentExists")
	return a.delegate.ImagePullIntentExists(image)
}

// GetImagePulledRecord 记录调用后委托给真实 delegate。
func (a *recordingPullRecordsAccessor) GetImagePulledRecord(imageRef string) (*kubeletconfiginternal.ImagePulledRecord, bool, error) {
	a.methodsCalled = append(a.methodsCalled, "GetImagePulledRecord")
	return a.delegate.GetImagePulledRecord(imageRef)
}

// WriteImagePullIntent 记录调用后委托给真实 delegate。
func (a *recordingPullRecordsAccessor) WriteImagePullIntent(logger klog.Logger, image string) error {
	a.methodsCalled = append(a.methodsCalled, "WriteImagePullIntent")
	return a.delegate.WriteImagePullIntent(logger, image)
}

// WriteImagePulledRecord 记录调用后委托给真实 delegate。
func (a *recordingPullRecordsAccessor) WriteImagePulledRecord(logger klog.Logger, rec *kubeletconfiginternal.ImagePulledRecord) error {
	a.methodsCalled = append(a.methodsCalled, "WriteImagePulledRecord")
	return a.delegate.WriteImagePulledRecord(logger, rec)
}

// DeleteImagePullIntent 记录调用后委托给真实 delegate。
func (a *recordingPullRecordsAccessor) DeleteImagePullIntent(logger klog.Logger, image string) error {
	a.methodsCalled = append(a.methodsCalled, "DeleteImagePullIntent")
	return a.delegate.DeleteImagePullIntent(logger, image)
}

// DeleteImagePulledRecord 记录调用后委托给真实 delegate。
func (a *recordingPullRecordsAccessor) DeleteImagePulledRecord(logger klog.Logger, imageRef string) error {
	a.methodsCalled = append(a.methodsCalled, "DeleteImagePulledRecord")
	return a.delegate.DeleteImagePulledRecord(logger, imageRef)
}

// TestNewCachedPullRecordsAccessor 验证构造缓存时能从 delegate 预填充数据，并在列表失败或超过容量时正确降级为非权威缓存。
func TestNewCachedPullRecordsAccessor(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 故意超过 50/100 的容量上限，用来覆盖截断路径。
	manyPullIntents := generateTestPullIntents(51)
	slices.SortFunc(manyPullIntents, pullIntentsCmp)

	manyPulledRecords := generateTestPulledRecords(101)
	slices.SortFunc(manyPulledRecords, pulledRecordsCmp)

	tests := []struct {
		name                           string
		delegate                       *testPullRecordsAccessor
		wantIntents                    []*kubeletconfiginternal.ImagePullIntent
		wantIntentsAuthoritative       bool
		wantCacheIntents               map[string]*kubeletconfiginternal.ImagePullIntent
		wantIntentsError               error
		wantPulledRecords              []*kubeletconfiginternal.ImagePulledRecord
		wantPulledRecordsAuthoritative bool
		wantCachePulledRecords         map[string]*kubeletconfiginternal.ImagePulledRecord
		wantPulledRecordsError         error
	}{
		{
			name: "no issues during init",
			delegate: newTestPullRecordsAccessor(
				testPullIntents(),
				testPulledRecords(),
			),
			wantCacheIntents:               pullIntentsToMap(testPullIntents()),
			wantIntents:                    testPullIntentsSorted(),
			wantIntentsAuthoritative:       true,
			wantPulledRecords:              testPulledRecordsSorted(),
			wantPulledRecordsAuthoritative: true,
			wantCachePulledRecords:         pulledRecordsToMap(testPulledRecordsSorted()),
		},
		{
			name: "delegate intents list error",
			delegate: newTestPullRecordsAccessor(
				testPullIntents(),
				testPulledRecords(),
			).withIntentsListError(fmt.Errorf("intentsError")),
			wantIntents:                    testPullIntentsSorted(),
			wantIntentsAuthoritative:       false,
			wantCacheIntents:               pullIntentsToMap(testPullIntents()),
			wantIntentsError:               fmt.Errorf("intentsError"),
			wantPulledRecords:              testPulledRecordsSorted(),
			wantPulledRecordsAuthoritative: true,
			wantCachePulledRecords:         pulledRecordsToMap(testPulledRecordsSorted()),
		},
		{
			name: "delegate pulledRecords list error",
			delegate: newTestPullRecordsAccessor(
				testPullIntents(),
				testPulledRecords(),
			).withPulledRecordsListError(fmt.Errorf("pulledRecordsError")),
			wantIntents:                    testPullIntentsSorted(),
			wantIntentsAuthoritative:       true,
			wantCacheIntents:               pullIntentsToMap(testPullIntents()),
			wantPulledRecords:              testPulledRecordsSorted(),
			wantPulledRecordsAuthoritative: false,
			wantCachePulledRecords:         pulledRecordsToMap(testPulledRecordsSorted()),
			wantPulledRecordsError:         fmt.Errorf("pulledRecordsError"),
		},
		{
			name: "both delegate lists fail during init",
			delegate: newTestPullRecordsAccessor(
				testPullIntents(),
				testPulledRecords(),
			).withPulledRecordsListError(fmt.Errorf("pulledRecordsError")).
				withIntentsListError(fmt.Errorf("intentsError")),
			wantIntents:                    testPullIntentsSorted(),
			wantIntentsAuthoritative:       false,
			wantCacheIntents:               pullIntentsToMap(testPullIntents()),
			wantIntentsError:               fmt.Errorf("intentsError"),
			wantPulledRecords:              testPulledRecordsSorted(),
			wantPulledRecordsAuthoritative: false,
			wantCachePulledRecords:         pulledRecordsToMap(testPulledRecordsSorted()),
			wantPulledRecordsError:         fmt.Errorf("pulledRecordsError"),
		},
		{
			name: "too many intents get cropped",
			delegate: newTestPullRecordsAccessor(
				manyPullIntents,
				testPulledRecords(),
			),
			wantIntents:                    manyPullIntents,
			wantIntentsAuthoritative:       false,
			wantCacheIntents:               pullIntentsToMap(manyPullIntents[:50]),
			wantPulledRecords:              testPulledRecordsSorted(),
			wantPulledRecordsAuthoritative: true,
			wantCachePulledRecords:         pulledRecordsToMap(testPulledRecordsSorted()),
		},
		{
			name: "too many pulled records get cropped",
			delegate: newTestPullRecordsAccessor(
				testPullIntents(),
				manyPulledRecords,
			),
			wantIntents:                    testPullIntentsSorted(),
			wantIntentsAuthoritative:       true,
			wantCacheIntents:               pullIntentsToMap(testPullIntents()),
			wantPulledRecords:              manyPulledRecords,
			wantPulledRecordsAuthoritative: false,
			wantCachePulledRecords:         pulledRecordsToMap(manyPulledRecords[:100]),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotAccessor := NewCachedPullRecordsAccessor(logger, tt.delegate, 50, 100, 10)
			// 断言返回的具体类型，否则后续无法直接检查缓存内部状态。
			gotMeteredAccessor, ok := gotAccessor.(*meteringRecordsAccessor)
			if !ok {
				t.Fatalf("the tested accessor must be a metered records accessor, got %T", gotAccessor)
			}
			// 剥离计量包装层，拿到真正的缓存实现来检查预填充结果。
			inMemAccessor, ok := gotMeteredAccessor.sizeExposedPullRecordsAccessor.(*cachedPullRecordsAccessor)
			if !ok {
				t.Fatalf("the metered accessor's delegate is not an inMemAccesor: %T", gotMeteredAccessor.sizeExposedPullRecordsAccessor)
			}

			// 预填充的缓存内容必须与期望一致；截断场景下期望值只包含容量内的元素。
			expectedCachedIntents := tt.wantCacheIntents
			if err := cmpRecordsMapAndCache(expectedCachedIntents, inMemAccessor.intents); err != nil {
				t.Errorf("NewCachedPullRecordsAccessor cache does not match: %v", err)
			}

			gotIntents, intentsErr := gotAccessor.ListImagePullIntents()
			if !reflect.DeepEqual(gotIntents, tt.wantIntents) {
				t.Errorf("NewCachedPullRecordsAccessor().ListImagePullIntents() = %v, want %v", gotIntents, tt.wantIntents)
			}

			// 列表错误需要原样透传，调用方据此判断缓存是否权威。
			if !cmpErrorStrings(intentsErr, tt.wantIntentsError) {
				t.Errorf("NewCachedPullRecordsAccessor().ListImagePullIntents() errors don't match = %v, want %v", intentsErr, tt.wantIntentsError)
			}
			expectedPulledRecords := tt.wantCachePulledRecords
			if err := cmpRecordsMapAndCache(expectedPulledRecords, inMemAccessor.pulledRecords); err != nil {
				t.Errorf("NewCachedPullRecordsAccessor cache does not match: %v", err)
			}

			gotPulledRecords, pulledRecordsErr := gotAccessor.ListImagePulledRecords()
			if !reflect.DeepEqual(gotPulledRecords, tt.wantPulledRecords) {
				t.Errorf("NewCachedPullRecordsAccessor().ListImagePulledRecords() = %v, want %v", gotPulledRecords, tt.wantPulledRecords)
			}

			if !cmpErrorStrings(pulledRecordsErr, tt.wantPulledRecordsError) {
				t.Errorf("NewCachedPullRecordsAccessor().ListImagePullIntents() errors don't match = %v, want %v", pulledRecordsErr, tt.wantPulledRecordsError)
			}

			// 权威标志决定后续读取能否跳过 delegate；列表失败或截断时必须为 false。
			if gotIntentsAuthoritative := inMemAccessor.intents.authoritative.Load(); gotIntentsAuthoritative != tt.wantIntentsAuthoritative {
				t.Errorf("NewCachedPullRecordsAccessor().intents.authoritative = %v, want %v", gotIntentsAuthoritative, tt.wantIntentsAuthoritative)
			}

			if gotPulledRecordsAuthoritative := inMemAccessor.pulledRecords.authoritative.Load(); gotPulledRecordsAuthoritative != tt.wantPulledRecordsAuthoritative {
				t.Errorf("NewCachedPullRecordsAccessor().pulledRecords.authoritative = %v, want %v", gotPulledRecordsAuthoritative, tt.wantPulledRecordsAuthoritative)
			}
		})
	}
}

// Test_cachedPullRecordsAccessor_ListImagePullIntents 验证缓存权威时直接返回缓存，非权威时回源 delegate 并用结果刷新缓存。
func Test_cachedPullRecordsAccessor_ListImagePullIntents(t *testing.T) {
	tests := []struct {
		name                 string
		delegate             *testPullRecordsAccessor
		inMemCache           *lruCache[string, kubeletconfiginternal.ImagePullIntent]
		initAuthoritative    bool
		wantAuthoritative    bool
		wantListIntents      []*kubeletconfiginternal.ImagePullIntent
		wantListIntentsError error
	}{
		{
			name: "successful intents list from delegate, start authoritative",
			delegate: newTestPullRecordsAccessor(
				testPullIntents(),
				nil,
			),
			inMemCache: mapToCache(pullIntentsToMap([]*kubeletconfiginternal.ImagePullIntent{
				{Image: "unexpected for this test"},
			})),
			initAuthoritative: true,
			wantAuthoritative: true,
			wantListIntents:   testPullIntentsSorted(),
		},
		{
			name: "successful intents list from delegate, start non-authoritative",
			delegate: newTestPullRecordsAccessor(
				testPullIntents(),
				nil,
			),
			inMemCache: mapToCache(pullIntentsToMap([]*kubeletconfiginternal.ImagePullIntent{
				{Image: "this would be wrong"},
			})),
			initAuthoritative: false,
			wantAuthoritative: true,
			wantListIntents:   testPullIntentsSorted(),
		},
		{
			name: "partial intents listed from delegate with an error, start authoritative",
			delegate: newTestPullRecordsAccessor(
				testPullIntents()[:2],
				nil,
			).withIntentsListError(fmt.Errorf("only partial list was returned")),
			inMemCache: mapToCache(pullIntentsToMap([]*kubeletconfiginternal.ImagePullIntent{
				{Image: "unexpected for this test"},
			})),
			initAuthoritative:    true,
			wantAuthoritative:    true,
			wantListIntents:      testPullIntentsSorted()[:2],
			wantListIntentsError: fmt.Errorf("only partial list was returned"),
		},
		{
			name: "partial intents listed from delegate with an error, start non-authoritative",
			delegate: newTestPullRecordsAccessor(
				testPullIntents(),
				nil,
			).withIntentsListError(fmt.Errorf("only partial list was returned")),
			inMemCache: mapToCache(pullIntentsToMap([]*kubeletconfiginternal.ImagePullIntent{
				{Image: "this would be wrong"},
			})),
			initAuthoritative:    false,
			wantAuthoritative:    false,
			wantListIntents:      testPullIntentsSorted(),
			wantListIntentsError: fmt.Errorf("only partial list was returned"),
		},
		{
			name:              "delegate returns empty list",
			delegate:          newTestPullRecordsAccessor(nil, nil),
			inMemCache:        mapToCache(pullIntentsToMap([]*kubeletconfiginternal.ImagePullIntent{})),
			initAuthoritative: true,
			wantAuthoritative: true,
			wantListIntents:   []*kubeletconfiginternal.ImagePullIntent{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 直接构造内部结构体，绕过构造函数以精确控制初始缓存与权威标志。
			cachedAccessor := &cachedPullRecordsAccessor{
				delegate:     tt.delegate,
				intentsLocks: NewStripedLockSet(10),
				intents:      tt.inMemCache,
			}
			cachedAccessor.intents.authoritative.Store(tt.initAuthoritative)

			gotIntents, gotErr := cachedAccessor.ListImagePullIntents()
			if !reflect.DeepEqual(gotIntents, tt.wantListIntents) {
				t.Errorf("ListImagePullIntents() = %v, want %v", gotIntents, tt.wantListIntents)
			}

			// 部分列表 + 错误时仍要返回已拿到的数据，同时把错误传给调用方。
			if !cmpErrorStrings(gotErr, tt.wantListIntentsError) {
				t.Errorf("ListImagePullIntents() error = %v, want %v", gotErr, tt.wantListIntentsError)
			}

			// 只有成功拿到完整列表才能升级为权威缓存。
			if gotAuthoritative := cachedAccessor.intents.authoritative.Load(); gotAuthoritative != tt.wantAuthoritative {
				t.Errorf("ListImagePullIntents() cache authoritative = %v, want %v", gotAuthoritative, tt.wantAuthoritative)
			}
		})
	}
}

// Test_cachedPullRecordsAccessor_ListImagePulledRecords 验证 pulled record 列表的回源与权威标志切换，行为与 intents 列表一致。
func Test_cachedPullRecordsAccessor_ListImagePulledRecords(t *testing.T) {
	tests := []struct {
		name                       string
		delegate                   *testPullRecordsAccessor
		inMemCache                 *lruCache[string, kubeletconfiginternal.ImagePulledRecord]
		initAuthoritative          bool
		wantAuthoritative          bool
		wantListPulledRecords      []*kubeletconfiginternal.ImagePulledRecord
		wantListPulledRecordsError error
	}{
		{
			name: "initial cache error gets cleared by successful imagePulledRecords list from delegate",
			delegate: newTestPullRecordsAccessor(
				nil,
				testPulledRecords(),
			),
			inMemCache: mapToCache(pulledRecordsToMap([]*kubeletconfiginternal.ImagePulledRecord{
				{ImageRef: "this would be wrong"},
			})),
			initAuthoritative:     false,
			wantAuthoritative:     true,
			wantListPulledRecords: testPulledRecordsSorted(),
		},
		{
			name: "initial cache error, partial imagePulledRecords listed from delegate with an error",
			delegate: newTestPullRecordsAccessor(
				nil,
				testPulledRecords(),
			).withPulledRecordsListError(fmt.Errorf("only partial list was returned")),
			inMemCache: mapToCache(pulledRecordsToMap([]*kubeletconfiginternal.ImagePulledRecord{
				{ImageRef: "this would be wrong"},
			})),
			initAuthoritative:          false,
			wantAuthoritative:          false,
			wantListPulledRecords:      testPulledRecordsSorted(),
			wantListPulledRecordsError: fmt.Errorf("only partial list was returned"),
		},
		{
			name:     "delegate returns empty list",
			delegate: newTestPullRecordsAccessor(nil, nil),
			inMemCache: mapToCache(pulledRecordsToMap([]*kubeletconfiginternal.ImagePulledRecord{
				{ImageRef: "unexpected for this test"},
			})),
			initAuthoritative:     true,
			wantAuthoritative:     true,
			wantListPulledRecords: []*kubeletconfiginternal.ImagePulledRecord{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cachedAccessor := &cachedPullRecordsAccessor{
				delegate:           tt.delegate,
				pulledRecordsLocks: NewStripedLockSet(10),
				pulledRecords:      tt.inMemCache,
			}
			cachedAccessor.pulledRecords.authoritative.Store(tt.initAuthoritative)

			gotPulledRecords, gotErr := cachedAccessor.ListImagePulledRecords()
			if !reflect.DeepEqual(gotPulledRecords, tt.wantListPulledRecords) {
				t.Errorf("ListImagePulledRecords() = %v, want %v", gotPulledRecords, tt.wantListPulledRecords)
			}

			// 即使返回部分数据，错误也必须透传，避免调用方误以为列表完整。
			if !cmpErrorStrings(gotErr, tt.wantListPulledRecordsError) {
				t.Errorf("ListImagePulledRecords() error = %v, want %v", gotErr, tt.wantListPulledRecordsError)
			}

			if gotAuthoritative := cachedAccessor.pulledRecords.authoritative.Load(); gotAuthoritative != tt.wantAuthoritative {
				t.Errorf("ListImagePulledRecords() cache authoritative = %v, want %v", gotAuthoritative, tt.wantAuthoritative)
			}
		})
	}
}

// Test_cachedPullRecordsAccessor_ImagePullIntentExists 验证缓存权威时以缓存为准，非权威时回源 delegate 并回填缓存。
func Test_cachedPullRecordsAccessor_ImagePullIntentExists(t *testing.T) {
	tests := []struct {
		name              string
		delegate          *testPullRecordsAccessor
		initialCache      *lruCache[string, kubeletconfiginternal.ImagePullIntent]
		initAuthoritative bool
		wantAuthoritative bool
		inputImage        string
		want              bool
		wantErr           error
		wantCacheIntents  map[string]*kubeletconfiginternal.ImagePullIntent
	}{
		{
			name: "intent exists in cache",
			delegate: newTestPullRecordsAccessor(nil, nil).
				withNextGetWriteError(fmt.Errorf("should not have occurred")),
			initialCache:      mapToCache(pullIntentsToMap(testPullIntents())),
			initAuthoritative: true,
			wantAuthoritative: true,
			inputImage:        "test.repo/org1/project1:tag",
			want:              true,
			wantCacheIntents:  pullIntentsToMap(testPullIntents()),
		},
		{
			name:     "intent exists in delegate, nonauthoritative cache gets updated",
			delegate: newTestPullRecordsAccessor(testPullIntents(), nil),
			initialCache: mapToCache(pullIntentsToMap([]*kubeletconfiginternal.ImagePullIntent{
				{Image: "test.repo/org1/project3:tag"},
				{Image: "test.repo/org2/project1:tag"},
			})),
			initAuthoritative: false,
			wantAuthoritative: false,
			inputImage:        "test.repo/org1/project1:tag",
			want:              true,
			wantCacheIntents: pullIntentsToMap([]*kubeletconfiginternal.ImagePullIntent{
				{Image: "test.repo/org1/project3:tag"},
				{Image: "test.repo/org2/project1:tag"},
				{Image: "test.repo/org1/project1:tag"},
			}),
		},
		{
			name:     "intent exists in delegate, authoritative cache sends not found", // 现实中不会发生，仅用于验证缓存权威时不会回源
			delegate: newTestPullRecordsAccessor(testPullIntents(), nil),
			initialCache: mapToCache(pullIntentsToMap([]*kubeletconfiginternal.ImagePullIntent{
				{Image: "test.repo/org1/project3:tag"},
				{Image: "test.repo/org2/project1:tag"},
			})),
			initAuthoritative: true,
			wantAuthoritative: true,
			inputImage:        "test.repo/org1/project1:tag",
			want:              false,
			wantCacheIntents: pullIntentsToMap([]*kubeletconfiginternal.ImagePullIntent{
				{Image: "test.repo/org1/project3:tag"},
				{Image: "test.repo/org2/project1:tag"},
			}),
		},
		{
			name:              "intent does not exist in cache or delegate",
			delegate:          newTestPullRecordsAccessor(testPullIntents(), nil),
			initialCache:      mapToCache(pullIntentsToMap(testPullIntents())),
			initAuthoritative: false,
			wantAuthoritative: false,
			inputImage:        "test.repo/org1/doesnotexist:tag",
			want:              false,
			wantCacheIntents:  pullIntentsToMap(testPullIntents()),
		},
		{
			name: "intent does not exist in cache, delegate returns error on GET",
			delegate: newTestPullRecordsAccessor(testPullIntents(), nil).
				withNextGetWriteError(fmt.Errorf("record malformed")),
			initialCache:      mapToCache(pullIntentsToMap(testPullIntents())),
			initAuthoritative: false,
			wantAuthoritative: false,
			inputImage:        "test.repo/org1/doesnotexist:tag",
			want:              false,
			wantErr:           fmt.Errorf("record malformed"),
			wantCacheIntents:  pullIntentsToMap(testPullIntents()),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &cachedPullRecordsAccessor{
				delegate:     tt.delegate,
				intentsLocks: NewStripedLockSet(10),
				intents:      tt.initialCache,
			}
			c.intents.authoritative.Store(tt.initAuthoritative)

			got, err := c.ImagePullIntentExists(tt.inputImage)
			// 先断言错误再断言结果，避免在错误路径上误判返回值含义。
			if !cmpErrorStrings(err, tt.wantErr) {
				t.Errorf("cachedPullRecordsAccessor.ImagePullIntentExists() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("cachedPullRecordsAccessor.ImagePullIntentExists() = %v, want %v", got, tt.want)
			}

			// 非权威缓存回源后应把结果回填，因此最终缓存内容可能比初始状态多。
			if err := cmpRecordsMapAndCache(tt.wantCacheIntents, c.intents); err != nil {
				t.Errorf("cachedPullRecordsAccessor.ImagePullIntentExists() expected cache state differs: %v", err)
			}

			if gotAuthoritative := c.intents.authoritative.Load(); gotAuthoritative != tt.wantAuthoritative {
				t.Errorf("cachedPullRecordsAccessor.ImagePullIntentExists() cache authoritative = %v, want %v", gotAuthoritative, tt.wantAuthoritative)
			}
		})
	}
}

// Test_cachedPullRecordsAccessor_WriteImagePullIntent 验证写入总是穿透到 delegate，并在缓存超出容量时降级为非权威。
func Test_cachedPullRecordsAccessor_WriteImagePullIntent(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 正好填满缓存容量，再写一条就会触发淘汰。
	justEnoughIntents := generateTestPullIntents(100)

	tests := []struct {
		name                string
		delegate            *recordingPullRecordsAccessor
		initialCache        *lruCache[string, kubeletconfiginternal.ImagePullIntent]
		initAuthoritative   bool
		wantAuthoritative   bool
		inputImage          string
		wantErr             error
		wantCacheIntents    map[string]*kubeletconfiginternal.ImagePullIntent
		wantDelegateIntents *lruCache[string, kubeletconfiginternal.ImagePullIntent]
		wantRecordedMethods []string
	}{
		{
			name: "successful write through an empty cache to delegate",
			delegate: &recordingPullRecordsAccessor{
				delegate: newTestPullRecordsAccessor(nil, nil),
			},
			initialCache:      newLRUCache[string, kubeletconfiginternal.ImagePullIntent](100),
			initAuthoritative: true,
			wantAuthoritative: true,
			inputImage:        "test.repo/org1/project1:tag",
			wantCacheIntents: pullIntentsToMap([]*kubeletconfiginternal.ImagePullIntent{
				{Image: "test.repo/org1/project1:tag"},
			}),
			wantRecordedMethods: []string{"WriteImagePullIntent"},
		},
		{
			name: "rewrite cached intent, write through to delegate",
			delegate: &recordingPullRecordsAccessor{
				delegate: newTestPullRecordsAccessor(nil, nil),
			},
			initialCache: mapToCache(map[string]*kubeletconfiginternal.ImagePullIntent{
				"test.repo/org1/project1:tag": {Image: "something else"}, // 非真实场景，仅用于验证缓存可被覆盖
			}),
			initAuthoritative: false,
			wantAuthoritative: false,
			inputImage:        "test.repo/org1/project1:tag",
			wantCacheIntents: pullIntentsToMap([]*kubeletconfiginternal.ImagePullIntent{
				{Image: "test.repo/org1/project1:tag"},
			}),
			wantRecordedMethods: []string{"WriteImagePullIntent"},
		},
		{
			name: "write even though the very same intent is already in the cache",
			delegate: &recordingPullRecordsAccessor{
				delegate: newTestPullRecordsAccessor(nil, nil),
			},
			initialCache: mapToCache(map[string]*kubeletconfiginternal.ImagePullIntent{
				"test.repo/org1/project1:tag": {Image: "test.repo/org1/project1:tag"},
			}),
			inputImage: "test.repo/org1/project1:tag",
			wantCacheIntents: pullIntentsToMap([]*kubeletconfiginternal.ImagePullIntent{
				{Image: "test.repo/org1/project1:tag"},
			}),
			wantRecordedMethods: []string{"WriteImagePullIntent"},
		},
		{
			name: "write to delegate returns error",
			delegate: &recordingPullRecordsAccessor{
				delegate: newTestPullRecordsAccessor(nil, nil).withNextGetWriteError(fmt.Errorf("write error")),
			},
			initialCache:        newLRUCache[string, kubeletconfiginternal.ImagePullIntent](100),
			inputImage:          "test.repo/org1/project1:tag",
			wantCacheIntents:    map[string]*kubeletconfiginternal.ImagePullIntent{},
			wantErr:             fmt.Errorf("write error"),
			wantRecordedMethods: []string{"WriteImagePullIntent"},
		},
		{
			name: "write to cache that's already populated",
			delegate: &recordingPullRecordsAccessor{
				delegate: newTestPullRecordsAccessor(nil, nil),
			},
			initialCache: mapToCache(pullIntentsToMap(testPullIntents())),
			inputImage:   "test.repo/org1/newimage:tag",
			wantCacheIntents: pullIntentsToMap(
				append(testPullIntents(), &kubeletconfiginternal.ImagePullIntent{Image: "test.repo/org1/newimage:tag"}),
			),
			wantRecordedMethods: []string{"WriteImagePullIntent"},
		},
		{
			name: "exceeding the cache size turns it from authoritative to non-authoritative",
			delegate: &recordingPullRecordsAccessor{
				delegate: newTestPullRecordsAccessor(nil, nil),
			},
			initialCache:      listToCache(justEnoughIntents, pullIntentToCacheKey),
			initAuthoritative: true,
			wantAuthoritative: false,
			inputImage:        "new.repo/neworg/newimage:tag",
			wantCacheIntents: pullIntentsToMap(
				append(justEnoughIntents[1:], &kubeletconfiginternal.ImagePullIntent{Image: "new.repo/neworg/newimage:tag"}),
			),
			wantRecordedMethods: []string{"WriteImagePullIntent"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &cachedPullRecordsAccessor{
				delegate:     tt.delegate,
				intentsLocks: NewStripedLockSet(10),
				intents:      tt.initialCache,
			}
			c.intents.authoritative.Store(tt.initAuthoritative)

			if err := c.WriteImagePullIntent(logger, tt.inputImage); !cmpErrorStrings(err, tt.wantErr) {
				t.Errorf("cachedPullRecordsAccessor.WriteImagePullIntent() error = %v, wantErr %v", err, tt.wantErr)
			}

			// 写入失败时缓存不应留下部分状态，因此期望缓存可能仍为空。
			if err := cmpRecordsMapAndCache(tt.wantCacheIntents, c.intents); err != nil {
				t.Errorf("cachedPullRecordsAccessor.ImagePullIntentExists() expected cache state differs: %v", err)
			}

			// 断言写穿透：即便缓存命中，也必须调用 delegate 的 WriteImagePullIntent。
			if !reflect.DeepEqual(tt.delegate.methodsCalled, tt.wantRecordedMethods) {
				t.Errorf("cachedPullRecordsAccessor.WriteImagePullIntent() recorded methods = %v, want %v", tt.delegate.methodsCalled, tt.wantRecordedMethods)
			}

			// 发生淘汰后缓存不再完整，权威标志必须被清除。
			if gotAuthoritative := c.intents.authoritative.Load(); gotAuthoritative != tt.wantAuthoritative {
				t.Errorf("cachedPullRecordsAccessor.WriteImagePullIntent() cache authoritative = %v, want %v", gotAuthoritative, tt.wantAuthoritative)
			}
		})
	}
}

// Test_cachedPullRecordsAccessor_WriteImagePulledRecord 验证 pulled record 的写穿透与容量淘汰，行为与 intent 写入一致。
func Test_cachedPullRecordsAccessor_WriteImagePulledRecord(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	// 正好填满缓存容量，用于触发 LRU 淘汰。
	justEnoughPulledRecords := generateTestPulledRecords(100)

	tests := []struct {
		name                   string
		delegate               *recordingPullRecordsAccessor
		initialCache           *lruCache[string, kubeletconfiginternal.ImagePulledRecord]
		initAuthoritative      bool
		wantAuthoritative      bool
		inputRecord            *kubeletconfiginternal.ImagePulledRecord
		wantErr                error
		wantCachePulledRecords map[string]*kubeletconfiginternal.ImagePulledRecord
		wantRecordedMethods    []string
	}{
		{
			name: "successful write through an empty cache to delegate",
			delegate: &recordingPullRecordsAccessor{
				delegate: newTestPullRecordsAccessor(nil, nil),
			},
			initialCache: newLRUCache[string, kubeletconfiginternal.ImagePulledRecord](100),
			inputRecord: &kubeletconfiginternal.ImagePulledRecord{
				ImageRef: "someref-1",
			},
			initAuthoritative: true,
			wantAuthoritative: true,
			wantCachePulledRecords: pulledRecordsToMap([]*kubeletconfiginternal.ImagePulledRecord{
				{ImageRef: "someref-1"},
			}),
			wantRecordedMethods: []string{"WriteImagePulledRecord"},
		},
		{
			name: "rewrite cached pulled record, write through to delegate",
			delegate: &recordingPullRecordsAccessor{
				delegate: newTestPullRecordsAccessor(nil, nil),
			},
			initialCache: mapToCache(map[string]*kubeletconfiginternal.ImagePulledRecord{
				"someref-1": {
					ImageRef: "someref-1",
					CredentialMapping: map[string]kubeletconfiginternal.ImagePullCredentials{
						"test.repo/org1/project1": {
							NodePodsAccessible: true,
						},
					},
				},
			}),
			inputRecord: &kubeletconfiginternal.ImagePulledRecord{
				ImageRef: "someref-1",
				CredentialMapping: map[string]kubeletconfiginternal.ImagePullCredentials{
					"test.repo/org1/project1": {
						NodePodsAccessible: true,
					},
					"test.repo/org2/project1": {
						KubernetesSecrets: []kubeletconfiginternal.ImagePullSecret{
							{Namespace: "ns1", Name: "secret1", UID: "uid1", CredentialHash: "somehash"},
						},
					},
				},
			},
			initAuthoritative: false,
			wantAuthoritative: false,
			wantCachePulledRecords: pulledRecordsToMap([]*kubeletconfiginternal.ImagePulledRecord{
				{
					ImageRef: "someref-1",
					CredentialMapping: map[string]kubeletconfiginternal.ImagePullCredentials{
						"test.repo/org1/project1": {
							NodePodsAccessible: true,
						},
						"test.repo/org2/project1": {
							KubernetesSecrets: []kubeletconfiginternal.ImagePullSecret{
								{Namespace: "ns1", Name: "secret1", UID: "uid1", CredentialHash: "somehash"},
							},
						},
					},
				},
			}),
			wantRecordedMethods: []string{"WriteImagePulledRecord"},
		},
		{
			name: "write even though the same pulled record is already in the cache",
			delegate: &recordingPullRecordsAccessor{
				delegate: newTestPullRecordsAccessor(nil, nil),
			},
			initialCache: mapToCache(map[string]*kubeletconfiginternal.ImagePulledRecord{
				"someref-1": {ImageRef: "someref-1"},
			}),
			inputRecord: &kubeletconfiginternal.ImagePulledRecord{
				ImageRef: "someref-1",
			},
			wantCachePulledRecords: pulledRecordsToMap([]*kubeletconfiginternal.ImagePulledRecord{
				{ImageRef: "someref-1"},
			}),
			wantRecordedMethods: []string{"WriteImagePulledRecord"},
		},
		{
			name: "write to delegate returns error",
			delegate: &recordingPullRecordsAccessor{
				delegate: newTestPullRecordsAccessor(nil, nil).withNextGetWriteError(fmt.Errorf("write error")),
			},
			initialCache: newLRUCache[string, kubeletconfiginternal.ImagePulledRecord](100),
			inputRecord: &kubeletconfiginternal.ImagePulledRecord{
				ImageRef: "someref-1",
			},
			wantCachePulledRecords: map[string]*kubeletconfiginternal.ImagePulledRecord{},
			wantErr:                fmt.Errorf("write error"),
			wantRecordedMethods:    []string{"WriteImagePulledRecord"},
		},
		{
			name: "write to cache that's already populated",
			delegate: &recordingPullRecordsAccessor{
				delegate: newTestPullRecordsAccessor(nil, nil),
			},
			initialCache: mapToCache(pulledRecordsToMap(testPulledRecords())),
			inputRecord: &kubeletconfiginternal.ImagePulledRecord{
				ImageRef: "newref",
			},
			wantCachePulledRecords: pulledRecordsToMap(
				append(testPulledRecords(), &kubeletconfiginternal.ImagePulledRecord{ImageRef: "newref"}),
			),
			wantRecordedMethods: []string{"WriteImagePulledRecord"},
		},
		{
			name: "exceeding the cache size turns it from authoritative to non-authoritative",
			delegate: &recordingPullRecordsAccessor{
				delegate: newTestPullRecordsAccessor(nil, nil),
			},
			initialCache:      listToCache(justEnoughPulledRecords, pulledRecordToCacheKey),
			initAuthoritative: true,
			wantAuthoritative: false,
			inputRecord: &kubeletconfiginternal.ImagePulledRecord{
				ImageRef: "newref",
			},
			wantCachePulledRecords: pulledRecordsToMap(
				append(justEnoughPulledRecords[1:], &kubeletconfiginternal.ImagePulledRecord{
					ImageRef: "newref",
				}),
			),
			wantRecordedMethods: []string{"WriteImagePulledRecord"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &cachedPullRecordsAccessor{
				delegate:           tt.delegate,
				pulledRecordsLocks: NewStripedLockSet(10),
				pulledRecords:      tt.initialCache,
			}
			c.pulledRecords.authoritative.Store(tt.initAuthoritative)

			if err := c.WriteImagePulledRecord(logger, tt.inputRecord); !cmpErrorStrings(err, tt.wantErr) {
				t.Errorf("cachedPullRecordsAccessor.WriteImagePulledRecord() error = %v, wantErr %v", err, tt.wantErr)
			}

			// 写入失败时缓存保持不变，成功时按 ImageRef 覆盖旧记录。
			if err := cmpRecordsMapAndCache(tt.wantCachePulledRecords, c.pulledRecords); err != nil {
				t.Errorf("cachedPullRecordsAccessor.WriteImagePulledRecord() expected cache state differs: %v", err)
			}

			// 与 intent 写入一样，必须记录到 delegate 调用。
			if !reflect.DeepEqual(tt.delegate.methodsCalled, tt.wantRecordedMethods) {
				t.Errorf("cachedPullRecordsAccessor.WriteImagePulledRecord() recorded methods = %v, want %v", tt.delegate.methodsCalled, tt.wantRecordedMethods)
			}

			if gotAuthoritative := c.pulledRecords.authoritative.Load(); gotAuthoritative != tt.wantAuthoritative {
				t.Errorf("cachedPullRecordsAccessor.WriteImagePulledRecord() cache authoritative = %v, want %v", gotAuthoritative, tt.wantAuthoritative)
			}
		})
	}
}

// Test_cachedPullRecordsAccessor_DeleteImagePullIntent 验证删除同时作用于缓存与 delegate，且不改变缓存权威标志。
func Test_cachedPullRecordsAccessor_DeleteImagePullIntent(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	tests := []struct {
		name         string
		delegate     *testPullRecordsAccessor
		initialCache *lruCache[string, kubeletconfiginternal.ImagePullIntent]
		inputImage   string
		wantErr      error
		wantIntents  map[string]*kubeletconfiginternal.ImagePullIntent
	}{
		{
			name:         "delete an existing intent from the cache",
			delegate:     newTestPullRecordsAccessor(testPullIntents(), nil),
			initialCache: mapToCache(pullIntentsToMap(testPullIntents())),
			inputImage:   "test.repo/org1/project1:tag",
			wantIntents:  deleteFromStringMap(pullIntentsToMap(testPullIntents()), "test.repo/org1/project1:tag"),
		},
		{
			name:         "attempt to delete a non-existent intent from the cache",
			delegate:     newTestPullRecordsAccessor(testPullIntents(), nil),
			initialCache: mapToCache(pullIntentsToMap(testPullIntents())),
			inputImage:   "test.repo/org1/doesnotexist:tag",
			wantIntents:  pullIntentsToMap(testPullIntents()),
		},
		{
			name:         "error deleting an intent from delegate",
			delegate:     newTestPullRecordsAccessor(testPullIntents(), nil).withNextGetWriteError(fmt.Errorf("error deleting intent")),
			initialCache: mapToCache(pullIntentsToMap(testPullIntents())),
			inputImage:   "test.repo/org1/project1:tag",
			wantIntents:  pullIntentsToMap(testPullIntents()),
			wantErr:      fmt.Errorf("error deleting intent"),
		},
	}
	for _, tt := range tests {
		// 删除语义与权威标志无关，因此两种初始状态都要跑一遍。
		for _, initAuthoritative := range []bool{true, false} {
			tt.name += fmt.Sprintf(", authoritative=%v", initAuthoritative)
			t.Run(tt.name, func(t *testing.T) {
				c := &cachedPullRecordsAccessor{
					delegate:     tt.delegate,
					intentsLocks: NewStripedLockSet(10),
					intents:      tt.initialCache,
				}
				c.intents.authoritative.Store(initAuthoritative)

				if err := c.DeleteImagePullIntent(logger, tt.inputImage); !cmpErrorStrings(err, tt.wantErr) {
					t.Errorf("cachedPullRecordsAccessor.DeleteImagePullIntent() error = %v, wantErr %v", err, tt.wantErr)
				}

				// 删除完成后不能残留 eviction 保护键，否则该 key 会被永久保留。
				c.intents.deletingKeys.Range(func(key any, _ any) bool {
					t.Errorf("key '%v' should no longer be present in the cache.ignoreEvictionKeys map", key)
					return true
				})

				if err := cmpRecordsMapAndCache(tt.wantIntents, c.intents); err != nil {
					t.Errorf("cachedPullRecordsAccessor.DeleteImagePullIntent() expected cache state differs: %v", err)
				}

				// 缓存与 delegate 必须同时删除，保证二者最终一致。
				if !reflect.DeepEqual(tt.delegate.intents, tt.wantIntents) {
					t.Errorf("cachedPullRecordsAccessor.DeleteImagePullIntent() delegate intents = %v, want %v", tt.delegate.intents, tt.wantIntents)
				}

				// 删除单个条目不影响缓存完整性，权威标志应保持原值。
				if gotAuthoritative := c.intents.authoritative.Load(); gotAuthoritative != initAuthoritative {
					t.Errorf("cachedPullRecordsAccessor.DeleteImagePullIntent() should not cause authoritative changes = %v, want %v", gotAuthoritative, initAuthoritative)
				}
			})
		}
	}
}

// Test_cachedPullRecordsAccessor_DeleteImagePulledRecord 验证 pulled record 删除的缓存/delegate 一致性与权威标志不变，行为与 intent 删除一致。
func Test_cachedPullRecordsAccessor_DeleteImagePulledRecord(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	tests := []struct {
		name              string
		delegate          *testPullRecordsAccessor
		initialCache      *lruCache[string, kubeletconfiginternal.ImagePulledRecord]
		imageRef          string
		wantErr           error
		wantPulledRecords map[string]*kubeletconfiginternal.ImagePulledRecord
	}{
		{
			name:              "delete an existing pulled record from the cache",
			delegate:          newTestPullRecordsAccessor(nil, testPulledRecords()),
			initialCache:      mapToCache(pulledRecordsToMap(testPulledRecords())),
			imageRef:          "ref-22",
			wantPulledRecords: deleteFromStringMap(pulledRecordsToMap(testPulledRecords()), "ref-22"),
		},
		{
			name:              "attempt to delete a non-existent imagePulledRecord from the cache",
			delegate:          newTestPullRecordsAccessor(nil, testPulledRecords()),
			initialCache:      mapToCache(pulledRecordsToMap(testPulledRecords())),
			imageRef:          "non-existent-ref",
			wantPulledRecords: pulledRecordsToMap(testPulledRecords()),
		},
		{
			name:              "error deleting an imagePulledRecord from delegate",
			delegate:          newTestPullRecordsAccessor(nil, testPulledRecords()).withNextGetWriteError(fmt.Errorf("error deleting imagePulledRecord")),
			initialCache:      mapToCache(pulledRecordsToMap(testPulledRecords())),
			imageRef:          "ref-22",
			wantPulledRecords: pulledRecordsToMap(testPulledRecords()),
			wantErr:           fmt.Errorf("error deleting imagePulledRecord"),
		},
	}
	for _, tt := range tests {
		// 同 intent 删除：权威与非权威两种起始状态都需要覆盖。
		for _, initAuthoritative := range []bool{true, false} {
			tt.name += fmt.Sprintf(", authoritative=%v", initAuthoritative)
			t.Run(tt.name, func(t *testing.T) {
				c := &cachedPullRecordsAccessor{
					delegate:           tt.delegate,
					pulledRecordsLocks: NewStripedLockSet(10),
					pulledRecords:      tt.initialCache,
				}
				c.pulledRecords.authoritative.Store(initAuthoritative)

				if err := c.DeleteImagePulledRecord(logger, tt.imageRef); !cmpErrorStrings(err, tt.wantErr) {
					t.Errorf("cachedPullRecordsAccessor.DeleteImagePulledRecord() error = %v, wantErr %v", err, tt.wantErr)
				}

				// eviction 保护键必须在删除结束后清空。
				c.pulledRecords.deletingKeys.Range(func(key any, _ any) bool {
					t.Errorf("key '%v' should no longer be present in the cache.ignoreEvictionKeys map", key)
					return true
				})

				if err := cmpRecordsMapAndCache(tt.wantPulledRecords, c.pulledRecords); err != nil {
					t.Errorf("cachedPullRecordsAccessor.DeleteImagePulledRecord() expected cache state differs: %v", err)
				}

				// delegate 侧必须同步删除，否则重启后已删除记录会复活。
				if !reflect.DeepEqual(tt.delegate.pulledRecords, tt.wantPulledRecords) {
					t.Errorf("cachedPullRecordsAccessor.DeleteImagePulledRecord() delegate imagePulledRecords = %v, want %v", tt.delegate.pulledRecords, tt.wantPulledRecords)
				}

				if gotAuthoritative := c.pulledRecords.authoritative.Load(); gotAuthoritative != initAuthoritative {
					t.Errorf("cachedPullRecordsAccessor.DeleteImagePullIntent() should not cause authoritative changes = %v, want %v", gotAuthoritative, initAuthoritative)
				}
			})
		}
	}
}

// Test_cachedPullRecordsAccessor_GetImagePulledRecord 验证缓存命中/未命中、回源回填以及 delegate 读错误的处理。
func Test_cachedPullRecordsAccessor_GetImagePulledRecord(t *testing.T) {

	tests := []struct {
		name                   string
		delegate               PullRecordsAccessor
		cachedPulledRecords    *lruCache[string, kubeletconfiginternal.ImagePulledRecord]
		initAuthoritative      bool
		wantAuthoritative      bool
		imageRef               string
		wantPulledRecord       *kubeletconfiginternal.ImagePulledRecord
		wantExists             bool
		wantErr                error
		wantCachePulledRecords map[string]*kubeletconfiginternal.ImagePulledRecord
	}{
		{
			name:                   "pulled record exists in cache",
			delegate:               newTestPullRecordsAccessor(nil, nil),
			cachedPulledRecords:    mapToCache(pulledRecordsToMap(testPulledRecords())),
			initAuthoritative:      true,
			wantAuthoritative:      true,
			imageRef:               "ref-009",
			wantPulledRecord:       pulledRecordsToMap(testPulledRecords())["ref-009"],
			wantExists:             true,
			wantCachePulledRecords: pulledRecordsToMap(testPulledRecords()),
		},
		{
			name:                   "pulled record exists in delegate, nonauthoritative cache gets updated",
			delegate:               newTestPullRecordsAccessor(nil, testPulledRecords()),
			cachedPulledRecords:    mapToCache(deleteFromStringMap(pulledRecordsToMap(testPulledRecords()), "ref-22")),
			initAuthoritative:      false,
			wantAuthoritative:      false,
			imageRef:               "ref-22",
			wantPulledRecord:       pulledRecordsToMap(testPulledRecords())["ref-22"],
			wantExists:             true,
			wantCachePulledRecords: pulledRecordsToMap(testPulledRecords()),
		},
		{
			name:                   "pulled record exists in delegate, authoritative cache return not found", // 非真实场景，仅用于验证缓存权威时不会回源
			delegate:               newTestPullRecordsAccessor(nil, testPulledRecords()),
			cachedPulledRecords:    mapToCache(deleteFromStringMap(pulledRecordsToMap(testPulledRecords()), "ref-22")),
			initAuthoritative:      true,
			wantAuthoritative:      true,
			imageRef:               "ref-22",
			wantPulledRecord:       nil,
			wantExists:             false,
			wantCachePulledRecords: deleteFromStringMap(pulledRecordsToMap(testPulledRecords()), "ref-22"),
		},
		{
			name:                   "pulled record does not exist",
			delegate:               newTestPullRecordsAccessor(nil, nil),
			cachedPulledRecords:    mapToCache(deleteFromStringMap(pulledRecordsToMap(testPulledRecords()), "ref-87")),
			initAuthoritative:      false,
			wantAuthoritative:      false,
			imageRef:               "ref-87",
			wantPulledRecord:       nil,
			wantExists:             false,
			wantCachePulledRecords: deleteFromStringMap(pulledRecordsToMap(testPulledRecords()), "ref-87"),
		},
		{
			name:                   "pulled record not cached and not in delegate, delegate returns error on read",
			delegate:               newTestPullRecordsAccessor(nil, nil).withNextGetWriteError(fmt.Errorf("error reading pulled record")),
			cachedPulledRecords:    newLRUCache[string, kubeletconfiginternal.ImagePulledRecord](100),
			initAuthoritative:      false,
			wantAuthoritative:      false,
			imageRef:               "doesnotexist",
			wantPulledRecord:       nil,
			wantExists:             false,
			wantErr:                fmt.Errorf("error reading pulled record"),
			wantCachePulledRecords: map[string]*kubeletconfiginternal.ImagePulledRecord{},
		},
		{
			name:                   "pulled record not cached, exists in delegate, delegate returns error",
			delegate:               newTestPullRecordsAccessor(nil, testPulledRecords()).withNextGetWriteError(fmt.Errorf("error reading pulled record")),
			cachedPulledRecords:    newLRUCache[string, kubeletconfiginternal.ImagePulledRecord](100),
			initAuthoritative:      false,
			wantAuthoritative:      false,
			imageRef:               "ref-3",
			wantPulledRecord:       nil,
			wantExists:             true,
			wantErr:                fmt.Errorf("error reading pulled record"),
			wantCachePulledRecords: map[string]*kubeletconfiginternal.ImagePulledRecord{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &cachedPullRecordsAccessor{
				delegate:           tt.delegate,
				pulledRecordsLocks: NewStripedLockSet(10),
				pulledRecords:      tt.cachedPulledRecords,
			}
			c.pulledRecords.authoritative.Store(tt.initAuthoritative)

			gotPulledRecord, gotExists, err := c.GetImagePulledRecord(tt.imageRef)
			// 先校验错误；错误路径下 exists 仍可能有意义（记录存在于 delegate），故随后单独断言。
			if !cmpErrorStrings(err, tt.wantErr) {
				t.Errorf("cachedPullRecordsAccessor.GetImagePulledRecord() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotPulledRecord, tt.wantPulledRecord) {
				t.Errorf("cachedPullRecordsAccessor.GetImagePulledRecord() gotPulledRecord = %v, want %v", gotPulledRecord, tt.wantPulledRecord)
			}
			if gotExists != tt.wantExists {
				t.Errorf("cachedPullRecordsAccessor.GetImagePulledRecord() gotExists = %v, want %v", gotExists, tt.wantExists)
			}

			// 回源成功后记录应被写入缓存，失败时缓存保持不变。
			if err := cmpRecordsMapAndCache(tt.wantCachePulledRecords, c.pulledRecords); err != nil {
				t.Errorf("cachedPullRecordsAccessor.GetImagePulledRecord() expected final cache state differs: %v", err)
			}

			if gotAuthoritative := c.pulledRecords.authoritative.Load(); gotAuthoritative != tt.wantAuthoritative {
				t.Errorf("cachedPullRecordsAccessor.GetImagePulledRecord() cache authoritative = %v, want %v", gotAuthoritative, tt.wantAuthoritative)
			}
		})
	}
}

// testPullIntents 返回一组固定 intent，故意不排序，以便测试排序/比较逻辑。
func testPullIntents() []*kubeletconfiginternal.ImagePullIntent {
	return []*kubeletconfiginternal.ImagePullIntent{
		{Image: "test.repo/org1/project2:tag"},
		{Image: "test.repo/org1/project1:tag"},
		{Image: "test.repo/org1/project3:tag"},
		{Image: "test.repo/org2/project1:tag"},
	}
}

// generateTestPullIntents 生成 recordsNum 个 intent，镜像名带随机后缀以避免冲突。
func generateTestPullIntents(recordsNum int) []*kubeletconfiginternal.ImagePullIntent {
	repoFormat := "test.repo/org%d/project%d:tag"
	ret := make([]*kubeletconfiginternal.ImagePullIntent, 0, recordsNum)
	for i := 0; i < recordsNum; i++ {
		ret = append(ret, &kubeletconfiginternal.ImagePullIntent{
			Image: fmt.Sprintf(repoFormat, i, randNonNegativeIntOrDie()),
		})
	}
	return ret
}

// testPulledRecords 返回一组固定记录，覆盖 NodePodsAccessible 与 KubernetesSecrets 两种凭据映射。
func testPulledRecords() []*kubeletconfiginternal.ImagePulledRecord {
	return []*kubeletconfiginternal.ImagePulledRecord{
		{
			ImageRef: "ref-87",
			CredentialMapping: map[string]kubeletconfiginternal.ImagePullCredentials{
				"image.some/org1/imagex": {
					NodePodsAccessible: true,
				},
			},
		},
		{
			ImageRef: "ref-3",
			CredentialMapping: map[string]kubeletconfiginternal.ImagePullCredentials{
				"image.some/org2/image": {
					NodePodsAccessible: true,
				},
			},
		},
		{ImageRef: "ref-22",
			CredentialMapping: map[string]kubeletconfiginternal.ImagePullCredentials{
				"image.some/org1/image-2": {
					KubernetesSecrets: []kubeletconfiginternal.ImagePullSecret{
						{Namespace: "ns1", Name: "secret1", UID: "uid1", CredentialHash: "somehash"},
					},
				},
			},
		},
		{ImageRef: "ref-17",
			CredentialMapping: map[string]kubeletconfiginternal.ImagePullCredentials{
				"image.some/org1/image-3": {
					NodePodsAccessible: true,
				},
			},
		},
		{ImageRef: "ref-009",
			CredentialMapping: map[string]kubeletconfiginternal.ImagePullCredentials{
				"image.some/org3/image": {
					NodePodsAccessible: true,
				},
			},
		},
	}
}

// generateTestPulledRecords 生成 recordsNum 条记录，ImageRef 递增保证唯一。
func generateTestPulledRecords(recordsNum int) []*kubeletconfiginternal.ImagePulledRecord {
	repoFormat := "image.some/org%d/image%d"
	ret := make([]*kubeletconfiginternal.ImagePulledRecord, 0, recordsNum)
	for i := 0; i < recordsNum; i++ {
		ret = append(ret,
			&kubeletconfiginternal.ImagePulledRecord{
				ImageRef: fmt.Sprintf("ref-%d", i),
				CredentialMapping: map[string]kubeletconfiginternal.ImagePullCredentials{
					fmt.Sprintf(repoFormat, randNonNegativeIntOrDie(), randNonNegativeIntOrDie()): {
						NodePodsAccessible: true,
					},
				},
			})
	}
	return ret
}

// pullIntentsToMap 以镜像名为键把 intent 切片转成 map，便于与缓存内容比较。
func pullIntentsToMap(intents []*kubeletconfiginternal.ImagePullIntent) map[string]*kubeletconfiginternal.ImagePullIntent {
	ret := make(map[string]*kubeletconfiginternal.ImagePullIntent, len(intents))
	for _, intent := range intents {
		ret[intent.Image] = intent
	}
	return ret
}

// pulledRecordsToMap 以 ImageRef 为键把记录切片转成 map。
func pulledRecordsToMap(records []*kubeletconfiginternal.ImagePulledRecord) map[string]*kubeletconfiginternal.ImagePulledRecord {
	ret := make(map[string]*kubeletconfiginternal.ImagePulledRecord, len(records))
	for _, record := range records {
		ret[record.ImageRef] = record
	}
	return ret
}

// mapToCache 用 map 填充容量为 100 的缓存；顺序无关，适用于不关心淘汰顺序的用例。
func mapToCache[V any](m map[string]*V) *lruCache[string, V] {
	ret := newLRUCache[string, V](100)
	for k, v := range m {
		ret.Set(k, v)
	}
	return ret
}

// listToCache 按给定顺序填充缓存，以便断言具体哪些条目被 LRU 淘汰。
func listToCache[V any](l []*V, recordToKey func(*V) string) *lruCache[string, V] {
	ret := newLRUCache[string, V](100)
	for _, v := range l {
		ret.Set(recordToKey(v), v)
	}
	return ret
}

// testPullIntentsSorted 返回排序后的固定 intent，匹配 delegate 的列表顺序。
func testPullIntentsSorted() []*kubeletconfiginternal.ImagePullIntent {
	intents := testPullIntents()
	slices.SortFunc(intents, pullIntentsCmp)
	return intents
}

// testPulledRecordsSorted 返回排序后的固定记录，匹配 delegate 的列表顺序。
func testPulledRecordsSorted() []*kubeletconfiginternal.ImagePulledRecord {
	records := testPulledRecords()
	slices.SortFunc(records, pulledRecordsCmp)
	return records
}

// cmpErrorStrings 按错误文本比较，避免测试依赖具体错误类型。
func cmpErrorStrings(a, b error) bool {
	if a == nil && b == nil {
		return true
	} else if a == nil || b == nil {
		return false
	}

	return strings.Compare(a.Error(), b.Error()) == 0
}

// deleteFromStringMap 原地删除并返回 map，用于简洁表达期望状态。
func deleteFromStringMap[V any](m map[string]V, key string) map[string]V {
	delete(m, key)
	return m
}

// cmpRecordsMapAndCache 逐键比较 map 与缓存，返回首个不一致之处。
func cmpRecordsMapAndCache[V any](m map[string]*V, c *lruCache[string, V]) error {
	if len(m) != c.Len() {
		return fmt.Errorf("length mismatch: map has %d items, cache has %d items", len(m), c.Len())
	}

	for k, v := range m {
		cacheValue, exists := c.Get(k)
		if !exists || !reflect.DeepEqual(v, cacheValue) {
			return fmt.Errorf("key %q mismatch: %v != %v", k, v, cacheValue)
		}
	}
	return nil
}

// randNonNegativeIntOrDie 返回 [0,10000) 的随机数；随机失败属于不可恢复错误，直接 panic。
func randNonNegativeIntOrDie() int64 {
	n, err := rand.Int(rand.Reader, big.NewInt(10000))
	if err != nil {
		panic(fmt.Sprintf("failed to generate random number: %v", err))
	}
	return n.Int64()
}

// pullIntentsCmp 按镜像名排序 intent。
func pullIntentsCmp(a, b *kubeletconfiginternal.ImagePullIntent) int {
	return strings.Compare(a.Image, b.Image)
}

// pulledRecordsCmp 按 ImageRef 排序记录。
func pulledRecordsCmp(a, b *kubeletconfiginternal.ImagePulledRecord) int {
	return strings.Compare(a.ImageRef, b.ImageRef)
}
