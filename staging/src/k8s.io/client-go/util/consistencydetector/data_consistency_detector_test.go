/*
Copyright 2023 The Kubernetes Authors.

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

package consistencydetector

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

// TestDataConsistencyChecker 表驱动覆盖一致性检查的决策逻辑：
//
//   - 一致时只发一次带精确 resourceVersion 的 List，不 panic；
//   - 支持 unstructured（dynamic 客户端）数据；
//   - legacy RV=0 请求会去掉 limit，非 legacy 或 watch cache 关闭时保留；
//   - 带 continue、或已同步 RV 为 0 时跳过检查；
//   - 数据不一致时主动 panic（作为检测器的告警机制）。
func TestDataConsistencyChecker(t *testing.T) {
	scenarios := []struct {
		name string

		lastSyncedResourceVersion string
		listResponse              runtime.Object
		retrievedItems            []runtime.Object
		requestOptions            metav1.ListOptions

		expectedRequestOptions []metav1.ListOptions
		expectedListRequests   int
		expectPanic            bool
	}{
		{
			// 一致：List 返回与本地缓存完全相同的内容。
			name:                      "data consistency check won't panic when data is consistent",
			lastSyncedResourceVersion: "2",
			listResponse: &v1.PodList{
				ListMeta: metav1.ListMeta{ResourceVersion: "2"},
				Items:    []v1.Pod{*makePod("p1", "1"), *makePod("p2", "2")},
			},
			requestOptions:       metav1.ListOptions{TimeoutSeconds: ptr.To(int64(39))},
			retrievedItems:       []runtime.Object{makePod("p1", "1"), makePod("p2", "2")},
			expectedListRequests: 1,
			expectedRequestOptions: []metav1.ListOptions{
				{
					ResourceVersion:      "2",
					ResourceVersionMatch: metav1.ResourceVersionMatchExact,
					TimeoutSeconds:       ptr.To(int64(39)),
				},
			},
		},

		{
			// dynamic 客户端返回 unstructured，检查器需同样能比较。
			name:                      "data consistency check works with unstructured data (dynamic client)",
			lastSyncedResourceVersion: "2",
			listResponse: &unstructured.UnstructuredList{
				Object: map[string]interface{}{
					"apiVersion": "vTest",
					"kind":       "rTestList",
				},
				Items: []unstructured.Unstructured{
					*makeUnstructuredObject("vTest", "rTest", "item1"),
					*makeUnstructuredObject("vTest", "rTest", "item2"),
				},
			},
			requestOptions: metav1.ListOptions{TimeoutSeconds: ptr.To(int64(39))},
			retrievedItems: []runtime.Object{
				makeUnstructuredObject("vTest", "rTest", "item1"),
				makeUnstructuredObject("vTest", "rTest", "item2"),
			},
			expectedListRequests: 1,
			expectedRequestOptions: []metav1.ListOptions{
				{
					ResourceVersion:      "2",
					ResourceVersionMatch: metav1.ResourceVersionMatchExact,
					TimeoutSeconds:       ptr.To(int64(39)),
				},
			},
		},

		{
			// RV=0 且服务器未遵守 limit：去掉 limit 以拿到完整快照用于比较。
			name:                      "legacy, the limit is removed from the list options when it wasn't honored by the watch cache",
			lastSyncedResourceVersion: "2",
			listResponse: &v1.PodList{
				ListMeta: metav1.ListMeta{ResourceVersion: "2"},
				Items:    []v1.Pod{*makePod("p1", "1"), *makePod("p2", "2"), *makePod("p3", "3")},
			},
			requestOptions:       metav1.ListOptions{ResourceVersion: "0", Limit: 2},
			retrievedItems:       []runtime.Object{makePod("p1", "1"), makePod("p2", "2"), makePod("p3", "3")},
			expectedListRequests: 1,
			expectedRequestOptions: []metav1.ListOptions{
				{
					ResourceVersion:      "2",
					ResourceVersionMatch: metav1.ResourceVersionMatchExact,
				},
			},
		},

		{
			// 非 legacy（RV 非 0）请求保留 limit，分页由调用方另行处理。
			name:                      "the limit is NOT removed from the list options for non-legacy request",
			lastSyncedResourceVersion: "2",
			listResponse: &v1.PodList{
				ListMeta: metav1.ListMeta{ResourceVersion: "2"},
				Items:    []v1.Pod{*makePod("p1", "1"), *makePod("p2", "2"), *makePod("p3", "3")},
			},
			requestOptions:       metav1.ListOptions{ResourceVersion: "2", Limit: 2},
			retrievedItems:       []runtime.Object{makePod("p1", "1"), makePod("p2", "2"), makePod("p3", "3")},
			expectedListRequests: 1,
			expectedRequestOptions: []metav1.ListOptions{
				{
					Limit:                2,
					ResourceVersion:      "2",
					ResourceVersionMatch: metav1.ResourceVersionMatchExact,
				},
			},
		},

		{
			// watch cache 关闭时 limit 必被服务器遵守，无需移除。
			name:                      "legacy, the limit is NOT removed from the list options when the watch cache is disabled",
			lastSyncedResourceVersion: "2",
			listResponse: &v1.PodList{
				ListMeta: metav1.ListMeta{ResourceVersion: "2"},
				Items:    []v1.Pod{*makePod("p1", "1"), *makePod("p2", "2"), *makePod("p3", "3")},
			},
			requestOptions:       metav1.ListOptions{ResourceVersion: "0", Limit: 5},
			retrievedItems:       []runtime.Object{makePod("p1", "1"), makePod("p2", "2"), makePod("p3", "3")},
			expectedListRequests: 1,
			expectedRequestOptions: []metav1.ListOptions{
				{
					Limit:                5,
					ResourceVersion:      "2",
					ResourceVersionMatch: metav1.ResourceVersionMatchExact,
				},
			},
		},

		{
			// 空列表也应视为一致，不能因索引越界而 panic。
			name:                      "data consistency check won't panic when there is no data",
			lastSyncedResourceVersion: "2",
			listResponse: &v1.PodList{
				ListMeta: metav1.ListMeta{ResourceVersion: "2"},
			},
			requestOptions:       metav1.ListOptions{TimeoutSeconds: ptr.To(int64(39))},
			expectedListRequests: 1,
			expectedRequestOptions: []metav1.ListOptions{
				{
					ResourceVersion:      "2",
					ResourceVersionMatch: metav1.ResourceVersionMatchExact,
					TimeoutSeconds:       ptr.To(int64(39)),
				},
			},
		},

		{
			// 分页仍在进行时本地缓存必然不完整，跳过检查避免误报。
			name:                 "data consistency check won't be performed when Continuation was set",
			requestOptions:       metav1.ListOptions{Continue: "fake continuation token"},
			expectedListRequests: 0,
		},

		{
			// RV=0 表示尚未建立同步点，无从比较。
			name:                      "data consistency check won't be performed when ResourceVersion was set to 0",
			lastSyncedResourceVersion: "0",
			listResponse: &v1.PodList{
				ListMeta: metav1.ListMeta{ResourceVersion: "0"},
				Items:    []v1.Pod{*makePod("p1", "1"), *makePod("p2", "2")},
			},
			requestOptions:       metav1.ListOptions{},
			expectedListRequests: 0,
		},

		{
			// 服务器多出本地没有的对象：缓存已过期，视为不一致并 panic。
			name:                      "data consistency panics when data is inconsistent",
			lastSyncedResourceVersion: "2",
			listResponse: &v1.PodList{
				ListMeta: metav1.ListMeta{ResourceVersion: "2"},
				Items:    []v1.Pod{*makePod("p1", "1"), *makePod("p2", "2"), *makePod("p3", "3")},
			},
			requestOptions:       metav1.ListOptions{TimeoutSeconds: ptr.To(int64(39))},
			retrievedItems:       []runtime.Object{makePod("p1", "1"), makePod("p2", "2")},
			expectedListRequests: 1,
			expectedRequestOptions: []metav1.ListOptions{
				{
					ResourceVersion:      "2",
					ResourceVersionMatch: metav1.ResourceVersionMatchExact,
					TimeoutSeconds:       ptr.To(int64(39)),
				},
			},
			expectPanic: true,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			ctx := context.TODO()
			if scenario.listResponse == nil {
				scenario.listResponse = &v1.PodList{}
			}
			fakeLister := &listWrapper{response: scenario.listResponse}
			retrievedItemsFunc := func() []runtime.Object {
				return scenario.retrievedItems
			}

			if scenario.expectPanic {
				require.Panics(t, func() {
					CheckDataConsistency(ctx, "", scenario.lastSyncedResourceVersion, fakeLister.List, nil, scenario.requestOptions, retrievedItemsFunc)
				})
			} else {
				CheckDataConsistency(ctx, "", scenario.lastSyncedResourceVersion, fakeLister.List, nil, scenario.requestOptions, retrievedItemsFunc)
			}

			require.Equal(t, scenario.expectedListRequests, fakeLister.counter)
			require.Equal(t, scenario.expectedRequestOptions, fakeLister.requestOptions)
		})
	}
}

// TestDataConsistencyCheckerRetry 验证 List 反复失败时会重试，直到成功或达到上限。
func TestDataConsistencyCheckerRetry(t *testing.T) {
	ctx := context.TODO()
	retrievedItemsFunc := func() []*v1.Pod {
		return nil
	}
	stopListErrorAfter := 5
	fakeErrLister := &errorLister{stopErrorAfter: stopListErrorAfter}

	CheckDataConsistency(ctx, "", "", fakeErrLister.List, nil, metav1.ListOptions{}, retrievedItemsFunc)
	require.Equal(t, fakeErrLister.listCounter, fakeErrLister.stopErrorAfter)
}

// errorLister 在第 stopErrorAfter 次调用时返回成功，之前均返回错误。
type errorLister struct {
	listCounter    int
	stopErrorAfter int
}

func (lw *errorLister) List(_ context.Context, _ metav1.ListOptions) (runtime.Object, error) {
	lw.listCounter++
	if lw.listCounter == lw.stopErrorAfter {
		return &v1.PodList{}, nil
	}
	return nil, fmt.Errorf("nasty error")
}

// listWrapper 记录调用次数与每次的 ListOptions，并返回固定响应。
type listWrapper struct {
	counter        int
	requestOptions []metav1.ListOptions
	response       runtime.Object
}

func (lw *listWrapper) List(_ context.Context, opts metav1.ListOptions) (runtime.Object, error) {
	lw.counter++
	lw.requestOptions = append(lw.requestOptions, opts)
	return lw.response, nil
}

// makePod 构造带 name/RV/UID 的最小 Pod，便于比较一致性。
func makePod(name, rv string) *v1.Pod {
	return &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, ResourceVersion: rv, UID: types.UID(name)}}
}

// makeUnstructuredObject 构造 dynamic 客户端场景下的 unstructured 对象。
func makeUnstructuredObject(version, kind, name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": version,
			"kind":       kind,
			"metadata": map[string]interface{}{
				"name": name,
			},
		},
	}
}
