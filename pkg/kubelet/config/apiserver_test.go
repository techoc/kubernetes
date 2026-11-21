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

package config

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

// fakePodLW 是返回预设 List/Watch 结果的 ListerWatcher 桩。
type fakePodLW struct {
	listResp  runtime.Object
	watchResp watch.Interface
}

// List 返回预设的列表响应。
func (lw fakePodLW) List(options metav1.ListOptions) (runtime.Object, error) {
	return lw.listResp, nil
}

// Watch 返回预设的 watch 响应。
func (lw fakePodLW) Watch(options metav1.ListOptions) (watch.Interface, error) {
	return lw.watchResp, nil
}

// IsWatchListSemanticsUnSupported 告知 reflector 该客户端不支持 WatchList 语义。
// 这是一个合成方法，只为满足 reflector 的可选接口检查；返回 true 表示不能使用 WatchList。
func (lw fakePodLW) IsWatchListSemanticsUnSupported() bool { return true }

// 编译期断言：fakePodLW 实现 cache.ListerWatcher。
var _ cache.ListerWatcher = fakePodLW{}

// TestNewSourceApiserver_UpdatesAndMultiplePods 验证 apiserver 来源
// 在新增、修改、删除 Pod 时推送的更新。
func TestNewSourceApiserver_UpdatesAndMultiplePods(t *testing.T) {
	pod1v1 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p"},
		Spec:       v1.PodSpec{Containers: []v1.Container{{Image: "image/one"}}}}
	pod1v2 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p"},
		Spec:       v1.PodSpec{Containers: []v1.Container{{Image: "image/two"}}}}
	pod2 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "q"},
		Spec:       v1.PodSpec{Containers: []v1.Container{{Image: "image/blah"}}}}

	// 构造 fake API client 与 watch。
	fakeWatch := watch.NewFake()
	lw := fakePodLW{
		listResp:  &v1.PodList{Items: []v1.Pod{*pod1v1}},
		watchResp: fakeWatch,
	}

	ch := make(chan sourceUpdate)

	newSourceApiserverFromLW(lw, ch)

	update, ok := <-ch
	if !ok {
		t.Errorf("Unable to read from channel when expected")
	}
	expected := createSourceUpdate(pod1v1)
	if !apiequality.Semantic.DeepEqual(expected, update) {
		t.Errorf("Expected %#v; update %#v", expected, update)
	}

	// 新增 Pod 后应收到包含两个 Pod 的更新。
	fakeWatch.Add(pod2)
	update, ok = <-ch
	if !ok {
		t.Errorf("Unable to read from channel when expected")
	}
	// 两个 Pod 的顺序不确定，两种期望都可接受。
	expectedA := createSourceUpdate(pod1v1, pod2)
	expectedB := createSourceUpdate(pod2, pod1v1)

	if !apiequality.Semantic.DeepEqual(expectedA, update) && !apiequality.Semantic.DeepEqual(expectedB, update) {
		t.Errorf("Expected %#v or %#v, update %#v", expectedA, expectedB, update)
	}

	// 修改 Pod 后应收到更新后的内容。
	fakeWatch.Modify(pod1v2)
	update, ok = <-ch
	if !ok {
		t.Errorf("Unable to read from channel when expected")
	}
	expectedA = createSourceUpdate(pod1v2, pod2)
	expectedB = createSourceUpdate(pod2, pod1v2)

	if !apiequality.Semantic.DeepEqual(expectedA, update) && !apiequality.Semantic.DeepEqual(expectedB, update) {
		t.Errorf("Expected %#v or %#v, update %#v", expectedA, expectedB, update)
	}

	// 删除 Pod 后更新中应只剩另一个 Pod。
	fakeWatch.Delete(pod1v2)
	update, ok = <-ch
	if !ok {
		t.Errorf("Unable to read from channel when expected")
	}
	expected = createSourceUpdate(pod2)
	if !apiequality.Semantic.DeepEqual(expected, update) {
		t.Errorf("Expected %#v, update %#v", expected, update)
	}

	// 删除最后一个 Pod 后应收到空更新。
	fakeWatch.Delete(pod2)
	update, ok = <-ch
	if !ok {
		t.Errorf("Unable to read from channel when expected")
	}
	expected = createSourceUpdate() // Empty update.
	if !apiequality.Semantic.DeepEqual(expected, update) {
		t.Errorf("Expected %#v, update %#v", expected, update)
	}
}

// TestNewSourceApiserver_TwoNamespacesSameName 验证不同命名空间中的同名 Pod 都被保留。
func TestNewSourceApiserver_TwoNamespacesSameName(t *testing.T) {
	pod1 := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "one"},
		Spec:       v1.PodSpec{Containers: []v1.Container{{Image: "image/one"}}}}
	pod2 := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "two"},
		Spec:       v1.PodSpec{Containers: []v1.Container{{Image: "image/blah"}}}}

	// 构造包含两个同名 Pod 的 fake client。
	fakeWatch := watch.NewFake()
	lw := fakePodLW{
		listResp:  &v1.PodList{Items: []v1.Pod{pod1, pod2}},
		watchResp: fakeWatch,
	}

	ch := make(chan sourceUpdate)

	newSourceApiserverFromLW(lw, ch)

	update, ok := <-ch
	if !ok {
		t.Errorf("Unable to read from channel when expected")
	}
	// 确认两个 Pod 都收到（回归 bug #2294）。
	if !(len(update.Pods) == 2) {
		t.Errorf("Expected %d, update %d", 2, len(update.Pods))
	}

	// 删除其中一个后应只剩一个。
	fakeWatch.Delete(&pod1)
	update, ok = <-ch
	if !ok {
		t.Errorf("Unable to read from channel when expected")
	}
	if !(len(update.Pods) == 1) {
		t.Errorf("Expected %d, update %d", 1, len(update.Pods))
	}
}

// TestNewSourceApiserverInitialEmptySendsEmptyPodUpdate 验证初始列表为空时仍会发送空更新。
func TestNewSourceApiserverInitialEmptySendsEmptyPodUpdate(t *testing.T) {
	// 构造列表为空的 fake client。
	fakeWatch := watch.NewFake()
	lw := fakePodLW{
		listResp:  &v1.PodList{Items: []v1.Pod{}},
		watchResp: fakeWatch,
	}

	ch := make(chan sourceUpdate)

	newSourceApiserverFromLW(lw, ch)

	update, ok := <-ch
	if !ok {
		t.Errorf("Unable to read from channel when expected")
	}
	expected := createSourceUpdate() // Expect empty update.
	if !apiequality.Semantic.DeepEqual(expected, update) {
		t.Errorf("Expected %#v; update %#v", expected, update)
	}
}
