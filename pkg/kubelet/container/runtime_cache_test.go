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

package container_test

import (
	"reflect"
	"testing"
	"time"

	. "k8s.io/kubernetes/pkg/kubelet/container"
	ctest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// comparePods 断言实际 Pod 列表与预期列表长度和内容一致。
func comparePods(t *testing.T, expected []*ctest.FakePod, actual []*Pod) {
	if len(expected) != len(actual) {
		t.Errorf("expected %d pods, got %d instead", len(expected), len(actual))
	}
	for i := range expected {
		if !reflect.DeepEqual(expected[i].Pod, actual[i]) {
			t.Errorf("expected %#v, got %#v", expected[i].Pod, actual[i])
		}
	}
}

// TestGetPods 验证缓存首次读取会从运行时拉取 Pod 列表。
func TestGetPods(t *testing.T) {
	tCtx := ktesting.Init(t)
	runtime := &ctest.FakeRuntime{}
	expected := []*ctest.FakePod{{Pod: &Pod{ID: "1111"}}, {Pod: &Pod{ID: "2222"}}, {Pod: &Pod{ID: "3333"}}}
	// 让 fake runtime 返回预置 Pod 列表。
	runtime.PodList = expected
	cache := NewTestRuntimeCache(runtime)
	actual, err := cache.GetPods(tCtx)
	if err != nil {
		t.Errorf("unexpected error %v", err)
	}

	comparePods(t, expected, actual)
}

// TestForceUpdateIfOlder 验证只有缓存早于期望时间时才会强制刷新。
func TestForceUpdateIfOlder(t *testing.T) {
	tCtx := ktesting.Init(t)
	runtime := &ctest.FakeRuntime{}
	cache := NewTestRuntimeCache(runtime)

	// 先缓存旧 Pod 列表。
	oldpods := []*ctest.FakePod{{Pod: &Pod{ID: "1111"}}}
	runtime.PodList = oldpods
	if err := cache.UpdateCacheWithLock(tCtx); err != nil {
		t.Errorf("unexpected error %v", err)
	}

	// 把运行时数据更新为新 Pod 列表。
	newpods := []*ctest.FakePod{{Pod: &Pod{ID: "1111"}}, {Pod: &Pod{ID: "2222"}}, {Pod: &Pod{ID: "3333"}}}
	runtime.PodList = newpods

	// 期望时间早于缓存时间，不应触发刷新。
	if err := cache.ForceUpdateIfOlder(tCtx, time.Now().Add(-20*time.Minute)); err != nil {
		t.Errorf("unexpected error %v", err)
	}
	actual := cache.GetCachedPods()
	comparePods(t, oldpods, actual)

	// 期望时间晚于缓存时间，应触发刷新。
	if err := cache.ForceUpdateIfOlder(tCtx, time.Now().Add(20*time.Second)); err != nil {
		t.Errorf("unexpected error %v", err)
	}
	actual = cache.GetCachedPods()
	comparePods(t, newpods, actual)
}
