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

package cache

import (
	"k8s.io/apimachinery/pkg/util/sets"
	"strings"
	"testing"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// testIndexFunc 按 Pod 的 "foo" 标签值建立索引，用于验证自定义索引函数。
func testIndexFunc(obj interface{}) ([]string, error) {
	pod := obj.(*v1.Pod)
	return []string{pod.Labels["foo"]}, nil
}

// TestGetIndexFuncValues 验证 ListIndexFuncValues 返回索引器中出现过的、去重后的索引键。
func TestGetIndexFuncValues(t *testing.T) {
	index := NewIndexer(MetaNamespaceKeyFunc, Indexers{"testmodes": testIndexFunc})

	pod1 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "one", Labels: map[string]string{"foo": "bar"}}}
	pod2 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "two", Labels: map[string]string{"foo": "bar"}}}
	pod3 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "tre", Labels: map[string]string{"foo": "biz"}}}

	index.Add(pod1)
	index.Add(pod2)
	index.Add(pod3)

	// "bar" 被两个 Pod 共享，因此只剩两个不同键。
	keys := index.ListIndexFuncValues("testmodes")
	if len(keys) != 2 {
		t.Errorf("Expected 2 keys but got %v", len(keys))
	}

	for _, key := range keys {
		if key != "bar" && key != "biz" {
			t.Errorf("Expected only 'bar' or 'biz' but got %s", key)
		}
	}
}

// testUsersIndexFunc 把 Pod 注解中的逗号分隔用户列表拆成多个索引键，
// 用于验证“一个对象映射到多个索引键”的行为。
func testUsersIndexFunc(obj interface{}) ([]string, error) {
	pod := obj.(*v1.Pod)
	usersString := pod.Annotations["users"]

	return strings.Split(usersString, ","), nil
}

// TestMultiIndexKeys 验证多键索引的建立，以及 Delete/Update 时旧键会被正确清理。
func TestMultiIndexKeys(t *testing.T) {
	index := NewIndexer(MetaNamespaceKeyFunc, Indexers{"byUser": testUsersIndexFunc})

	pod1 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "one", Annotations: map[string]string{"users": "ernie,bert"}}}
	pod2 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "two", Annotations: map[string]string{"users": "bert,oscar"}}}
	pod3 := &v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "tre", Annotations: map[string]string{"users": "ernie,elmo"}}}

	index.Add(pod1)
	index.Add(pod2)
	index.Add(pod3)

	// 期望的“用户 -> Pod 名集合”映射；"elmo1" 用于确认不存在的键返回空集。
	expected := map[string]sets.Set[string]{}
	expected["ernie"] = sets.New("one", "tre")
	expected["bert"] = sets.New("one", "two")
	expected["elmo"] = sets.New("tre")
	expected["oscar"] = sets.New("two")
	expected["elmo1"] = sets.Set[string]{}
	{
		for k, v := range expected {
			found := sets.Set[string]{}
			indexResults, err := index.ByIndex("byUser", k)
			if err != nil {
				t.Errorf("Unexpected error %v", err)
			}
			for _, item := range indexResults {
				found.Insert(item.(*v1.Pod).Name)
			}
			if !found.Equal(v) {
				t.Errorf("missing items, index %s, expected %v but found %v", k, sets.List(v), sets.List(found))
			}
		}
	}

	// 删除 pod3 后，"ernie" 应只剩 pod1，"elmo" 应清空。
	index.Delete(pod3)
	erniePods, err := index.ByIndex("byUser", "ernie")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(erniePods) != 1 {
		t.Errorf("Expected 1 pods but got %v", len(erniePods))
	}
	for _, erniePod := range erniePods {
		if erniePod.(*v1.Pod).Name != "one" {
			t.Errorf("Expected only 'one' but got %s", erniePod.(*v1.Pod).Name)
		}
	}

	elmoPods, err := index.ByIndex("byUser", "elmo")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(elmoPods) != 0 {
		t.Errorf("Expected 0 pods but got %v", len(elmoPods))
	}

	// 修改 pod2 的 users 后，旧键 "bert" 必须被移除，只保留 pod1。
	copyOfPod2 := pod2.DeepCopy()
	copyOfPod2.Annotations["users"] = "oscar"
	index.Update(copyOfPod2)
	bertPods, err := index.ByIndex("byUser", "bert")
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(bertPods) != 1 {
		t.Errorf("Expected 1 pods but got %v", len(bertPods))
	}
	for _, bertPod := range bertPods {
		if bertPod.(*v1.Pod).Name != "one" {
			t.Errorf("Expected only 'one' but got %s", bertPod.(*v1.Pod).Name)
		}
	}
}
