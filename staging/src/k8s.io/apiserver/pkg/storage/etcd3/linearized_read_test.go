/*
Copyright 2022 The Kubernetes Authors.

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

package etcd3

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/apis/example"
	"k8s.io/apiserver/pkg/storage"
)

// TestLinearizedReadRevisionInvariant 验证线性化读不会抬高 etcd 的 store revision。
// 背景（etcd 文档 [1]）：线性化请求必须经过 Raft 共识，一轮共识会向 Raft log
// 追加条目，etcd 可能把其中部分体现为响应头中更高的 store revision。
// Kubernetes 会在 List 等响应中暴露该 revision，因此它最终是面向客户端的。
// 默认情况下 *etcd3.store{} 发出的所有请求（包括读）都是线性化的，
// 但我们不希望非变更请求导致 List 的 resourceVersion 增长。
// 当前 etcd 的 revision 递增机制保证线性化读不会递增 KV store revision，
// 本测试用于在该实现细节变化时及时报警。
// [1] https://etcd.io/docs/v3.5/learning/api_guarantees/#isolation-level-and-consistency-of-replicas
func TestLinearizedReadRevisionInvariant(t *testing.T) {
	ctx, store, etcdClient := testSetup(t)

	dir := "/pods/"
	key := dir + "/testkey"
	out := &example.Pod{}
	obj := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", SelfLink: "testlink"}}

	if err := store.Create(ctx, key, obj, out, 0); err != nil {
		t.Fatalf("Set failed: %v", err)
	}
	originalRevision := out.ResourceVersion

	for i := 0; i < 5; i++ {
		// 默认即为线性化读；客户端库只提供 WithSerializable() 来改成非线性的读
		if _, err := etcdClient.KV.Get(ctx, key); err != nil {
			t.Fatalf("failed to get key: %v", err)
		}
	}

	list := &example.PodList{}
	if err := store.GetList(ctx, dir, storage.ListOptions{Predicate: storage.Everything, Recursive: true}, list); err != nil {
		t.Errorf("Unexpected List error: %v", err)
	}
	finalRevision := list.ResourceVersion

	// revision 不变才说明线性化读没有产生新的 Raft 写入。
	if originalRevision != finalRevision {
		t.Fatalf("original revision (%s) did not match final revision after linearized reads (%s)", originalRevision, finalRevision)
	}
}
