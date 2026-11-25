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

package storage

import (
	"context"
	"strings"
	"testing"

	apiserverstorage "k8s.io/apiserver/pkg/storage"
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/registry/core/service/allocator"
	"k8s.io/kubernetes/pkg/registry/registrytest"
)

func newStorage(t *testing.T) (*Etcd, *etcd3testing.EtcdTestServer, allocator.Interface, *storagebackend.Config) {
	etcdStorage, server := registrytest.NewEtcdStorage(t, "")
	mem := allocator.NewAllocationMap(100, "rangeSpecValue")
	etcd, err := NewEtcd(mem, "/ranges/serviceips", etcdStorage.ForResource(api.Resource("serviceipallocations")))
	if err != nil {
		t.Fatalf("unexpected error creating etcd: %v", err)
	}
	return etcd, server, mem, &etcdStorage.Config
}

func validNewRangeAllocation() *api.RangeAllocation {
	return &api.RangeAllocation{
		Range: "rangeSpecValue",
	}
}

func key() string {
	return "/ranges/serviceips"
}

func TestEmpty(t *testing.T) {
	storage, server, _, _ := newStorage(t)
	defer server.Terminate(t)
	if _, err := storage.Allocate(1); !strings.Contains(err.Error(), "cannot allocate resources of type serviceipallocations at this time") {
		t.Fatal(err)
	}
}

// TestStore 验证本地分配器修改会持久化为 RangeAllocation 快照，并在新分配器实例中恢复。
func TestStore(t *testing.T) {
	storage, server, backing, config := newStorage(t)
	defer server.Terminate(t)
	if err := storage.storage.Create(context.TODO(), key(), validNewRangeAllocation(), nil, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if _, err := storage.Allocate(2); err != nil {
		t.Fatal(err)
	}
	ok, err := backing.Allocate(2)
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("Expected backing allocation to fail")
	}
	if ok, err := storage.Allocate(2); ok || err != nil {
		t.Fatal("Expected allocation to fail")
	}

	other := allocator.NewAllocationMap(100, "rangeSpecValue")

	allocation := &api.RangeAllocation{}
	if err := storage.storage.Get(context.TODO(), key(), apiserverstorage.GetOptions{}, allocation); err != nil {
		t.Fatal(err)
	}
	if allocation.Range != "rangeSpecValue" {
		t.Errorf("unexpected stored Range: %s", allocation.Range)
	}
	if err := other.Restore("rangeSpecValue", allocation.Data); err != nil {
		t.Fatal(err)
	}
	if !other.Has(2) {
		t.Fatalf("could not restore allocated IP: %#v", other)
	}

	other = allocator.NewAllocationMap(100, "rangeSpecValue")
	otherStorage, err := NewEtcd(other, "/ranges/serviceips", config.ForResource(api.Resource("serviceipallocations")))
	if err != nil {
		t.Fatalf("unexpected error creating etcd: %v", err)
	}
	if ok, err := otherStorage.Allocate(2); ok || err != nil {
		t.Fatal(err)
	}
}

// 验证某个取值在存储中已被分配、但本地位图中尚未分配的情况：
// 尝试分配它应当失败，尽管本地位图里它是空闲的，
// 但存储中并非如此。
func TestAllocatedStorageButReleasedLocally(t *testing.T) {
	storage, server, backing, _ := newStorage(t)
	defer server.Terminate(t)
	if err := storage.storage.Create(context.TODO(), key(), validNewRangeAllocation(), nil, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 在存储中分配一个取值
	if _, err := storage.Allocate(2); err != nil {
		t.Fatal(err)
	}

	// 在本地位图中释放该取值，
	// 模拟本地与存储不同步的状态
	err := backing.Release(2)
	if err != nil {
		t.Fatal(err)
	}

	// 尽管它在本地位图中是空闲的，但分配仍应失败，
	// 因为存储中并不空闲
	ok, err := storage.Allocate(2)
	if ok || err != nil {
		t.Fatal(err)
	}

}

// 验证某个取值在存储中空闲、但本地已分配的情况：
// 尝试分配它应当成功，尽管本地位图中已标记为已分配，
// 但存储中并没有被占用。
func TestAllocatedLocallyButReleasedStorage(t *testing.T) {
	storage, server, backing, _ := newStorage(t)
	defer server.Terminate(t)
	if err := storage.storage.Create(context.TODO(), key(), validNewRangeAllocation(), nil, 0); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// 只在本地位图中分配该取值，存储中不分配，
	// 模拟本地与存储不同步的状态
	if _, err := backing.Allocate(2); err != nil {
		t.Fatal(err)
	}

	// 此时应当能成功分配，
	// 因为它在存储中是空闲的
	ok, err := storage.Allocate(2)
	if !ok || err != nil {
		t.Fatal(err)
	}

}
