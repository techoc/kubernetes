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

package storageversiongc

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	apiserverinternalv1alpha1 "k8s.io/api/apiserverinternal/v1alpha1"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/utils/ptr"
)

// setupController 启动 Lease 与 StorageVersion 的共享 informer，等待缓存同步后再运行 GC 控制器。
// 先同步缓存可以避免控制器在列表尚未填充时误判，把仍在使用的 StorageVersion 当作过期对象清理。
func setupController(ctx context.Context, clientset kubernetes.Interface) {
	informerFactory := informers.NewSharedInformerFactory(clientset, 100*time.Millisecond)
	leaseInformer := informerFactory.Coordination().V1().Leases()
	storageVersionInformer := informerFactory.Internal().V1alpha1().StorageVersions()

	controller := NewStorageVersionGC(ctx, clientset, leaseInformer, storageVersionInformer)
	informerFactory.Start(ctx.Done())
	// 控制器的 worker 直接读取 informer 本地缓存；若缓存尚未填充完成就开始处理，
	// 可能把暂时看不到的对象误判为已删除，所以必须先等待同步。
	if !cache.WaitForCacheSync(ctx.Done(), controller.leasesSynced, controller.storageVersionSynced) {
		panic("timed out waiting for caches to sync")
	}
	// Run 使用独立的 context，避免测试 context 结束后控制器被提前取消，
	// 否则刚刚触发的删除事件可能来不及被处理。
	go controller.Run(context.Background())
}

// newKubeApiserverLease 构造一个带 kube-apiserver 身份标签的 Lease。
// holderIdentity 同时也是 StorageVersion 状态中记录各 apiserver 编码版本所用的 APIServerID。
func newKubeApiserverLease(name, holderIdentity string) *coordinationv1.Lease {
	return &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: metav1.NamespaceSystem,
			Labels: map[string]string{
				"apiserver.kubernetes.io/identity": "kube-apiserver",
			},
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: ptr.To(holderIdentity),
		},
	}
}

// Test_StorageVersionUpdatedWithAllEncodingVersionsEqualOnLeaseDeletion 验证删除某个 kube-apiserver Lease 后，
// StorageVersion 状态中的 serverStorageVersions 会剔除对应条目；
// 当剩余 Lease 对编码版本达成一致时，commonEncodingVersion 应更新为新版本，
// 并置上 AllEncodingVersionsEqual 条件。
func Test_StorageVersionUpdatedWithAllEncodingVersionsEqualOnLeaseDeletion(t *testing.T) {
	// 三个 apiserver 各对应一个 Lease：lease1 使用 v1，另外两个使用 v2。
	// 删除 lease1 后，剩余节点才能形成新的共识版本 v2。
	lease1 := newKubeApiserverLease("kube-apiserver-1", "kube-apiserver-1")
	lease2 := newKubeApiserverLease("kube-apiserver-2", "kube-apiserver-2")
	lease3 := newKubeApiserverLease("kube-apiserver-3", "kube-apiserver-3")

	storageVersion := &apiserverinternalv1alpha1.StorageVersion{
		ObjectMeta: metav1.ObjectMeta{
			Name: "k8s.test.resources",
		},
		Status: apiserverinternalv1alpha1.StorageVersionStatus{
			StorageVersions: []apiserverinternalv1alpha1.ServerStorageVersion{
				{
					APIServerID:       "kube-apiserver-1",
					EncodingVersion:   "v1",
					DecodableVersions: []string{"v1"},
				},
				{
					APIServerID:       "kube-apiserver-2",
					EncodingVersion:   "v2",
					DecodableVersions: []string{"v2"},
				},
				{
					APIServerID:       "kube-apiserver-3",
					EncodingVersion:   "v2",
					DecodableVersions: []string{"v2"},
				},
			},
			// 初始共识版本为 v1，与 lease1 的编码版本保持一致。
			CommonEncodingVersion: ptr.To("v1"),
		},
	}

	clientset := fake.NewClientset(lease1, lease2, lease3, storageVersion)
	_, ctx := ktesting.NewTestContext(t)
	setupController(ctx, clientset)

	// 删除 lease1，触发控制器重新计算 StorageVersion 状态。
	if err := clientset.CoordinationV1().Leases(metav1.NamespaceSystem).Delete(context.Background(), "kube-apiserver-1", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("error deleting lease object: %v", err)
	}

	expectedServerStorageVersions := []apiserverinternalv1alpha1.ServerStorageVersion{
		{
			APIServerID:       "kube-apiserver-2",
			EncodingVersion:   "v2",
			DecodableVersions: []string{"v2"},
		},
		{
			APIServerID:       "kube-apiserver-3",
			EncodingVersion:   "v2",
			DecodableVersions: []string{"v2"},
		},
	}
	// 记录最后一次不匹配的原因，便于轮询超时后定位具体是哪一步断言失败。
	var lastErr error

	// 控制器通过 informer 异步处理删除事件，需轮询等待状态收敛；
	// 每 100ms 检查一次，最多 5 秒，给协调循环留出足够时间。
	err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		storageVersion, err := clientset.InternalV1alpha1().StorageVersions().Get(
			ctx, "k8s.test.resources", metav1.GetOptions{},
		)
		if err != nil {
			lastErr = fmt.Errorf("failed to get StorageVersion: %w", err)
			return false, err
		}

		// 被删除 Lease 对应的 APIServerID 必须从 serverStorageVersions 中消失。
		if !reflect.DeepEqual(storageVersion.Status.StorageVersions, expectedServerStorageVersions) {
			lastErr = fmt.Errorf("storage versions mismatch: got %+v, expected %+v", storageVersion.Status.StorageVersions, expectedServerStorageVersions)
			return false, err
		}
		// 剩余两个 apiserver 都使用 v2，commonEncodingVersion 必须随之更新。
		if *storageVersion.Status.CommonEncodingVersion != "v2" {
			t.Errorf("unexpected common encoding version")
			t.Logf("got: %q", *storageVersion.Status.CommonEncodingVersion)
			t.Logf("expected: %q", "v2")
			return false, err
		}
		// 版本达成一致后，应当记录 AllEncodingVersionsEqual 条件。
		if len(storageVersion.Status.Conditions) != 1 {
			lastErr = fmt.Errorf("CommonEncodingVersion mismatch: got %v, expected %q", storageVersion.Status.CommonEncodingVersion, "v2")
			return false, err
		}

		// 同时校验条件类型与状态，确认控制器明确报告了"编码版本一致"这一事实。
		if storageVersion.Status.Conditions[0].Type != apiserverinternalv1alpha1.AllEncodingVersionsEqual {
			lastErr = fmt.Errorf("expected condition type 'AllEncodingVersionsEqual', got: %q", storageVersion.Status.Conditions[0].Type)
			return false, nil
		}

		if storageVersion.Status.Conditions[0].Status != apiserverinternalv1alpha1.ConditionTrue {
			lastErr = fmt.Errorf("expected condition status 'True', got: %q", storageVersion.Status.Conditions[0].Status)
			return false, nil
		}
		return true, nil
	})

	if err != nil {
		t.Fatalf("controller did not reconcile storage version in time: %v\nLast mismatch: %v", err, lastErr)
	}
}

// Test_StorageVersionUpdatedWithDifferentEncodingVersionsOnLeaseDeletion 验证删除 Lease 后
// serverStorageVersions 会剔除过期条目；若剩余 Lease 的编码版本不一致，
// 则 commonEncodingVersion 必须保持原值，不能武断地更新为其中某个版本。
func Test_StorageVersionUpdatedWithDifferentEncodingVersionsOnLeaseDeletion(t *testing.T) {
	lease1 := newKubeApiserverLease("kube-apiserver-1", "kube-apiserver-1")
	lease2 := newKubeApiserverLease("kube-apiserver-2", "kube-apiserver-2")
	lease3 := newKubeApiserverLease("kube-apiserver-3", "kube-apiserver-3")

	storageVersion := &apiserverinternalv1alpha1.StorageVersion{
		ObjectMeta: metav1.ObjectMeta{
			Name: "k8s.test.resources",
		},
		Status: apiserverinternalv1alpha1.StorageVersionStatus{
			StorageVersions: []apiserverinternalv1alpha1.ServerStorageVersion{
				{
					APIServerID:       "kube-apiserver-1",
					EncodingVersion:   "v1",
					DecodableVersions: []string{"v1"},
				},
				{
					APIServerID:       "kube-apiserver-3",
					EncodingVersion:   "v2",
					DecodableVersions: []string{"v2"},
				},
			},
			CommonEncodingVersion: ptr.To("v1"),
		},
	}

	clientset := fake.NewSimpleClientset(lease1, lease2, lease3, storageVersion)
	_, ctx := ktesting.NewTestContext(t)
	setupController(ctx, clientset)

	// 删除 lease2 并验证状态；其持有者 ID 本就不在 StorageVersion 状态中，应不影响编码版本共识。
	if err := clientset.CoordinationV1().Leases(metav1.NamespaceSystem).Delete(context.Background(), "kube-apiserver-2", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("error deleting lease object: %v", err)
	}

	// 控制器处理删除事件是异步的，短暂等待以确认状态未被错误修改。
	time.Sleep(2 * time.Second)

	storageVersion, err := clientset.InternalV1alpha1().StorageVersions().Get(context.Background(), "k8s.test.resources", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("error getting StorageVersion: %v", err)
	}

	expectedServerStorageVersions := []apiserverinternalv1alpha1.ServerStorageVersion{
		{
			APIServerID:       "kube-apiserver-1",
			EncodingVersion:   "v1",
			DecodableVersions: []string{"v1"},
		},
		{
			APIServerID:       "kube-apiserver-3",
			EncodingVersion:   "v2",
			DecodableVersions: []string{"v2"},
		},
	}

	// serverStorageVersions 应剔除已删除 Lease 对应的记录，其余条目保持不变。
	if !reflect.DeepEqual(storageVersion.Status.StorageVersions, expectedServerStorageVersions) {
		t.Error("unexpected storage version object")
		t.Logf("got: %+v", storageVersion)
		t.Logf("expected: %+v", expectedServerStorageVersions)
	}

	// 剩余版本仍是 v1 与 v2，没有形成共识，公共编码版本必须保持原值。
	if *storageVersion.Status.CommonEncodingVersion != "v1" {
		t.Errorf("unexpected common encoding version")
		t.Logf("got: %q", *storageVersion.Status.CommonEncodingVersion)
		t.Logf("expected: %q", "v1")
	}
}

// Test_StorageVersionContainsInvalidLeaseID 验证状态清理逻辑：
// serverStorageVersions 中对应 Lease 已不存在的条目（过期持有者身份）必须被移除。
func Test_StorageVersionContainsInvalidLeaseID(t *testing.T) {
	lease1 := newKubeApiserverLease("kube-apiserver-1", "kube-apiserver-1")
	lease2 := newKubeApiserverLease("kube-apiserver-2", "kube-apiserver-2")
	lease3 := newKubeApiserverLease("kube-apiserver-3", "kube-apiserver-3")

	storageVersion := &apiserverinternalv1alpha1.StorageVersion{
		ObjectMeta: metav1.ObjectMeta{
			Name: "k8s.test.resources",
		},
		Status: apiserverinternalv1alpha1.StorageVersionStatus{
			StorageVersions: []apiserverinternalv1alpha1.ServerStorageVersion{
				{
					APIServerID:       "kube-apiserver-1",
					EncodingVersion:   "v1",
					DecodableVersions: []string{"v1"},
				},
				{
					APIServerID:       "kube-apiserver-2",
					EncodingVersion:   "v2",
					DecodableVersions: []string{"v2"},
				},
				{
					APIServerID:       "kube-apiserver-3",
					EncodingVersion:   "v2",
					DecodableVersions: []string{"v2"},
				},
				{
					// 该 APIServerID 没有对应 Lease，属于应被清理的过期持有者记录。
					APIServerID:       "kube-apiserver-4", // 不存在
					EncodingVersion:   "v2",
					DecodableVersions: []string{"v1"},
				},
			},
			CommonEncodingVersion: ptr.To("v1"),
		},
	}

	clientset := fake.NewSimpleClientset(lease1, lease2, lease3, storageVersion)
	_, ctx := ktesting.NewTestContext(t)
	setupController(ctx, clientset)

	// 等待控制器完成对过期持有者记录的清理。
	time.Sleep(2 * time.Second)

	storageVersion, err := clientset.InternalV1alpha1().StorageVersions().Get(context.Background(), "k8s.test.resources", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("error getting StorageVersion: %v", err)
	}

	expectedServerStorageVersions := []apiserverinternalv1alpha1.ServerStorageVersion{
		{
			APIServerID:       "kube-apiserver-1",
			EncodingVersion:   "v1",
			DecodableVersions: []string{"v1"},
		},
		{
			APIServerID:       "kube-apiserver-2",
			EncodingVersion:   "v2",
			DecodableVersions: []string{"v2"},
		},
		{
			APIServerID:       "kube-apiserver-3",
			EncodingVersion:   "v2",
			DecodableVersions: []string{"v2"},
		},
	}

	// 只清理过期条目，仍然存在的三个 Lease 对应记录必须保留。
	if !reflect.DeepEqual(storageVersion.Status.StorageVersions, expectedServerStorageVersions) {
		t.Error("unexpected storage version object")
		t.Logf("got: %+v", storageVersion)
		t.Logf("expected: %+v", expectedServerStorageVersions)
	}

	// 即使编码版本不一致，也应记录相应条件来反映当前状态。
	if len(storageVersion.Status.Conditions) != 1 {
		t.Errorf("expected 1 condition, got: %d", len(storageVersion.Status.Conditions))
	}

	if storageVersion.Status.Conditions[0].Type != apiserverinternalv1alpha1.AllEncodingVersionsEqual {
		t.Errorf("expected condition type 'AllEncodingVersionsEqual', got: %q", storageVersion.Status.Conditions[0].Type)
	}

	// 此处期望 ConditionFalse；错误文案中的 'True' 是既有笔误，不影响断言语义。
	if storageVersion.Status.Conditions[0].Status != apiserverinternalv1alpha1.ConditionFalse {
		t.Errorf("expected condition status 'True', got: %q", storageVersion.Status.Conditions[0].Status)
	}
}

// Test_StorageVersionDeletedOnLeaseDeletion 验证当集群中最后一个 kube-apiserver Lease 被删除时，
// 已无使用者依赖该资源，对应的 StorageVersion 对象应被 GC 删除。
func Test_StorageVersionDeletedOnLeaseDeletion(t *testing.T) {
	lease1 := newKubeApiserverLease("kube-apiserver-1", "kube-apiserver-1")

	storageVersion := &apiserverinternalv1alpha1.StorageVersion{
		ObjectMeta: metav1.ObjectMeta{
			Name: "k8s.test.resources",
		},
		Status: apiserverinternalv1alpha1.StorageVersionStatus{
			StorageVersions: []apiserverinternalv1alpha1.ServerStorageVersion{
				{
					APIServerID:       "kube-apiserver-1",
					EncodingVersion:   "v1",
					DecodableVersions: []string{"v1"},
				},
			},
		},
	}

	clientset := fake.NewClientset(lease1, storageVersion)
	_, ctx := ktesting.NewTestContext(t)
	setupController(ctx, clientset)

	// 删除唯一的 Lease，触发后续 StorageVersion 的清理。
	if err := clientset.CoordinationV1().Leases(metav1.NamespaceSystem).Delete(context.Background(), "kube-apiserver-1", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("error deleting lease object: %v", err)
	}

	// 删除事件经 informer 异步传播，轮询等待 StorageVersion 真正消失。
	err := wait.PollUntilContextTimeout(context.Background(), time.Second, 5*time.Second, true, func(ctx context.Context) (bool, error) {
		// 期望 GET 返回 NotFound：这表明 StorageVersion 已被控制器删除。
		_, err := clientset.InternalV1alpha1().StorageVersions().Get(context.Background(), "k8s.test.resources", metav1.GetOptions{})
		return apierrors.IsNotFound(err), nil
	})
	if err != nil {
		t.Fatalf("expected IsNotFound error, but got error: %v", err)
	}
}
