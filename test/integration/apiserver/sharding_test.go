/*
Copyright The Kubernetes Authors.

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

package apiserver

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/sharding"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/dynamic"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	"k8s.io/kubernetes/test/integration/etcd"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// calculateShardRange 将 64 位哈希空间平均分配给各个 shard，
// 返回给定索引对应的十六进制编码起始值（含）和结束值（不含）。
func calculateShardRange(index, total int) (start, end string) {
	maxVal := new(big.Int).Lsh(big.NewInt(1), 64) // 2^64
	span := new(big.Int).Div(maxVal, big.NewInt(int64(total)))

	startVal := new(big.Int).Mul(span, big.NewInt(int64(index)))
	endVal := new(big.Int).Mul(span, big.NewInt(int64(index+1)))
	if index == total-1 {
		endVal = maxVal // 最后一个 shard 覆盖剩余部分
	}

	start = fmt.Sprintf("0x%016x", startVal)
	end = fmt.Sprintf("0x%016x", endVal)
	return start, end
}

func shardSelectorString(index, total int) string {
	return shardSelectorStringForField("object.metadata.uid", index, total)
}

func shardSelectorStringForField(field string, index, total int) string {
	start, end := calculateShardRange(index, total)
	return fmt.Sprintf("shardRange(%s, '%s', '%s')", field, start, end)
}

// objectInShard 在对象的 UID 哈希落在给定 shard 范围内时返回 true。
func objectInShard(uid string, index, total int) bool {
	return valueInShard(uid, index, total)
}

// valueInShard 在 value 的哈希落在给定 shard 范围内时返回 true。
func valueInShard(value string, index, total int) bool {
	start, end := calculateShardRange(index, total)
	hash := "0x" + sharding.HashField(value)
	return !sharding.HexLess(hash, start) && sharding.HexLess(hash, end)
}

func TestShardedList(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ShardedListAndWatch, true)

	ctx, client, _, tearDownFn := setup(t)
	defer tearDownFn()

	ns := framework.CreateNamespaceOrDie(client, "shard-list", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	// 创建一批 configmap。
	const numObjects = 20
	created := make([]*v1.ConfigMap, 0, numObjects)
	for range numObjects {
		cm, err := client.CoreV1().ConfigMaps(ns.Name).Create(ctx, &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "shard-test-",
			},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("failed to create configmap: %v", err)
		}
		created = append(created, cm)
	}

	const numShards = 3
	allFound := make(map[string]bool)

	for shard := range numShards {
		selector := shardSelectorString(shard, numShards)
		list, err := client.CoreV1().ConfigMaps(ns.Name).List(ctx, metav1.ListOptions{
			ShardSelector: selector,
		})
		if err != nil {
			t.Fatalf("shard %d: failed to list: %v", shard, err)
		}

		// 响应中必须包含 shard 信息，并回显 selector。
		if list.ShardInfo == nil {
			t.Errorf("shard %d: expected shardInfo to be set", shard)
		} else if list.ShardInfo.Selector != selector {
			t.Errorf("shard %d: expected shardInfo.selector=%q, got %q", shard, selector, list.ShardInfo.Selector)
		}

		for _, cm := range list.Items {
			uid := string(cm.UID)
			if !objectInShard(uid, shard, numShards) {
				t.Errorf("shard %d: object %s (UID %s) should not be in this shard", shard, cm.Name, uid)
			}
			if allFound[uid] {
				t.Errorf("shard %d: object %s (UID %s) appeared in multiple shards", shard, cm.Name, uid)
			}
			allFound[uid] = true
		}
	}

	// 每个创建的对象都必须且只能出现在一个 shard 中。
	for _, cm := range created {
		if !allFound[string(cm.UID)] {
			t.Errorf("object %s (UID %s) was not returned by any shard", cm.Name, cm.UID)
		}
	}
}

func TestShardedWatch(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ShardedListAndWatch, true)

	ctx, client, _, tearDownFn := setup(t)
	defer tearDownFn()

	ns := framework.CreateNamespaceOrDie(client, "shard-watch", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	// 获取初始 resource version。
	list, err := client.CoreV1().ConfigMaps(ns.Name).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("initial list failed: %v", err)
	}
	rv := list.ResourceVersion

	const numShards = 2

	// 每个 shard 启动一个 watch。
	watchers := make([]watch.Interface, numShards)
	for shard := range numShards {
		w, err := client.CoreV1().ConfigMaps(ns.Name).Watch(ctx, metav1.ListOptions{
			ResourceVersion: rv,
			ShardSelector:   shardSelectorString(shard, numShards),
		})
		if err != nil {
			t.Fatalf("shard %d: failed to start watch: %v", shard, err)
		}
		defer w.Stop()
		watchers[shard] = w
	}

	// 创建对象，并记录每个对象应该被哪个 shard 看到。
	const numObjects = 10
	expectedShard := make(map[string]int) // UID -> shard 索引
	created := make([]*v1.ConfigMap, 0, numObjects)

	for range numObjects {
		cm, err := client.CoreV1().ConfigMaps(ns.Name).Create(ctx, &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "shard-watch-",
			},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("failed to create configmap: %v", err)
		}
		created = append(created, cm)
		for shard := range numShards {
			if objectInShard(string(cm.UID), shard, numShards) {
				expectedShard[string(cm.UID)] = shard
				break
			}
		}
	}

	// 将所有 watcher 的 channel 复用为单个 channel，便于消费。
	type shardEvent struct {
		shard     int
		eventType watch.EventType
		uid       string
	}
	eventCh := make(chan shardEvent, 100)
	for shard, w := range watchers {
		go func(shard int, w watch.Interface) {
			for evt := range w.ResultChan() {
				cm, ok := evt.Object.(*v1.ConfigMap)
				if !ok {
					continue
				}
				eventCh <- shardEvent{shard: shard, eventType: evt.Type, uid: string(cm.UID)}
			}
		}(shard, w)
	}

	// waitForEvents 从复用后的 channel 中持续读取，直到收集到指定数量的
	// 给定类型事件，并返回每个 shard 对应的 UID 集合。
	waitForEvents := func(eventType watch.EventType, count int) []map[string]bool {
		t.Helper()
		perShard := make([]map[string]bool, numShards)
		for i := range perShard {
			perShard[i] = make(map[string]bool)
		}
		timeout := time.After(30 * time.Second)
		collected := 0
		for collected < count {
			select {
			case evt := <-eventCh:
				if evt.eventType != eventType {
					continue
				}
				perShard[evt.shard][evt.uid] = true
				collected++
			case <-timeout:
				t.Fatalf("timed out waiting for %s events: got %d/%d", eventType, collected, count)
			}
		}
		return perShard
	}

	// 收集 ADDED 事件。
	added := waitForEvents(watch.Added, numObjects)
	verifyShardEvents(t, "ADDED", added, expectedShard, numShards)

	// 通过设置 annotation 更新每个对象，然后收集 MODIFIED 事件。
	for _, cm := range created {
		cm.Annotations = map[string]string{"updated": "true"}
		if _, err := client.CoreV1().ConfigMaps(ns.Name).Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("failed to update configmap %s: %v", cm.Name, err)
		}
	}
	modified := waitForEvents(watch.Modified, numObjects)
	verifyShardEvents(t, "MODIFIED", modified, expectedShard, numShards)

	// 删除每个对象，然后收集 DELETED 事件。
	for _, cm := range created {
		if err := client.CoreV1().ConfigMaps(ns.Name).Delete(ctx, cm.Name, metav1.DeleteOptions{}); err != nil {
			t.Fatalf("failed to delete configmap %s: %v", cm.Name, err)
		}
	}
	deleted := waitForEvents(watch.Deleted, numObjects)
	verifyShardEvents(t, "DELETED", deleted, expectedShard, numShards)
}

// verifyShardEvents 检查每个 UID 是否只被预期的 shard 看到。
func verifyShardEvents(t *testing.T, eventType string, perShard []map[string]bool, expectedShard map[string]int, numShards int) {
	t.Helper()
	for uid, expectedIdx := range expectedShard {
		if !perShard[expectedIdx][uid] {
			t.Errorf("%s: UID %s expected in shard %d but not received", eventType, uid, expectedIdx)
		}
		for other := range numShards {
			if other != expectedIdx && perShard[other][uid] {
				t.Errorf("%s: UID %s received in shard %d but expected only in shard %d", eventType, uid, other, expectedIdx)
			}
		}
	}
}

func TestShardedListFeatureGateDisabled(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ShardedListAndWatch, false)

	ctx, client, _, tearDownFn := setup(t)
	defer tearDownFn()

	ns := framework.CreateNamespaceOrDie(client, "shard-disabled", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	// 创建一个 configmap，使 namespace 非空。
	_, err := client.CoreV1().ConfigMaps(ns.Name).Create(ctx, &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "test"},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("failed to create configmap: %v", err)
	}

	// 使用 shard selector 进行 list —— 当 gate 关闭时，selector 应被忽略，
	// 返回所有对象（不做分片）。
	list, err := client.CoreV1().ConfigMaps(ns.Name).List(ctx, metav1.ListOptions{
		ShardSelector: shardSelectorString(0, 2),
	})
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}

	if list.ShardInfo != nil {
		t.Errorf("expected shardInfo=nil when feature gate is disabled")
	}
	if len(list.Items) != 1 {
		t.Errorf("expected 1 item (selector ignored), got %d", len(list.Items))
	}
}

func TestShardedListComplete(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ShardedListAndWatch, true)

	ctx, client, _, tearDownFn := setup(t)
	defer tearDownFn()

	ns := framework.CreateNamespaceOrDie(client, "shard-complete", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	// 空/全量 selector 应返回所有对象，且不带 sharded=true。
	const numObjects = 5
	for range numObjects {
		_, err := client.CoreV1().ConfigMaps(ns.Name).Create(ctx, &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: "complete-",
			},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("failed to create configmap: %v", err)
		}
	}

	// 覆盖完整范围的单个 shard（shard 0/1）起始和结束值均为空，
	// 这意味着没有 selector 字符串 —— 直接使用普通 list 即可。
	list, err := client.CoreV1().ConfigMaps(ns.Name).List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list failed: %v", err)
	}
	if list.ShardInfo != nil {
		t.Errorf("expected shardInfo=nil for unsharded list")
	}
	if len(list.Items) != numObjects {
		t.Errorf("expected %d items, got %d", numObjects, len(list.Items))
	}
}

func TestShardedListByNamespace(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ShardedListAndWatch, true)

	ctx, client, _, tearDownFn := setup(t)
	defer tearDownFn()

	// 创建多个 namespace，每个包含一个 ConfigMap。
	const numNamespaces = 10
	type nsObj struct {
		namespace string
		uid       string
	}
	var objects []nsObj
	for i := range numNamespaces {
		nsName := fmt.Sprintf("shard-ns-%d", i)
		ns := framework.CreateNamespaceOrDie(client, nsName, t)
		defer framework.DeleteNamespaceOrDie(client, ns, t)

		cm, err := client.CoreV1().ConfigMaps(nsName).Create(ctx, &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "test"},
		}, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("failed to create configmap in %s: %v", nsName, err)
		}
		objects = append(objects, nsObj{namespace: nsName, uid: string(cm.UID)})
	}

	const numShards = 3
	allFound := make(map[string]bool) // UID -> 是否已找到

	for shard := range numShards {
		selector := shardSelectorStringForField("object.metadata.namespace", shard, numShards)
		// 跨所有 namespace 进行 list。
		list, err := client.CoreV1().ConfigMaps("").List(ctx, metav1.ListOptions{
			ShardSelector: selector,
		})
		if err != nil {
			t.Fatalf("shard %d: failed to list: %v", shard, err)
		}

		if list.ShardInfo == nil {
			t.Errorf("shard %d: expected shardInfo to be set", shard)
		}

		for _, cm := range list.Items {
			if !valueInShard(cm.Namespace, shard, numShards) {
				t.Errorf("shard %d: object %s/%s namespace hash should not be in this shard", shard, cm.Namespace, cm.Name)
			}
			allFound[string(cm.UID)] = true
		}
	}

	// 我们创建的每个对象都必须且只能出现在一个 shard 中。
	for _, obj := range objects {
		if !allFound[obj.uid] {
			t.Errorf("object in namespace %s (UID %s) was not returned by any shard", obj.namespace, obj.uid)
		}
	}
}

// TestShardedListAllResources 验证每个 API resource
// 都已接入分片。它沿用 dryrun 集成测试所用的
// discovery + etcd 模式。
func TestShardedListAllResources(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ShardedListAndWatch, true)

	ctx := ktesting.Init(t)
	client, config, tearDownFn := framework.StartTestServer(ctx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			opts.Admission.GenericAdmission.DisablePlugins = []string{"ServiceAccount"}
		},
	})
	defer tearDownFn()

	dynamicClient, err := dynamic.NewForConfig(config)
	if err != nil {
		t.Fatal(err)
	}

	testNamespace := "shard-allresources"
	if _, err := client.CoreV1().Namespaces().Create(ctx, &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: testNamespace},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	storageData := etcd.GetEtcdStorageDataForNamespace(testNamespace)

	_, serverResources, err := client.Discovery().ServerGroupsAndResources()
	if err != nil {
		t.Fatalf("failed to get ServerGroupsAndResources: %v", err)
	}

	for _, resourceToTest := range etcd.GetResources(t, serverResources) {
		mapping := resourceToTest.Mapping
		gvr := mapping.Resource

		testData, hasData := storageData[gvr]
		if !hasData {
			continue
		}

		// 检查该 resource 是否支持 list。
		hasListVerb := false
		for _, discoveryGroup := range serverResources {
			for _, r := range discoveryGroup.APIResources {
				gv, _ := schema.ParseGroupVersion(discoveryGroup.GroupVersion)
				if gv.WithResource(r.Name) == gvr {
					for _, verb := range r.Verbs {
						if verb == "list" {
							hasListVerb = true
						}
					}
				}
			}
		}
		if !hasListVerb {
			continue
		}

		t.Run(gvr.String(), func(t *testing.T) {
			res, obj, err := etcd.JSONToUnstructured(testData.Stub, testNamespace, mapping, dynamicClient)
			if err != nil {
				t.Fatalf("failed to unmarshal stub: %v", err)
			}

			created, err := res.Create(ctx, obj, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("failed to create %s: %v", gvr, err)
			}
			uid := string(created.GetUID())

			// 使用 2 个 shard 进行 list，验证对象只出现在其中一个。
			const numShards = 2
			foundInShard := -1
			for shard := range numShards {
				selector := shardSelectorString(shard, numShards)
				listResult, err := res.List(ctx, metav1.ListOptions{
					ShardSelector: selector,
				})
				if err != nil {
					t.Fatalf("shard %d: list failed for %s: %v", shard, gvr, err)
				}
				for _, item := range listResult.Items {
					if string(item.GetUID()) == uid {
						if foundInShard >= 0 {
							t.Errorf("object %s appeared in shard %d and %d", uid, foundInShard, shard)
						}
						foundInShard = shard
					}
				}
			}
			if foundInShard < 0 {
				t.Errorf("object %s not found in any shard for %s", uid, gvr)
			}

			if err := res.Delete(context.TODO(), created.GetName(), *metav1.NewDeleteOptions(0)); err != nil {
				t.Logf("cleanup delete failed for %s: %v", gvr, err)
			}
		})
	}
}
