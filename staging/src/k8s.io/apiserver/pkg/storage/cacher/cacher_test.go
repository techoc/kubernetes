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

// 本文件用共享的 storagetesting 用例套件验证 Cacher（watch cache + 委托存储）的行为，
// 覆盖 CRUD、删除建议、分页、watch、快照一致性等；每个 TestXxx 大多只负责
// 装配带特定选项的 cacher，再把断言交给 storagetesting。

package cacher

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/api/apitesting"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/rand"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/apis/example"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/etcd3"
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"
	"k8s.io/apiserver/pkg/storage/value"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientfeatures "k8s.io/client-go/features"
	"k8s.io/client-go/tools/cache"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

// init 把测试用 scheme 补齐 metav1 与 example 类型，供序列化/反序列化使用。
func init() {
	metav1.AddToGroupVersion(scheme, metav1.SchemeGroupVersion)
	utilruntime.Must(example.AddToScheme(scheme))
	utilruntime.Must(examplev1.AddToScheme(scheme))
}

// GetPodAttrs 返回给定对象用于过滤的 labels 与 fields。
// cacher 依赖该函数把对象映射为字段集合，从而支持字段选择器索引与过滤。
func GetPodAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	pod, ok := obj.(*example.Pod)
	if !ok {
		return nil, nil, fmt.Errorf("not a pod")
	}
	return labels.Set(pod.ObjectMeta.Labels), PodToSelectableFields(pod), nil
}

// PodToSelectableFields 返回一个代表该对象的字段集合。
// TODO: 字段不是 label，不适用于 label 的校验规则。
// 预分配容量 5 是为了减少构造 fields.Set 时的扩容分配。
func PodToSelectableFields(pod *example.Pod) fields.Set {
	// 预分配指定容量的目的，是减少创建 fields.Set 时的分配次数。
	// 若在此添加字段或 object-meta 相关字段数量发生变化，
	// 应相应调整该容量。
	//
	podSpecificFieldsSet := make(fields.Set, 5)
	podSpecificFieldsSet["spec.nodeName"] = pod.Spec.NodeName
	podSpecificFieldsSet["spec.restartPolicy"] = string(pod.Spec.RestartPolicy)
	podSpecificFieldsSet["status.phase"] = string(pod.Status.Phase)
	return AddObjectMetaFieldsSet(podSpecificFieldsSet, &pod.ObjectMeta, true)
}

// AddObjectMetaFieldsSet 把对象元数据追加到字段集合，测试中复刻生产实现以保持字段名一致。
func AddObjectMetaFieldsSet(source fields.Set, objectMeta *metav1.ObjectMeta, hasNamespaceField bool) fields.Set {
	source["metadata.name"] = objectMeta.Name
	if hasNamespaceField {
		source["metadata.namespace"] = objectMeta.Namespace
	}
	return source
}

// checkStorageInvariants 是空实现：cacher 把创建直接透传给底层存储，无需额外校验。
func checkStorageInvariants(ctx context.Context, t *testing.T, key string) {
	// 空操作：cacher 只是把创建请求透传给底层存储。
}

// TestCreate 验证通过 cacher 创建对象的基础语义。
func TestCreate(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestCreate(ctx, t, cacher, checkStorageInvariants)
}

// TestCreateWithTTL 验证带 TTL 的创建路径。
func TestCreateWithTTL(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestCreateWithTTL(ctx, t, cacher)
}

// TestCreateWithKeyExist 验证键已存在时创建返回 AlreadyExists。
func TestCreateWithKeyExist(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestCreateWithKeyExist(ctx, t, cacher)
}

// TestGet 验证读取路径（含缓存命中与回源）。
func TestGet(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestGet(ctx, t, cacher)
}

// TestUnconditionalDelete 验证无条件删除。
func TestUnconditionalDelete(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestUnconditionalDelete(ctx, t, cacher)
}

// TestConditionalDelete 验证带前置条件的删除。
func TestConditionalDelete(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestConditionalDelete(ctx, t, cacher)
}

// TestDeleteWithSuggestion 验证删除冲突时返回最新对象作为“建议值”。
func TestDeleteWithSuggestion(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestDeleteWithSuggestion(ctx, t, cacher)
}

// TestDeleteWithSuggestionAndConflict 验证冲突建议值与版本冲突叠加时的行为。
func TestDeleteWithSuggestionAndConflict(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestDeleteWithSuggestionAndConflict(ctx, t, cacher)
}

// TestDeleteWithSuggestionOfDeletedObject 验证对已删除对象删除时返回 NotFound 而非冲突建议。
func TestDeleteWithSuggestionOfDeletedObject(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestDeleteWithSuggestionOfDeletedObject(ctx, t, cacher)
}

// TestValidateDeletionWithSuggestion 验证删除校验失败时仍返回建议对象。
func TestValidateDeletionWithSuggestion(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestValidateDeletionWithSuggestion(ctx, t, cacher)
}

// TestValidateDeletionWithOnlySuggestionValid 验证删除校验失败但前置条件满足时的行为。
func TestValidateDeletionWithOnlySuggestionValid(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestValidateDeletionWithOnlySuggestionValid(ctx, t, cacher)
}

// TestDeleteWithConflict 验证版本冲突的删除路径。
func TestDeleteWithConflict(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestDeleteWithConflict(ctx, t, cacher)
}

// testTransformer 包装真实 transformer，可切换为“总是失败”，
// 用于模拟存储数据无法解密/转换的场景。
type testTransformer struct {
	value.Transformer
	fail atomic.Bool
}

// setFailing 原子地切换失败状态，测试可在并发读路径中安全调用。
func (tt *testTransformer) setFailing(c bool) {
	tt.fail.Store(c)
}

// TransformFromStorage 在 fail 为真时返回合成错误，否则委托给真实实现。
func (tt *testTransformer) TransformFromStorage(ctx context.Context, data []byte, dataCtx value.Context) (out []byte, stale bool, err error) {
	if tt.fail.Load() {
		return nil, false, errors.New("synthetic error")
	}
	return tt.Transformer.TransformFromStorage(ctx, data, dataCtx)
}

// testCodec 包装真实 codec，可切换为“总是解码失败”，用于构造损坏对象。
type testCodec struct {
	runtime.Codec
	fail atomic.Bool
}

// setFailing 原子地切换失败状态。
func (tc *testCodec) setFailing(c bool) {
	tc.fail.Store(c)
}

// Decode 在 fail 为真时返回合成错误，否则委托给真实实现。
func (tc *testCodec) Decode(data []byte, defaults *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	if tc.fail.Load() {
		return nil, nil, errors.New("synthetic error")
	}
	return tc.Codec.Decode(data, defaults, into)
}

// TestDeleteWithConflictAndMissingExpectedTransformOrDecodeError 验证未开启“忽略转换/解码错误”
// 时，带冲突的删除不会把存储读取错误当作可忽略错误处理。
func TestDeleteWithConflictAndMissingExpectedTransformOrDecodeError(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.AllowUnsafeMalformedObjectDeletion, true)

	transformer := &testTransformer{Transformer: identity.NewEncryptCheckTransformer()}
	ctx, s, _ := testSetup(t, withTransformer(transformer))

	storagetesting.RunTestDeleteWithConflictAndMissingExpectedTransformOrDecodeError(ctx, t, s, transformer.setFailing)
}

// TestDeleteWithConflictAndExpectedTransformError 验证删除时转换错误被显式标记为可忽略
// （ExpectTransformOrDecodeError），从而允许删除损坏对象。
func TestDeleteWithConflictAndExpectedTransformError(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.AllowUnsafeMalformedObjectDeletion, true)

	transformer := &testTransformer{Transformer: identity.NewEncryptCheckTransformer()}
	ctx, cacher, terminate := testSetup(t, withTransformer(transformer))
	t.Cleanup(terminate)

	storagetesting.RunTestDeleteExpectedTransformOrDecodeError(ctx, t, cacher, transformer.setFailing)
}

// TestDeleteWithConflictAndExpectedDecodeError 验证解码错误同样可被显式忽略并完成删除。
func TestDeleteWithConflictAndExpectedDecodeError(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.AllowUnsafeMalformedObjectDeletion, true)

	// 测试中解码错误会触发 panic。本用例刻意构造存储字节无法解码的场景，
	// 因此需要临时关闭该行为。
	etcd3.TestOnlySetFatalOnDecodeError(t, false)

	codec := &testCodec{Codec: apitesting.TestCodec(codecs, examplev1.SchemeGroupVersion)}
	ctx, cacher, terminate := testSetup(t, withCodec(codec))
	t.Cleanup(terminate)

	storagetesting.RunTestDeleteExpectedTransformOrDecodeError(ctx, t, cacher, codec.setFailing)
}

// TestDeleteWithSuggestionAndMissingExpectedTransformOrDecodeError 验证带建议值的删除路径
// 在未声明可忽略错误时不会吞掉存储读取错误。
func TestDeleteWithSuggestionAndMissingExpectedTransformOrDecodeError(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.AllowUnsafeMalformedObjectDeletion, true)

	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestDeleteWithSuggestionAndMissingExpectedTransformOrDecodeError(ctx, t, cacher)
}

// TestPreconditionalDeleteWithSuggestion 验证带前置条件的删除在冲突时返回建议值。
func TestPreconditionalDeleteWithSuggestion(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestPreconditionalDeleteWithSuggestion(ctx, t, cacher)
}

// TestPreconditionalDeleteWithSuggestionPass 验证前置条件满足时删除成功。
func TestPreconditionalDeleteWithSuggestionPass(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestPreconditionalDeleteWithOnlySuggestionPass(ctx, t, cacher)
}

// TestListPaging 验证分页列表的续页（continue token）语义。
func TestListPaging(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestListPaging(ctx, t, cacher)
}

// TestLists 在 ListFromCacheSnapshot 开/关两种模式下验证列表行为，
// 包括普通 List、一致性 List 与非递归列表；子测试并行以缩短总耗时。
func TestLists(t *testing.T) {
	for _, listFromCacheSnapshot := range []bool{true, false} {
		t.Run(fmt.Sprintf("ListFromCacheSnapshot=%v", listFromCacheSnapshot), func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ListFromCacheSnapshot, listFromCacheSnapshot)
			t.Run("List", func(t *testing.T) {
				t.Parallel()
				ctx, cacher, server, terminate := testSetupWithEtcdServer(t)
				t.Cleanup(terminate)
				storagetesting.RunTestList(ctx, t, cacher, compactStore(cacher, server.V3Client.Client), true, server.V3Client.Kubernetes.(*storagetesting.KubernetesRecorder))
			})

			t.Run("ConsistentList", func(t *testing.T) {
				t.Parallel()
				ctx, cacher, server, terminate := testSetupWithEtcdServer(t)
				t.Cleanup(terminate)
				storagetesting.RunTestConsistentList(ctx, t, cacher, increaseRVFunc(server.V3Client.Client), true, true, listFromCacheSnapshot)
			})

			t.Run("GetListNonRecursive", func(t *testing.T) {
				t.Parallel()
				ctx, cacher, server, terminate := testSetupWithEtcdServer(t)
				t.Cleanup(terminate)
				storagetesting.RunTestGetListNonRecursive(ctx, t, increaseRVFunc(server.V3Client.Client), cacher)
			})
		})
	}
}

// TestCompactRevision 验证 etcd 压缩版本变化能被 cacher 感知并反映到列表行为。
func TestCompactRevision(t *testing.T) {
	// 该测试要求存储能观察到压缩 revision 的外部变化，需要开启对 compact key 的专用 watch。
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ListFromCacheSnapshot, true)
	ctx, cacher, server, terminate := testSetupWithEtcdServer(t)
	t.Cleanup(terminate)
	storagetesting.RunTestCompactRevision(ctx, t, cacher, increaseRVFunc(server.V3Client.Client), compactStore(cacher, server.V3Client.Client))
}

// TestMarkConsistent 验证缓存一致性标记对快照与回源的影响：
// 一致时可服务快照、跳过 etcd；标记为不一致时清空旧快照并回源，且不再收集新快照。
func TestMarkConsistent(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ListFromCacheSnapshot, true)
	ctx, cacher, server, terminate := testSetupWithEtcdServer(t)
	t.Cleanup(terminate)
	recorder := server.V3Client.Kubernetes.(*storagetesting.KubernetesRecorder)

	t.Log("New cache collects snapshots, list skips etcd")
	resourceVersion1 := createObject(t, ctx, cacher)
	etcdRequests := etcdListRequests(t, ctx, cacher, recorder, storage.ListOptions{
		Predicate:            storage.Everything,
		ResourceVersionMatch: metav1.ResourceVersionMatchExact,
		ResourceVersion:      resourceVersion1,
		Recursive:            true,
	})
	if len(etcdRequests) != 0 {
		t.Errorf("Expected no requests to etcd, got: %+v", etcdRequests)
	}

	t.Log("Inconsistent cache clears old snapshots, list hits etcd")
	cacher.cacher.MarkConsistent(false)
	etcdRequests = etcdListRequests(t, ctx, cacher, recorder, storage.ListOptions{
		Predicate:            storage.Everything,
		ResourceVersionMatch: metav1.ResourceVersionMatchExact,
		ResourceVersion:      resourceVersion1,
		Recursive:            true,
	})
	if len(etcdRequests) != 1 {
		t.Errorf("Expected request to etcd, got: %+v", etcdRequests)
	}

	t.Log("Inconsistent cache doesn't collect new snapshot, list hits etcd")
	resourceVersion2 := createObject(t, ctx, cacher)
	etcdRequests = etcdListRequests(t, ctx, cacher, recorder, storage.ListOptions{
		Predicate:            storage.Everything,
		ResourceVersionMatch: metav1.ResourceVersionMatchExact,
		ResourceVersion:      resourceVersion2,
		Recursive:            true,
	})
	if len(etcdRequests) != 1 {
		t.Errorf("Expected request to etcd, got: %+v", etcdRequests)
	}

	t.Log("Marking cache consistent allows it to collect new snapshots, list skips etcd")
	cacher.cacher.MarkConsistent(true)
	resourceVersion3 := createObject(t, ctx, cacher)
	etcdRequests = etcdListRequests(t, ctx, cacher, recorder, storage.ListOptions{
		Predicate:            storage.Everything,
		ResourceVersionMatch: metav1.ResourceVersionMatchExact,
		ResourceVersion:      resourceVersion3,
		Recursive:            true,
	})
	if len(etcdRequests) != 0 {
		t.Errorf("Expected no requests to etcd, got: %+v", etcdRequests)
	}
}

// createObject 创建随机命名的 Pod 并返回其 resourceVersion，便于后续按精确版本查询。
func createObject(t *testing.T, ctx context.Context, store storage.Interface) string {
	var out example.Pod
	pod := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: rand.String(10)}}
	err := store.Create(ctx, computePodKey(pod), pod, &out, 0)
	if err != nil {
		t.Fatal(err)
	}
	return out.ResourceVersion
}

// etcdListRequests 执行一次列表并返回本次请求触发的 etcd 调用记录；
// 通过随机 context key 把本次列表与记录器中的其他调用区分开。
func etcdListRequests(t *testing.T, ctx context.Context, store storage.Interface, recorder *storagetesting.KubernetesRecorder, opts storage.ListOptions) []storagetesting.RecordedList {
	key := rand.String(10)
	listCtx := context.WithValue(ctx, storagetesting.RecorderContextKey, key)
	listOut := &example.PodList{}
	if err := store.GetList(listCtx, "/pods/", opts, listOut); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	return recorder.ListRequestForKey(key)
}

// TestGetListRecursivePrefix 验证递归前缀列表的键匹配范围。
func TestGetListRecursivePrefix(t *testing.T) {
	ctx, store, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestGetListRecursivePrefix(ctx, t, store)
}

// checkStorageCalls 是空实现：分页调用直接透传给底层存储，cacher 不额外拦截。
func checkStorageCalls(t *testing.T, pageSize, estimatedProcessedObjects uint64) {
	// 目前为空操作：cacher 把分页调用透传给底层存储。
}

// TestListContinuation 验证列表续页的连续性。
func TestListContinuation(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestListContinuation(ctx, t, cacher, checkStorageCalls)
}

// TestListPaginationRareObject 验证稀疏对象下的分页仍能正确翻页。
func TestListPaginationRareObject(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestListPaginationRareObject(ctx, t, cacher, checkStorageCalls)
}

// TestListContinuationWithFilter 验证带字段过滤时分页语义仍然正确。
func TestListContinuationWithFilter(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestListContinuationWithFilter(ctx, t, cacher, checkStorageCalls)
}

// TestListInconsistentContinuation 暂未启用，等待 #109831 提供压缩控制后再接入共享用例。
func TestListInconsistentContinuation(t *testing.T) {
	// TODO(#109831): 设置压缩后启用该用例。
}

// TestListResourceVersionMatch 暂未启用，原因同 TestListInconsistentContinuation。
func TestListResourceVersionMatch(t *testing.T) {
	// TODO(#109831): 设置压缩后启用该用例。
}

// TestNamespaceScopedList 在配置了 nodeName/namespace 索引时验证命名空间级列表。
func TestNamespaceScopedList(t *testing.T) {
	ctx, cacher, terminate := testSetup(t, withNodeNameAndNamespaceIndex)
	t.Cleanup(terminate)
	storagetesting.RunTestNamespaceScopedList(ctx, t, cacher)
}

// TestGuaranteedUpdate 暂未启用，等待 #109831。
func TestGuaranteedUpdate(t *testing.T) {
	// TODO(#109831): 设置压缩后启用该用例。
}

// TestGuaranteedUpdateWithTTL 验证带 TTL 的保证更新。
func TestGuaranteedUpdateWithTTL(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestGuaranteedUpdateWithTTL(ctx, t, cacher)
}

// TestGuaranteedUpdateChecksStoredData 暂未启用，等待 #109831。
func TestGuaranteedUpdateChecksStoredData(t *testing.T) {
	// TODO(#109831): 设置压缩后启用该用例。
}

// TestGuaranteedUpdateWithConflict 验证更新冲突时的重试语义。
func TestGuaranteedUpdateWithConflict(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestGuaranteedUpdateWithConflict(ctx, t, cacher)
}

// TestGuaranteedUpdateWithSuggestionAndConflict 验证冲突建议值与重试结合时的行为。
func TestGuaranteedUpdateWithSuggestionAndConflict(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestGuaranteedUpdateWithSuggestionAndConflict(ctx, t, cacher)
}

// TestTransformationFailure 暂未启用，等待 #109831。
func TestTransformationFailure(t *testing.T) {
	// TODO(#109831): 设置压缩后启用该用例。
}

// TestStats 在 SizeBasedListCostEstimate 开/关两种模式下验证存储统计指标。
func TestStats(t *testing.T) {
	for _, sizeBasedListCostEstimate := range []bool{true, false} {
		t.Run(fmt.Sprintf("SizeBasedListCostEstimate=%v", sizeBasedListCostEstimate), func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.SizeBasedListCostEstimate, sizeBasedListCostEstimate)
			ctx, cacher, terminate := testSetup(t)
			t.Cleanup(terminate)
			storagetesting.RunTestStats(ctx, t, cacher, examplev1ProtoCodec, identity.NewEncryptCheckTransformer(), sizeBasedListCostEstimate)
		})
	}
}

// TestKeySchema 验证键编码/解码的 schema 兼容性。
func TestKeySchema(t *testing.T) {
	ctx, cacher, terminate := testSetup(t)
	t.Cleanup(terminate)
	storagetesting.RunTestKeySchema(ctx, t, cacher)
}

// TestWatch 汇总 watch 相关子用例：初始事件、从零/非零版本开始、删除事件、
// 延迟投递、bookmark、超时、上下文取消（部分用例因 #109831 暂未启用）以及集群级/命名空间级 watch。
func TestWatch(t *testing.T) {
	t.Run("Watch", func(t *testing.T) {
		ctx, cacher, terminate := testSetup(t)
		t.Cleanup(terminate)
		storagetesting.RunTestWatch(ctx, t, cacher)
	})
	t.Run("WatchFromZero", func(t *testing.T) {
		ctx, cacher, server, terminate := testSetupWithEtcdServer(t)
		t.Cleanup(terminate)
		storagetesting.RunTestWatchFromZero(ctx, t, cacher, compactWatch(cacher, server.V3Client.Client))
	})
	t.Run("DeleteTriggerWatch", func(t *testing.T) {
		ctx, cacher, terminate := testSetup(t)
		t.Cleanup(terminate)
		storagetesting.RunTestDeleteTriggerWatch(ctx, t, cacher)
	})
	t.Run("WatchFromNonZero", func(t *testing.T) {
		ctx, cacher, terminate := testSetup(t)
		t.Cleanup(terminate)
		storagetesting.RunTestWatchFromNonZero(ctx, t, cacher)
	})
	t.Run("DelayedWatchDelivery", func(t *testing.T) {
		ctx, cacher, terminate := testSetup(t)
		t.Cleanup(terminate)
		storagetesting.RunTestDelayedWatchDelivery(ctx, t, cacher)
	})
	t.Run("WatchError", func(t *testing.T) {
		// TODO(#109831): 设置压缩后启用该用例。
	})
	t.Run("WatchContextCancel", func(t *testing.T) {
		// TODO(#109831): 设置压缩后启用该用例。
	})
	t.Run("WatcherTimeout", func(t *testing.T) {
		ctx, cacher, terminate := testSetup(t)
		t.Cleanup(terminate)
		storagetesting.RunTestWatcherTimeout(ctx, t, cacher)
	})
	t.Run("WatchDeleteEventObjectHaveLatestRV", func(t *testing.T) {
		ctx, cacher, terminate := testSetup(t)
		t.Cleanup(terminate)
		storagetesting.RunTestWatchDeleteEventObjectHaveLatestRV(ctx, t, cacher)
	})
	t.Run("WatchInitializationSignal", func(t *testing.T) {
		ctx, cacher, terminate := testSetup(t)
		t.Cleanup(terminate)
		storagetesting.RunTestWatchInitializationSignal(ctx, t, cacher)
	})
	t.Run("ClusterScopedWatch", func(t *testing.T) {
		ctx, cacher, terminate := testSetup(t, withClusterScopedKeyFunc, withNodeNameAndNamespaceIndex)
		t.Cleanup(terminate)
		storagetesting.RunTestClusterScopedWatch(ctx, t, cacher)
	})
	t.Run("NamespaceScopedWatch", func(t *testing.T) {
		ctx, cacher, terminate := testSetup(t, withNodeNameAndNamespaceIndex)
		t.Cleanup(terminate)
		storagetesting.RunTestNamespaceScopedWatch(ctx, t, cacher)
	})
	t.Run("WatchDispatchBookmarkEvents", func(t *testing.T) {
		ctx, cacher, terminate := testSetup(t)
		t.Cleanup(terminate)
		storagetesting.RunTestWatchDispatchBookmarkEvents(ctx, t, cacher, true)
	})
	t.Run("WatchBookmarksWithCorrectResourceVersion", func(t *testing.T) {
		ctx, cacher, terminate := testSetup(t)
		t.Cleanup(terminate)
		storagetesting.RunTestOptionalWatchBookmarksWithCorrectResourceVersion(ctx, t, cacher)
	})
	t.Run("SendInitialEventsBackwardCompatibility", func(t *testing.T) {
		ctx, store, terminate := testSetup(t)
		t.Cleanup(terminate)
		storagetesting.RunSendInitialEventsBackwardCompatibility(ctx, t, store)
	})
	t.Run("WatchSemantics", func(t *testing.T) {
		store, terminate := testSetupWithEtcdAndCreateWrapper(t)
		t.Cleanup(terminate)
		storagetesting.RunWatchSemantics(context.TODO(), t, store)
	})
	t.Run("WatchSemanticInitialEventsExtended", func(t *testing.T) {
		store, terminate := testSetupWithEtcdAndCreateWrapper(t)
		t.Cleanup(terminate)
		storagetesting.RunWatchSemanticInitialEventsExtended(context.TODO(), t, store)
	})
	t.Run("WatchListMatchSingle", func(t *testing.T) {
		store, terminate := testSetupWithEtcdAndCreateWrapper(t)
		t.Cleanup(terminate)
		storagetesting.RunWatchListMatchSingle(context.TODO(), t, store)
	})
}

// ===================================================
// 以下是测试装配相关的函数。
// ===================================================

// tearDownFunc 清理测试启动的 cacher、delegator 与 etcd 服务。
type tearDownFunc func()

// setupOptions 汇总测试装配的可选参数，由 setupOption 函数修改。
type setupOptions struct {
	resourcePrefix string
	keyFunc        func(runtime.Object) (string, error)
	indexerFuncs   map[string]storage.IndexerFunc
	indexers       cache.Indexers
	clock          clock.WithTicker
	codec          runtime.Codec
	transformer    value.Transformer
}

// setupOption 以函数式选项方式覆盖 setupOptions 的默认值。
type setupOption func(*setupOptions)

// withDefaults 设置命名空间级 Pod 存储的默认参数；它总是作为第一个选项执行，
// 后续选项在其基础上覆盖。
func withDefaults(options *setupOptions) {
	prefix := "/pods/"

	options.resourcePrefix = prefix
	options.keyFunc = func(obj runtime.Object) (string, error) { return storage.NamespaceKeyFunc(prefix, obj) }
	options.clock = clock.RealClock{}
	options.codec = examplev1ProtoCodec
	options.transformer = identity.NewEncryptCheckTransformer()
}

// withClusterScopedKeyFunc 切换到集群级资源键函数，用于验证无命名空间的 watch/list。
func withClusterScopedKeyFunc(options *setupOptions) {
	options.keyFunc = func(obj runtime.Object) (string, error) {
		return storage.NoNamespaceKeyFunc(options.resourcePrefix, obj)
	}
}

// 索引配置与 pkg/registry/core/pod/strategy.go 保持一致。
// 同时配置 cacher 内部索引与 client-go cache 索引，确保测试覆盖两条索引路径。
func withNodeNameAndNamespaceIndex(options *setupOptions) {
	options.indexerFuncs = map[string]storage.IndexerFunc{
		"spec.nodeName": func(obj runtime.Object) string {
			pod, ok := obj.(*example.Pod)
			if !ok {
				return ""
			}
			return pod.Spec.NodeName
		},
	}
	options.indexers = map[string]cache.IndexFunc{
		"f:spec.nodeName": func(obj interface{}) ([]string, error) {
			pod := obj.(*example.Pod)
			return []string{pod.Spec.NodeName}, nil
		},
		"f:metadata.namespace": func(obj interface{}) ([]string, error) {
			pod := obj.(*example.Pod)
			return []string{pod.ObjectMeta.Namespace}, nil
		},
	}
}

// withCodec 覆盖默认 codec，用于注入解码失败。
func withCodec(codec runtime.Codec) setupOption {
	return func(options *setupOptions) {
		options.codec = codec
	}
}

// withTransformer 覆盖默认 transformer，用于注入解密/转换失败。
func withTransformer(transformer value.Transformer) setupOption {
	return func(options *setupOptions) {
		options.transformer = transformer
	}
}

// testSetup 是 testSetupWithEtcdServer 的简化封装，丢弃 etcd server 句柄。
func testSetup(t *testing.T, opts ...setupOption) (context.Context, *CacheDelegator, tearDownFunc) {
	ctx, cacher, _, tearDown := testSetupWithEtcdServer(t, opts...)
	return ctx, cacher, tearDown
}

// testSetupWithEtcdServer 启动真实 etcd 测试服务与 cacher，并等待两者就绪。
// 注意两个刻意的“预热”步骤：
//   - 注入一次 list 错误，确保测试覆盖 relist 恢复路径；
//   - 等待 watch cache 完成初始化，避免测试因 429（初始化中）而失败。
func testSetupWithEtcdServer(t testing.TB, opts ...setupOption) (context.Context, *CacheDelegator, *etcd3testing.EtcdTestServer, tearDownFunc) {
	setupOpts := setupOptions{}
	opts = append([]setupOption{withDefaults}, opts...)
	for _, opt := range opts {
		opt(&setupOpts)
	}

	server, etcdStorage := newEtcdTestStorageWithOptions(t, etcd3testing.PathPrefix(), setupOpts.codec, setupOpts.transformer)
	// 注入一次 list 错误，以覆盖重新列举路径。
	listErrors := 1
	if clientfeatures.FeatureGates().Enabled(clientfeatures.WatchListClient) {
		// WatchListClient 特性会改变 reflector 使用 WATCH 的方式。
		// 而非 LIST，因此不应出现错误。
		listErrors = 0
	}
	wrappedStorage := &storagetesting.StorageInjectingListErrors{
		Interface: etcdStorage,
		Errors:    listErrors,
	}

	config := Config{
		Storage:             wrappedStorage,
		Versioner:           storage.APIObjectVersioner{},
		GroupResource:       schema.GroupResource{Resource: "pods"},
		EventsHistoryWindow: DefaultEventFreshDuration,
		ResourcePrefix:      setupOpts.resourcePrefix,
		KeyFunc:             setupOpts.keyFunc,
		GetAttrsFunc:        GetPodAttrs,
		NewFunc:             newPod,
		NewListFunc:         newPodList,
		IndexerFuncs:        setupOpts.indexerFuncs,
		Indexers:            &setupOpts.indexers,
		Codec:               setupOpts.codec,
		Clock:               setupOpts.clock,
	}
	cacher, err := NewCacherFromConfig(config)
	if err != nil {
		t.Fatalf("Failed to initialize cacher: %v", err)
	}
	ctx := context.Background()

	// 某些测试依赖 GetList 不失败，
	// 等待底层存储的错误被消费掉。
	if err := wait.PollInfinite(100*time.Millisecond, wrappedStorage.ErrorsConsumed); err != nil {
		t.Fatalf("Failed to inject list errors: %v", err)
	}

	// 测试假定 Get/GetList/Watch 调用不会失败。
	// 不过，watchcache 初始化期间现在也可能返回 429。
	// 为避免改写所有测试，这里等待 watchcache 完成初始化。
	if err := cacher.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	delegator := NewCacheDelegator(cacher, wrappedStorage)
	terminate := func() {
		delegator.Stop()
		cacher.Stop()
		server.Terminate(t)
	}

	return ctx, delegator, server, terminate
}

// testSetupWithEtcdAndCreateWrapper 返回一个 Create 后会等待对象在缓存中可见的存储包装，
// 供 watch 语义测试使用——它们依赖“写入后立即可被 watch 观察到”。
func testSetupWithEtcdAndCreateWrapper(t *testing.T, opts ...setupOption) (storage.Interface, tearDownFunc) {
	_, cacher, _, tearDown := testSetupWithEtcdServer(t, opts...)

	return &createWrapper{CacheDelegator: cacher}, tearDown
}

// createWrapper 在创建后轮询缓存，直到读到的对象与写入值一致。
type createWrapper struct {
	*CacheDelegator
}

// Create 先委托创建，再以 ResourceVersion="0"（任意版本）轮询读取，
// 消除“创建返回但 watch cache 尚未应用事件”的竞态。
func (c *createWrapper) Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error {
	if err := c.CacheDelegator.Create(ctx, key, obj, out, ttl); err != nil {
		return err
	}
	return wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, wait.ForeverTestTimeout, true, func(ctx context.Context) (bool, error) {
		currentObj := c.CacheDelegator.cacher.newFunc()
		err := c.CacheDelegator.Get(ctx, key, storage.GetOptions{ResourceVersion: "0"}, currentObj)
		if err != nil {
			if storage.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}
		if !apiequality.Semantic.DeepEqual(currentObj, out) {
			return false, nil
		}
		return true, nil
	})
}

// BenchmarkStoreWriteThroughput 测量在接近生产规模的数据集上写入的吞吐。
// 通过包装 eventHandler 记录事件延迟，模拟真实 apiserver 的观测方式。
func BenchmarkStoreWriteThroughput(b *testing.B) {
	klog.SetLogger(logr.Discard())
	dimensions := []struct {
		namespaceCount       int
		podPerNamespaceCount int
		nodeCount            int
	}{
		{
			namespaceCount:       50,
			podPerNamespaceCount: 3_000,
			nodeCount:            5_000,
		},
	}
	for _, dims := range dimensions {
		b.Run(fmt.Sprintf("Namespaces=%d/Pods=%d/Nodes=%d", dims.namespaceCount, dims.namespaceCount*dims.podPerNamespaceCount, dims.nodeCount), func(b *testing.B) {
			opts := []setupOption{withNodeNameAndNamespaceIndex}
			ctx, cacher, _, terminate := testSetupWithEtcdServer(b, opts...)
			b.Cleanup(terminate)
			data := storagetesting.PrepareBenchmarkData(dims.namespaceCount, dims.podPerNamespaceCount, dims.nodeCount)
			tracker := storagetesting.NewWatchLatencyTracker(clock.RealClock{})
			originalHandler := cacher.cacher.watchCache.config.eventHandler
			cacher.cacher.watchCache.config.eventHandler = func(event *watchCacheEvent) {
				if originalHandler != nil {
					originalHandler(event)
				}
				tracker.HandleEvent(event.Object)
			}
			b.Cleanup(func() {
				cacher.cacher.watchCache.config.eventHandler = originalHandler
			})
			storagetesting.RunBenchmarkWriteThroughput(ctx, b, cacher, data, true, tracker)
		})
	}
}

// BenchmarkStoreList 在多种集群规模下比较使用索引与全量扫描的列表性能。
func BenchmarkStoreList(b *testing.B) {
	klog.SetLogger(logr.Discard())
	// 参考 https://github.com/kubernetes/community/blob/master/sig-scalability/configs-and-limits/thresholds.md
	dimensions := []struct {
		namespaceCount       int
		podPerNamespaceCount int
		nodeCount            int
	}{
		{
			namespaceCount:       10_000,
			podPerNamespaceCount: 15,
			nodeCount:            5_000,
		},
		{
			namespaceCount:       50,
			podPerNamespaceCount: 3_000,
			nodeCount:            5_000,
		},
		{
			namespaceCount:       100,
			podPerNamespaceCount: 1_100,
			nodeCount:            1000,
		},
	}
	for _, dims := range dimensions {
		b.Run(fmt.Sprintf("Namespaces=%d/Pods=%d/Nodes=%d", dims.namespaceCount, dims.namespaceCount*dims.podPerNamespaceCount, dims.nodeCount), func(b *testing.B) {
			data := storagetesting.PrepareBenchmarkData(dims.namespaceCount, dims.podPerNamespaceCount, dims.nodeCount)
			ctx, cacher, _, terminate := testSetupWithEtcdServer(b, withNodeNameAndNamespaceIndex)
			b.Cleanup(terminate)
			require.NoError(b, storagetesting.PrecreateBenchmarkPods(ctx, cacher, data))
			for _, useIndex := range []bool{true, false} {
				b.Run(fmt.Sprintf("Indexed=%v", useIndex), func(b *testing.B) {
					storagetesting.RunBenchmarkStoreList(ctx, b, cacher, data, useIndex)
				})
			}
		})
	}
}

// BenchmarkStoreStats 测量统计接口在 15 万对象规模下的开销。
func BenchmarkStoreStats(b *testing.B) {
	klog.SetLogger(logr.Discard())
	data := storagetesting.PrepareBenchmarkData(50, 3_000, 5_000)
	ctx, cacher, _, terminate := testSetupWithEtcdServer(b)
	b.Cleanup(terminate)
	var out example.Pod
	for _, pod := range data.Pods {
		err := cacher.Create(ctx, computePodKey(pod), pod, &out, 0)
		if err != nil {
			b.Fatal(err)
		}
	}
	storagetesting.RunBenchmarkStoreStats(ctx, b, cacher)
}
