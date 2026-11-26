/*
Copyright 2016 The Kubernetes Authors.

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
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"go.etcd.io/etcd/server/v3/embed"
	"google.golang.org/grpc/grpclog"

	"k8s.io/apimachinery/pkg/api/apitesting"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/apis/example"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/etcd3/metrics"
	"k8s.io/apiserver/pkg/storage/etcd3/testserver"
	etcdfeature "k8s.io/apiserver/pkg/storage/feature"
	storagemetrics "k8s.io/apiserver/pkg/storage/metrics"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"
	"k8s.io/apiserver/pkg/storage/value"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

var scheme = runtime.NewScheme()
var codecs = serializer.NewCodecFactory(scheme)

const defaultTestPrefix = "test!"

func init() {
	metav1.AddToGroupVersion(scheme, metav1.SchemeGroupVersion)
	utilruntime.Must(example.AddToScheme(scheme))
	utilruntime.Must(examplev1.AddToScheme(scheme))

	// grpclog 的 Info/Warning 输出丢弃，只保留 Error：etcd 嵌入式实例在测试中
	// 会产生大量噪音，但错误仍需要可见。
	grpclog.SetLoggerV2(grpclog.NewLoggerV2(io.Discard, io.Discard, os.Stderr))
}

// newPod 返回测试用的 example.Pod 空对象。
func newPod() runtime.Object {
	return &example.Pod{}
}

// newPodList 返回测试用的 example.PodList 空对象。
func newPodList() runtime.Object {
	return &example.PodList{}
}

// checkStorageInvariants 断言写入 etcd 的原始值满足存储层不变量：
// 能被 codec 解码，且持久化对象不应携带 resourceVersion/selfLink。
// RV 由 etcd 在读取时填充，selfLink 已被废弃；若写入时就带上它们，
// 说明序列化路径有误，会污染后续读出的对象。
func checkStorageInvariants(etcdClient *clientv3.Client, codec runtime.Codec) storagetesting.KeyValidation {
	return func(ctx context.Context, t *testing.T, key string) {
		getResp, err := etcdClient.KV.Get(ctx, key)
		if err != nil {
			t.Fatalf("etcdClient.KV.Get failed: %v", err)
		}
		if len(getResp.Kvs) == 0 {
			t.Fatalf("expecting non empty result on key: %s", key)
		}
		decoded, err := runtime.Decode(codec, getResp.Kvs[0].Value[len(defaultTestPrefix):])
		if err != nil {
			t.Fatalf("expecting successful decode of object from %v\n%v", err, string(getResp.Kvs[0].Value))
		}
		obj := decoded.(*example.Pod)
		if obj.ResourceVersion != "" {
			t.Errorf("stored object should have empty resource version")
		}
		if obj.SelfLink != "" {
			t.Errorf("stored output should have empty selfLink")
		}
	}
}

// TestCreate 验证创建语义，并额外检查落盘的原始字节满足存储不变量。
func TestCreate(t *testing.T) {
	ctx, store, etcdClient := testSetup(t)
	storagetesting.RunTestCreate(ctx, t, store, checkStorageInvariants(etcdClient.Client, store.codec))
}

// TestCreateWithTTL 验证带 TTL 的创建：对象应挂到 lease 上并在到期后消失。
func TestCreateWithTTL(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestCreateWithTTL(ctx, t, store)
}

// TestCreateWithKeyExist 验证对已存在 key 的创建应返回 AlreadyExists。
func TestCreateWithKeyExist(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestCreateWithKeyExist(ctx, t, store)
}

// TestGet 验证按键读取，包括不存在与 resourceVersion 匹配选项。
func TestGet(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestGet(ctx, t, store)
}

// TestUnconditionalDelete 验证无条件删除：对象不存在时返回 NotFound。
func TestUnconditionalDelete(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestUnconditionalDelete(ctx, t, store)
}

// TestConditionalDelete 验证带前置条件（UID/RV）的删除：条件不满足时返回冲突。
func TestConditionalDelete(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestConditionalDelete(ctx, t, store)
}

// TestDeleteWithSuggestion 验证冲突响应中携带当前对象（suggestion），供客户端重试。
func TestDeleteWithSuggestion(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestDeleteWithSuggestion(ctx, t, store)
}

// TestDeleteWithSuggestionAndConflict 验证删除时建议值本身已过期（并发修改）的处理。
func TestDeleteWithSuggestionAndConflict(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestDeleteWithSuggestionAndConflict(ctx, t, store)
}

// TestDeleteWithSuggestionOfDeletedObject 验证对象已被删除时不再返回建议值。
func TestDeleteWithSuggestionOfDeletedObject(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestDeleteWithSuggestionOfDeletedObject(ctx, t, store)
}

// TestValidateDeletionWithSuggestion 验证前置条件校验失败时返回的建议值。
func TestValidateDeletionWithSuggestion(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestValidateDeletionWithSuggestion(ctx, t, store)
}

// TestValidateDeletionWithOnlySuggestionValid 验证仅当建议值满足全部前置条件时才被返回。
func TestValidateDeletionWithOnlySuggestionValid(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestValidateDeletionWithOnlySuggestionValid(ctx, t, store)
}

// TestDeleteWithConflict 验证删除遇到 resourceVersion 冲突时返回 Conflict 并附当前对象。
func TestDeleteWithConflict(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestDeleteWithConflict(ctx, t, store)
}

// testTransformer 包装真实转换器，可通过 setFailing 注入变换失败。
type testTransformer struct {
	value.Transformer
	fail atomic.Bool
}

// setFailing 切换是否注入合成错误；使用原子布尔以支持并发访问。
func (tt *testTransformer) setFailing(c bool) {
	tt.fail.Store(c)
}

// TransformFromStorage 在失败模式下直接返回合成错误，模拟密文损坏。
func (tt *testTransformer) TransformFromStorage(ctx context.Context, data []byte, dataCtx value.Context) (out []byte, stale bool, err error) {
	if tt.fail.Load() {
		return nil, false, errors.New("synthetic error")
	}
	return tt.Transformer.TransformFromStorage(ctx, data, dataCtx)
}

// testCodec 包装真实 codec，可通过 setFailing 注入解码失败。
type testCodec struct {
	runtime.Codec
	fail atomic.Bool
}

// setFailing 切换是否注入解码错误。
func (tc *testCodec) setFailing(c bool) {
	tc.fail.Store(c)
}

// Decode 在失败模式下返回合成错误，模拟存储内容无法被解析。
func (tc *testCodec) Decode(data []byte, defaults *schema.GroupVersionKind, into runtime.Object) (runtime.Object, *schema.GroupVersionKind, error) {
	if tc.fail.Load() {
		return nil, nil, errors.New("synthetic error")
	}
	return tc.Codec.Decode(data, defaults, into)
}

// TestDeleteWithConflictAndMissingExpectedTransformOrDecodeError 验证：当损坏数据并非
// 由预期的变换/解码错误导致时，即使开启不安全删除门控也不应放行。
func TestDeleteWithConflictAndMissingExpectedTransformOrDecodeError(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.AllowUnsafeMalformedObjectDeletion, true)

	codec := &testCodec{Codec: apitesting.TestCodec(codecs, examplev1.SchemeGroupVersion)}
	ctx, s, _ := testSetup(t, withCodec(codec))

	storagetesting.RunTestDeleteWithConflictAndMissingExpectedTransformOrDecodeError(ctx, t, s, codec.setFailing)
}

// TestDeleteWithConflictAndExpectedTransformError 验证变换失败时可走不安全删除路径
// （AllowUnsafeMalformedObjectDeletion），避免损坏对象永久阻塞删除。
func TestDeleteWithConflictAndExpectedTransformError(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.AllowUnsafeMalformedObjectDeletion, true)

	transformer := &testTransformer{Transformer: newTestTransformer()}
	ctx, s, _ := testSetup(t, withTransformer(transformer))

	storagetesting.RunTestDeleteExpectedTransformOrDecodeError(ctx, t, s, transformer.setFailing)
}

// TestDeleteWithConflictAndExpectedDecodeError 验证解码失败时的不安全删除路径。
func TestDeleteWithConflictAndExpectedDecodeError(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.AllowUnsafeMalformedObjectDeletion, true)

	codec := &testCodec{Codec: apitesting.TestCodec(codecs, examplev1.SchemeGroupVersion)}
	ctx, s, _ := testSetup(t, withCodec(codec))

	storagetesting.RunTestDeleteExpectedTransformOrDecodeError(ctx, t, s, codec.setFailing)
}

// TestDeleteWithSuggestionAndMissingExpectedTransformOrDecodeFailure 验证建议值场景下
// 错误类型不匹配时的拒绝行为。
func TestDeleteWithSuggestionAndMissingExpectedTransformOrDecodeFailure(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.AllowUnsafeMalformedObjectDeletion, true)

	ctx, store, _ := testSetup(t)
	storagetesting.RunTestDeleteWithSuggestionAndMissingExpectedTransformOrDecodeError(ctx, t, store)
}

// TestPreconditionalDeleteWithSuggestion 验证带前置条件的删除在冲突时返回建议值。
func TestPreconditionalDeleteWithSuggestion(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestPreconditionalDeleteWithSuggestion(ctx, t, store)
}

// TestPreconditionalDeleteWithSuggestionPass 验证前置条件满足时删除成功且不返回建议值。
func TestPreconditionalDeleteWithSuggestionPass(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestPreconditionalDeleteWithOnlySuggestionPass(ctx, t, store)
}

// TestListPaging 验证分页：continue token 能正确续读且不重不漏。
func TestListPaging(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestListPaging(ctx, t, store)
}

// TestGetListNonRecursive 验证非递归列举只返回直接子节点（模拟「目录」语义）。
func TestGetListNonRecursive(t *testing.T) {
	ctx, store, client := testSetup(t)
	storagetesting.RunTestGetListNonRecursive(ctx, t, increaseRVFunc(client.Client), store)
}

// TestGetListRecursivePrefix 验证递归前缀列举返回所有后代。
func TestGetListRecursivePrefix(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestGetListRecursivePrefix(ctx, t, store)
}

// TestKeySchema 验证键的编码/解码往返，确保路径结构稳定。
func TestKeySchema(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestKeySchema(ctx, t, store)
}

// TestGetListWithErrorAggregation 验证门控开启时，列举会聚合损坏对象错误并继续，
// 而不是整批失败——这保证单个坏对象不会让整个 LIST 不可用。
func TestGetListWithErrorAggregation(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.AllowUnsafeMalformedObjectDeletion, true)
	ctx, s, _ := testSetup(t)
	store := NewStoreWithUnsafeCorruptObjectDeletion(s, s.groupResource)
	corruptErr := &corruptObjectError{err: fmt.Errorf("bits flipped"), errType: untransformable}
	storagetesting.RunTestGetListWithErrorAggregation(ctx, t, &storeWithTransformerOverride{Interface: store, store: s}, corruptErr)
}

// TestGetListWithoutErrorAggregation 验证门控关闭时，遇到损坏对象直接返回错误。
func TestGetListWithoutErrorAggregation(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.AllowUnsafeMalformedObjectDeletion, false)
	ctx, s, _ := testSetup(t)
	corruptErr := &corruptObjectError{err: fmt.Errorf("bits flipped"), errType: untransformable}
	storagetesting.RunTestGetListWithoutErrorAggregation(ctx, t, &storeWithTransformerOverride{Interface: s, store: s}, corruptErr)
}

// storeWithPrefixTransformer 让测试临时替换 store 与 watcher 的前缀转换器。
type storeWithPrefixTransformer struct {
	*store
}

// UpdatePrefixTransformer 替换转换器并返回恢复函数。
// 必须同时更新 store 与 watcher 的转换器，否则二者对同一份数据
// 使用不同前缀，测试会出现难以解释的解码失败。
func (s *storeWithPrefixTransformer) UpdatePrefixTransformer(modifier storagetesting.PrefixTransformerModifier) func() {
	originalTransformer := s.transformer.(*storagetesting.PrefixTransformer)
	transformer := *originalTransformer
	s.transformer = modifier(&transformer)
	s.watcher.transformer = modifier(&transformer)
	return func() {
		s.transformer = originalTransformer
		s.watcher.transformer = originalTransformer
	}
}

// storeWithTransformerOverride 把 storage.Interface 调用委托给包装后的 store，
// 同时保留原始 *store 以便修改其转换器。
type storeWithTransformerOverride struct {
	storage.Interface
	// 保留原始 *store 以便修改其转换器。
	store *store
}

// UpdateTransformer 替换任意转换器并返回恢复函数，同样同步 watcher。
func (s *storeWithTransformerOverride) UpdateTransformer(modifier storagetesting.TransformerModifier) func() {
	orig := s.store.transformer
	next := modifier(orig)
	s.store.transformer = next
	s.store.watcher.transformer = next
	return func() {
		s.store.transformer = orig
		s.store.watcher.transformer = orig
	}
}

// TestGuaranteedUpdate 验证「读取-修改-重试」的保证式更新，包括写回不变量检查。
func TestGuaranteedUpdate(t *testing.T) {
	ctx, store, client := testSetup(t)
	storagetesting.RunTestGuaranteedUpdate(ctx, t, &storeWithPrefixTransformer{store}, checkStorageInvariants(client.Client, store.codec))
}

// TestGuaranteedUpdateWithTTL 验证更新时 TTL 的变更（设置/清除）语义。
func TestGuaranteedUpdateWithTTL(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestGuaranteedUpdateWithTTL(ctx, t, store)
}

// TestGuaranteedUpdateChecksStoredData 验证更新不会覆盖「读取后又被外部修改」的数据，
// 而是基于最新存储内容重新应用变更。
func TestGuaranteedUpdateChecksStoredData(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestGuaranteedUpdateChecksStoredData(ctx, t, &storeWithPrefixTransformer{store})
}

// TestGuaranteedUpdateWithConflict 验证更新遇到 RV 冲突时自动重试并最终成功。
func TestGuaranteedUpdateWithConflict(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestGuaranteedUpdateWithConflict(ctx, t, store)
}

// TestGuaranteedUpdateWithSuggestionAndConflict 验证带建议值的更新冲突处理。
func TestGuaranteedUpdateWithSuggestionAndConflict(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestGuaranteedUpdateWithSuggestionAndConflict(ctx, t, store)
}

// TestTransformationFailure 验证读取时变换失败会返回错误而不是返回损坏数据。
func TestTransformationFailure(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestTransformationFailure(ctx, t, &storeWithPrefixTransformer{store})
}

// TestList 在 EtcdRangeStream 开关两种取值下验证 LIST 契约。
// 开启时必须确实走 RangeStream，关闭时不得发起流式请求——
// 通过 KVRecorder 计数来断言，避免功能开关形同虚设。
func TestList(t *testing.T) {
	for _, rangeStream := range []bool{false, true} {
		t.Run(fmt.Sprintf("rangeStream=%v", rangeStream), func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.EtcdRangeStream, rangeStream)
			resetFeatureSupportCheckerDuringTest(t)
			ctx, store, client := testSetup(t)
			kvRecorder := client.KV.(*storagetesting.KVRecorder)
			storagetesting.RunTestList(ctx, t, store, compactStorage(store, client.Client), false, client.Kubernetes.(*storagetesting.KubernetesRecorder))
			streamReads := kvRecorder.GetStreamReadsAndReset()
			if rangeStream && streamReads == 0 {
				t.Error("expected lists to be served by RangeStream")
			}
			if !rangeStream && streamReads > 0 {
				t.Errorf("expected no RangeStream requests with the feature disabled, got %d", streamReads)
			}
		})
	}
}

// TestGetListStreamFallsBackToPaginated 验证对不支持 RangeStream 的服务端发起
// GetList 时，该特性会被标记为不支持并回退到分页路径。
// Unimplemented 是「服务端不支持」的确定信号，因此必须记住并停止重试，
// 否则每个 LIST 都会先失败一次再回退，造成持续的性能损失。
func TestGetListStreamFallsBackToPaginated(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.EtcdRangeStream, true)
	resetFeatureSupportCheckerDuringTest(t)

	ctx, store, _ := testSetup(t)
	initList, err := initStoreData(ctx, store)
	if err != nil {
		t.Fatal(err)
	}

	kvWrapper := newEtcdClientKVWrapper(store.client.KV)
	kvWrapper.streamKV = unimplementedRangeStreamKV()
	store.client.KV = kvWrapper

	metrics.Register()
	legacyregistry.Reset()
	t.Cleanup(legacyregistry.Reset)

	out := &example.PodList{}
	if err := store.GetList(ctx, "/pods/", storage.ListOptions{Predicate: storage.Everything, Recursive: true}, out); err != nil {
		t.Fatalf("GetList failed: %v", err)
	}

	if kvWrapper.getStreamCallCounter != 1 {
		t.Errorf("expected GetStream to be called once, got %d", kvWrapper.getStreamCallCounter)
	}
	// Unimplemented 探测不计入指标，只记录实际服务 list 的一元请求。
	expected := `# HELP etcd_requests_total [ALPHA] Etcd request counts for each operation and object type.
# TYPE etcd_requests_total counter
etcd_requests_total{group="",operation="list",resource="pods"} 1
`
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expected), "etcd_requests_total", "etcd_request_errors_total"); err != nil {
		t.Error(err)
	}
	if kvWrapper.getCallCounter == 0 {
		t.Error("expected the paginated Get path to serve the list after fallback")
	}
	if etcdfeature.DefaultFeatureSupportChecker.Supports(storage.RangeStream) {
		t.Error("expected RangeStream to be marked unsupported after the Unimplemented fallback")
	}
	if len(out.Items) != len(initList) {
		t.Errorf("returned %d items, want %d", len(out.Items), len(initList))
	}

	// 该特性已被标记为不支持，后续 list 不应再尝试流式接口。
	if err := store.GetList(ctx, "/pods/", storage.ListOptions{Predicate: storage.Everything, Recursive: true}, &example.PodList{}); err != nil {
		t.Fatalf("GetList failed: %v", err)
	}
	if kvWrapper.getStreamCallCounter != 1 {
		t.Errorf("expected no further GetStream calls after fallback, got %d", kvWrapper.getStreamCallCounter)
	}
}

// TestGetListStreamMultiChunk 验证跨多个 RangeStream 分块（etcd 服务端按每 10 个键
// 分块）的列表上流式请求的形态。
// 同时验证 Exact（固定 RV，看不到之后写入）与 NotOlderThan（可见最新写入）
// 两种语义，以及流式列举不应产生分页字段。
func TestGetListStreamMultiChunk(t *testing.T) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.EtcdRangeStream, true)
	resetFeatureSupportCheckerDuringTest(t)
	ctx, store, client := testSetup(t)
	kvRecorder := client.KV.(*storagetesting.KVRecorder)

	const podCount = 25
	for i := range podCount {
		pod := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: fmt.Sprintf("pod-%02d", i)}}
		if err := store.Create(ctx, fmt.Sprintf("/pods/ns/pod-%02d", i), pod, &example.Pod{}, 0); err != nil {
			t.Fatal(err)
		}
	}

	streamedList := func(t *testing.T, rv string, rvMatch metav1.ResourceVersionMatch) *example.PodList {
		t.Helper()
		out := &example.PodList{}
		opts := storage.ListOptions{ResourceVersion: rv, ResourceVersionMatch: rvMatch, Predicate: storage.Everything, Recursive: true}
		if err := store.GetList(ctx, "/pods/", opts, out); err != nil {
			t.Fatalf("GetList failed: %v", err)
		}
		if streams := kvRecorder.GetStreamReadsAndReset(); streams == 0 {
			t.Error("expected the list to be served by RangeStream")
		}
		if out.Continue != "" || out.RemainingItemCount != nil {
			t.Errorf("streamed list must not paginate, got continue %q, remainingItemCount %v", out.Continue, out.RemainingItemCount)
		}
		return out
	}

	snapshot := streamedList(t, "", "")
	if len(snapshot.Items) != podCount {
		t.Errorf("consistent list returned %d items, want %d", len(snapshot.Items), podCount)
	}

	extraPod := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pod-extra"}}
	if err := store.Create(ctx, "/pods/ns/pod-extra", extraPod, &example.Pod{}, 0); err != nil {
		t.Fatal(err)
	}

	// 精确匹配的流返回指定 revision 的快照，不含之后的写入。
	exact := streamedList(t, snapshot.ResourceVersion, metav1.ResourceVersionMatchExact)
	if len(exact.Items) != podCount {
		t.Errorf("exact list returned %d items, want %d", len(exact.Items), podCount)
	}
	if exact.ResourceVersion != snapshot.ResourceVersion {
		t.Errorf("exact list returned resourceVersion %s, want %s", exact.ResourceVersion, snapshot.ResourceVersion)
	}

	// 只给出最小 revision 的流返回最新数据。
	notOlder := streamedList(t, snapshot.ResourceVersion, "")
	if len(notOlder.Items) != podCount+1 {
		t.Errorf("notOlderThan list returned %d items, want %d", len(notOlder.Items), podCount+1)
	}
}

// TestListMetrics 在流式开关两种取值下验证 LIST 指标口径一致。
func TestListMetrics(t *testing.T) {
	for _, rangeStream := range []bool{false, true} {
		t.Run(fmt.Sprintf("rangeStream=%v", rangeStream), func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.EtcdRangeStream, rangeStream)
			resetFeatureSupportCheckerDuringTest(t)
			testListMetrics(t)
		})
	}
}

// testListMetrics 断言一次 LIST 后四个存储指标的完整输出。
func testListMetrics(t *testing.T) {
	ctx, store, _ := testSetup(t)

	storagemetrics.Register()
	legacyregistry.Reset()
	t.Cleanup(legacyregistry.Reset)

	pod := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "ns"}}
	if err := store.Create(ctx, computePodKey(pod), pod, &example.Pod{}, 0); err != nil {
		t.Fatal(err)
	}

	result := &example.PodList{}
	if err := store.GetList(ctx, "/pods/ns", storage.ListOptions{Predicate: storage.Everything, Recursive: true}, result); err != nil {
		t.Fatal(err)
	}

	expected := `# HELP apiserver_storage_list_evaluated_objects_total [ALPHA] Number of objects tested in the course of serving a LIST request from storage
# TYPE apiserver_storage_list_evaluated_objects_total counter
apiserver_storage_list_evaluated_objects_total{group="",resource="pods",storage="etcd"} 1
# HELP apiserver_storage_list_fetched_objects_total [ALPHA] Number of objects read from storage in the course of serving a LIST request
# TYPE apiserver_storage_list_fetched_objects_total counter
apiserver_storage_list_fetched_objects_total{group="",index="",resource="pods",storage="etcd"} 1
# HELP apiserver_storage_list_returned_objects_total [ALPHA] Number of objects returned for a LIST request from storage
# TYPE apiserver_storage_list_returned_objects_total counter
apiserver_storage_list_returned_objects_total{group="",resource="pods",storage="etcd"} 1
# HELP apiserver_storage_list_total [ALPHA] Number of LIST requests served from storage
# TYPE apiserver_storage_list_total counter
apiserver_storage_list_total{group="",index="",resource="pods",storage="etcd"} 1
`
	if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(expected),
		"apiserver_storage_list_total",
		"apiserver_storage_list_fetched_objects_total",
		"apiserver_storage_list_evaluated_objects_total",
		"apiserver_storage_list_returned_objects_total",
	); err != nil {
		t.Fatal(err)
	}
}

// TestConsistentList 验证一致性列举（resourceVersion=0 的 quorum 读）语义。
func TestConsistentList(t *testing.T) {
	for _, rangeStream := range []bool{false, true} {
		t.Run(fmt.Sprintf("rangeStream=%v", rangeStream), func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.EtcdRangeStream, rangeStream)
			resetFeatureSupportCheckerDuringTest(t)
			ctx, store, client := testSetup(t)
			storagetesting.RunTestConsistentList(ctx, t, store, increaseRVFunc(client.Client), false, true, false)
		})
	}
}

// TestCompactRevision 验证 store 能观察到 etcd 的压缩版本变化。
// 依赖 ListFromCacheSnapshot 开启的专用 watch——压缩事件不会通过
// 普通资源 watch 传递，必须单独监听 compact key。
func TestCompactRevision(t *testing.T) {
	// 本测试要求 store 能观察到外部对压缩版本的修改，因此需要由 ListFromCacheSnapshot 开启的 compact key 专用 watch。
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ListFromCacheSnapshot, true)
	ctx, store, client := testSetup(t)
	storagetesting.RunTestCompactRevision(ctx, t, store, increaseRVFunc(client.Client), compactStorage(store, client.Client))
}

// checkStorageCallsInvariants 校验分页列举的实际 etcd 调用次数与变换读取次数。
// apiserver 在收到非满页时会指数级放大 pageSize，因此调用次数应远小于
// 「每页固定 1000」的朴素实现；这里按 1+2+4+... 的几何级数推算期望值。
func checkStorageCallsInvariants(transformer *storagetesting.PrefixTransformer, recorder *storagetesting.KVRecorder) storagetesting.CallsValidation {
	return func(t *testing.T, pageSize, estimatedProcessedObjects uint64) {
		if reads := transformer.GetReadsAndReset(); reads != estimatedProcessedObjects {
			t.Errorf("unexpected reads: %d, expected: %d", reads, estimatedProcessedObjects)
		}
		estimatedGetCalls := uint64(1)
		if pageSize != 0 {
			// kube-apiserver 在页未满时会加大页大小，因此页数应远少于 1000
			//（后者相当于始终用 pred.Limit 作为页大小直接问 etcd）。
			// 预期调用次数为 n+1，其中 n 是满足
			// pageSize + pageSize*2 + ... + pageSize*2^n >= podCount 的最小值。
			// pageSize=1、podCount=1000 时 n+1=10，2^10=1024。
			currLimit := pageSize
			for sum := uint64(1); sum < estimatedProcessedObjects; {
				currLimit *= 2
				if currLimit > maxLimit {
					currLimit = maxLimit
				}
				sum += currLimit
				estimatedGetCalls++
			}
		}
		if reads := recorder.GetReadsAndReset() + recorder.GetStreamReadsAndReset(); reads != estimatedGetCalls {
			t.Fatalf("unexpected reads: %d, want: %d", reads, estimatedGetCalls)
		}
	}
}

// TestListContinuation 验证分页续读的完整流程与调用次数约束。
func TestListContinuation(t *testing.T) {
	ctx, store, client := testSetup(t)
	validation := checkStorageCallsInvariants(
		store.transformer.(*storagetesting.PrefixTransformer), client.KV.(*storagetesting.KVRecorder))
	storagetesting.RunTestListContinuation(ctx, t, store, validation)
}

// TestListPaginationRareObject 验证「稀有对象」场景下分页仍能覆盖全部数据。
func TestListPaginationRareObject(t *testing.T) {
	// ListFromCacheSnapshots 会额外发一次 Get 读取 compact key。
	// // TODO: 改写调用校验，只统计对 pods 的请求。
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ListFromCacheSnapshot, false)
	ctx, store, client := testSetup(t)
	validation := checkStorageCallsInvariants(
		store.transformer.(*storagetesting.PrefixTransformer), client.KV.(*storagetesting.KVRecorder))
	storagetesting.RunTestListPaginationRareObject(ctx, t, store, validation)
}

// TestListContinuationWithFilter 验证带字段/标签过滤的分页续读。
func TestListContinuationWithFilter(t *testing.T) {
	ctx, store, client := testSetup(t)
	validation := checkStorageCallsInvariants(
		store.transformer.(*storagetesting.PrefixTransformer), client.KV.(*storagetesting.KVRecorder))
	storagetesting.RunTestListContinuationWithFilter(ctx, t, store, validation)
}

// TestNamespaceScopedList 验证按命名空间前缀列举只返回该命名空间的对象。
func TestNamespaceScopedList(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestNamespaceScopedList(ctx, t, store)
}

// compactStorage 对给定 RV 执行 etcd 压缩，并在需要时等待 store 观察到压缩版本。
// 压缩可能需要重试（已压缩的 RV 会返回错误），因此第二次失败才视为失败；
// 开启 ListFromCacheSnapshot 时必须轮询 CompactRevision，否则后续断言会与压缩竞态。
func compactStorage(s *store, client *clientv3.Client) storagetesting.Compaction {
	return func(ctx context.Context, t *testing.T, resourceVersion string) {
		versioner := storage.APIObjectVersioner{}
		rv, err := versioner.ParseResourceVersion(resourceVersion)
		if err != nil {
			t.Fatal(err)
		}
		var currentVersion int64
		currentVersion, _, _, err = Compact(ctx, client, currentVersion, int64(rv))
		if err != nil {
			_, _, _, err = Compact(ctx, client, currentVersion, int64(rv))
		}
		if err != nil {
			t.Fatal(err)
		}
		// 等待压缩事件被观测到。
		if utilfeature.DefaultFeatureGate.Enabled(features.ListFromCacheSnapshot) {
			for {
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(100 * time.Millisecond):
				}
				compactedRev := s.CompactRevision()
				if compactedRev == int64(rv) {
					break
				}
			}
		}
	}
}

// increaseRVFunc 通过一次无关写入推进 etcd 全局 revision，并返回新值。
// 用于模拟「期间发生了其他写入」，测试 RV 的推进与快照边界。
func increaseRVFunc(client *clientv3.Client) storagetesting.IncreaseRVFunc {
	return func(ctx context.Context, t *testing.T) int64 {
		resp, err := client.KV.Put(ctx, "increaseRV", "ok")
		if err != nil {
			t.Fatalf("Could not update increaseRV: %v", err)
		}
		return resp.Header.Revision
	}
}

// TestListInconsistentContinuation 验证续读期间发生压缩时的错误（410 Gone）与提示。
func TestListInconsistentContinuation(t *testing.T) {
	ctx, store, client := testSetup(t)
	storagetesting.RunTestListInconsistentContinuation(ctx, t, store, compactStorage(store, client.Client))
}

// TestListResourceVersionMatch 验证 Exact 与 NotOlderThan 两种 RV 匹配语义。
func TestListResourceVersionMatch(t *testing.T) {
	ctx, store, _ := testSetup(t)
	storagetesting.RunTestListResourceVersionMatch(ctx, t, &storeWithPrefixTransformer{store})
}

// TestStats 在开启/关闭大小估算两种模式下验证 Stats 输出。
func TestStats(t *testing.T) {
	for _, sizeBasedListCostEstimate := range []bool{true, false} {
		t.Run(fmt.Sprintf("SizeBasedListCostEstimate=%v", sizeBasedListCostEstimate), func(t *testing.T) {
			ctx, store, _ := testSetup(t)
			if sizeBasedListCostEstimate {
				err := store.EnableResourceSizeEstimation(store.getKeys)
				if err != nil {
					t.Fatal(err)
				}
			}
			storagetesting.RunTestStats(ctx, t, store, store.codec, store.transformer, sizeBasedListCostEstimate)
		})
	}
}

// =======================================================================
// 以下是针对实现的测试：它们检验实现细节而非存储接口对外的契约，
// 因此可能关注性能影响等非功能性方面。
// =======================================================================

// TestPrefix 验证配置前缀会被规范化为「以 / 开头、以 / 结尾」的形式。
// 规范化保证键空间不会因多余斜杠或缺少前导斜杠而与相邻前缀混淆。
func TestPrefix(t *testing.T) {
	testcases := map[string]string{
		"custom/prefix":     "/custom/prefix/",
		"/custom//prefix//": "/custom/prefix/",
		"/registry":         "/registry/",
	}
	for configuredPrefix, effectivePrefix := range testcases {
		_, store, _ := testSetup(t, withPrefix(configuredPrefix))
		if store.pathPrefix != effectivePrefix {
			t.Errorf("configured prefix of %s, expected effective prefix of %s, got %s", configuredPrefix, effectivePrefix, store.pathPrefix)
		}
	}
}

// Test_growSlice 验证按对象大小预估扩容切片时，容量与长度不被破坏。
// growSlice 在反序列化 LIST 结果前预分配，既能避免多次扩容，
// 又必须遵守 maxCapacity 上限，防止恶意/异常大小导致内存暴涨。
func Test_growSlice(t *testing.T) {
	type args struct {
		initialCapacity int
		initialLen      int
		v               reflect.Value
		maxCapacity     int
		sizes           []int
	}
	tests := []struct {
		name string
		args args
		cap  int
		len  int
	}{
		{
			name: "empty",
			args: args{v: reflect.ValueOf([]example.Pod{})},
			cap:  0,
		},
		{
			name: "no sizes",
			args: args{v: reflect.ValueOf([]example.Pod{}), maxCapacity: 10},
			cap:  10,
		},
		{
			name: "above maxCapacity",
			args: args{v: reflect.ValueOf([]example.Pod{}), maxCapacity: 10, sizes: []int{1, 12}},
			cap:  10,
		},
		{
			name: "takes max",
			args: args{v: reflect.ValueOf([]example.Pod{}), maxCapacity: 10, sizes: []int{8, 4}},
			cap:  8,
		},
		{
			name: "with existing capacity above max",
			args: args{initialCapacity: 12, maxCapacity: 10, sizes: []int{8, 4}},
			cap:  12,
		},
		{
			name: "with existing capacity below max",
			args: args{initialCapacity: 5, maxCapacity: 10, sizes: []int{8, 4}},
			cap:  8,
		},
		{
			name: "with existing capacity and length above max",
			args: args{initialCapacity: 12, initialLen: 5, maxCapacity: 10, sizes: []int{8, 4}},
			cap:  12,
			len:  5,
		},
		{
			name: "with existing capacity and length below max",
			args: args{initialCapacity: 5, initialLen: 3, maxCapacity: 10, sizes: []int{8, 4}},
			cap:  8,
			len:  3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.args.initialCapacity > 0 {
				val := make([]example.Pod, tt.args.initialLen, tt.args.initialCapacity)
				for i := 0; i < tt.args.initialLen; i++ {
					val[i].Name = fmt.Sprintf("test-%d", i)
				}
				tt.args.v = reflect.ValueOf(val)
			}
			// 反射调用 Set 要求值可取地址，因此要确保新值分配在堆上（正常使用中不成问题）。
			if !tt.args.v.CanAddr() {
				x := reflect.New(tt.args.v.Type())
				x.Elem().Set(tt.args.v)
				tt.args.v = x.Elem()
			}
			growSlice(tt.args.v, tt.args.maxCapacity, tt.args.sizes...)
			if tt.cap != tt.args.v.Cap() {
				t.Errorf("Unexpected capacity: got=%d want=%d", tt.args.v.Cap(), tt.cap)
			}
			if tt.len != tt.args.v.Len() {
				t.Errorf("Unexpected length: got=%d want=%d", tt.args.v.Len(), tt.len)
			}
			for i := 0; i < tt.args.v.Len(); i++ {
				nameWanted := fmt.Sprintf("test-%d", i)
				val := tt.args.v.Index(i).Interface()
				pod, ok := val.(example.Pod)
				if !ok || pod.Name != nameWanted {
					t.Errorf("Unexpected element value: got=%s, want=%s", pod.Name, nameWanted)
				}
			}
		})
	}
}

// TestLeaseMaxObjectCount 验证单个 lease 挂载对象数达到上限后会换用新 lease。
// 限制单 lease 对象数可避免 lease 撤销时一次性删除过多键；
// 新 lease 的计数应从 1 重新开始。
func TestLeaseMaxObjectCount(t *testing.T) {
	ctx, store, _ := testSetup(t, withLeaseConfig(LeaseManagerConfig{
		ReuseDurationSeconds: defaultLeaseReuseDurationSeconds,
		MaxObjectCount:       2,
	}))

	obj := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo"}}
	out := &example.Pod{}

	testCases := []struct {
		key                 string
		expectAttachedCount int64
	}{
		{
			key:                 "/pods/testkey1",
			expectAttachedCount: 1,
		},
		{
			key:                 "/pods/testkey2",
			expectAttachedCount: 2,
		},
		{
			key: "/pods/testkey3",
			// 假设每个 lease 挂 1 个对象，因此新授予的 lease 对应计数为 1。
			expectAttachedCount: 1,
		},
	}

	for _, tc := range testCases {
		err := store.Create(ctx, tc.key, obj, out, 120)
		if err != nil {
			t.Fatalf("Set failed: %v", err)
		}
		if store.leaseManager.leaseAttachedObjectCount != tc.expectAttachedCount {
			t.Errorf("Lease manager recorded count %v should be %v", store.leaseManager.leaseAttachedObjectCount, tc.expectAttachedCount)
		}
	}
}

// ===================================================
// 以下是测试辅助函数。
// ===================================================

// newTestLeaseManagerConfig 把 lease 复用窗口缩短到 1 秒，适配测试的全局超时。
func newTestLeaseManagerConfig() LeaseManagerConfig {
	cfg := NewDefaultLeaseManagerConfig()
	// 全局配置的默认测试超时是 30s，单次等待不能超过它；
	// 因此这里缩短到 1s。参见 wait.ForeverTestTimeout。
	cfg.ReuseDurationSeconds = 1
	return cfg
}

// newTestTransformer 返回带固定测试前缀的转换器，便于断言落盘字节。
func newTestTransformer() value.Transformer {
	return storagetesting.NewPrefixTransformer([]byte(defaultTestPrefix), false)
}

// setupOptions 收集 testSetup 的可覆盖参数。
type setupOptions struct {
	client         func(testing.TB) *kubernetes.Client
	codec          runtime.Codec
	newFunc        func() runtime.Object
	newListFunc    func() runtime.Object
	prefix         string
	resourcePrefix string
	groupResource  schema.GroupResource
	transformer    value.Transformer
	leaseConfig    LeaseManagerConfig
}

// setupOption 是 testSetup 的函数式选项。
type setupOption func(*setupOptions)

// withClientConfig 用指定 etcd 配置启动测试实例。
func withClientConfig(config *embed.Config) setupOption {
	return func(options *setupOptions) {
		options.client = func(t testing.TB) *kubernetes.Client {
			return testserver.RunEtcd(t, config)
		}
	}
}

// withPrefix 覆盖存储层全局前缀。
func withPrefix(prefix string) setupOption {
	return func(options *setupOptions) {
		options.prefix = prefix
	}
}

// withResourcePrefix 覆盖资源级前缀（如 /pods/）。
func withResourcePrefix(prefix string) setupOption {
	return func(options *setupOptions) {
		options.resourcePrefix = prefix
	}
}

// withLeaseConfig 覆盖 lease 管理器配置。
func withLeaseConfig(leaseConfig LeaseManagerConfig) setupOption {
	return func(options *setupOptions) {
		options.leaseConfig = leaseConfig
	}
}

// withTransformer 覆盖存储使用的值转换器。
func withTransformer(transformer value.Transformer) setupOption {
	return func(options *setupOptions) {
		options.transformer = transformer
	}
}

// withCodec 覆盖存储使用的编解码器。
func withCodec(codec runtime.Codec) setupOption {
	return func(options *setupOptions) {
		options.codec = codec
	}
}

// withDefaults 填入一组可直接使用的默认配置，作为选项链的起点。
func withDefaults(options *setupOptions) {
	options.client = func(t testing.TB) *kubernetes.Client {
		return testserver.RunEtcd(t, nil)
	}
	options.codec = apitesting.TestCodec(codecs, examplev1.SchemeGroupVersion)
	options.newFunc = newPod
	options.newListFunc = newPodList
	options.prefix = ""
	options.resourcePrefix = "/pods/"
	options.groupResource = schema.GroupResource{Resource: "pods"}
	options.transformer = newTestTransformer()
	options.leaseConfig = newTestLeaseManagerConfig()
}

var _ setupOption = withDefaults

// testSetup 启动嵌入式 etcd 并构造被测 store，注册清理以终止实例与 compactor。
// withDefaults 被放在选项链最前，使调用方提供的选项能覆盖默认值。
func testSetup(t testing.TB, opts ...setupOption) (context.Context, *store, *kubernetes.Client) {
	setupOpts := setupOptions{}
	opts = append([]setupOption{withDefaults}, opts...)
	for _, opt := range opts {
		opt(&setupOpts)
	}
	client := setupOpts.client(t)
	versioner := storage.APIObjectVersioner{}
	compactor := NewCompactor(client.Client, 0, clock.RealClock{}, nil)
	t.Cleanup(compactor.Stop)
	store, err := New(
		client,
		compactor,
		setupOpts.codec,
		setupOpts.newFunc,
		setupOpts.newListFunc,
		setupOpts.prefix,
		setupOpts.resourcePrefix,
		setupOpts.groupResource,
		setupOpts.transformer,
		setupOpts.leaseConfig,
		NewDefaultDecoder(setupOpts.codec, versioner),
		versioner,
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	ctx := context.Background()
	return ctx, store, client
}

// TestPrepareKey 验证键规范化：补全全局前缀、按递归与否追加结尾斜杠，
// 并拒绝包含 "." 或 ".." 路径段的键。
// 路径段校验是安全边界——".." 可让键逃逸出预期前缀，
// 从而读写其他资源甚至其他组件的数据。
func TestPrepareKey(t *testing.T) {
	validKeys := []string{
		"/pods/foo/bar/baz/a.b.c/",
		"/pods/foo",
		"/pods/foo/bar/baz",
		"/pods/foo/bar..baz/",
		"/pods/foo/bar..",
		"/pods/foo",
		"/pods/foo/bar",
		"/pods/foo/bar/",
		"/pods/",
	}
	invalidKeys := []string{
		"/pods",
		"/pods/foo/bar/../a.b.c/",
		"/pods/..",
		"/pods/../",
		"/pods/foo/bar/..",
		"/pods/../foo/bar",
		"/pods/../foo",
		"/pods/foo/bar/../",
		"/pods/.",
		"/pods/./",
		"/pods/foo/.",
		"/pods/./bar",
		"/pods/foo/./bar/",
		"/foo/bar/../a.b.c/",
		"..",
		"/..",
		"../",
		"/foo/bar/..",
		"../foo/bar",
		"/../foo",
		"/foo/bar/../",
		".",
		"/.",
		"./",
		"/./",
		"/foo/.",
		"./bar",
		"/foo/./bar/",
	}
	const (
		pathPrefix   = "/first/second"
		expectPrefix = pathPrefix + "/"
	)
	_, store, _ := testSetup(t, withPrefix(pathPrefix))

	t.Run("Non-recursive", func(t *testing.T) {
		for _, key := range validKeys {
			preparedKey, err := store.prepareKey(key, false)
			if err != nil {
				t.Errorf("preparing key %q should be valid; unexpected error: %v", key, err)
				continue
			}
			if !strings.HasPrefix(preparedKey, expectPrefix) {
				t.Errorf("prepared key %q should have prefix %q", preparedKey, expectPrefix)
			}
			if !strings.HasSuffix(preparedKey, key) {
				t.Errorf("Prepared non-recursive key %q should have suffix %q", preparedKey, key)
			}
		}
	})

	t.Run("Recursive", func(t *testing.T) {
		for _, key := range validKeys {
			preparedKey, err := store.prepareKey(key, true)
			if err != nil {
				t.Errorf("preparing key %q should be valid; unexpected error: %v", key, err)
				continue
			}
			if !strings.HasPrefix(preparedKey, expectPrefix) {
				t.Errorf("prepared key %q should have prefix %q", preparedKey, expectPrefix)
			}
			if !strings.HasSuffix(preparedKey, "/") {
				t.Errorf("Prepared non-recursive key %q should have suffix '/'", preparedKey)
			}
		}
	})

	t.Run("Invalid", func(t *testing.T) {
		for _, key := range invalidKeys {
			_, err := store.prepareKey(key, false)
			if err == nil {
				t.Errorf("key %q should be invalid", key)
			}
		}
	})
}

// TestInvalidKeys 验证所有公开存储方法对非法键统一返回 "invalid key" 错误，
// 避免某些入口绕过校验。
func TestInvalidKeys(t *testing.T) {
	const invalidKey = "/foo/bar/../baz"
	expectedError := fmt.Sprintf("invalid key: %q", invalidKey)

	expectInvalidKey := func(methodName string, err error) {
		if err == nil {
			t.Errorf("[%s] expected invalid key error; got nil", methodName)
		} else if err.Error() != expectedError {
			t.Errorf("[%s] expected invalid key error; got %v", methodName, err)
		}
	}

	ctx, store, _ := testSetup(t)
	expectInvalidKey("Create", store.Create(ctx, invalidKey, nil, nil, 0))
	expectInvalidKey("Delete", store.Delete(ctx, invalidKey, nil, nil, nil, nil, storage.DeleteOptions{}))
	_, watchErr := store.Watch(ctx, invalidKey, storage.ListOptions{})
	expectInvalidKey("Watch", watchErr)
	expectInvalidKey("Get", store.Get(ctx, invalidKey, storage.GetOptions{}, nil))
	expectInvalidKey("GetList", store.GetList(ctx, invalidKey, storage.ListOptions{}, nil))
	expectInvalidKey("GuaranteedUpdate", store.GuaranteedUpdate(ctx, invalidKey, nil, true, nil, nil, nil))
}

// BenchmarkStore_GetList 测量大对象场景下 LIST 的吞吐，覆盖不同对象大小与选择器。
func BenchmarkStore_GetList(b *testing.B) {
	generateBigPod := func(index int, total int, expect int) runtime.Object {
		l := map[string]string{}
		if index%(total/expect) == 0 {
			l["foo"] = "bar"
		}
		terminationGracePeriodSeconds := int64(42)
		activeDeadlineSeconds := int64(42)
		pod := &examplev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Labels: l,
			},
			Spec: examplev1.PodSpec{
				RestartPolicy:                 examplev1.RestartPolicy("Always"),
				TerminationGracePeriodSeconds: &terminationGracePeriodSeconds,
				ActiveDeadlineSeconds:         &activeDeadlineSeconds,
				NodeSelector:                  map[string]string{},
				ServiceAccountName:            "demo-sa",
			},
		}
		pod.Name = fmt.Sprintf("object-%d", index)
		data := make([]byte, 1024*2, 1024*2) // 2k labels
		rand.Read(data)
		pod.Spec.NodeSelector["key"] = string(data)
		return pod
	}
	testCases := []struct {
		name              string
		objectNum         int
		expectNum         int
		selector          labels.Selector
		newObjectFunc     func(index int, total int, expect int) runtime.Object
		newListObjectFunc func() runtime.Object
	}{
		{
			name:              "pick 50 pods out of 5000 pod",
			objectNum:         5000,
			expectNum:         50,
			selector:          labels.SelectorFromSet(map[string]string{"foo": "bar"}),
			newObjectFunc:     generateBigPod,
			newListObjectFunc: func() runtime.Object { return &examplev1.PodList{} },
		},
		{
			name:              "pick 500 pods out of 5000 pod",
			objectNum:         5000,
			expectNum:         500,
			selector:          labels.SelectorFromSet(map[string]string{"foo": "bar"}),
			newObjectFunc:     generateBigPod,
			newListObjectFunc: func() runtime.Object { return &examplev1.PodList{} },
		},
		{
			name:              "pick 1000 pods out of 5000 pod",
			objectNum:         5000,
			expectNum:         1000,
			selector:          labels.SelectorFromSet(map[string]string{"foo": "bar"}),
			newObjectFunc:     generateBigPod,
			newListObjectFunc: func() runtime.Object { return &examplev1.PodList{} },
		},
		{
			name:              "pick 2500 pods out of 5000 pod",
			objectNum:         5000,
			expectNum:         2500,
			selector:          labels.SelectorFromSet(map[string]string{"foo": "bar"}),
			newObjectFunc:     generateBigPod,
			newListObjectFunc: func() runtime.Object { return &examplev1.PodList{} },
		},
		{
			name:              "pick 5000 pods out of 5000 pod",
			objectNum:         5000,
			expectNum:         5000,
			selector:          labels.SelectorFromSet(map[string]string{"foo": "bar"}),
			newObjectFunc:     generateBigPod,
			newListObjectFunc: func() runtime.Object { return &examplev1.PodList{} },
		},
	}
	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// booting etcd instance
			ctx, store, etcdClient := testSetup(b)
			defer etcdClient.Close()

			// 构造假对象。
			dir := "/testing"
			originalRevision := ""
			for i := 0; i < tc.objectNum; i++ {
				obj := tc.newObjectFunc(i, tc.objectNum, tc.expectNum)
				o := obj.(metav1.Object)
				key := fmt.Sprintf("/testing/testkey/%s", o.GetName())
				out := tc.newObjectFunc(i, tc.objectNum, tc.expectNum)
				if err := store.Create(ctx, key, obj, out, 0); err != nil {
					b.Fatalf("Set failed: %v", err)
				}
				originalRevision = out.(metav1.Object).GetResourceVersion()
			}

			// 准备结果与选择器
			pred := storage.SelectionPredicate{
				Label: tc.selector,
				Field: fields.Everything(),
				GetAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
					pod, ok := obj.(*examplev1.Pod)
					if !ok {
						return nil, nil, fmt.Errorf("not a pod")
					}
					return pod.ObjectMeta.Labels, fields.Set{
						"metadata.name": pod.Name,
					}, nil
				},
			}

			// now we start benchmarking
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				list := tc.newListObjectFunc()
				if err := store.GetList(ctx, dir, storage.ListOptions{Predicate: pred, Recursive: true}, list); err != nil {
					b.Errorf("Unexpected List error: %v", err)
				}
				listObject := list.(*examplev1.PodList)
				if originalRevision != listObject.GetResourceVersion() {
					b.Fatalf("original revision (%s) did not match final revision after linearized reads (%s)", originalRevision, listObject.GetResourceVersion())
				}
				if len(listObject.Items) != tc.expectNum {
					b.Fatalf("expect (%d) items but got (%d)", tc.expectNum, len(listObject.Items))
				}
			}
		})
	}
}

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
			ctx, store, _ := testSetup(b)
			data := storagetesting.PrepareBenchmarkData(dims.namespaceCount, dims.podPerNamespaceCount, dims.nodeCount)
			b.ResetTimer()
			storagetesting.RunBenchmarkWriteThroughput(ctx, b, store, data, false, nil)
		})
	}
}

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
		for _, sizeBasedEnabled := range []bool{true, false} {
			featuregatetesting.SetFeatureGateDuringTest(b, utilfeature.DefaultFeatureGate, features.SizeBasedListCostEstimate, sizeBasedEnabled)
			b.Run(fmt.Sprintf("SizeBasedListCostEstimate=%v/Namespaces=%d/Pods=%d/Nodes=%d", sizeBasedEnabled, dims.namespaceCount, dims.namespaceCount*dims.podPerNamespaceCount, dims.nodeCount), func(b *testing.B) {
				data := storagetesting.PrepareBenchmarkData(dims.namespaceCount, dims.podPerNamespaceCount, dims.nodeCount)
				ctx, store, _ := testSetup(b)
				require.NoError(b, storagetesting.PrecreateBenchmarkPods(ctx, store, data))
				storagetesting.RunBenchmarkStoreList(ctx, b, store, data, false)
			})
		}
	}
}

// computePodKey 按 /pods/<namespace>/<name> 规则生成测试键。
func computePodKey(obj *example.Pod) string {
	return fmt.Sprintf("/pods/%s/%s", obj.Namespace, obj.Name)
}

// TestGetCurrentResourceVersion 验证返回的是 etcd 全局 revision，
// 且无关写入会推进该值但不改变已有对象的 RV。
func TestGetCurrentResourceVersion(t *testing.T) {
	ctx, store, _ := testSetup(t)

	makePod := func(name string) *example.Pod {
		return &example.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name},
		}
	}
	createPod := func(obj *example.Pod) *example.Pod {
		key := "/pods/" + obj.Namespace + "/" + obj.Name
		out := &example.Pod{}
		err := store.Create(context.TODO(), key, obj, out, 0)
		require.NoError(t, err)
		return out
	}
	getPod := func(name, ns string) *example.Pod {
		key := "/pods/" + ns + "/" + name
		out := &example.Pod{}
		err := store.Get(context.TODO(), key, storage.GetOptions{}, out)
		require.NoError(t, err)
		return out
	}

	// 创建 Pod，确认其 RV 与 etcd 中的全局 RV 一致。
	pod := createPod(makePod("pod-1"))
	currentStorageRV, err := store.GetCurrentResourceVersion(context.TODO())
	require.NoError(t, err)
	podRV, err := store.versioner.ParseResourceVersion(pod.ResourceVersion)
	require.NoError(t, err)
	require.Equal(t, currentStorageRV, podRV, "expected the global etcd RV to be equal to pod's RV")

	// 再做一次无关写入，确认被测函数返回的是 etcd 的全局 RV。
	resp, err := store.client.KV.Put(ctx, "compact_rev_key", pod.ResourceVersion)
	require.NoError(t, err)
	currentStorageRV, err = store.GetCurrentResourceVersion(context.TODO())
	require.NoError(t, err)
	require.NoError(t, err)
	require.Equal(t, currentStorageRV, uint64(resp.Header.Revision), "expected the global etcd RV to be equal to replicaset's RV")

	// 确认 Pod 的 RV 未被改动。
	currentPod := getPod(pod.Name, pod.Namespace)
	currentPodRV, err := store.versioner.ParseResourceVersion(currentPod.ResourceVersion)
	require.NoError(t, err)
	require.Equal(t, currentPodRV, podRV, "didn't expect to see the pod's RV changed")
}

// BenchmarkStoreStats 测量 Stats 调用的开销，数据集模拟多个命名空间下的海量对象。
func BenchmarkStoreStats(b *testing.B) {
	klog.SetLogger(logr.Discard())
	data := storagetesting.PrepareBenchmarkData(50, 3_000, 5_000)
	ctx, store, _ := testSetup(b)
	require.NoError(b, storagetesting.PrecreateBenchmarkPods(ctx, store, data))
	storagetesting.RunBenchmarkStoreStats(ctx, b, store)
}

// BenchmarkStatsCacheCleanKeys 测量 cleanupKeys 的耗时：它会阻塞 watch 更新，直接影响 watch 延迟。
func BenchmarkStatsCacheCleanKeys(b *testing.B) {
	klog.SetLogger(logr.Discard())
	namespaceCount := 50
	podPerNamespaceCount := 3_000
	data := storagetesting.PrepareBenchmarkData(namespaceCount, podPerNamespaceCount, 5_000)
	ctx, store, _ := testSetup(b)
	require.NoError(b, storagetesting.PrecreateBenchmarkPods(ctx, store, data))
	// 先 list 一次，让 statsCache 收集对象大小。
	listOut := &example.PodList{}
	err := store.GetList(ctx, "/pods/", storage.ListOptions{Recursive: true, Predicate: storage.Everything}, listOut)
	if err != nil {
		b.Fatal(err)
	}
	if len(store.resourceSizeEstimator.keys) < namespaceCount*podPerNamespaceCount {
		b.Fatalf("Unexpected number of keys in stats, want: %d, got: %d", namespaceCount*podPerNamespaceCount, len(store.resourceSizeEstimator.keys))
	}
	// 取出 key 列表，确保计时只覆盖 cleanupKeys。
	keys, err := store.getKeys(ctx)
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		store.resourceSizeEstimator.cleanKeys(keys)
	}
	if len(store.resourceSizeEstimator.keys) < namespaceCount*podPerNamespaceCount {
		b.Fatalf("Unexpected number of keys in stats, want: %d, got: %d", namespaceCount*podPerNamespaceCount, len(store.resourceSizeEstimator.keys))
	}
}

// TestPrefixGetKeys 验证 getKeys 只返回资源前缀下的键，
// 不会误包含同名前缀（如 /registry/podskey）或全局前缀下的其他键。
func TestPrefixGetKeys(t *testing.T) {
	ctx, store, c := testSetup(t, withPrefix("/registry"), withResourcePrefix("/pods"))
	_, err := c.KV.Put(ctx, "key", "a")
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.KV.Put(ctx, "/registry/key", "b")
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.KV.Put(ctx, "/registry/pods/key", "c")
	if err != nil {
		t.Fatal(err)
	}

	_, err = c.KV.Put(ctx, "/registry/podskey", "d")
	if err != nil {
		t.Fatal(err)
	}

	gotKeys, err := store.getKeys(ctx)
	if err != nil {
		t.Fatal(err)
	}

	wantKeys := []string{"/registry/pods/key"}
	if diff := cmp.Diff(wantKeys, gotKeys); diff != "" {
		t.Errorf("getKeys diff:\n%s", diff)
	}
}

// TestPrefixStats 验证 Stats 在关闭/开启大小估算时的差异：
// 开启后应报告资源前缀下对象的平均大小（此处为 3 字节）。
func TestPrefixStats(t *testing.T) {
	tcs := []struct {
		name        string
		estimate    bool
		expectStats storage.Stats
	}{
		{
			name:        "Estimate=false",
			estimate:    false,
			expectStats: storage.Stats{ObjectCount: 1},
		},
		{
			name:        "Estimate=true",
			estimate:    true,
			expectStats: storage.Stats{ObjectCount: 1, EstimatedAverageObjectSizeBytes: 3},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctx, store, c := testSetup(t, withPrefix("/registry"), withResourcePrefix("/pods/"))
			if tc.estimate {
				err := store.EnableResourceSizeEstimation(store.getKeys)
				if err != nil {
					t.Fatal(err)
				}
			}
			_, err := c.KV.Put(ctx, "key", "a")
			if err != nil {
				t.Fatal(err)
			}

			_, err = c.KV.Put(ctx, "/registry/key", "ab")
			if err != nil {
				t.Fatal(err)
			}

			_, err = c.KV.Put(ctx, "/registry/pods/key", "abc")
			if err != nil {
				t.Fatal(err)
			}

			_, err = c.KV.Put(ctx, "/registry/podskey", "abcd")
			if err != nil {
				t.Fatal(err)
			}

			listOut := &example.PodList{}
			// 解码预期会失败，忽略错误。
			_ = store.GetList(ctx, "/pods/", storage.ListOptions{Predicate: storage.Everything, Recursive: true}, listOut)

			gotStats, err := store.Stats(ctx)
			if err != nil {
				t.Fatal(err)
			}

			if diff := cmp.Diff(tc.expectStats, gotStats); diff != "" {
				t.Errorf("Stats diff:\n%s", diff)
			}

		})
	}
}
