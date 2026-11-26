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

package cacher

import (
	"context"
	"fmt"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/runtime/serializer/protobuf"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/apis/example"
	examplev1 "k8s.io/apiserver/pkg/apis/example/v1"
	example2v1 "k8s.io/apiserver/pkg/apis/example2/v1"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/etcd3"
	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	"k8s.io/apiserver/pkg/storage/etcd3/testserver"
	storagetesting "k8s.io/apiserver/pkg/storage/testing"
	"k8s.io/apiserver/pkg/storage/value"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"
	"k8s.io/utils/clock"
)

var (
	// 测试共用的 scheme 与 codec；init 中注册 example API，作为 cacher 测试的“业务”类型。
	scheme   = runtime.NewScheme()
	codecs   = serializer.NewCodecFactory(scheme)
	errDummy = fmt.Errorf("dummy error")
)

func init() {
	metav1.AddToGroupVersion(scheme, metav1.SchemeGroupVersion)
	utilruntime.Must(example.AddToScheme(scheme))
	utilruntime.Must(examplev1.AddToScheme(scheme))
	utilruntime.Must(example2v1.AddToScheme(scheme))
	utilruntime.Must(corev1.AddToScheme(scheme))
	utilruntime.Must(metav1.AddMetaToScheme(scheme))
	scheme.AddUnversionedTypes(corev1.SchemeGroupVersion, &metav1.Status{})
	pb := protobuf.NewSerializer(scheme, scheme)
	// 存储测试使用 protobuf 编解码，与真实 apiserver 的默认存储编码保持一致。
	corev1ProtoCodec = codecs.CodecForVersions(pb, pb, schema.GroupVersions{corev1.SchemeGroupVersion}, schema.GroupVersions{corev1.SchemeGroupVersion})
	examplev1ProtoCodec = codecs.CodecForVersions(pb, pb, schema.GroupVersions{examplev1.SchemeGroupVersion}, nil)
}

var (
	corev1ProtoCodec    runtime.Codec
	examplev1ProtoCodec runtime.Codec
)

// newPod / newPodList 是 etcd3 存储所需的类型构造器。
func newPod() runtime.Object     { return &example.Pod{} }
func newPodList() runtime.Object { return &example.PodList{} }

// newEtcdTestStorage 启动测试 etcd 并返回以 example 类型为载荷的存储实例。
func newEtcdTestStorage(t testing.TB, prefix string) (*etcd3testing.EtcdTestServer, storage.Interface) {
	return newEtcdTestStorageWithCodec(t, prefix, examplev1ProtoCodec)
}

// newEtcdTestStorageWithCodec 使用指定 codec，默认启用加密检查转换器。
func newEtcdTestStorageWithCodec(t testing.TB, prefix string, codec runtime.Codec) (*etcd3testing.EtcdTestServer, storage.Interface) {
	return newEtcdTestStorageWithOptions(t, prefix, codec, identity.NewEncryptCheckTransformer())
}

// newEtcdTestStorageWithOptions 组装 etcd3 存储；compactor 与 storage 都注册 cleanup，
// 防止测试结束后泄漏 goroutine 与连接。
func newEtcdTestStorageWithOptions(t testing.TB, prefix string, codec runtime.Codec, transformer value.Transformer) (*etcd3testing.EtcdTestServer, storage.Interface) {
	server, _ := etcd3testing.NewUnsecuredEtcd3TestClientServer(t)
	versioner := storage.APIObjectVersioner{}
	compactor := etcd3.NewCompactor(server.V3Client.Client, 0, clock.RealClock{}, nil)
	t.Cleanup(compactor.Stop)
	storage, err := etcd3.New(
		server.V3Client,
		compactor,
		codec,
		newPod,
		newPodList,
		prefix,
		"/pods/",
		schema.GroupResource{Resource: "pods"},
		transformer,
		etcd3.NewDefaultLeaseManagerConfig(),
		etcd3.NewDefaultDecoder(codec, versioner),
		versioner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(storage.Close)
	return server, storage
}

// computePodKey 生成 etcd 中 pod 的完整 key，与存储层的前缀规则一致。
func computePodKey(obj *example.Pod) string {
	return fmt.Sprintf("/pods/%s/%s", obj.Namespace, obj.Name)
}

// newCorev1EtcdTestStorage 使用真实 corev1.Pod 类型，并调大 etcd 配额以容纳大规模测试数据。
func newCorev1EtcdTestStorage(t testing.TB) (*etcd3testing.EtcdTestServer, storage.Interface) {
	cfg := testserver.NewTestConfig(t)
	cfg.QuotaBackendBytes = 4 << 30 // 4 GiB (default 2 GiB is too small for 150k pods)
	server := &etcd3testing.EtcdTestServer{V3Client: testserver.RunEtcd(t, cfg)}
	versioner := storage.APIObjectVersioner{}
	compactor := etcd3.NewCompactor(server.V3Client.Client, 0, clock.RealClock{}, nil)
	t.Cleanup(compactor.Stop)
	s, err := etcd3.New(
		server.V3Client,
		compactor,
		corev1ProtoCodec,
		func() runtime.Object { return &corev1.Pod{} },
		func() runtime.Object { return &corev1.PodList{} },
		etcd3testing.PathPrefix(),
		"/pods/",
		schema.GroupResource{Resource: "pods"},
		identity.NewEncryptCheckTransformer(),
		etcd3.NewDefaultLeaseManagerConfig(),
		etcd3.NewDefaultDecoder(corev1ProtoCodec, versioner),
		versioner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return server, s
}

// getCorev1PodAttrs 提供 cacher 索引所需的 label/field 集合。
func getCorev1PodAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil, nil, fmt.Errorf("not a pod")
	}
	fs := fields.Set{
		"metadata.name":      pod.Name,
		"metadata.namespace": pod.Namespace,
		"spec.nodeName":      pod.Spec.NodeName,
		"spec.restartPolicy": string(pod.Spec.RestartPolicy),
		"status.phase":       string(pod.Status.Phase),
	}
	return labels.Set(pod.Labels), fs, nil
}

// compactWatch 模拟 watch cache 视角下的压缩：先等待 watch cache 跟上指定 RV，
// 再推进历史窗口起点，最后在 etcd 上真正执行压缩。
func compactWatch(c *CacheDelegator, client *clientv3.Client) storagetesting.Compaction {
	return func(ctx context.Context, t *testing.T, resourceVersion string) {
		versioner := storage.APIObjectVersioner{}
		rv, err := versioner.ParseResourceVersion(resourceVersion)
		if err != nil {
			t.Fatal(err)
		}

		c.cacher.watchCache.RLock()
		err = c.cacher.watchCache.waitUntilFreshLocked(context.TODO(), false, rv)
		c.cacher.watchCache.RUnlock()
		if err != nil {
			t.Fatalf("WatchCache didn't caught up to RV: %v", rv)
		}
		c.cacher.watchCache.Lock()
		defer c.cacher.watchCache.Unlock()
		c.cacher.Lock()
		defer c.cacher.Unlock()

		if c.cacher.watchCache.resourceVersion < rv {
			t.Fatalf("Can't compact into a future version: %v", resourceVersion)
		}

		if len(c.cacher.watchers.allWatchers) > 0 || len(c.cacher.watchers.valueWatchers) > 0 {
			// 可以考虑终止这些 watcher，但考虑到
			// watchcache 并不真正支持压缩，测试中也
			// 不涉及它，这里直接抛错。
			t.Error("Open watchers are not supported during compaction")
		}

		for c.cacher.watchCache.history.startIndex < c.cacher.watchCache.history.endIndex {
			index := c.cacher.watchCache.history.startIndex % c.cacher.watchCache.history.capacity
			if c.cacher.watchCache.history.cache[index].ResourceVersion > rv {
				break
			}

			c.cacher.watchCache.history.startIndex++
		}
		c.cacher.watchCache.storage.UpdateListResourceVersion(rv)
		if _, err := client.Compact(ctx, int64(rv)); err != nil {
			t.Fatalf("Could not compact: %v", err)
		}
	}
}

// compactStore 只在存储层做压缩，并轮询等待 cacher 观察到压缩进度。
func compactStore(c *CacheDelegator, client *clientv3.Client) storagetesting.Compaction {
	return func(ctx context.Context, t *testing.T, resourceVersion string) {
		versioner := storage.APIObjectVersioner{}
		rv, err := versioner.ParseResourceVersion(resourceVersion)
		if err != nil {
			t.Fatal(err)
		}
		var currentVersion int64
		currentVersion, _, _, err = etcd3.Compact(ctx, client, currentVersion, int64(rv))
		if err != nil {
			_, _, _, err = etcd3.Compact(ctx, client, currentVersion, int64(rv))
		}
		if err != nil {
			t.Fatal(err)
		}
		// 等待观察到压缩完成。
		if c.cacher.compactor != nil {
			for {
				select {
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				case <-time.After(100 * time.Millisecond):
				}
				compactedRev := c.storage.CompactRevision()
				if compactedRev == int64(rv) {
					break
				}
			}
			c.cacher.compactor.compactIfNeeded()
		}
	}
}

// increaseRVFunc 通过写入一个无关 key 来推进全局 revision，用于测试 watch 的 RV 语义。
func increaseRVFunc(client *clientv3.Client) storagetesting.IncreaseRVFunc {
	return func(ctx context.Context, t *testing.T) int64 {
		resp, err := client.KV.Put(ctx, "increaseRV", "ok")
		if err != nil {
			t.Fatalf("Could not update increaseRV: %v", err)
		}
		return resp.Header.Revision
	}
}
