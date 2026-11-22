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

package cacher

import (
	"context"
	"fmt"
	"os"
	"testing"

	"golang.org/x/sync/errgroup"
	"sigs.k8s.io/yaml"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilrand "k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/etcd3"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/utils/clock"
)

// BenchmarkCacherInit 测量从零初始化 cacher（含 etcd 全量回填）的耗时与内存，
// 覆盖 EtcdRangeStream 与 ConcurrentWatchObjectDecode 两个 feature gate 的四种组合。
// 数据量固定为 15 万 Pod，以模拟大规模集群启动时的缓存预热成本。
func BenchmarkCacherInit(b *testing.B) {
	const pods = 150_000

	ctx := context.Background()

	server, etcdStorage := newCorev1EtcdTestStorage(b)
	b.Cleanup(func() { server.Terminate(b) })

	seedCorev1PodsParallel(b, ctx, etcdStorage, 1, pods, 32)

	config := Config{
		Storage:             etcdStorage,
		Versioner:           storage.APIObjectVersioner{},
		GroupResource:       schema.GroupResource{Resource: "pods"},
		EventsHistoryWindow: DefaultEventFreshDuration,
		ResourcePrefix:      "/pods/",
		KeyFunc: func(obj runtime.Object) (string, error) {
			return storage.NamespaceKeyFunc("/pods/", obj)
		},
		GetAttrsFunc: getCorev1PodAttrs,
		NewFunc:      func() runtime.Object { return &corev1.Pod{} },
		NewListFunc:  func() runtime.Object { return &corev1.PodList{} },
		Codec:        corev1ProtoCodec,
		Clock:        clock.RealClock{},
	}

	for _, rangeStream := range []bool{false, true} {
		for _, concurrentDecode := range []bool{false, true} {
			name := fmt.Sprintf("RangeStream=%v/ConcurrentDecode=%v", rangeStream, concurrentDecode)
			b.Run(name, func(b *testing.B) {
				featuregatetesting.SetFeatureGateDuringTest(b, utilfeature.DefaultFeatureGate, features.EtcdRangeStream, rangeStream)
				featuregatetesting.SetFeatureGateDuringTest(b, utilfeature.DefaultFeatureGate, features.ConcurrentWatchObjectDecode, concurrentDecode)

				b.ResetTimer()
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					cacher, err := NewCacherFromConfig(config)
					if err != nil {
						b.Fatal(err)
					}
					// Wait 阻塞到缓存完成初次同步，使测量包含全量回填成本。
					if err := cacher.Wait(ctx); err != nil {
						b.Fatal(err)
					}
					// Stop 与资源大小估算重置不计入计时，避免污染迭代结果。
					b.StopTimer()
					cacher.Stop()
					etcd3.TestOnlyResetResourceSizeEstimator(etcdStorage)
					b.StartTimer()
				}
				b.ReportMetric(float64(pods), "pods/cache")
			})
		}
	}
}

// loadExemplarPod 读取一个真实形状的 Pod 作为种子模板；
// 用真实对象而非最小对象可以让 JSON/protobuf 大小更接近生产。
func loadExemplarPod(b *testing.B) *corev1.Pod {
	const path = "testdata/exemplar_pod.yaml"
	data, err := os.ReadFile(path)
	if err != nil {
		b.Fatalf("read %q: %v", path, err)
	}
	var pod corev1.Pod
	if err := yaml.Unmarshal(data, &pod); err != nil {
		b.Fatalf("decode %q: %v", path, err)
	}
	return &pod
}

// randomizeCorev1Pod 为每个种子对象生成唯一名字/UID/节点名，
// 避免 etcd 中键冲突，同时模拟节点分布。
func randomizeCorev1Pod(pod *corev1.Pod, ns string) {
	pod.Namespace = ns
	pod.Name = pod.GenerateName + utilrand.String(10)
	pod.UID = uuid.NewUUID()
	pod.ResourceVersion = ""
	pod.Spec.NodeName = "some-node-prefix-" + utilrand.String(6)
}

// seedCorev1PodsParallel 用 errgroup 并发写入种子 Pod：
// 一个 goroutine 生成对象，workers 个 goroutine 消费并写入 etcd。
// 有界 channel 限制内存占用，避免 15 万对象一次性驻留。
func seedCorev1PodsParallel(b *testing.B, ctx context.Context, etcdStorage storage.Interface, namespaces, podsPerNS, workers int) {
	exemplar := loadExemplarPod(b)
	g, gctx := errgroup.WithContext(ctx)
	pods := make(chan *corev1.Pod, workers*4)

	for range workers {
		g.Go(func() error {
			var out corev1.Pod
			for pod := range pods {
				key := fmt.Sprintf("/pods/%s/%s", pod.Namespace, pod.Name)
				if err := etcdStorage.Create(gctx, key, pod, &out, 0); err != nil {
					return err
				}
			}
			return nil
		})
	}

	g.Go(func() error {
		defer close(pods)
		for range namespaces {
			ns := utilrand.String(10)
			for range podsPerNS {
				p := exemplar.DeepCopy()
				randomizeCorev1Pod(p, ns)
				select {
				case pods <- p:
				case <-gctx.Done():
					return gctx.Err()
				}
			}
		}
		return nil
	})

	if err := g.Wait(); err != nil {
		b.Fatalf("seed: %v", err)
	}
}
