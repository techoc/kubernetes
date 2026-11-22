/*
Copyright 2024 The Kubernetes Authors.

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

// Package testing 提供存储层实现（etcd3、cacher 等）共用的行为测试与
// 基准测试脚手架，包括数据准备、后台负载模拟与 watch 延迟统计。
package testing

import (
	"context"
	_ "embed"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/clock"
	"sigs.k8s.io/yaml"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/apis/example"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/storage"
)

//go:embed testdata/exemplar_pod.yaml
var exemplarPodYAML []byte

// scope 表示 list 的查询范围：整个集群、单个节点或单个命名空间。
type scope string

var (
	cluster   scope = "Cluster"
	node      scope = "Node"
	namespace scope = "Namespace"
)

const (
	// 后台负载类型：无负载、watch、普通 list、带精确/不早于 RV 的 list、
	// watch-list（流式初始列表）。写入流量类型则包括删除后重建与
	// GuaranteedUpdate 打补丁。
	loadNone               = "None"
	loadWatcher            = "Watcher"
	loadLister             = "Lister"
	loadListerExactRV      = "ListerExactRV"
	loadListerNotOlderThan = "ListerNotOlderThan"
	loadWatchList          = "WatchList"
	trafficDeleteCreate    = "DeleteCreate"
	trafficPatch           = "Patch"
)

// RunBenchmarkWriteThroughput 测试写吞吐。外层按流量类型、并行度、
// 后台负载类型与是否使用索引组合出子基准，使不同读负载对写路径的
// 影响可单独观察。tracker 非 nil 时同时统计 watch 端到端 p99 延迟。
func RunBenchmarkWriteThroughput(ctx context.Context, b *testing.B, store storage.Interface, data BenchmarkData, hasIndex bool, tracker *WatchLatencyTracker) {
	require.NoError(b, PrecreateBenchmarkPods(ctx, store, data))
	require.NoError(b, waitForConsistent(ctx, store))

	for _, trafficType := range []string{trafficDeleteCreate, trafficPatch} {
		b.Run(fmt.Sprintf("Traffic=%s", trafficType), func(b *testing.B) {
			for _, parallelism := range []int{25} {
				b.Run(fmt.Sprintf("Parallelism=%d", parallelism), func(b *testing.B) {
					for _, loadType := range []string{loadNone, loadWatcher, loadLister, loadListerExactRV, loadListerNotOlderThan, loadWatchList} {
						// 只有存储支持索引且确实存在后台读时才比较索引开关。
						useIndexOptions := []bool{false}
						if hasIndex && loadType != loadNone {
							useIndexOptions = []bool{false, true}
						}
						for _, readIndexed := range useIndexOptions {
							b.Run(fmt.Sprintf("Background=%s/UseIndex=%v", loadType, readIndexed), func(b *testing.B) {
								b.SetParallelism(parallelism)
								if tracker != nil {
									tracker.Reset()
								}
								runBenchmarkWriteThroughput(ctx, b, store, data, trafficType, loadType, readIndexed, tracker)
							})
						}
					}
				})
			}
		})
	}
}

// runBenchmarkWriteThroughput 启动选定的后台读负载，跑完写入循环后
// 以 1 秒为周期报告写吞吐，并分别报告读侧吞吐。计数器在计时开始前
// 清零，保证数据只反映被测阶段。
func runBenchmarkWriteThroughput(ctx context.Context, b *testing.B, store storage.Interface, data BenchmarkData, trafficType string, loadType string, readIndexed bool, tracker *WatchLatencyTracker) {
	stopBackgroundLoadCh := make(chan struct{})
	var workersWg sync.WaitGroup
	var stopOnce sync.Once
	// stopBackgroundLoad 幂等：正常结束与 defer 都会调用，保证 goroutine
	// 一定被回收且只等待一次。
	stopBackgroundLoad := func() {
		stopOnce.Do(func() {
			close(stopBackgroundLoadCh)
			workersWg.Wait()
		})
	}
	defer stopBackgroundLoad()

	var writes atomic.Uint64
	var watchEvents atomic.Uint64
	var listCalls atomic.Uint64
	var listObjects atomic.Uint64
	var index atomic.Uint64
	var latestRV atomic.Pointer[string]
	initialRV := "0"
	latestRV.Store(&initialRV)

	switch loadType {
	case loadNone:
	case loadWatcher:
		startBackgroundWatchers(ctx, store, data, 10, readIndexed, &workersWg, stopBackgroundLoadCh, &watchEvents)
	case loadLister:
		startBackgroundListers(ctx, store, data, 1, readIndexed, &workersWg, stopBackgroundLoadCh, &listCalls, &listObjects, "", &latestRV)
	case loadListerExactRV:
		startBackgroundListers(ctx, store, data, 1, readIndexed, &workersWg, stopBackgroundLoadCh, &listCalls, &listObjects, metav1.ResourceVersionMatchExact, &latestRV)
	case loadListerNotOlderThan:
		startBackgroundListers(ctx, store, data, 1, readIndexed, &workersWg, stopBackgroundLoadCh, &listCalls, &listObjects, metav1.ResourceVersionMatchNotOlderThan, &latestRV)
	case loadWatchList:
		startBackgroundWatchListers(ctx, store, data, 1, readIndexed, &workersWg, stopBackgroundLoadCh, &listCalls, &listObjects)
	default:
		panic(fmt.Sprintf("Unknown load type: %s", loadType))
	}
	writes.Store(0)
	watchEvents.Store(0)
	listCalls.Store(0)
	listObjects.Store(0)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// 取模轮转对象，避免并发写者总是竞争同一 key。
			i := int(index.Add(1)) % len(data.PodKeys)
			writes.Add(runTraffic(ctx, b, store, data, trafficType, i, &latestRV, tracker))
		}
	})
	elapsedSeconds := b.Elapsed().Seconds()
	require.NoError(b, waitForConsistent(ctx, store))
	b.ReportMetric(float64(writes.Load())/elapsedSeconds, "writes/s")

	stopBackgroundLoad()

	switch loadType {
	case loadWatcher:
		b.ReportMetric(float64(watchEvents.Load())/elapsedSeconds, "watch-events/s")
	case loadLister, loadListerExactRV, loadListerNotOlderThan, loadWatchList:
		b.ReportMetric(float64(listCalls.Load())/elapsedSeconds, "list-calls/s")
		b.ReportMetric(float64(listObjects.Load())/elapsedSeconds, "list-objs/s")
	}

	if tracker != nil {
		// 样本不足 100 时 p99 无意义，tracker 返回 0 表示不报告。
		if p99 := tracker.GetP99Latency(); p99 > 0 {
			b.ReportMetric(p99.Seconds(), "watch-latency-p99-s")
		}
	}
}

// waitForConsistent 通过一次 Limit=1 的 list 确认存储已追上此前写入，
// 避免在缓存尚未同步时开始计时。
func waitForConsistent(ctx context.Context, store storage.Interface) error {
	listOut := &example.PodList{}
	err := store.GetList(ctx, "/pods/", storage.ListOptions{
		Recursive: true,
		Predicate: storage.SelectionPredicate{
			Label: labels.Everything(),
			Field: fields.Everything(),
			Limit: 1,
		},
	}, listOut)
	if err != nil {
		return fmt.Errorf("unexpected error waiting for consistency: %w", err)
	}
	return nil
}

// runTraffic 对单个对象执行一次写入流量。DeleteCreate 模式下 NotFound
// 与 AlreadyExists 都是预期竞态，不计为错误；Patch 使用 GuaranteedUpdate
// 以免并发更新相互覆盖。成功写入后更新 latestRV，供精确 RV list 使用。
func runTraffic(ctx context.Context, b *testing.B, store storage.Interface, data BenchmarkData, trafficType string, index int, latestRV *atomic.Pointer[string], tracker *WatchLatencyTracker) (writes uint64) {
	var podOut *example.Pod
	switch trafficType {
	case trafficDeleteCreate:
		podOut = &example.Pod{}
		err := store.Delete(ctx, data.PodKeys[index], podOut, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{})
		if err == nil {
			writes += 1
		} else if !storage.IsNotFound(err) {
			panic(fmt.Sprintf("Unexpected error on Delete %q: %v", data.PodKeys[index], err))
		}
		pod := data.Pods[index].DeepCopy()
		if tracker != nil {
			tracker.RecordWrite(pod)
		}
		podOut = &example.Pod{}
		err = store.Create(ctx, data.PodKeys[index], pod, podOut, 0)
		if err == nil {
			writes += 1
			latestRV.Store(&podOut.ResourceVersion)
		} else if !storage.IsExist(err) {
			panic(fmt.Sprintf("Unexpected error on Create %q: %v", data.PodKeys[index], err))
		}
	case trafficPatch:
		podOut = &example.Pod{}
		err := store.GuaranteedUpdate(ctx, data.PodKeys[index], podOut, false, nil, patchFunc(index, tracker), nil)
		if err != nil {
			panic(fmt.Sprintf("Unexpected error on Patch %q: %v", data.PodKeys[index], err))
		} else {
			writes += 1
			latestRV.Store(&podOut.ResourceVersion)
		}
	default:
		panic(fmt.Sprintf("Unknown traffic type: %s", trafficType))
	}
	return writes
}

// patchFunc 返回 GuaranteedUpdate 的更新函数：写入一个携带序号与时间戳
// 的注解。tracker 在更新函数内记录写时间，确保时间戳与被写入对象一致。
func patchFunc(i int, tracker *WatchLatencyTracker) func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
	return func(input runtime.Object, res storage.ResponseMeta) (runtime.Object, *uint64, error) {
		curr := input.(*example.Pod)
		if curr.Annotations == nil {
			curr.Annotations = make(map[string]string)
		}
		curr.Annotations["updated-by-benchmark"] = strconv.Itoa(i)
		if tracker != nil {
			tracker.RecordWrite(curr)
		}
		return curr, nil, nil
	}
}

// startBackgroundWatchers 启动 count 个并发 watch，仅消费事件并计数，
// 用来模拟大量客户端监听对写路径造成的通知压力。readIndexed 为 true
// 时按节点字段走索引过滤，比较索引与全量扫描的代价。
func startBackgroundWatchers(ctx context.Context, store storage.Interface, data BenchmarkData, count int, readIndexed bool, wg *sync.WaitGroup, stopCh <-chan struct{}, eventCounter *atomic.Uint64) {
	for i := range count {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			opts := storage.ListOptions{
				Recursive: true,
				Predicate: storage.Everything,
			}
			if readIndexed {
				nodeName := "default-node"
				if len(data.NodeNames) > 0 {
					nodeName = data.NodeNames[i%len(data.NodeNames)]
				}
				opts.Predicate.GetAttrs = podAttr
				opts.Predicate.IndexFields = []string{"spec.nodeName"}
				opts.Predicate.Field = fields.SelectorFromSet(fields.Set{"spec.nodeName": nodeName})
			}
			w, err := store.Watch(ctx, "/pods/", opts)
			if err != nil {
				return
			}
			defer w.Stop()
			for {
				select {
				case <-stopCh:
					return
				case <-ctx.Done():
					return
				case ev, ok := <-w.ResultChan():
					if !ok {
						return
					}
					eventCounter.Add(1)
					_ = ev
				}
			}
		}(i)
	}
}

// startBackgroundListers 以 10ms 周期重复 list，模拟控制器的高频
// 重新同步。带 Exact/NotOlderThan 时使用当前最新 RV；尚无有效 RV 时
// 跳过本轮，避免以 "0" 发起无意义查询。
func startBackgroundListers(ctx context.Context, store storage.Interface, data BenchmarkData, count int, readIndexed bool, wg *sync.WaitGroup, stopCh <-chan struct{}, listCounter *atomic.Uint64, objCounter *atomic.Uint64, rvMatch metav1.ResourceVersionMatch, latestRV *atomic.Pointer[string]) {
	for i := range count {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			listOut := &example.PodList{}
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stopCh:
					return
				case <-ctx.Done():
					return
				case <-ticker.C:
					opts := storage.ListOptions{
						Recursive:            true,
						ResourceVersionMatch: rvMatch,
						Predicate:            storage.Everything,
					}
					switch rvMatch {
					case metav1.ResourceVersionMatchExact, metav1.ResourceVersionMatchNotOlderThan:
						rv := *latestRV.Load()
						if rv == "0" || rv == "" {
							continue
						}
						opts.ResourceVersion = rv
					case "":
					default:
						panic(fmt.Sprintf("Unknown rvMatch: %s", rvMatch))
					}
					if readIndexed {
						nodeName := "default-node"
						if len(data.NodeNames) > 0 {
							nodeName = data.NodeNames[i%len(data.NodeNames)]
						}
						opts.Predicate.GetAttrs = podAttr
						opts.Predicate.IndexFields = []string{"spec.nodeName"}
						opts.Predicate.Field = fields.SelectorFromSet(fields.Set{"spec.nodeName": nodeName})
					}
					err := store.GetList(ctx, "/pods/", opts, listOut)
					if err == nil {
						listCounter.Add(1)
						objCounter.Add(uint64(len(listOut.Items)))
					}
				}
			}
		}(i)
	}
}

// startBackgroundWatchListers 反复建立 watch-list 流：读取初始事件直到
// 带 InitialEventsAnnotationKey 的 bookmark，然后停止并重新开始。用于
// 衡量流式初始列表（SendInitialEvents）在高频重连下的开销。
func startBackgroundWatchListers(ctx context.Context, store storage.Interface, data BenchmarkData, count int, readIndexed bool, wg *sync.WaitGroup, stopCh <-chan struct{}, listCounter *atomic.Uint64, objCounter *atomic.Uint64) {
	for i := range count {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			opts := storage.ListOptions{
				Recursive:         true,
				Predicate:         storage.Everything,
				SendInitialEvents: new(true),
			}
			opts.Predicate.AllowWatchBookmarks = true

			if readIndexed {
				nodeName := "default-node"
				if len(data.NodeNames) > 0 {
					nodeName = data.NodeNames[i%len(data.NodeNames)]
				}
				opts.Predicate.GetAttrs = podAttr
				opts.Predicate.IndexFields = []string{"spec.nodeName"}
				opts.Predicate.Field = fields.SelectorFromSet(fields.Set{"spec.nodeName": nodeName})
			}

			for {
				select {
				case <-stopCh:
					return
				case <-ctx.Done():
					return
				default:
				}

				w, err := store.Watch(ctx, "/pods/", opts)
				if err != nil {
					time.Sleep(10 * time.Millisecond)
					continue
				}

				initialFinished := false
				for !initialFinished {
					select {
					case <-stopCh:
						w.Stop()
						return
					case <-ctx.Done():
						w.Stop()
						return
					case ev, ok := <-w.ResultChan():
						if !ok {
							initialFinished = true
							break
						}
						switch ev.Type {
						case watch.Bookmark:
							pod, ok := ev.Object.(*example.Pod)
							if !ok {
								panic("Unexpected type in event")
							}
							if pod.Annotations != nil && pod.Annotations[metav1.InitialEventsAnnotationKey] == "true" {
								initialFinished = true
							}
						default:
							objCounter.Add(1)
						}
					}
				}
				w.Stop()
				listCounter.Add(1)
			}
		}(i)
	}
}

// RunBenchmarkStoreList 测试 list 吞吐，组合 RV 匹配模式、查询范围与
// 分页大小。分页选项只在预期元素足够多时才加入，避免测出无意义的
// 单页结果。
func RunBenchmarkStoreList(ctx context.Context, b *testing.B, store storage.Interface, data BenchmarkData, useIndex bool) {
	for _, rvm := range []metav1.ResourceVersionMatch{"", metav1.ResourceVersionMatchExact, metav1.ResourceVersionMatchNotOlderThan} {
		b.Run(fmt.Sprintf("RV=%s", rvm), func(b *testing.B) {
			for _, scope := range []scope{cluster, node, namespace} {
				b.Run(fmt.Sprintf("Scope=%s", scope), func(b *testing.B) {
					var expectedElements int
					switch scope {
					case namespace:
						expectedElements = len(data.Pods) / len(data.NamespaceNames)
					case node:
						expectedElements = len(data.Pods) / len(data.NodeNames)
					case cluster:
						expectedElements = len(data.Pods)
					}
					limitOptions := []int64{0}
					switch {
					case expectedElements > 1000:
						limitOptions = append(limitOptions, 1000)
					case expectedElements > 100:
						limitOptions = append(limitOptions, 100)
					}
					for _, limit := range limitOptions {
						b.Run(fmt.Sprintf("Paginate=%v", limit), func(b *testing.B) {
							runBenchmarkStoreList(ctx, b, store, limit, rvm, scope, data, useIndex)
						})
					}
				})
			}
		})
	}
}

// runBenchmarkStoreList 并发执行 list。带 RV 匹配时取最近 100 个修订号
// 轮转，模拟真实客户端追赶到接近最新的状态；namespace 范围在启用索引
// 时通过 RequestInfo 传递命名空间，验证按命名空间索引的路径。
func runBenchmarkStoreList(ctx context.Context, b *testing.B, store storage.Interface, limit int64, match metav1.ResourceVersionMatch, scope scope, data BenchmarkData, useIndex bool) {
	objectCount := atomic.Uint64{}
	listCount := atomic.Uint64{}
	var index atomic.Uint64

	b.SetParallelism(4)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(index.Add(1))
			resourceVersion := ""
			switch match {
			case metav1.ResourceVersionMatchExact, metav1.ResourceVersionMatchNotOlderThan:
				maxRevision := 1 + len(data.Pods)
				resourceVersion = fmt.Sprintf("%d", maxRevision-99+i%100)
			}
			nodeName := data.NodeNames[i%len(data.NodeNames)]
			namespaceName := data.NamespaceNames[i%len(data.NamespaceNames)]

			opts := storage.ListOptions{
				Recursive:            true,
				ResourceVersion:      resourceVersion,
				ResourceVersionMatch: match,
				Predicate: storage.SelectionPredicate{
					GetAttrs: podAttr,
					Label:    labels.Everything(),
					Field:    fields.Everything(),
					Limit:    limit,
				},
			}
			switch scope {
			case cluster:
				objects, lists := paginateList(ctx, store, "/pods/", opts)
				objectCount.Add(uint64(objects))
				listCount.Add(uint64(lists))
			case node:
				if useIndex {
					opts.Predicate.GetAttrs = podAttr
					opts.Predicate.IndexFields = []string{"spec.nodeName"}
					opts.Predicate.Field = fields.SelectorFromSet(fields.Set{"spec.nodeName": nodeName})
				}
				objects, lists := paginateList(ctx, store, "/pods/", opts)
				objectCount.Add(uint64(objects))
				listCount.Add(uint64(lists))
			case namespace:
				ctx := ctx
				if useIndex {
					opts.Predicate.IndexFields = []string{"metadata.namespace"}
					ctx = request.WithRequestInfo(ctx, &request.RequestInfo{Namespace: namespaceName})
				}
				objects, lists := paginateList(ctx, store, "/pods/"+namespaceName, opts)
				objectCount.Add(uint64(objects))
				listCount.Add(uint64(lists))
			}
		}
	})
	elapsedSeconds := b.Elapsed().Seconds()
	b.ReportMetric(float64(objectCount.Load())/elapsedSeconds, "list-objs/s")
	b.ReportMetric(float64(listCount.Load())/elapsedSeconds, "list-calls/s")
}

// paginateList 反复使用 Continue token 取完所有页。首页之后必须清空
// ResourceVersion 与 Match：分页语义要求后续页从快照继续，而不是重新
// 按 RV 查询。
func paginateList(ctx context.Context, store storage.Interface, key string, opts storage.ListOptions) (objectCount int, listCount int) {
	listOut := &example.PodList{}
	err := store.GetList(ctx, key, opts, listOut)
	if err != nil {
		panic(fmt.Sprintf("Unexpected error %s", err))
	}
	opts.Predicate.Continue = listOut.Continue
	opts.ResourceVersion = ""
	opts.ResourceVersionMatch = ""
	listCount += 1
	objectCount += len(listOut.Items)
	for opts.Predicate.Continue != "" {
		listOut := &example.PodList{}
		err := store.GetList(ctx, key, opts, listOut)
		if err != nil {
			panic(fmt.Sprintf("Unexpected error %s", err))
		}
		opts.Predicate.Continue = listOut.Continue
		listCount += 1
		objectCount += len(listOut.Items)
	}
	return objectCount, listCount
}

// podAttr 提供按字段索引所需的属性。label 返回 nil 表示不带标签；
// 字段值必须与索引配置一致，否则索引过滤不会命中。
func podAttr(obj runtime.Object) (labels.Set, fields.Set, error) {
	pod := obj.(*example.Pod)
	return nil, fields.Set{
		"spec.nodeName":      pod.Spec.NodeName,
		"metadata.namespace": pod.Namespace,
	}, nil
}

// PrepareBenchmarkData 生成指定规模的随机命名空间/节点与被随机化的
// Pod。节点按取模分配，保证各节点对象数大致均衡，避免索引基准偏向
// 某一个热点节点。
func PrepareBenchmarkData(namespaceCount, podPerNamespaceCount, nodeCount int) (data BenchmarkData) {
	exemplar := loadExemplarPod()
	data.NodeNames = make([]string, nodeCount)
	for i := 0; i < nodeCount; i++ {
		data.NodeNames[i] = rand.String(10)
	}
	data.NamespaceNames = make([]string, namespaceCount)
	for i := 0; i < namespaceCount; i++ {
		namespace := rand.String(10)
		data.NamespaceNames[i] = namespace
		for j := 0; j < podPerNamespaceCount; j++ {
			p := exemplar.DeepCopy()
			nodeIdx := (i*podPerNamespaceCount + j) % nodeCount
			randomizePod(p, namespace, data.NodeNames[nodeIdx])
			data.Pods = append(data.Pods, p)
			data.PodKeys = append(data.PodKeys, computePodKey(p))
		}
	}
	return data
}

// PrecreateBenchmarkPods 预写入全部 Pod。AlreadyExists 视为成功，
// 使基准可以在同一存储上重复运行而不必清理。
func PrecreateBenchmarkPods(ctx context.Context, store storage.Interface, data BenchmarkData) error {
	podOut := &example.Pod{}
	for _, pod := range data.Pods {
		key := computePodKey(pod)
		err := store.Create(ctx, key, pod, podOut, 0)
		if err != nil && !storage.IsExist(err) {
			return fmt.Errorf("unexpected error pre-creating pod %q: %w", key, err)
		}
	}
	return nil
}

// BenchmarkData 是基准使用的数据集；PodKeys 与 Pods 一一对应，便于
// 按索引直接定位存储 key。
type BenchmarkData struct {
	Pods           []*example.Pod
	PodKeys        []string
	NamespaceNames []string
	NodeNames      []string
}

// loadExemplarPod 从嵌入的 YAML 模板解码一个 Pod；模板缺失或解码失败
// 属于测试数据错误，直接 panic。
func loadExemplarPod() *example.Pod {
	var pod example.Pod
	if len(exemplarPodYAML) == 0 {
		panic("exemplar pod empty")
	}
	if err := yaml.Unmarshal(exemplarPodYAML, &pod); err != nil {
		panic(fmt.Sprintf("decode exemplar pod: %v", err))
	}
	return &pod
}

// randomizePod 为模板 Pod 填入唯一命名空间、名称、UID 与节点，并清空
// ResourceVersion：Create 必须由存储分配 RV，携带旧值会导致冲突。
func randomizePod(pod *example.Pod, ns string, nodeName string) {
	pod.Namespace = ns
	pod.Name = pod.GenerateName + rand.String(10)
	pod.UID = types.UID(rand.String(36))
	pod.ResourceVersion = ""
	pod.Spec.NodeName = nodeName
}

// RunBenchmarkStoreStats 单独基准 Stats 调用，用于评估对象大小估算
// 的额外开销。
func RunBenchmarkStoreStats(ctx context.Context, b *testing.B, store storage.Interface) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := store.Stats(ctx)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// latencyTimestampAnnotation 承载写入时刻（UnixNano），watch 事件到达
// 时据此计算端到端延迟。使用注解而非旁路通道，使时间戳随对象一起
// 经过序列化与传输，测量结果包含真实链路开销。
const latencyTimestampAnnotation = "watch-latency-timestamp"

// WatchLatencyTracker 在写入对象上打时间戳，并在消费 watch 事件时
// 计算延迟样本；mu 保护 durations，因为记录与读取来自不同 goroutine。
type WatchLatencyTracker struct {
	clock     clock.Clock
	mu        sync.Mutex
	durations []time.Duration
}

// NewWatchLatencyTracker 使用可注入的 clock 创建追踪器，测试中可
// 用假时钟稳定地构造延迟。
func NewWatchLatencyTracker(clk clock.Clock) *WatchLatencyTracker {
	return &WatchLatencyTracker{
		clock: clk,
	}
}

// Reset 清空样本，避免跨子基准累积。
func (t *WatchLatencyTracker) Reset() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.durations = nil
}

// RecordWrite 在对象注解中写入当前时间。必须直接修改传入对象，因为
// 该对象随后会被存储层序列化；非 metav1.Object 直接忽略。
func (t *WatchLatencyTracker) RecordWrite(obj interface{}) {
	metaObj, ok := obj.(metav1.Object)
	if !ok {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	annotations := metaObj.GetAnnotations()
	if annotations == nil {
		annotations = make(map[string]string)
	}
	annotations[latencyTimestampAnnotation] = serializeTimestamp(t.clock.Now())
	metaObj.SetAnnotations(annotations)
}

// HandleEvent 从 watch 事件对象读取写入时间并记录延迟；没有时间戳
// （例如初始列表中的旧对象）或时间戳损坏时跳过。
func (t *WatchLatencyTracker) HandleEvent(obj interface{}) {
	metaObj, ok := obj.(metav1.Object)
	if !ok {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	annotations := metaObj.GetAnnotations()
	if annotations == nil {
		return
	}
	tStr, ok := annotations[latencyTimestampAnnotation]
	if !ok {
		return
	}
	writeTime, err := parseTimestamp(tStr)
	if err != nil {
		return
	}
	delay := t.clock.Since(writeTime)
	t.durations = append(t.durations, delay)
}

// GetP99Latency 返回 p99 延迟；样本少于 100 时返回 0 表示不具统计意义。
// 排序会修改内部切片，因此在锁内进行。
func (t *WatchLatencyTracker) GetP99Latency() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.durations) < 100 {
		return 0
	}
	slices.Sort(t.durations)
	idx := len(t.durations)*99/100 - 1
	return t.durations[idx]
}

// serializeTimestamp 用 UnixNano 十进制字符串表示时间，避免依赖
// 时区或格式解析的歧义。
func serializeTimestamp(t time.Time) string {
	return strconv.FormatInt(t.UnixNano(), 10)
}

// parseTimestamp 是 serializeTimestamp 的逆操作。
func parseTimestamp(s string) (time.Time, error) {
	tNano, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, tNano), nil
}
