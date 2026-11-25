//go:build linux

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

package e2enode

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	kubeletstatsv1alpha1 "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	kubemetrics "k8s.io/kubernetes/pkg/kubelet/metrics"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubelet "k8s.io/kubernetes/test/e2e/framework/kubelet"
	e2emetrics "k8s.io/kubernetes/test/e2e/framework/metrics"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
)

const (
	kubeletAddr = "localhost:10255"
)

var _ = SIGDescribe("Density", framework.WithSerial(), framework.WithSlow(), func() {
	const (
		// resource collector 与独立 cadvisor 的数据采集时间不同步，
		// 因此前者可能漏采数据或重复采集。
		containerStatsPollingPeriod = 500 * time.Millisecond
	)

	var (
		rc *ResourceCollector
	)

	f := framework.NewDefaultFramework("density-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.BeforeEach(func(ctx context.Context) {
		// 用 createSync 启动独立的 cadvisor Pod，返回时 Pod 已在运行
		e2epod.NewPodClient(f).CreateSync(ctx, getCadvisorPod())
		// resource collector 通过独立 Cadvisor 监控细粒度的 CPU/内存用量，
		// 其 housekeeping 间隔为 1 秒
		rc = NewResourceCollector(containerStatsPollingPeriod)
	})

	f.Context("create a batch of pods", framework.WithFlaky(), func() {
		// 若该用例报出类似
		// "container "runtime": expected 95th% usage < 0.900; got 0.941" 的错误，
		// 通常需要提高 cpuLimits 或 memLimits。
		// 实际资源用量可在测试输出中找到，例如：
		// I1029 11:28:15.671913 1005 resource_usage_test.go:206] CPU usage of containers:
		// container 50th% 90th% 95th% 99th% 100th%
		// "runtime" 0.004 0.941 0.941 0.941 0.976
		// "kubelet" 0.009 0.082 0.082 0.082 0.101
		dTests := []densityTest{
			{
				podsNr:   10,
				interval: 0 * time.Millisecond,
				cpuLimits: e2ekubelet.ContainersCPUSummary{
					kubeletstatsv1alpha1.SystemContainerKubelet: {0.50: 0.1, 0.95: 0.20},
					kubeletstatsv1alpha1.SystemContainerRuntime: {0.50: 0.1, 0.95: 1.5},
				},
				memLimits: e2ekubelet.ResourceUsagePerContainer{
					kubeletstatsv1alpha1.SystemContainerKubelet: &e2ekubelet.ContainerResourceUsage{MemoryRSSInBytes: 50 * 1024 * 1024},
					kubeletstatsv1alpha1.SystemContainerRuntime: &e2ekubelet.ContainerResourceUsage{MemoryRSSInBytes: 500 * 1024 * 1024},
				},
				// 单个 Pod 启动延迟的百分位上限
				podStartupLimits: e2emetrics.LatencyMetric{
					Perc50: 16 * time.Second,
					Perc90: 18 * time.Second,
					Perc99: 20 * time.Second,
				},
				// 一批 Pod 启动延迟的上限
				podBatchStartupLimit: 25 * time.Second,
			},
		}

		for _, testArg := range dTests {
			desc := fmt.Sprintf("latency/resource should be within limit when create %d pods with %v interval", testArg.podsNr, testArg.interval)
			ginkgo.It(desc, func(ctx context.Context) {
				testArg.createMethod = "batch"
				testInfo := getTestNodeInfo(f, testArg.getTestName(), desc)

				batchLag, e2eLags := runDensityBatchTest(ctx, f, rc, testArg, testInfo, false)

				ginkgo.By("Verifying latency")
				logAndVerifyLatency(ctx, batchLag, e2eLags, testArg.podStartupLimits, testArg.podBatchStartupLimit, testInfo, true)

				ginkgo.By("Verifying resource")
				logAndVerifyResource(ctx, f, rc, testArg.cpuLimits, testArg.memLimits, testInfo, true)
			})
		}
	})

	ginkgo.Context("create a batch of pods", func() {
		dTests := []densityTest{
			{
				podsNr:   10,
				interval: 0 * time.Millisecond,
			},
			{
				podsNr:   35,
				interval: 0 * time.Millisecond,
			},
			{
				podsNr:   90,
				interval: 0 * time.Millisecond,
			},
			{
				podsNr:   10,
				interval: 100 * time.Millisecond,
			},
			{
				podsNr:   35,
				interval: 100 * time.Millisecond,
			},
			{
				podsNr:   90,
				interval: 100 * time.Millisecond,
			},
			{
				podsNr:   10,
				interval: 300 * time.Millisecond,
			},
			{
				podsNr:   35,
				interval: 300 * time.Millisecond,
			},
			{
				podsNr:   90,
				interval: 300 * time.Millisecond,
			},
		}

		for _, testArg := range dTests {
			desc := fmt.Sprintf("latency/resource should be within limit when create %d pods with %v interval [Benchmark]", testArg.podsNr, testArg.interval)
			ginkgo.It(desc, func(ctx context.Context) {
				testArg.createMethod = "batch"
				testInfo := getTestNodeInfo(f, testArg.getTestName(), desc)

				batchLag, e2eLags := runDensityBatchTest(ctx, f, rc, testArg, testInfo, true)

				ginkgo.By("Verifying latency")
				logAndVerifyLatency(ctx, batchLag, e2eLags, testArg.podStartupLimits, testArg.podBatchStartupLimit, testInfo, false)

				ginkgo.By("Verifying resource")
				logAndVerifyResource(ctx, f, rc, testArg.cpuLimits, testArg.memLimits, testInfo, false)
			})
		}
	})

	ginkgo.Context("create a batch of pods with higher API QPS", func() {
		dTests := []densityTest{
			{
				podsNr:      90,
				interval:    0 * time.Millisecond,
				APIQPSLimit: 60,
			},
			{
				podsNr:      90,
				interval:    100 * time.Millisecond,
				APIQPSLimit: 60,
			},
			{
				podsNr:      90,
				interval:    300 * time.Millisecond,
				APIQPSLimit: 60,
			},
		}

		for _, testArg := range dTests {
			ginkgo.Context("", func() {
				desc := fmt.Sprintf("latency/resource should be within limit when create %d pods with %v interval (QPS %d) [Benchmark]", testArg.podsNr, testArg.interval, testArg.APIQPSLimit)
				// API QPS 限制造成的延迟在 e2e 延迟中占比很大（可达 ~33%），
				// 会低估 Kubelet 的 Pod 启动延迟（以及创建吞吐）。
				// 因此这里将 API QPS 限制从默认的 5 提高到 60，以测试真实的 Kubelet 性能。
				// 注意这会带来更高的资源用量。
				tempSetCurrentKubeletConfig(f, func(ctx context.Context, cfg *kubeletconfig.KubeletConfiguration) {
					framework.Logf("Old QPS limit is: %d", cfg.KubeAPIQPS)
					// 设置新的 API QPS 限制
					cfg.KubeAPIQPS = int32(testArg.APIQPSLimit)
				})
				ginkgo.It(desc, func(ctx context.Context) {
					testArg.createMethod = "batch"
					testInfo := getTestNodeInfo(f, testArg.getTestName(), desc)
					batchLag, e2eLags := runDensityBatchTest(ctx, f, rc, testArg, testInfo, true)

					ginkgo.By("Verifying latency")
					logAndVerifyLatency(ctx, batchLag, e2eLags, testArg.podStartupLimits, testArg.podBatchStartupLimit, testInfo, false)

					ginkgo.By("Verifying resource")
					logAndVerifyResource(ctx, f, rc, testArg.cpuLimits, testArg.memLimits, testInfo, false)
				})
			})
		}
	})

	f.Context("create a sequence of pods", framework.WithFlaky(), func() {
		dTests := []densityTest{
			{
				podsNr:   10,
				bgPodsNr: 50,
				cpuLimits: e2ekubelet.ContainersCPUSummary{
					kubeletstatsv1alpha1.SystemContainerKubelet: {0.50: 0.30, 0.95: 0.50},
					kubeletstatsv1alpha1.SystemContainerRuntime: {0.50: 0.40, 0.95: 0.60},
				},
				memLimits: e2ekubelet.ResourceUsagePerContainer{
					kubeletstatsv1alpha1.SystemContainerKubelet: &e2ekubelet.ContainerResourceUsage{MemoryRSSInBytes: 100 * 1024 * 1024},
					kubeletstatsv1alpha1.SystemContainerRuntime: &e2ekubelet.ContainerResourceUsage{MemoryRSSInBytes: 500 * 1024 * 1024},
				},
				podStartupLimits: e2emetrics.LatencyMetric{
					Perc50: 5000 * time.Millisecond,
					Perc90: 9000 * time.Millisecond,
					Perc99: 10000 * time.Millisecond,
				},
			},
		}

		for _, testArg := range dTests {
			desc := fmt.Sprintf("latency/resource should be within limit when create %d pods with %d background pods", testArg.podsNr, testArg.bgPodsNr)
			ginkgo.It(desc, func(ctx context.Context) {
				testArg.createMethod = "sequence"
				testInfo := getTestNodeInfo(f, testArg.getTestName(), desc)
				batchlag, e2eLags := runDensitySeqTest(ctx, f, rc, testArg, testInfo)

				ginkgo.By("Verifying latency")
				logAndVerifyLatency(ctx, batchlag, e2eLags, testArg.podStartupLimits, testArg.podBatchStartupLimit, testInfo, true)

				ginkgo.By("Verifying resource")
				logAndVerifyResource(ctx, f, rc, testArg.cpuLimits, testArg.memLimits, testInfo, true)
			})
		}
	})

	ginkgo.Context("create a sequence of pods", func() {
		dTests := []densityTest{
			{
				podsNr:   10,
				bgPodsNr: 50,
			},
			{
				podsNr:   30,
				bgPodsNr: 50,
			},
			{
				podsNr:   50,
				bgPodsNr: 50,
			},
		}

		for _, testArg := range dTests {
			desc := fmt.Sprintf("latency/resource should be within limit when create %d pods with %d background pods [Benchmark]", testArg.podsNr, testArg.bgPodsNr)
			ginkgo.It(desc, func(ctx context.Context) {
				testArg.createMethod = "sequence"
				testInfo := getTestNodeInfo(f, testArg.getTestName(), desc)
				batchlag, e2eLags := runDensitySeqTest(ctx, f, rc, testArg, testInfo)

				ginkgo.By("Verifying latency")
				logAndVerifyLatency(ctx, batchlag, e2eLags, testArg.podStartupLimits, testArg.podBatchStartupLimit, testInfo, false)

				ginkgo.By("Verifying resource")
				logAndVerifyResource(ctx, f, rc, testArg.cpuLimits, testArg.memLimits, testInfo, false)
			})
		}
	})
})

type densityTest struct {
	// Pod 数量
	podsNr int
	// 后台 Pod 数量
	bgPodsNr int
	// 创建 Pod 之间的间隔（用于控制速率）
	interval time.Duration
	// 以 batch 还是 sequence 方式创建 Pod
	createMethod string
	// API QPS 限制
	APIQPSLimit int
	// 性能上限
	cpuLimits            e2ekubelet.ContainersCPUSummary
	memLimits            e2ekubelet.ResourceUsagePerContainer
	podStartupLimits     e2emetrics.LatencyMetric
	podBatchStartupLimit time.Duration
}

func (dt *densityTest) getTestName() string {
	// 当前默认的 API QPS 限制是 5
	// TODO(coufon): 有没有办法避免对此硬编码？
	APIQPSLimit := 5
	if dt.APIQPSLimit > 0 {
		APIQPSLimit = dt.APIQPSLimit
	}
	return fmt.Sprintf("density_create_%s_%d_%d_%d_%d", dt.createMethod, dt.podsNr, dt.bgPodsNr,
		dt.interval.Nanoseconds()/1000000, APIQPSLimit)
}

// runDensityBatchTest 运行批量创建 Pod 的密度测试
func runDensityBatchTest(ctx context.Context, f *framework.Framework, rc *ResourceCollector, testArg densityTest, testInfo map[string]string,
	isLogTimeSeries bool) (time.Duration, []e2emetrics.PodLatencyData) {
	const (
		podType               = "density_test_pod"
		sleepBeforeCreatePods = 30 * time.Second
	)
	var (
		mutex      = &sync.Mutex{}
		watchTimes = make(map[string]metav1.Time, 0)
		stopCh     = make(chan struct{})
	)

	// 构造测试 Pod 的数据结构
	pods := newTestPods(testArg.podsNr, true, imageutils.GetPauseImageName(), podType)

	// controller 负责监听 Pod 状态变化
	controller := newInformerWatchPod(ctx, f, mutex, watchTimes, podType)
	go controller.Run(stopCh)
	defer close(stopCh)

	// TODO(coufon): 测试中发现 kubelet 启动时可能仍忙于其他事务，导致 syncLoop 无法立即响应 Pod 创建，
	// 创建第一个 Pod 会有约 5 秒延迟。此时节点状态已经是 ready，因此等待并检查节点就绪于事无补。
	// 这里留出一段缓冲时间，让 syncLoop 准备就绪
	time.Sleep(sleepBeforeCreatePods)

	rc.Start()
	ginkgo.DeferCleanup(rc.Stop)

	ginkgo.By("Creating a batch of pods")
	// 返回一个 map['pod name']'creation time'，保存各 Pod 的创建时间戳
	createTimes := createBatchPodWithRateControl(ctx, f, pods, testArg.interval)
	ginkgo.DeferCleanup(deletePodsSync, f, pods)
	ginkgo.DeferCleanup(deletePodsSync, f, []*v1.Pod{getCadvisorPod()})

	ginkgo.By("Waiting for all Pods to be observed by the watch...")

	gomega.Eventually(ctx, func() bool {
		return len(watchTimes) == testArg.podsNr
	}, 10*time.Minute, 10*time.Second).Should(gomega.BeTrueBecause("All pods should be observed by the watch."))

	if len(watchTimes) < testArg.podsNr {
		framework.Failf("Timeout reached waiting for all Pods to be observed by the watch.")
	}

	// 分析结果
	var (
		firstCreate metav1.Time
		lastRunning metav1.Time
		init        = true
		e2eLags     = make([]e2emetrics.PodLatencyData, 0)
	)

	for name, create := range createTimes {
		watch := watchTimes[name]
		gomega.Expect(watchTimes).To(gomega.HaveKey(name))

		e2eLags = append(e2eLags,
			e2emetrics.PodLatencyData{Name: name, Latency: watch.Time.Sub(create.Time)})

		if !init {
			if firstCreate.Time.After(create.Time) {
				firstCreate = create
			}
			if lastRunning.Time.Before(watch.Time) {
				lastRunning = watch
			}
		} else {
			init = false
			firstCreate, lastRunning = create, watch
		}
	}

	sort.Sort(e2emetrics.LatencySlice(e2eLags))
	batchLag := lastRunning.Time.Sub(firstCreate.Time)

	// 记录时间序列数据。
	if isLogTimeSeries {
		logDensityTimeSeries(rc, createTimes, watchTimes, testInfo)
	}
	// 记录吞吐数据。
	logPodCreateThroughput(batchLag, e2eLags, testArg.podsNr, testInfo)

	return batchLag, e2eLags
}

// runDensitySeqTest 运行顺序创建 Pod 的密度测试
func runDensitySeqTest(ctx context.Context, f *framework.Framework, rc *ResourceCollector, testArg densityTest, testInfo map[string]string) (time.Duration, []e2emetrics.PodLatencyData) {
	const (
		podType               = "density_test_pod"
		sleepBeforeCreatePods = 30 * time.Second
	)
	bgPods := newTestPods(testArg.bgPodsNr, true, imageutils.GetPauseImageName(), "background_pod")
	testPods := newTestPods(testArg.podsNr, true, imageutils.GetPauseImageName(), podType)

	ginkgo.By("Creating a batch of background pods")

	// CreatBatch 是同步的，返回时所有 Pod 都已在运行
	e2epod.NewPodClient(f).CreateBatch(ctx, bgPods)
	ginkgo.DeferCleanup(deletePodsSync, f, bgPods)
	ginkgo.DeferCleanup(deletePodsSync, f, []*v1.Pod{getCadvisorPod()})

	time.Sleep(sleepBeforeCreatePods)

	rc.Start()
	ginkgo.DeferCleanup(rc.Stop)

	// 依次创建 Pod（一个接一个）。e2eLags 已经排好序。
	batchlag, e2eLags := createBatchPodSequential(ctx, f, testPods, podType)
	ginkgo.DeferCleanup(deletePodsSync, f, testPods)

	// 记录吞吐数据。
	logPodCreateThroughput(batchlag, e2eLags, testArg.podsNr, testInfo)

	return batchlag, e2eLags
}

// createBatchPodWithRateControl 并发创建一批 Pod，每次创建使用一个 goroutine。
// 各次创建之间留有间隔，用于控制吞吐。
func createBatchPodWithRateControl(ctx context.Context, f *framework.Framework, pods []*v1.Pod, interval time.Duration) map[string]metav1.Time {
	createTimes := make(map[string]metav1.Time)
	for i := range pods {
		pod := pods[i]
		createTimes[pod.ObjectMeta.Name] = metav1.Now()
		go e2epod.NewPodClient(f).Create(ctx, pod)
		time.Sleep(interval)
	}
	return createTimes
}

// getPodStartLatency 从 kubelet 获取 prometheus 指标 pod start latency
func getPodStartLatency(ctx context.Context, node string) (e2emetrics.KubeletLatencyMetrics, error) {
	latencyMetrics := e2emetrics.KubeletLatencyMetrics{}
	ms, err := e2emetrics.GrabKubeletMetricsWithoutProxy(ctx, node, "/metrics")
	framework.ExpectNoError(err, "Failed to get kubelet metrics without proxy in node %s", node)

	for _, samples := range ms {
		for _, sample := range samples {
			if sample.Metric["__name__"] == kubemetrics.KubeletSubsystem+"_"+kubemetrics.PodStartDurationKey {
				quantile, _ := strconv.ParseFloat(string(sample.Metric["quantile"]), 64)
				latencyMetrics = append(latencyMetrics,
					e2emetrics.KubeletLatencyMetric{
						Quantile: quantile,
						Method:   kubemetrics.PodStartDurationKey,
						Latency:  time.Duration(int(sample.Value)) * time.Microsecond})
			}
		}
	}
	return latencyMetrics, nil
}

// newInformerWatchPod 创建一个 informer，用于检查所有 Pod 是否都在运行。
func newInformerWatchPod(ctx context.Context, f *framework.Framework, mutex *sync.Mutex, watchTimes map[string]metav1.Time, podType string) cache.Controller {
	ns := f.Namespace.Name
	checkPodRunning := func(p *v1.Pod) {
		mutex.Lock()
		defer mutex.Unlock()
		defer ginkgo.GinkgoRecover()

		if p.Status.Phase == v1.PodRunning {
			if _, found := watchTimes[p.Name]; !found {
				watchTimes[p.Name] = metav1.Now()
			}
		}
	}

	_, controller := cache.NewInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				options.LabelSelector = labels.SelectorFromSet(labels.Set{"type": podType}).String()
				obj, err := f.ClientSet.CoreV1().Pods(ns).List(ctx, options)
				return runtime.Object(obj), err
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				options.LabelSelector = labels.SelectorFromSet(labels.Set{"type": podType}).String()
				return f.ClientSet.CoreV1().Pods(ns).Watch(ctx, options)
			},
		},
		&v1.Pod{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				p, ok := obj.(*v1.Pod)
				if !ok {
					framework.Failf("Failed to cast object %T to Pod", obj)
				}
				go checkPodRunning(p)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				p, ok := newObj.(*v1.Pod)
				if !ok {
					framework.Failf("Failed to cast object %T to Pod", newObj)
				}
				go checkPodRunning(p)
			},
		},
	)
	return controller
}

// createBatchPodSequential 按顺序一个接一个地创建 Pod。
func createBatchPodSequential(ctx context.Context, f *framework.Framework, pods []*v1.Pod, podType string) (time.Duration, []e2emetrics.PodLatencyData) {
	var (
		mutex       = &sync.Mutex{}
		watchTimes  = make(map[string]metav1.Time, 0)
		stopCh      = make(chan struct{})
		firstCreate metav1.Time
		lastRunning metav1.Time
		init        = true
	)
	// controller 负责监听 Pod 状态变化
	controller := newInformerWatchPod(ctx, f, mutex, watchTimes, podType)
	go controller.Run(stopCh)
	defer close(stopCh)

	batchStartTime := metav1.Now()
	e2eLags := make([]e2emetrics.PodLatencyData, 0)
	createTimes := make(map[string]metav1.Time)
	for _, pod := range pods {
		create := metav1.Now()
		createTimes[pod.Name] = create
		p := e2epod.NewPodClient(f).Create(ctx, pod)
		framework.ExpectNoError(wait.PollUntilContextTimeout(ctx, 2*time.Second, framework.PodStartTimeout, true, podWatchedRunning(watchTimes, p.Name)))
		e2eLags = append(e2eLags,
			e2emetrics.PodLatencyData{Name: pod.Name, Latency: watchTimes[pod.Name].Time.Sub(create.Time)})
	}

	for name, create := range createTimes {
		watch := watchTimes[name]
		gomega.Expect(watchTimes).To(gomega.HaveKey(name))
		if !init {
			if firstCreate.Time.After(create.Time) {
				firstCreate = create
			}
			if lastRunning.Time.Before(watch.Time) {
				lastRunning = watch
			}
		} else {
			init = false
			firstCreate, lastRunning = create, watch
		}
	}
	batchLag := lastRunning.Time.Sub(batchStartTime.Time)
	sort.Sort(e2emetrics.LatencySlice(e2eLags))
	return batchLag, e2eLags
}

// podWatchedRunning 检查 Pod 是否变为 Running，依据是 informer 设置的 watchTime
func podWatchedRunning(watchTimes map[string]metav1.Time, podName string) wait.ConditionWithContextFunc {
	return func(ctx context.Context) (done bool, err error) {
		if _, found := watchTimes[podName]; found {
			return true, nil
		}
		return false, nil
	}
}

// verifyLatencyWithinThreshold 检查某个延迟指标的 50、90 和 99 分位数是否都在预期阈值以内。
func verifyLatencyWithinThreshold(threshold, actual e2emetrics.LatencyMetric, metricName string) error {
	if actual.Perc50 > threshold.Perc50 {
		return fmt.Errorf("too high %v latency 50th percentile: %v", metricName, actual.Perc50)
	}
	if actual.Perc90 > threshold.Perc90 {
		return fmt.Errorf("too high %v latency 90th percentile: %v", metricName, actual.Perc90)
	}
	if actual.Perc99 > threshold.Perc99 {
		return fmt.Errorf("too high %v latency 99th percentile: %v", metricName, actual.Perc99)
	}
	return nil
}

// extractLatencyMetrics 返回各百分位（50th、90th 和 99th）的延迟指标。
func extractLatencyMetrics(latencies []e2emetrics.PodLatencyData) e2emetrics.LatencyMetric {
	length := len(latencies)
	perc50 := latencies[int(math.Ceil(float64(length*50)/100))-1].Latency
	perc90 := latencies[int(math.Ceil(float64(length*90)/100))-1].Latency
	perc99 := latencies[int(math.Ceil(float64(length*99)/100))-1].Latency
	perc100 := latencies[length-1].Latency
	return e2emetrics.LatencyMetric{Perc50: perc50, Perc90: perc90, Perc99: perc99, Perc100: perc100}
}

// printLatencies 以易读的格式将延迟数据写入日志。
func printLatencies(latencies []e2emetrics.PodLatencyData, header string) {
	metrics := extractLatencyMetrics(latencies)
	framework.Logf("10%% %s: %v", header, latencies[(len(latencies)*9)/10:])
	framework.Logf("perc50: %v, perc90: %v, perc99: %v", metrics.Perc50, metrics.Perc90, metrics.Perc99)
}

// logAndVerifyLatency 记录并检查 Pod 创建延迟是否满足限制。
func logAndVerifyLatency(ctx context.Context, batchLag time.Duration, e2eLags []e2emetrics.PodLatencyData, podStartupLimits e2emetrics.LatencyMetric,
	podBatchStartupLimit time.Duration, testInfo map[string]string, isVerify bool) {
	printLatencies(e2eLags, "worst client e2e total latencies")

	// TODO(coufon): 不要相信 kubelet 指标，因为它们不会被重置！
	latencyMetrics, _ := getPodStartLatency(ctx, kubeletAddr)
	framework.Logf("Kubelet Prometheus metrics (not reset):\n%s", framework.PrettyPrintJSON(latencyMetrics))

	podStartupLatency := extractLatencyMetrics(e2eLags)

	// 记录延迟性能数据
	logPerfData(getLatencyPerfData(podStartupLatency, testInfo), "latency")

	if isVerify {
		// 检查 e2e Pod 启动时间是否可接受。
		framework.ExpectNoError(verifyLatencyWithinThreshold(podStartupLimits, podStartupLatency, "pod startup"))

		// 检查批量 Pod 创建延迟
		if podBatchStartupLimit > 0 {
			if batchLag > podBatchStartupLimit {
				framework.Failf("Batch creation startup time %v exceed limit %v", batchLag, podBatchStartupLimit)
			}
		}
	}
}

// logPodCreateThroughput 计算并记录 Pod 创建吞吐。
func logPodCreateThroughput(batchLag time.Duration, e2eLags []e2emetrics.PodLatencyData, podsNr int, testInfo map[string]string) {
	logPerfData(getThroughputPerfData(batchLag, e2eLags, podsNr, testInfo), "throughput")
}
