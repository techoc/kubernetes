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

package benchmark

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"maps"
	"math"
	"os"
	"path"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	resourcealpha "k8s.io/api/resource/v1alpha3"
	resourcev1beta1 "k8s.io/api/resource/v1beta1"
	resourcev1beta2 "k8s.io/api/resource/v1beta2"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/klog/v2"
	kubeschedulerconfigv1 "k8s.io/kube-scheduler/config/v1"
	apiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	kubeschedulerscheme "k8s.io/kubernetes/pkg/scheduler/apis/config/scheme"
	schedulermetrics "k8s.io/kubernetes/pkg/scheduler/metrics"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/integration/util"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
)

const (
	dateFormat               = "2006-01-02T15:04:05Z"
	testNamespace            = "sched-test"
	setupNamespace           = "sched-setup"
	throughputSampleInterval = time.Second
)

var dataItemsDir = flag.String("data-items-dir", "", "destination directory for storing generated data items for perf dashboard or performance profiles")

var runID = time.Now().Format(dateFormat)

func newDefaultComponentConfig() (*config.KubeSchedulerConfiguration, error) {
	gvk := kubeschedulerconfigv1.SchemeGroupVersion.WithKind("KubeSchedulerConfiguration")
	cfg := config.KubeSchedulerConfiguration{}
	_, _, err := kubeschedulerscheme.Codecs.UniversalDecoder().Decode(nil, &gvk, &cfg)
	if err != nil {
		return nil, err
	}
	return &cfg, nil
}

// mustSetupCluster 启动以下组件：
// - k8s api server
// - scheduler
// - 部分 kube-controller-manager 控制器
//
// 它返回常规客户端和动态客户端，以及 destroyFunc，
// 测试结束后应使用 destroyFunc 删除资源。
// 关于限速器的说明：
//   - client rate limit 设置为 5000。
func mustSetupCluster(tCtx ktesting.TContext, config *config.KubeSchedulerConfiguration, enabledFeatures map[featuregate.Feature]bool, opts *schedulerPerfOptions) (*scheduler.Scheduler, informers.SharedInformerFactory, <-chan struct{}, ktesting.TContext) {
	var runtimeConfig []string
	if enabledFeatures[features.DynamicResourceAllocation] {
		runtimeConfig = append(runtimeConfig, fmt.Sprintf("%s=true", resourceapi.SchemeGroupVersion))
		runtimeConfig = append(runtimeConfig, fmt.Sprintf("%s=true", resourcev1beta2.SchemeGroupVersion))
		runtimeConfig = append(runtimeConfig, fmt.Sprintf("%s=true", resourcev1beta1.SchemeGroupVersion))
		runtimeConfig = append(runtimeConfig, fmt.Sprintf("%s=true", resourcealpha.SchemeGroupVersion))
	}
	if enabledFeatures[features.GenericWorkload] {
		runtimeConfig = append(runtimeConfig, fmt.Sprintf("%s=true", schedulingapi.SchemeGroupVersion))
	}
	customFlags := []string{
		// 禁用 ServiceAccount admission 插件，因为没有运行 serviceaccount controller。
		"--disable-admission-plugins=ServiceAccount,TaintNodesByCondition,Priority",
		"--runtime-config=" + strings.Join(runtimeConfig, ","),
	}
	serverOpts := apiservertesting.NewDefaultTestServerOptions()
	// 超时时间设置得足够长，以便处理最大测试用例中的 Pod 删除操作。
	serverOpts.RequestTimeout = 10 * time.Minute
	server, err := apiservertesting.StartTestServer(tCtx, serverOpts, customFlags, framework.SharedEtcd())
	if err != nil {
		tCtx.Fatalf("start apiserver: %v", err)
	}
	// 清理顺序与创建顺序相反：先通过取消子 context 清理客户端，
	// 再清理 server。
	tCtx.Cleanup(server.TearDownFn)
	tCtx = tCtx.WithCancel()
	tCtx.Cleanup(func() {
		tCtx.Cancel("test is done")
	})

	// TODO: 客户端连接配置（例如 QPS 或 Burst）理论上是可以配置的，这些值可以从 `config` 推导出来，
	// 当出现依赖这类配置的测试用例时，需要支持这一功能。
	cfg := restclient.CopyConfig(server.ClientConfig)
	cfg.QPS = 5000.0
	cfg.Burst = 5000

	// 如果此处传入的 config 为 nil，则使用默认的组件配置。
	if config == nil {
		var err error
		config, err = newDefaultComponentConfig()
		if err != nil {
			tCtx.Fatalf("Error creating default component config: %v", err)
		}
	}

	tCtx = tCtx.WithRESTConfig(cfg)

	// 并非所有配置项都会生效；启动 Scheduler 时只会应用那些主要与
	// 调度性能相关的配置，其中大部分定义在 `scheduler.schedulerOptions` 中。
	scheduler, informerFactory, done := util.StartSchedulerWithDone(tCtx, config, opts.outOfTreePluginRegistry)
	util.StartFakePVController(tCtx, tCtx.Client(), informerFactory)
	runGC := util.CreateGCController(tCtx, tCtx, *cfg, informerFactory)
	runNS := util.CreateNamespaceController(tCtx, tCtx, *cfg, informerFactory)
	runResourceClaimController := func() {}
	if enabledFeatures[features.DynamicResourceAllocation] {
		// 使用 inline resource claim 的 DRA 测试依赖此控制器
		// 来创建和删除 ResourceClaim。
		runResourceClaimController = util.CreateResourceClaimController(tCtx, tCtx, tCtx.Client(), informerFactory)
	}

	informerFactory.Start(tCtx.Done())
	informerFactory.WaitForCacheSync(tCtx.Done())
	go runGC()
	go runNS()
	go runResourceClaimController()

	return scheduler, informerFactory, done, tCtx
}

func isAttempted(pod *v1.Pod) bool {
	for _, cond := range pod.Status.Conditions {
		if cond.Type == v1.PodScheduled {
			return true
		}
	}
	return false
}

// getScheduledPods 返回指定 namespace 中已调度、已尝试调度但不可调度
// 以及尚未尝试调度的 Pod 列表。
// 可以使用 label selector 来过滤 Pod。
// 注意，未指定任何 namespace 时会匹配所有 namespace。
func getScheduledPods(podInformer coreinformers.PodInformer, labelSelector map[string]string, namespaces ...string) ([]*v1.Pod, []*v1.Pod, []*v1.Pod, error) {
	pods, err := podInformer.Lister().List(labels.Everything())
	if err != nil {
		return nil, nil, nil, err
	}

	s := sets.New(namespaces...)
	scheduled := make([]*v1.Pod, 0, len(pods))
	attempted := make([]*v1.Pod, 0, len(pods))
	unattempted := make([]*v1.Pod, 0, len(pods))
	for i := range pods {
		pod := pods[i]
		if (len(s) == 0 || s.Has(pod.Namespace)) && labelsMatch(pod.Labels, labelSelector) {
			if len(pod.Spec.NodeName) > 0 {
				scheduled = append(scheduled, pod)
			} else if isAttempted(pod) {
				attempted = append(attempted, pod)
			} else {
				unattempted = append(unattempted, pod)
			}
		}
	}
	return scheduled, attempted, unattempted, nil
}

// DataItem 表示一个数据点。
type DataItem struct {
	// Data 是从 bucket 到实际数据点的映射（例如 "Perc90" -> 23.5）。注意，
	// 所有具有相同 label 组合的 data item 应该具有相同的 bucket。
	Data map[string]float64 `json:"data"`
	// Unit 是数据的单位。注意，所有具有相同 label 组合的 data item
	// 应该具有相同的单位。
	Unit string `json:"unit"`
	// Labels 是该 data item 的 label。
	Labels map[string]string `json:"labels,omitempty"`

	// progress 包含随时间变化的已调度 Pod 数量。
	progress []podScheduling
	start    time.Time
}

// DataItems 是数据点的集合。它是 perf dashboard 所期望的结构体。
type DataItems struct {
	Version   string     `json:"version"`
	DataItems []DataItem `json:"dataItems"`
}

type podScheduling struct {
	ts            time.Time
	attempts      int
	completed     int
	observedTotal int
	observedRate  float64
	activePods    int
	backoffPods   int
	unschedulable int
	gatedPods     int
}

// makeBasePod 创建一个用作模板的 Pod 对象。
func makeBasePod() *v1.Pod {
	basePod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "pod-",
		},
		Spec: MakePodSpec(),
	}
	return basePod
}

// makeBaseNode 使用给定的 nodeNamePrefix 创建一个用作模板的 Node 对象。
func makeBaseNode(nodeNamePrefix string) *v1.Node {
	return &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: nodeNamePrefix,
		},
		Status: v1.NodeStatus{
			Capacity: v1.ResourceList{
				v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
				v1.ResourceCPU:    resource.MustParse("4"),
				v1.ResourceMemory: resource.MustParse("32Gi"),
			},
			Allocatable: v1.ResourceList{
				v1.ResourcePods:   *resource.NewQuantity(110, resource.DecimalSI),
				v1.ResourceCPU:    resource.MustParse("4"),
				v1.ResourceMemory: resource.MustParse("32Gi"),
			},
			Phase: v1.NodeRunning,
			Conditions: []v1.NodeCondition{
				{Type: v1.NodeReady, Status: v1.ConditionTrue},
			},
		},
	}

}

func dataItems2JSONFile(dataItems DataItems, namePrefix string) error {
	// perfdash 要求所有 data item 具有相同的一组 label。
	// 然后它会为每个 label 渲染下拉按钮，列出为该 label 找到的所有值。
	// 如果我们存储了缺少某个 label 的 data item，perfdash 将永远不会显示这些
	// data item，因为它只显示具有当前所选 label 值的 data item。
	// 为避免这种情况，我们收集所有曾用到的 label，
	// 并为缺失的 label 添加值为 "not applicable" 的项。
	labels := sets.New[string]()
	for _, item := range dataItems.DataItems {
		for label := range item.Labels {
			labels.Insert(label)
		}
	}
	for _, item := range dataItems.DataItems {
		for label := range labels {
			if _, ok := item.Labels[label]; !ok {
				item.Labels[label] = "not applicable"
			}
		}
	}

	b, err := json.Marshal(dataItems)
	if err != nil {
		return err
	}

	destFile := fmt.Sprintf("%v_%v.json", namePrefix, time.Now().Format(dateFormat))
	if *dataItemsDir != "" {
		// 确保 "dataItemsDir" 路径有效。
		if err := os.MkdirAll(*dataItemsDir, 0750); err != nil {
			return fmt.Errorf("dataItemsDir path %v does not exist and cannot be created: %v", *dataItemsDir, err)
		}
		destFile = path.Join(*dataItemsDir, destFile)
	}
	formatted := &bytes.Buffer{}
	if err := json.Indent(formatted, b, "", "  "); err != nil {
		return fmt.Errorf("indenting error: %v", err)
	}
	return os.WriteFile(destFile, formatted.Bytes(), 0644)
}

func createOutputFile(suffix string) (*os.File, error) {
	destFile := perTestFilePrefix + suffix
	if *dataItemsDir != "" {
		// Ensure the "dataItemsDir" path is valid.
		if err := os.MkdirAll(*dataItemsDir, 0750); err != nil {
			return nil, fmt.Errorf("dataItemsDir path %v does not exist and cannot be created: %w", *dataItemsDir, err)
		}
		destFile = path.Join(*dataItemsDir, destFile)
	}
	f, err := os.Create(destFile)
	if err != nil {
		return nil, fmt.Errorf("create output file: %w", err)
	}
	return f, nil
}

type labelValues struct {
	Label  string
	Values []string
}

// metricsCollectorConfig 是要序列化为 YAML 配置文件的配置。
// 注意：这里的映射方式意味着只支持一个过滤器，即只能收集 `values` 列表中的值。
type metricsCollectorConfig struct {
	Metrics map[string][]*labelValues
}

// metricsCollector 从 legacyregistry.DefaultGatherer.Gather() 端点收集指标。
// 目前只支持 Histogram 类型的指标。
type metricsCollector struct {
	*metricsCollectorConfig
	labels map[string]string
}

func newMetricsCollector(config *metricsCollectorConfig, labels map[string]string) *metricsCollector {
	return &metricsCollector{
		metricsCollectorConfig: config,
		labels:                 labels,
	}
}

func (mc *metricsCollector) init() error {
	// 重置这些指标，以免本次测量受到之前步骤中收集结果的影响。
	m, err := legacyregistry.DefaultGatherer.Gather()
	if err != nil {
		return fmt.Errorf("failed to gather metrics to reset: %w", err)
	}
	for _, mFamily := range m {
		// 只重置该 collector 中定义的指标。
		if _, ok := mc.Metrics[mFamily.GetName()]; ok {
			mFamily.Reset()
		}
	}
	return nil
}

func (*metricsCollector) run(tCtx ktesting.TContext) {
	// metricCollector 不需要在测试前启动，因此这里无需做任何事。
}

func (mc *metricsCollector) collect() []DataItem {
	gm, err := testutil.GatherMetrics(legacyregistry.DefaultGatherer)
	if err != nil {
		klog.ErrorS(err, "failed to gather metrics for collection")
		return nil
	}

	var dataItems []DataItem
	for metric, labelValsSlice := range mc.Metrics {
		// 未指定过滤器时，聚合同一 metricFamily 中的所有指标。
		if labelValsSlice == nil {
			dataItem := collectHistogramVec(gm, metric, mc.labels, nil)
			if dataItem != nil {
				dataItems = append(dataItems, *dataItem)
			}
		} else {
			for _, lvMap := range uniqueLVCombos(labelValsSlice) {
				dataItem := collectHistogramVec(gm, metric, mc.labels, lvMap)
				if dataItem != nil {
					dataItems = append(dataItems, *dataItem)
				}
			}
		}
	}
	return dataItems
}

// uniqueLVCombos 列出所有可能的 label 值组合。
// 例如，如果有 3 个 labelValues，每个包含 2 个值，
// 结果将是 {A: a1, B: b1, C: c1}、{A: a2, B: b1, C: c1}、{A: a1, B: b2, C: c1} 等（2^3 = 8 种组合）。
func uniqueLVCombos(lvs []*labelValues) []map[string]string {
	if len(lvs) == 0 {
		return []map[string]string{{}}
	}

	remainingCombos := uniqueLVCombos(lvs[1:])

	results := make([]map[string]string, 0)

	current := lvs[0]
	for _, value := range current.Values {
		for _, combo := range remainingCombos {
			newCombo := make(map[string]string, len(combo)+1)
			for k, v := range combo {
				newCombo[k] = v
			}
			newCombo[current.Label] = value
			results = append(results, newCombo)
		}
	}
	return results
}

func collectHistogramVec(gm *testutil.GatheredMetrics, metric string, labels map[string]string, lvMap map[string]string) *DataItem {
	vec, err := gm.GetHistogramVec(metric, lvMap)
	if err != nil {
		// "metric ... not found" 很常见，不要在输出中反复打印它！
		if !strings.HasSuffix(err.Error(), "not found") {
			klog.Error(err)
		}
		return nil
	}

	if err := vec.Validate(); err != nil {
		klog.ErrorS(err, "the validation for HistogramVec is failed. The data for this metric won't be stored in a benchmark result file", "metric", metric, "labels", labels)
		return nil
	}

	if vec.GetAggregatedSampleCount() == 0 {
		return nil
	}

	q50 := vec.Quantile(0.50)
	q90 := vec.Quantile(0.90)
	q95 := vec.Quantile(0.95)
	q99 := vec.Quantile(0.99)
	avg := vec.Average()

	msFactor := float64(time.Second) / float64(time.Millisecond)

	// 复制 label，并为该指标添加 "Metric" label。
	labelMap := map[string]string{"Metric": metric}
	for k, v := range labels {
		labelMap[k] = v
	}
	for k, v := range lvMap {
		labelMap[k] = v
	}
	return &DataItem{
		Labels: labelMap,
		Data: map[string]float64{
			"Perc50":  q50 * msFactor,
			"Perc90":  q90 * msFactor,
			"Perc95":  q95 * msFactor,
			"Perc99":  q99 * msFactor,
			"Average": avg * msFactor,
		},
		Unit: "ms",
	}
}

type throughputCollector struct {
	podInformer           coreinformers.PodInformer
	schedulingThroughputs []float64
	labelSelector         map[string]string
	resultLabels          map[string]string
	namespaces            sets.Set[string]
	errorMargin           float64

	progress []podScheduling
	start    time.Time
}

func newThroughputCollector(podInformer coreinformers.PodInformer, resultLabels map[string]string, labelSelector map[string]string, namespaces []string, errorMargin float64) *throughputCollector {
	return &throughputCollector{
		podInformer:   podInformer,
		labelSelector: labelSelector,
		resultLabels:  resultLabels,
		namespaces:    sets.New(namespaces...),
		errorMargin:   errorMargin,
	}
}

func (tc *throughputCollector) init() error {
	return nil
}

func (tc *throughputCollector) run(tCtx ktesting.TContext) {
	// 该 collector 基于 informer 缓存事件，而不是定期 list Pod，原因是：
	// - 轮询会带来更多开销
	// - 当 Pod 被快速创建、调度并删除时，轮询无法正常工作
	//
	// 通常情况下，informer 不能可靠地用于观察状态变化。
	// 它只能保证报告*部分*更新，而不是*全部*。
	// 但在 scheduler_perf 中，Scheduler 和测试共享同一个 informer，
	// 因此我们一定能看到没有 NodeName 的新 Pod（因为
	// Scheduler 调度时需要看到这样的 Pod），
	// 然后看到带有 NodeName 的更新后 Pod（因为之后不会再修改它）。
	var mutex sync.Mutex
	scheduledPods := 0
	getScheduledPods := func() int {
		mutex.Lock()
		defer mutex.Unlock()
		return scheduledPods
	}
	onPodChange := func(oldObj, newObj any) {
		oldPod, newPod, err := schedutil.As[*v1.Pod](oldObj, newObj)
		if err != nil {
			tCtx.Errorf("unexpected pod events: %v", err)
			return
		}

		if !tc.namespaces.Has(newPod.Namespace) || !labelsMatch(newPod.Labels, tc.labelSelector) {
			return
		}

		mutex.Lock()
		defer mutex.Unlock()
		if (oldPod == nil || oldPod.Spec.NodeName == "") && newPod.Spec.NodeName != "" {
			// 已完成调度。
			scheduledPods++
		}
	}
	handle, err := tc.podInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			onPodChange(nil, obj)
		},
		UpdateFunc: func(oldObj, newObj any) {
			onPodChange(oldObj, newObj)
		},
	})
	if err != nil {
		tCtx.Fatalf("register pod event handler: %v", err)
	}
	defer func() {
		tCtx.ExpectNoError(tc.podInformer.Informer().RemoveEventHandler(handle), "remove event handler")
	}()

	// 等待初始同步没有奏效，`handle.HasSynced` 始终返回
	// false —— 可能是因为事件处理程序被添加到一个已经运行的 informer 上。
	// 这基本可以接受，因为吞吐量通常是在空 namespace 中测量的。
	//
	// syncTicker := time.NewTicker(time.Millisecond)
	// defer syncTicker.Stop()
	// for {
	// 	select {
	// 	case <-syncTicker.C:
	// 		if handle.HasSynced() {
	// 			break
	// 		}
	// 	case <-tCtx.Done():
	// 		return
	// 	}
	// }
	tCtx.Logf("Started pod throughput collector for namespace(s) %s, %d pods scheduled so far", sets.List(tc.namespaces), getScheduledPods())

	lastScheduledCount := getScheduledPods()
	ticker := time.NewTicker(throughputSampleInterval)
	defer ticker.Stop()
	lastSampleTime := time.Now()
	started := false
	skipped := 0

	activePods := schedulermetrics.ActivePods()
	backoffPods := schedulermetrics.BackoffPods()
	unschedulablePods := schedulermetrics.UnschedulablePods()
	gatedPods := schedulermetrics.GatedPods()

	for {
		select {
		case <-tCtx.Done():
			return
		case <-ticker.C:
			now := time.Now()

			scheduled := getScheduledPods()
			// 只有在已调度 Pod 数量大于零时才进行采样。
			if scheduled == 0 {
				continue
			}
			if !started {
				started = true
				// 跳过第一个样本。由于采样和创建 Pod 是独立启动的，
				// 该样本很可能是异常值。
				lastScheduledCount = scheduled
				lastSampleTime = now
				tc.start = now
				continue
			}

			newScheduled := scheduled - lastScheduledCount
			if newScheduled == 0 {
				// 该时间间隔内的吞吐量将为零。
				// 与其记录 0 pods/s，不如继续等待，
				// 直到看到至少又有一个 Pod 完成调度。
				skipped++
				continue
			}

			// 该值大致应等于
			// throughputSampleInterval * (skipped + 1)，但我们
			// 不能依赖这一点，因为定时器触发时
			// goroutine 可能不会立即被调度。
			// 因此我们记录实际的时间戳。
			duration := now.Sub(lastSampleTime)
			expectedDuration := throughputSampleInterval * time.Duration(skipped+1)
			errorMargin := (duration - expectedDuration).Seconds() / expectedDuration.Seconds() * 100
			if tc.errorMargin > 0 && math.Abs(errorMargin) > tc.errorMargin {
				// 这可能会影响结果，需要报告。
				klog.Infof("WARNING: Expected throughput collector to sample at regular time intervals. The %d most recent intervals took %s instead of %s, a difference of %0.1f%%.", skipped+1, duration, expectedDuration, errorMargin)
			}

			// 为保证百分位数准确，如果跳过了某些时间间隔，
			// 我们就必须记录多个具有相同吞吐量值的样本。
			throughput := float64(newScheduled) / duration.Seconds()
			for i := 0; i <= skipped; i++ {
				tc.schedulingThroughputs = append(tc.schedulingThroughputs, throughput)
			}

			// 记录指标样本。
			counters, err := testutil.GetCounterValuesFromGatherer(legacyregistry.DefaultGatherer, "scheduler_schedule_attempts_total", map[string]string{"profile": "default-scheduler"}, "result")
			if err != nil {
				klog.Error(err)
			}
			activePods, _ := testutil.GetGaugeMetricValue(activePods)
			backoffPods, _ := testutil.GetGaugeMetricValue(backoffPods)
			unschedulable, _ := testutil.GetGaugeMetricValue(unschedulablePods)
			gatedPods, _ := testutil.GetGaugeMetricValue(gatedPods)
			tc.progress = append(tc.progress, podScheduling{
				ts:            now,
				attempts:      int(counters["unschedulable"] + counters["error"] + counters["scheduled"]),
				completed:     int(counters["scheduled"]),
				observedTotal: scheduled,
				observedRate:  throughput,
				activePods:    int(activePods),
				backoffPods:   int(backoffPods),
				unschedulable: int(unschedulable),
				gatedPods:     int(gatedPods),
			})

			lastScheduledCount = scheduled
			klog.Infof("%d pods have been scheduled successfully", lastScheduledCount)
			skipped = 0
			lastSampleTime = now
		}
	}
}

func (tc *throughputCollector) collect() []DataItem {
	throughputSummary := DataItem{
		Labels:   tc.resultLabels,
		progress: tc.progress,
		start:    tc.start,
	}

	// 如果场景中的 Pod 和 Node 数量不足，调度耗时没有超过
	// throughputSampleInterval（即 1 秒），tc.schedulingThroughputs 可能为空。
	length := len(tc.schedulingThroughputs)
	if length == 0 {
		klog.Warningf("Failed to measure SchedulingThroughput for %s. Increase pods and/or nodes to make scheduling take longer", tc.resultLabels["Name"])
		return []DataItem{throughputSummary}
	}

	sort.Float64s(tc.schedulingThroughputs)
	sum := 0.0
	for i := range tc.schedulingThroughputs {
		sum += tc.schedulingThroughputs[i]
	}

	throughputSummary.Labels["Metric"] = "SchedulingThroughput"
	throughputSummary.Data = map[string]float64{
		"Average": sum / float64(length),
		"Perc50":  tc.schedulingThroughputs[int(math.Ceil(float64(length*50)/100))-1],
		"Perc90":  tc.schedulingThroughputs[int(math.Ceil(float64(length*90)/100))-1],
		"Perc95":  tc.schedulingThroughputs[int(math.Ceil(float64(length*95)/100))-1],
		"Perc99":  tc.schedulingThroughputs[int(math.Ceil(float64(length*99)/100))-1],
	}
	throughputSummary.Unit = "pods/s"

	return []DataItem{throughputSummary}
}

// memoryCollector 在测试期间收集内存使用指标。
type memoryCollector struct {
	samples      []memorySample
	resultLabels map[string]string
	interval     time.Duration
	mu           sync.RWMutex
}

type memorySample struct {
	timestamp   time.Time
	heapInuseMB float64
}

func newMemoryCollector(resultLabels map[string]string, interval time.Duration) *memoryCollector {
	return &memoryCollector{
		resultLabels: resultLabels,
		interval:     interval,
	}
}

func (mc *memoryCollector) init() error {
	mc.collectSample()
	return nil
}

func (mc *memoryCollector) run(tCtx ktesting.TContext) {
	ticker := time.NewTicker(mc.interval)
	defer ticker.Stop()

	for {
		select {
		case <-tCtx.Done():
			return
		case <-ticker.C:
			mc.collectSample()
		}
	}
}

func (mc *memoryCollector) collectSample() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	sample := memorySample{
		timestamp:   time.Now(),
		heapInuseMB: float64(m.HeapInuse) / 1024 / 1024,
	}

	mc.mu.Lock()
	mc.samples = append(mc.samples, sample)
	mc.mu.Unlock()
}

func (mc *memoryCollector) createMetricDataItem(values []float64, unit, metricName string) DataItem {
	sort.Float64s(values)

	sum := 0.0
	for _, v := range values {
		sum += v
	}

	labels := maps.Clone(mc.resultLabels)
	labels["Metric"] = metricName

	return DataItem{
		Labels: labels,
		Data: map[string]float64{
			"Perc50":  values[int(math.Ceil(float64(len(values)*50)/100))-1],
			"Perc90":  values[int(math.Ceil(float64(len(values)*90)/100))-1],
			"Perc95":  values[int(math.Ceil(float64(len(values)*95)/100))-1],
			"Perc99":  values[int(math.Ceil(float64(len(values)*99)/100))-1],
			"Average": sum / float64(len(values)),
			"Max":     values[len(values)-1],
		},
		Unit: unit,
	}
}

func (mc *memoryCollector) collect() []DataItem {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	length := len(mc.samples)
	if length == 0 {
		return nil
	}

	firstSample := mc.samples[0]
	lastSample := firstSample
	if length >= 2 {
		lastSample = mc.samples[length-1]
	}
	durationMin := lastSample.timestamp.Sub(firstSample.timestamp).Minutes()
	growthRateMBPerMin := 0.0
	if durationMin > 0 {
		growthRateMBPerMin = (lastSample.heapInuseMB - firstSample.heapInuseMB) / durationMin
	}
	growthItem := DataItem{
		Labels: maps.Clone(mc.resultLabels),
		Data: map[string]float64{
			"GrowthRate": growthRateMBPerMin,
		},
		Unit: "MB/min",
	}
	growthItem.Labels["Metric"] = "memory_growth_rate"

	heapValues := make([]float64, len(mc.samples))
	for i, s := range mc.samples {
		heapValues[i] = s.heapInuseMB
	}

	return []DataItem{
		mc.createMetricDataItem(heapValues, "MB", "heap_memory_usage"),
		growthItem,
	}
}

// schedulingDurationCollector 计算调度阶段的总时长，包括 Pod 创建时间。
type schedulingDurationCollector struct {
	resultLabels map[string]string
	duration     time.Duration
}

func newSchedulingDurationCollector(resultLabels map[string]string) *schedulingDurationCollector {
	return &schedulingDurationCollector{
		resultLabels: resultLabels,
	}
}

func (sdc *schedulingDurationCollector) init() error {
	return nil
}

func (sdc *schedulingDurationCollector) run(tCtx ktesting.TContext) {
	start := time.Now()
	// 等待调度完成。
	<-tCtx.Done()
	sdc.duration = time.Since(start)
}

func (sdc *schedulingDurationCollector) collect() []DataItem {
	labels := maps.Clone(sdc.resultLabels)
	labels["Metric"] = "SchedulingDuration"
	return []DataItem{{
		Labels: labels,
		Data: map[string]float64{
			"Duration": sdc.duration.Seconds(),
		},
		Unit: "s",
	}}
}
