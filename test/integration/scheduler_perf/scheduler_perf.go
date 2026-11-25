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

package benchmark

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"path"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/component-base/logs"
	logsapi "k8s.io/component-base/logs/api/v1"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/scheme"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/validation"
	schedframework "k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
	"k8s.io/kubernetes/test/utils/ktesting/initoption"
	"sigs.k8s.io/yaml"
)

type operationCode string

const (
	allocResourceClaimsOpcode    operationCode = "allocResourceClaims"
	createAnyOpcode              operationCode = "createAny"
	createNodesOpcode            operationCode = "createNodes"
	createNamespacesOpcode       operationCode = "createNamespaces"
	createPodsOpcode             operationCode = "createPods"
	createPodSetsOpcode          operationCode = "createPodSets"
	deletePodsOpcode             operationCode = "deletePods"
	createResourceClaimsOpcode   operationCode = "createResourceClaims"
	churnOpcode                  operationCode = "churn"
	updateAnyOpcode              operationCode = "updateAny"
	barrierOpcode                operationCode = "barrier"
	sleepOpcode                  operationCode = "sleep"
	startCollectingMetricsOpcode operationCode = "startCollectingMetrics"
	stopCollectingMetricsOpcode  operationCode = "stopCollectingMetrics"
	createPodGroupsOpcode        operationCode = "createPodGroups"
	startCollectingProfileOpcode operationCode = "startCollectingProfile"
	stopCollectingProfileOpcode  operationCode = "stopCollectingProfile"
)

const (
	// "churn" 操作支持两种模式。

	// Create 持续创建 API 对象而不删除它们。
	Create = "create"
	// Recreate 创建一批 API 对象后将其删除，并重复该过程。
	Recreate = "recreate"
)

const (
	extensionPointsLabelName = "extension_point"
	resultLabelName          = "result"
	pluginLabelName          = "plugin"
	eventLabelName           = "event"
)

// 使用 -v=2 运行，这是生产环境中的默认日志级别。
//
// 在 PR 中可以临时调高该值，以便运行 pull-kubernetes-scheduler-perf
// 时输出更多日志。
const DefaultLoggingVerbosity = 2

var LoggingFeatureGate FeatureGateFlag
var LoggingConfig *logsapi.LoggingConfiguration

type FeatureGateFlag interface {
	featuregate.FeatureGate
	flag.Value
}

func init() {
	f := featuregate.NewFeatureGate()
	utilruntime.Must(logsapi.AddFeatureGates(f))
	LoggingFeatureGate = f

	LoggingConfig = logsapi.NewLoggingConfiguration()
	LoggingConfig.Verbosity = DefaultLoggingVerbosity
}

var (
	defaultMetricsCollectorConfig = metricsCollectorConfig{
		Metrics: map[string][]*labelValues{
			"scheduler_framework_extension_point_duration_seconds": {
				{
					Label:  extensionPointsLabelName,
					Values: metrics.ExtensionPoints,
				},
			},
			"scheduler_scheduling_attempt_duration_seconds": {
				{
					Label:  resultLabelName,
					Values: []string{metrics.ScheduledResult, metrics.UnschedulableResult, metrics.ErrorResult},
				},
			},
			"scheduler_pod_scheduling_duration_seconds": nil,
			"scheduler_plugin_execution_duration_seconds": {
				{
					Label:  pluginLabelName,
					Values: PluginNames,
				},
				{
					Label:  extensionPointsLabelName,
					Values: metrics.ExtensionPoints,
				},
			},
			"scheduler_queueing_hint_execution_duration_seconds": {
				{
					Label:  pluginLabelName,
					Values: PluginNames,
				},
				{
					Label:  eventLabelName,
					Values: schedframework.AllClusterEventLabels(),
				},
			},
			"scheduler_event_handling_duration_seconds": {
				{
					Label:  eventLabelName,
					Values: schedframework.AllClusterEventLabels(),
				},
			},
			"scheduler_podgroup_scheduling_attempt_duration_seconds": {
				{
					Label:  resultLabelName,
					Values: []string{metrics.WaitingOnPreemptionResult, metrics.ScheduledResult, metrics.UnschedulableResult, metrics.ErrorResult},
				},
			},
			"scheduler_podgroup_scheduling_algorithm_duration_seconds": nil,
		},
	}

	// PluginNames 是 scheduler_perf 为其收集指标的插件名称列表。
	// 我们导出该变量，因为 k/k 之外的人可能希望加入自己的自定义插件。
	PluginNames = []string{
		names.PrioritySort,
		names.DefaultBinder,
		names.DefaultPreemption,
		names.DynamicResources,
		names.ImageLocality,
		names.InterPodAffinity,
		names.NodeAffinity,
		names.NodeName,
		names.NodePorts,
		names.NodeResourcesBalancedAllocation,
		names.NodeResourcesFit,
		names.NodeUnschedulable,
		names.NodeVolumeLimits,
		names.PodTopologySpread,
		names.SchedulingGates,
		names.TaintToleration,
		names.VolumeBinding,
		names.VolumeRestrictions,
		names.VolumeZone,
	}
)

var UseTestingLog bool
var PerfSchedulingLabelFilter string
var TestSchedulingLabelFilter string
var enableCPUProfile bool
var enableMemProfile bool
var enableBlockProfile bool
var enableMutexProfile bool
var enableTrace bool
var withDataItemsUniqueID bool
var perTestFilePrefix string

// InitTests 应在每个配置子目录的 TestMain 中调用。
func InitTests() error {
	// 使用 -v=2 运行，这是生产环境中的默认日志级别。
	ktesting.SetDefaultVerbosity(DefaultLoggingVerbosity)

	// test/integration/framework/flags.go 会无条件初始化
	// 日志相关的 flag。对大多数测试来说这是正确的，但在
	// scheduler_perf 测试中我们希望更好地控制这些 flag，
	// 因此在这里将它们剥离。
	var fs flag.FlagSet
	flag.CommandLine.VisitAll(func(f *flag.Flag) {
		switch f.Name {
		case "log-flush-frequency", "v", "vmodule":
			// 这些稍后由我们自己添加，不复制过来。
		default:
			fs.Var(f.Value, f.Name, f.Usage)
		}
	})
	flag.CommandLine = &fs

	flag.Var(LoggingFeatureGate, "feature-gate",
		"A set of key=value pairs that describe feature gates for alpha/experimental features. "+
			"Options are:\n"+strings.Join(LoggingFeatureGate.KnownFeatures(), "\n"))

	flag.BoolVar(&UseTestingLog, "use-testing-log", false, "Write log entries with testing.TB.Log. This is more suitable for unit testing and debugging, but less realistic in real benchmarks.")
	flag.StringVar(&PerfSchedulingLabelFilter, "perf-scheduling-label-filter", "performance", "comma-separated list of labels which a testcase must have (no prefix or +) or must not have (-), used by BenchmarkPerfScheduling")
	flag.StringVar(&TestSchedulingLabelFilter, "test-scheduling-label-filter", "integration-test,-performance", "comma-separated list of labels which a testcase must have (no prefix or +) or must not have (-), used by TestScheduling")
	flag.BoolVar(&enableCPUProfile, "perf-cpuprofile", false, "write a CPU profiles for each test")
	flag.BoolVar(&enableMemProfile, "perf-memprofile", false, "write a memory profile for each test")
	flag.BoolVar(&enableBlockProfile, "perf-blockprofile", false, "write a block profile for each test")
	flag.BoolVar(&enableMutexProfile, "perf-mutexprofile", false, "write a mutex profile for each test")
	flag.BoolVar(&enableTrace, "perf-trace", false, "write an execution trace for each test")
	flag.BoolVar(&withDataItemsUniqueID, "with-data-items-unique-id", true, "include a unique run ID in the names of files written to -data-items-dir")

	// 如果没有去掉上面的日志 flag，这里就会失败。
	logsapi.AddGoFlags(LoggingConfig, flag.CommandLine)

	flag.Parse()

	logs.InitLogs()
	return logsapi.ValidateAndApply(LoggingConfig, LoggingFeatureGate)
}

// testCase 定义一组测试用例，用于在共享的总体设置（如
// feature gate 和收集的指标）下，测试不同规模的类似工作负载的性能。
type testCase struct {
	// testCase 的名称。
	Name string
	// 运行测试前要设置的 feature gate。
	// 可选
	FeatureGates map[featuregate.Feature]bool
	// 要收集的指标列表。如果未指定，则默认使用
	// defaultMetricsCollectorConfig。
	// 可选
	MetricsCollectorConfig *metricsCollectorConfig
	// 每个工作负载必须遵循的操作序列模板。每个操作将
	// 按顺序依次执行。
	WorkloadTemplate []op
	// 在该 testCase 下运行的工作负载列表。
	Workloads []*Workload
	// SchedulerConfigPath 是调度器配置的路径
	// 可选
	SchedulerConfigPath string
	// 描述要创建的 Pod 的 spec 文件的默认路径。
	// 可以在 createPodsOp 中通过设置 PodTemplatePath 覆盖此路径。
	// 可选
	DefaultPodTemplatePath *string
	// Labels 可用于启用或禁用该测试用例内的工作负载。
	Labels []string
	// DefaultThresholdMetricSelector 定义用于阈值比较的默认指标。
	// 它只会填充给未设置 ThresholdMetricSelector 的工作负载。
	// 如果为 nil，默认指标设为 "SchedulingThroughput"，数据桶为 "Average"。
	// 可选
	DefaultThresholdMetricSelector *thresholdMetricSelector
}

func (tc *testCase) collectsMetrics() bool {
	for _, op := range tc.WorkloadTemplate {
		if op.realOp.collectsMetrics() {
			return true
		}
	}
	return false
}

func (tc *testCase) workloadNamesUnique() error {
	workloadUniqueNames := map[string]bool{}
	for _, w := range tc.Workloads {
		if workloadUniqueNames[w.Name] {
			return fmt.Errorf("%s: workload name %s is not unique", tc.Name, w.Name)
		}
		workloadUniqueNames[w.Name] = true
	}
	return nil
}

// Workload 是 testCase 下的子测试，用于按特定的操作顺序测试调度器性能。
// 一个 Workload 中创建的节点和被调度的 Pod 可以是异构的。
type Workload struct {
	// 工作负载的名称。
	Name string
	// workloadTemplate 中使用的参数值。
	Params params
	// Labels 可用于启用或禁用某个工作负载。
	Labels []string
	// Threshold 会与 thresholdMetricSelector 指定指标的平均值进行比较。
	// 比较针对 CollectMetrics 设为 true 的操作进行。
	// 如果测量值低于阈值，该工作负载所属的测试用例将失败。
	// 如果设为零，则禁用阈值检查。
	//
	// 可以是单个值，也可以是主题名到值的映射。
	// 如果映射中没有对应主题名的条目，则使用单个值。
	// 主题名会传给 RunBenchmarkPerfScheduling。这种方式
	// 使得同一批测试用例可以在不同配置下复用。
	//
	// 可选
	Threshold thresholds
	// ThresholdMetricSelector 定义 Threshold 与哪个指标进行比较。
	// 如果为 nil，则使用 testCase 的 DefaultThresholdMetricSelector。
	// 如果 DefaultThresholdMetricSelector 也为 nil，则使用 "SchedulingThroughput"。
	// 可选
	ThresholdMetricSelector *thresholdMetricSelector
	// 运行工作负载前要设置的 feature gate。
	// 在此映射中显式设置某个 feature 会覆盖测试用例的设置。
	// 可选
	FeatureGates map[featuregate.Feature]bool
}

// GetParam 从 Workload 的参数中按整数取出一个参数。
func (w *Workload) GetParam(key string) (int, error) {
	return w.Params.get(key)
}

func (w *Workload) isValid(mcc *metricsCollectorConfig) error {
	if w.Threshold.value < 0 {
		return fmt.Errorf("invalid Threshold=%f; should be non-negative", w.Threshold.value)
	}
	for topicName, value := range w.Threshold.valuesByTopic {
		if value < 0 {
			return fmt.Errorf("invalid Threshold=%f for topic %q; should be non-negative", value, topicName)
		}
	}

	return w.ThresholdMetricSelector.isValid(mcc)
}

func (w *Workload) setDefaults(testCaseThresholdMetricSelector *thresholdMetricSelector) {
	if w.ThresholdMetricSelector != nil {
		return
	}
	if testCaseThresholdMetricSelector != nil {
		w.ThresholdMetricSelector = testCaseThresholdMetricSelector
		return
	}
	// 默认情况下，应将 SchedulingThroughput 的 Average 与阈值比较。
	w.ThresholdMetricSelector = &thresholdMetricSelector{
		Name:       "SchedulingThroughput",
		DataBucket: "Average",
	}
}

type thresholds struct {
	value         float64
	valuesByTopic map[string]float64
}

func (t *thresholds) UnmarshalJSON(text []byte) error {
	if errFloat64 := json.Unmarshal(text, &t.value); errFloat64 != nil {
		// 不是普通数字，尝试按映射解析。
		if errMap := json.Unmarshal(text, &t.valuesByTopic); errMap != nil {
			return fmt.Errorf("expected either float64 or topic name -> float64 map: %w, %w", errFloat64, errMap)
		}
	}
	return nil
}

func (t *thresholds) Get(topicName string) float64 {
	if value, ok := t.valuesByTopic[topicName]; ok {
		return value
	}
	return t.value
}

// thresholdMetricSelector 定义要与阈值比较的指标的名称和标签。
type thresholdMetricSelector struct {
	// 指标名称，与 DataItem 标签中的 "Metric" 字段比较。
	Name string
	// 指标的标签。必须全部与指标的标签匹配才认为相等。
	Labels map[string]string
	// DataBucket 指定应将数据桶中的哪个值与阈值比较。
	DataBucket string
	// ExpectLower 定义阈值是否表示指标的最大允许值。
	// 如果为 false，则阈值表示最小允许值。
	// 可选
	ExpectLower bool
}

func (ms thresholdMetricSelector) isValid(mcc *metricsCollectorConfig) error {
	if ms.DataBucket == "" {
		return fmt.Errorf("dataBucket should be set for metric %v", ms.Name)
	}
	if ms.Name == "SchedulingThroughput" {
		return nil
	}

	if mcc == nil {
		mcc = &defaultMetricsCollectorConfig
	}

	labels, ok := mcc.Metrics[ms.Name]
	if !ok {
		return fmt.Errorf("the metric %v is targeted, but it's not collected during the test. Make sure the MetricsCollectorConfig is valid", ms.Name)
	}

	for _, labelsComb := range uniqueLVCombos(labels) {
		if labelsMatch(labelsComb, ms.Labels) {
			return nil
		}
	}
	return fmt.Errorf("no matching labels found for metric %v", ms.Name)
}

type params struct {
	params map[string]any
	// isUsed 字段记录参数是否被使用过。
	isUsed map[string]bool
}

// UnmarshalJSON 是 params 的自定义反序列化器。
//
// from(json):
//
//	{
//		"initNodes": 500,
//		"initPods": 50
//	}
//
// to:
//
//	params{
//		params: map[string]any{
//			"intNodes": 500,
//			"initPods": 50,
//		},
//		isUsed: map[string]bool{}, // 空 map
//	}
func (p *params) UnmarshalJSON(b []byte) error {
	aux := map[string]any{}

	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}

	p.params = aux
	p.isUsed = map[string]bool{}
	return nil
}

// get 以整数形式取出参数
func (p params) get(key string) (int, error) {
	// JSON 反序列化会把 "any" 字段中的整数常量解为浮点数。
	f, err := getParam[float64](p, key)
	if err != nil {
		return 0, err
	}
	return int(f), nil
}

// getParam 以指定类型取出参数。这里不做类型转换，
// 因此实际上只有 JSON 反序列化所使用的类型
// （float64、string、bool）才有效。
func getParam[T float64 | string | bool](p params, key string) (T, error) {
	p.isUsed[key] = true
	param, ok := p.params[key]
	var t T
	if !ok {
		return t, fmt.Errorf("parameter %s is undefined", key)
	}
	t, ok = param.(T)
	if !ok {
		return t, fmt.Errorf("parameter %s has the wrong type %T", key, param)
	}
	return t, nil
}

// unusedParams 返回未使用参数的名称
func (w Workload) unusedParams() []string {
	var ret []string
	for name := range w.Params.params {
		if !w.Params.isUsed[name] {
			ret = append(ret, name)
		}
	}
	return ret
}

// op 是一个哑结构体，真正的操作存放在其内部。
type op struct {
	realOp realOp
}

// UnmarshalJSON 是 op 结构体的自定义反序列化器，因为在运行时
// 我们并不知道正在解码的是哪个操作。
func (op *op) UnmarshalJSON(b []byte) error {
	possibleOps := map[operationCode]realOp{
		allocResourceClaimsOpcode:    &allocResourceClaimsOp{},
		createAnyOpcode:              &createAny{},
		createNodesOpcode:            &createNodesOp{},
		createNamespacesOpcode:       &createNamespacesOp{},
		createPodsOpcode:             &createPodsOp{},
		createPodSetsOpcode:          &createPodSetsOp{},
		deletePodsOpcode:             &deletePodsOp{},
		createResourceClaimsOpcode:   &createResourceClaimsOp{},
		churnOpcode:                  &churnOp{},
		updateAnyOpcode:              &updateAny{},
		barrierOpcode:                &barrierOp{},
		sleepOpcode:                  &sleepOp{},
		startCollectingMetricsOpcode: &startCollectingMetricsOp{},
		stopCollectingMetricsOpcode:  &stopCollectingMetricsOp{},
		createPodGroupsOpcode:        &createPodGroups{},
		startCollectingProfileOpcode: &startCollectingProfileOp{},
		stopCollectingProfileOpcode:  &stopCollectingProfileOp{},
		// TODO(#94601): add a delete nodes op to simulate scaling behaviour?
	}
	// 首先使用宽松解码（即忽略多余字段）确定 opcode。
	var possibleOp struct {
		Opcode operationCode
	}
	if err := json.Unmarshal(b, &possibleOp); err != nil {
		return fmt.Errorf("decoding opcode from %s: %w", string(b), err)
	}
	realOp, ok := possibleOps[possibleOp.Opcode]
	if !ok {
		return fmt.Errorf("unknown opcode %q in %s", possibleOp.Opcode, string(b))
	}
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(realOp); err != nil {
		return fmt.Errorf("decoding %s into %T: %w", string(b), realOp, err)
	}
	if err := realOp.isValid(true); err != nil {
		return fmt.Errorf("%s not valid for %T: %w", string(b), realOp, err)
	}
	op.realOp = realOp
	return nil
}

// realOp 是由不同结构体实现的接口。为了在解析时检查操作的有效性，
// 必须实现 isValid 函数。
type realOp interface {
	// isValid 检查操作参数（如节点/Pod 数量）的有效性。注意，
	// 在这个阶段我们不会捕获未定义的参数。
	//
	// 如果配置的操作不匹配，则返回 errInvalidOp。
	isValid(allowParameterization bool) error
	// collectsMetrics 检查该操作是否收集指标。
	collectsMetrics() bool
	// patchParams 在将可参数化的值替换为工作负载特定的值之后，
	// 返回一个同类型的修补后 realOp。尽管调用是通过 *realOp 发起的，
	// 但该方法应实现在值接收者的基础类型上，而不是指针接收者的基础类型上。
	// 这是因为调用方不希望接收者意外地修改 realOp
	//（而是将其作为返回值返回）。
	patchParams(w *Workload) (realOp, error)
}

// runnableOp 是部分操作实现的接口。它使得执行操作无需在 runWorkload 中
// 添加额外的代码。
type runnableOp interface {
	realOp

	// requiredNamespaces 返回 runWorkload 在运行该操作前
	// 必须创建的所有命名空间。
	requiredNamespaces() []string
	// run 执行该操作提供的步骤。
	run(ktesting.TContext)
}

func isValidParameterizable(val string) bool {
	return strings.HasPrefix(val, "$")
}

func isValidCount(allowParameterization bool, count int, countParam string) bool {
	if !allowParameterization || countParam == "" {
		// 忽略参数，值本身必须合法。
		return count >= 0
	}
	return isValidParameterizable(countParam)
}

func initTestOutput(tb testing.TB) io.Writer {
	var output io.Writer
	if UseTestingLog {
		output = framework.NewTBWriter(tb)
	} else {
		tmpDir := tb.TempDir()
		logfileName := path.Join(tmpDir, "output.log")
		fileOutput, err := os.Create(logfileName)
		if err != nil {
			tb.Fatalf("create log file: %v", err)
		}
		output = fileOutput

		tb.Cleanup(func() {
			// 测试结束后转储日志输出。若测试成功，用户
			// 可决定显示多少内容："go test" 会截断，
			// "go test -v" 则不会。失败时会显示全部内容。
			if err := fileOutput.Close(); err != nil {
				tb.Fatalf("close log file: %v", err)
			}
			log, err := os.ReadFile(logfileName)
			if err != nil {
				tb.Fatalf("read log file: %v", err)
			}
			tb.Logf("full log output:\n%s", string(log))
		})
	}
	return output
}

var specialFilenameChars = regexp.MustCompile(`[^a-zA-Z0-9-_]`)

func setupTestCase(t testing.TB, tc *testCase, featureGates map[featuregate.Feature]bool, workload *Workload, opts *schedulerPerfOptions) (*scheduler.Scheduler, informers.SharedInformerFactory, <-chan struct{}, ktesting.TContext) {
	tCtx := ktesting.Init(t, initoption.PerTestOutput(UseTestingLog))
	artifacts, doArtifacts := os.LookupEnv("ARTIFACTS")
	if !UseTestingLog && doArtifacts {
		// 重新配置日志，使其按测试写入单独的文件，
		// 而不是写入 stderr。如果测试通过，该文件会被
		// 删除。总体输出可能非常大（ci-benchmark-scheduler-perf-master
		// 超过 200 MB）。采用这种方式，我们既能保留失败时的日志输出，
		// 又无需存储大量无人查看的数据。性能与写入 stderr 相同。
		if err := logsapi.ResetForTest(LoggingFeatureGate); err != nil {
			t.Fatalf("Failed to reset the logging configuration: %v", err)
		}
		logfileName := path.Join(artifacts, specialFilenameChars.ReplaceAllString(t.Name(), "_")+".log")
		out, err := os.Create(logfileName)
		if err != nil {
			t.Fatalf("Failed to create per-test log output file: %v", err)
		}
		t.Cleanup(func() {
			// 到这里所有内容都应该已经停止，下面的
			// GoleakCheck 会进行检查（它在测试
			// 关闭时最先运行！）。因此可以进行清理。错误会被记录
			// 并使测试失败，但清理仍会继续进行。
			//
			// 注意，如果没有显式等待某个 goroutine 停止，
			// 竞态检测器就会将它标记为引发了竞态。我们知道它们
			// 一定已经停止（GoLeakCheck！），但竞态检测器不知道。
			//
			// 这是个大问题，因为许多 Kubernetes 的 goroutine 启动时
			// 并没有等待它们停止 :-(
			//
			// 实际上，klog 自身的 flushing 就被竞态检测器点名了。
			// 既然我们知道这一点，就可以显式强制它停止，
			// 以满足竞态检测器的要求。
			klog.StopFlushDaemon()
			if err := logsapi.ResetForTest(LoggingFeatureGate); err != nil {
				t.Errorf("Failed to reset the logging configuration: %v", err)
			}
			if err := out.Close(); err != nil {
				t.Errorf("Failed to close the per-test log output file: %s: %v", logfileName, err)
			}
			if !t.Failed() {
				if err := os.Remove(logfileName); err != nil {
					t.Errorf("Failed to remove the per-test log output file: %v", err)
				}
			}
		})
		opts := &logsapi.LoggingOptions{
			ErrorStream: out,
			InfoStream:  out,
		}
		if err := logsapi.ValidateAndApplyWithOptions(LoggingConfig, opts, LoggingFeatureGate); err != nil {
			t.Fatalf("Failed to apply the per-test logging configuration: %v", err)
		}
	}

	// 确保没有泄漏的
	// goroutine。它们可能影响
	// 下一个基准测试的性能。
	// 这必须在 RedirectKlog *之后* 执行，
	// 因为这样在清理时，测试会在恢复 klog 设置
	// *之前* 等待 goroutine 退出。
	framework.GoleakCheck(t)

	// 我们需要为 NodeDeclaredFeatures feature gate 设置模拟版本，该 gate 锁定在 1.37。
	// 仅当 NodeDeclaredFeatures 被显式禁用时才模拟 v1.36。
	if ndfEnabled, exists := featureGates[features.NodeDeclaredFeatures]; exists && !ndfEnabled {
		featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.36"))
	}
	featuregatetesting.SetFeatureGatesDuringTest(t, utilfeature.DefaultFeatureGate, featureGates)

	if opts.preRunFn != nil {
		cleanup, err := opts.preRunFn(tCtx, workload)
		if err != nil {
			t.Fatalf("failed to run preRunFn for workload %s: %v", workload.Name, err)
		}
		if cleanup != nil {
			t.Cleanup(cleanup)
		}
	}

	// 现在准备就绪，启动一个全新的 etcd。
	logger := tCtx.Logger()
	if !UseTestingLog {
		// 将写入全局日志的输出与当前测试关联起来。
		logger = logger.WithName(tCtx.Name())
	}
	framework.StartEtcd(logger, t, true)

	// 即使是 5000 节点的测试，30 分钟也应该足够了。
	timeout := 30 * time.Minute
	tCtx = tCtx.WithTimeout(timeout, fmt.Sprintf("timed out after the %s per-test timeout", timeout))

	return setupClusterForWorkload(tCtx, tc.SchedulerConfigPath, featureGates, opts)
}

func featureGatesMerge(src map[featuregate.Feature]bool, overrides map[featuregate.Feature]bool) map[featuregate.Feature]bool {
	result := make(map[featuregate.Feature]bool)
	maps.Copy(result, src)
	maps.Copy(result, overrides)
	return result
}

// fixJSONOutput 用于绕开 Go 不为子基准测试发出 "pass" 动作的问题
// （https://github.com/golang/go/issues/66825#issuecomment-2343229005），
// 该问题会导致 gotestsum 将成功的基准测试运行报告为失败
// （https://github.com/gotestyourself/gotestsum/issues/413#issuecomment-2343206787）。
//
// 它的做法是打印缺失的 "PASS" 输出行，test2json 随后会将其
// 转换为 "pass" 动作。
func fixJSONOutput(b *testing.B) {
	if !slices.Contains(os.Args, "-test.v=test2json") {
		// 不输出 JSON。
		return
	}

	start := time.Now()
	b.Cleanup(func() {
		if b.Failed() {
			// 确实失败了，不做处理。
			return
		}
		// 使用 -test.v=test2json 时会注入 SYN，参见
		// https://cs.opensource.google/go/go/+/refs/tags/go1.23.3:src/testing/testing.go;drc=87ec2c959c73e62bfae230ef7efca11ec2a90804;l=527
		fmt.Fprintf(os.Stderr, "%c--- PASS: %s (%.2fs)\n", 22 /* SYN */, b.Name(), time.Since(start).Seconds())
	})
}

// RunBenchmarkPerfScheduling 运行调度器性能基准测试。
//
// 可以通过 outOfTreePluginRegistry 传入自己的调度器插件。
// 此外，你可能还希望将插件加入本包的 PluginNames 变量，
// 以便为它们收集指标。
func RunBenchmarkPerfScheduling(b *testing.B, configFile string, topicName string, outOfTreePluginRegistry frameworkruntime.Registry, options ...SchedulerPerfOption) {
	opts := &schedulerPerfOptions{
		outOfTreePluginRegistry: outOfTreePluginRegistry,
	}

	for _, option := range options {
		option(opts)
	}

	testCases, err := getTestCases(configFile)
	if err != nil {
		b.Fatal(err)
	}
	if err = validateTestCases(testCases); err != nil {
		b.Fatal(err)
	}
	fixJSONOutput(b)

	if testing.Short() {
		PerfSchedulingLabelFilter += ",+short"
	}
	testcaseLabelSelectors := strings.Split(PerfSchedulingLabelFilter, ",")

	output := initTestOutput(b)

	// 由于我们是顺序运行的，因此可以更改全局
	// klog logger 并重定向日志输出。仍有相当多的代码在使用它，
	// 而不支持上下文日志。
	//
	// 由于我们泄漏了一个调用 klog 的 goroutine，因此无法恢复
	// 之前的状态。
	_ = framework.RedirectKlog(b, output)

	dataItems := DataItems{Version: "v1"}
	for _, tc := range testCases {
		b.Run(tc.Name, func(b *testing.B) {
			fixJSONOutput(b)
			for _, w := range tc.Workloads {
				b.Run(w.Name, func(b *testing.B) {
					if !enabled(testcaseLabelSelectors, append(tc.Labels, w.Labels...)...) {
						b.Skipf("disabled by label filter %q", PerfSchedulingLabelFilter)
					}
					fixJSONOutput(b)

					featureGates := featureGatesMerge(tc.FeatureGates, w.FeatureGates)
					scheduler, informerFactory, schedulerDone, tCtx := setupTestCase(b, tc, featureGates, w, opts)
					tCtx.TB().Cleanup(func() {
						tCtx.Cancel("workload is done")
						<-schedulerDone
						// 重置指标，防止当前工作负载产生的指标
						// 被带入下一个工作负载。
						legacyregistry.Reset()
					})

					err := w.isValid(tc.MetricsCollectorConfig)
					if err != nil {
						b.Fatalf("workload %s is not valid: %v", w.Name, err)
					}

					if opts.prepareFn != nil {
						err = opts.prepareFn(tCtx)
						if err != nil {
							b.Fatalf("failed to run prepareFn: %v", err)
						}
					}

					// 这用于我们自定义的 .dat 文件和 profile 文件。
					if withDataItemsUniqueID {
						perTestFilePrefix = strings.ReplaceAll(fmt.Sprintf("%s_%s_%s_%s", tc.Name, w.Name, topicName, runID), "/", "_")
					} else {
						perTestFilePrefix = strings.ReplaceAll(fmt.Sprintf("%s_%s_%s", tc.Name, w.Name, topicName), "/", "_")
					}

					results, err := runWorkload(tCtx, tc, w, topicName, scheduler, informerFactory, opts)
					if err != nil {
						tCtx.Fatalf("Error running workload %s: %s", w.Name, err)
					}
					dataItems.DataItems = append(dataItems.DataItems, results...)

					if len(results) > 0 {
						// 默认的 ns/op 没有
						// 意义，因为它包含了
						// 初始化和关闭所花的
						// 时间。这里将其抑制。
						b.ReportMetric(0, "ns/op")

						// 改为报告同样会存入
						// JSON 文件中的结果。
						for _, result := range results {
							// 对于某些指标（如
							// scheduler_framework_extension_point_duration_seconds），
							// 实际值的单位有所不同。我们修改 key
							// 以使其看起来正确。
							metric := strings.ReplaceAll(result.Labels["Metric"], "_seconds", "_"+result.Unit)
							for key, value := range result.Data {
								b.ReportMetric(value, metric+"/"+key)
							}
						}
					}

					// 无论如何，我们都应确保场景运行后 InFlightEvents 为空。
					if err = checkEmptyInFlightEvents(); err != nil {
						tCtx.Errorf("%s: %s", w.Name, err)
					}

					// 预期只有一个结果包含进度信息。
					for _, item := range results {
						if len(item.progress) == 0 {
							continue
						}

						f, err := createOutputFile(".dat")
						if err != nil {
							b.Fatalf("prepare data file: %v", err)
						}

						// 打印随时间变化的进度。
						for _, sample := range item.progress {
							if _, err := fmt.Fprintf(f, "%.1fs %d %d %d %f %d %d %d %d\n", sample.ts.Sub(item.start).Seconds(), sample.completed, sample.attempts, sample.observedTotal, sample.observedRate, sample.activePods, sample.backoffPods, sample.unschedulable, sample.gatedPods); err != nil {
								b.Fatalf("write to data file: %v", err)
							}
						}
						if err := f.Close(); err != nil {
							b.Fatalf("closing data file: %v", err)
						}
					}
				})
			}
		})
	}
	// 支持不同的顶层 BenchmarkPerfScheduling* 测试，只要它们使用唯一的主题名。
	// 最终的 JSON 文件总是命名为 BenchmarkPerfScheduling_benchmark_<topic name>_<date+time>.json，
	// 因为 perf-dash 被配置为读取该名称：
	// https://github.com/kubernetes/perf-tests/blob/581139e45e79cf04b9c2777b82677957f1e7f90b/perfdash/config.go#L520-L525
	namePrefix := b.Name()
	namePrefix = regexp.MustCompile(`^BenchmarkPerfScheduling[^/]*`).ReplaceAllString(namePrefix, "BenchmarkPerfScheduling")
	namePrefix = strings.ReplaceAll(namePrefix, "/", "_")
	namePrefix += "_benchmark_" + topicName
	if err := dataItems2JSONFile(dataItems, namePrefix); err != nil {
		b.Fatalf("unable to write measured data %+v: %v", dataItems, err)
	}
}

// RunIntegrationPerfScheduling 运行调度器性能集成测试。
func RunIntegrationPerfScheduling(t *testing.T, configFile string, options ...SchedulerPerfOption) {
	opts := &schedulerPerfOptions{}
	for _, option := range options {
		option(opts)
	}

	testCases, err := getTestCases(configFile)
	if err != nil {
		t.Fatal(err)
	}
	if err = validateTestCases(testCases); err != nil {
		t.Fatal(err)
	}

	if testing.Short() {
		TestSchedulingLabelFilter += ",+short"
	}
	testcaseLabelSelectors := strings.Split(TestSchedulingLabelFilter, ",")

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			for _, w := range tc.Workloads {
				t.Run(w.Name, func(t *testing.T) {
					if !enabled(testcaseLabelSelectors, append(tc.Labels, w.Labels...)...) {
						t.Skipf("disabled by label filter %q", TestSchedulingLabelFilter)
					}
					featureGates := featureGatesMerge(tc.FeatureGates, w.FeatureGates)
					scheduler, informerFactory, schedulerDone, tCtx := setupTestCase(t, tc, featureGates, w, opts)
					tCtx.TB().Cleanup(func() {
						tCtx.Cancel("workload is done")
						<-schedulerDone
						// 重置指标，防止当前工作负载产生的指标
						// 被带入下一个工作负载。
						legacyregistry.Reset()
					})
					err := w.isValid(tc.MetricsCollectorConfig)
					if err != nil {
						t.Fatalf("workload %s is not valid: %v", w.Name, err)
					}

					_, err = runWorkload(tCtx, tc, w, "" /* topic name not relevant */, scheduler, informerFactory, opts)
					if err != nil {
						tCtx.Fatalf("Error running workload %s: %s", w.Name, err)
					}

					// 无论如何，我们都应确保场景运行后 InFlightEvents 为空。
					if err = checkEmptyInFlightEvents(); err != nil {
						tCtx.Errorf("%s: %s", w.Name, err)
					}
				})
			}
		})
	}
}

func loadSchedulerConfig(file string) (*config.KubeSchedulerConfiguration, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	// UniversalDecoder 默认执行默认值填充并返回内部类型。
	obj, gvk, err := scheme.Codecs.UniversalDecoder().Decode(data, nil, nil)
	if err != nil {
		return nil, err
	}
	if cfgObj, ok := obj.(*config.KubeSchedulerConfiguration); ok {
		return cfgObj, nil
	}
	return nil, fmt.Errorf("couldn't decode as KubeSchedulerConfiguration, got %s: ", gvk)
}

func unrollWorkloadTemplate(tb ktesting.TB, wt []op, w *Workload) []op {
	var unrolled []op
	for opIndex, o := range wt {
		realOp, err := o.realOp.patchParams(w)
		if err != nil {
			tb.Fatalf("op %d: %v", opIndex, err)
		}
		switch concreteOp := realOp.(type) {
		case *createPodSetsOp:
			tb.Logf("Creating %d pod sets %s", concreteOp.Count, concreteOp.CountParam)
			for i := 0; i < concreteOp.Count; i++ {
				copy := concreteOp.CreatePodsOp
				ns := fmt.Sprintf("%s-%d", concreteOp.NamespacePrefix, i)
				copy.Namespace = &ns
				unrolled = append(unrolled, op{realOp: &copy})
			}
		default:
			unrolled = append(unrolled, o)
		}
	}
	return unrolled
}

func setupClusterForWorkload(tCtx ktesting.TContext, configPath string, featureGates map[featuregate.Feature]bool, opts *schedulerPerfOptions) (*scheduler.Scheduler, informers.SharedInformerFactory, <-chan struct{}, ktesting.TContext) {
	var cfg *config.KubeSchedulerConfiguration
	var err error
	if configPath != "" {
		cfg, err = loadSchedulerConfig(configPath)
		if err != nil {
			tCtx.Fatalf("error loading scheduler config file: %v", err)
		}
		if err = validation.ValidateKubeSchedulerConfiguration(cfg); err != nil {
			tCtx.Fatalf("validate scheduler config file failed: %v", err)
		}
	}
	return mustSetupCluster(tCtx, cfg, featureGates, opts)
}

func labelsMatch(actualLabels, requiredLabels map[string]string) bool {
	for requiredLabel, requiredValue := range requiredLabels {
		actualValue, ok := actualLabels[requiredLabel]
		if !ok || requiredValue != actualValue {
			return false
		}
	}
	return true
}

func valueWithinThreshold(value, threshold float64, expectLower bool) bool {
	if expectLower {
		return value < threshold
	}
	return value > threshold
}

// applyThreshold 将阈值添加到由 metricSelector 指定指标的数据项中，
// 并验证该指标值是否在阈值范围内。
func applyThreshold(items []DataItem, threshold float64, metricSelector thresholdMetricSelector) error {
	if threshold == 0 {
		return nil
	}
	dataBucket := metricSelector.DataBucket
	var errs []error
	for _, item := range items {
		if item.Labels["Metric"] != metricSelector.Name || !labelsMatch(item.Labels, metricSelector.Labels) {
			continue
		}
		thresholdItemName := dataBucket + "Threshold"
		item.Data[thresholdItemName] = threshold
		dataItem, ok := item.Data[dataBucket]
		if !ok {
			errs = append(errs, fmt.Errorf("%s: no data present for %q metric %q bucket", item.Labels["Name"], metricSelector.Name, dataBucket))
			continue
		}
		if !valueWithinThreshold(dataItem, threshold, metricSelector.ExpectLower) {
			if metricSelector.ExpectLower {
				errs = append(errs, fmt.Errorf("%s: expected %q %q to be lower: got %f, want %f", item.Labels["Name"], metricSelector.Name, dataBucket, dataItem, threshold))
			} else {
				errs = append(errs, fmt.Errorf("%s: expected %q %q to be higher: got %f, want %f", item.Labels["Name"], metricSelector.Name, dataBucket, dataItem, threshold))
			}
		}
	}
	return errors.Join(errs...)
}

func checkEmptyInFlightEvents() error {
	// checkEmptyInFlightEvents 似乎会与场景完成过程发生竞态，
	// 在加快场景中指标收集速度后，这一情况开始频繁出问题。
	//
	// TODO: 当它重新可靠时再次启用。
	return nil
	// labels := append(schedframework.AllClusterEventLabels(), metrics.PodPoppedInFlightEvent)
	// for _, label := range labels {
	// 	value, err := testutil.GetGaugeMetricValue(metrics.InFlightEvents.WithLabelValues(label))
	// 	if err != nil {
	// 		return fmt.Errorf("failed to get InFlightEvents metric for label %s", label)
	// 	}
	//	if value > 0 {
	//		return fmt.Errorf("InFlightEvents for label %s should be empty, but has %v items", label, value)
	//	}
	//}
	// return nil
}

func runWorkload(tCtx ktesting.TContext, tc *testCase, w *Workload, topicName string, scheduler *scheduler.Scheduler, informerFactory informers.SharedInformerFactory, opts *schedulerPerfOptions) ([]DataItem, error) {
	b, benchmarking := tCtx.TB().(*testing.B)
	if benchmarking {
		start := time.Now()
		b.Cleanup(func() {
			duration := time.Since(start)
			// 其中包含启动和关闭时间，因此不能
			// 反映调度性能。它有助于大致了解
			// 每个工作负载整体的运行时长。
			b.ReportMetric(duration.Seconds(), "runtime_seconds")
		})
	}

	// 默认禁用吞吐量收集器中采样间隔长度的错误检查。
	// 运行基准测试时，如果采样不规律，则将其报告为测试失败。
	var throughputErrorMargin float64
	if benchmarking {
		// TODO: 为防止性能测试失败，我们增大了误差范围；如果仍不够，
		// 将来应该考虑用其他方式避免这种取巧做法。
		throughputErrorMargin = 30
	}

	// 测试所需的额外 informer。Pod informer 之前
	// 已经创建（scheduler.NewInformerFactory），并且
	// 对应的 factory 已经启动（mustSetupCluster），因此无需
	// 再次启动。
	podInformer := informerFactory.Core().V1().Pods()

	// 该函数启动的其他所有内容都会在其返回前停止。
	tCtx = tCtx.WithCancel()

	executor := WorkloadExecutor{
		scheduler:                    scheduler,
		numPodsScheduledPerNamespace: make(map[string]int),
		podInformer:                  podInformer,
		podGroupInformer:             informerFactory.Scheduling().V1beta1().PodGroups(),
		throughputErrorMargin:        throughputErrorMargin,
		testCase:                     tc,
		workload:                     w,
		topicName:                    topicName,
		opts:                         opts,
	}

	tCtx.TB().Cleanup(func() {
		tCtx.Cancel("workload is done")
		executor.wait()
	})

	for opIndex, op := range unrollWorkloadTemplate(tCtx, tc.WorkloadTemplate, w) {
		realOp, err := op.realOp.patchParams(w)
		if err != nil {
			return nil, fmt.Errorf("op %d: %w", opIndex, err)
		}
		select {
		case <-tCtx.Done():
			return nil, fmt.Errorf("op %d: %w", opIndex, context.Cause(tCtx))
		default:
		}
		err = executor.runOp(tCtx, realOp, opIndex)
		if err != nil {
			return nil, fmt.Errorf("op %d: %w", opIndex, err)
		}
	}

	// 检查未使用的参数并提示用户
	unusedParams := w.unusedParams()
	if len(unusedParams) != 0 {
		return nil, fmt.Errorf("the parameters %v are defined on workload %s, but unused.\nPlease make sure there are no typos", unusedParams, w.Name)
	}

	// 某些测试包含不可调度的 Pod。不要在末尾添加隐式屏障，
	// 因为我们不想等待这些 Pod。
	return executor.dataItems, nil
}

func getSpecFromFile(path *string, spec interface{}) error {
	bytes, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	return yaml.UnmarshalStrict(bytes, spec)
}

func getTestCases(path string) ([]*testCase, error) {
	testCases := make([]*testCase, 0)
	if err := getSpecFromFile(&path, &testCases); err != nil {
		return nil, fmt.Errorf("parsing test cases error: %w", err)
	}
	for _, tc := range testCases {
		for _, w := range tc.Workloads {
			w.setDefaults(tc.DefaultThresholdMetricSelector)
		}
	}
	return testCases, nil
}

func validateTestCases(testCases []*testCase) error {
	if len(testCases) == 0 {
		return fmt.Errorf("no test cases defined")
	}
	testCaseUniqueNames := map[string]bool{}
	for _, tc := range testCases {
		if testCaseUniqueNames[tc.Name] {
			return fmt.Errorf("%s: name is not unique", tc.Name)
		}
		testCaseUniqueNames[tc.Name] = true
		if len(tc.Workloads) == 0 {
			return fmt.Errorf("%s: no workloads defined", tc.Name)
		}
		if err := tc.workloadNamesUnique(); err != nil {
			return err
		}
		if len(tc.WorkloadTemplate) == 0 {
			return fmt.Errorf("%s: no ops defined", tc.Name)
		}
		// 确保每个工作负载中至少有一个 collectMetrics 设为 true 的
		// CreatePods 操作，或者同时存在 startCollectingMetricsOp
		// 和 stopCollectingMetricsOp。如果不收集统计数据用于报告，
		// 运行性能基准测试还有什么意义？
		if !tc.collectsMetrics() {
			return fmt.Errorf("%s: no op in the workload template collects metrics", tc.Name)
		}
	}
	return nil
}
