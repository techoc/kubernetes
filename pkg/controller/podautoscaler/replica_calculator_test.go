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

package podautoscaler

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	autoscalingv2 "k8s.io/api/autoscaling/v2"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta/testrestmapper"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/controller"
	metricsclient "k8s.io/kubernetes/pkg/controller/podautoscaler/metrics"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/utils/ktesting"
	cmapi "k8s.io/metrics/pkg/apis/custom_metrics/v1beta2"
	emapi "k8s.io/metrics/pkg/apis/external_metrics/v1beta1"
	metricsapi "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsfake "k8s.io/metrics/pkg/client/clientset/versioned/fake"
	cmfake "k8s.io/metrics/pkg/client/custom_metrics/fake"
	emfake "k8s.io/metrics/pkg/client/external_metrics/fake"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testNamespace       = "test-namespace"
	podNamePrefix       = "test-pod"
	numContainersPerPod = 2
)

// testDeploymentRef 是测试中默认使用的对象引用，指向 apps/v1 版本的 Deployment
// "some-deployment"，供对象指标（object metric）相关场景复用。
var testDeploymentRef = &autoscalingv2.CrossVersionObjectReference{
	Kind:       "Deployment",
	APIVersion: "apps/v1",
	Name:       "some-deployment",
}

// testExternalSelector 是外部指标测试常用的标签选择器，匹配 label=value，用于构造带 selector 的 customMetric 夹具。
var testExternalSelector = &metav1.LabelSelector{MatchLabels: map[string]string{"label": "value"}}

// cpuResource 描述基于资源（CPU）的测试中，伪 Pod 客户端与指标客户端所需的
// 每容器 CPU request 以及 Pod 级别的指标数值。
type cpuResource struct {
	// requests 是每个 Pod 的 CPU request；len(requests) 即 Pod 的数量。
	requests []resource.Quantity
	// levels[i][j] 表示第 i 个 Pod 中第 j 个容器的用量；越界值
	// 用来模拟缺失指标的容器。
	levels [][]int64
	// podNames 非空时覆盖伪指标客户端在第 i 个索引处返回的 Pod 名称，
	// 可用于构造指标名称与 Pod 名称不匹配的场景。
	podNames []string
}

// customMetric 描述基于自定义/外部指标的测试中，伪指标客户端应如何响应。
// name 为指标名称，levels 为返回的数值序列。
// name 是指标名称。
type customMetric struct {
	name string
	// levels 是指标客户端返回的值，每个 Pod 一个
	// （外部指标则每个样本一个）。
	levels []int64
	// singleObject 用于 object 与 objectPerPod 指标测试。
	singleObject *autoscalingv2.CrossVersionObjectReference
	// selector 用于 external 与 externalPerPod 指标测试。
	selector *metav1.LabelSelector
}

// calcScenario 是纯粹的输入集合：描述存在哪些 Pod、它们上报的指标，以及就绪状态与阶段。
type calcScenario struct {
	currentReplicas int32

	// resource 与 metric 二者恰好设置一个。
	resource *cpuResource
	metric   *customMetric

	// container 非空时将 GetResourceReplicas 的计算限定到指定容器。
	container string

	// timestamp 是伪指标客户端为所有返回样本使用的时间戳。
	timestamp time.Time

	// 每个 Pod 的状态。非 nil 时每个切片长度至少为 currentReplicas
	// （若夹具还要描述未计入 currentReplicas 的失败/被删除 Pod，
	// 则可更长）。
	podReadiness         []v1.ConditionStatus
	podStartTime         []metav1.Time
	podPhase             []v1.PodPhase
	podDeletionTimestamp []bool
}

// cpuRequests 返回 n 个从 val 解析出的相同 CPU 资源数量。
func cpuRequests(n int, val string) []resource.Quantity {
	requests := make([]resource.Quantity, n)
	for i := range requests {
		requests[i] = resource.MustParse(val)
	}
	return requests
}

// makePodMetricLevels 返回 [Pod][容器]int64 二维数组，
// 其中 Pod i 的每个容器都上报相同用量 containerMetric[i]。
func makePodMetricLevels(containerMetric ...int64) [][]int64 {
	metrics := make([][]int64, len(containerMetric))
	for i := range containerMetric {
		metrics[i] = make([]int64, numContainersPerPod)
		for j := range numContainersPerPod {
			metrics[i][j] = containerMetric[i]
		}
	}
	return metrics
}

// assertMetricReplicas 校验基于指标计算副本数的结果：期望出错时只校验错误非空且包含期望文本；
// 否则要求无错误，并逐项比对副本数、用量与时间戳。时间戳用 Equal 比较，
// 避免 time.Time 中单调时钟或位置差异造成误判。
func assertMetricReplicas(t *testing.T, wantReplicas int32, wantUsage int64, wantTimestamp time.Time, wantErr error,
	gotReplicas int32, gotUsage int64, gotTimestamp time.Time, gotErr error) {
	t.Helper()
	if wantErr != nil {
		require.Error(t, gotErr, "there should be an error calculating the replica count")
		assert.ErrorContains(t, gotErr, wantErr.Error(), "error message should contain expected text")
		return
	}
	require.NoError(t, gotErr, "there should not have been an error calculating the replica count")
	assert.Equal(t, wantReplicas, gotReplicas, "replicas should be as expected")
	assert.Equal(t, wantUsage, gotUsage, "usage should be as expected")
	assert.True(t, wantTimestamp.Equal(gotTimestamp), "timestamp should be as expected")
}

// replicaCalcSetup 持有一个 ReplicaCalculator 及其使用的伪客户端。
// 测试通过 .calc 调用方法并检查结果。
type replicaCalcSetup struct {
	calc       *ReplicaCalculator
	ctx        context.Context
	selector   labels.Selector
	namespace  string
	tolerances Tolerances
}

// resourceCase 将单个输入夹具与基于资源的副本数测试的期望输出打包。
type resourceCase struct {
	name    string
	fixture calcScenario

	targetUtilization   int32
	expectedReplicas    int32
	expectedUtilization int32
	expectedRawValue    int64
	expectedError       error

	// tolerances 非 nil 时覆盖 setup 中的默认容忍度。
	tolerances *Tolerances
}

// metricCase 将单个输入夹具与基于 Pod 指标、对象指标、外部指标的副本数测试期望输出打包。
type metricCase struct {
	name    string
	fixture calcScenario

	targetUsage      int64
	expectedReplicas int32
	expectedUsage    int64
	expectedError    error

	// tolerances 非 nil 时覆盖 setup 中的默认容忍度。
	tolerances *Tolerances
}

// perPodMetricCase 将单个输入夹具与基于每 Pod 对象指标、每 Pod 外部指标的测试期望输出打包。
type perPodMetricCase struct {
	name    string
	fixture calcScenario

	perPodTargetUsage int64
	expectedReplicas  int32
	expectedUsage     int64
	expectedError     error

	// tolerances 非 nil 时覆盖 setup 中的默认容忍度。
	tolerances *Tolerances
}

// newReplicaCalcSetup 创建伪 Pod、资源指标、自定义指标与外部指标客户端，
// 并将它们注入 ReplicaCalculator；返回的 setup 包含计算器、上下文、选择器、命名空间与默认容忍度。
func newReplicaCalcSetup(t *testing.T, f *calcScenario) *replicaCalcSetup {
	t.Helper()

	tCtx := ktesting.Init(t)

	podClient := newFakePodClient(f)
	metricsClient := metricsclient.NewRESTMetricsClient(
		newFakeResourceMetricsClient(f).MetricsV1beta1(),
		newFakeCustomMetricsClient(t, f),
		newFakeExternalMetricsClient(t, f),
	)

	informerFactory := informers.NewSharedInformerFactory(podClient, controller.NoResyncPeriodFunc())
	informer := informerFactory.Core().V1().Pods()

	calc := NewReplicaCalculator(metricsClient, informer.Lister(),
		defaultTestingCPUInitializationPeriod, defaultTestingDelayOfInitialReadinessStatus)

	// 必须先启动 informer，Lister 才能看到伪客户端里生成的 Pod。
	informerFactory.Start(tCtx.Done())

	// 给缓存同步设置超时上限，避免夹具异常时测试永久阻塞。
	syncCtx, cancel := context.WithTimeout(tCtx, 10*time.Second)
	defer cancel()
	if !cache.WaitForNamedCacheSyncWithContext(syncCtx, informer.Informer().HasSynced) {
		tCtx.Fatal("Failed to sync informer cache within the 10s timeout")
	}

	// 选择器必须与 buildFakePod 中给 Pod 打的标签保持一致。
	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: map[string]string{"name": podNamePrefix},
	})
	require.NoError(t, err, "test pod label selector should parse")

	return &replicaCalcSetup{
		calc:       calc,
		ctx:        tCtx,
		selector:   selector,
		namespace:  testNamespace,
		tolerances: Tolerances{defaultTestingTolerance, defaultTestingTolerance},
	}
}

// newFakePodClient 返回一个伪 Kubernetes clientset，其 list pods reactor 按夹具生成 Pod 列表。
func newFakePodClient(f *calcScenario) *fake.Clientset {
	fakeClient := &fake.Clientset{}
	fakeClient.AddReactor("list", "pods", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		obj := &v1.PodList{}
		podsCount := int(f.currentReplicas)
		// 失败 Pod 不计入 currentReplicas，但仍要列出以验证计算器会忽略它们。
		if f.podPhase != nil && len(f.podPhase) > podsCount {
			podsCount = len(f.podPhase)
		}
		for i := 0; i < podsCount; i++ {
			obj.Items = append(obj.Items, buildFakePod(f, i))
		}
		return true, obj, nil
	})
	return fakeClient
}

// buildFakePod 根据夹具与 Pod 索引 i 构造伪 Pod：索引决定名称，readiness/startTime/phase/deletionTimestamp/request 由夹具覆盖默认值。
func buildFakePod(f *calcScenario, i int) v1.Pod {
	podReadiness := v1.ConditionTrue
	if f.podReadiness != nil && i < len(f.podReadiness) {
		podReadiness = f.podReadiness[i]
	}
	var podStartTime metav1.Time
	if f.podStartTime != nil {
		podStartTime = f.podStartTime[i]
	}
	podPhase := v1.PodRunning
	if f.podPhase != nil {
		podPhase = f.podPhase[i]
	}
	deleted := f.podDeletionTimestamp != nil && f.podDeletionTimestamp[i]

	pod := v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", podNamePrefix, i),
			Namespace: testNamespace,
			Labels:    map[string]string{"name": podNamePrefix},
		},
		Status: v1.PodStatus{
			Phase:     podPhase,
			StartTime: &podStartTime,
			Conditions: []v1.PodCondition{
				{Type: v1.PodReady, Status: podReadiness},
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{{Name: "container1"}, {Name: "container2"}},
		},
	}
	// 即使处于 Running，也要能被删除时间戳标记，以验证忽略逻辑。
	if deleted {
		pod.DeletionTimestamp = &metav1.Time{Time: time.Now()}
	}
	// 仅当夹具声明了 CPU 资源时才给两个容器设置 request。
	if f.resource != nil && i < len(f.resource.requests) {
		req := v1.ResourceRequirements{
			Requests: v1.ResourceList{v1.ResourceCPU: f.resource.requests[i]},
		}
		pod.Spec.Containers[0].Resources = req
		pod.Spec.Containers[1].Resources = req
	}
	return pod
}

// newFakeResourceMetricsClient 返回一个伪 metrics.k8s.io 客户端，按夹具生成 PodMetricsList。
func newFakeResourceMetricsClient(f *calcScenario) *metricsfake.Clientset {
	fakeMetricsClient := &metricsfake.Clientset{}
	fakeMetricsClient.AddReactor("list", "pods", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		if f.resource == nil {
			return true, nil, fmt.Errorf("no pod resource metrics specified in test client")
		}
		metrics := &metricsapi.PodMetricsList{}
		for i, resValue := range f.resource.levels {
			podName := fmt.Sprintf("%s-%d", podNamePrefix, i)
			if len(f.resource.podNames) > i {
				podName = f.resource.podNames[i]
			}
			// list reactor 会替我们做标签选择器过滤，因此返回的 Pod 指标
			// 必须带有与选择器匹配的标签。
			podMetric := metricsapi.PodMetrics{
				ObjectMeta: metav1.ObjectMeta{
					Name:      podName,
					Namespace: testNamespace,
					Labels:    map[string]string{"name": podNamePrefix},
				},
				Timestamp:  metav1.Time{Time: f.timestamp},
				Window:     metav1.Duration{Duration: time.Minute},
				Containers: make([]metricsapi.ContainerMetrics, numContainersPerPod),
			}
			for j, m := range resValue {
				podMetric.Containers[j] = metricsapi.ContainerMetrics{
					Name: fmt.Sprintf("container%v", j+1),
					Usage: v1.ResourceList{
						v1.ResourceCPU: *resource.NewMilliQuantity(m, resource.DecimalSI),
					},
				}
			}
			metrics.Items = append(metrics.Items, podMetric)
		}
		return true, metrics, nil
	})
	return fakeMetricsClient
}

// newFakeCustomMetricsClient 返回一个伪自定义指标客户端：名称 "*" 返回多 Pod 指标，
// 具体对象名返回单对象指标，并校验指标名、资源类型与对象名是否符合夹具。
func newFakeCustomMetricsClient(t *testing.T, f *calcScenario) *cmfake.FakeCustomMetricsClient {
	fakeCMClient := &cmfake.FakeCustomMetricsClient{}
	// get reactor 同时覆盖多 Pod 查询（名称 "*"）与单对象查询。
	fakeCMClient.AddReactor("get", "*", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		getForAction, wasGetFor := action.(cmfake.GetForAction)
		if !wasGetFor {
			return true, nil, fmt.Errorf("expected a get-for action, got %v instead", action)
		}
		if f.metric == nil {
			return true, nil, fmt.Errorf("no custom metrics specified in test client")
		}
		assert.Equal(t, f.metric.name, getForAction.GetMetricName(), "the metric requested should have matched the one specified")

		if getForAction.GetName() == "*" {
			// 多 Pod 查询：名称 "*" 表示返回所有 Pod 的指标。
			assert.Equal(t, "pods", getForAction.GetResource().Resource, "the type of object that we requested multiple metrics for should have been pods")
			metrics := cmapi.MetricValueList{}
			for i, level := range f.metric.levels {
				metrics.Items = append(metrics.Items, cmapi.MetricValue{
					DescribedObject: v1.ObjectReference{
						Kind:      "Pod",
						Name:      fmt.Sprintf("%s-%d", podNamePrefix, i),
						Namespace: testNamespace,
					},
					Timestamp: metav1.Time{Time: f.timestamp},
					Metric:    cmapi.MetricIdentifier{Name: f.metric.name},
					Value:     *resource.NewMilliQuantity(level, resource.DecimalSI),
				})
			}
			return true, &metrics, nil
		}

		// 单对象查询：校验 GroupKind 映射的资源与对象名。
		name := getForAction.GetName()
		assert.NotNil(t, f.metric.singleObject, "should have only requested a single-object metric when calling GetObjectMetricReplicas")
		mapper := testrestmapper.TestOnlyStaticRESTMapper(legacyscheme.Scheme)
		gk := schema.FromAPIVersionAndKind(f.metric.singleObject.APIVersion, f.metric.singleObject.Kind).GroupKind()
		mapping, err := mapper.RESTMapping(gk)
		if err != nil {
			return true, nil, fmt.Errorf("unable to get mapping for %s: %w", gk.String(), err)
		}
		assert.Equal(t, mapping.Resource.GroupResource().String(), getForAction.GetResource().Resource, "should have requested metrics for the resource matching the GroupKind passed in")
		assert.Equal(t, f.metric.singleObject.Name, name, "should have requested metrics for the object matching the name passed in")

		return true, &cmapi.MetricValueList{
			Items: []cmapi.MetricValue{{
				DescribedObject: v1.ObjectReference{
					Kind:       f.metric.singleObject.Kind,
					APIVersion: f.metric.singleObject.APIVersion,
					Name:       name,
				},
				Timestamp: metav1.Time{Time: f.timestamp},
				Metric:    cmapi.MetricIdentifier{Name: f.metric.name},
				Value:     *resource.NewMilliQuantity(f.metric.levels[0], resource.DecimalSI),
			}},
		}, nil
	})
	return fakeCMClient
}

// newFakeExternalMetricsClient 返回一个伪外部指标客户端，按夹具返回外部指标值列表，
// 并校验指标名与标签选择器是否与夹具一致。
func newFakeExternalMetricsClient(t *testing.T, f *calcScenario) *emfake.FakeExternalMetricsClient {
	fakeEMClient := &emfake.FakeExternalMetricsClient{}
	// 外部指标通过 list 动作查询，需校验标签选择器与夹具一致。
	fakeEMClient.AddReactor("list", "*", func(action core.Action) (handled bool, ret runtime.Object, err error) {
		listAction, wasList := action.(core.ListAction)
		if !wasList {
			return true, nil, fmt.Errorf("expected a list-for action, got %v instead", action)
		}
		if f.metric == nil {
			return true, nil, fmt.Errorf("no external metrics specified in test client")
		}
		assert.Equal(t, f.metric.name, listAction.GetResource().Resource, "the metric requested should have matched the one specified")

		selector, err := metav1.LabelSelectorAsSelector(f.metric.selector)
		if err != nil {
			return true, nil, fmt.Errorf("failed to convert label selector specified in test client")
		}
		assert.Equal(t, selector, listAction.GetListRestrictions().Labels, "the metric selector should have matched the one specified")

		metrics := emapi.ExternalMetricValueList{}
		for _, level := range f.metric.levels {
			metrics.Items = append(metrics.Items, emapi.ExternalMetricValue{
				Timestamp:  metav1.Time{Time: f.timestamp},
				MetricName: f.metric.name,
				Value:      *resource.NewMilliQuantity(level, resource.DecimalSI),
			})
		}
		return true, &metrics, nil
	})
	return fakeEMClient
}

// assertResourceReplicas 校验 GetResourceReplicas 的结果：期望出错时只校验错误包含关系；
// 否则比对副本数、利用率、原始值与时间戳。
func assertResourceReplicas(t *testing.T, wantReplicas int32, wantUtilization int32, wantRawValue int64, wantTimestamp time.Time, wantErr error,
	gotReplicas int32, gotUtilization int32, gotRawValue int64, gotTimestamp time.Time, gotErr error) {
	t.Helper()
	if wantErr != nil {
		require.Error(t, gotErr, "there should be an error calculating the replica count")
		assert.ErrorContains(t, gotErr, wantErr.Error(), "error message should contain expected text")
		return
	}
	require.NoError(t, gotErr, "there should not have been an error calculating the replica count")
	assert.Equal(t, wantReplicas, gotReplicas, "replicas should be as expected")
	assert.Equal(t, wantUtilization, gotUtilization, "utilization should be as expected")
	assert.Equal(t, wantRawValue, gotRawValue, "raw value should be as expected")
	assert.True(t, wantTimestamp.Equal(gotTimestamp), "timestamp should be as expected")
}

// TestReplicaCalcResourceScale 覆盖 GetResourceReplicas 的扩容、缩容与不伸缩场景：
// 包括指定容器、未就绪/刚启动（CPU 过热）/失败/正在删除的 Pod 被排除或降权，
// 以及“仍在运行但已标记删除”的 Pod 必须被忽略的回归用例。
func TestReplicaCalcResourceScale(t *testing.T) {
	testCases := []resourceCase{
		{
			name: "scale up",
			fixture: calcScenario{
				currentReplicas: 3,
				resource: &cpuResource{
					requests: cpuRequests(3, "1.0"),
					levels:   makePodMetricLevels(300, 500, 700),
				},
			},
			targetUtilization:   30,
			expectedReplicas:    5,
			expectedUtilization: 50,
			expectedRawValue:    numContainersPerPod * 500,
		},
		{
			// 只统计指定容器的用量，另一容器的高值不应影响结果。
			name: "scale up: container metric",
			fixture: calcScenario{
				currentReplicas: 3,
				container:       "container2",
				resource: &cpuResource{
					requests: cpuRequests(3, "1.0"),
					levels:   [][]int64{{1000, 300}, {1000, 500}, {1000, 700}},
				},
			},
			targetUtilization:   30,
			expectedReplicas:    5,
			expectedUtilization: 50,
			expectedRawValue:    500,
		},
		{
			name: "scale up: unready pod scales less",
			fixture: calcScenario{
				currentReplicas: 3,
				podReadiness:    []v1.ConditionStatus{v1.ConditionFalse, v1.ConditionTrue, v1.ConditionTrue},
				resource: &cpuResource{
					requests: cpuRequests(3, "1.0"),
					levels:   makePodMetricLevels(300, 500, 700),
				},
			},
			targetUtilization:   30,
			expectedReplicas:    4,
			expectedUtilization: 60,
			expectedRawValue:    numContainersPerPod * 600,
		},
		{
			name: "scale up: hot-CPU container scales less",
			fixture: calcScenario{
				currentReplicas: 3,
				podStartTime:    []metav1.Time{hotCPUCreationTime(), coolCPUCreationTime(), coolCPUCreationTime()},
				container:       "container2",
				resource: &cpuResource{
					requests: cpuRequests(3, "1.0"),
					levels:   [][]int64{{0, 300}, {0, 500}, {0, 700}},
				},
			},
			targetUtilization:   30,
			expectedReplicas:    4,
			expectedUtilization: 60,
			expectedRawValue:    600,
		},
		{
			name: "scale up: hot-CPU pod scales less",
			fixture: calcScenario{
				currentReplicas: 3,
				podStartTime:    []metav1.Time{hotCPUCreationTime(), coolCPUCreationTime(), coolCPUCreationTime()},
				resource: &cpuResource{
					requests: cpuRequests(3, "1.0"),
					levels:   makePodMetricLevels(300, 500, 700),
				},
			},
			targetUtilization:   30,
			expectedReplicas:    4,
			expectedUtilization: 60,
			expectedRawValue:    numContainersPerPod * 600,
		},
		{
			// 未就绪 Pod 的指标会被忽略，仅用就绪 Pod 计算。
			name: "no scale: unready pods ignored",
			fixture: calcScenario{
				currentReplicas: 3,
				podReadiness:    []v1.ConditionStatus{v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse},
				resource: &cpuResource{
					requests: cpuRequests(3, "1.0"),
					levels:   makePodMetricLevels(400, 500, 700),
				},
			},
			targetUtilization:   30,
			expectedReplicas:    3,
			expectedUtilization: 40,
			expectedRawValue:    numContainersPerPod * 400,
		},
		{
			name: "no scale: hot-CPU pods ignored",
			fixture: calcScenario{
				currentReplicas: 3,
				podReadiness:    []v1.ConditionStatus{v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse},
				podStartTime:    []metav1.Time{coolCPUCreationTime(), hotCPUCreationTime(), hotCPUCreationTime()},
				resource: &cpuResource{
					requests: cpuRequests(3, "1.0"),
					levels:   makePodMetricLevels(400, 500, 700),
				},
			},
			targetUtilization:   30,
			expectedReplicas:    3,
			expectedUtilization: 40,
			expectedRawValue:    numContainersPerPod * 400,
		},
		{
			// 失败 Pod 不计入 currentReplicas，但其指标仍会出现在 API 返回中。
			name: "scale up: failed pods ignored",
			fixture: calcScenario{
				currentReplicas: 2,
				podReadiness:    []v1.ConditionStatus{v1.ConditionTrue, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse},
				podPhase:        []v1.PodPhase{v1.PodRunning, v1.PodRunning, v1.PodFailed, v1.PodFailed},
				resource: &cpuResource{
					requests: cpuRequests(4, "1.0"),
					levels:   makePodMetricLevels(500, 700),
				},
			},
			targetUtilization:   30,
			expectedReplicas:    4,
			expectedUtilization: 60,
			expectedRawValue:    numContainersPerPod * 600,
		},
		{
			name: "scale up: failed pods ignored with missing container metric",
			fixture: calcScenario{
				currentReplicas: 2,
				podReadiness:    []v1.ConditionStatus{v1.ConditionTrue, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse},
				podPhase:        []v1.PodPhase{v1.PodRunning, v1.PodRunning, v1.PodFailed, v1.PodFailed},
				container:       "container2",
				resource: &cpuResource{
					requests: cpuRequests(4, "1.0"),
					levels:   [][]int64{{1000, 500}, {9000, 700}, {1000}, {9000}},
				},
			},
			targetUtilization:   30,
			expectedReplicas:    4,
			expectedUtilization: 60,
			expectedRawValue:    600,
		},
		{
			name: "scale up: container metrics with failed pods ignored",
			fixture: calcScenario{
				currentReplicas: 2,
				podReadiness:    []v1.ConditionStatus{v1.ConditionTrue, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse},
				podPhase:        []v1.PodPhase{v1.PodRunning, v1.PodRunning, v1.PodFailed, v1.PodFailed},
				container:       "container2",
				resource: &cpuResource{
					requests: cpuRequests(4, "1.0"),
					levels:   [][]int64{{1000, 500}, {9000, 700}},
				},
			},
			targetUtilization:   30,
			expectedReplicas:    4,
			expectedUtilization: 60,
			expectedRawValue:    600,
		},
		{
			name: "scale up: pods being deleted are ignored",
			fixture: calcScenario{
				currentReplicas:      2,
				podReadiness:         []v1.ConditionStatus{v1.ConditionTrue, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse},
				podPhase:             []v1.PodPhase{v1.PodRunning, v1.PodRunning, v1.PodRunning, v1.PodRunning},
				podDeletionTimestamp: []bool{false, false, true, true},
				resource: &cpuResource{
					requests: cpuRequests(4, "1.0"),
					levels:   makePodMetricLevels(500, 700),
				},
			},
			targetUtilization:   30,
			expectedReplicas:    4,
			expectedUtilization: 60,
			expectedRawValue:    numContainersPerPod * 600,
		},
		{
			name: "scale up: container metrics with pods being deleted ignored",
			fixture: calcScenario{
				currentReplicas:      2,
				podReadiness:         []v1.ConditionStatus{v1.ConditionTrue, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse},
				podPhase:             []v1.PodPhase{v1.PodRunning, v1.PodRunning, v1.PodRunning, v1.PodRunning},
				podDeletionTimestamp: []bool{false, false, true, true},
				container:            "container1",
				resource: &cpuResource{
					requests: cpuRequests(4, "1.0"),
					levels:   makePodMetricLevels(500, 700), // TODO: 该用例本身有问题，只是因缺失指标才碰巧通过。
				},
			},
			targetUtilization:   30,
			expectedReplicas:    4,
			expectedUtilization: 60,
			expectedRawValue:    600,
		},
		{
			// 5 个 Pod 平均用量低于目标，应缩容到 3 个。
			name: "scale down",
			fixture: calcScenario{
				currentReplicas: 5,
				resource: &cpuResource{
					requests: cpuRequests(5, "1.0"),
					levels:   makePodMetricLevels(100, 300, 500, 250, 250),
				},
			},
			targetUtilization:   50,
			expectedReplicas:    3,
			expectedUtilization: 28,
			expectedRawValue:    numContainersPerPod * 280,
		},
		{
			name: "scale down: container metric",
			fixture: calcScenario{
				currentReplicas: 5,
				container:       "container2",
				resource: &cpuResource{
					requests: cpuRequests(5, "1.0"),
					levels:   [][]int64{{1000, 100}, {1000, 300}, {1000, 500}, {1000, 250}, {1000, 250}},
				},
			},
			targetUtilization:   50,
			expectedReplicas:    3,
			expectedUtilization: 28,
			expectedRawValue:    280,
		},
		{
			name: "scale down: exclude unready pods",
			fixture: calcScenario{
				currentReplicas: 5,
				podReadiness:    []v1.ConditionStatus{v1.ConditionTrue, v1.ConditionTrue, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse},
				resource: &cpuResource{
					requests: cpuRequests(5, "1.0"),
					levels:   makePodMetricLevels(100, 300, 500, 250, 250),
				},
			},
			targetUtilization:   50,
			expectedReplicas:    2,
			expectedUtilization: 30,
			expectedRawValue:    numContainersPerPod * 300,
		},
		{
			name: "scale down: container metric excludes unready pods",
			fixture: calcScenario{
				currentReplicas: 5,
				podReadiness:    []v1.ConditionStatus{v1.ConditionTrue, v1.ConditionTrue, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse},
				container:       "container2",
				resource: &cpuResource{
					requests: cpuRequests(5, "1.0"),
					levels:   [][]int64{{1000, 100}, {1000, 300}, {1000, 500}, {1000, 250}, {1000, 250}},
				},
			},
			targetUtilization:   50,
			expectedReplicas:    2,
			expectedUtilization: 30,
			expectedRawValue:    300,
		},
		{
			// Pending Pod 没有 CPU 用量，不应拉低平均值导致过度缩容。
			name: "scale down: exclude unscheduled pods",
			fixture: calcScenario{
				currentReplicas: 5,
				podReadiness:    []v1.ConditionStatus{v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse},
				podPhase:        []v1.PodPhase{v1.PodRunning, v1.PodPending, v1.PodPending, v1.PodPending, v1.PodPending},
				resource: &cpuResource{
					requests: cpuRequests(5, "1.0"),
					levels:   makePodMetricLevels(100),
				},
			},
			targetUtilization:   50,
			expectedReplicas:    1,
			expectedUtilization: 10,
			expectedRawValue:    numContainersPerPod * 100,
		},
		{
			name: "scale down: container metric excludes unscheduled pods",
			fixture: calcScenario{
				currentReplicas: 5,
				podReadiness:    []v1.ConditionStatus{v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse, v1.ConditionFalse},
				podPhase:        []v1.PodPhase{v1.PodRunning, v1.PodPending, v1.PodPending, v1.PodPending, v1.PodPending},
				container:       "container2",
				resource: &cpuResource{
					requests: cpuRequests(5, "1.0"),
					levels:   [][]int64{{1000, 100}, {1000, 300}, {1000, 500}, {1000, 250}, {1000, 250}},
				},
			},
			targetUtilization:   50,
			expectedReplicas:    1,
			expectedUtilization: 10,
			expectedRawValue:    100,
		},
		{
			// 刚启动的 Pod CPU 指标不稳定，应排除以免错误缩容。
			name: "scale down: ignore hot-CPU pods",
			fixture: calcScenario{
				currentReplicas: 5,
				podStartTime:    []metav1.Time{coolCPUCreationTime(), coolCPUCreationTime(), coolCPUCreationTime(), hotCPUCreationTime(), hotCPUCreationTime()},
				resource: &cpuResource{
					requests: cpuRequests(5, "1.0"),
					levels:   makePodMetricLevels(100, 300, 500, 250, 250),
				},
			},
			targetUtilization:   50,
			expectedReplicas:    2,
			expectedUtilization: 30,
			expectedRawValue:    numContainersPerPod * 300,
		},
		{
			name: "scale down: container metric ignores hot-CPU pods",
			fixture: calcScenario{
				currentReplicas: 5,
				podStartTime:    []metav1.Time{coolCPUCreationTime(), coolCPUCreationTime(), coolCPUCreationTime(), hotCPUCreationTime(), hotCPUCreationTime()},
				container:       "container2",
				resource: &cpuResource{
					requests: cpuRequests(5, "1.0"),
					levels:   [][]int64{{1000, 100}, {1000, 300}, {1000, 500}, {1000, 1000}, {1000, 1000}},
				},
			},
			targetUtilization:   50,
			expectedReplicas:    2,
			expectedUtilization: 30,
			expectedRawValue:    300,
		},
		{
			name: "scale down: failed pods ignored",
			fixture: calcScenario{
				currentReplicas: 5,
				podReadiness:    []v1.ConditionStatus{v1.ConditionTrue, v1.ConditionTrue, v1.ConditionTrue, v1.ConditionTrue, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse},
				podPhase:        []v1.PodPhase{v1.PodRunning, v1.PodRunning, v1.PodRunning, v1.PodRunning, v1.PodRunning, v1.PodFailed, v1.PodFailed},
				resource: &cpuResource{
					requests: cpuRequests(7, "1.0"),
					levels:   makePodMetricLevels(100, 300, 500, 250, 250),
				},
			},
			targetUtilization:   50,
			expectedReplicas:    3,
			expectedUtilization: 28,
			expectedRawValue:    numContainersPerPod * 280,
		},
		{
			name: "scale down: container metric with failed pods ignored",
			fixture: calcScenario{
				currentReplicas: 5,
				podReadiness:    []v1.ConditionStatus{v1.ConditionTrue, v1.ConditionTrue, v1.ConditionTrue, v1.ConditionTrue, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse},
				podPhase:        []v1.PodPhase{v1.PodRunning, v1.PodRunning, v1.PodRunning, v1.PodRunning, v1.PodRunning, v1.PodFailed, v1.PodFailed},
				container:       "container2",
				resource: &cpuResource{
					requests: cpuRequests(7, "1.0"),
					levels:   [][]int64{{1000, 100}, {1000, 300}, {1000, 500}, {1000, 250}, {1000, 250}}, // TODO: 该用例本身有问题。
				},
			},
			targetUtilization:   50,
			expectedReplicas:    3,
			expectedUtilization: 28,
			expectedRawValue:    280,
		},
		{
			name: "scale down: pods being deleted are ignored",
			fixture: calcScenario{
				currentReplicas:      5,
				podReadiness:         []v1.ConditionStatus{v1.ConditionTrue, v1.ConditionTrue, v1.ConditionTrue, v1.ConditionTrue, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse},
				podPhase:             []v1.PodPhase{v1.PodRunning, v1.PodRunning, v1.PodRunning, v1.PodRunning, v1.PodRunning, v1.PodRunning, v1.PodRunning},
				podDeletionTimestamp: []bool{false, false, false, false, false, true, true},
				resource: &cpuResource{
					requests: cpuRequests(7, "1.0"),
					levels:   makePodMetricLevels(100, 300, 500, 250, 250),
				},
			},
			targetUtilization:   50,
			expectedReplicas:    3,
			expectedUtilization: 28,
			expectedRawValue:    numContainersPerPod * 280,
		},
		{
			// 针对 https://github.com/kubernetes/kubernetes/issues/83561 的回归测试。
			name: "scale down: still-running pods being deleted are ignored",
			fixture: calcScenario{
				currentReplicas:      5,
				podReadiness:         []v1.ConditionStatus{v1.ConditionTrue, v1.ConditionTrue, v1.ConditionTrue, v1.ConditionTrue, v1.ConditionTrue, v1.ConditionFalse, v1.ConditionFalse},
				podPhase:             []v1.PodPhase{v1.PodRunning, v1.PodRunning, v1.PodRunning, v1.PodRunning, v1.PodRunning, v1.PodRunning, v1.PodRunning},
				podDeletionTimestamp: []bool{false, false, false, false, false, true, true},
				container:            "container2",
				resource: &cpuResource{
					requests: cpuRequests(7, "1.0"),
					levels:   [][]int64{{1000, 100}, {1000, 300}, {1000, 500}, {1000, 250}, {1000, 250}},
				},
			},
			targetUtilization:   50,
			expectedReplicas:    3,
			expectedUtilization: 28,
			expectedRawValue:    280,
		},
	}
	// 每个子测试重建 setup，防止 Pod 与指标夹具在用例之间串扰。
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := newReplicaCalcSetup(t, &tc.fixture)
			if tc.tolerances != nil {
				h.tolerances = *tc.tolerances
			}
			replicas, util, raw, ts, err := h.calc.GetResourceReplicas(
				h.ctx, tc.fixture.currentReplicas, tc.targetUtilization,
				v1.ResourceCPU, h.tolerances, h.namespace, h.selector, tc.fixture.container,
			)
			assertResourceReplicas(t,
				tc.expectedReplicas, tc.expectedUtilization, tc.expectedRawValue, tc.fixture.timestamp, tc.expectedError,
				replicas, util, raw, ts, err,
			)
		})
	}
}

// TestReplicaCalcExternalPerPodMetric 覆盖 GetExternalPerPodMetricReplicas 的扩容、缩容、
// 默认与可配置容忍度，以及整数溢出：副本数溢出时饱和到 MaxInt32，目标值极大时保持副本数不变。
func TestReplicaCalcExternalPerPodMetric(t *testing.T) {
	// 构造带外部选择器、名为 qps 的外部每 Pod 指标夹具。
	externalPerPodMetric := func(levels ...int64) *customMetric {
		return &customMetric{name: "qps", levels: levels, selector: testExternalSelector}
	}

	cases := []perPodMetricCase{
		{
			name: "replica count overflow saturates to MaxInt32",
			fixture: calcScenario{
				currentReplicas: 1,
				metric:          externalPerPodMetric(math.MaxInt64),
			},
			// 目标为 1:1（指标值与 Pod 数之比），放大副本数以触发溢出。
			perPodTargetUsage: 1,
			expectedReplicas:  math.MaxInt32,
			expectedUsage:     math.MaxInt64,
		},
		{
			name: "usage overflow with huge target leaves replicas unchanged",
			fixture: calcScenario{
				currentReplicas: 1,
				metric:          externalPerPodMetric(math.MaxInt64),
			},
			// 目标为高值:1，使副本数计算结果很小，从而让用量计算溢出。
			perPodTargetUsage: math.MaxInt64,
			expectedReplicas:  1,
			expectedUsage:     math.MaxInt64,
		},
		{
			name: "scale up",
			fixture: calcScenario{
				currentReplicas: 3,
				metric:          externalPerPodMetric(8600),
			},
			perPodTargetUsage: 2150,
			expectedReplicas:  4,
			expectedUsage:     2867,
		},
		{
			name: "scale down",
			fixture: calcScenario{
				currentReplicas: 5,
				metric:          externalPerPodMetric(8600),
			},
			perPodTargetUsage: 2867,
			expectedReplicas:  3,
			expectedUsage:     1720,
		},
		{
			name: "within default tolerance",
			fixture: calcScenario{
				currentReplicas: 3,
				metric:          externalPerPodMetric(8600),
			},
			perPodTargetUsage: 2900,
			expectedReplicas:  3,
			expectedUsage:     2867,
		},
		{
			name:       "outside configurable 1% tolerance",
			tolerances: &Tolerances{defaultTestingTolerance, .01},
			fixture: calcScenario{
				currentReplicas: 3,
				metric:          externalPerPodMetric(8600),
			},
			perPodTargetUsage: 2800,
			expectedReplicas:  4,
			expectedUsage:     2867,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// 每个子测试重建 setup，防止伪客户端状态在用例间泄漏。
			h := newReplicaCalcSetup(t, &tc.fixture)
			if tc.tolerances != nil {
				h.tolerances = *tc.tolerances
			}
			replicas, usage, ts, err := h.calc.GetExternalPerPodMetricReplicas(
				tc.fixture.currentReplicas, tc.perPodTargetUsage, tc.fixture.metric.name,
				h.tolerances, h.namespace, tc.fixture.metric.selector,
			)
			assertMetricReplicas(t,
				tc.expectedReplicas, tc.expectedUsage, tc.fixture.timestamp, tc.expectedError,
				replicas, usage, ts, err,
			)
		})
	}
}

// TestReplicaCalcPodMetric 覆盖 GetMetricReplicas 的扩容、缩容、容忍度与缺失指标，
// 以及未就绪/CPU 过热 Pod 对计算的影响，还有滚动更新 maxSurge 时多出的 Pod 缺失指标的场景。
func TestReplicaCalcPodMetric(t *testing.T) {
	// 构造名为 qps 的 Pod 指标夹具。
	podMetric := func(levels ...int64) *customMetric {
		return &customMetric{name: "qps", levels: levels}
	}
	testCases := []metricCase{
		{
			name: "scale up",
			fixture: calcScenario{
				currentReplicas: 3,
				metric:          podMetric(20000, 10000, 30000),
			},
			targetUsage:      15000,
			expectedReplicas: 4,
			expectedUsage:    20000,
		},
		{
			name: "scale up: unready hot-CPU pod scales less",
			fixture: calcScenario{
				currentReplicas: 3,
				podReadiness:    []v1.ConditionStatus{v1.ConditionTrue, v1.ConditionTrue, v1.ConditionFalse},
				podStartTime:    []metav1.Time{coolCPUCreationTime(), coolCPUCreationTime(), hotCPUCreationTime()},
				metric:          podMetric(50000, 10000, 30000),
			},
			targetUsage:      15000,
			expectedReplicas: 6,
			expectedUsage:    30000,
		},
		{
			name: "scale up: unready hot-CPU pods avoid scale-down",
			fixture: calcScenario{
				currentReplicas: 3,
				podReadiness:    []v1.ConditionStatus{v1.ConditionFalse, v1.ConditionTrue, v1.ConditionFalse},
				podStartTime:    []metav1.Time{hotCPUCreationTime(), coolCPUCreationTime(), hotCPUCreationTime()},
				metric:          podMetric(50000, 15000, 30000),
			},
			targetUsage:      15000,
			expectedReplicas: 7,
			expectedUsage:    31666,
		},
		{
			name: "scale down",
			fixture: calcScenario{
				currentReplicas: 5,
				metric:          podMetric(12000, 12000, 12000, 12000, 12000),
			},
			targetUsage:      20000,
			expectedReplicas: 3,
			expectedUsage:    12000,
		},
		{
			name: "within default tolerance",
			fixture: calcScenario{
				currentReplicas: 3,
				metric:          podMetric(20000, 21000, 21000),
			},
			targetUsage:      20000,
			expectedReplicas: 3,
			expectedUsage:    20666,
		},
		{
			name:       "outside configurable 1% tolerance",
			tolerances: &Tolerances{defaultTestingTolerance, .01},
			fixture: calcScenario{
				currentReplicas: 3,
				metric:          podMetric(20000, 21000, 21000),
			},
			targetUsage:      20000,
			expectedReplicas: 4,
			expectedUsage:    20666,
		},
		{
			// 部分 Pod 缺失指标时不修改副本数，避免误缩容。
			name: "no change: missing metrics",
			fixture: calcScenario{
				currentReplicas: 5,
				metric:          podMetric(20000, 19000, 21000),
			},
			targetUsage:      20000,
			expectedReplicas: 5,
			expectedUsage:    20000,
		},
		{
			// maxSurge 产生的额外 Pod 没有指标，不应导致副本数变化。
			name: "rolling update with maxSurge: extra pod missing metric",
			fixture: calcScenario{
				currentReplicas: 2,
				podPhase:        []v1.PodPhase{v1.PodRunning, v1.PodRunning, v1.PodRunning},
				metric:          podMetric(10000, 10000),
			},
			targetUsage:      17000,
			expectedReplicas: 2,
			expectedUsage:    10000,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 每个子测试重建 setup，避免指标与 Pod 状态在用例间串扰。
			h := newReplicaCalcSetup(t, &tc.fixture)
			if tc.tolerances != nil {
				h.tolerances = *tc.tolerances
			}
			replicas, usage, ts, err := h.calc.GetMetricReplicas(
				tc.fixture.currentReplicas, tc.targetUsage, tc.fixture.metric.name,
				h.tolerances, h.namespace, h.selector, nil,
			)
			assertMetricReplicas(t,
				tc.expectedReplicas, tc.expectedUsage, tc.fixture.timestamp, tc.expectedError,
				replicas, usage, ts, err,
			)
		})
	}
}

// TestReplicaCalcObjectMetric 覆盖 GetObjectMetricReplicas 的扩容、缩容与容忍度，
// 包括极大指标值使副本数饱和到 MaxInt32、忽略未就绪 Pod 等场景。
func TestReplicaCalcObjectMetric(t *testing.T) {
	// 构造指向默认 Deployment 的对象指标夹具。
	objectMetric := func(level int64) *customMetric {
		return &customMetric{name: "qps", levels: []int64{level}, singleObject: testDeploymentRef}
	}

	cases := []metricCase{
		{
			name: "scale up",
			fixture: calcScenario{
				currentReplicas: 3,
				metric:          objectMetric(20000),
			},
			targetUsage:      15000,
			expectedReplicas: 4,
			expectedUsage:    20000,
		},
		{
			name: "scale up: very large metric saturates replicas to MaxInt32",
			fixture: calcScenario{
				currentReplicas: 3,
				metric:          objectMetric(math.MaxInt64), // 取 MaxInt64 以构造极大的指标值。
			},
			targetUsage:      1, // 目标值取 1，强制触发大幅扩容。
			expectedReplicas: math.MaxInt32,
			expectedUsage:    math.MaxInt64,
		},
		{
			name: "scale up: ignores unready pods",
			fixture: calcScenario{
				currentReplicas: 3,
				podReadiness:    []v1.ConditionStatus{v1.ConditionFalse, v1.ConditionTrue, v1.ConditionFalse},
				metric:          objectMetric(50000),
			},
			targetUsage:      10000,
			expectedReplicas: 5,
			expectedUsage:    50000,
		},
		{
			name: "scale down",
			fixture: calcScenario{
				currentReplicas: 5,
				metric:          objectMetric(12000),
			},
			targetUsage:      20000,
			expectedReplicas: 3,
			expectedUsage:    12000,
		},
		{
			name: "within default tolerance",
			fixture: calcScenario{
				currentReplicas: 3,
				metric:          objectMetric(20666),
			},
			targetUsage:      20000,
			expectedReplicas: 3,
			expectedUsage:    20666,
		},
		{
			name:       "outside configurable 1% tolerance",
			tolerances: &Tolerances{defaultTestingTolerance, .01},
			fixture: calcScenario{
				currentReplicas: 3,
				metric:          objectMetric(20666),
			},
			targetUsage:      20000,
			expectedReplicas: 4,
			expectedUsage:    20666,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// 每个子测试重建 setup，保证对象指标查询互不干扰。
			h := newReplicaCalcSetup(t, &tc.fixture)
			if tc.tolerances != nil {
				h.tolerances = *tc.tolerances
			}
			replicas, usage, ts, err := h.calc.GetObjectMetricReplicas(
				tc.fixture.currentReplicas, tc.targetUsage, tc.fixture.metric.name,
				h.tolerances, h.namespace, tc.fixture.metric.singleObject, h.selector, nil,
			)
			assertMetricReplicas(t,
				tc.expectedReplicas, tc.expectedUsage, tc.fixture.timestamp, tc.expectedError,
				replicas, usage, ts, err,
			)
		})
	}
}

// TestReplicaCalcObjectPerPodMetric 覆盖 GetObjectPerPodMetricReplicas 的扩容、缩容与容忍度行为。
func TestReplicaCalcObjectPerPodMetric(t *testing.T) {
	// 构造指向默认 Deployment 的每 Pod 对象指标夹具。
	perPodMetric := func(level int64) *customMetric {
		return &customMetric{name: "qps", levels: []int64{level}, singleObject: testDeploymentRef}
	}

	cases := []perPodMetricCase{
		{
			name: "scale up",
			fixture: calcScenario{
				currentReplicas: 3,
				metric:          perPodMetric(20000),
			},
			perPodTargetUsage: 5000,
			expectedReplicas:  4,
			expectedUsage:     6667,
		},
		{
			name: "scale down",
			fixture: calcScenario{
				currentReplicas: 5,
				metric:          perPodMetric(6000),
			},
			perPodTargetUsage: 2000,
			expectedReplicas:  3,
			expectedUsage:     1200,
		},
		{
			name: "within default tolerance",
			fixture: calcScenario{
				currentReplicas: 4,
				metric:          perPodMetric(20166),
			},
			perPodTargetUsage: 5000,
			expectedReplicas:  4,
			expectedUsage:     5042,
		},
		{
			name:       "outside configurable 1% tolerance",
			tolerances: &Tolerances{defaultTestingTolerance, .01},
			fixture: calcScenario{
				currentReplicas: 4,
				metric:          perPodMetric(20208),
			},
			perPodTargetUsage: 5000,
			expectedReplicas:  5,
			expectedUsage:     5052,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// 每个子测试重建 setup，避免夹具状态残留。
			h := newReplicaCalcSetup(t, &tc.fixture)
			if tc.tolerances != nil {
				h.tolerances = *tc.tolerances
			}
			replicas, usage, ts, err := h.calc.GetObjectPerPodMetricReplicas(
				tc.fixture.currentReplicas, tc.perPodTargetUsage, tc.fixture.metric.name,
				h.tolerances, h.namespace, tc.fixture.metric.singleObject, nil,
			)
			assertMetricReplicas(t,
				tc.expectedReplicas, tc.expectedUsage, tc.fixture.timestamp, tc.expectedError,
				replicas, usage, ts, err,
			)
		})
	}
}

// TestReplicaCalcExternalMetric 覆盖 GetExternalMetricReplicas 的扩容、缩容与容忍度，
// 包括忽略未就绪 Pod（否则会错误扩容）以及用量溢出封顶到 MaxInt64。
func TestReplicaCalcExternalMetric(t *testing.T) {
	// 构造带外部选择器、名为 qps 的外部指标夹具。
	externalMetric := func(levels ...int64) *customMetric {
		return &customMetric{name: "qps", levels: levels, selector: testExternalSelector}
	}

	cases := []metricCase{
		{
			name: "scale down: ignores unready pods (would otherwise scale up)",
			fixture: calcScenario{
				currentReplicas: 3,
				podReadiness:    []v1.ConditionStatus{v1.ConditionFalse, v1.ConditionTrue, v1.ConditionFalse},
				metric:          externalMetric(8600),
			},
			targetUsage:      4400,
			expectedReplicas: 2, // 若不忽略未就绪 Pod，此处会算成 6。
			expectedUsage:    8600,
		},
		{
			name: "scale down",
			fixture: calcScenario{
				currentReplicas: 5,
				metric:          externalMetric(8600),
			},
			targetUsage:      14334,
			expectedReplicas: 3,
			expectedUsage:    8600,
		},
		{
			name: "within default tolerance",
			fixture: calcScenario{
				currentReplicas: 3,
				metric:          externalMetric(8600),
			},
			targetUsage:      8888,
			expectedReplicas: 3,
			expectedUsage:    8600,
		},
		{
			name:       "outside configurable 1% tolerance",
			tolerances: &Tolerances{defaultTestingTolerance, .01},
			fixture: calcScenario{
				currentReplicas: 3,
				metric:          externalMetric(8900),
			},
			targetUsage:      8800,
			expectedReplicas: 4,
			expectedUsage:    8900,
		},
		{
			name: "usage overflow caps at MaxInt64",
			fixture: calcScenario{
				currentReplicas: 3,
				// 两个值相加会超出 int64 范围。
				metric: externalMetric(math.MaxInt64/2+1, math.MaxInt64/2+1),
			},
			targetUsage:      math.MaxInt64, // 目标值很高，阻止扩容。
			expectedReplicas: 3,
			expectedUsage:    math.MaxInt64, // 用量被封顶到 MaxInt64。
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// 每个子测试重建 setup，避免外部指标客户端状态残留。
			h := newReplicaCalcSetup(t, &tc.fixture)
			if tc.tolerances != nil {
				h.tolerances = *tc.tolerances
			}
			replicas, usage, ts, err := h.calc.GetExternalMetricReplicas(
				tc.fixture.currentReplicas, tc.targetUsage, tc.fixture.metric.name,
				h.tolerances, h.namespace, tc.fixture.metric.selector, h.selector,
			)
			assertMetricReplicas(t,
				tc.expectedReplicas, tc.expectedUsage, tc.fixture.timestamp, tc.expectedError,
				replicas, usage, ts, err,
			)
		})
	}
}

// TestReplicaCalcResourceTolerance 覆盖 GetResourceReplicas 的默认与可配置容忍度：
// 分别验证扩容/缩容容忍度边界内不伸缩、越界才伸缩，以及两个方向的容忍度互不干扰。
func TestReplicaCalcResourceTolerance(t *testing.T) {
	// 三个 Pod 的 request 各不相同，用于验证按各 Pod 分别计算比率，而不是假设 request 相同。
	mixedRequests := []resource.Quantity{resource.MustParse("0.9"), resource.MustParse("1.0"), resource.MustParse("1.1")}
	testCases := []resourceCase{
		{
			name: "within default tolerance",
			fixture: calcScenario{
				currentReplicas: 3,
				resource: &cpuResource{
					requests: mixedRequests,
					levels:   makePodMetricLevels(1010, 1030, 1020),
				},
			},
			targetUtilization:   100,
			expectedReplicas:    3,
			expectedUtilization: 102,
			expectedRawValue:    numContainersPerPod * 1020,
		},
		{
			// 容忍度为 0 时，任何偏差都应触发伸缩。
			name:       "outside 0% tolerance",
			tolerances: &Tolerances{0., 0.},
			fixture: calcScenario{
				currentReplicas: 3,
				resource: &cpuResource{
					requests: mixedRequests,
					levels:   makePodMetricLevels(909, 1010, 1111),
				},
			},
			targetUtilization:   100,
			expectedReplicas:    4,
			expectedUtilization: 101,
			expectedRawValue:    numContainersPerPod * 1010,
		},
		{
			name:       "within 200% scale-up tolerance",
			tolerances: &Tolerances{defaultTestingTolerance, 2.},
			fixture: calcScenario{
				currentReplicas: 3,
				resource: &cpuResource{
					requests: mixedRequests,
					levels:   makePodMetricLevels(1890, 1910, 1900),
				},
			},
			targetUtilization:   100,
			expectedReplicas:    3,
			expectedUtilization: 190,
			expectedRawValue:    numContainersPerPod * 1900,
		},
		{
			name:       "outside 8% scale-up tolerance (superfluous scale-down tolerance)",
			tolerances: &Tolerances{2., .08},
			fixture: calcScenario{
				currentReplicas: 3,
				resource: &cpuResource{
					requests: mixedRequests,
					levels:   makePodMetricLevels(1100, 1080, 1090),
				},
			},
			targetUtilization:   100,
			expectedReplicas:    4,
			expectedUtilization: 109,
			expectedRawValue:    numContainersPerPod * 1090,
		},
		{
			name:       "within 36% scale-down tolerance",
			tolerances: &Tolerances{.36, defaultTestingTolerance},
			fixture: calcScenario{
				currentReplicas: 3,
				resource: &cpuResource{
					requests: mixedRequests,
					levels:   makePodMetricLevels(660, 640, 650),
				},
			},
			targetUtilization:   100,
			expectedReplicas:    3,
			expectedUtilization: 65,
			expectedRawValue:    numContainersPerPod * 650,
		},
		{
			// 缩容容忍度略低于当前偏差，应触发缩容。
			name:       "outside 34% scale-down tolerance",
			tolerances: &Tolerances{.34, defaultTestingTolerance},
			fixture: calcScenario{
				currentReplicas: 3,
				resource: &cpuResource{
					requests: mixedRequests,
					levels:   makePodMetricLevels(660, 640, 650),
				},
			},
			targetUtilization:   100,
			expectedReplicas:    2,
			expectedUtilization: 65,
			expectedRawValue:    numContainersPerPod * 650,
		},
	}
	// 每个子测试重建 setup，确保不同容忍度设置互不干扰。
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := newReplicaCalcSetup(t, &tc.fixture)
			if tc.tolerances != nil {
				h.tolerances = *tc.tolerances
			}
			replicas, util, raw, ts, err := h.calc.GetResourceReplicas(
				h.ctx, tc.fixture.currentReplicas, tc.targetUtilization,
				v1.ResourceCPU, h.tolerances, h.namespace, h.selector, tc.fixture.container,
			)
			assertResourceReplicas(t,
				tc.expectedReplicas, tc.expectedUtilization, tc.expectedRawValue, tc.fixture.timestamp, tc.expectedError,
				replicas, util, raw, ts, err,
			)
		})
	}
}

// TestReplicaCalcResourceMissingMetrics 覆盖 GetResourceReplicas 在指标缺失、冗余、
// 部分可用时的行为及错误路径：多余指标会被计入、缺失指标导致缩容、
// 未就绪/CPU 过热 Pod 的指标缺失有特殊处理，以及名称不匹配、无指标数据、容器缺少 CPU request 等错误。
func TestReplicaCalcResourceMissingMetrics(t *testing.T) {
	testCases := []resourceCase{
		{
			// API 返回的指标数多于 Pod 数时，多余指标仍会被计入总量。
			name: "superfluous metrics counted",
			fixture: calcScenario{
				currentReplicas: 4,
				resource: &cpuResource{
					requests: cpuRequests(4, "1.0"),
					levels:   makePodMetricLevels(4000, 9500, 3000, 7000, 3200, 2000),
				},
			},
			targetUtilization:   100,
			expectedReplicas:    24,
			expectedUtilization: 587,
			expectedRawValue:    numContainersPerPod * 5875,
		},
		{
			name: "some pods missing metrics scales down",
			fixture: calcScenario{
				currentReplicas: 4,
				resource: &cpuResource{
					requests: cpuRequests(4, "1.0"),
					levels:   makePodMetricLevels(400, 95),
				},
			},
			targetUtilization:   100,
			expectedReplicas:    3,
			expectedUtilization: 24,
			expectedRawValue:    495, // 即 numContainersPerPod * 247（247 足够大时的取整结果）。
		},
		{
			name: "no change: metric equal to target",
			fixture: calcScenario{
				currentReplicas: 2,
				resource: &cpuResource{
					requests: cpuRequests(2, "1.0"),
					levels:   makePodMetricLevels(1000),
				},
			},
			targetUtilization:   100,
			expectedReplicas:    2,
			expectedUtilization: 100,
			expectedRawValue:    numContainersPerPod * 1000,
		},
		{
			name: "no change: metric above target",
			fixture: calcScenario{
				currentReplicas: 2,
				resource: &cpuResource{
					requests: cpuRequests(2, "1.0"),
					levels:   makePodMetricLevels(1900),
				},
			},
			targetUtilization:   100,
			expectedReplicas:    2,
			expectedUtilization: 190,
			expectedRawValue:    numContainersPerPod * 1900,
		},
		{
			name: "no change: metric below target",
			fixture: calcScenario{
				currentReplicas: 2,
				resource: &cpuResource{
					requests: cpuRequests(2, "1.0"),
					levels:   makePodMetricLevels(600),
				},
			},
			targetUtilization:   100,
			expectedReplicas:    2,
			expectedUtilization: 60,
			expectedRawValue:    numContainersPerPod * 600,
		},
		{
			// 未就绪 Pod 缺失指标不影响计算，直接用其余 Pod 的指标。
			name: "no change: unready pod metric missing",
			fixture: calcScenario{
				currentReplicas: 3,
				podReadiness:    []v1.ConditionStatus{v1.ConditionFalse, v1.ConditionTrue, v1.ConditionTrue},
				resource: &cpuResource{
					requests: cpuRequests(3, "1.0"),
					levels:   makePodMetricLevels(100, 450),
				},
			},
			targetUtilization:   50,
			expectedReplicas:    3,
			expectedUtilization: 45,
			expectedRawValue:    numContainersPerPod * 450,
		},
		{
			name: "no change: hot-CPU pod metric missing",
			fixture: calcScenario{
				currentReplicas: 3,
				podStartTime:    []metav1.Time{hotCPUCreationTime(), coolCPUCreationTime(), coolCPUCreationTime()},
				resource: &cpuResource{
					requests: cpuRequests(3, "1.0"),
					levels:   makePodMetricLevels(100, 450),
				},
			},
			targetUtilization:   50,
			expectedReplicas:    3,
			expectedUtilization: 45,
			expectedRawValue:    numContainersPerPod * 450,
		},
		{
			name: "scale up: unready pod metric missing",
			fixture: calcScenario{
				currentReplicas: 3,
				podReadiness:    []v1.ConditionStatus{v1.ConditionFalse, v1.ConditionTrue, v1.ConditionTrue},
				resource: &cpuResource{
					requests: cpuRequests(3, "1.0"),
					levels:   makePodMetricLevels(100, 2000),
				},
			},
			targetUtilization:   50,
			expectedReplicas:    4,
			expectedUtilization: 200,
			expectedRawValue:    numContainersPerPod * 2000,
		},
		{
			name: "scale up: hot-CPU pod metric missing",
			fixture: calcScenario{
				currentReplicas: 3,
				podReadiness:    []v1.ConditionStatus{v1.ConditionFalse, v1.ConditionTrue, v1.ConditionTrue},
				podStartTime:    []metav1.Time{hotCPUCreationTime(), coolCPUCreationTime(), coolCPUCreationTime()},
				resource: &cpuResource{
					requests: cpuRequests(3, "1.0"),
					levels:   makePodMetricLevels(100, 2000),
				},
			},
			targetUtilization:   50,
			expectedReplicas:    4,
			expectedUtilization: 200,
			expectedRawValue:    numContainersPerPod * 2000,
		},
		{
			// 指标数少于 Pod 数会被视为部分 Pod 已消失，从而缩容。
			name: "scale down: unready pod metric missing",
			fixture: calcScenario{
				currentReplicas: 4,
				podReadiness:    []v1.ConditionStatus{v1.ConditionFalse, v1.ConditionTrue, v1.ConditionTrue, v1.ConditionTrue},
				resource: &cpuResource{
					requests: cpuRequests(4, "1.0"),
					levels:   makePodMetricLevels(100, 100, 100),
				},
			},
			targetUtilization:   50,
			expectedReplicas:    3,
			expectedUtilization: 10,
			expectedRawValue:    numContainersPerPod * 100,
		},
		{
			name: "scale down: target over 100",
			fixture: calcScenario{
				currentReplicas: 4,
				podReadiness:    []v1.ConditionStatus{v1.ConditionFalse, v1.ConditionTrue, v1.ConditionTrue, v1.ConditionTrue},
				resource: &cpuResource{
					requests: []resource.Quantity{resource.MustParse("1.0"), resource.MustParse("1.0"), resource.MustParse("2.0"), resource.MustParse("2.0")},
					levels:   makePodMetricLevels(200, 100, 100),
				},
			},
			targetUtilization:   300,
			expectedReplicas:    2,
			expectedUtilization: 6,
			expectedRawValue:    numContainersPerPod * 100,
		},
		{
			name: "rolling update with maxSurge: extra pod missing metric",
			fixture: calcScenario{
				currentReplicas: 2,
				podPhase:        []v1.PodPhase{v1.PodRunning, v1.PodRunning, v1.PodRunning},
				resource: &cpuResource{
					requests: cpuRequests(3, "1.0"),
					levels:   makePodMetricLevels(100, 100),
				},
			},
			targetUtilization:   50,
			expectedReplicas:    2,
			expectedUtilization: 10,
			expectedRawValue:    numContainersPerPod * 100,
		},
		{
			// 指标名称与已知 Pod 完全不匹配时应报错。
			name: "disjoint pod and metric names",
			fixture: calcScenario{
				currentReplicas: 1,
				resource: &cpuResource{
					requests: cpuRequests(1, "1.0"),
					levels:   makePodMetricLevels(100),
					podNames: []string{"an-older-pod-name"},
				},
			},
			targetUtilization: 100,
			expectedError:     fmt.Errorf("no metrics returned matched known pods"),
		},
		{
			name: "no metrics returned from metrics API",
			fixture: calcScenario{
				currentReplicas: 4,
				resource: &cpuResource{
					requests: cpuRequests(3, "1.0"),
					levels:   makePodMetricLevels(),
				},
			},
			targetUtilization: 100,
			expectedError:     fmt.Errorf("unable to get metrics for resource cpu: no metrics returned from resource metrics API"),
		},
		{
			// 容器未声明 CPU request 时无法计算利用率，应报错。
			name: "missing CPU request on container",
			fixture: calcScenario{
				currentReplicas: 1,
				resource: &cpuResource{
					requests: []resource.Quantity{},
					levels:   makePodMetricLevels(200),
				},
			},
			targetUtilization: 100,
			expectedError:     fmt.Errorf("missing request for"),
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			h := newReplicaCalcSetup(t, &tc.fixture)
			if tc.tolerances != nil {
				h.tolerances = *tc.tolerances
			}
			replicas, util, raw, ts, err := h.calc.GetResourceReplicas(
				h.ctx, tc.fixture.currentReplicas, tc.targetUtilization,
				v1.ResourceCPU, h.tolerances, h.namespace, h.selector, tc.fixture.container,
			)
			assertResourceReplicas(t,
				tc.expectedReplicas, tc.expectedUtilization, tc.expectedRawValue, tc.fixture.timestamp, tc.expectedError,
				replicas, util, raw, ts, err,
			)
		})
	}
}

// TestReplicaCalcComputedToleranceAlgImplementation 是回归测试：根据容差值反推出触发缩容所需的
// 最小利用率增幅，验证增幅刚越过容忍度时会缩容，而非常接近但仍在容差内时不发生伸缩。
func TestReplicaCalcComputedToleranceAlgImplementation(t *testing.T) {

	startPods := int32(10)
	// 每个 Pod 使用 150 mCPU。
	totalUsedCPUOfAllPods := int64(startPods * 150)
	// 每个 Pod 最初申请的额度是实际需要的 2 倍，
	// 因此 used/requested 比率为 50%。
	totalRequestedCPUOfAllPods := int32(2 * totalUsedCPUOfAllPods)
	requestedToUsed := float64(totalRequestedCPUOfAllPods / int32(totalUsedCPUOfAllPods))
	// 将申请量均摊到 10 个 Pod，随后在上报的 request 中加入抖动。
	perPodRequested := totalRequestedCPUOfAllPods / startPods

	// 通过满足 (tolerance < 1 - resourcesUsedRatio) 强制触发一次幅度最小的伸缩。
	target := math.Abs(1/(requestedToUsed*(1-defaultTestingTolerance))) + .01
	finalCPUPercentTarget := int32(target * 100)
	resourcesUsedRatio := float64(totalUsedCPUOfAllPods) / float64(float64(totalRequestedCPUOfAllPods)*target)

	// 例如 .60 * 20，期望触发缩容。
	finalPods := int32(math.Ceil(resourcesUsedRatio * float64(startPods)))

	// 为越过容忍度，使利用率比率与 usageRatioToleranceValue 相差一个 tolerance。
	buildFixture := func() calcScenario {
		return calcScenario{
			currentReplicas: startPods,
			resource: &cpuResource{
				levels: makePodMetricLevels(
					totalUsedCPUOfAllPods/10,
					totalUsedCPUOfAllPods/10,
					totalUsedCPUOfAllPods/10,
					totalUsedCPUOfAllPods/10,
					totalUsedCPUOfAllPods/10,
					totalUsedCPUOfAllPods/10,
					totalUsedCPUOfAllPods/10,
					totalUsedCPUOfAllPods/10,
					totalUsedCPUOfAllPods/10,
					totalUsedCPUOfAllPods/10,
				),
				requests: []resource.Quantity{
					resource.MustParse(fmt.Sprint(perPodRequested+100) + "m"),
					resource.MustParse(fmt.Sprint(perPodRequested-100) + "m"),
					resource.MustParse(fmt.Sprint(perPodRequested+10) + "m"),
					resource.MustParse(fmt.Sprint(perPodRequested-10) + "m"),
					resource.MustParse(fmt.Sprint(perPodRequested+2) + "m"),
					resource.MustParse(fmt.Sprint(perPodRequested-2) + "m"),
					resource.MustParse(fmt.Sprint(perPodRequested+1) + "m"),
					resource.MustParse(fmt.Sprint(perPodRequested-1) + "m"),
					resource.MustParse(fmt.Sprint(perPodRequested) + "m"),
					resource.MustParse(fmt.Sprint(perPodRequested) + "m"),
				},
			},
		}
	}

	expectedUtilization := int32(totalUsedCPUOfAllPods*100) / totalRequestedCPUOfAllPods
	expectedRawValue := numContainersPerPod * totalUsedCPUOfAllPods / 10
	f1 := buildFixture()
	h1 := newReplicaCalcSetup(t, &f1)
	replicas, util, raw, ts, err := h1.calc.GetResourceReplicas(
		h1.ctx, f1.currentReplicas, finalCPUPercentTarget,
		v1.ResourceCPU, h1.tolerances, h1.namespace, h1.selector, f1.container,
	)
	assertResourceReplicas(t,
		finalPods, expectedUtilization, expectedRawValue, f1.timestamp, nil,
		replicas, util, raw, ts, err,
	)

	// 复用上面的夹具，现在测试“不伸缩”的情况。
	// 当利用率与容差边界非常接近但仍在容差内时，应当不发生伸缩。
	// 将增幅从 .01 收窄到 .004，使其落在容忍度之内。
	target = math.Abs(1/(requestedToUsed*(1-defaultTestingTolerance))) + .004
	finalCPUPercentTarget = int32(target * 100)

	f2 := buildFixture()
	h2 := newReplicaCalcSetup(t, &f2)
	replicas, util, raw, ts, err = h2.calc.GetResourceReplicas(
		h2.ctx, f2.currentReplicas, finalCPUPercentTarget,
		v1.ResourceCPU, h2.tolerances, h2.namespace, h2.selector, f2.container,
	)
	assertResourceReplicas(t,
		startPods, expectedUtilization, expectedRawValue, f2.timestamp, nil,
		replicas, util, raw, ts, err,
	)
}

// TestGroupPods 覆盖 groupPods 对 Pod 的四分类：就绪 Pod 计数、未就绪 Pod、
// 缺失指标 Pod 与忽略的 Pod（被删除或失败）。重点验证 CPU 指标在初始化期内对
// “新鲜指标”的要求与延迟就绪判定，以及内存资源不受该新鲜度限制。
func TestGroupPods(t *testing.T) {
	tests := []struct {
		name                string
		pods                []*v1.Pod
		metrics             metricsclient.PodMetricsInfo
		resource            v1.ResourceName
		expectReadyPodCount int
		expectUnreadyPods   sets.Set[string]
		expectMissingPods   sets.Set[string]
		expectIgnoredPods   sets.Set[string]
	}{
		{
			// 空输入应得到全空的分类结果。
			name:                "void",
			pods:                []*v1.Pod{},
			metrics:             metricsclient.PodMetricsInfo{},
			resource:            v1.ResourceCPU,
			expectReadyPodCount: 0,
			expectUnreadyPods:   sets.New[string](),
			expectMissingPods:   sets.New[string](),
			expectIgnoredPods:   sets.New[string](),
		}, {
			name: "count in a ready pod - memory",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "bentham",
					},
					Status: v1.PodStatus{
						Phase: v1.PodSucceeded,
					},
				},
			},
			metrics: metricsclient.PodMetricsInfo{
				"bentham": metricsclient.PodMetric{Value: 1, Timestamp: time.Now(), Window: time.Minute},
			},
			resource:            v1.ResourceMemory,
			expectReadyPodCount: 1,
			expectUnreadyPods:   sets.New[string](),
			expectMissingPods:   sets.New[string](),
			expectIgnoredPods:   sets.New[string](),
		}, {
			// CPU 资源下没有 Ready 条件的 Pod 视为未就绪。
			name: "unready a pod without ready condition - CPU",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "lucretius",
					},
					Status: v1.PodStatus{
						Phase: v1.PodSucceeded,
						StartTime: &metav1.Time{
							Time: time.Now(),
						},
					},
				},
			},
			metrics: metricsclient.PodMetricsInfo{
				"lucretius": metricsclient.PodMetric{Value: 1},
			},
			resource:            v1.ResourceCPU,
			expectReadyPodCount: 0,
			expectUnreadyPods:   sets.New[string]("lucretius"),
			expectMissingPods:   sets.New[string](),
			expectIgnoredPods:   sets.New[string](),
		}, {
			// 初始化期内指标窗口足够短才算“新鲜”，可计入就绪副本。
			name: "count in a ready pod with fresh metrics during initialization period - CPU",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "bentham",
					},
					Status: v1.PodStatus{
						Phase: v1.PodSucceeded,
						StartTime: &metav1.Time{
							Time: time.Now().Add(-1 * time.Minute),
						},
						Conditions: []v1.PodCondition{
							{
								Type:               v1.PodReady,
								LastTransitionTime: metav1.Time{Time: time.Now().Add(-30 * time.Second)},
								Status:             v1.ConditionTrue,
							},
						},
					},
				},
			},
			metrics: metricsclient.PodMetricsInfo{
				"bentham": metricsclient.PodMetric{Value: 1, Timestamp: time.Now(), Window: 30 * time.Second},
			},
			resource:            v1.ResourceCPU,
			expectReadyPodCount: 1,
			expectUnreadyPods:   sets.New[string](),
			expectMissingPods:   sets.New[string](),
			expectIgnoredPods:   sets.New[string](),
		}, {
			name: "unready a ready pod without fresh metrics during initialization period - CPU",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "bentham",
					},
					Status: v1.PodStatus{
						Phase: v1.PodSucceeded,
						StartTime: &metav1.Time{
							Time: time.Now().Add(-1 * time.Minute),
						},
						Conditions: []v1.PodCondition{
							{
								Type:               v1.PodReady,
								LastTransitionTime: metav1.Time{Time: time.Now().Add(-30 * time.Second)},
								Status:             v1.ConditionTrue,
							},
						},
					},
				},
			},
			metrics: metricsclient.PodMetricsInfo{
				"bentham": metricsclient.PodMetric{Value: 1, Timestamp: time.Now(), Window: 60 * time.Second},
			},
			resource:            v1.ResourceCPU,
			expectReadyPodCount: 0,
			expectUnreadyPods:   sets.New[string]("bentham"),
			expectMissingPods:   sets.New[string](),
			expectIgnoredPods:   sets.New[string](),
		}, {
			name: "unready an unready pod during initialization period - CPU",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "lucretius",
					},
					Status: v1.PodStatus{
						Phase: v1.PodSucceeded,
						StartTime: &metav1.Time{
							Time: time.Now().Add(-10 * time.Minute),
						},
						Conditions: []v1.PodCondition{
							{
								Type:               v1.PodReady,
								LastTransitionTime: metav1.Time{Time: time.Now().Add(-9*time.Minute - 54*time.Second)},
								Status:             v1.ConditionFalse,
							},
						},
					},
				},
			},
			metrics: metricsclient.PodMetricsInfo{
				"lucretius": metricsclient.PodMetric{Value: 1},
			},
			resource:            v1.ResourceCPU,
			expectReadyPodCount: 0,
			expectUnreadyPods:   sets.New[string]("lucretius"),
			expectMissingPods:   sets.New[string](),
			expectIgnoredPods:   sets.New[string](),
		}, {
			name: "count in a ready pod without fresh metrics after initialization period - CPU",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "bentham",
					},
					Status: v1.PodStatus{
						Phase: v1.PodSucceeded,
						StartTime: &metav1.Time{
							Time: time.Now().Add(-3 * time.Minute),
						},
						Conditions: []v1.PodCondition{
							{
								Type:               v1.PodReady,
								LastTransitionTime: metav1.Time{Time: time.Now().Add(-3 * time.Minute)},
								Status:             v1.ConditionTrue,
							},
						},
					},
				},
			},
			metrics: metricsclient.PodMetricsInfo{
				"bentham": metricsclient.PodMetric{Value: 1, Timestamp: time.Now().Add(-2 * time.Minute), Window: time.Minute},
			},
			resource:            v1.ResourceCPU,
			expectReadyPodCount: 1,
			expectUnreadyPods:   sets.New[string](),
			expectMissingPods:   sets.New[string](),
			expectIgnoredPods:   sets.New[string](),
		}, {
			name: "count in an unready pod that was ready after initialization period - CPU",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "lucretius",
					},
					Status: v1.PodStatus{
						Phase: v1.PodSucceeded,
						StartTime: &metav1.Time{
							Time: time.Now().Add(-10 * time.Minute),
						},
						Conditions: []v1.PodCondition{
							{
								Type:               v1.PodReady,
								LastTransitionTime: metav1.Time{Time: time.Now().Add(-9 * time.Minute)},
								Status:             v1.ConditionFalse,
							},
						},
					},
				},
			},
			metrics: metricsclient.PodMetricsInfo{
				"lucretius": metricsclient.PodMetric{Value: 1},
			},
			resource:            v1.ResourceCPU,
			expectReadyPodCount: 1,
			expectUnreadyPods:   sets.New[string](),
			expectMissingPods:   sets.New[string](),
			expectIgnoredPods:   sets.New[string](),
		}, {
			name: "unready pod that has never been ready after initialization period - CPU",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "lucretius",
					},
					Status: v1.PodStatus{
						Phase: v1.PodSucceeded,
						StartTime: &metav1.Time{
							Time: time.Now().Add(-10 * time.Minute),
						},
						Conditions: []v1.PodCondition{
							{
								Type:               v1.PodReady,
								LastTransitionTime: metav1.Time{Time: time.Now().Add(-9*time.Minute - 50*time.Second)},
								Status:             v1.ConditionFalse,
							},
						},
					},
				},
			},
			metrics: metricsclient.PodMetricsInfo{
				"lucretius": metricsclient.PodMetric{Value: 1},
			},
			resource:            v1.ResourceCPU,
			expectReadyPodCount: 1,
			expectUnreadyPods:   sets.New[string](),
			expectMissingPods:   sets.New[string](),
			expectIgnoredPods:   sets.New[string](),
		}, {
			// 无指标上报的 Pod 归入 missing 集合。
			name: "a missing pod",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "epicurus",
					},
					Status: v1.PodStatus{
						Phase: v1.PodSucceeded,
						StartTime: &metav1.Time{
							Time: time.Now().Add(-3 * time.Minute),
						},
					},
				},
			},
			metrics:             metricsclient.PodMetricsInfo{},
			resource:            v1.ResourceCPU,
			expectReadyPodCount: 0,
			expectUnreadyPods:   sets.New[string](),
			expectMissingPods:   sets.New[string]("epicurus"),
			expectIgnoredPods:   sets.New[string](),
		}, {
			name: "several pods",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "lucretius",
					},
					Status: v1.PodStatus{
						Phase: v1.PodSucceeded,
						StartTime: &metav1.Time{
							Time: time.Now(),
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "niccolo",
					},
					Status: v1.PodStatus{
						Phase: v1.PodSucceeded,
						StartTime: &metav1.Time{
							Time: time.Now().Add(-3 * time.Minute),
						},
						Conditions: []v1.PodCondition{
							{
								Type:               v1.PodReady,
								LastTransitionTime: metav1.Time{Time: time.Now().Add(-3 * time.Minute)},
								Status:             v1.ConditionTrue,
							},
						},
					},
				},
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "epicurus",
					},
					Status: v1.PodStatus{
						Phase: v1.PodSucceeded,
						StartTime: &metav1.Time{
							Time: time.Now().Add(-3 * time.Minute),
						},
					},
				},
			},
			metrics: metricsclient.PodMetricsInfo{
				"lucretius": metricsclient.PodMetric{Value: 1},
				"niccolo":   metricsclient.PodMetric{Value: 1},
			},
			resource:            v1.ResourceCPU,
			expectReadyPodCount: 1,
			expectUnreadyPods:   sets.New[string]("lucretius"),
			expectMissingPods:   sets.New[string]("epicurus"),
			expectIgnoredPods:   sets.New[string](),
		}, {
			name: "pending pods are unreadied",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "unscheduled",
					},
					Status: v1.PodStatus{
						Phase: v1.PodPending,
					},
				},
			},
			metrics:             metricsclient.PodMetricsInfo{},
			resource:            v1.ResourceCPU,
			expectReadyPodCount: 0,
			expectUnreadyPods:   sets.New[string]("unscheduled"),
			expectMissingPods:   sets.New[string](),
			expectIgnoredPods:   sets.New[string](),
		}, {
			// 带删除时间戳的 Pod 无论状态如何都应忽略。
			name: "ignore pods with deletion timestamps",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name:              "deleted",
						DeletionTimestamp: &metav1.Time{Time: time.Unix(1, 0)},
					},
					Status: v1.PodStatus{
						Phase: v1.PodPending,
					},
				},
			},
			metrics: metricsclient.PodMetricsInfo{
				"deleted": metricsclient.PodMetric{Value: 1},
			},
			resource:            v1.ResourceCPU,
			expectReadyPodCount: 0,
			expectUnreadyPods:   sets.New[string](),
			expectMissingPods:   sets.New[string](),
			expectIgnoredPods:   sets.New[string]("deleted"),
		}, {
			name: "ignore pods in a failed state",
			pods: []*v1.Pod{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "failed",
					},
					Status: v1.PodStatus{
						Phase: v1.PodFailed,
					},
				},
			},
			metrics: metricsclient.PodMetricsInfo{
				"failed": metricsclient.PodMetric{Value: 1},
			},
			resource:            v1.ResourceCPU,
			expectReadyPodCount: 0,
			expectUnreadyPods:   sets.New[string](),
			expectMissingPods:   sets.New[string](),
			expectIgnoredPods:   sets.New[string]("failed"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 逐个核对四类结果，确保 Pod 被分到正确类别。
			readyPodCount, unreadyPods, missingPods, ignoredPods := groupPods(tc.pods, tc.metrics, tc.resource, defaultTestingCPUInitializationPeriod, defaultTestingDelayOfInitialReadinessStatus)
			if readyPodCount != tc.expectReadyPodCount {
				t.Errorf("%s got readyPodCount %d, expected %d", tc.name, readyPodCount, tc.expectReadyPodCount)
			}
			if !unreadyPods.Equal(tc.expectUnreadyPods) {
				t.Errorf("%s got unreadyPods %v, expected %v", tc.name, unreadyPods, tc.expectUnreadyPods)
			}
			if !missingPods.Equal(tc.expectMissingPods) {
				t.Errorf("%s got missingPods %v, expected %v", tc.name, missingPods, tc.expectMissingPods)
			}
			if !ignoredPods.Equal(tc.expectIgnoredPods) {
				t.Errorf("%s got ignoredPods %v, expected %v", tc.name, ignoredPods, tc.expectIgnoredPods)
			}
		})
	}
}

// TestCalculateRequests 覆盖 calculateRequests 的 request 计算：未启用 Pod 级资源时累加各容器 request，
// 启用后 Pod 级 resource 优先（未设置则回退到容器求和），指定容器名时只计算该容器，
// 容器缺失 request 时报错，以及可重启 init 容器的 request 需要计入。
func TestCalculateRequests(t *testing.T) {
	containerRestartPolicyAlways := v1.ContainerRestartPolicyAlways
	testPod := "test-pod"

	tests := []struct {
		name                    string
		pods                    []*v1.Pod
		container               string
		resource                v1.ResourceName
		enablePodLevelResources bool
		expectedRequests        map[string]int64
		expectedError           error
	}{
		{
			// 空 Pod 列表返回空结果。
			name:             "void",
			pods:             []*v1.Pod{},
			container:        "",
			resource:         v1.ResourceCPU,
			expectedRequests: map[string]int64{},
			expectedError:    nil,
		},
		{
			// 特性关闭时按容器 request 求和。
			name: "Sum container requests if pod-level feature is disabled",
			pods: []*v1.Pod{{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testPod,
					Namespace: testNamespace,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "container1", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: *resource.NewMilliQuantity(100, resource.DecimalSI)}}},
						{Name: "container2", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: *resource.NewMilliQuantity(50, resource.DecimalSI)}}},
					},
				},
			}},
			container:        "",
			resource:         v1.ResourceCPU,
			expectedRequests: map[string]int64{testPod: 150},
			expectedError:    nil,
		},
		{
			name:                    "Pod-level resources are enabled, but not set: fallback to sum container requests",
			enablePodLevelResources: true,
			pods: []*v1.Pod{{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testPod,
					Namespace: testNamespace,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "container1", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: *resource.NewMilliQuantity(100, resource.DecimalSI)}}},
						{Name: "container2", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: *resource.NewMilliQuantity(50, resource.DecimalSI)}}},
					},
				},
			}},
			container:        "",
			resource:         v1.ResourceCPU,
			expectedRequests: map[string]int64{testPod: 150},
			expectedError:    nil,
		},
		{
			// 特性开启且设置了 Pod 级 request 时，以 Pod 级值覆盖容器求和。
			name:                    "Pod-level resources override container requests when feature enabled and pod resources specified",
			enablePodLevelResources: true,
			pods: []*v1.Pod{{

				ObjectMeta: metav1.ObjectMeta{
					Name:      testPod,
					Namespace: testNamespace,
				},
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: *resource.NewMilliQuantity(800, resource.DecimalSI)},
					},
					Containers: []v1.Container{
						{Name: "container1", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: *resource.NewMilliQuantity(100, resource.DecimalSI)}}},
						{Name: "container2", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: *resource.NewMilliQuantity(50, resource.DecimalSI)}}},
					},
				},
			}},
			container:        "",
			resource:         v1.ResourceCPU,
			expectedRequests: map[string]int64{testPod: 800},
			expectedError:    nil,
		},
		{
			name: "Fail if at least one of the containers is missing requests and pod-level feature/requests are not set",
			pods: []*v1.Pod{{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testPod,
					Namespace: testNamespace,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "container1"},
						{Name: "container2", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: *resource.NewMilliQuantity(50, resource.DecimalSI)}}},
					},
				},
			}},
			container:        "",
			resource:         v1.ResourceCPU,
			expectedRequests: nil,
			expectedError:    fmt.Errorf("missing request for %s in container %s of Pod %s", v1.ResourceCPU, "container1", testPod),
		},
		{
			name:                    "Pod-level resources override missing container requests when feature enabled and pod resources specified",
			enablePodLevelResources: true,
			pods: []*v1.Pod{{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testPod,
					Namespace: testNamespace,
				},
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: *resource.NewMilliQuantity(800, resource.DecimalSI)},
					},
					Containers: []v1.Container{
						{Name: "container1"},
						{Name: "container2", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: *resource.NewMilliQuantity(50, resource.DecimalSI)}}},
					},
				},
			}},
			container:        "",
			resource:         v1.ResourceCPU,
			expectedRequests: map[string]int64{testPod: 800},
			expectedError:    nil,
		},
		{
			// 指定容器名时只计算该容器的 request。
			name: "Container: if a container name is specified, calculate requests only for that container",
			pods: []*v1.Pod{{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testPod,
					Namespace: testNamespace,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "container1", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: *resource.NewMilliQuantity(100, resource.DecimalSI)}}},
						{Name: "container2", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: *resource.NewMilliQuantity(50, resource.DecimalSI)}}},
					},
				},
			}},
			container:        "container1",
			resource:         v1.ResourceCPU,
			expectedRequests: map[string]int64{testPod: 100},
			expectedError:    nil,
		},
		{
			name:                    "Container: if a container name is specified, calculate requests only for that container and ignore pod-level requests",
			enablePodLevelResources: true,
			pods: []*v1.Pod{{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testPod,
					Namespace: testNamespace,
				},
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: *resource.NewMilliQuantity(800, resource.DecimalSI)},
					},
					Containers: []v1.Container{
						{Name: "container1", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: *resource.NewMilliQuantity(100, resource.DecimalSI)}}},
						{Name: "container2", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: *resource.NewMilliQuantity(50, resource.DecimalSI)}}},
					},
				},
			}},
			container:        "container1",
			resource:         v1.ResourceCPU,
			expectedRequests: map[string]int64{testPod: 100},
			expectedError:    nil,
		},
		{
			// 可重启的 init 容器 request 需要计入总量，普通 init 容器则不需要。
			name: "pod with restartable init containers",
			pods: []*v1.Pod{{
				ObjectMeta: metav1.ObjectMeta{
					Name:      testPod,
					Namespace: testNamespace,
				},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "container1", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: *resource.NewMilliQuantity(100, resource.DecimalSI)}}},
					},
					InitContainers: []v1.Container{
						{Name: "init-container1", Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: *resource.NewMilliQuantity(20, resource.DecimalSI)}}},
						{Name: "restartable-container1", RestartPolicy: &containerRestartPolicyAlways, Resources: v1.ResourceRequirements{Requests: v1.ResourceList{v1.ResourceCPU: *resource.NewMilliQuantity(50, resource.DecimalSI)}}},
					},
				},
			}},
			container:        "",
			resource:         v1.ResourceCPU,
			expectedRequests: map[string]int64{testPod: 150},
			expectedError:    nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 每个子测试单独设置特性开关，确保 Pod 级资源行为互不干扰。
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.PodLevelResources, tc.enablePodLevelResources)

			requests, err := calculateRequests(tc.pods, tc.container, tc.resource)
			assert.Equal(t, tc.expectedRequests, requests, "requests should be as expected")
			assert.Equal(t, tc.expectedError, err, "error should be as expected")
		})
	}
}

// TestCalculatePodRequestsFromContainers_NonExistentContainer 验证当指定容器名在 Pod 中不存在时，
// calculatePodRequestsFromContainers 返回 0 以及格式正确的错误信息。
func TestCalculatePodRequestsFromContainers_NonExistentContainer(t *testing.T) {
	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-pod",
			Namespace: testNamespace,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name: "container1",
					Resources: v1.ResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceCPU: resource.MustParse("100m"),
						},
					},
				},
			},
		},
	}

	request, err := calculatePodRequestsFromContainers(pod, "non-existent-container", v1.ResourceCPU)

	require.Error(t, err, "expected error for non-existent container")
	expectedErr := "container non-existent-container not found in Pod test-pod"
	assert.Equal(t, expectedErr, err.Error(), "error message should match expected format")
	assert.Equal(t, int64(0), request, "request should be 0 when container does not exist")
}
