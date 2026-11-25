/*
Copyright 2019 The Kubernetes Authors.

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

package flowcontrol

import (
	"context"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	"k8s.io/kubernetes/pkg/controlplane"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/ktesting"
)

const (
	nominalConcurrencyLimitMetricsName = "apiserver_flowcontrol_nominal_limit_seats"
	requestExecutionSecondsSumName     = "apiserver_flowcontrol_request_execution_seconds_sum"
	requestExecutionSecondsCountName   = "apiserver_flowcontrol_request_execution_seconds_count"
	priorityLevelSeatUtilSumName       = "apiserver_flowcontrol_priority_level_seat_utilization_sum"
	priorityLevelSeatUtilCountName     = "apiserver_flowcontrol_priority_level_seat_utilization_count"
	fakeworkDuration                   = 200 * time.Millisecond
	testWarmUpTime                     = 2 * time.Second
	testTime                           = 10 * time.Second
)

type SumAndCount struct {
	Sum   float64
	Count int
}

type plMetrics struct {
	execSeconds    SumAndCount
	seatUtil       SumAndCount
	availableSeats int
}

// metricSnapshot 按 priority level 标签索引关心的 APF 指标快照
type metricSnapshot map[string]plMetrics

// clientLatencyMeasurement 记录客户端请求时延
type clientLatencyMeasurement struct {
	SumAndCount
	SumSq float64 // 时延平方和
	Mu    sync.Mutex
}

func (clm *clientLatencyMeasurement) reset() {
	clm.Mu.Lock()
	defer clm.Mu.Unlock()
	clm.Sum = 0
	clm.Count = 0
	clm.SumSq = 0
}

func (clm *clientLatencyMeasurement) update(duration float64) {
	clm.Mu.Lock()
	defer clm.Mu.Unlock()
	clm.Count += 1
	clm.Sum += duration
	clm.SumSq += duration * duration
}

func (clm *clientLatencyMeasurement) getStats() clientLatencyStats {
	clm.Mu.Lock()
	defer clm.Mu.Unlock()
	mean := clm.Sum / float64(clm.Count)
	ss := clm.SumSq - mean*clm.Sum // 由 sumsq - 2*mean*sum + count*mean*mean 化简得来
	// 浮点运算可能导致 ss 为负，此时按 0 处理
	if ss < 0 {
		ss = 0
	}
	stdDev := math.Sqrt(ss / float64(clm.Count))
	cv := stdDev / mean
	return clientLatencyStats{mean: mean, stdDev: stdDev, cv: cv}
}

type clientLatencyStats struct {
	mean   float64 // 平均时延
	stdDev float64 // 总体标准差
	cv     float64 // 变异系数
}

type plMetricAvg struct {
	reqExecution float64 // 平均请求执行时间
	seatUtil     float64 // 平均 seat 利用率
}

func intervalMetricAvg(snapshot0, snapshot1 metricSnapshot, plLabel string) plMetricAvg {
	plmT0 := snapshot0[plLabel]
	plmT1 := snapshot1[plLabel]
	return plMetricAvg{
		reqExecution: (plmT1.execSeconds.Sum - plmT0.execSeconds.Sum) / float64(plmT1.execSeconds.Count-plmT0.execSeconds.Count),
		seatUtil:     (plmT1.seatUtil.Sum - plmT0.seatUtil.Sum) / float64(plmT1.seatUtil.Count-plmT0.seatUtil.Count),
	}
}

type noxuDelayingAuthorizer struct {
	Authorizer authorizer.UnconditionalAuthorizer
}

func (d *noxuDelayingAuthorizer) Authorize(ctx context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
	if a.GetUser().GetName() == "noxu1" || a.GetUser().GetName() == "noxu2" {
		time.Sleep(fakeworkDuration) // 用 sleep 模拟耗时操作
	}
	return d.Authorizer.Authorize(ctx, a)
}

// ConditionsAwareAuthorize 本身不处理 conditions，仅转换 Authorize 的决策结果。
func (d *noxuDelayingAuthorizer) ConditionsAwareAuthorize(ctx context.Context, a authorizer.Attributes) authorizer.ConditionsAwareDecision {
	return authorizer.ConditionsAwareDecisionFromParts(d.Authorize(ctx, a))
}

// EvaluateConditions 该 authorizer 不支持条件求值。
func (*noxuDelayingAuthorizer) EvaluateConditions(_ context.Context, _ authorizer.ConditionsAwareDecision, _ authorizer.ConditionsData) (authorizer.Decision, string, error) {
	return authorizer.DecisionDeny, "", authorizer.ErrorConditionEvaluationNotSupported
}

// TestConcurrencyIsolation 验证不同 priority level 之间的并发隔离。
// 测试为此定义两个 priority level 及相应的 flow schema：其中一个发送远超配置并发上限的请求，
// 另一个发送低于上限的请求。主要断言是低流量方仍能拿到所需的全部 seat，
// 但需考虑系统中不可控的额外开销，故判定留有裕度。
//
// 与 TestPriorityLevelIsolation 不同，后者检查的是吞吐量而非并发数。
// 为抑制系统噪声的影响，这里用延迟 authorizer 人为拉长请求执行时间，使噪声相对可忽略。
// 此外还把观测到的 seat 利用率与"客户端观测吞吐量等于服务端执行吞吐量"的推算值做比对。
func TestConcurrencyIsolation(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, kubeConfig, closeFn := framework.StartTestServer(tCtx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			// 放行所有客户端，避免鉴权影响并发测量
			opts.Authorization.Modes = []string{"AlwaysAllow"}
			opts.GenericServerRunOptions.MaxRequestsInFlight = 10
			opts.GenericServerRunOptions.MaxMutatingRequestsInFlight = 10
		},
		ModifyServerConfig: func(config *controlplane.Config) {
			// 包装默认 authorizer，对 noxu 客户端的请求人为增加延迟
			config.ControlPlane.Generic.Authorization.Authorizer = &noxuDelayingAuthorizer{config.ControlPlane.Generic.Authorization.Authorizer}
		},
	})
	defer closeFn()

	loopbackClient := clientset.NewForConfigOrDie(kubeConfig)
	noxu1Client := getClientFor(kubeConfig, "noxu1")
	noxu2Client := getClientFor(kubeConfig, "noxu2")

	queueLength := 50
	concurrencyShares := 100

	plNoxu1, _, err := createPriorityLevelAndBindingFlowSchemaForUser(
		loopbackClient, "noxu1", concurrencyShares, queueLength)
	if err != nil {
		t.Error(err)
	}
	plNoxu2, _, err := createPriorityLevelAndBindingFlowSchemaForUser(
		loopbackClient, "noxu2", concurrencyShares, queueLength)
	if err != nil {
		t.Error(err)
	}

	stopCh := make(chan struct{})
	wg := sync.WaitGroup{}

	// "elephant"：高并发大流量的一方
	noxu1NumGoroutines := 5 + queueLength
	var noxu1LatMeasure clientLatencyMeasurement
	wg.Add(noxu1NumGoroutines)
	streamRequests(noxu1NumGoroutines, func() {
		start := time.Now()
		_, err := noxu1Client.CoreV1().Namespaces().Get(tCtx, "default", metav1.GetOptions{})
		duration := time.Since(start).Seconds()
		noxu1LatMeasure.update(duration)
		if err != nil {
			t.Error(err)
		}
	}, &wg, stopCh)
	// "mouse"：低并发小流量的一方
	noxu2NumGoroutines := 3
	var noxu2LatMeasure clientLatencyMeasurement
	wg.Add(noxu2NumGoroutines)
	streamRequests(noxu2NumGoroutines, func() {
		start := time.Now()
		_, err := noxu2Client.CoreV1().Namespaces().Get(tCtx, "default", metav1.GetOptions{})
		duration := time.Since(start).Seconds()
		noxu2LatMeasure.update(duration)
		if err != nil {
			t.Error(err)
		}
	}, &wg, stopCh)

	// 预热，避开启动阶段的不稳定数据
	time.Sleep(testWarmUpTime)

	noxu1LatMeasure.reset()
	noxu2LatMeasure.reset()
	snapshot0, err := getRequestMetricsSnapshot(loopbackClient)
	if err != nil {
		t.Error(err)
	}
	time.Sleep(testTime) // 预热结束后进入稳态，再采样
	snapshot1, err := getRequestMetricsSnapshot(loopbackClient)
	if err != nil {
		t.Error(err)
	}
	close(stopCh)

	// 先检验测试前提是否成立，否则后续断言无意义
	noxu1T0 := snapshot0[plNoxu1.Name]
	noxu1T1 := snapshot1[plNoxu1.Name]
	noxu2T0 := snapshot0[plNoxu2.Name]
	noxu2T1 := snapshot1[plNoxu2.Name]
	if noxu1T0.seatUtil.Count >= noxu1T1.seatUtil.Count || noxu2T0.seatUtil.Count >= noxu2T1.seatUtil.Count {
		t.Errorf("SeatUtilCount check failed: noxu1 t0 count %d, t1 count %d; noxu2 t0 count %d, t1 count %d",
			noxu1T0.seatUtil.Count, noxu1T1.seatUtil.Count, noxu2T0.seatUtil.Count, noxu2T1.seatUtil.Count)
	}
	t.Logf("noxu1 priority level concurrency limit: %d", noxu1T0.availableSeats)
	t.Logf("noxu2 priority level concurrency limit: %d", noxu2T0.availableSeats)
	if (noxu1T0.availableSeats != noxu1T1.availableSeats) || (noxu2T0.availableSeats != noxu2T1.availableSeats) {
		t.Errorf("The number of available seats changed: noxu1 (%d, %d) noxu2 (%d, %d)",
			noxu1T0.availableSeats, noxu1T1.availableSeats, noxu2T0.availableSeats, noxu2T1.availableSeats)
	}
	if (noxu1T0.availableSeats <= 4) || (noxu2T0.availableSeats <= 4) {
		t.Errorf("The number of available seats for test client priority levels are too small: (%d, %d). Expecting a number > 4",
			noxu1T0.availableSeats, noxu2T0.availableSeats)
	}
	// 正常情况下不应有请求被拒绝
	_, rejectedReqCounts, err := getRequestCountOfPriorityLevel(loopbackClient)
	if err != nil {
		t.Error(err)
	}
	if rejectedReqCounts[plNoxu1.Name] > 0 {
		t.Errorf(`%d requests from the "elephant" stream were rejected unexpectedly`, rejectedReqCounts[plNoxu1.Name])
	}
	if rejectedReqCounts[plNoxu2.Name] > 0 {
		t.Errorf(`%d requests from the "mouse" stream were rejected unexpectedly`, rejectedReqCounts[plNoxu2.Name])
	}

	// 计算测试区间内服务端 APF 指标的均值
	noxu1Avg := intervalMetricAvg(snapshot0, snapshot1, plNoxu1.Name)
	noxu2Avg := intervalMetricAvg(snapshot0, snapshot1, plNoxu2.Name)
	t.Logf("\nnoxu1 avg request execution time %v\nnoxu2 avg request execution time %v", noxu1Avg.reqExecution, noxu2Avg.reqExecution)
	t.Logf("\nnoxu1 avg seat utilization %v\nnoxu2 avg seat utilization %v", noxu1Avg.seatUtil, noxu2Avg.seatUtil)

	// 等客户端 goroutine 结束后再统计时延，避免数据仍在写入
	wg.Wait()
	noxu1LatStats := noxu1LatMeasure.getStats()
	noxu2LatStats := noxu2LatMeasure.getStats()
	t.Logf("noxu1 client request count %d duration mean %v stddev %v cv %v", noxu1LatMeasure.Count, noxu1LatStats.mean, noxu1LatStats.stdDev, noxu1LatStats.cv)
	t.Logf("noxu2 client request count %d duration mean %v stddev %v cv %v", noxu2LatMeasure.Count, noxu2LatStats.mean, noxu2LatStats.stdDev, noxu2LatStats.cv)

	// 按服务端指标推算实际并发数
	noxu1ObservedConcurrency := noxu1Avg.seatUtil * float64(noxu1T0.availableSeats)
	noxu2ObservedConcurrency := noxu2Avg.seatUtil * float64(noxu2T0.availableSeats)
	// 依据客户端与服务端吞吐量相等的假设计算期望并发数
	noxu1ExpectedConcurrency := float64(noxu1NumGoroutines) * noxu1Avg.reqExecution / noxu1LatStats.mean
	noxu2ExpectedConcurrency := float64(noxu2NumGoroutines) * noxu2Avg.reqExecution / noxu2LatStats.mean
	t.Logf("Concurrency of noxu1:noxu2 - expected (%v:%v), observed (%v:%v)", noxu1ExpectedConcurrency, noxu2ExpectedConcurrency, noxu1ObservedConcurrency, noxu2ObservedConcurrency)

	// 系统中存在不可控的额外开销，会引入噪声。变异系数（CV，标准差除以均值）
	// 刻画了某类流量的全部噪声。noxu1 的 CV 通常远大于 noxu2，
	// 因为它还会触发排队等待等行为。两者都需考虑，因此组合它们得出本测试的容差阈值。
	margin := noxu1LatStats.cv + 2*noxu2LatStats.cv
	t.Logf("Error margin is %v", margin)

	isConcurrencyExpected := func(name string, observed float64, expected float64) bool {
		relativeErr := math.Abs(expected-observed) / expected
		t.Logf("%v relative error is %v", name, relativeErr)
		return relativeErr <= margin
	}
	if !isConcurrencyExpected(plNoxu1.Name, noxu1ObservedConcurrency, noxu1ExpectedConcurrency) {
		t.Errorf("Concurrency observed by noxu1 is off. Expected: %v, observed: %v", noxu1ExpectedConcurrency, noxu1ObservedConcurrency)
	}
	if !isConcurrencyExpected(plNoxu2.Name, noxu2ObservedConcurrency, noxu2ExpectedConcurrency) {
		t.Errorf("Concurrency observed by noxu2 is off. Expected: %v, observed: %v", noxu2ExpectedConcurrency, noxu2ObservedConcurrency)
	}

	// 校验服务端观测到的 APF seat 利用率
	if math.Abs(1-noxu1Avg.seatUtil) > 0.05 {
		t.Errorf("noxu1Avg.seatUtil=%v is too far from expected=1.0", noxu1Avg.seatUtil)
	}
	noxu2ExpectedSeatUtil := float64(noxu2NumGoroutines) / float64(noxu2T0.availableSeats)
	if math.Abs(noxu2ExpectedSeatUtil-noxu2Avg.seatUtil) > 0.05 {
		t.Errorf("noxu2Avg.seatUtil=%v is too far from expected=%v", noxu2Avg.seatUtil, noxu2ExpectedSeatUtil)
	}
}

func getRequestMetricsSnapshot(c clientset.Interface) (metricSnapshot, error) {

	resp, err := getMetrics(c)
	if err != nil {
		return nil, err
	}

	dec := expfmt.NewDecoder(strings.NewReader(string(resp)), expfmt.NewFormat(expfmt.TypeTextPlain))
	decoder := expfmt.SampleDecoder{
		Dec:  dec,
		Opts: &expfmt.DecodeOptions{},
	}

	snapshot := metricSnapshot{}

	for {
		var v model.Vector
		if err := decoder.Decode(&v); err != nil {
			if err == io.EOF {
				// EOF 即预期的循环结束条件
				return snapshot, nil
			}
			return nil, fmt.Errorf("failed decoding metrics: %v", err)
		}
		for _, metric := range v {
			plLabel := string(metric.Metric[labelPriorityLevel])
			entry := plMetrics{}
			if v, ok := snapshot[plLabel]; ok {
				entry = v
			}
			switch name := string(metric.Metric[model.MetricNameLabel]); name {
			case requestExecutionSecondsSumName:
				entry.execSeconds.Sum = float64(metric.Value)
			case requestExecutionSecondsCountName:
				entry.execSeconds.Count = int(metric.Value)
			case priorityLevelSeatUtilSumName:
				entry.seatUtil.Sum = float64(metric.Value)
			case priorityLevelSeatUtilCountName:
				entry.seatUtil.Count = int(metric.Value)
			case nominalConcurrencyLimitMetricsName:
				entry.availableSeats = int(metric.Value)
			}
			snapshot[plLabel] = entry
		}
	}
}
