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

package metrics

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/component-base/metrics/testutil"
	"k8s.io/kubernetes/test/e2e/framework"
)

const (
	// dockerOperationsLatencyKey 是操作延迟指标的键。
	// 取自 k8s.io/kubernetes/pkg/kubelet/dockershim/metrics
	dockerOperationsLatencyKey = "docker_operations_duration_seconds"
	// 取自 k8s.io/kubernetes/pkg/kubelet/metrics
	kubeletSubsystem = "kubelet"
	// 取自 k8s.io/kubernetes/pkg/kubelet/metrics
	podWorkerDurationKey = "pod_worker_duration_seconds"
	// 取自 k8s.io/kubernetes/pkg/kubelet/metrics
	podStartDurationKey = "pod_start_duration_seconds"
	// 取自 k8s.io/kubernetes/pkg/kubelet/metrics
	podStartSLIDurationKey = "pod_start_sli_duration_seconds"
	// 取自 k8s.io/kubernetes/pkg/kubelet/metrics
	cgroupManagerOperationsKey = "cgroup_manager_duration_seconds"
	// 取自 k8s.io/kubernetes/pkg/kubelet/metrics
	podWorkerStartDurationKey = "pod_worker_start_duration_seconds"
	// 取自 k8s.io/kubernetes/pkg/kubelet/metrics
	plegRelistDurationKey = "pleg_relist_duration_seconds"
)

// KubeletMetrics 是 kubelet 的指标集合
type KubeletMetrics testutil.Metrics

// Equal 在所有指标与参数相同时返回 true。
func (m *KubeletMetrics) Equal(o KubeletMetrics) bool {
	return (*testutil.Metrics)(m).Equal(testutil.Metrics(o))
}

// NewKubeletMetrics 返回已初始化的新指标对象。
func NewKubeletMetrics() KubeletMetrics {
	result := testutil.NewMetrics()
	return KubeletMetrics(result)
}

// GrabKubeletMetricsWithoutProxy 直接通过 HTTP GET 从指定节点的 kubelet 获取指标，不经过代理。
func GrabKubeletMetricsWithoutProxy(ctx context.Context, nodeName, path string) (KubeletMetrics, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("http://%s%s", nodeName, path), nil)
	if err != nil {
		return KubeletMetrics{}, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return KubeletMetrics{}, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return KubeletMetrics{}, err
	}
	return parseKubeletMetrics(string(body))
}

func parseKubeletMetrics(data string) (KubeletMetrics, error) {
	result := NewKubeletMetrics()
	if err := testutil.ParseMetrics(data, (*testutil.Metrics)(&result)); err != nil {
		return KubeletMetrics{}, err
	}
	return result, nil
}

// KubeletLatencyMetric 存储从 kubelet /metric 端点抓取的指标。
// TODO: 为这些指标和该类型增加更清晰的结构
type KubeletLatencyMetric struct {
	// eg: list, info, create
	Operation string
	// eg: sync_pods, pod_worker
	Method string
	// 0 <= quantile <= 1，例如 0.95 表示 95 分位，0.5 表示中位数。
	Quantile float64
	Latency  time.Duration
}

// KubeletLatencyMetrics 基于 latency 字段为 []KubeletMetric 实现 sort.Interface。
type KubeletLatencyMetrics []KubeletLatencyMetric

func (a KubeletLatencyMetrics) Len() int           { return len(a) }
func (a KubeletLatencyMetrics) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KubeletLatencyMetrics) Less(i, j int) bool { return a[i].Latency > a[j].Latency }

// 传入 apiserver 客户端时，函数会尝试从指标抓取器获取 kubelet 指标；
// 否则会直接从节点获取。
func getKubeletMetricsFromNode(ctx context.Context, c clientset.Interface, nodeName string) (KubeletMetrics, error) {
	if c == nil {
		return GrabKubeletMetricsWithoutProxy(ctx, nodeName, "/metrics")
	}
	grabber, err := NewMetricsGrabber(ctx, c, nil, nil, true, false, false, false, false, false)
	if err != nil {
		return KubeletMetrics{}, err
	}
	return grabber.GrabFromKubelet(ctx, nodeName)
}

// GetKubeletMetrics 从指定节点获取 kubelet 子系统的所有指标，并去掉子系统前缀。
func GetKubeletMetrics(ctx context.Context, c clientset.Interface, nodeName string) (KubeletMetrics, error) {
	ms, err := getKubeletMetricsFromNode(ctx, c, nodeName)
	if err != nil {
		return KubeletMetrics{}, err
	}

	kubeletMetrics := make(KubeletMetrics)
	for name, samples := range ms {
		const prefix = kubeletSubsystem + "_"
		if !strings.HasPrefix(name, prefix) {
			// 不是 kubelet 指标。
			continue
		}
		method := strings.TrimPrefix(name, prefix)
		kubeletMetrics[method] = samples
	}
	return kubeletMetrics, nil
}

// GetDefaultKubeletLatencyMetrics 使用一组默认的 metricName 调用 GetKubeletLatencyMetrics
// identifying common latency metrics.
// 注意传入的 KubeletMetrics 不应包含子系统前缀。
func GetDefaultKubeletLatencyMetrics(ms KubeletMetrics) KubeletLatencyMetrics {
	latencyMetricNames := sets.NewString(
		podWorkerDurationKey,
		podWorkerStartDurationKey,
		podStartDurationKey,
		podStartSLIDurationKey,
		cgroupManagerOperationsKey,
		dockerOperationsLatencyKey,
		podWorkerStartDurationKey,
		plegRelistDurationKey,
	)
	return GetKubeletLatencyMetrics(ms, latencyMetricNames)
}

// GetKubeletLatencyMetrics 只保留 ms 中属于 metricNames 集合的指标，
// 再基于这些指标的样本构造 KubeletLatencyMetrics 列表。
func GetKubeletLatencyMetrics(ms KubeletMetrics, filterMetricNames sets.String) KubeletLatencyMetrics {
	var latencyMetrics KubeletLatencyMetrics
	for name, samples := range ms {
		if !filterMetricNames.Has(name) {
			continue
		}
		for _, sample := range samples {
			latency := sample.Value
			operation := string(sample.Metric["operation_type"])
			var quantile float64
			if val, ok := sample.Metric[testutil.QuantileLabel]; ok {
				var err error
				if quantile, err = strconv.ParseFloat(string(val), 64); err != nil {
					continue
				}
			}

			latencyMetrics = append(latencyMetrics, KubeletLatencyMetric{
				Operation: operation,
				Method:    name,
				Quantile:  quantile,
				Latency:   time.Duration(int64(latency)) * time.Microsecond,
			})
		}
	}
	return latencyMetrics
}

// HighLatencyKubeletOperations 记录并统计 kubelet 通过 /metrics 导出的高延迟指标。
func HighLatencyKubeletOperations(ctx context.Context, c clientset.Interface, threshold time.Duration, nodeName string, logFunc func(fmt string, args ...interface{})) (KubeletLatencyMetrics, error) {
	ms, err := GetKubeletMetrics(ctx, c, nodeName)
	if err != nil {
		return KubeletLatencyMetrics{}, err
	}
	latencyMetrics := GetDefaultKubeletLatencyMetrics(ms)
	sort.Sort(latencyMetrics)
	var badMetrics KubeletLatencyMetrics
	logFunc("\nLatency metrics for node %v", nodeName)
	for _, m := range latencyMetrics {
		if m.Latency > threshold {
			badMetrics = append(badMetrics, m)
			framework.Logf("%+v", m)
		}
	}
	return badMetrics, nil
}
