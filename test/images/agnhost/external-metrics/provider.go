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

package externalmetrics

import (
	"fmt"
	"strings"
	"sync"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type externalMetric struct {
	metricName string
	labels     map[string]string
	value      int
	shouldFail bool        // 若为 true，该 metric 返回错误
	timestamp  metav1.Time // 为了 mock，我们只返回当前时间
}

type metricProvider struct {
	externalMetricsLock sync.RWMutex
	externalMetrics     map[string]*externalMetric
}

func NewConfigurableProvider() *metricProvider {
	p := &metricProvider{
		externalMetrics:     make(map[string]*externalMetric),
		externalMetricsLock: sync.RWMutex{},
	}
	return p
}

// buildMetricKey 通过组合 metric 名称和标签创建一个唯一的 key。
// 这允许同一 metric 拥有多个不同标签的实例。
func buildMetricKey(metricName string, metricLabels map[string]string) string {
	key := metricName
	if len(metricLabels) > 0 {
		var labelPairs []string
		for k, v := range metricLabels {
			labelPairs = append(labelPairs, k+"="+v)
		}
		key = metricName + ":" + strings.Join(labelPairs, ",")
	}
	return key
}

// parseLabels 从查询字符串格式中解析 label selector。
// 示例："env=prod,region=us-west"
func parseLabels(labelSelector string) map[string]string {
	labels := make(map[string]string)
	if labelSelector == "" {
		return labels
	}

	pairs := strings.Split(labelSelector, ",")
	for _, pair := range pairs {
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) == 2 {
			labels[strings.TrimSpace(kv[0])] = strings.TrimSpace(kv[1])
		}
	}
	return labels
}

// setMetricFailure 控制某个特定 metric 是否应当失败
func (p *metricProvider) setMetricFailure(metricKey string, failStr string) error {
	shouldFail := failStr == "true"

	p.externalMetricsLock.Lock()
	defer p.externalMetricsLock.Unlock()

	metric, exists := p.externalMetrics[metricKey]
	if !exists {
		return fmt.Errorf("metric %s not found", metricKey)
	}
	metric.shouldFail = shouldFail
	return nil
}

// setMetricFailure 控制某个特定 metric 是否应当失败
func (p *metricProvider) setMetricValue(metricKey string, metricValue int) error {
	p.externalMetricsLock.Lock()
	defer p.externalMetricsLock.Unlock()
	metric, exists := p.externalMetrics[metricKey]
	if !exists {
		return fmt.Errorf("metric %s not found", metricKey)
	}
	metric.value = metricValue
	return nil
}

func (p *metricProvider) createMetric(metricName string, metricLabels map[string]string, metricValue int, shouldFail bool) {
	p.externalMetricsLock.Lock()
	defer p.externalMetricsLock.Unlock()
	key := buildMetricKey(metricName, metricLabels)
	p.externalMetrics[key] = &externalMetric{
		metricName: metricName,
		labels:     metricLabels,
		value:      metricValue,
		shouldFail: shouldFail,
		timestamp:  metav1.Now(),
	}
}

// getMetrics 返回与给定名称和标签选择器匹配的所有指标
func (p *metricProvider) getMetrics(metricName string, labelSelector map[string]string) []*externalMetric {
	p.externalMetricsLock.RLock()
	defer p.externalMetricsLock.RUnlock()

	var results []*externalMetric

	// 如果提供了 label selector，先尝试精确匹配
	if len(labelSelector) > 0 {
		key := buildMetricKey(metricName, labelSelector)
		if metric, exists := p.externalMetrics[key]; exists {
			results = append(results, metric)
			return results
		}
	}

	// 否则，查找所有具有给定名称的 metric
	for _, metric := range p.externalMetrics {
		if metric.metricName != metricName {
			continue
		}

		// 如果没有 label selector，则包含所有具有此名称的 metric
		if len(labelSelector) == 0 {
			results = append(results, metric)
			continue
		}

		// 检查 metric 的标签是否匹配 selector
		matches := true
		for selectorKey, selectorValue := range labelSelector {
			if metricValue, exists := metric.labels[selectorKey]; !exists || metricValue != selectorValue {
				matches = false
				break
			}
		}
		if matches {
			results = append(results, metric)
		}
	}

	// 为向后兼容，还要检查 metricName 是否作为不带标签的 key 存在
	if len(results) == 0 && len(labelSelector) == 0 {
		if metric, exists := p.externalMetrics[metricName]; exists {
			results = append(results, metric)
		}
	}

	return results
}
