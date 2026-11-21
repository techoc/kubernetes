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
	"fmt"
)

// GetResourceUtilizationRatio 根据指标集合、对应的 request 集合和目标利用率百分比，
// 计算期望利用率与实际利用率的比值，并返回该比值、当前利用率和原始平均值。
func GetResourceUtilizationRatio(metrics PodMetricsInfo, requests map[string]int64, targetUtilization int32) (utilizationRatio float64, currentUtilization int32, rawAverageValue int64, err error) {
	metricsTotal := int64(0)
	requestsTotal := int64(0)
	numEntries := 0

	for podName, metric := range metrics {
		request, hasRequest := requests[podName]
		if !hasRequest {
			// 缺失 request 的情况会在其他地方检查；这里将没有对应 request 的指标视为多余指标直接跳过。
			continue
		}

		metricsTotal += metric.Value
		requestsTotal += request
		numEntries++
	}

	// 若指标集合与 request 集合完全不相交，request 总量会为 0，此时无法计算利用率，必须报错。
	if requestsTotal == 0 {
		return 0, 0, 0, fmt.Errorf("no metrics returned matched known pods")
	}

	currentUtilization = int32((metricsTotal * 100) / requestsTotal)

	return float64(currentUtilization) / float64(targetUtilization), currentUtilization, metricsTotal / int64(numEntries), nil
}

// GetMetricUsageRatio 根据指标集合与目标使用量，计算期望使用量与实际使用量的比值，
// 并返回该比值和当前使用量。
func GetMetricUsageRatio(metrics PodMetricsInfo, targetUsage int64) (usageRatio float64, currentUsage int64) {
	metricsTotal := int64(0)
	for _, metric := range metrics {
		metricsTotal += metric.Value
	}

	currentUsage = metricsTotal / int64(len(metrics))

	return float64(currentUsage) / float64(targetUsage), currentUsage
}
