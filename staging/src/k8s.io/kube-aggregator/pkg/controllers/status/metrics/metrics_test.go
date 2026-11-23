/*
Copyright 2020 The Kubernetes Authors.

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
	"strings"
	"testing"

	"k8s.io/component-base/metrics/testutil"
)

// 指标暴露的是不可用状态而不是“可用布尔值”：正常情况下应该没有时间序列，
// 这样告警只需要匹配出现的条目，不必为大量健康的 APIService 维护常量序列。
func TestAPIServiceAvailabilityCollection(t *testing.T) {
	collector := newAvailabilityCollector()

	availableAPIService := "available"
	unavailableAPIService := "unavailable"

	collector.SetAPIServiceAvailable(availableAPIService)
	collector.SetAPIServiceUnavailable(unavailableAPIService)

	err := testutil.CustomCollectAndCompare(collector, strings.NewReader(`
	# HELP aggregator_unavailable_apiservice [ALPHA] Gauge of APIServices which are marked as unavailable broken down by APIService name.
	# TYPE aggregator_unavailable_apiservice gauge
	aggregator_unavailable_apiservice{name="available"} 0
	aggregator_unavailable_apiservice{name="unavailable"} 1
	`))
	if err != nil {
		t.Fatal(err)
	}

	collector.ClearState()

	// Forget 必须把标签对应的序列彻底移除；只置零会让已经恢复的服务
	// 永远以 0 的形式留在指标里，告警规则就无从区分“恢复”和“从未出现”。
	collector.ForgetAPIService(availableAPIService)
	collector.ForgetAPIService(unavailableAPIService)

	err = testutil.CustomCollectAndCompare(collector, strings.NewReader(`
	# HELP aggregator_unavailable_apiservice [ALPHA] Gauge of APIServices which are marked as unavailable broken down by APIService name.
	# TYPE aggregator_unavailable_apiservice gauge
	`))
	if err != nil {
		t.Fatal(err)
	}
}
