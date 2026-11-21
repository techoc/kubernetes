/*
Copyright 2025 The Kubernetes Authors.

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

package dra

import (
	"strings"

	"k8s.io/component-base/metrics/testutil"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// testClaimsInUseMetric 将 collector 采集到的 dra_resource_claims_in_use 指标与期望文本比较。
func testClaimsInUseMetric(tCtx ktesting.TContext, claimInfoCache *claimInfoCache, expectedMetric string) {
	tCtx.Helper()
	// 必须模拟注册流程调用 Create，否则采集时会崩溃。
	collector := &claimInfoCollector{cache: claimInfoCache}
	collector.Create(nil, collector)
	err := testutil.CollectAndCompare(collector, strings.NewReader(expectedMetric), "dra_resource_claims_in_use")
	if err != nil {
		tCtx.Error(err)
	}
}
