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

package metrics

import (
	"context"
	"fmt"
	"strings"
	"time"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/client-go/kubernetes"
	metricstestutil "k8s.io/component-base/metrics/testutil"
)

const (
	// apiServerValidationDeclarativeValidationMismatchTotal 是声明式验证不匹配的指标名。
	apiServerValidationDeclarativeValidationMismatchTotal = "apiserver_validation_declarative_validation_mismatch_total"
	// apiServerValidationDeclarativeValidationPanicTotal 是声明式验证 panic 的指标名。
	apiServerValidationDeclarativeValidationPanicTotal = "apiserver_validation_declarative_validation_panic_total"
)

// metricInvariant 定义某个指标的不变量检查。
type metricInvariant struct {
	metricName string
	// 与该不变量关联的 sig
	// 与此不变量检查失败相关的 bug 应打上
	// 此 sig 标签。
	sig string
	// owners 是负责该不变量的个人
	// 与此不变量检查失败相关的 bug 应分配给这些
	// GitHub 账号
	owners  []string
	isValid func(metricstestutil.Samples) bool
}

// apiServerInvariants 是对 API server 检查的标准不变量。
//
// 在此添加任何内容前请先与 SIG-Testing 负责人沟通。
var apiServerInvariants = []metricInvariant{
	{
		metricName: apiServerValidationDeclarativeValidationMismatchTotal,
		sig:        "api-machinery",
		owners:     []string{"aaron-prindle", "jpbetz", "thockin"},
		isValid:    allSamplesZero,
	},
	{
		metricName: apiServerValidationDeclarativeValidationPanicTotal,
		sig:        "api-machinery",
		owners:     []string{"aaron-prindle", "jpbetz", "thockin"},
		isValid:    allSamplesZero,
	},
}

// checkInvariants 根据不变量列表检查给定的指标。
// 如果 failOnMissing 为 true，当不变量中列出的指标在 metrics map 中缺失时返回错误。
func checkInvariants(metrics map[string]metricstestutil.Samples, invariants []metricInvariant, failOnMissing bool) error {
	var errs []error
	for _, inv := range invariants {
		samples, ok := metrics[inv.metricName]
		if !ok || len(samples) == 0 {
			if failOnMissing {
				errs = append(errs, fmt.Errorf(`metric %s is missing from scrape (SIG: %s, Owners: %s)

If this failed on a pull request, please check if the PR changes may be related to the failure.
If not, you can also search for an existing GitHub issue before filing a new issue.

If this failed in a periodic CI job, please file a bug and /assign the owners`, inv.metricName, inv.sig, strings.Join(inv.owners, ", ")))
			}
			continue
		}
		if !inv.isValid(samples) {
			errs = append(errs, fmt.Errorf(`metric %s invariant failed (SIG: %s, Owners: %s)

If this failed on a pull request, please check if the PR changes may be related to the failure.
If not, you can also search for an existing GitHub issue before filing a new issue.

If this failed in a periodic CI job, please file a bug and /assign the owners`, inv.metricName, inv.sig, strings.Join(inv.owners, ", ")))
		}
	}
	return utilerrors.NewAggregate(errs)
}

// allSamplesZero 是所有样本值都为零时返回 true 的辅助函数。
func allSamplesZero(samples metricstestutil.Samples) bool {
	for _, sample := range samples {
		if sample.Value != 0 {
			return false
		}
	}
	return true
}

// CheckMetricInvariants 从 API server 抓取指标，并检查
// 不变量指标是否具有预期值。
func CheckMetricInvariants(ctx context.Context, client kubernetes.Interface, failOnMissing bool) error {
	scrapeCtx, cancelScrape := context.WithTimeout(ctx, 5*time.Second)
	defer cancelScrape()
	body, err := client.Discovery().RESTClient().Get().AbsPath("metrics").DoRaw(scrapeCtx)
	if err != nil {
		return fmt.Errorf("failed to scrape metrics: %w", err)
	}

	scrapedMetrics := metricstestutil.NewMetrics()
	if err := metricstestutil.ParseMetrics(string(body), &scrapedMetrics); err != nil {
		return fmt.Errorf("failed to parse metrics: %w", err)
	}

	return checkInvariants(scrapedMetrics, apiServerInvariants, failOnMissing)
}
