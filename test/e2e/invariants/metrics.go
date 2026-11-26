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

// 向这个 package 添加内容前请先咨询 SIG-Testing 负责人
// see: https://git.k8s.io/enhancements/keps/sig-testing/5468-invariant-testing
package invariants

import (
	"context"

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/e2e/invariants/metrics"

	"github.com/onsi/ginkgo/v2"
	ginkgotypes "github.com/onsi/ginkgo/v2/types"
)

// 检查 api-server 指标中是否存在表明发生内部 bug 的情况
const invariantMetricsLeafText = "should enable checking invariant metrics"

var _ = framework.SIGDescribe("testing")("Invariant Metrics", func() {
	// 这个测试是选择 report after suite 逻辑的哨兵
	//
	// 这让我们能在多数 job 中默认运行它，但也可以选择退出，
	// 选择 Conformance 时不运行，遇到问题还可以
	// 给它打上 Flaky 标签
	ginkgo.It(invariantMetricsLeafText, func() {})
})

var _ = ginkgo.ReportAfterSuite("Invariant Metrics", func(ctx ginkgo.SpecContext, report ginkgo.Report) {
	// dry-run 模式下如果没真正运行任何测试，提前跳过
	if report.SuiteConfig.DryRun {
		return
	}
	// 检查是否运行了 'should enabled checking invariants' 这个"测试"
	invariantsSelected := false
	for _, spec := range report.SpecReports {
		if spec.LeafNodeText == invariantMetricsLeafText {
			invariantsSelected = spec.State.Is(ginkgotypes.SpecStatePassed)
			break
		}
	}
	// 如果关联的"测试"被跳过则跳过
	if !invariantsSelected {
		return
	}
	// 否则现在真正检查 invariants
	checkInvariantMetrics(ctx)
})

func checkInvariantMetrics(ctx context.Context) {
	// 获取指标
	config, err := framework.LoadConfig()
	if err != nil {
		framework.Failf("error loading client config: %v", err)
	}
	c, err := clientset.NewForConfig(config)
	if err != nil {
		framework.Failf("error loading client config: %v", err)
	}

	// 检查 invariant 指标。
	if err := metrics.CheckMetricInvariants(ctx, c, false); err != nil {
		framework.Failf("Invariant check failed: %v", err)
	}
}
