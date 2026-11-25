/*
Copyright 2024 The Kubernetes Authors.

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
	"fmt"
	"os"
	"testing"

	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	_ "k8s.io/component-base/logs/json/register"
	"k8s.io/dynamic-resource-allocation/structured"
	"k8s.io/kubernetes/pkg/features"
	perf "k8s.io/kubernetes/test/integration/scheduler_perf"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
)

func TestMain(m *testing.M) {
	if err := perf.InitTests(); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	m.Run()
}

func TestSchedulerPerf(t *testing.T) {
	// 使用所有可用的 allocator 验证行为正确。
	for _, allocatorName := range []string{"stable", "incubating", "experimental"} {
		t.Run(allocatorName, func(t *testing.T) {
			structured.EnableAllocators(allocatorName)
			defer structured.EnableAllocators()

			// 为了使用 "stable" 实现运行，我们必须禁用一些特性。
			// YAML 配置中没有指定这些特性，因为对于其他实现我们希望使用默认特性。
			// 在这里使用 "AllAlpha" 和 "AllBeta" 会更好，
			// 但会与之后将模拟版本设为 1.33 产生冲突
			// （"scheduler_perf.go:1117: failed to set emulation version to 1.33 during test:
			// cannot set feature gate NominatedNodeNameForExpectation to false, feature is PreAlpha at emulated version 1.33, ..."）。
			//
			// 一旦当前的 "incubating" 变成 "stable"，就可以将其替换为两个子测试：
			// - "ga-only"：继续禁用可选特性
			// - "default"：不更改特性
			var options []perf.SchedulerPerfOption
			if allocatorName == "stable" {
				options = append(options, perf.WithPreRunFn(func(tCtx ktesting.TContext, _ *perf.Workload) (func(), error) {
					gate := utilfeature.DefaultFeatureGate.(featuregate.MutableVersionedFeatureGate)
					overrides := featuregatetesting.FeatureOverrides{}
					// 如果版本模拟已经使特性关闭，
					// 那么我们不需要、甚至可能无法再关闭它们
					//（pre-alpha = 特性甚至不存在）。
					if gate.EmulationVersion().AtLeast(version.MustParse("1.34")) {
						overrides[features.DRAConsumableCapacity] = false
						overrides[features.DRADeviceBindingConditions] = false
					}
					featuregatetesting.SetFeatureGatesDuringTest(tCtx, utilfeature.DefaultFeatureGate, overrides)
					return nil, nil
				}))
			}

			perf.RunIntegrationPerfScheduling(t, "performance-config.yaml", options...)
		})
	}
}

// 这些基准测试必须包含 BenchmarkPerfScheduling 字样，才能被基准测试任务选中。
//
// 子测试本来也应该可以工作，但 gotestsum 不接受。

func BenchmarkPerfScheduling(b *testing.B) {
	// 将基准测试限制为默认的 allocator。
	structured.EnableAllocators("incubating")
	defer structured.EnableAllocators()

	// 传统上该基准测试被称为 "dra"。
	// 保留此名称是为了避免改变 perf-dash 的结果。
	perf.RunBenchmarkPerfScheduling(b, "performance-config.yaml", "dra", nil)
}

func BenchmarkPerfSchedulingExperimental(b *testing.B) {
	// 现在轮到实验性 allocator。
	structured.EnableAllocators("experimental")
	defer structured.EnableAllocators()

	perf.RunBenchmarkPerfScheduling(b, "performance-config.yaml", "dra_experimental", nil)
}
