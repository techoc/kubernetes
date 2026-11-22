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

package metrics

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	apimachineryversion "k8s.io/apimachinery/pkg/version"
)

// testCustomCollector 是 StableCollector 的测试实现：
// 固定一组 Desc，Collect 时每个 Desc 输出一个值为 1 的 gauge。
type testCustomCollector struct {
	BaseStableCollector

	descriptors []*Desc
}

// newTestCustomCollector 用给定 Desc 构造测试收集器。
func newTestCustomCollector(ds ...*Desc) *testCustomCollector {
	c := &testCustomCollector{}
	c.descriptors = append(c.descriptors, ds...)

	return c
}

// DescribeWithStability 把全部 Desc 送入通道，供注册表做唯一性校验。
func (tc *testCustomCollector) DescribeWithStability(ch chan<- *Desc) {
	for i := range tc.descriptors {
		ch <- tc.descriptors[i]
	}
}

// CollectWithStability 为每个 Desc 产出一个常量 gauge，便于断言 HELP/TYPE 文本。
func (tc *testCustomCollector) CollectWithStability(ch chan<- Metric) {
	for i := range tc.descriptors {
		ch <- NewLazyConstMetric(tc.descriptors[i], GaugeValue, 1, "value")
	}
}

// TestBaseCustomCollector 验证 StableCollector 注册后：
// 各稳定性级别 HELP 带前缀，deprecated 指标显示弃用版本，超过宽限期的 hidden 指标不出现。
func TestBaseCustomCollector(t *testing.T) {
	var currentVersion = apimachineryversion.Info{
		Major:      "1",
		Minor:      "17",
		GitVersion: "v1.17.0-alpha-1.12345",
	}

	var (
		alphaDesc = NewDesc("metric_alpha", "alpha metric", []string{"name"}, nil,
			ALPHA, "")
		internalDesc = NewDesc("metric_internal", "internal metrics", []string{"name"}, nil,
			INTERNAL, "")
		stableDesc = NewDesc("metric_stable", "stable metrics", []string{"name"}, nil,
			STABLE, "")
		deprecatedDesc = NewDesc("metric_deprecated", "stable deprecated metrics", []string{"name"}, nil,
			STABLE, "1.17.0")
		hiddenDesc = NewDesc("metric_hidden", "stable hidden metrics", []string{"name"}, nil,
			STABLE, "1.14.0")
	)

	registry := newKubeRegistry(currentVersion)
	customCollector := newTestCustomCollector(alphaDesc, internalDesc, stableDesc, deprecatedDesc, hiddenDesc)

	if err := registry.CustomRegister(customCollector); err != nil {
		t.Fatalf("register collector failed with err: %v", err)
	}

	expectedMetrics := `
        # HELP metric_alpha [ALPHA] alpha metric
        # TYPE metric_alpha gauge
        metric_alpha{name="value"} 1
		# HELP metric_internal [INTERNAL] internal metrics
        # TYPE metric_internal gauge
        metric_internal{name="value"} 1
        # HELP metric_stable [STABLE] stable metrics
        # TYPE metric_stable gauge
        metric_stable{name="value"} 1
        # HELP metric_deprecated [STABLE] (Deprecated since 1.17.0) stable deprecated metrics
        # TYPE metric_deprecated gauge
        metric_deprecated{name="value"} 1
	`

	err := testutil.GatherAndCompare(registry, strings.NewReader(expectedMetrics), alphaDesc.fqName,
		internalDesc.fqName, stableDesc.fqName, deprecatedDesc.fqName, hiddenDesc.fqName)
	if err != nil {
		t.Fatal(err)
	}
}

// TestInvalidCustomCollector 验证无名字指标和重名指标会在 MustRegister 时 panic，
// 避免坏指标静默进入注册表。
func TestInvalidCustomCollector(t *testing.T) {
	var currentVersion = apimachineryversion.Info{
		Major:      "1",
		Minor:      "17",
		GitVersion: "v1.17.0-alpha-1.12345",
	}
	var namelessDesc = NewDesc("", "this is a nameless metric", nil, nil, ALPHA, "")
	var duplicatedDescA = NewDesc("test_duplicated_metric", "this is a duplicated metric A", nil, nil, ALPHA, "")
	var duplicatedDescB = NewDesc("test_duplicated_metric", "this is a duplicated metric B", nil, nil, ALPHA, "")

	var tests = []struct {
		name        string
		descriptors []*Desc
		panicStr    string
	}{
		{
			name:        "nameless metric will be not allowed",
			descriptors: []*Desc{namelessDesc},
			panicStr:    "nameless metrics will be not allowed",
		},
		{
			name:        "duplicated metric will be not allowed",
			descriptors: []*Desc{duplicatedDescA, duplicatedDescB},
			panicStr:    fmt.Sprintf("duplicate metrics (%s) will be not allowed", duplicatedDescA.fqName),
		},
	}

	for _, test := range tests {
		tc := test
		t.Run(tc.name, func(t *testing.T) {
			registry := newKubeRegistry(currentVersion)
			customCollector := newTestCustomCollector(tc.descriptors...)
			assert.Panics(t, func() {
				registry.CustomMustRegister(customCollector)
			}, tc.panicStr)
		})
	}
}

// TestCustomCollectorClearState guarantees `ClearState()` will fully clear a collector.
// It is necessary because we may forget to clear some new-added fields in the future.
func TestCustomCollectorClearState(t *testing.T) {
	var currentVersion = parseVersion(apimachineryversion.Info{
		Major:      "1",
		Minor:      "17",
		GitVersion: "v1.17.0-alpha-1.12345",
	})

	var (
		alphaDesc = NewDesc("metric_alpha", "alpha metric", []string{"name"}, nil,
			ALPHA, "")
		stableDesc = NewDesc("metric_stable", "stable metrics", []string{"name"}, nil,
			STABLE, "")
		deprecatedDesc = NewDesc("metric_deprecated", "stable deprecated metrics", []string{"name"}, nil,
			STABLE, "1.17.0")
		hiddenDesc = NewDesc("metric_hidden", "stable hidden metrics", []string{"name"}, nil,
			STABLE, "1.16.0")
	)

	benchmarkA := newTestCustomCollector(alphaDesc, stableDesc, deprecatedDesc, hiddenDesc)
	benchmarkB := newTestCustomCollector(alphaDesc, stableDesc, deprecatedDesc, hiddenDesc)

	// Create 会在收集器上标记隐藏/弃用状态；ClearState 应把这些状态全部还原。
	if benchmarkA.Create(&currentVersion, benchmarkA) == false {
		t.Fatal("collector should be created")
	}
	benchmarkA.ClearState()

	// 用同样构造的干净副本比较，防止将来新增字段后忘记在 ClearState 中重置。
	if !reflect.DeepEqual(*benchmarkA, *benchmarkB) {
		t.Fatal("custom collector state hasn't be fully cleared")
	}
}
