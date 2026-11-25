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
	"bytes"
	"context"
	"sync"
	"testing"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	apimachineryversion "k8s.io/apimachinery/pkg/version"
)

// TestCounter 验证单个 Counter 的暴露与累加：
// 期望的 HELP 前缀反映稳定性级别；已弃用/已隐藏的指标要么带弃用说明，
// 要么完全不暴露。
func TestCounter(t *testing.T) {
	version1_15Alpha1 := apimachineryversion.Info{
		Major:      "1",
		Minor:      "15",
		GitVersion: "v1.15.0-alpha-1.12345",
	}

	var tests = []struct {
		desc string
		*CounterOpts
		currentVersion      apimachineryversion.Info
		expectedMetricCount int
		expectedHelp        string
	}{
		// 非弃用指标
		{
			desc: "ALPHA metric non deprecated",
			CounterOpts: &CounterOpts{
				Namespace:      "namespace",
				Name:           "metric_test_name",
				Subsystem:      "subsystem",
				StabilityLevel: ALPHA,
				Help:           "counter help",
			},
			expectedMetricCount: 1,
			expectedHelp:        "[ALPHA] counter help",
		},
		{
			desc: "BETA metric non deprecated",
			CounterOpts: &CounterOpts{
				Namespace:      "namespace",
				Name:           "metric_test_name",
				Subsystem:      "subsystem",
				StabilityLevel: BETA,
				Help:           "counter help",
			},
			expectedMetricCount: 1,
			expectedHelp:        "[BETA] counter help",
		},
		{
			desc: "STABLE metric non deprecated",
			CounterOpts: &CounterOpts{
				Namespace:      "namespace",
				Name:           "metric_test_name",
				Subsystem:      "subsystem",
				StabilityLevel: STABLE,
				Help:           "counter help",
			},
			expectedMetricCount: 1,
			expectedHelp:        "[STABLE] counter help",
		},
		// 已弃用指标
		{
			desc: "ALPHA metric deprecated",
			CounterOpts: &CounterOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				Help:              "counter help",
				StabilityLevel:    ALPHA,
				DeprecatedVersion: "1.15.0",
			},
			expectedMetricCount: 0,
			expectedHelp:        "counter help",
		},
		{
			desc: "BETA metric deprecated",
			CounterOpts: &CounterOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				Help:              "counter help",
				StabilityLevel:    BETA,
				DeprecatedVersion: "1.15.0",
			},
			expectedMetricCount: 1,
			expectedHelp:        "[BETA] (Deprecated since 1.15.0) counter help",
		},
		{
			desc: "STABLE metric deprecated",
			CounterOpts: &CounterOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				Help:              "counter help",
				StabilityLevel:    STABLE,
				DeprecatedVersion: "1.14.0",
			},
			expectedMetricCount: 1,
			expectedHelp:        "[STABLE] (Deprecated since 1.14.0) counter help",
		},
		// 隐藏指标
		{
			desc: "ALPHA metric hidden",
			CounterOpts: &CounterOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				Help:              "counter help",
				StabilityLevel:    ALPHA,
				DeprecatedVersion: "1.15.0",
			},
			expectedMetricCount: 0,
			expectedHelp:        "counter help",
		},
		{
			desc: "BETA metric hidden",
			CounterOpts: &CounterOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				Help:              "counter help",
				StabilityLevel:    BETA,
				DeprecatedVersion: "1.14.0",
			},
			expectedMetricCount: 0},
		{
			desc: "STABLE metric hidden",
			CounterOpts: &CounterOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				Help:              "counter help",
				StabilityLevel:    STABLE,
				DeprecatedVersion: "1.12.0",
			},
			expectedMetricCount: 0,
			expectedHelp:        "counter help",
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			registry := newKubeRegistry(version1_15Alpha1)
			// c 是 Counter 指针。
			c := NewCounter(test.CounterOpts)
			registry.MustRegister(c)
			// mfs 是 dto.MetricFamily 指针切片。
			mfs, err := registry.Gather()
			var buf bytes.Buffer
			enc := expfmt.NewEncoder(&buf, "text/plain; version=0.0.4; charset=utf-8")
			assert.Lenf(t, mfs, test.expectedMetricCount, "Got %v metrics, Want: %v metrics", len(mfs), test.expectedMetricCount)
			require.NoError(t, err, "Gather failed %v", err)
			for _, metric := range mfs {
				err := enc.Encode(metric)
				require.NoError(t, err, "Unexpected err %v in encoding the metric", err)
				assert.Equalf(t, test.expectedHelp, metric.GetHelp(), "Got %s as help message, want %s", metric.GetHelp(), test.expectedHelp)
			}

			// 将 Counter 累加 N 次，验证指标正确保留计数值。
			numberOfTimesToIncrement := 3
			for i := 0; i < numberOfTimesToIncrement; i++ {
				c.Inc()
			}
			mfs, err = registry.Gather()
			require.NoError(t, err, "Gather failed %v", err)

			for _, mf := range mfs {
				mfMetric := mf.GetMetric()
				for _, m := range mfMetric {
					assert.Equalf(t, numberOfTimesToIncrement, int(m.GetCounter().GetValue()), "Got %v, wanted %v as the count", m.GetCounter().GetValue(), numberOfTimesToIncrement)
				}
			}
		})
	}
}

// TestCounterVec 验证带 label 的 Counter 在部分指标被隐藏时，
// 其余 label 组合仍能正常计数与暴露。
func TestCounterVec(t *testing.T) {
	version1_15Alpha1 := apimachineryversion.Info{
		Major:      "1",
		Minor:      "15",
		GitVersion: "v1.15.0-alpha-1.12345",
	}

	var tests = []struct {
		desc string
		*CounterOpts
		labels                    []string
		expectedMetricFamilyCount int
		expectedHelp              string
	}{
		// 非弃用指标
		{
			desc: "ALPHA metric non deprecated",
			CounterOpts: &CounterOpts{
				Namespace:      "namespace",
				Name:           "metric_test_name",
				Subsystem:      "subsystem",
				StabilityLevel: ALPHA,
				Help:           "counter help",
			},
			labels:                    []string{"label_a", "label_b"},
			expectedMetricFamilyCount: 1,
			expectedHelp:              "[ALPHA] counter help",
		},
		{
			desc: "BETA metric non deprecated",
			CounterOpts: &CounterOpts{
				Namespace:      "namespace",
				Name:           "metric_test_name",
				Subsystem:      "subsystem",
				StabilityLevel: BETA,
				Help:           "counter help",
			},
			labels:                    []string{"label_a", "label_b"},
			expectedMetricFamilyCount: 1,
			expectedHelp:              "[BETA] counter help",
		},
		{
			desc: "STABLE metric non deprecated",
			CounterOpts: &CounterOpts{
				Namespace:      "namespace",
				Name:           "metric_test_name",
				Subsystem:      "subsystem",
				StabilityLevel: STABLE,
				Help:           "counter help",
			},
			labels:                    []string{"label_a", "label_b"},
			expectedMetricFamilyCount: 1,
			expectedHelp:              "[STABLE] counter help",
		},
		// 已弃用指标
		{
			desc: "ALPHA metric deprecated",
			CounterOpts: &CounterOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    ALPHA,
				Help:              "counter help",
				DeprecatedVersion: "1.15.0",
			},
			labels:                    []string{"label_a", "label_b"},
			expectedMetricFamilyCount: 0,
			expectedHelp:              "counter help",
		},
		{
			desc: "BETA metric deprecated",
			CounterOpts: &CounterOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    BETA,
				Help:              "counter help",
				DeprecatedVersion: "1.15.0",
			},
			labels:                    []string{"label_a", "label_b"},
			expectedMetricFamilyCount: 1,
			expectedHelp:              "[BETA] (Deprecated since 1.15.0) counter help",
		},
		{
			desc: "STABLE metric deprecated",
			CounterOpts: &CounterOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    STABLE,
				Help:              "counter help",
				DeprecatedVersion: "1.15.0",
			},
			labels:                    []string{"label_a", "label_b"},
			expectedMetricFamilyCount: 1,
			expectedHelp:              "[STABLE] (Deprecated since 1.15.0) counter help",
		},
		// 隐藏指标
		{
			desc: "ALPHA metric hidden",
			CounterOpts: &CounterOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    ALPHA,
				Help:              "counter help",
				DeprecatedVersion: "1.14.0",
			},
			labels:                    []string{"label_a", "label_b"},
			expectedMetricFamilyCount: 0,
			expectedHelp:              "counter help",
		},
		{
			desc: "BETA metric hidden",
			CounterOpts: &CounterOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    BETA,
				Help:              "counter help",
				DeprecatedVersion: "1.14.0",
			},
			labels:                    []string{"label_a", "label_b"},
			expectedMetricFamilyCount: 0,
			expectedHelp:              "counter help",
		},
		{
			desc: "STABLE metric hidden",
			CounterOpts: &CounterOpts{
				Namespace:         "namespace",
				Name:              "metric_test_name",
				Subsystem:         "subsystem",
				StabilityLevel:    STABLE,
				Help:              "counter help",
				DeprecatedVersion: "1.12.0",
			},
			labels:                    []string{"label_a", "label_b"},
			expectedMetricFamilyCount: 0,
			expectedHelp:              "counter help",
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			registry := newKubeRegistry(version1_15Alpha1)
			c := NewCounterVec(test.CounterOpts, test.labels)
			registry.MustRegister(c)
			c.WithLabelValues("1", "2").Inc()
			mfs, err := registry.Gather()
			assert.Lenf(t, mfs, test.expectedMetricFamilyCount, "Got %v metric families, Want: %v metric families", len(mfs), test.expectedMetricFamilyCount)
			require.NoError(t, err, "Gather failed %v", err)

			// 指标隐藏时没有指标族，因此无需处理。
			for _, mf := range mfs {
				assert.Lenf(t, mf.GetMetric(), 1, "Got %v metrics, wanted 1 as the count", len(mf.GetMetric()))
				assert.Equalf(t, test.expectedHelp, mf.GetHelp(), "Got %s as help message, want %s", mf.GetHelp(), test.expectedHelp)
			}

			// 继续累加 Counter，验证指标仍能正常工作。
			c.WithLabelValues("1", "3").Inc()
			c.WithLabelValues("2", "3").Inc()
			mfs, err = registry.Gather()
			require.NoError(t, err, "Gather failed %v", err)

			// 指标隐藏时没有指标族，因此无需处理。
			for _, mf := range mfs {
				assert.Lenf(t, mf.GetMetric(), 3, "Got %v metrics, wanted 3 as the count", len(mf.GetMetric()))
			}
		})
	}
}

// TestCounterWithLabelValueAllowList 验证 allow list 的约束：
// 不在允许列表中的 label 值会被替换为固定值（unexpected），
// 从而限制指标基数。
func TestCounterWithLabelValueAllowList(t *testing.T) {
	labelAllowValues := map[string]string{
		"namespace_subsystem_metric_allowlist_test,label_a": "allowed",
	}
	labels := []string{"label_a", "label_b"}
	opts := &CounterOpts{
		Namespace: "namespace",
		Name:      "metric_allowlist_test",
		Subsystem: "subsystem",
	}
	var tests = []struct {
		desc               string
		labelValues        [][]string
		expectMetricValues map[string]int
	}{
		{
			desc:        "Test no unexpected input",
			labelValues: [][]string{{"allowed", "b1"}, {"allowed", "b2"}},
			expectMetricValues: map[string]int{
				"allowed b1": 1,
				"allowed b2": 1,
			},
		},
		{
			desc:        "Test unexpected input",
			labelValues: [][]string{{"allowed", "b1"}, {"not_allowed", "b1"}},
			expectMetricValues: map[string]int{
				"allowed b1":    1,
				"unexpected b1": 1,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			labelValueAllowLists = map[string]*MetricLabelAllowList{}

			registry := newKubeRegistry(apimachineryversion.Info{
				Major:      "1",
				Minor:      "15",
				GitVersion: "v1.15.0-alpha-1.12345",
			})
			c := NewCounterVec(opts, labels)
			registry.MustRegister(c)
			// allow list 必须在取出子指标之前设置，否则不会生效。
			SetLabelAllowList(labelAllowValues)
			for _, lv := range test.labelValues {
				c.WithLabelValues(lv...).Inc()
			}
			mfs, err := registry.Gather()
			require.NoError(t, err, "Gather failed %v", err)

			for _, mf := range mfs {
				if *mf.Name != BuildFQName(opts.Namespace, opts.Subsystem, opts.Name) {
					continue
				}
				mfMetric := mf.GetMetric()

				for _, m := range mfMetric {
					var aValue, bValue string
					for _, l := range m.Label {
						if *l.Name == "label_a" {
							aValue = *l.Value
						}
						if *l.Name == "label_b" {
							bValue = *l.Value
						}
					}
					labelValuePair := aValue + " " + bValue
					expectedValue, ok := test.expectMetricValues[labelValuePair]
					assert.True(t, ok, "Got unexpected label values, lable_a is %v, label_b is %v", aValue, bValue)
					actualValue := int(m.GetCounter().GetValue())
					assert.Equalf(t, expectedValue, actualValue, "Got %v, wanted %v as the count while setting label_a to %v and label b to %v", actualValue, expectedValue, aValue, bValue)
				}
			}
		})
	}
}

// TestCounterWithExemplar 验证 WithContext 会把 trace/span 信息作为
// exemplar 附加到指标上，供监控系统关联调用链。
func TestCounterWithExemplar(t *testing.T) {
	// 设置 exemplar。
	// 生成 16 字节的确定 ID；offset 用于让 traceID 与 spanID 不同。
	fn := func(offset int) []byte {
		arr := make([]byte, 16)
		for i := 0; i < 16; i++ {
			arr[i] = byte(2<<7 - i - offset)
		}
		return arr
	}
	traceID := trace.TraceID(fn(1))
	spanID := trace.SpanID(fn(2))
	ctxForSpanCtx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		SpanID:     spanID,
		TraceID:    traceID,
		TraceFlags: trace.FlagsSampled,
	}))
	toAdd := float64(40)

	// 创建带上下文的 Counter。
	counter := NewCounter(&CounterOpts{
		Name: "metric_exemplar_test",
		Help: "helpless",
	})

	// 注册 Counter。
	registry := newKubeRegistry(apimachineryversion.Info{
		Major:      "1",
		Minor:      "15",
		GitVersion: "v1.15.0-alpha-1.12345",
	})
	registry.MustRegister(counter)

	// 调用底层 exemplar 方法。
	// 三种调用方式都应命中同一个 counter 并携带 exemplar。
	counter.WithContext(ctxForSpanCtx).Add(toAdd)
	counter.WithContext(ctxForSpanCtx).Inc()
	counter.WithContext(ctxForSpanCtx).Inc()

	// 采集指标。
	mfs, err := registry.Gather()
	if err != nil {
		t.Fatalf("Gather failed %v", err)
	}
	if len(mfs) != 1 {
		t.Fatalf("Got %v metric families, Want: 1 metric family", len(mfs))
	}

	// 验证指标类型。
	mf := mfs[0]
	var m *dto.Metric
	switch mf.GetType() {
	case dto.MetricType_COUNTER:
		m = mfs[0].GetMetric()[0]
	default:
		t.Fatalf("Got %v metric type, Want: %v metric type", mf.GetType(), dto.MetricType_COUNTER)
	}

	// 验证数值。
	want := toAdd + 2
	got := m.GetCounter().GetValue()
	if got != want {
		t.Fatalf("Got %f, wanted %f as the count", got, want)
	}

	// 验证 exemplar。
	e := m.GetCounter().GetExemplar()
	if e == nil {
		t.Fatalf("Got nil exemplar, wanted an exemplar")
	}
	eLabels := e.GetLabel()
	if eLabels == nil {
		t.Fatalf("Got nil exemplar label, wanted an exemplar label")
	}
	if len(eLabels) != 2 {
		t.Fatalf("Got %v exemplar labels, wanted 2 exemplar labels", len(eLabels))
	}
	for _, l := range eLabels {
		switch *l.Name {
		case "trace_id":
			if *l.Value != traceID.String() {
				t.Fatalf("Got %s as traceID, wanted %s", *l.Value, traceID.String())
			}
		case "span_id":
			if *l.Value != spanID.String() {
				t.Fatalf("Got %s as spanID, wanted %s", *l.Value, spanID.String())
			}
		default:
			t.Fatalf("Got unexpected label %s", *l.Name)
		}
	}
}

// TestCounterConcurrentWithContextRace 复现 Counter.WithContext 中的数据竞争：
// withExemplar 方法读取 c.ctx 时，会与并发写入发生冲突。
// 该测试模拟触发竞争的实际认证流程：
// x509.AuthenticateRequest -> union.AuthenticateRequest -> group.AuthenticateRequest
//
// 该测试必须在 -race 下运行才有意义：它并发地写 WithContext 的 ctx
// 与读 exemplar，验证修复后不再存在数据竞争。
func TestCounterConcurrentWithContextRace(t *testing.T) {
	opts := &CounterOpts{
		Namespace: "apiserver",
		Subsystem: "authentication",
		Name:      "requests_total",
		Help:      "Authentication requests counter for race condition testing",
	}

	c := NewCounter(opts)

	// 直接调用 initializeMetric 强制初始化。
	c.initializeMetric()

	// 创建带 trace span 的上下文，触发 exemplar 代码路径。
	ctx1, span1 := createContextWithSpanCounter("x509-auth", "authenticate-request")
	defer span1.End()
	ctx2, span2 := createContextWithSpanCounter("union-auth", "union-authenticate")
	defer span2.End()

	var wg sync.WaitGroup
	iterations := 10000 // 增加迭代次数，提高触发数据竞争的概率

	// 协程 1：模拟 x509 Authenticator 调用 WithContext
	// 对应调用路径：x509.(*Authenticator).AuthenticateRequest() 调用 WithContext
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			// 模拟认证请求处理
			simulateX509AuthenticateRequestCounter(c, ctx1, i)
		}
	}()

	// 协程 2：模拟并发 Add/Inc 调用（指标采集）
	// 对应读取 c.ctx 的 withExemplar/Add 路径
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			c.Add(float64(i % 10))
			if i%2 == 0 {
				c.Inc()
			}
		}
	}()

	// 协程 3：模拟 union AuthenticateRequest 调用 WithContext
	// 对应调用路径：union.(*unionAuthRequestHandler).AuthenticateRequest()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			// 模拟 union 认证处理
			simulateUnionAuthenticateRequestCounter(c, ctx2, i)
		}
	}()

	// 协程 4：模拟 group AuthenticateRequest 调用 WithContext
	// 对应调用路径：group.(*AuthenticatedGroupAdder).AuthenticateRequest()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			// 模拟 group 认证处理
			simulateGroupAuthenticateRequestCounter(c, ctx1, ctx2, i)
		}
	}()

	wg.Wait()
}

// simulateX509AuthenticateRequestCounter 模拟 x509 认证器的调用路径。
func simulateX509AuthenticateRequestCounter(c CounterMetric, ctx context.Context, iteration int) {
	if cWithCtx, ok := c.(*Counter); ok {
		cWithCtx.WithContext(ctx)
	}
}

// simulateUnionAuthenticateRequestCounter 模拟 union 认证器的调用路径。
func simulateUnionAuthenticateRequestCounter(c CounterMetric, ctx context.Context, iteration int) {
	if cWithCtx, ok := c.(*Counter); ok {
		cWithCtx.WithContext(ctx)
	}
}

// simulateGroupAuthenticateRequestCounter 模拟 group 认证器的调用路径。
func simulateGroupAuthenticateRequestCounter(c CounterMetric, ctx1, ctx2 context.Context, iteration int) {
	// 在两个上下文之间交替，模拟不同的认证场景。
	ctx := ctx1
	if iteration%3 == 0 {
		ctx = ctx2
	}

	if cWithCtx, ok := c.(*Counter); ok {
		cWithCtx.WithContext(ctx)
	}
}

// 辅助函数：创建带有效 trace span 的上下文。
func createContextWithSpanCounter(traceID, spanID string) (context.Context, trace.Span) {
	ctx := context.Background()

	// 创建用于测试的 noop tracer 和 span。
	tracer := tracenoop.NewTracerProvider().Tracer("test")
	ctx, span := tracer.Start(ctx, "test-span")

	return ctx, span
}
