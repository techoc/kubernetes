/*
Copyright 2022 The Kubernetes Authors.

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

package tracing

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"k8s.io/klog/v2"
	utiltrace "k8s.io/utils/trace"
)

func init() {
	klog.InitFlags(flag.CommandLine)
	_ = flag.CommandLine.Lookup("logtostderr").Value.Set("false")
	_ = flag.CommandLine.Lookup("v").Value.Set("2")
}

// TestOpenTelemetryTracing 验证 Start/AddEvent/RecordError/ContextWithSpan 在
// OpenTelemetry 下生成正确的父子层级与事件序列。
func TestOpenTelemetryTracing(t *testing.T) {
	// 为测试配置 OpenTelemetry
	fakeRecorder := tracetest.NewSpanRecorder()
	otelTracer := trace.NewTracerProvider(trace.WithSpanProcessor(fakeRecorder)).Tracer(instrumentationScope)

	func() {
		ctx := context.Background()
		// 创建父 OpenTelemetry span
		ctx, span := otelTracer.Start(ctx, "parent otel span")
		defer span.End()

		// 创建子 span
		_, tr := Start(ctx, "frobber", attribute.String("foo", "bar"))
		defer tr.End(10 * time.Second)

		time.Sleep(5 * time.Millisecond)
		// 为 frobber span 添加一个事件
		tr.AddEvent("reticulated splines", attribute.Bool("should I do it?", false)) // 耗时 5ms
		time.Sleep(10 * time.Millisecond)

		// 为 frobber span 添加错误事件
		tr.RecordError(fmt.Errorf("something went wrong"))

		// 确认将 span 写入 context 后，下一个 span 会成为其子 span
		ctx = ContextWithSpan(context.Background(), tr)

		// 从 context 取出 span 后，为 frobber span 添加另一个事件
		SpanFromContext(ctx).AddEvent("sequenced particles", attribute.Int("inches in foot", 12)) // 耗时 10ms

		// 创建嵌套的子 span
		_, tr = Start(ctx, "nested child span")
		defer tr.End(10 * time.Second)
	}()

	output := fakeRecorder.Ended()
	if len(output) != 3 {
		t.Fatalf("got %d; expected len(output) == 3", len(output))
	}
	// 嵌套的子 span 最先结束
	nestedChild := output[0]
	if nestedChild.Name() != "nested child span" {
		t.Fatalf("got %s; expected nestedChild.Name() == nested child span", nestedChild.Name())
	}
	// 子 span 第二个结束
	child := output[1]
	if !nestedChild.Parent().Equal(child.SpanContext()) {
		t.Errorf("got child: %v, parent: %v; expected child.Parent() == parent.SpanContext()", nestedChild.Parent(), child.SpanContext())
	}
	if child.Name() != "frobber" {
		t.Errorf("got %s; expected child.Name() == frobber", child.Name())
	}
	if len(child.Attributes()) != 1 {
		t.Errorf("got attributes %v; expected one attribute in child.Attributes()", child.Attributes())
	}
	if len(child.Events()) != 3 {
		t.Errorf("got events %v; expected 2 events in child.Events()", child.Events())
	}
	if child.Events()[0].Name != "reticulated splines" {
		t.Errorf("got event %v; expected child.Events()[0].Name == reticulated splines", child.Events()[0])
	}
	if len(child.Events()[0].Attributes) != 1 {
		t.Errorf("got event %v; expected 1 attribute in child.Events()[0].Attributes", child.Events()[0])
	}
	if child.Events()[1].Name != "exception" {
		t.Errorf("got event %v; expected child.Events()[1].Name == something went wrong", child.Events()[1])
	}
	if len(child.Events()[1].Attributes) != 2 {
		t.Errorf("got event %#v; expected 2 attribute in child.Events()[1].Attributes", child.Events()[1])
	}
	if child.Events()[2].Name != "sequenced particles" {
		t.Errorf("got event %v; expected child.Events()[2].Name == sequenced particles", child.Events()[2])
	}
	if len(child.Events()[2].Attributes) != 1 {
		t.Errorf("got event %v; expected 1 attribute in child.Events()[2].Attributes", child.Events()[2])
	}
	// 父 span 最后结束
	parent := output[2]
	if !child.Parent().Equal(parent.SpanContext()) {
		t.Fatalf("got child: %v, parent: %v; expected child.Parent() == parent.SpanContext()", child.Parent(), parent.SpanContext())
	}
	if parent.Name() != "parent otel span" {
		t.Fatalf("got %s; expected parent.Name() == parent otel span", parent.Name())
	}
	if len(parent.Attributes()) != 0 {
		t.Fatalf("got attributes %v; expected empty parent.Attributes()", parent.Attributes())
	}
}

// TestUtilTracing 验证同一套 API 在 k8s.io/utils/trace 下的行为：
// 子 span 结束时不打印，父 span 结束时才一次性输出嵌套结构。
func TestUtilTracing(t *testing.T) {
	var buf bytes.Buffer
	klog.SetOutput(&buf)

	ctx := context.Background()
	// 创建 utiltrace span
	tr0 := utiltrace.New("parent utiltrace span")
	ctx = utiltrace.ContextWithTrace(ctx, tr0)

	// 创建子 span
	_, tr1 := Start(ctx, "frobber", attribute.String("foo", "bar"))

	time.Sleep(5 * time.Millisecond)
	// 为 frobber span 添加一个事件
	tr1.AddEvent("reticulated splines", attribute.Bool("should I do it?", false)) // 耗时 5ms

	time.Sleep(10 * time.Millisecond)

	// 确认将 span 写入 context 后，下一个 span 会成为其子 span
	ctx = ContextWithSpan(context.Background(), tr1)

	// 从 context 取出 span 后，为 frobber span 添加另一个事件
	SpanFromContext(ctx).AddEvent("sequenced particles", attribute.Int("inches in foot", 12)) // 耗时 10ms

	// 创建嵌套的子 span
	_, tr2 := Start(ctx, "nested child span")
	// 始终记录日志
	tr2.End(0 * time.Second)
	tr1.End(0 * time.Second)

	// 所有追踪均为嵌套关系，因此此时不应产生日志
	if buf.String() != "" {
		t.Errorf("child traces were printed out before the parent span completed: %v", buf.String())
	}

	// 结束父 span，触发日志输出
	tr0.Log()

	expected := []string{
		`"frobber" foo:bar`,
		`---"reticulated splines" should I do it?:false`,
		`---"sequenced particles" inches in foot:12`,
		`"nested child span"`,
		`"parent utiltrace span"`,
	}
	for _, msg := range expected {
		if !strings.Contains(buf.String(), msg) {
			t.Errorf("\nMsg %q not found in log: \n%v\n", msg, buf.String())
		}
	}
}

// TestContextNoPanic 验证空 context 取出的 span 可安全调用，不会触发 nil panic。
func TestContextNoPanic(t *testing.T) {
	ctx := context.Background()
	// 确认对从 context 取出的 span 调用方法不会触发 panic
	SpanFromContext(ctx).AddEvent("foo")
	SpanFromContext(ctx).End(time.Minute)
}
