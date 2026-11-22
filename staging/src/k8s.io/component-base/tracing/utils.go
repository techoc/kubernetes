/*
Copyright 2021 The Kubernetes Authors.

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
	"context"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
	noopoteltrace "go.opentelemetry.io/otel/trace/noop"

	"k8s.io/client-go/transport"
	v1 "k8s.io/component-base/tracing/api/v1"
)

// TracerProvider is an OpenTelemetry TracerProvider which can be shut down
// 在 otel 接口上补充 Shutdown，使组件可以在退出时冲刷并关闭 exporter。
type TracerProvider interface {
	oteltrace.TracerProvider
	Shutdown(context.Context) error
}

// noopTracerProvider 让空 tracing 配置也能返回一个可安全 Shutdown 的 provider。
type noopTracerProvider struct {
	oteltrace.TracerProvider
}

// Shutdown 对空 provider 是空操作。
func (n *noopTracerProvider) Shutdown(context.Context) error {
	return nil
}

// NewNoopTracerProvider 返回不做任何采集的 provider；
// 它仍保留 context 传播能力（在 WithTracing/WrapperFor 中由 otelhttp 处理）。
func NewNoopTracerProvider() TracerProvider {
	return &noopTracerProvider{TracerProvider: noopoteltrace.NewTracerProvider()}
}

// NewProvider creates a TracerProvider in a component, and enforces recommended tracing behavior
func NewProvider(ctx context.Context,
	tracingConfig *v1.TracingConfiguration,
	addedOpts []otlptracegrpc.Option,
	resourceOpts []resource.Option,
) (TracerProvider, error) {
	// 未配置 tracing 时返回 noop，调用方无需到处判空。
	if tracingConfig == nil {
		return NewNoopTracerProvider(), nil
	}
	// 复制一份，避免对调用方传入的切片产生副作用。
	opts := append([]otlptracegrpc.Option{}, addedOpts...)
	if tracingConfig.Endpoint != nil {
		opts = append(opts, otlptracegrpc.WithEndpoint(*tracingConfig.Endpoint))
	}
	// Kubernetes 组件间的 OTLP 默认不启用 TLS；需要 TLS 的部署通过 addedOpts 覆盖。
	opts = append(opts, otlptracegrpc.WithInsecure())
	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, err
	}
	res, err := resource.New(ctx, resourceOpts...)
	if err != nil {
		return nil, err
	}

	// sampler respects parent span's sampling rate or
	// otherwise never samples.
	// 默认不采样：tracing 是可选能力，未显式配置时避免产生额外开销。
	sampler := sdktrace.NeverSample()
	// Or, emit spans for a fraction of transactions
	if tracingConfig.SamplingRatePerMillion != nil && *tracingConfig.SamplingRatePerMillion > 0 {
		sampler = sdktrace.TraceIDRatioBased(float64(*tracingConfig.SamplingRatePerMillion) / float64(1000000))
	}
	// batch span processor to aggregate spans before export.
	bsp := sdktrace.NewBatchSpanProcessor(exporter)
	// ParentBased 保证已采样链路的子 span 跟随父决策，避免断链。
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.ParentBased(sampler)),
		sdktrace.WithSpanProcessor(bsp),
		sdktrace.WithResource(res),
	)
	return tp, nil
}

// WithTracing adds tracing to requests if the incoming request is sampled
func WithTracing(handler http.Handler, tp oteltrace.TracerProvider, spanName string) http.Handler {
	opts := []otelhttp.Option{
		otelhttp.WithPropagators(Propagators()),
		otelhttp.WithTracerProvider(tp),
	}
	wrappedHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Add the http.target attribute to the otelhttp span
		// Workaround for https://github.com/open-telemetry/opentelemetry-go-contrib/issues/3743
		// otelhttp 目前不会自动补充 http.target，这里手动设置以保留原始的请求 URI。
		if r.URL != nil {
			oteltrace.SpanFromContext(r.Context()).SetAttributes(attribute.Key("http.target").String(r.URL.RequestURI()))
		}
		handler.ServeHTTP(w, r)
	})
	// With Noop TracerProvider, the otelhttp still handles context propagation.
	// See https://github.com/open-telemetry/opentelemetry-go/tree/main/example/passthrough
	return otelhttp.NewHandler(wrappedHandler, spanName, opts...)
}

// WrapperFor can be used to add tracing to a *rest.Config.
// Example usage:
// tp := NewProvider(...)
// config, _ := rest.InClusterConfig()
// config.Wrap(WrapperFor(tp))
// kubeclient, _ := clientset.NewForConfig(config)
func WrapperFor(tp oteltrace.TracerProvider) transport.WrapperFunc {
	return func(rt http.RoundTripper) http.RoundTripper {
		opts := []otelhttp.Option{
			otelhttp.WithPropagators(Propagators()),
			otelhttp.WithTracerProvider(tp),
		}
		// With Noop TracerProvider, the otelhttp still handles context propagation.
		// See https://github.com/open-telemetry/opentelemetry-go/tree/main/example/passthrough
		// 即使 provider 为 noop，也保留 traceparent/baggage 的透传。
		return otelhttp.NewTransport(rt, opts...)
	}
}

// Propagators returns the recommended set of propagators.
// 使用 W3C TraceContext + Baggage，与 otel 生态的默认互操作格式一致。
func Propagators() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
}
