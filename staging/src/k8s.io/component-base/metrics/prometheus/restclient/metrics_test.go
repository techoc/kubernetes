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

package restclient

import (
	"context"
	"strings"
	"testing"

	"k8s.io/client-go/tools/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
)

// TestMain 确保本包 init 通过 RegisterOpts.RegisterFn 安装的延迟注册回调，
// 在任何测试执行适配器路径前触发。生产环境中该回调由
// rest.RESTClientForConfigAndClient 调用 metrics.EnsureRegistered 触发；
// 这些测试绕过 REST client 构造，直接写入适配器。
func TestMain(m *testing.M) {
	metrics.EnsureRegistered()
	m.Run()
}

// TestClientGOMetrics 验证 client-go 各钩子（请求、重试、exec 插件、传输缓存）
// 都能落到对应的 Prometheus 指标，且标签名与 HELP 文本稳定。
func TestClientGOMetrics(t *testing.T) {
	tests := []struct {
		description string
		name        string
		metric      interface{}
		update      func()
		want        string
	}{
		{
			description: "Number of HTTP requests, partitioned by status code, verb, and host.",
			name:        "rest_client_requests_total",
			metric:      requestResult,
			update: func() {
				metrics.RequestResult.Increment(context.TODO(), "200", "POST", "www.foo.com")
			},
			want: `
			            # HELP rest_client_requests_total [ALPHA] Number of HTTP requests, partitioned by status code, method, and host.
			            # TYPE rest_client_requests_total counter
			            rest_client_requests_total{code="200",host="www.foo.com",method="POST"} 1
				`,
		},
		{
			description: "Number of request retries, partitioned by status code, verb, and host.",
			name:        "rest_client_request_retries_total",
			metric:      requestRetry,
			update: func() {
				metrics.RequestRetry.IncrementRetry(context.TODO(), "500", "GET", "www.bar.com")
			},
			want: `
			            # HELP rest_client_request_retries_total [ALPHA] Number of request retries, partitioned by status code, verb, and host.
			            # TYPE rest_client_request_retries_total counter
			            rest_client_request_retries_total{code="500",host="www.bar.com",verb="GET"} 1
				`,
		},
		{
			description: "Number of calls to an exec plugin",
			name:        "rest_client_exec_plugin_call_total",
			metric:      execPluginCalls,
			update: func() {
				metrics.ExecPluginCalls.Increment(0, "no_error")
			},
			want: `
						# HELP rest_client_exec_plugin_call_total [ALPHA] Number of calls to an exec plugin, partitioned by the type of event encountered (no_error, plugin_execution_error, plugin_not_found_error, client_internal_error) and an optional exit code. The exit code will be set to 0 if and only if the plugin call was successful.
        				# TYPE rest_client_exec_plugin_call_total counter
        				rest_client_exec_plugin_call_total{call_status="no_error",code="0"} 1
				`,
		},
		{
			description: "Number of calls to get a new transport",
			name:        "rest_client_transport_create_calls_total",
			metric:      transportCacheCalls,
			update: func() {
				metrics.TransportCreateCalls.Increment("hit")
			},
			want: `
			            # HELP rest_client_transport_create_calls_total [ALPHA] Number of calls to get a new transport, partitioned by the result of the operation hit: obtained from the cache, miss: created and added to the cache, miss-gc: recreated and added back to the cache after being garbage collected, uncacheable: created and not cached
			            # TYPE rest_client_transport_create_calls_total counter
			            rest_client_transport_create_calls_total{result="hit"} 1
				`,
		},
	}

	// 此处无需注册指标，因为本包的 init 函数已注册全部 client-go 指标。
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			resetter, resettable := test.metric.(interface {
				Reset()
			})
			if !resettable {
				t.Fatalf("the metric must be resettaable: %s", test.name)
			}

			// 全局注册表意味着其他测试可能已更新指标，因此必须先清零并串行执行，
			// 否则计数会与断言期望不一致。
			resetter.Reset()
			test.update()

			if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(test.want), test.name); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestTransportCAReloadsMetric 覆盖 CA 重载的成功/失败与各类原因组合，
// 确认 result 与 reason 两个标签都被正确传递。
func TestTransportCAReloadsMetric(t *testing.T) {
	tests := []struct {
		description string
		name        string
		metric      interface{}
		update      func()
		want        string
	}{
		{
			description: "Reload success, reason: unchanged",
			name:        "rest_client_transport_ca_reload_total",
			metric:      transportCAReloads,
			update: func() {
				metrics.TransportCAReloads.Increment("success", "unchanged")
			},
			want: `
			            # HELP rest_client_transport_ca_reload_total [ALPHA] Number of times a CA reload is attempted, partitioned by the result and reason for the reload attempt
			            # TYPE rest_client_transport_ca_reload_total counter
			            rest_client_transport_ca_reload_total{reason="unchanged", result="success"} 1
				`,
		},
		{
			description: "Reload success, reason: updated",
			name:        "rest_client_transport_ca_reload_total",
			metric:      transportCAReloads,
			update: func() {
				metrics.TransportCAReloads.Increment("success", "updated")
			},
			want: `
			            # HELP rest_client_transport_ca_reload_total [ALPHA] Number of times a CA reload is attempted, partitioned by the result and reason for the reload attempt
			            # TYPE rest_client_transport_ca_reload_total counter
			            rest_client_transport_ca_reload_total{reason="updated", result="success"} 1
				`,
		},
		{
			description: "Reload failure, reason: empty",
			name:        "rest_client_transport_ca_reload_total",
			metric:      transportCAReloads,
			update: func() {
				metrics.TransportCAReloads.Increment("failure", "empty")
			},
			want: `
			            # HELP rest_client_transport_ca_reload_total [ALPHA] Number of times a CA reload is attempted, partitioned by the result and reason for the reload attempt
			            # TYPE rest_client_transport_ca_reload_total counter
			            rest_client_transport_ca_reload_total{reason="empty", result="failure"} 1
				`,
		},
		{
			description: "Reload failure, reason: read_error",
			name:        "rest_client_transport_ca_reload_total",
			metric:      transportCAReloads,
			update: func() {
				metrics.TransportCAReloads.Increment("failure", "read_error")
			},
			want: `
			            # HELP rest_client_transport_ca_reload_total [ALPHA] Number of times a CA reload is attempted, partitioned by the result and reason for the reload attempt
			            # TYPE rest_client_transport_ca_reload_total counter
			            rest_client_transport_ca_reload_total{reason="read_error", result="failure"} 1
				`,
		},
		{
			description: "Reload failure, reason: ca_parse_error",
			name:        "rest_client_transport_ca_reload_total",
			metric:      transportCAReloads,
			update: func() {
				metrics.TransportCAReloads.Increment("failure", "ca_parse_error")
			},
			want: `
			            # HELP rest_client_transport_ca_reload_total [ALPHA] Number of times a CA reload is attempted, partitioned by the result and reason for the reload attempt
			            # TYPE rest_client_transport_ca_reload_total counter
			            rest_client_transport_ca_reload_total{reason="ca_parse_error", result="failure"} 1
				`,
		},
	}
	// 此处无需注册指标，因为本包的 init 函数已注册全部 client-go 指标。
	for _, test := range tests {
		t.Run(test.description, func(t *testing.T) {
			resetter, resettable := test.metric.(interface {
				Reset()
			})
			if !resettable {
				t.Fatalf("the metric must be resettaable: %s", test.name)
			}

			// 全局注册表意味着其他测试可能已更新指标，因此必须先清零并串行执行，
			// 否则计数会与断言期望不一致。
			resetter.Reset()
			test.update()

			if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(test.want), test.name); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestTransportCleanupMetrics 验证传输缓存 GC 相关计数：
// 取消回调调用次数，以及删除入口时 deleted/skipped 两种结果。
func TestTransportCleanupMetrics(t *testing.T) {
	t.Run("cleanup cancel calls", func(t *testing.T) {
		transportCertRotationGCCalls.Reset()
		metrics.TransportCertRotationGCCalls.Increment()
		metrics.TransportCertRotationGCCalls.Increment()

		want := `
			# HELP rest_client_transport_cert_rotation_gc_calls_total [ALPHA] Number of times a cert rotation goroutine cancel func is called via GC cleanup of the associated transport
			# TYPE rest_client_transport_cert_rotation_gc_calls_total counter
			rest_client_transport_cert_rotation_gc_calls_total 2
		`
		if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(want), "rest_client_transport_cert_rotation_gc_calls_total"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("cleanup delete calls: deleted", func(t *testing.T) {
		transportCacheGCCalls.Reset()
		metrics.TransportCacheGCCalls.Increment("deleted")

		want := `
			# HELP rest_client_transport_cache_gc_calls_total [ALPHA] Number of times a GC cleanup attempts to delete a transport cache entry, partitioned by the result: deleted, skipped
			# TYPE rest_client_transport_cache_gc_calls_total counter
			rest_client_transport_cache_gc_calls_total{result="deleted"} 1
		`
		if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(want), "rest_client_transport_cache_gc_calls_total"); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("cleanup delete calls: skipped", func(t *testing.T) {
		transportCacheGCCalls.Reset()
		metrics.TransportCacheGCCalls.Increment("skipped")

		want := `
			# HELP rest_client_transport_cache_gc_calls_total [ALPHA] Number of times a GC cleanup attempts to delete a transport cache entry, partitioned by the result: deleted, skipped
			# TYPE rest_client_transport_cache_gc_calls_total counter
			rest_client_transport_cache_gc_calls_total{result="skipped"} 1
		`
		if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(want), "rest_client_transport_cache_gc_calls_total"); err != nil {
			t.Fatal(err)
		}
	})
}
