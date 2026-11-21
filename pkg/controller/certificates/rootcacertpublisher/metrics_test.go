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

package rootcacertpublisher

import (
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
)

// TestSyncCounter 验证同步计数器按错误类型归类：成功记为 200，
// API 错误取其 HTTP code，普通错误和无 code 的 StatusError 记为 500。
func TestSyncCounter(t *testing.T) {
	testCases := []struct {
		// desc 是用例名；err 是模拟的同步错误；metrics 是要抓取的指标；
		// want 是期望的 Prometheus 文本输出，含 code 标签。
		desc    string
		err     error
		metrics []string
		want    string
	}{
		{
			desc: "nil error", // 同步成功应记为 200
			err:  nil,
			metrics: []string{
				"root_ca_cert_publisher_sync_total",
			},
			want: `
# HELP root_ca_cert_publisher_sync_total [ALPHA] Number of namespace syncs happened in root ca cert publisher.
# TYPE root_ca_cert_publisher_sync_total counter
root_ca_cert_publisher_sync_total{code="200"} 1
				`,
		},
		{
			desc: "kube api error", // API 错误使用其 HTTP 状态码 404
			err:  apierrors.NewNotFound(corev1.Resource("configmap"), "test-configmap"),
			metrics: []string{
				"root_ca_cert_publisher_sync_total",
			},
			want: `
# HELP root_ca_cert_publisher_sync_total [ALPHA] Number of namespace syncs happened in root ca cert publisher.
# TYPE root_ca_cert_publisher_sync_total counter
root_ca_cert_publisher_sync_total{code="404"} 1
				`,
		},
		{
			desc: "kube api error without code", // 没有 code 的 StatusError 回退为 500
			err:  &apierrors.StatusError{},
			metrics: []string{
				"root_ca_cert_publisher_sync_total",
			},
			want: `
# HELP root_ca_cert_publisher_sync_total [ALPHA] Number of namespace syncs happened in root ca cert publisher.
# TYPE root_ca_cert_publisher_sync_total counter
root_ca_cert_publisher_sync_total{code="500"} 1
				`,
		},
		{
			desc: "general error", // 普通错误无法提取 code，回退为 500
			err:  errors.New("test"),
			metrics: []string{
				"root_ca_cert_publisher_sync_total",
			},
			want: `
# HELP root_ca_cert_publisher_sync_total [ALPHA] Number of namespace syncs happened in root ca cert publisher.
# TYPE root_ca_cert_publisher_sync_total counter
root_ca_cert_publisher_sync_total{code="500"} 1
				`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			recordMetrics(time.Now(), tc.err)
			// 计数器是全局状态，每个子测试后重置，避免用例间相互污染。
			defer syncCounter.Reset()
			// 比较完整的指标文本（含 HELP/TYPE 与标签），确保归类标签正确。
			if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(tc.want), tc.metrics...); err != nil {
				t.Fatal(err)
			}
		})
	}
}
