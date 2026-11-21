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

package clustertrustbundlepublisher

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	certificatesv1 "k8s.io/api/certificates/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/component-base/metrics/testutil"
)

// TestSyncCounter 验证 recordMetrics 会把同步结果按错误类型映射为正确的 HTTP code 标签，
// 覆盖成功、API 错误（含嵌套错误）和一般错误等场景。
func TestSyncCounter(t *testing.T) {
	testCases := []struct {
		desc    string
		err     error
		metrics []string
		want    string
	}{
		{
			// 成功同步应记为 200，证明无错误时不会错误地归类为失败。
			desc: "nil error",
			err:  nil,
			metrics: []string{
				"clustertrustbundle_publisher_sync_total",
			},
			want: `
# HELP clustertrustbundle_publisher_sync_total [ALPHA] Number of syncs that occurred in cluster trust bundle publisher.
# TYPE clustertrustbundle_publisher_sync_total counter
clustertrustbundle_publisher_sync_total{code="200"} 1
				`,
		},
		{
			desc: "kube api error",
			err:  apierrors.NewNotFound(certificatesv1.Resource("clustertrustbundle"), "test.test:testSigner:something"),
			metrics: []string{
				"clustertrustbundle_publisher_sync_total",
			},
			want: `
# HELP clustertrustbundle_publisher_sync_total [ALPHA] Number of syncs that occurred in cluster trust bundle publisher.
# TYPE clustertrustbundle_publisher_sync_total counter
clustertrustbundle_publisher_sync_total{code="404"} 1
				`,
		},
		{
			// 包装后的 API 错误仍能提取出 code，防止嵌套后丢掉错误分类。
			desc: "nested kube api error",
			err:  fmt.Errorf("oh noes: %w", apierrors.NewBadRequest("bad request!")),
			metrics: []string{
				"clustertrustbundle_publisher_sync_total",
			},
			want: `
# HELP clustertrustbundle_publisher_sync_total [ALPHA] Number of syncs that occurred in cluster trust bundle publisher.
# TYPE clustertrustbundle_publisher_sync_total counter
clustertrustbundle_publisher_sync_total{code="400"} 1
				`,
		},
		{
			// 无法提取 code 的 API 错误必须降级为 500，避免产生无意义或空的标签值。
			desc: "kube api error without code",
			err:  &apierrors.StatusError{},
			metrics: []string{
				"clustertrustbundle_publisher_sync_total",
			},
			want: `
# HELP clustertrustbundle_publisher_sync_total [ALPHA] Number of syncs that occurred in cluster trust bundle publisher.
# TYPE clustertrustbundle_publisher_sync_total counter
clustertrustbundle_publisher_sync_total{code="500"} 1
				`,
		},
		{
			// 非 API 错误统一记为 500，保证指标标签取值集合稳定。
			desc: "general error",
			err:  errors.New("test"),
			metrics: []string{
				"clustertrustbundle_publisher_sync_total",
			},
			want: `
# HELP clustertrustbundle_publisher_sync_total [ALPHA] Number of syncs that occurred in cluster trust bundle publisher.
# TYPE clustertrustbundle_publisher_sync_total counter
clustertrustbundle_publisher_sync_total{code="500"} 1
				`,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			recordMetrics(time.Now(), tc.err)
			// 计数器是全局共享的，每个用例结束必须重置，防止前一个用例的累加污染后续断言。
			defer syncCounter.Reset()
			// 对比完整指标文本（含标签），确保 code 标签值和计数都精确匹配预期。
			if err := testutil.GatherAndCompare(legacyregistry.DefaultGatherer, strings.NewReader(tc.want), tc.metrics...); err != nil {
				t.Fatal(err)
			}
		})
	}
}
