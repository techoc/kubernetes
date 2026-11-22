/*
Copyright The Kubernetes Authors.

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

package configuration

import (
	"reflect"
	"testing"

	v1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
)

// TestExcludedResourcesNamedByRule 验证按规则匹配“被排除资源”的逻辑：
// 只有规则里显式列出的 group/resource 才应命中，通配符（'*'）故意不展开，
// 因为通配符规则本身无法精确指向某个资源，误报会把整条 webhook 标记为
// 可能影响认证/授权这类系统关键路径。
func TestExcludedResourcesNamedByRule(t *testing.T) {
	tests := []struct {
		name      string
		apiGroups []string
		resources []string
		want      []schema.GroupResource
	}{
		{
			name:      "explicit excluded resource",
			apiGroups: []string{"authentication.k8s.io"}, resources: []string{"tokenreviews"},
			want: []schema.GroupResource{{Group: "authentication.k8s.io", Resource: "tokenreviews"}},
		},
		{
			name:      "non-excluded resource",
			apiGroups: []string{""}, resources: []string{"pods"},
			want: nil,
		},
		{
			name:      "wildcard group is not flagged",
			apiGroups: []string{"*"}, resources: []string{"tokenreviews"},
			want: nil,
		},
		{
			name:      "wildcard resource is not flagged",
			apiGroups: []string{"authentication.k8s.io"}, resources: []string{"*"},
			want: nil,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 固定一组默认排除项，模拟 apiserver 自带的“不应被 webhook 拦截”的资源集合。
			excluded := sets.New(
				schema.GroupResource{Group: "authentication.k8s.io", Resource: "tokenreviews"},
				schema.GroupResource{Group: "authorization.k8s.io", Resource: "subjectaccessreviews"},
			)
			if got := excludedResourcesNamedByRule(tc.apiGroups, tc.resources, excluded); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("excludedResourcesNamedByRule() = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestLogExcludedResourcesForWebhook is a smoke test ensuring the logging helpers run without
// panicking when no resources are excluded.
//
// 中文说明：这里只验证 mutating/validating 两条日志路径在“排除集合为空”时
// 不会因 nil 集合或空切片而 panic；日志内容本身不是断言目标。
func TestLogExcludedResourcesForWebhook(t *testing.T) {
	logExcludedResourcesForValidatingWebhook("test-config", []v1.ValidatingWebhook{{
		Name:  "test-webhook",
		Rules: []v1.RuleWithOperations{{Rule: v1.Rule{APIGroups: []string{"authentication.k8s.io"}, APIVersions: []string{"v1"}, Resources: []string{"tokenreviews"}}}},
	}}, nil)
	logExcludedResourcesForMutatingWebhook("test-config", []v1.MutatingWebhook{{
		Name:  "test-mutating-webhook",
		Rules: []v1.RuleWithOperations{{Rule: v1.Rule{APIGroups: []string{"authorization.k8s.io"}, APIVersions: []string{"v1"}, Resources: []string{"subjectaccessreviews"}}}},
	}}, nil)
}
