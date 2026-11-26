/*
Copyright 2018 The Kubernetes Authors.

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

package path

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

// NewAuthorizer 构造一个接受给定路径集合的授权器。
// 路径要么全匹配，要么以 * 结尾表示前缀匹配；开头的 / 可省略。
//
// 该授权器只处理非资源请求（/healthz、/metrics 等本地端点）：资源请求一律
// 返回 NoOpinion，交由后续授权器决定，避免误放行 API 对象访问。
// 通配符只允许出现在末尾，前缀匹配意味着授权范围随路径语义放大，
// 因此拒绝中间的 '*'，防止 "/*/foo" 这类含糊规则被误写成过度授权。
func NewAuthorizer(alwaysAllowPaths []string) (authorizer.Authorizer, error) {
	var prefixes []string
	paths := sets.NewString()
	for _, p := range alwaysAllowPaths {
		// 统一去掉前导 '/'，这样配置 "/healthz" 与 "healthz" 等价。
		p = strings.TrimPrefix(p, "/")
		if len(p) == 0 {
			// matches "/"
			paths.Insert(p)
			continue
		}
		// 检查除最后一个字符外是否还有 '*'：只允许尾随通配。
		if strings.ContainsRune(p[:len(p)-1], '*') {
			return nil, fmt.Errorf("only trailing * allowed in %q", p)
		}
		if strings.HasSuffix(p, "*") {
			prefixes = append(prefixes, p[:len(p)-1])
		} else {
			paths.Insert(p)
		}
	}

	return authorizer.AuthorizerFunc(func(ctx context.Context, a authorizer.Attributes) (authorizer.Decision, string, error) {
		// 只对非资源请求表态；资源请求留给 RBAC 等授权器。
		if a.IsResourceRequest() {
			return authorizer.DecisionNoOpinion, "", nil
		}

		pth := strings.TrimPrefix(a.GetPath(), "/")
		if paths.Has(pth) {
			return authorizer.DecisionAllow, "", nil
		}

		for _, prefix := range prefixes {
			if strings.HasPrefix(pth, prefix) {
				return authorizer.DecisionAllow, "", nil
			}
		}

		// 未匹配时返回 NoOpinion 而非 Deny，保证授权链中的后续授权器仍有决定权。
		return authorizer.DecisionNoOpinion, "", nil
	}), nil
}
