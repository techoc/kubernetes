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

package filters

import (
	"net/http"

	"k8s.io/apimachinery/pkg/types"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/audit"

	"github.com/google/uuid"
)

// WithAuditInit 初始化审计上下文，并把与请求关联的 Audit-ID 附加到上下文。
//
// a. 如果调用方没有在请求头中指定 Audit-ID，我们会生成一个新的审计 ID
// b. 我们通过响应头 'Audit-ID' 把 Audit-ID 的值回显给调用方。
//
// 该过滤器必须位于审计过滤器之前：后续的审计逻辑依赖 context 中的审计上下文与 ID。
func WithAuditInit(handler http.Handler) http.Handler {
	return withAuditInit(handler, func() string {
		return uuid.New().String()
	})
}

// withAuditInit 抽出 ID 生成函数以便测试注入确定性 ID。
func withAuditInit(handler http.Handler, newAuditIDFunc func() string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := audit.WithAuditContext(r.Context())
		r = r.WithContext(ctx)

		// 优先沿用调用方传入的 ID：kubectl 等客户端会带着同一个 ID 重试或串联多跳，
		// 保持原值才能把一次逻辑操作的多条审计记录关联起来。
		auditID := r.Header.Get(auditinternal.HeaderAuditID)
		if len(auditID) == 0 {
			auditID = newAuditIDFunc()
		}

		// 注意：原样保存用户指定的 Audit-ID 头值，不做截断。
		audit.WithAuditID(ctx, types.UID(auditID))

		// 把 Audit-ID 回显到响应头。并不保证所有请求都能收到 Audit-ID HTTP 头，
		// 例如用户执行 "kubectl exec" 时，apiserver 用代理 handler 处理请求，
		// 用户只能拿到 kubelet 节点返回的 HTTP 头。
		//
		// 该过滤器也会被其他聚合 apiserver 使用。对聚合 API 来说，
		// 同一个审计 ID 不应该出现多次。
		//
		// 先判断是否已有值再写：聚合服务器链路中前面的 apiserver 可能已经回写了 ID，
		// 重复 Set 会让客户端看到多个值。
		if value := w.Header().Get(auditinternal.HeaderAuditID); len(value) == 0 {
			w.Header().Set(auditinternal.HeaderAuditID, auditID)
		}

		handler.ServeHTTP(w, r)
	})
}
