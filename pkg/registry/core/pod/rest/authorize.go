/*
Copyright 2025 The Kubernetes Authors.

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

package rest

import (
	"context"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/filters"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
)

// Exec/Attach/PortForward 的授权动词会受 SPDY 与 WebSocket 协议影响：SPDY POST 映射为 create，WebSocket GET 映射为 get。
// 为兼容既有 RBAC 策略，在特性启用时额外检查 create 权限。
// 参考：https://issues.k8s.io/133515
// ensureAuthorizedForVerb 在原始 HTTP 动词授权之外，再用指定 verb 构造一份授权属性并请求授权器判断。
func ensureAuthorizedForVerb(ctx context.Context, a authorizer.UnconditionalAuthorizer, verb string) error {
	// 请求上下文缺少 RequestInfo 时无法确定资源和名称，按 API Server 内部错误处理。
	requestInfo, ok := genericapirequest.RequestInfoFrom(ctx)
	if !ok {
		return apierrors.NewInternalError(errors.New("no request info in context"))
	}
	// 原始请求动词已经等于目标动词，外层过滤器刚完成授权，无需重复发起检查。
	if requestInfo.Verb == verb {
		return nil
	}

	if a == nil {
		return apierrors.NewInternalError(errors.New("no authorizer available"))
	}
	originalAttrs, err := filters.GetAuthorizerAttributes(ctx)
	if err != nil {
		return apierrors.NewInternalError(fmt.Errorf("error building authorizer attributes: %w", err))
	}
	// 复用原请求的用户、资源、namespace 等属性，只替换 verb；这样不会丢失原请求的身份上下文。
	authorized, reason, err := a.Authorize(ctx, &overrideVerb{Attributes: originalAttrs, verb: verb})
	if err != nil {
		return err
	}
	if authorized != authorizer.DecisionAllow {
		return apierrors.NewForbidden(schema.GroupResource{Group: requestInfo.APIGroup, Resource: requestInfo.Resource}, requestInfo.Name, errors.New(reason))
	}
	return nil
}

// overrideVerb 嵌入原始授权属性，只覆盖 GetVerb 的返回值，避免复制/重建其余属性。
type overrideVerb struct {
	authorizer.Attributes
	verb string
}

func (o *overrideVerb) GetVerb() string {
	return o.verb
}
