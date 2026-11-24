/*
Copyright 2015 The Kubernetes Authors.

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

package tokenreview

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/apis/authentication"
)

var badAuthenticatorAuds = apierrors.NewInternalError(errors.New("error validating audiences"))

// REST 实现 TokenReview 的无状态 REST 接口，通过请求认证器验证提交的 Token。
type REST struct {
	tokenAuthenticator authenticator.Request
	apiAudiences       []string
}

// NewREST 创建 TokenReview 处理器，并保存 Token 认证器及 API 默认受众集合。
func NewREST(tokenAuthenticator authenticator.Request, apiAudiences []string) *REST {
	return &REST{
		tokenAuthenticator: tokenAuthenticator,
		apiAudiences:       apiAudiences,
	}
}

// NamespaceScoped 返回 false，表示 TokenReview 是集群级资源。
func (r *REST) NamespaceScoped() bool {
	return false
}

// New 返回用于解码请求的空 TokenReview 对象。
func (r *REST) New() runtime.Object {
	return &authentication.TokenReview{}
}

// Destroy 实现 REST 销毁接口；处理器不持有底层 Store，无需显式释放资源。
func (r *REST) Destroy() {
}

var _ rest.SingularNameProvider = &REST{}

// GetSingularName 返回 TokenReview 的单数资源名称。
func (r *REST) GetSingularName() string {
	return "tokenreview"
}

// Create 校验 TokenReview 请求，使用 Bearer Token 构造认证请求，并将认证结果、用户身份和受众写入 Status。
func (r *REST) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	tokenReview, ok := obj.(*authentication.TokenReview)
	if !ok {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("not a TokenReview: %#v", obj))
	}
	namespace := genericapirequest.NamespaceValue(ctx)
	if len(namespace) != 0 {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("namespace is not allowed on this type: %v", namespace))
	}

	if len(tokenReview.Spec.Token) == 0 {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("token is required for TokenReview in authentication"))
	}

	if createValidation != nil {
		if err := createValidation(ctx, obj.DeepCopyObject()); err != nil {
			return nil, err
		}
	}

	if r.tokenAuthenticator == nil {
		return tokenReview, nil
	}

	// 构造仅包含待验证 Bearer Token 的 HTTP Authorization 请求头，避免携带原始请求中的其他头信息。
	fakeReq := &http.Request{Header: http.Header{}}
	fakeReq.Header.Add("Authorization", "Bearer "+tokenReview.Spec.Token)

	auds := tokenReview.Spec.Audiences
	if len(auds) == 0 {
		auds = r.apiAudiences
	}
	if len(auds) > 0 {
		fakeReq = fakeReq.WithContext(authenticator.WithAudiences(fakeReq.Context(), auds))
	}

	resp, ok, err := r.tokenAuthenticator.AuthenticateRequest(fakeReq)
	tokenReview.Status.Authenticated = ok
	if err != nil {
		tokenReview.Status.Error = err.Error()
	}

	if len(auds) > 0 && resp != nil && len(authenticator.Audiences(auds).Intersect(resp.Audiences)) == 0 {
		klog.Errorf("error validating audience. want=%q got=%q", auds, resp.Audiences)
		return nil, badAuthenticatorAuds
	}

	if resp != nil && resp.User != nil {
		tokenReview.Status.User = authentication.UserInfo{
			Username: resp.User.GetName(),
			UID:      resp.User.GetUID(),
			Groups:   resp.User.GetGroups(),
			Extra:    map[string]authentication.ExtraValue{},
		}
		for k, v := range resp.User.GetExtra() {
			tokenReview.Status.User.Extra[k] = authentication.ExtraValue(v)
		}
		tokenReview.Status.Audiences = resp.Audiences
	}

	return tokenReview, nil
}
