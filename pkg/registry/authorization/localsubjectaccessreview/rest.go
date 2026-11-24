/*
Copyright 2016 The Kubernetes Authors.

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

package localsubjectaccessreview

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	authorizationapi "k8s.io/kubernetes/pkg/apis/authorization"
	authorizationvalidation "k8s.io/kubernetes/pkg/apis/authorization/validation"
	authorizationutil "k8s.io/kubernetes/pkg/registry/authorization/util"
)

// REST 实现 LocalSubjectAccessReview 的无状态 REST 接口：接收授权请求并返回实时计算的授权结果。
type REST struct {
	authorizer authorizer.UnconditionalAuthorizer
	scheme     *runtime.Scheme
}

// NewREST(authorizer authorizer.UnconditionalAuthorizer, scheme *runtime.Scheme) 创建 LocalSubjectAccessReview REST 处理器，并注入授权器和内部 API Scheme。
func NewREST(authorizer authorizer.UnconditionalAuthorizer, scheme *runtime.Scheme) *REST {
	return &REST{authorizer, scheme}
}

// NamespaceScoped 返回 true，表示 LocalSubjectAccessReview 属于命名空间。
func (r *REST) NamespaceScoped() bool {
	return true
}

// New 返回用于解码请求的空 LocalSubjectAccessReview 对象。
func (r *REST) New() runtime.Object {
	return &authorizationapi.LocalSubjectAccessReview{}
}

// Destroy 实现 REST 销毁接口；该处理器不持有底层 Store 或其他需显式释放的资源。
func (r *REST) Destroy() {
}

var _ rest.SingularNameProvider = &REST{}

// GetSingularName 返回该 REST 资源的单数名称 localsubjectaccessreview。
func (r *REST) GetSingularName() string {
	return "localsubjectaccessreview"
}

// Create 校验 LocalSubjectAccessReview 请求，构造授权属性并调用 Authorizer；结果写入 Status 后直接返回，不持久化对象。
func (r *REST) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	localSubjectAccessReview, ok := obj.(*authorizationapi.LocalSubjectAccessReview)
	if !ok {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("not a LocaLocalSubjectAccessReview: %#v", obj))
	}

	if errs := authorizationvalidation.ValidateLocalSubjectAccessReviewCreate(ctx, r.scheme, localSubjectAccessReview); len(errs) > 0 {
		return nil, apierrors.NewInvalid(authorizationapi.Kind(localSubjectAccessReview.Kind), "", errs)
	}
	namespace := genericapirequest.NamespaceValue(ctx)
	if len(namespace) == 0 {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("namespace is required on this type: %v", namespace))
	}
	if namespace != localSubjectAccessReview.Namespace {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("spec.resourceAttributes.namespace must match namespace: %v", namespace))
	}

	if createValidation != nil {
		if err := createValidation(ctx, obj.DeepCopyObject()); err != nil {
			return nil, err
		}
	}

	authorizationAttributes := authorizationutil.AuthorizationAttributesFrom(localSubjectAccessReview.Spec)
	decision, reason, evaluationErr := r.authorizer.Authorize(ctx, authorizationAttributes)

	localSubjectAccessReview.Status = authorizationapi.SubjectAccessReviewStatus{
		Allowed: (decision == authorizer.DecisionAllow),
		Denied:  (decision == authorizer.DecisionDeny),
		Reason:  reason,
	}
	localSubjectAccessReview.Status.EvaluationError = authorizationutil.BuildEvaluationError(evaluationErr, authorizationAttributes)

	return localSubjectAccessReview, nil
}
