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

package selfsubjectreview

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	authenticationapi "k8s.io/kubernetes/pkg/apis/authentication"
)

var _ interface {
	rest.Creater
	rest.NamespaceScopedStrategy
	rest.Scoper
	rest.Storage
} = &REST{}

// REST 实现 SelfSubjectReview 的无状态 REST 接口，用于返回当前请求主体的身份信息。
type REST struct {
}

// NewREST 创建 SelfSubjectReview REST 处理器；该接口不需要外部认证器或持久化 Store。
func NewREST() *REST {
	return &REST{}
}

// NamespaceScoped 实现 rest.Scoper；SelfSubjectReview 是集群级资源。
func (r *REST) NamespaceScoped() bool {
	return false
}

// New 返回用于解码请求的空 SelfSubjectReview 对象。
func (r *REST) New() runtime.Object {
	return &authenticationapi.SelfSubjectReview{}
}

// Destroy 实现 REST 销毁接口；处理器不持有底层 Store，无需显式释放资源。
func (r *REST) Destroy() {
}

// Create 从请求上下文读取当前用户身份，并将用户名、UID、组和 Extra 信息写入 SelfSubjectReview.Status。
func (r *REST) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	if createValidation != nil {
		if err := createValidation(ctx, obj.DeepCopyObject()); err != nil {
			return nil, err
		}
	}

	_, ok := obj.(*authenticationapi.SelfSubjectReview)
	if !ok {
		return nil, apierrors.NewBadRequest(fmt.Sprintf("not a SelfSubjectReview: %#v", obj))
	}

	user, ok := genericapirequest.UserFrom(ctx)
	if !ok {
		return nil, apierrors.NewBadRequest("no user present on request")
	}

	extra := user.GetExtra()

	selfSR := &authenticationapi.SelfSubjectReview{
		ObjectMeta: metav1.ObjectMeta{
			CreationTimestamp: metav1.NewTime(time.Now()),
		},
		Status: authenticationapi.SelfSubjectReviewStatus{
			UserInfo: authenticationapi.UserInfo{
				Username: user.GetName(),
				UID:      user.GetUID(),
				Groups:   user.GetGroups(),
				Extra:    make(map[string]authenticationapi.ExtraValue, len(extra)),
			},
		},
	}
	for key, attr := range extra {
		selfSR.Status.UserInfo.Extra[key] = attr
	}

	return selfSR, nil
}

var _ rest.SingularNameProvider = &REST{}

// GetSingularName 返回 SelfSubjectReview 的单数资源名称。
func (r *REST) GetSingularName() string {
	return "selfsubjectreview"
}
