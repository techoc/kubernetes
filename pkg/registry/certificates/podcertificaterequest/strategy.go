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

// Package podcertificaterequest 提供 PodCertificateRequest 的 Registry 接口及 RESTStorage 实现。
package podcertificaterequest

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/certificates"
	certvalidation "k8s.io/kubernetes/pkg/apis/certificates/validation"
	"k8s.io/kubernetes/pkg/certauthorization"
	"k8s.io/utils/clock"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// Strategy 定义 PodCertificateRequest 的 REST 写入策略。
type Strategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

var _ rest.RESTCreateStrategy = (*Strategy)(nil)
var _ rest.RESTUpdateStrategy = (*Strategy)(nil)
var _ rest.RESTDeleteStrategy = (*Strategy)(nil)

func NewStrategy() *Strategy {
	return &Strategy{
		DeclarativeValidation: rest.DeclarativeValidation{Scheme: legacyscheme.Scheme},
		NameGenerator:         names.SimpleNameGenerator,
	}
}

func (s *Strategy) NamespaceScoped() bool {
	return true
}

func (s *Strategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	req := obj.(*certificates.PodCertificateRequest)
	req.Status = certificates.PodCertificateRequestStatus{}
}

func (s *Strategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	req := obj.(*certificates.PodCertificateRequest)
	return certvalidation.ValidatePodCertificateRequestCreate(req)
}

func (s *Strategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nil
}

func (s *Strategy) Canonicalize(obj runtime.Object) {}

func (s *Strategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

func (s *Strategy) PrepareForUpdate(ctx context.Context, new, old runtime.Object) {
	newReq := new.(*certificates.PodCertificateRequest)
	oldReq := old.(*certificates.PodCertificateRequest)
	newReq.Status = oldReq.Status
}

func (s *Strategy) ValidateUpdate(ctx context.Context, new, old runtime.Object) field.ErrorList {
	newReq := new.(*certificates.PodCertificateRequest)
	oldReq := old.(*certificates.PodCertificateRequest)
	return certvalidation.ValidatePodCertificateRequestUpdate(newReq, oldReq)
}

func (s *Strategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

func (s *Strategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return false
}

// GetResetFields 返回主资源写入时由服务端重置、不能由用户直接修改的 Status 字段集合。
func (s *Strategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"certificates.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
		"certificates.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}
	return fields
}

// StatusStrategy 定义 PodCertificateRequest Status 子资源的专用写入策略。
type StatusStrategy struct {
	*Strategy
	authorizer authorizer.UnconditionalAuthorizer
	clock      clock.PassiveClock
}

func NewStatusStrategy(strategy *Strategy, authorizer authorizer.UnconditionalAuthorizer, clock clock.PassiveClock) *StatusStrategy {
	return &StatusStrategy{
		Strategy:   strategy,
		authorizer: authorizer,
		clock:      clock,
	}
}

// GetResetFields 返回 Status 子资源更新时由服务端重置、不能由请求修改的 Metadata 和 Spec 字段。
func (s *StatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"certificates.k8s.io/v1beta1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
		"certificates.k8s.io/v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("metadata"),
			fieldpath.MakePathOrDie("spec"),
		),
	}
	return fields
}

func (s *StatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newReq := obj.(*certificates.PodCertificateRequest)
	oldReq := old.(*certificates.PodCertificateRequest)

	// Status 子资源请求只能修改 Status，不能借此更新 Spec。
	newReq.Spec = oldReq.Spec

	metav1.ResetObjectMetaForStatus(&newReq.ObjectMeta, &oldReq.ObjectMeta)
}

func (s *StatusStrategy) ValidateUpdate(ctx context.Context, new, old runtime.Object) field.ErrorList {
	oldPCR := old.(*certificates.PodCertificateRequest)
	newPCR := new.(*certificates.PodCertificateRequest)

	errs := certvalidation.ValidatePodCertificateRequestStatusUpdate(newPCR, oldPCR, s.clock)
	if len(errs) != 0 {
		return errs
	}

	// 调用方若要修改 Status，必须对该请求指定的 signerName 具有相应的 sign 授权。
	if !equality.Semantic.DeepEqual(oldPCR.Status, newPCR.Status) {
		user, ok := genericapirequest.UserFrom(ctx)
		if !ok {
			return field.ErrorList{
				field.InternalError(field.NewPath("spec", "signerName"), fmt.Errorf("cannot determine calling user to perform \"sign\" check")),
			}
		}

		if !certauthorization.IsAuthorizedForSignerName(ctx, s.authorizer, user, "sign", oldPCR.Spec.SignerName) {
			klog.V(4).Infof("user not permitted to sign PodCertificateRequest %q with signerName %q", oldPCR.Name, oldPCR.Spec.SignerName)
			return field.ErrorList{
				field.Forbidden(field.NewPath("spec", "signerName"), fmt.Sprintf("User %q is not permitted to \"sign\" for signer %q", user.GetName(), oldPCR.Spec.SignerName)),
			}
		}
	}

	return nil
}

// WarningsOnUpdate 返回 Status 子资源更新时的非阻断性警告；当前不产生额外警告。
func (s *StatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// Canonicalize 在校验后规范化对象；当前无需额外处理。
func (s *StatusStrategy) Canonicalize(obj runtime.Object) {}
