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

package storage

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/kubernetes/pkg/apis/certificates"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	csrregistry "k8s.io/kubernetes/pkg/registry/certificates/certificates"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// REST 为 CertificateSigningRequest（CSR）提供基于通用 Store 的 REST 存储实现。
type REST struct {
	*genericregistry.Store
}

// NewREST 创建 CSR 主资源存储，并派生 Status 与 Approval 两个子资源存储。
func NewREST(optsGetter generic.RESTOptionsGetter) (*REST, *StatusREST, *ApprovalREST, error) {
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &certificates.CertificateSigningRequest{} },
		NewListFunc:               func() runtime.Object { return &certificates.CertificateSigningRequestList{} },
		DefaultQualifiedResource:  certificates.Resource("certificatesigningrequests"),
		SingularQualifiedResource: certificates.Resource("certificatesigningrequest"),

		CreateStrategy:      csrregistry.Strategy,
		UpdateStrategy:      csrregistry.Strategy,
		DeleteStrategy:      csrregistry.Strategy,
		ResetFieldsStrategy: csrregistry.Strategy,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	options := &generic.StoreOptions{RESTOptions: optsGetter, AttrFunc: csrregistry.GetAttrs}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, nil, nil, err
	}

	// 子资源与主资源共享底层 Store 和创建策略；该策略只允许创建空子资源对象。
	// 对已有对象的子资源更新则分别交由专用策略处理。
	statusStore := *store
	statusStore.UpdateStrategy = csrregistry.StatusStrategy
	statusStore.ResetFieldsStrategy = csrregistry.StatusStrategy
	statusStore.BeginUpdate = countCSRDurationMetric(csrDurationRequested, csrDurationHonored)

	approvalStore := *store
	approvalStore.UpdateStrategy = csrregistry.ApprovalStrategy
	approvalStore.ResetFieldsStrategy = csrregistry.ApprovalStrategy

	return &REST{store}, &StatusREST{store: &statusStore}, &ApprovalREST{store: &approvalStore}, nil
}

// 声明 REST 实现 ShortNamesProvider 接口。
var _ rest.ShortNamesProvider = &REST{}

// ShortNames 实现 ShortNamesProvider，返回 CSR 资源支持的简称。
func (r *REST) ShortNames() []string {
	return []string{"csr"}
}

// StatusREST 实现用于更新 CSR Status 的 REST 子资源端点。
type StatusREST struct {
	store *genericregistry.Store
}

// New 返回一个空的 CertificateSigningRequest 对象。
func (r *StatusREST) New() runtime.Object {
	return &certificates.CertificateSigningRequest{}
}

// Destroy 在 API Server 关闭时清理该 REST 子资源所拥有的资源。
func (r *StatusREST) Destroy() {
	// 底层 Store 与主资源 REST 共享，因此这里不能重复销毁。
}

// Get 从底层存储读取 CSR；REST 子资源需要实现该方法以支持 Patch。
func (r *StatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// Update 只更新 CSR 的 Status 子集，不允许通过该入口修改 Spec。
func (r *StatusREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	// 调用底层 Store 时显式将 forceAllowCreate 设为 false，子资源绝不能通过更新请求创建对象。
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

// GetResetFields 实现 rest.ResetFieldsStrategy，返回 Status 子资源需要重置的字段集合。
func (r *StatusREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.store.GetResetFields()
}

func (r *StatusREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.store.ConvertToTable(ctx, object, tableOptions)
}

var _ = rest.Patcher(&StatusREST{})

// ApprovalREST 实现用于更新 CSR 批准状态的 REST 子资源端点。
type ApprovalREST struct {
	store *genericregistry.Store
}

// New 返回一个空的 CertificateSigningRequest 对象。
func (r *ApprovalREST) New() runtime.Object {
	return &certificates.CertificateSigningRequest{}
}

// Destroy 在 API Server 关闭时清理 Approval 子资源拥有的资源。
func (r *ApprovalREST) Destroy() {
	// 底层 Store 与主资源 REST 共享，因此这里不能重复销毁。
}

// Get 从底层存储读取 CSR；REST 子资源需要实现该方法以支持 Patch。
func (r *ApprovalREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// Update 只更新 CSR 的批准条件，不允许通过该入口修改 Spec 或证书内容。
func (r *ApprovalREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	// 调用底层 Store 时显式将 forceAllowCreate 设为 false，子资源绝不能通过更新请求创建对象。
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

// GetResetFields 实现 rest.ResetFieldsStrategy，返回 Approval 子资源需要重置的字段集合。
func (r *ApprovalREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.store.GetResetFields()
}

var _ = rest.Patcher(&ApprovalREST{})
