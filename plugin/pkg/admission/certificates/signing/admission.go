/*
Copyright 2020 The Kubernetes Authors.

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

package signing

import (
	"context"
	"fmt"
	"io"
	"reflect"

	"k8s.io/klog/v2"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apiserver/pkg/admission"
	genericadmissioninit "k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	api "k8s.io/kubernetes/pkg/apis/certificates"
	"k8s.io/kubernetes/pkg/certauthorization"
)

// PluginName 是本准入插件的名称。
const PluginName = "CertificateSigning"

// Register 向插件注册表注册本准入插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		return NewPlugin(), nil
	})
}

// Plugin 持有并实现证书签名准入插件所需的状态。
type Plugin struct {
	*admission.Handler
	authz authorizer.UnconditionalAuthorizer
}

// SetUnconditionalAuthorizer 设置用于签名权限检查的授权器。
func (p *Plugin) SetUnconditionalAuthorizer(authz authorizer.UnconditionalAuthorizer) {
	p.authz = authz
}

// ValidateInitialization 确认授权器已注入。
func (p *Plugin) ValidateInitialization() error {
	if p.authz == nil {
		return fmt.Errorf("%s requires an authorizer", PluginName)
	}
	return nil
}

var _ admission.ValidationInterface = &Plugin{}
var _ genericadmissioninit.WantsUnconditionalAuthorizer = &Plugin{}

// NewPlugin 创建新的 CSR 签名准入插件实例。
func NewPlugin() *Plugin {
	return &Plugin{
		Handler: admission.NewHandler(admission.Update),
	}
}

// csrGroupResource 是插件关心的 CSR 资源标识。
var csrGroupResource = api.Resource("certificatesigningrequests")

// Validate 校验请求用户是否有权为指定 signerName 签名 CertificateSigningRequest。
func (p *Plugin) Validate(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	// 只关心 CSR 的 status 子资源更新，其他请求直接放行。
	if a.GetSubresource() != "status" ||
		a.GetResource().GroupResource() != csrGroupResource {
		return nil
	}

	oldCSR, ok := a.GetOldObject().(*api.CertificateSigningRequest)
	if !ok {
		return admission.NewForbidden(a, fmt.Errorf("expected type CertificateSigningRequest, got: %T", a.GetOldObject()))
	}
	csr, ok := a.GetObject().(*api.CertificateSigningRequest)
	if !ok {
		return admission.NewForbidden(a, fmt.Errorf("expected type CertificateSigningRequest, got: %T", a.GetObject()))
	}

	// 证书和条件都没变时无需鉴权，避免误伤其他 status 字段更新。
	if reflect.DeepEqual(oldCSR.Status.Certificate, csr.Status.Certificate) && apiequality.Semantic.DeepEqual(oldCSR.Status.Conditions, csr.Status.Conditions) {
		return nil
	}

	if !certauthorization.IsAuthorizedForSignerName(ctx, p.authz, a.GetUserInfo(), "sign", oldCSR.Spec.SignerName) {
		klog.V(4).Infof("user not permitted to sign CertificateSigningRequest %q with signerName %q", oldCSR.Name, oldCSR.Spec.SignerName)
		return admission.NewForbidden(a, fmt.Errorf("user not permitted to sign requests with signerName %q", oldCSR.Spec.SignerName))
	}

	return nil
}
