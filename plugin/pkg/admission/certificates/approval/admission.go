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

package approval

import (
	"context"
	"fmt"
	"io"

	"k8s.io/apiserver/pkg/admission"
	genericadmissioninit "k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/klog/v2"
	api "k8s.io/kubernetes/pkg/apis/certificates"
	"k8s.io/kubernetes/pkg/certauthorization"
)

// PluginName 是插件名称。
const PluginName = "CertificateApproval"

// Register 注册插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		return NewPlugin(), nil
	})
}

// Plugin 保存运行状态并实现该准入插件。
type Plugin struct {
	*admission.Handler
	authz authorizer.UnconditionalAuthorizer
}

// SetUnconditionalAuthorizer 设置鉴权器。
func (p *Plugin) SetUnconditionalAuthorizer(authz authorizer.UnconditionalAuthorizer) {
	p.authz = authz
}

// ValidateInitialization 确保已注入鉴权器。
func (p *Plugin) ValidateInitialization() error {
	if p.authz == nil {
		return fmt.Errorf("%s requires an authorizer", PluginName)
	}
	return nil
}

var _ admission.ValidationInterface = &Plugin{}
var _ genericadmissioninit.WantsUnconditionalAuthorizer = &Plugin{}

// NewPlugin 创建一个新的 CSR 审批准入插件。
func NewPlugin() *Plugin {
	return &Plugin{
		Handler: admission.NewHandler(admission.Update),
	}
}

var csrGroupResource = api.Resource("certificatesigningrequests")

// Validate 检查请求用户是否有权批准指定 signerName 的 CertificateSigningRequest。
func (p *Plugin) Validate(ctx context.Context, a admission.Attributes, _ admission.ObjectInterfaces) error {
	// 只处理 certificatesigningrequests/approval 的 UPDATE 请求。
	if a.GetSubresource() != "approval" ||
		a.GetResource().GroupResource() != csrGroupResource {
		return nil
	}

	// 针对资源的旧版本鉴权，防止用户借审批请求篡改 SignerName（该操作不被允许）。
	csr, ok := a.GetOldObject().(*api.CertificateSigningRequest)
	if !ok {
		return admission.NewForbidden(a, fmt.Errorf("expected type CertificateSigningRequest, got: %T", a.GetOldObject()))
	}

	if !certauthorization.IsAuthorizedForSignerName(ctx, p.authz, a.GetUserInfo(), "approve", csr.Spec.SignerName) {
		klog.V(4).Infof("user not permitted to approve CertificateSigningRequest %q with signerName %q", csr.Name, csr.Spec.SignerName)
		return admission.NewForbidden(a, fmt.Errorf("user not permitted to approve requests with signerName %q", csr.Spec.SignerName))
	}

	return nil
}
