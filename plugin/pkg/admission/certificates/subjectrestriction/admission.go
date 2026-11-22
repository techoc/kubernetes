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

package subjectrestriction

import (
	"context"
	"fmt"
	"io"

	certificatesv1 "k8s.io/api/certificates/v1"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/klog/v2"
	certificatesapi "k8s.io/kubernetes/pkg/apis/certificates"
)

// PluginName 是该插件的名称。
const PluginName = "CertificateSubjectRestriction"

// Register 注册该插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		return NewPlugin(), nil
	})
}

// Plugin 持有该准入插件的状态并实现其逻辑。
type Plugin struct {
	*admission.Handler
}

// ValidateInitialization 无需检查任何初始化状态，始终返回 nil。
func (p *Plugin) ValidateInitialization() error {
	return nil
}

var _ admission.ValidationInterface = &Plugin{}

// NewPlugin 构造一个新的 CertificateSubjectRestrictions 准入插件实例。
func NewPlugin() *Plugin {
	return &Plugin{
		Handler: admission.NewHandler(admission.Create),
	}
}

var csrGroupResource = certificatesapi.Resource("certificatesigningrequests")

// Validate 确保当 CSR 的 signerName 为 `kubernetes.io/kube-apiserver-client` 时，
// 其 organization（group）属性不被设置为 `system:masters`，
// 防止通过客户端证书提权到高权限组。
func (p *Plugin) Validate(_ context.Context, a admission.Attributes, _ admission.ObjectInterfaces) error {
	if a.GetResource().GroupResource() != csrGroupResource || a.GetSubresource() != "" {
		return nil
	}

	csr, ok := a.GetObject().(*certificatesapi.CertificateSigningRequest)
	if !ok {
		return admission.NewForbidden(a, fmt.Errorf("expected type CertificateSigningRequest, got: %T", a.GetObject()))
	}

	if csr.Spec.SignerName != certificatesv1.KubeAPIServerClientSignerName {
		return nil
	}

	csrParsed, err := certificatesapi.ParseCSR(csr.Spec.Request)
	if err != nil {
		return admission.NewForbidden(a, fmt.Errorf("failed to parse CSR: %v", err))
	}

	for _, group := range csrParsed.Subject.Organization {
		// 仅针对此签名者拒绝 system:masters，其他签名者不受此插件约束。
		if group == "system:masters" {
			klog.V(4).Infof("CSR %s rejected by admission plugin %s for attempting to use signer %s with system:masters group",
				csr.Name, PluginName, certificatesv1.KubeAPIServerClientSignerName)
			return admission.NewForbidden(a, fmt.Errorf("use of %s signer with system:masters group is not allowed",
				certificatesv1.KubeAPIServerClientSignerName))
		}
	}

	return nil
}
