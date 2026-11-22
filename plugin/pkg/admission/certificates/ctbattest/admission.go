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

// Package ctbattest 提供 ClusterTrustBundleAttest 准入控制器：
// 校验用户是否有权为指定 signerName attest ClusterTrustBundle。
package ctbattest

import (
	"context"
	"fmt"
	"io"

	"k8s.io/apiserver/pkg/admission"
	genericadmissioninit "k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/component-base/featuregate"
	"k8s.io/klog/v2"
	api "k8s.io/kubernetes/pkg/apis/certificates"
	kapihelper "k8s.io/kubernetes/pkg/apis/core/helper"
	"k8s.io/kubernetes/pkg/certauthorization"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/registry/rbac"
)

// PluginName 是本准入插件的名称。
const PluginName = "ClusterTrustBundleAttest"

// Register 向 apiserver 注册本插件的工厂函数。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		return NewPlugin(), nil
	})
}

// Plugin 是 ClusterTrustBundle attest 插件。
//
// 创建或更新设置了 signerName 的 ClusterTrustBundle 时，用户必须具备如下权限：
// group=certificates.k8s.io、resource=signers、resourceName=<签名者名称>、verb=attest。
type Plugin struct {
	*admission.Handler
	authz authorizer.UnconditionalAuthorizer

	inspectedFeatureGates bool
	enabled               bool
}

var _ admission.ValidationInterface = &Plugin{}
var _ admission.InitializationValidator = &Plugin{}
var _ genericadmissioninit.WantsUnconditionalAuthorizer = &Plugin{}
var _ genericadmissioninit.WantsFeatures = &Plugin{}

// NewPlugin 创建一个 ClusterTrustBundleAttest 准入插件实例。
func NewPlugin() *Plugin {
	return &Plugin{
		Handler: admission.NewHandler(admission.Create, admission.Update),
	}
}

// SetUnconditionalAuthorizer 注入插件使用的 authorizer。
func (p *Plugin) SetUnconditionalAuthorizer(authz authorizer.UnconditionalAuthorizer) {
	p.authz = authz
}

// InspectFeatureGates 实现 WantsFeatures，读取 ClusterTrustBundle 特性开关。
func (p *Plugin) InspectFeatureGates(featureGates featuregate.FeatureGate) {
	p.enabled = featureGates.Enabled(features.ClusterTrustBundle)
	p.inspectedFeatureGates = true
}

// ValidateInitialization 检查插件已正确注入 authorizer 并读取了特性开关。
func (p *Plugin) ValidateInitialization() error {
	if p.authz == nil {
		return fmt.Errorf("%s requires an authorizer", PluginName)
	}
	if !p.inspectedFeatureGates {
		return fmt.Errorf("%s did not see feature gates", PluginName)
	}
	return nil
}

// clusterTrustBundleGroupResource 是本插件关注的 ClusterTrustBundle 资源标识。
var clusterTrustBundleGroupResource = api.Resource("clustertrustbundles")

// Validate 校验用户是否有权对带 signerName 的 ClusterTrustBundle 执行 attest。
func (p *Plugin) Validate(ctx context.Context, a admission.Attributes, _ admission.ObjectInterfaces) error {
	if !p.enabled {
		return nil
	}
	if a.GetResource().GroupResource() != clusterTrustBundleGroupResource {
		return nil
	}

	newBundle, ok := a.GetObject().(*api.ClusterTrustBundle)
	if !ok {
		return admission.NewForbidden(a, fmt.Errorf("expected type ClusterTrustBundle, got: %T", a.GetOldObject()))
	}

	// 与 CSR 不同，这里可以直接校验新对象：signerName 的修改会被校验环节拒绝。

	// 没有 signerName 就无需做 attest 校验。
	if newBundle.Spec.SignerName == "" {
		return nil
	}

	// 对象语义未变时跳过 attest 校验，避免阻塞存储迁移和 GC 流程。
	if a.GetOperation() == admission.Update && rbac.IsOnlyMutatingGCFields(a.GetObject(), a.GetOldObject(), kapihelper.Semantic) {
		return nil
	}

	if !certauthorization.IsAuthorizedForSignerName(ctx, p.authz, a.GetUserInfo(), "attest", newBundle.Spec.SignerName) {
		klog.V(4).Infof("user not permitted to attest ClusterTrustBundle %q with signerName %q", newBundle.Name, newBundle.Spec.SignerName)
		return admission.NewForbidden(a, fmt.Errorf("user not permitted to attest for signerName %q", newBundle.Spec.SignerName))
	}

	return nil
}
