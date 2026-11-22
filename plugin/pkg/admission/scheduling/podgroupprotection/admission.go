/*
Copyright The Kubernetes Authors.

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

package podgroupprotection

import (
	"context"
	"fmt"
	"io"
	"slices"

	"k8s.io/apiserver/pkg/admission"
	apiserveradmission "k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/component-base/featuregate"
	"k8s.io/klog/v2"
	schedulingapi "k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/features"
)

const (
	// PluginName 是 PodGroup 保护准入插件的名称。
	PluginName = "PodGroupProtection"
)

// Register 注册该插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		return newPlugin(), nil
	})
}

// podGroupProtectionPlugin 为 PodGroup 添加保护性 finalizer，防止其被提前删除。
type podGroupProtectionPlugin struct {
	*admission.Handler
	enabled               bool
	inspectedFeatureGates bool
}

var _ admission.MutationInterface = &podGroupProtectionPlugin{}
var _ apiserveradmission.WantsFeatures = &podGroupProtectionPlugin{}

// newPlugin 创建 PodGroup 保护准入插件实例，仅处理 Create 操作。
func newPlugin() *podGroupProtectionPlugin {
	return &podGroupProtectionPlugin{
		Handler: admission.NewHandler(admission.Create),
	}
}

// InspectFeatureGates 根据 GenericWorkload 特性门控决定是否启用该插件。
func (p *podGroupProtectionPlugin) InspectFeatureGates(featureGates featuregate.FeatureGate) {
	p.enabled = featureGates.Enabled(features.GenericWorkload)
	p.inspectedFeatureGates = true
}

// ValidateInitialization 确保特性门控已被检查，否则拒绝以不确定状态运行。
func (p *podGroupProtectionPlugin) ValidateInitialization() error {
	if !p.inspectedFeatureGates {
		return fmt.Errorf("feature gates not inspected")
	}
	return nil
}

// podGroupResource 是本插件关心的 PodGroup 资源标识。
var podGroupResource = schedulingapi.Resource("podgroups")

// Admit 为每个新建的 PodGroup 打上 PodGroupProtectionFinalizer，
// 防止仍有 Pod 引用它时被删除。当 PodGroup 不再被使用时，
// 该 finalizer 由 PodGroupProtection 控制器移除。
func (p *podGroupProtectionPlugin) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	if !p.enabled {
		return nil
	}

	if a.GetResource().GroupResource() != podGroupResource {
		return nil
	}
	if len(a.GetSubresource()) != 0 {
		return nil
	}

	pg, ok := a.GetObject().(*schedulingapi.PodGroup)
	if !ok {
		return nil
	}

	if slices.Contains(pg.Finalizers, schedulingapi.PodGroupProtectionFinalizer) {
		return nil
	}

	klog.V(4).InfoS("Adding protection finalizer to PodGroup", "podGroup", klog.KObj(pg))
	pg.Finalizers = append(pg.Finalizers, schedulingapi.PodGroupProtectionFinalizer)
	return nil
}
