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

package setdefault

import (
	"context"
	"fmt"
	"io"

	"k8s.io/kubernetes/pkg/volume/util"

	"k8s.io/klog/v2"

	"k8s.io/apiserver/pkg/admission"
	genericadmissioninitializer "k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/client-go/informers"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/helper"
)

const (
	// PluginName 是本准入控制器插件的名称。
	PluginName = "DefaultStorageClass"
)

// Register 注册插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		plugin := newPlugin()
		return plugin, nil
	})
}

// claimDefaulterPlugin 保存状态并实现该准入插件。
type claimDefaulterPlugin struct {
	*admission.Handler

	lister storagev1listers.StorageClassLister
}

var _ admission.Interface = &claimDefaulterPlugin{}
var _ admission.MutationInterface = &claimDefaulterPlugin{}
var _ = genericadmissioninitializer.WantsExternalKubeInformerFactory(&claimDefaulterPlugin{})

// newPlugin 创建一个新的准入插件。
func newPlugin() *claimDefaulterPlugin {
	return &claimDefaulterPlugin{
		Handler: admission.NewHandler(admission.Create),
	}
}

// SetExternalKubeInformerFactory 注入 StorageClass informer，并等待其缓存同步完成。
func (a *claimDefaulterPlugin) SetExternalKubeInformerFactory(f informers.SharedInformerFactory) {
	informer := f.Storage().V1().StorageClasses()
	a.lister = informer.Lister()
	a.SetReadyFunc(informer.Informer().HasSynced)
}

// ValidateInitialization 确保 lister 已注入。
func (a *claimDefaulterPlugin) ValidateInitialization() error {
	if a.lister == nil {
		return fmt.Errorf("missing lister")
	}
	return nil
}

// Admit 在用户未指定 StorageClass 时为 PersistentVolumeClaim 设置默认值。
//
// 1. 查找可用的 StorageClass。
// 2. 找出默认的那一个。
// 3. 写入 PVC。
func (a *claimDefaulterPlugin) Admit(ctx context.Context, attr admission.Attributes, o admission.ObjectInterfaces) error {
	if attr.GetResource().GroupResource() != api.Resource("persistentvolumeclaims") {
		return nil
	}

	if len(attr.GetSubresource()) != 0 {
		return nil
	}

	pvc, ok := attr.GetObject().(*api.PersistentVolumeClaim)
	// 无法转换说明不是本插件处理的对象，直接放行。
	if !ok {
		return nil
	}

	if helper.PersistentVolumeClaimHasClass(pvc) {
		// 用户已显式指定 StorageClass，尊重用户选择，不覆盖。
		return nil
	}

	klog.V(4).Infof("no storage class for claim %s (generate: %s)", pvc.Name, pvc.GenerateName)

	def, err := util.GetDefaultClass(a.lister)
	if err != nil {
		return admission.NewForbidden(attr, err)
	}
	if def == nil {
		// 没有默认 StorageClass，保持 PVC 原样。
		return nil
	}

	klog.V(4).Infof("defaulting storage class for claim %s (generate: %s) to %s", pvc.Name, pvc.GenerateName, def.Name)
	pvc.Spec.StorageClassName = &def.Name
	return nil
}
