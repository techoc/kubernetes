/*
Copyright 2017 The Kubernetes Authors.

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

package resize

import (
	"context"
	"fmt"
	"io"

	"k8s.io/apiserver/pkg/admission"
	genericadmissioninitializer "k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/client-go/informers"
	storagev1listers "k8s.io/client-go/listers/storage/v1"
	api "k8s.io/kubernetes/pkg/apis/core"
	apihelper "k8s.io/kubernetes/pkg/apis/core/helper"
)

const (
	// PluginName 是 PVC 扩容准入插件的名称。
	PluginName = "PersistentVolumeClaimResize"
)

// Register 注册该插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		plugin := newPlugin()
		return plugin, nil
	})
}

var _ admission.Interface = &persistentVolumeClaimResize{}
var _ admission.ValidationInterface = &persistentVolumeClaimResize{}
var _ = genericadmissioninitializer.WantsExternalKubeInformerFactory(&persistentVolumeClaimResize{})

// persistentVolumeClaimResize 校验 PVC 扩容请求是否合法。
type persistentVolumeClaimResize struct {
	*admission.Handler

	scLister storagev1listers.StorageClassLister
}

// newPlugin 创建 PVC 扩容准入插件实例，仅处理 Update 操作。
func newPlugin() *persistentVolumeClaimResize {
	return &persistentVolumeClaimResize{
		Handler: admission.NewHandler(admission.Update),
	}
}

// SetExternalKubeInformerFactory 获取 StorageClass lister，并在其同步完成后才标记插件就绪。
func (pvcr *persistentVolumeClaimResize) SetExternalKubeInformerFactory(f informers.SharedInformerFactory) {
	scInformer := f.Storage().V1().StorageClasses()
	pvcr.scLister = scInformer.Lister()
	pvcr.SetReadyFunc(scInformer.Informer().HasSynced)
}

// ValidateInitialization 确保 StorageClass lister 已设置。
func (pvcr *persistentVolumeClaimResize) ValidateInitialization() error {
	if pvcr.scLister == nil {
		return fmt.Errorf("missing storageclass lister")
	}
	return nil
}

// Validate 拒绝未绑定 PVC 的扩容，以及存储类未显式允许扩容的 PVC 扩容请求。
func (pvcr *persistentVolumeClaimResize) Validate(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	if a.GetResource().GroupResource() != api.Resource("persistentvolumeclaims") {
		return nil
	}

	if len(a.GetSubresource()) != 0 {
		return nil
	}

	pvc, ok := a.GetObject().(*api.PersistentVolumeClaim)
	// 类型转换失败意味着不是本插件关心的对象，直接放行。
	if !ok {
		return nil
	}
	oldPvc, ok := a.GetOldObject().(*api.PersistentVolumeClaim)
	if !ok {
		return nil
	}

	oldSize := oldPvc.Spec.Resources.Requests[api.ResourceStorage]
	newSize := pvc.Spec.Resources.Requests[api.ResourceStorage]

	if newSize.Cmp(oldSize) <= 0 {
		return nil
	}

	if oldPvc.Status.Phase != api.ClaimBound {
		return admission.NewForbidden(a, fmt.Errorf("Only bound persistent volume claims can be expanded"))
	}

	// 只有存储类显式允许扩容的 PVC 才能扩容。
	if !pvcr.allowResize(pvc, oldPvc) {
		return admission.NewForbidden(a, fmt.Errorf("only dynamically provisioned pvc can be resized and "+
			"the storageclass that provisions the pvc must support resize"))
	}

	return nil
}

// allowResize 判断 PVC 的存储类在扩容前后未被修改，且该存储类显式允许扩容。
// 存储类为空或查询失败都视为不允许，避免误放行动态制备之外的 PVC。
func (pvcr *persistentVolumeClaimResize) allowResize(pvc, oldPvc *api.PersistentVolumeClaim) bool {
	pvcStorageClass := apihelper.GetPersistentVolumeClaimClass(pvc)
	oldPvcStorageClass := apihelper.GetPersistentVolumeClaimClass(oldPvc)
	if pvcStorageClass == "" || oldPvcStorageClass == "" || pvcStorageClass != oldPvcStorageClass {
		return false
	}
	sc, err := pvcr.scLister.Get(pvcStorageClass)
	if err != nil {
		return false
	}
	if sc.AllowVolumeExpansion != nil {
		return *sc.AllowVolumeExpansion
	}
	return false
}
