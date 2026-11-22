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

package storageobjectinuseprotection

import (
	"context"
	"io"

	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	api "k8s.io/kubernetes/pkg/apis/core"
	storageapi "k8s.io/kubernetes/pkg/apis/storage"
	"k8s.io/kubernetes/pkg/features"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
)

const (
	// PluginName 是本准入控制器插件的名称。
	PluginName = "StorageObjectInUseProtection"
)

// Register 注册插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		plugin := newPlugin()
		return plugin, nil
	})
}

// storageProtectionPlugin 保存状态并实现该准入插件。
type storageProtectionPlugin struct {
	*admission.Handler
}

var _ admission.Interface = &storageProtectionPlugin{}

// newPlugin 创建一个新的准入插件。
func newPlugin() *storageProtectionPlugin {
	return &storageProtectionPlugin{
		Handler: admission.NewHandler(admission.Create),
	}
}

var (
	pvResource  = api.Resource("persistentvolumes")
	pvcResource = api.Resource("persistentvolumeclaims")
	vacResource = storageapi.Resource("volumeattributesclasses")
)

// Admit 给所有 PVC（PV）加上 finalizer；当对象不再被引用时，
// 由 PVCProtectionController（PVProtectionController）移除该 finalizer。
//
// 这样可防止用户删除仍被运行中 Pod 使用的 PVC，也防止管理员删除仍被 PVC 绑定的 PV。
func (c *storageProtectionPlugin) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	switch a.GetResource().GroupResource() {
	case pvResource:
		return c.admitPV(a)
	case pvcResource:
		return c.admitPVC(a)
	case vacResource:
		if feature.DefaultFeatureGate.Enabled(features.VolumeAttributesClass) {
			return c.admitVAC(a)
		}
		return nil

	default:
		return nil
	}
}

// admitPV 为 PV 添加保护 finalizer，非 PV 或已有 finalizer 时跳过。
func (c *storageProtectionPlugin) admitPV(a admission.Attributes) error {
	// 子资源操作不会创建新的 PV，无需处理。
	if len(a.GetSubresource()) != 0 {
		return nil
	}

	pv, ok := a.GetObject().(*api.PersistentVolume)
	// 无法转换为 PV 说明不是本插件关心的对象，直接放行。
	if !ok {
		return nil
	}
	for _, f := range pv.Finalizers {
		if f == volumeutil.PVProtectionFinalizer {
			// finalizer 已存在，无需重复添加。
			return nil
		}
	}
	klog.V(4).Infof("adding PV protection finalizer to %s", pv.Name)
	pv.Finalizers = append(pv.Finalizers, volumeutil.PVProtectionFinalizer)

	return nil
}

// admitPVC 为 PVC 添加保护 finalizer，非 PVC 或已有 finalizer 时跳过。
func (c *storageProtectionPlugin) admitPVC(a admission.Attributes) error {
	// 子资源操作不会创建新的 PVC，无需处理。
	if len(a.GetSubresource()) != 0 {
		return nil
	}

	pvc, ok := a.GetObject().(*api.PersistentVolumeClaim)
	// 无法转换为 PVC 说明不是本插件关心的对象，直接放行。
	if !ok {
		return nil
	}

	for _, f := range pvc.Finalizers {
		if f == volumeutil.PVCProtectionFinalizer {
			// finalizer 已存在，无需重复添加。
			return nil
		}
	}

	klog.V(4).Infof("adding PVC protection finalizer to %s/%s", pvc.Namespace, pvc.Name)
	pvc.Finalizers = append(pvc.Finalizers, volumeutil.PVCProtectionFinalizer)
	return nil
}

// admitVAC 为 VAC 添加保护 finalizer，非 VAC 或已有 finalizer 时跳过。
func (c *storageProtectionPlugin) admitVAC(a admission.Attributes) error {
	// 子资源操作不会创建新的 VAC，无需处理。
	if len(a.GetSubresource()) != 0 {
		return nil
	}

	vac, ok := a.GetObject().(*storageapi.VolumeAttributesClass)
	// 无法转换为 VAC 说明不是本插件关心的对象，直接放行。
	if !ok {
		klog.V(2).Infof("can't convert the obj to VAC to %s", vac.Name)
		return nil
	}
	for _, f := range vac.Finalizers {
		if f == volumeutil.VACProtectionFinalizer {
			// finalizer 已存在，无需重复添加。
			return nil
		}
	}
	klog.V(4).Infof("adding VAC protection finalizer to %s", vac.Name)
	vac.Finalizers = append(vac.Finalizers, volumeutil.VACProtectionFinalizer)

	return nil
}
