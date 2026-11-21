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

package v1

import (
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/features"
)

// addDefaultingFuncs 把本包的默认值注册函数挂到 scheme 上，供反序列化时自动填充默认值。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_StorageClass 为 StorageClass 未指定的字段填充默认值 Delete 与 Immediate。
func SetDefaults_StorageClass(obj *storagev1.StorageClass) {
	// 未指定回收策略时默认删除 PV，未指定绑定模式时默认立即绑定，均为 API 约定。
	if obj.ReclaimPolicy == nil {
		obj.ReclaimPolicy = new(v1.PersistentVolumeReclaimPolicy)
		*obj.ReclaimPolicy = v1.PersistentVolumeReclaimDelete
	}

	if obj.VolumeBindingMode == nil {
		obj.VolumeBindingMode = new(storagev1.VolumeBindingMode)
		*obj.VolumeBindingMode = storagev1.VolumeBindingImmediate
	}
}

// SetDefaults_CSIDriver 为 CSIDriver 的布尔开关与枚举字段填充默认值。
// 部分字段的默认值只在对应特性门控开启时才写入，避免关闭特性时对象出现受门控影响的字段。
func SetDefaults_CSIDriver(obj *storagev1.CSIDriver) {
	// 默认需要 attach 操作，且不要求挂载时传递 Pod 信息、不启用容量感知调度。
	if obj.Spec.AttachRequired == nil {
		obj.Spec.AttachRequired = new(bool)
		*(obj.Spec.AttachRequired) = true
	}
	if obj.Spec.PodInfoOnMount == nil {
		obj.Spec.PodInfoOnMount = new(bool)
		*(obj.Spec.PodInfoOnMount) = false
	}
	if obj.Spec.StorageCapacity == nil {
		obj.Spec.StorageCapacity = new(bool)
		*(obj.Spec.StorageCapacity) = false
	}
	if obj.Spec.FSGroupPolicy == nil {
		obj.Spec.FSGroupPolicy = new(storagev1.FSGroupPolicy)
		*obj.Spec.FSGroupPolicy = storagev1.ReadWriteOnceWithFSTypeFSGroupPolicy
	}
	// 生命周期模式列表为空时默认按 CSI 规范支持持久卷。
	if len(obj.Spec.VolumeLifecycleModes) == 0 {
		obj.Spec.VolumeLifecycleModes = append(obj.Spec.VolumeLifecycleModes, storagev1.VolumeLifecyclePersistent)
	}
	if obj.Spec.RequiresRepublish == nil {
		obj.Spec.RequiresRepublish = new(bool)
		*(obj.Spec.RequiresRepublish) = false
	}
	// 特性门控开启时才填充 SELinuxMount 默认值，避免门控关闭时注入仅在该特性下有意义的配置。
	if obj.Spec.SELinuxMount == nil && utilfeature.DefaultFeatureGate.Enabled(features.SELinuxMountReadWriteOncePod) {
		obj.Spec.SELinuxMount = new(bool)
		*(obj.Spec.SELinuxMount) = false
	}

	if obj.Spec.PreventPodSchedulingIfMissing == nil && utilfeature.DefaultFeatureGate.Enabled(features.VolumeLimitScaling) {
		obj.Spec.PreventPodSchedulingIfMissing = new(bool)
		*(obj.Spec.PreventPodSchedulingIfMissing) = false
	}
}
