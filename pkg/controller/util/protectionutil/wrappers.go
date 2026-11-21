/*
Copyright 2024 The Kubernetes Authors.

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

package protectionutil

import (
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// PodWrapper 包装一个 Pod，用于以链式调用的方式构造测试对象。
type PodWrapper struct{ v1.Pod }

// MakePod 创建一个空的 Pod 包装器。
func MakePod() *PodWrapper {
	return &PodWrapper{v1.Pod{}}
}

// Obj 返回被包装的 Pod，便于将构造好的对象传给被测函数。
func (p *PodWrapper) Obj() *v1.Pod {
	return &p.Pod
}

// Name 设置被包装 Pod 的名称。
func (p *PodWrapper) Name(s string) *PodWrapper {
	p.SetName(s)
	return p
}

// UID 设置被包装 Pod 的 UID。
func (p *PodWrapper) UID(s string) *PodWrapper {
	p.SetUID(types.UID(s))
	return p
}

// SchedulerName 设置被包装 Pod 的调度器名称，
// 供按调度器过滤 Pod 的逻辑使用。
func (p *PodWrapper) SchedulerName(s string) *PodWrapper {
	p.Spec.SchedulerName = s
	return p
}

// Namespace 设置被包装 Pod 的命名空间。
func (p *PodWrapper) Namespace(s string) *PodWrapper {
	p.SetNamespace(s)
	return p
}

// Terminating 将删除时间戳设为当前时间，模拟 Pod 正处于删除流程中。
// 保护控制器据此判断是否应允许删除。
func (p *PodWrapper) Terminating() *PodWrapper {
	now := metav1.Now()
	p.DeletionTimestamp = &now
	return p
}

// PVC 增加一个引用指定 PVC 的卷，使 Pod 与该 PVC 产生关联。
func (p *PodWrapper) PVC(name string) *PodWrapper {
	p.Spec.Volumes = append(p.Spec.Volumes, v1.Volume{
		Name: name,
		VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{ClaimName: name},
		},
	})
	return p
}

// Annotation 为被包装的 Pod 添加一个注解键值项。
// 使用 SetMetaDataAnnotation 可确保注解 map 已初始化。
func (p *PodWrapper) Annotation(key, value string) *PodWrapper {
	metav1.SetMetaDataAnnotation(&p.ObjectMeta, key, value)
	return p
}

// Annotations 批量添加多个注解项。
func (p *PodWrapper) Annotations(annotations map[string]string) *PodWrapper {
	for k, v := range annotations {
		p.Annotation(k, v)
	}
	return p
}

// PersistentVolumeClaimWrapper 包装一个 PersistentVolumeClaim，便于链式构造测试对象。
type PersistentVolumeClaimWrapper struct{ v1.PersistentVolumeClaim }

// MakePersistentVolumeClaim 创建一个空的 PersistentVolumeClaim 包装器。
func MakePersistentVolumeClaim() *PersistentVolumeClaimWrapper {
	return &PersistentVolumeClaimWrapper{}
}

// Obj 返回被包装的 PersistentVolumeClaim。
func (p *PersistentVolumeClaimWrapper) Obj() *v1.PersistentVolumeClaim {
	return &p.PersistentVolumeClaim
}

// Name 设置被包装 PVC 的名称。
func (p *PersistentVolumeClaimWrapper) Name(s string) *PersistentVolumeClaimWrapper {
	p.SetName(s)
	return p
}

// Namespace 设置被包装 PVC 的命名空间。
func (p *PersistentVolumeClaimWrapper) Namespace(s string) *PersistentVolumeClaimWrapper {
	p.SetNamespace(s)
	return p
}

// Annotation 为被包装的 PVC 添加一个注解键值项。
func (p *PersistentVolumeClaimWrapper) Annotation(key, value string) *PersistentVolumeClaimWrapper {
	metav1.SetMetaDataAnnotation(&p.ObjectMeta, key, value)
	return p
}

// VolumeName 设置 PVC 绑定的卷名，用于模拟已绑定状态。
func (p *PersistentVolumeClaimWrapper) VolumeName(name string) *PersistentVolumeClaimWrapper {
	p.PersistentVolumeClaim.Spec.VolumeName = name
	return p
}

// Finalizer 追加一个 finalizer，用于模拟保护控制器阻止 PVC 被删除的场景。
func (p *PersistentVolumeClaimWrapper) Finalizer(s string) *PersistentVolumeClaimWrapper {
	p.Finalizers = append(p.Finalizers, s)
	return p
}

// VolumeAttributesClassName 设置 PVC 期望的 VolumeAttributesClassName。
func (p *PersistentVolumeClaimWrapper) VolumeAttributesClassName(s string) *PersistentVolumeClaimWrapper {
	p.Spec.VolumeAttributesClassName = &s
	return p
}

// CurrentVolumeAttributesClassName 设置 PVC 当前生效的卷属性类名称。
func (p *PersistentVolumeClaimWrapper) CurrentVolumeAttributesClassName(s string) *PersistentVolumeClaimWrapper {
	p.Status.CurrentVolumeAttributesClassName = &s
	return p
}

// TargetVolumeAttributesClassName 设置修改卷属性的目标类名，
// 同时将状态置为 Pending，表示修改请求已受理但尚未完成。
func (p *PersistentVolumeClaimWrapper) TargetVolumeAttributesClassName(s string) *PersistentVolumeClaimWrapper {
	p.Status.ModifyVolumeStatus = &v1.ModifyVolumeStatus{
		TargetVolumeAttributesClassName: s,
		Status:                          v1.PersistentVolumeClaimModifyVolumePending,
	}
	return p
}

// PersistentVolumeWrapper 包装一个 PersistentVolume，便于链式构造测试对象。
type PersistentVolumeWrapper struct{ v1.PersistentVolume }

// MakePersistentVolume 创建一个空的 PersistentVolume 包装器。
func MakePersistentVolume() *PersistentVolumeWrapper {
	return &PersistentVolumeWrapper{}
}

// Obj 返回被包装的 PersistentVolume。
func (p *PersistentVolumeWrapper) Obj() *v1.PersistentVolume {
	return &p.PersistentVolume
}

// Name 设置被包装 PV 的名称。
func (p *PersistentVolumeWrapper) Name(s string) *PersistentVolumeWrapper {
	p.SetName(s)
	return p
}

// VolumeAttributesClassName 设置 PV 的卷属性类名称。
func (p *PersistentVolumeWrapper) VolumeAttributesClassName(s string) *PersistentVolumeWrapper {
	p.Spec.VolumeAttributesClassName = &s
	return p
}

// VolumeAttributesClassWrapper 包装一个 VolumeAttributesClass，便于链式构造测试对象。
type VolumeAttributesClassWrapper struct {
	storagev1.VolumeAttributesClass
}

// MakeVolumeAttributesClass 创建一个空的 VolumeAttributesClass 包装器。
func MakeVolumeAttributesClass() *VolumeAttributesClassWrapper {
	return &VolumeAttributesClassWrapper{}
}

// Obj 返回被包装的 VolumeAttributesClass。
func (v *VolumeAttributesClassWrapper) Obj() *storagev1.VolumeAttributesClass {
	return &v.VolumeAttributesClass
}

// Name 设置被包装卷属性类的名称。
func (v *VolumeAttributesClassWrapper) Name(s string) *VolumeAttributesClassWrapper {
	v.SetName(s)
	return v
}

// Terminating 将删除时间戳设为非零值，模拟卷属性类正处于删除中。
// 保护逻辑据此判断是否可以移除 finalizer。
func (v *VolumeAttributesClassWrapper) Terminating() *VolumeAttributesClassWrapper {
	v.DeletionTimestamp = &metav1.Time{}
	return v
}

// Finalizer 追加一个 finalizer，用于模拟保护控制器仍持有该对象的场景。
func (v *VolumeAttributesClassWrapper) Finalizer(s string) *VolumeAttributesClassWrapper {
	v.Finalizers = append(v.Finalizers, s)
	return v
}
