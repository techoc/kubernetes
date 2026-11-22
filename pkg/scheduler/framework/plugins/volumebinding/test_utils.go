/*
Copyright 2021 The Kubernetes Authors.

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

package volumebinding

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/component-helpers/storage/volume"
	"k8s.io/utils/ptr"
)

// nodeBuilder 用链式调用构造测试用的 Node 对象。
// 内嵌 *v1.Node，便于直接访问或修改节点字段。
type nodeBuilder struct {
	*v1.Node
}

// makeNode 创建一个带指定名称的 nodeBuilder。
// 默认设置 hostname 标签与节点名一致，模拟真实节点的标签配置。
func makeNode(name string) nodeBuilder {
	return nodeBuilder{Node: &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Labels: map[string]string{
				v1.LabelHostname: name,
			},
		},
	}}
}

// withLabel 为节点添加标签，返回 builder 以便继续链式调用。
func (nb nodeBuilder) withLabel(key, value string) nodeBuilder {
	if nb.Node.ObjectMeta.Labels == nil {
		nb.Node.ObjectMeta.Labels = map[string]string{}
	}
	nb.Node.ObjectMeta.Labels[key] = value
	return nb
}

// pvBuilder 用链式调用构造测试用的 PersistentVolume 对象。
// 内嵌 *v1.PersistentVolume，便于直接访问或修改 PV 字段。
type pvBuilder struct {
	*v1.PersistentVolume
}

// makePV 创建一个指定名称和 StorageClass 的 pvBuilder。
func makePV(name, className string) pvBuilder {
	return pvBuilder{PersistentVolume: &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: v1.PersistentVolumeSpec{
			StorageClassName: className,
		},
	}}
}

// withNodeAffinity 为 PV 设置节点亲和性（Required，Operator 为 In）。
// keyValues 中每个键值对生成一个节点选择器要求，用于测试 PV 与节点的匹配逻辑。
func (pvb pvBuilder) withNodeAffinity(keyValues map[string][]string) pvBuilder {
	matchExpressions := make([]v1.NodeSelectorRequirement, 0)
	for key, values := range keyValues {
		matchExpressions = append(matchExpressions, v1.NodeSelectorRequirement{
			Key:      key,
			Operator: v1.NodeSelectorOpIn,
			Values:   values,
		})
	}
	pvb.PersistentVolume.Spec.NodeAffinity = &v1.VolumeNodeAffinity{
		Required: &v1.NodeSelector{
			NodeSelectorTerms: []v1.NodeSelectorTerm{
				{
					MatchExpressions: matchExpressions,
				},
			},
		},
	}
	return pvb
}

// withVersion 设置 PV 的 ResourceVersion，用于测试中模拟对象的版本变化。
func (pvb pvBuilder) withVersion(version string) pvBuilder {
	pvb.PersistentVolume.ObjectMeta.ResourceVersion = version
	return pvb
}

// withCapacity 设置 PV 的存储容量。
func (pvb pvBuilder) withCapacity(capacity resource.Quantity) pvBuilder {
	pvb.PersistentVolume.Spec.Capacity = v1.ResourceList{
		v1.ResourceName(v1.ResourceStorage): capacity,
	}
	return pvb
}

// withPhase 设置 PV 的相位（如 Available、Bound、Released）。
func (pvb pvBuilder) withPhase(phase v1.PersistentVolumePhase) pvBuilder {
	pvb.PersistentVolume.Status = v1.PersistentVolumeStatus{
		Phase: phase,
	}
	return pvb
}

// pvcBuilder 用链式调用构造测试用的 PersistentVolumeClaim 对象。
// 内嵌 *v1.PersistentVolumeClaim，便于直接访问或修改 PVC 字段。
type pvcBuilder struct {
	*v1.PersistentVolumeClaim
}

// makePVC 创建一个指定名称和 StorageClass 的 pvcBuilder。
// 默认命名空间为 default。
func makePVC(name string, storageClassName string) pvcBuilder {
	return pvcBuilder{PersistentVolumeClaim: &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: v1.NamespaceDefault,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			StorageClassName: ptr.To(storageClassName),
		},
	}}
}

// withBoundPV 将 PVC 标记为已绑定到指定 PV。
// 同时设置 VolumeName 和绑定完成注解，因为调度器依赖该注解判断绑定是否真正完成。
func (pvcb pvcBuilder) withBoundPV(pvName string) pvcBuilder {
	pvcb.PersistentVolumeClaim.Spec.VolumeName = pvName
	metav1.SetMetaDataAnnotation(&pvcb.PersistentVolumeClaim.ObjectMeta, volume.AnnBindCompleted, "true")
	return pvcb
}

// withRequestStorage 设置 PVC 请求的存储容量。
func (pvcb pvcBuilder) withRequestStorage(request resource.Quantity) pvcBuilder {
	pvcb.PersistentVolumeClaim.Spec.Resources = v1.VolumeResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceName(v1.ResourceStorage): request,
		},
	}
	return pvcb
}

// withPhase 设置 PVC 的相位（如 Pending、Bound、Lost）。
func (pvcb pvcBuilder) withPhase(phase v1.PersistentVolumeClaimPhase) pvcBuilder {
	pvcb.PersistentVolumeClaim.Status = v1.PersistentVolumeClaimStatus{
		Phase: phase,
	}
	return pvcb
}

// podBuilder 用链式调用构造测试用的 Pod 对象。
// 内嵌 *v1.Pod，便于直接访问或修改 Pod 字段。
type podBuilder struct {
	*v1.Pod
}

// makePod 创建一个指定名称的 podBuilder。
// 默认命名空间为 default，并初始化空的 Volumes 列表以便后续追加卷。
func makePod(name string) podBuilder {
	pb := podBuilder{Pod: &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: v1.NamespaceDefault,
		},
	}}
	pb.Pod.Spec.Volumes = make([]v1.Volume, 0)
	return pb
}

// withNodeName 设置 Pod 已调度到的节点名。
func (pb podBuilder) withNodeName(name string) podBuilder {
	pb.Pod.Spec.NodeName = name
	return pb
}

// withNamespace 设置 Pod 的命名空间。
func (pb podBuilder) withNamespace(name string) podBuilder {
	pb.Pod.ObjectMeta.Namespace = name
	return pb
}

// withPVCVolume 为 Pod 添加一个引用指定 PVC 的卷，卷名为 name。
func (pb podBuilder) withPVCVolume(pvcName, name string) podBuilder {
	pb.Pod.Spec.Volumes = append(pb.Pod.Spec.Volumes, v1.Volume{
		Name: name,
		VolumeSource: v1.VolumeSource{
			PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
				ClaimName: pvcName,
			},
		},
	})
	return pb
}

// withPVCSVolume 批量添加多个 PVC 卷，卷名按 vol0、vol1... 顺序生成。
func (pb podBuilder) withPVCSVolume(pvcs []*v1.PersistentVolumeClaim) podBuilder {
	for i, pvc := range pvcs {
		pb.withPVCVolume(pvc.Name, fmt.Sprintf("vol%v", i))
	}
	return pb
}

// withEmptyDirVolume 为 Pod 添加一个 emptyDir 卷（不使用 PVC）。
func (pb podBuilder) withEmptyDirVolume() podBuilder {
	pb.Pod.Spec.Volumes = append(pb.Pod.Spec.Volumes, v1.Volume{
		VolumeSource: v1.VolumeSource{
			EmptyDir: &v1.EmptyDirVolumeSource{},
		},
	})
	return pb
}

// withGenericEphemeralVolume 为 Pod 添加一个通用临时卷（generic ephemeral volume）。
// 这类卷会在 Pod 创建时由控制器自动创建对应的 PVC，是 podHasPVCs 需要特殊处理的场景。
func (pb podBuilder) withGenericEphemeralVolume(name string) podBuilder {
	pb.Pod.Spec.Volumes = append(pb.Pod.Spec.Volumes, v1.Volume{
		Name: name,
		VolumeSource: v1.VolumeSource{
			Ephemeral: &v1.EphemeralVolumeSource{},
		},
	})
	return pb
}

// withCSI 为 Pod 添加一个使用指定 CSI 驱动的卷，用于测试 CSI 相关逻辑。
func (pb podBuilder) withCSI(driver string) podBuilder {
	pb.Pod.Spec.Volumes = append(pb.Pod.Spec.Volumes, v1.Volume{
		VolumeSource: v1.VolumeSource{
			CSI: &v1.CSIVolumeSource{
				Driver: driver,
			},
		},
	})
	return pb
}
