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

package framework

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	appslisters "k8s.io/client-go/listers/apps/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	fwk "k8s.io/kube-scheduler/framework"
)

var _ corelisters.ServiceLister = &ServiceLister{}

// ServiceLister 基于 []*v1.Service 实现 corelisters.ServiceLister，供测试使用。
type ServiceLister []*v1.Service

// Services 返回指定命名空间下 Service 的 namespace lister。
func (f ServiceLister) Services(namespace string) corelisters.ServiceNamespaceLister {
	var services []*v1.Service
	for i := range f {
		if f[i].Namespace == namespace {
			services = append(services, f[i])
		}
	}
	return &serviceNamespaceLister{
		services:  services,
		namespace: namespace,
	}
}

// List 返回全部 Service；fake 实现忽略 selector。
func (f ServiceLister) List(labels.Selector) ([]*v1.Service, error) {
	return f, nil
}

// serviceNamespaceLister 是 Services() 返回的 ServiceNamespaceLister 实现。
type serviceNamespaceLister struct {
	services  []*v1.Service
	namespace string
}

// Get 未实现，测试场景不会调用。
func (f *serviceNamespaceLister) Get(name string) (*v1.Service, error) {
	return nil, fmt.Errorf("not implemented")
}

// List 返回该命名空间下的 Service；返回前已在 Services() 中过滤过。
func (f *serviceNamespaceLister) List(selector labels.Selector) ([]*v1.Service, error) {
	return f.services, nil
}

var _ corelisters.ReplicationControllerLister = &ControllerLister{}

// ControllerLister 基于 []*v1.ReplicationController 实现 ReplicationControllerLister，供测试使用。
type ControllerLister []*v1.ReplicationController

// List 返回全部 ReplicationController；fake 实现忽略 selector。
func (f ControllerLister) List(labels.Selector) ([]*v1.ReplicationController, error) {
	return f, nil
}

// GetPodControllers 返回标签选择器能匹配给定 Pod 的 ReplicationController。
func (f ControllerLister) GetPodControllers(pod *v1.Pod) (controllers []*v1.ReplicationController, err error) {
	var selector labels.Selector

	for i := range f {
		controller := f[i]
		if controller.Namespace != pod.Namespace {
			continue
		}
		selector = labels.Set(controller.Spec.Selector).AsSelectorPreValidated()
		if selector.Matches(labels.Set(pod.Labels)) {
			controllers = append(controllers, controller)
		}
	}
	if len(controllers) == 0 {
		err = fmt.Errorf("could not find Replication Controller for pod %s in namespace %s with labels: %v", pod.Name, pod.Namespace, pod.Labels)
	}

	return
}

// ReplicationControllers 返回 nil，测试场景不会使用按命名空间的查询。
func (f ControllerLister) ReplicationControllers(namespace string) corelisters.ReplicationControllerNamespaceLister {
	return nil
}

var _ appslisters.ReplicaSetLister = &ReplicaSetLister{}

// ReplicaSetLister 基于 []*appsv1.ReplicaSet 实现 ReplicaSetLister，供测试使用。
type ReplicaSetLister []*appsv1.ReplicaSet

// List 返回全部 ReplicaSet；fake 实现忽略 selector。
func (f ReplicaSetLister) List(labels.Selector) ([]*appsv1.ReplicaSet, error) {
	return f, nil
}

// GetPodReplicaSets 返回标签选择器能匹配给定 Pod 的 ReplicaSet。
func (f ReplicaSetLister) GetPodReplicaSets(pod *v1.Pod) (rss []*appsv1.ReplicaSet, err error) {
	var selector labels.Selector

	for _, rs := range f {
		if rs.Namespace != pod.Namespace {
			continue
		}
		selector, err = metav1.LabelSelectorAsSelector(rs.Spec.Selector)
		if err != nil {
			// 选择器非法时视为不匹配，跳过该对象。
			continue
		}

		if selector.Matches(labels.Set(pod.Labels)) {
			rss = append(rss, rs)
		}
	}
	if len(rss) == 0 {
		err = fmt.Errorf("could not find ReplicaSet for pod %s in namespace %s with labels: %v", pod.Name, pod.Namespace, pod.Labels)
	}

	return
}

// ReplicaSets 返回 nil，测试场景不会使用按命名空间的查询。
func (f ReplicaSetLister) ReplicaSets(namespace string) appslisters.ReplicaSetNamespaceLister {
	return nil
}

var _ appslisters.StatefulSetLister = &StatefulSetLister{}

// StatefulSetLister 基于 []*appsv1.StatefulSet 实现 StatefulSetLister，供测试使用。
type StatefulSetLister []*appsv1.StatefulSet

// List 返回全部 StatefulSet；fake 实现忽略 selector。
func (f StatefulSetLister) List(labels.Selector) ([]*appsv1.StatefulSet, error) {
	return f, nil
}

// GetPodStatefulSets 返回标签选择器能匹配给定 Pod 的 StatefulSet。
func (f StatefulSetLister) GetPodStatefulSets(pod *v1.Pod) (sss []*appsv1.StatefulSet, err error) {
	var selector labels.Selector

	for _, ss := range f {
		if ss.Namespace != pod.Namespace {
			continue
		}
		selector, err = metav1.LabelSelectorAsSelector(ss.Spec.Selector)
		if err != nil {
			// 选择器非法时视为不匹配，跳过该对象。
			continue
		}
		if selector.Matches(labels.Set(pod.Labels)) {
			sss = append(sss, ss)
		}
	}
	if len(sss) == 0 {
		err = fmt.Errorf("could not find StatefulSet for pod %s in namespace %s with labels: %v", pod.Name, pod.Namespace, pod.Labels)
	}
	return
}

// StatefulSets 返回 nil，测试场景不会使用按命名空间的查询。
func (f StatefulSetLister) StatefulSets(namespace string) appslisters.StatefulSetNamespaceLister {
	return nil
}

// persistentVolumeClaimNamespaceLister 是 PersistentVolumeClaimLister 返回的命名空间 lister 实现。
type persistentVolumeClaimNamespaceLister struct {
	pvcs      []*v1.PersistentVolumeClaim
	namespace string
}

// Get 按名称与命名空间查找 PVC，未找到时返回 NotFound 错误。
func (f *persistentVolumeClaimNamespaceLister) Get(name string) (*v1.PersistentVolumeClaim, error) {
	for _, pvc := range f.pvcs {
		if pvc.Name == name && pvc.Namespace == f.namespace {
			return pvc, nil
		}
	}
	return nil, errors.NewNotFound(v1.Resource("persistentvolumeclaims"), name)
}

// List 未实现，测试场景不会调用。
func (f persistentVolumeClaimNamespaceLister) List(selector labels.Selector) (ret []*v1.PersistentVolumeClaim, err error) {
	return nil, fmt.Errorf("not implemented")
}

// PersistentVolumeClaimLister 声明 []v1.PersistentVolumeClaim 类型，用于测试。
type PersistentVolumeClaimLister []v1.PersistentVolumeClaim

var _ corelisters.PersistentVolumeClaimLister = PersistentVolumeClaimLister{}

// List 未实现，测试场景不会调用。
func (pvcs PersistentVolumeClaimLister) List(selector labels.Selector) (ret []*v1.PersistentVolumeClaim, err error) {
	return nil, fmt.Errorf("not implemented")
}

// PersistentVolumeClaims 返回一个 fake 的命名空间级 PVC lister。
func (pvcs PersistentVolumeClaimLister) PersistentVolumeClaims(namespace string) corelisters.PersistentVolumeClaimNamespaceLister {
	ps := make([]*v1.PersistentVolumeClaim, len(pvcs))
	for i := range pvcs {
		ps[i] = &pvcs[i]
	}
	return &persistentVolumeClaimNamespaceLister{
		pvcs:      ps,
		namespace: namespace,
	}
}

// NodeInfoLister 基于 []fwk.NodeInfo 实现节点信息查询，供测试使用。
type NodeInfoLister []fwk.NodeInfo

// Get 按节点名返回 fake 节点对象。
func (nodes NodeInfoLister) Get(nodeName string) (fwk.NodeInfo, error) {
	for _, node := range nodes {
		if node != nil && node.Node().Name == nodeName {
			return node, nil
		}
	}
	return nil, fmt.Errorf("unable to find node: %s", nodeName)
}

// List 返回全部节点。
func (nodes NodeInfoLister) List() ([]fwk.NodeInfo, error) {
	return nodes, nil
}

// HavePodsWithAffinityList 本应只返回含亲和性 Pod 的节点；fake 实现直接返回全部节点。
func (nodes NodeInfoLister) HavePodsWithAffinityList() ([]fwk.NodeInfo, error) {
	return nodes, nil
}

// HavePodsWithRequiredAntiAffinityList 本应只返回含强制反亲和性 Pod 的节点；fake 实现直接返回全部节点。
func (nodes NodeInfoLister) HavePodsWithRequiredAntiAffinityList() ([]fwk.NodeInfo, error) {
	return nodes, nil
}

// HavePodsWithRequiredNonHostScopedAntiAffinityList 本应只返回含强制非主机级反亲和性 Pod 的节点；
// fake 实现直接返回全部节点。
func (nodes NodeInfoLister) HavePodsWithRequiredNonHostScopedAntiAffinityList() ([]fwk.NodeInfo, error) {
	return nodes, nil
}

var _ storagelisters.CSINodeLister = CSINodeLister{}

// CSINodeLister 声明 []storagev1.CSINode 类型，用于测试。
type CSINodeLister []storagev1.CSINode

// Get 按名称返回 fake CSINode 对象。
func (n CSINodeLister) Get(name string) (*storagev1.CSINode, error) {
	for _, cn := range n {
		if cn.Name == name {
			return &cn, nil
		}
	}
	return nil, errors.NewNotFound(storagev1.Resource("csinodes"), name)
}

// List 未实现，测试场景不会调用。
func (n CSINodeLister) List(selector labels.Selector) (ret []*storagev1.CSINode, err error) {
	return nil, fmt.Errorf("not implemented")
}

// PersistentVolumeLister 声明 []v1.PersistentVolume 类型，用于测试。
type PersistentVolumeLister []v1.PersistentVolume

var _ corelisters.PersistentVolumeLister = PersistentVolumeLister{}

// Get 按 PV ID 返回 fake PV 对象。
func (pvs PersistentVolumeLister) Get(pvID string) (*v1.PersistentVolume, error) {
	for _, pv := range pvs {
		if pv.Name == pvID {
			return &pv, nil
		}
	}
	return nil, errors.NewNotFound(v1.Resource("persistentvolumes"), pvID)
}

// List 未实现，测试场景不会调用。
func (pvs PersistentVolumeLister) List(selector labels.Selector) ([]*v1.PersistentVolume, error) {
	return nil, fmt.Errorf("not implemented")
}

// StorageClassLister 声明 []storagev1.StorageClass 类型，用于测试。
type StorageClassLister []storagev1.StorageClass

var _ storagelisters.StorageClassLister = StorageClassLister{}

// Get 按名称返回 fake StorageClass 对象。
func (classes StorageClassLister) Get(name string) (*storagev1.StorageClass, error) {
	for _, sc := range classes {
		if sc.Name == name {
			return &sc, nil
		}
	}
	return nil, errors.NewNotFound(storagev1.Resource("storageclasses"), name)
}

// List 未实现，测试场景不会调用。
func (classes StorageClassLister) List(selector labels.Selector) ([]*storagev1.StorageClass, error) {
	return nil, fmt.Errorf("not implemented")
}

// VolumeAttachmentLister 声明 []storagev1.VolumeAttachment 类型，用于测试。
type VolumeAttachmentLister []storagev1.VolumeAttachment

var _ storagelisters.VolumeAttachmentLister = VolumeAttachmentLister{}

// List 返回全部 VolumeAttachment。
func (val VolumeAttachmentLister) List(selector labels.Selector) (ret []*storagev1.VolumeAttachment, err error) {
	var list []*storagev1.VolumeAttachment
	for i := range val {
		list = append(list, &val[i])
	}
	return list, nil
}

// Get 按名称返回 fake VolumeAttachment 对象。
func (val VolumeAttachmentLister) Get(name string) (*storagev1.VolumeAttachment, error) {
	for _, va := range val {
		if va.Name == name {
			return &va, nil
		}
	}
	return nil, errors.NewNotFound(storagev1.Resource("volumeattachments"), name)
}
