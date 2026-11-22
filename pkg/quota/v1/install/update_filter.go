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

package install

import (
	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/util/feature"
	podutil "k8s.io/kubernetes/pkg/api/v1/pod"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/quota/v1/evaluator/core"
	"k8s.io/utils/clock"
)

// DefaultUpdateFilter 返回默认的更新过滤器，判断资源更新事件是否需要重新计算配额。
// 只有当资源的配额相关属性发生变化时，才返回 true。
func DefaultUpdateFilter() func(resource schema.GroupVersionResource, oldObj, newObj interface{}) bool {
	return func(resource schema.GroupVersionResource, oldObj, newObj interface{}) bool {
		switch resource.GroupResource() {
		case schema.GroupResource{Resource: "pods"}:
			oldPod := oldObj.(*v1.Pod)
			newPod := newObj.(*v1.Pod)
			// 原地垂直扩缩时，容器资源变化需要重新计算配额
			if feature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) && hasResourcesChanged(oldPod, newPod) {
				return true
			}

			// Pod 的 terminating 状态变化时，需要重新计算配额
			if core.IsTerminating(oldPod) != core.IsTerminating(newPod) {
				return true
			}

			// Pod 从活跃状态变为终止状态，需要释放配额
			return core.QuotaV1Pod(oldPod, clock.RealClock{}) && !core.QuotaV1Pod(newPod, clock.RealClock{})
		case schema.GroupResource{Resource: "services"}:
			oldService := oldObj.(*v1.Service)
			newService := newObj.(*v1.Service)
			// Service 类型变化（如 ClusterIP -> NodePort）需要重新计算配额
			return core.GetQuotaServiceType(oldService) != core.GetQuotaServiceType(newService)
		case schema.GroupResource{Resource: "persistentvolumeclaims"}:
			oldPVC := oldObj.(*v1.PersistentVolumeClaim)
			newPVC := newObj.(*v1.PersistentVolumeClaim)
			// PVC 的 AllocatedResources 或 VolumeAttributesClass 变化时需要补充配额
			return core.RequiresQuotaReplenish(newPVC, oldPVC)
		}

		return false
	}
}

// hasResourcesChanged 比较 Pod 的容器状态中的资源是否发生变化。
// 用于原地垂直扩缩（InPlacePodVerticalScaling）特性，检测资源更新。
func hasResourcesChanged(oldPod *v1.Pod, newPod *v1.Pod) bool {
	for _, oldStatus := range oldPod.Status.ContainerStatuses {
		newStatus, ok := podutil.GetContainerStatus(newPod.Status.ContainerStatuses, oldStatus.Name)
		if ok && !apiequality.Semantic.DeepEqual(oldStatus.Resources, newStatus.Resources) {
			return true
		}
	}
	for _, oldInitContainerStatus := range oldPod.Status.InitContainerStatuses {
		newInitContainerStatus, ok := podutil.GetContainerStatus(newPod.Status.InitContainerStatuses, oldInitContainerStatus.Name)
		if ok && !apiequality.Semantic.DeepEqual(oldInitContainerStatus.Resources, newInitContainerStatus.Resources) {
			return true
		}
	}
	return false
}
