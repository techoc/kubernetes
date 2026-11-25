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
	"fmt"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// ReplicationControllerListerExpansion allows custom methods to be added to
// ReplicationControllerLister.
//
// 扩展接口由手写代码实现，生成器会把该接口嵌入 ReplicationControllerLister，
// 从而在不修改生成文件的前提下增加自定义方法。
type ReplicationControllerListerExpansion interface {
	GetPodControllers(pod *v1.Pod) ([]*v1.ReplicationController, error)
}

// ReplicationControllerNamespaceListerExpansion allows custom methods to be added to
// ReplicationControllerNamespaceLister.
type ReplicationControllerNamespaceListerExpansion interface{}

// GetPodControllers returns a list of ReplicationControllers that potentially match a pod.
// Only the one specified in the Pod's ControllerRef will actually manage it.
// Returns an error only if no matching ReplicationControllers are found.
//
// 该方法只按标签筛选“候选”控制器；最终归属仍由 Pod 的 ownerReferences
// （ControllerRef）决定。因此调用方需自行比对 controllerRef 才能确定唯一管理者。
func (s *replicationControllerLister) GetPodControllers(pod *v1.Pod) ([]*v1.ReplicationController, error) {
	// 无标签的 Pod 不可能被选择器匹配，直接报错可避免误判为“无控制器”。
	if len(pod.Labels) == 0 {
		return nil, fmt.Errorf("no controllers found for pod %v because it has no labels", pod.Name)
	}

	// 从本地缓存读取同命名空间的全部 RC，不产生 API 调用。
	items, err := s.ReplicationControllers(pod.Namespace).List(labels.Everything())
	if err != nil {
		return nil, err
	}

	var controllers []*v1.ReplicationController
	for i := range items {
		rc := items[i]
		selector := labels.Set(rc.Spec.Selector).AsSelectorPreValidated()

		// 若混入 nil 或空选择器的 rc，它应匹配零个对象而非全部：
		// 空选择器在 K8s 语义中表示“不选中任何对象”，不能退化为 labels.Everything()。
		if selector.Empty() || !selector.Matches(labels.Set(pod.Labels)) {
			continue
		}
		controllers = append(controllers, rc)
	}

	if len(controllers) == 0 {
		return nil, fmt.Errorf("could not find controller for pod %s in namespace %s with labels: %v", pod.Name, pod.Namespace, pod.Labels)
	}

	return controllers, nil
}
