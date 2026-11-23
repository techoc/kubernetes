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

// Package ephemeral 提供通用临时内联卷访问 PVC 的标准流程支持：
//
// - 根据内联卷来源确定对应的 PVC 名称
// - 获取该 PVC
// - 校验该 PVC 确实归 Pod 所有
// - 使用该 PVC
//
// 把这些步骤收拢到同一个包，是因为它们必须配套使用：光靠名字找到 PVC 不够，
// 还得验证归属，否则一个 Pod 可能挂上别人创建的同名 PVC。
package ephemeral

import (
	"fmt"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// VolumeClaimName 返回为通用临时内联卷创建的 PersistentVolumeClaim 对象的名字。
// 名字是确定性的，因此本函数只需要 Pod 名和卷名，不需要其他信息，也不会失败。
//
// 在把 PVC 交给 Pod 使用之前，调用方必须调用 VolumeIsForPod 检查它确实是
// 为该 Pod 创建的 PVC。
//
// 名字完全由 Pod 名和卷名拼出，是因为内联卷的 PVC 由控制器代建，
// 双方必须能独立算出同一个名字；这也意味着调用方绝不能自己改这个命名规则。
func VolumeClaimName(pod *v1.Pod, volume *v1.Volume) string {
	return pod.Name + "-" + volume.Name
}

// VolumeIsForPod 检查 PVC 是否为该 Pod 创建的临时卷。
// 返回的错误信息足够完整，调用方可以直接返回，无需再补充 Pod 或 PVC 的细节。
//
// 归属检查是安全边界：名字可预测意味着任何 Pod 都能“猜到”别的 Pod 的 PVC 名，
// 只有同时校验命名空间和 ownerReference，才能防止跨 Pod 误挂 Volume。
func VolumeIsForPod(pod *v1.Pod, pvc *v1.PersistentVolumeClaim) error {
	// 命名空间检查属于纵深防御：调用方本应保证传入的 PVC 与 Pod 同命名空间，
	// 但多查一次成本极低，且能挡住误传对象导致的跨命名空间挂载。
	if pvc.Namespace != pod.Namespace || !metav1.IsControlledBy(pvc, pod) {
		return fmt.Errorf("PVC %s/%s was not created for pod %s/%s (pod is not owner)", pvc.Namespace, pvc.Name, pod.Namespace, pod.Name)
	}
	return nil
}
