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

package pods

import (
	"fmt"

	"k8s.io/apimachinery/pkg/util/validation/field"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/fieldpath"
)

// ContainerVisitorWithPath 在访问每个容器及其对应的 field.Path 时被调用，返回 true 表示继续访问。
type ContainerVisitorWithPath func(container *api.Container, path *field.Path) bool

// VisitContainersWithPath 依次用指向每个容器 spec 的指针及其 field.Path 调用 visitor，
// 容器范围包括 initContainers、containers 与 ephemeralContainers。
// 若 visitor 返回 false，则提前结束遍历。全部访问完成则返回 true，被提前终止则返回 false。
func VisitContainersWithPath(podSpec *api.PodSpec, specPath *field.Path, visitor ContainerVisitorWithPath) bool {
	fldPath := specPath.Child("initContainers")
	for i := range podSpec.InitContainers {
		if !visitor(&podSpec.InitContainers[i], fldPath.Index(i)) {
			return false
		}
	}
	fldPath = specPath.Child("containers")
	for i := range podSpec.Containers {
		if !visitor(&podSpec.Containers[i], fldPath.Index(i)) {
			return false
		}
	}
	fldPath = specPath.Child("ephemeralContainers")
	for i := range podSpec.EphemeralContainers {
		if !visitor((*api.Container)(&podSpec.EphemeralContainers[i].EphemeralContainerCommon), fldPath.Index(i)) {
			return false
		}
	}
	return true
}

// ConvertDownwardAPIFieldLabel 将指定版本 Pod 中的 downward API 字段标签及其值转换为内部版本，
// 返回转换后的标签与值。转换失败时返回错误。
// 目前仅支持 v1 版本，并对旧客户端发送的 spec.host 做兼容映射到 spec.nodeName。
func ConvertDownwardAPIFieldLabel(version, label, value string) (string, string, error) {
	if version != "v1" {
		return "", "", fmt.Errorf("unsupported pod version: %s", version)
	}

	// metadata.annotations / metadata.labels 允许带下标（如 metadata.labels['foo']），
	// 其余字段的下标形式不支持，直接拒绝以免生成无效的 downward API 引用。
	if path, _, ok := fieldpath.SplitMaybeSubscriptedPath(label); ok {
		switch path {
		case "metadata.annotations", "metadata.labels":
			return label, value, nil
		default:
			return "", "", fmt.Errorf("field label does not support subscript: %s", label)
		}
	}

	switch label {
	case "metadata.annotations",
		"metadata.labels",
		"metadata.name",
		"metadata.namespace",
		"metadata.uid",
		"spec.nodeName",
		"spec.restartPolicy",
		"spec.serviceAccountName",
		"spec.schedulerName",
		"status.phase",
		"status.hostIP",
		"status.hostIPs",
		"status.podIP",
		"status.podIPs":
		return label, value, nil
	// 兼容仍发送 spec.host 的旧 v1 客户端，将其映射到新的 spec.nodeName。
	case "spec.host":
		return "spec.nodeName", value, nil
	default:
		return "", "", fmt.Errorf("field label not supported: %s", label)
	}
}
