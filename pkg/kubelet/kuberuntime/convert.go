/*
Copyright 2020 The Kubernetes Authors.

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

package kuberuntime

import (
	"sort"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubernetes/pkg/features"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

// 本文件包含 kuberuntime 类型与 CRI 运行时 API 类型之间互相转换的辅助函数。

// ToKubeContainerImageSpec 把 CRI Image 转换为 kubelet 内部 ImageSpec，
// 并按键排序注解，保证结果稳定。
func ToKubeContainerImageSpec(image *runtimeapi.Image) kubecontainer.ImageSpec {
	var annotations []kubecontainer.Annotation

	if image.Spec != nil && len(image.Spec.Annotations) > 0 {
		annotationKeys := make([]string, 0, len(image.Spec.Annotations))
		for k := range image.Spec.Annotations {
			annotationKeys = append(annotationKeys, k)
		}
		sort.Strings(annotationKeys)
		for _, k := range annotationKeys {
			annotations = append(annotations, kubecontainer.Annotation{
				Name:  k,
				Value: image.Spec.Annotations[k],
			})
		}
	}

	spec := kubecontainer.ImageSpec{
		Image:       image.Id,
		Annotations: annotations,
	}
	// 启用 RuntimeClassInImageCriAPI 特性时，设置 CRI 的 runtimeHandler 字段。
	if utilfeature.DefaultFeatureGate.Enabled(features.RuntimeClassInImageCriAPI) {
		runtimeHandler := ""
		if image.Spec != nil {
			runtimeHandler = image.Spec.RuntimeHandler
		}
		spec.RuntimeHandler = runtimeHandler
	}

	return spec
}

// ToRuntimeAPIImageSpec 把内部 ImageSpec 转换为 CRI ImageSpec。
func ToRuntimeAPIImageSpec(imageSpec kubecontainer.ImageSpec) *runtimeapi.ImageSpec {
	var annotations = make(map[string]string)
	if imageSpec.Annotations != nil {
		for _, a := range imageSpec.Annotations {
			annotations[a.Name] = a.Value
		}
	}

	spec := runtimeapi.ImageSpec{
		Image:       imageSpec.Image,
		Annotations: annotations,
	}
	// 启用 RuntimeClassInImageCriAPI 特性时，设置 CRI 的 runtimeHandler 字段。
	if utilfeature.DefaultFeatureGate.Enabled(features.RuntimeClassInImageCriAPI) {
		spec.RuntimeHandler = imageSpec.RuntimeHandler
	}

	return &spec
}
