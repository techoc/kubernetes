/*
Copyright 2025 The Kubernetes Authors.

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

package util

import (
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// HasDefaultAnnotation 判断对象元数据上是否设置了“默认 IngressClass”注解。
// 该注解由 networkingv1.AnnotationIsDefaultIngressClass 定义，值必须严格为 "true"。
func HasDefaultAnnotation(obj metav1.ObjectMeta) bool {
	return obj.Annotations[networkingv1.AnnotationIsDefaultIngressClass] == "true"
}
