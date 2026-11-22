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

package v1

const (
	// AnnotationIsDefaultIngressClass 可用于表示某个 IngressClass 应被视为默认类。
	// 当且仅当一个 IngressClass 资源将此注解设置为 true 时，未指定 class 的新 Ingress
	// 资源将被分配此默认类。
	AnnotationIsDefaultIngressClass = "ingressclass.kubernetes.io/is-default-class"
)
