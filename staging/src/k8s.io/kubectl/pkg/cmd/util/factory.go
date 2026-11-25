/*
Copyright 2014 The Kubernetes Authors.

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
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/resource"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	openapiclient "k8s.io/client-go/openapi"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubectl/pkg/util/openapi"
	"k8s.io/kubectl/pkg/validation"
)

// Factory 提供的抽象使 Kubectl 命令能够扩展到多种
// 资源类型和不同的 API 集合。
// 分层（ring）的存在有其原因：为了让组合者能够提供自定义的 factory 实现，
// 他们需要提供*某些*函数的底层组件，这样 factory 回调自身时
// 就会使用这些函数的定制版本。与其试图枚举所有可能被覆盖的内容，
// 不如将 factory 拆分为多个层：每层可以依赖更早层中的方法，
// 但不能依赖同一层中的同级方法。
// TODO: 将这些函数改为接口
// TODO: 将 factory 上的各个接口直接传入命令构造函数（从而
// 让命令与 factory 解耦）。
type Factory interface {
	genericclioptions.RESTClientGetter

	// DynamicClient 返回可直接使用的 dynamic client
	DynamicClient() (dynamic.Interface, error)

	// KubernetesClientSet 返回一个外部的 clientset
	KubernetesClientSet() (*kubernetes.Clientset, error)

	// 返回用于访问 Kubernetes 资源的 RESTClient，或返回错误。
	RESTClient() (*restclient.RESTClient, error)

	// NewBuilder 返回一个对象，可帮助从磁盘和服务端加载对象，
	// 并实现了 CLI 与通用资源交互的常见模式。
	NewBuilder() *resource.Builder

	// 返回用于处理指定 RESTMapping 的 RESTClient，或返回错误。它面向
	// 任意资源，并不保证指向 Kubernetes APIServer。
	ClientForMapping(mapping *meta.RESTMapping) (resource.RESTClient, error)
	// 返回用于处理 Unstructured 对象的 RESTClient。
	UnstructuredClientForMapping(mapping *meta.RESTMapping) (resource.RESTClient, error)

	// 返回一个可用于校验磁盘上对象的 schema。
	Validator(validationDirective string) (validation.Schema, error)

	// 用于获取 openapi v2 资源。
	openapi.OpenAPIResourcesGetter

	// OpenAPIV3Schema 返回一个 client，用于获取任意 group version
	// 的解析后 schema
	OpenAPIV3Client() (openapiclient.Client, error)
}
