/*
Copyright 2018 The Kubernetes Authors.

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

package dynamic

import (
	"context"
	"fmt"
	"net/http"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/features"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/apply"
)

// DynamicClient 是动态客户端的实现：不依赖类型化代码，所有操作都以
// unstructured.Unstructured 与 GroupVersionResource 为参数，
// 因而能处理 apiserver 暴露的任意资源（包括 CRD）。
type DynamicClient struct {
	client rest.Interface
}

// 编译期断言：DynamicClient 实现了 Interface。
var _ Interface = &DynamicClient{}

// ConfigFor returns a copy of the provided config with the
// appropriate dynamic client defaults set.
//
// ConfigFor 返回一份设置了动态客户端默认值的配置副本（不修改入参）。
// 动态客户端无法预知资源类型，因此使用 unstructured 协商序列化器，
// 并按特性开关决定是否启用 CBOR 以降低传输开销。
func ConfigFor(inConfig *rest.Config) *rest.Config {
	config := rest.CopyConfig(inConfig)

	config.ContentType = "application/json"
	config.AcceptContentTypes = "application/json"
	// CBOR 特性门控：启用时优先协商 CBOR（q=1），JSON 仍作为回退（q=0.9）。
	if features.FeatureGates().Enabled(features.ClientsAllowCBOR) {
		config.AcceptContentTypes = "application/json;q=0.9,application/cbor;q=1"
		if features.FeatureGates().Enabled(features.ClientsPreferCBOR) {
			config.ContentType = "application/cbor"
		}
	}

	config.NegotiatedSerializer = newBasicNegotiatedSerializer()
	if config.UserAgent == "" {
		config.UserAgent = rest.DefaultKubernetesUserAgent()
	}
	return config
}

// New creates a new DynamicClient for the given RESTClient.
func New(c rest.Interface) *DynamicClient {
	return &DynamicClient{client: c}
}

// NewForConfigOrDie creates a new DynamicClient for the given config and
// panics if there is an error in the config.
func NewForConfigOrDie(c *rest.Config) *DynamicClient {
	ret, err := NewForConfig(c)
	if err != nil {
		panic(err)
	}
	return ret
}

// NewForConfig creates a new dynamic client or returns an error.
// NewForConfig is equivalent to NewForConfigAndClient(c, httpClient),
// where httpClient was generated with rest.HTTPClientFor(c).
func NewForConfig(inConfig *rest.Config) (*DynamicClient, error) {
	config := ConfigFor(inConfig)

	httpClient, err := rest.HTTPClientFor(config)
	if err != nil {
		return nil, err
	}
	return NewForConfigAndClient(config, httpClient)
}

// NewForConfigAndClient creates a new dynamic client for the given config and http client.
// Note the http client provided takes precedence over the configured transport values.
//
// NewForConfigAndClient 用调用方提供的 http.Client 创建动态客户端；
// 由于动态客户端按资源拼接绝对路径，这里使用 UnversionedRESTClient，
// 并把 GroupVersion 置空、APIPath 设为占位值，避免 rest 客户端自动前缀。
func NewForConfigAndClient(inConfig *rest.Config, h *http.Client) (*DynamicClient, error) {
	config := ConfigFor(inConfig)
	config.GroupVersion = nil
	// 占位路径：动态客户端总用 AbsPath 指定完整路径，此值不应被使用；
	// 若意外出现在 URL 中，便于排查拼路径错误。
	config.APIPath = "/if-you-see-this-search-for-the-break"

	restClient, err := rest.UnversionedRESTClientForConfigAndClient(config, h)
	if err != nil {
		return nil, err
	}
	return &DynamicClient{client: restClient}, nil
}

// dynamicResourceClient 绑定到一个具体资源（GVR），可再附加命名空间。
type dynamicResourceClient struct {
	client    *DynamicClient
	namespace string
	resource  schema.GroupVersionResource
}

// Resource 返回绑定到指定 GVR 的资源客户端。
func (c *DynamicClient) Resource(resource schema.GroupVersionResource) NamespaceableResourceInterface {
	return &dynamicResourceClient{client: c, resource: resource}
}

// Namespace 返回绑定到 ns 的副本；不修改原对象，因此可安全复用于集群级操作。
func (c *dynamicResourceClient) Namespace(ns string) ResourceInterface {
	ret := *c
	ret.namespace = ns
	return &ret
}

// Create 创建对象。对子资源（如 status）执行 Create 时必须显式提供名称，
// 因为子资源路径需要 name 段。
func (c *dynamicResourceClient) Create(ctx context.Context, obj *unstructured.Unstructured, opts metav1.CreateOptions, subresources ...string) (*unstructured.Unstructured, error) {
	name := ""
	if len(subresources) > 0 {
		accessor, err := meta.Accessor(obj)
		if err != nil {
			return nil, err
		}
		name = accessor.GetName()
		if len(name) == 0 {
			return nil, fmt.Errorf("name is required")
		}
	}
	if err := validateNamespaceWithOptionalName(c.namespace, name); err != nil {
		return nil, err
	}

	var out unstructured.Unstructured
	if err := c.client.client.
		Post().
		AbsPath(append(c.makeURLSegments(name), subresources...)...).
		Body(obj).
		SpecificallyVersionedParams(&opts, dynamicParameterCodec, versionV1).
		Do(ctx).Into(&out); err != nil {
		return nil, err
	}

	return &out, nil
}

// Update 整体替换对象；必须能从对象中取得名称，且名称与路径段一致。
func (c *dynamicResourceClient) Update(ctx context.Context, obj *unstructured.Unstructured, opts metav1.UpdateOptions, subresources ...string) (*unstructured.Unstructured, error) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	name := accessor.GetName()
	if len(name) == 0 {
		return nil, fmt.Errorf("name is required")
	}
	if err := validateNamespaceWithOptionalName(c.namespace, name); err != nil {
		return nil, err
	}

	var out unstructured.Unstructured
	if err := c.client.client.
		Put().
		AbsPath(append(c.makeURLSegments(name), subresources...)...).
		Body(obj).
		SpecificallyVersionedParams(&opts, dynamicParameterCodec, versionV1).
		Do(ctx).Into(&out); err != nil {
		return nil, err
	}

	return &out, nil
}

// UpdateStatus 通过 status 子资源替换对象状态，从而绕过 spec 的乐观并发检查，
// 避免与控制器对 spec 的更新相互覆盖。
func (c *dynamicResourceClient) UpdateStatus(ctx context.Context, obj *unstructured.Unstructured, opts metav1.UpdateOptions) (*unstructured.Unstructured, error) {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	name := accessor.GetName()
	if len(name) == 0 {
		return nil, fmt.Errorf("name is required")
	}
	if err := validateNamespaceWithOptionalName(c.namespace, name); err != nil {
		return nil, err
	}

	var out unstructured.Unstructured
	if err := c.client.client.
		Put().
		AbsPath(append(c.makeURLSegments(name), "status")...).
		Body(obj).
		SpecificallyVersionedParams(&opts, dynamicParameterCodec, versionV1).
		Do(ctx).Into(&out); err != nil {
		return nil, err
	}

	return &out, nil
}

// Delete 删除单个对象；opts 中可携带 UID/预条件等以提供幂等保护。
func (c *dynamicResourceClient) Delete(ctx context.Context, name string, opts metav1.DeleteOptions, subresources ...string) error {
	if len(name) == 0 {
		return fmt.Errorf("name is required")
	}
	if err := validateNamespaceWithOptionalName(c.namespace, name); err != nil {
		return err
	}

	result := c.client.client.
		Delete().
		AbsPath(append(c.makeURLSegments(name), subresources...)...).
		Body(&opts).
		Do(ctx)
	return result.Error()
}

// DeleteCollection 批量删除集合；路径中不含名称，listOptions 用于限定范围。
func (c *dynamicResourceClient) DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOptions metav1.ListOptions) error {
	if err := validateNamespaceWithOptionalName(c.namespace); err != nil {
		return err
	}

	result := c.client.client.
		Delete().
		AbsPath(c.makeURLSegments("")...).
		Body(&opts).
		SpecificallyVersionedParams(&listOptions, dynamicParameterCodec, versionV1).
		Do(ctx)
	return result.Error()
}

// Get 读取单个对象，结果解码为 unstructured。
func (c *dynamicResourceClient) Get(ctx context.Context, name string, opts metav1.GetOptions, subresources ...string) (*unstructured.Unstructured, error) {
	if len(name) == 0 {
		return nil, fmt.Errorf("name is required")
	}
	if err := validateNamespaceWithOptionalName(c.namespace, name); err != nil {
		return nil, err
	}
	var out unstructured.Unstructured
	if err := c.client.client.
		Get().
		AbsPath(append(c.makeURLSegments(name), subresources...)...).
		SpecificallyVersionedParams(&opts, dynamicParameterCodec, versionV1).
		Do(ctx).Into(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// List 列举集合；返回 UnstructuredList，条目均为 unstructured。
func (c *dynamicResourceClient) List(ctx context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error) {
	if err := validateNamespaceWithOptionalName(c.namespace); err != nil {
		return nil, err
	}
	var out unstructured.UnstructuredList
	if err := c.client.client.
		Get().
		AbsPath(c.makeURLSegments("")...).
		SpecificallyVersionedParams(&opts, dynamicParameterCodec, versionV1).
		Do(ctx).Into(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Watch 通过设置 ListOptions.Watch 复用 List 端点，返回 watch.Interface。
func (c *dynamicResourceClient) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	opts.Watch = true
	if err := validateNamespaceWithOptionalName(c.namespace); err != nil {
		return nil, err
	}
	return c.client.client.Get().AbsPath(c.makeURLSegments("")...).
		SpecificallyVersionedParams(&opts, dynamicParameterCodec, versionV1).
		Watch(ctx)
}

// Patch 以指定补丁类型（JSON merge / strategic merge / JSON patch / apply）修改对象。
func (c *dynamicResourceClient) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (*unstructured.Unstructured, error) {
	if len(name) == 0 {
		return nil, fmt.Errorf("name is required")
	}
	if err := validateNamespaceWithOptionalName(c.namespace, name); err != nil {
		return nil, err
	}
	var out unstructured.Unstructured
	if err := c.client.client.
		Patch(pt).
		AbsPath(append(c.makeURLSegments(name), subresources...)...).
		Body(data).
		SpecificallyVersionedParams(&opts, dynamicParameterCodec, versionV1).
		Do(ctx).Into(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// Apply 执行 server-side apply：把对象转换为 apply configuration 补丁后提交。
// 若对象已带有 managedFields，说明它来自服务端而非“期望状态”，
// 直接 apply 会污染字段管理归属，因此拒绝并提示改用 UnstructuredExtractor。
func (c *dynamicResourceClient) Apply(ctx context.Context, name string, obj *unstructured.Unstructured, opts metav1.ApplyOptions, subresources ...string) (*unstructured.Unstructured, error) {
	if len(name) == 0 {
		return nil, fmt.Errorf("name is required")
	}
	if err := validateNamespaceWithOptionalName(c.namespace, name); err != nil {
		return nil, err
	}
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	managedFields := accessor.GetManagedFields()
	if len(managedFields) > 0 {
		return nil, fmt.Errorf(`cannot apply an object with managed fields already set.
		Use the client-go/applyconfigurations "UnstructructuredExtractor" to obtain the unstructured ApplyConfiguration for the given field manager that you can use/modify here to apply`)
	}
	patchOpts := opts.ToPatchOptions()

	// 将 unstructured 转换为 apply 补丁请求（content-type: apply-patch+yaml 等）。
	request, err := apply.NewRequest(c.client.client, obj.Object)
	if err != nil {
		return nil, err
	}

	var out unstructured.Unstructured
	if err := request.
		AbsPath(append(c.makeURLSegments(name), subresources...)...).
		SpecificallyVersionedParams(&patchOpts, dynamicParameterCodec, versionV1).
		Do(ctx).Into(&out); err != nil {
		return nil, err
	}
	return &out, nil
}

// ApplyStatus 等价于对 status 子资源执行 Apply。
func (c *dynamicResourceClient) ApplyStatus(ctx context.Context, name string, obj *unstructured.Unstructured, opts metav1.ApplyOptions) (*unstructured.Unstructured, error) {
	return c.Apply(ctx, name, obj, opts, "status")
}

// validateNamespaceWithOptionalName 校验命名空间与（可选的）名称能否安全用作 URL 路径段。
// 该检查防止路径注入与歧义（例如名称中含 "/" 或 ".."），必须在拼接 URL 前执行。
func validateNamespaceWithOptionalName(namespace string, name ...string) error {
	if msgs := rest.IsValidPathSegmentName(namespace); len(msgs) != 0 {
		return fmt.Errorf("invalid namespace %q: %v", namespace, msgs)
	}
	if len(name) > 1 {
		panic("Invalid number of names")
	} else if len(name) == 1 {
		if msgs := rest.IsValidPathSegmentName(name[0]); len(msgs) != 0 {
			return fmt.Errorf("invalid resource name %q: %v", name[0], msgs)
		}
	}
	return nil
}

// makeURLSegments 按 Kubernetes API 的 URL 结构拼出路径段：
// 核心组用 /api，其他组用 /apis/<group>，随后是版本、命名空间（可选）、资源与名称（可选）。
func (c *dynamicResourceClient) makeURLSegments(name string) []string {
	url := []string{}
	if len(c.resource.Group) == 0 {
		url = append(url, "api")
	} else {
		url = append(url, "apis", c.resource.Group)
	}
	url = append(url, c.resource.Version)

	if len(c.namespace) > 0 {
		url = append(url, "namespaces", c.namespace)
	}
	url = append(url, c.resource.Resource)

	if len(name) > 0 {
		url = append(url, name)
	}

	return url
}
