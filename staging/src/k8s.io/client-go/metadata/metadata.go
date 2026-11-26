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

package metadata

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"k8s.io/klog/v2"

	metainternalversionscheme "k8s.io/apimachinery/pkg/apis/meta/internalversion/scheme"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
)

// deleteScheme/parameterScheme 只注册 metav1 基础类型，分别服务于删除选项编码和查询参数编码。
var deleteScheme = runtime.NewScheme()
var parameterScheme = runtime.NewScheme()
var deleteOptionsCodec = serializer.NewCodecFactory(deleteScheme)
var dynamicParameterCodec = runtime.NewParameterCodec(parameterScheme)

var versionV1 = schema.GroupVersion{Version: "v1"}

func init() {
	metav1.AddToGroupVersion(parameterScheme, versionV1)
	metav1.AddToGroupVersion(deleteScheme, versionV1)
}

// Client 让调用方能获取任意兼容 Kubernetes 的 API 端点的对象元数据。
// 它优先通过 meta.k8s.io/v1 PartialObjectMetadata 只取所需元数据以节省带宽；
// 对旧服务端（Kubernetes 1.14 及更早）则拉回整个对象再提取元数据。
type Client struct {
	client *rest.RESTClient
}

var _ Interface = &Client{}

// ConfigFor 复制配置并设置 metadata 客户端所需的默认值。
func ConfigFor(inConfig *rest.Config) *rest.Config {
	config := rest.CopyConfig(inConfig)
	// 优先请求 protobuf 以降低带宽；回退 JSON 兼容不支持 protobuf 的端点。
	config.AcceptContentTypes = "application/vnd.kubernetes.protobuf,application/json"
	config.ContentType = "application/vnd.kubernetes.protobuf"
	// WithoutConversion 让 meta.k8s.io 内部类型直接用于序列化，省去版本转换。
	config.NegotiatedSerializer = metainternalversionscheme.Codecs.WithoutConversion()
	if config.UserAgent == "" {
		config.UserAgent = rest.DefaultKubernetesUserAgent()
	}
	return config
}

// NewForConfigOrDie 根据配置创建 metadata 客户端，配置有误时直接 panic。
func NewForConfigOrDie(c *rest.Config) Interface {
	ret, err := NewForConfig(c)
	if err != nil {
		panic(err)
	}
	return ret
}

// NewForConfig 创建 metadata 客户端，以 PartialObjectMetadata 形式获取核心、
// 聚合或自定义资源的元数据；配置有误时返回错误。
// 等价于 NewForConfigAndClient(c, httpClient)，其中 httpClient 由 rest.HTTPClientFor(c) 生成。
func NewForConfig(inConfig *rest.Config) (Interface, error) {
	config := ConfigFor(inConfig)

	httpClient, err := rest.HTTPClientFor(config)
	if err != nil {
		return nil, err
	}
	return NewForConfigAndClient(config, httpClient)
}

// NewForConfigAndClient 根据配置与 http client 创建 metadata 客户端，传入的 http client 优先于配置中的 transport 设置。
func NewForConfigAndClient(inConfig *rest.Config, h *http.Client) (Interface, error) {
	config := ConfigFor(inConfig)
	// 用于序列化选项。
	// GroupVersion 与 APIPath 留占位值：实际路径由每个请求的 AbsPath 决定，
	// 这里的值绝不会出现在线上请求中。
	config.GroupVersion = &schema.GroupVersion{}
	config.APIPath = "/this-value-should-never-be-sent"

	restClient, err := rest.RESTClientForConfigAndClient(config, h)
	if err != nil {
		return nil, err
	}

	return &Client{client: restClient}, nil
}

// client 绑定某一 GVR，并可选绑定命名空间。
//
// 值拷贝语义（Namespace 中复制结构体）使不同命名空间的视图互不影响，
// 也避免共享可变状态。
type client struct {
	client    *Client
	namespace string
	resource  schema.GroupVersionResource
}

// Resource 返回可访问集群级或命名空间级资源的接口。
func (c *Client) Resource(resource schema.GroupVersionResource) Getter {
	return &client{client: c, resource: resource}
}

// Namespace 返回可访问指定资源命名空间级实例的接口。
func (c *client) Namespace(ns string) ResourceInterface {
	ret := *c
	ret.namespace = ns
	return &ret
}

// Delete 从服务端删除指定资源。
func (c *client) Delete(ctx context.Context, name string, opts metav1.DeleteOptions, subresources ...string) error {
	if len(name) == 0 {
		return fmt.Errorf("name is required")
	}
	// 若把 DeleteOptions 交给 Negotiator 序列化，请求会带 "Content-Type: application/vnd.kubernetes.protobuf"；
	// apiextensions-apiserver 用 unstructuredNegotiatedSerializer 解码后会返回 406。
	// 这里特殊处理以兼容 CRD Handler，详见：
	// see: https://github.com/kubernetes/kubernetes/blob/1a845ccd076bbf1b03420fe694c85a5cd3bd6bed/staging/src/k8s.io/apiextensions-apiserver/pkg/apiserver/customresource_handler.go#L843
	//
	// 因此这里手工编码 JSON 并显式设置 Content-Type，绕过协商序列化器。
	deleteOptionsByte, err := runtime.Encode(deleteOptionsCodec.LegacyCodec(schema.GroupVersion{Version: "v1"}), &opts)
	if err != nil {
		return err
	}

	result := c.client.client.
		Delete().
		AbsPath(append(c.makeURLSegments(name), subresources...)...).
		SetHeader("Content-Type", runtime.ContentTypeJSON).
		Body(deleteOptionsByte).
		Do(ctx)
	return result.Error()
}

// DeleteCollection 删除指定范围（命名空间或集群）内的所有资源。
func (c *client) DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOptions metav1.ListOptions) error {
	// 与 Delete 同理。
	// 同样手工编码 DeleteOptions，避免 CRD 端点因 protobuf Content-Type 返回 406。
	deleteOptionsByte, err := runtime.Encode(deleteOptionsCodec.LegacyCodec(schema.GroupVersion{Version: "v1"}), &opts)
	if err != nil {
		return err
	}

	result := c.client.client.
		Delete().
		AbsPath(c.makeURLSegments("")...).
		SetHeader("Content-Type", runtime.ContentTypeJSON).
		Body(deleteOptionsByte).
		SpecificallyVersionedParams(&listOptions, dynamicParameterCodec, versionV1).
		Do(ctx)
	return result.Error()
}

// Get 从指定范围（命名空间或集群）返回指定名称的资源。
func (c *client) Get(ctx context.Context, name string, opts metav1.GetOptions, subresources ...string) (*metav1.PartialObjectMetadata, error) {
	if len(name) == 0 {
		return nil, fmt.Errorf("name is required")
	}
	// Accept 头同时声明 PartialObjectMetadata（protobuf 与 JSON），
	// 让支持 meta.k8s.io/v1 的服务器只返回元数据而非完整对象。
	result := c.client.client.Get().AbsPath(append(c.makeURLSegments(name), subresources...)...).
		SetHeader("Accept", "application/vnd.kubernetes.protobuf;as=PartialObjectMetadata;g=meta.k8s.io;v=v1,application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1,application/json").
		SpecificallyVersionedParams(&opts, dynamicParameterCodec, versionV1).
		Do(ctx)
	if err := result.Error(); err != nil {
		return nil, err
	}
	obj, err := result.Get()
	if runtime.IsNotRegisteredError(err) {
		// 旧服务器（1.14 及更早）不认识 PartialObjectMetadata，返回的是完整对象；
		// 这里从原始字节中尽力提取 ObjectMeta。
		klog.FromContext(ctx).V(5).Info("Could not retrieve PartialObjectMetadata", "err", err)
		rawBytes, err := result.Raw()
		if err != nil {
			return nil, err
		}
		var partial metav1.PartialObjectMetadata
		if err := json.Unmarshal(rawBytes, &partial); err != nil {
			return nil, fmt.Errorf("unable to decode returned object as PartialObjectMetadata: %v", err)
		}
		// 校验关键元数据字段，防止把任意 JSON 误当作对象元数据。
		if !isLikelyObjectMetadata(&partial) {
			return nil, fmt.Errorf("object does not appear to match the ObjectMeta schema: %#v", partial)
		}
		partial.TypeMeta = metav1.TypeMeta{}
		return &partial, nil
	}
	if err != nil {
		return nil, err
	}
	partial, ok := obj.(*metav1.PartialObjectMetadata)
	if !ok {
		return nil, fmt.Errorf("unexpected object, expected PartialObjectMetadata but got %T", obj)
	}
	return partial, nil
}

// List 返回指定范围（命名空间或集群）内的所有资源。
func (c *client) List(ctx context.Context, opts metav1.ListOptions) (*metav1.PartialObjectMetadataList, error) {
	result := c.client.client.Get().AbsPath(c.makeURLSegments("")...).
		SetHeader("Accept", "application/vnd.kubernetes.protobuf;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1,application/json;as=PartialObjectMetadataList;g=meta.k8s.io;v=v1,application/json").
		SpecificallyVersionedParams(&opts, dynamicParameterCodec, versionV1).
		Do(ctx)
	if err := result.Error(); err != nil {
		return nil, err
	}
	obj, err := result.Get()
	if runtime.IsNotRegisteredError(err) {
		// 同 Get：兼容旧服务器，从完整对象列表中退化为元数据列表。
		klog.FromContext(ctx).V(5).Info("Could not retrieve PartialObjectMetadataList", "err", err)
		rawBytes, err := result.Raw()
		if err != nil {
			return nil, err
		}
		var partial metav1.PartialObjectMetadataList
		if err := json.Unmarshal(rawBytes, &partial); err != nil {
			return nil, fmt.Errorf("unable to decode returned object as PartialObjectMetadataList: %v", err)
		}
		partial.TypeMeta = metav1.TypeMeta{}
		return &partial, nil
	}
	if err != nil {
		return nil, err
	}
	partial, ok := obj.(*metav1.PartialObjectMetadataList)
	if !ok {
		return nil, fmt.Errorf("unexpected object, expected PartialObjectMetadata but got %T", obj)
	}
	return partial, nil
}

// Watch 监听指定范围（命名空间或集群）内资源的所有变化。
func (c *client) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	// watch 请求必须显式置位 Watch=true，事件同样以 PartialObjectMetadata 形式返回。
	opts.Watch = true
	return c.client.client.Get().
		AbsPath(c.makeURLSegments("")...).
		SetHeader("Accept", "application/vnd.kubernetes.protobuf;as=PartialObjectMetadata;g=meta.k8s.io;v=v1,application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1,application/json").
		SpecificallyVersionedParams(&opts, dynamicParameterCodec, versionV1).
		Timeout(timeout).
		Watch(ctx)
}

// Patch 修改指定范围（命名空间或集群）内的具名资源。
func (c *client) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (*metav1.PartialObjectMetadata, error) {
	if len(name) == 0 {
		return nil, fmt.Errorf("name is required")
	}
	result := c.client.client.
		Patch(pt).
		AbsPath(append(c.makeURLSegments(name), subresources...)...).
		Body(data).
		SetHeader("Accept", "application/vnd.kubernetes.protobuf;as=PartialObjectMetadata;g=meta.k8s.io;v=v1,application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1,application/json").
		SpecificallyVersionedParams(&opts, dynamicParameterCodec, versionV1).
		Do(ctx)
	if err := result.Error(); err != nil {
		return nil, err
	}
	obj, err := result.Get()
	if runtime.IsNotRegisteredError(err) {
		// 旧服务器返回完整对象，按同样策略做 JSON 降级解码。
		rawBytes, err := result.Raw()
		if err != nil {
			return nil, err
		}
		var partial metav1.PartialObjectMetadata
		if err := json.Unmarshal(rawBytes, &partial); err != nil {
			return nil, fmt.Errorf("unable to decode returned object as PartialObjectMetadata: %v", err)
		}
		if !isLikelyObjectMetadata(&partial) {
			return nil, fmt.Errorf("object does not appear to match the ObjectMeta schema")
		}
		partial.TypeMeta = metav1.TypeMeta{}
		return &partial, nil
	}
	if err != nil {
		return nil, err
	}
	partial, ok := obj.(*metav1.PartialObjectMetadata)
	if !ok {
		return nil, fmt.Errorf("unexpected object, expected PartialObjectMetadata but got %T", obj)
	}
	return partial, nil
}

// makeURLSegments 按 Kubernetes REST 路径约定拼装请求路径：
// 核心组用 /api，其余组用 /apis/<group>，命名空间资源额外插入 namespaces/<ns>。
func (c *client) makeURLSegments(name string) []string {
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

// isLikelyObjectMetadata 用若干必填元数据字段做启发式校验：
// 真正的对象至少应有 UID、creationTimestamp、name 或 generateName 之一。
func isLikelyObjectMetadata(meta *metav1.PartialObjectMetadata) bool {
	return len(meta.UID) > 0 || !meta.CreationTimestamp.IsZero() || len(meta.Name) > 0 || len(meta.GenerateName) > 0
}
