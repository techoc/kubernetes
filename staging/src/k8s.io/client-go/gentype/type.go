/*
Copyright 2024 The Kubernetes Authors.

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

package gentype

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/apply"
)

// objectWithMeta 匹配同时实现 runtime.Object 与 metav1.Object 的对象。
// 既可序列化又带 metadata，是泛型客户端对对象的最小要求。
type objectWithMeta interface {
	runtime.Object
	metav1.Object
}

// namedObject 匹配实现 GetName() 且可比较的对象，用于 apply 声明式配置。
// 要求 comparable，是为了能用零值比较判断 Apply 传入的对象是否为 nil（C 可能是值类型而非指针）。
type namedObject interface {
	comparable
	GetName() *string
}

// Client 表示可选命名空间、不支持 list 与 apply 声明式配置的客户端。
// 所有生成类型客户端共享的基础实现：通过 rest.Interface 拼接通用 REST 请求，
// 具体资源名、命名空间与空对象构造器由生成代码注入。
type Client[T objectWithMeta] struct {
	resource       string
	client         rest.Interface
	namespace      string // "" for non-namespaced clients
	newObject      func() T
	parameterCodec runtime.ParameterCodec

	// prefersProtobuf 控制请求时是否优先协商 protobuf；由 NewClient 的 Option 设置。
	prefersProtobuf bool
}

// ClientWithList 表示支持 list 的客户端。
// 通过组合 alsoLister 获得 List/DeleteCollection 能力，而不是继承，便于按需拼装不同能力。
type ClientWithList[T objectWithMeta, L runtime.Object] struct {
	*Client[T]
	alsoLister[T, L]
}

// ClientWithApply 表示支持 apply 声明式配置的客户端。
type ClientWithApply[T objectWithMeta, C namedObject] struct {
	*Client[T]
	alsoApplier[T, C]
}

// ClientWithListAndApply 表示同时支持 list 与 apply 声明式配置的客户端。
type ClientWithListAndApply[T objectWithMeta, L runtime.Object, C namedObject] struct {
	*Client[T]
	alsoLister[T, L]
	alsoApplier[T, C]
}

// 用于组合的辅助类型：alsoLister 为 Client 组合出列表能力；
// 它持有 client 指针以复用同一 REST 配置。
type alsoLister[T objectWithMeta, L runtime.Object] struct {
	client  *Client[T]
	newList func() L
}

// alsoApplier 为 Client 组合出 apply 能力。
type alsoApplier[T objectWithMeta, C namedObject] struct {
	client *Client[T]
}

// Option 是构造 Client 时可选的配置项（函数式选项模式），避免构造函数参数继续膨胀。
type Option[T objectWithMeta] func(*Client[T])

// PrefersProtobuf 让客户端在支持时优先使用 protobuf 编解码，通常能显著降低带宽与 CPU 开销。
func PrefersProtobuf[T objectWithMeta]() Option[T] {
	return func(c *Client[T]) { c.prefersProtobuf = true }
}

// NewClient 构造客户端，可带命名空间或不带；不支持 list 与 apply。
// 传入空 namespace ("") 即构造非命名空间客户端。
func NewClient[T objectWithMeta](
	resource string, client rest.Interface, parameterCodec runtime.ParameterCodec, namespace string, emptyObjectCreator func() T,
	options ...Option[T],
) *Client[T] {
	c := &Client[T]{
		resource:       resource,
		client:         client,
		parameterCodec: parameterCodec,
		namespace:      namespace,
		newObject:      emptyObjectCreator,
	}
	// 按调用方给出的顺序应用选项。
	for _, option := range options {
		option(c)
	}
	return c
}

// NewClientWithList 构造支持 list 的命名空间客户端。
func NewClientWithList[T objectWithMeta, L runtime.Object](
	resource string, client rest.Interface, parameterCodec runtime.ParameterCodec, namespace string, emptyObjectCreator func() T,
	emptyListCreator func() L, options ...Option[T],
) *ClientWithList[T, L] {
	typeClient := NewClient[T](resource, client, parameterCodec, namespace, emptyObjectCreator, options...)
	// 复用同一个 Client 实例，确保列表请求与单对象请求共享命名空间、编码器等配置。
	return &ClientWithList[T, L]{
		typeClient,
		alsoLister[T, L]{typeClient, emptyListCreator},
	}
}

// NewClientWithApply 构造支持 apply 声明式配置的命名空间客户端。
func NewClientWithApply[T objectWithMeta, C namedObject](
	resource string, client rest.Interface, parameterCodec runtime.ParameterCodec, namespace string, emptyObjectCreator func() T,
	options ...Option[T],
) *ClientWithApply[T, C] {
	typeClient := NewClient[T](resource, client, parameterCodec, namespace, emptyObjectCreator, options...)
	return &ClientWithApply[T, C]{
		typeClient,
		alsoApplier[T, C]{typeClient},
	}
}

// NewClientWithListAndApply 构造同时支持 list 与 apply 的命名空间客户端。
func NewClientWithListAndApply[T objectWithMeta, L runtime.Object, C namedObject](
	resource string, client rest.Interface, parameterCodec runtime.ParameterCodec, namespace string, emptyObjectCreator func() T,
	emptyListCreator func() L, options ...Option[T],
) *ClientWithListAndApply[T, L, C] {
	typeClient := NewClient[T](resource, client, parameterCodec, namespace, emptyObjectCreator, options...)
	return &ClientWithListAndApply[T, L, C]{
		typeClient,
		alsoLister[T, L]{typeClient, emptyListCreator},
		alsoApplier[T, C]{typeClient},
	}
}

// GetClient 返回 REST 接口。
func (c *Client[T]) GetClient() rest.Interface {
	return c.client
}

// GetNamespace 返回客户端的命名空间（若有）。
func (c *Client[T]) GetNamespace() string {
	return c.namespace
}

// Get 按名称读取资源，返回对应对象或错误。
func (c *Client[T]) Get(ctx context.Context, name string, options metav1.GetOptions) (T, error) {
	// 每次请求都新建目标对象，避免多个并发调用共享同一份可变状态。
	result := c.newObject()
	err := c.client.Get().
		UseProtobufAsDefaultIfPreferred(c.prefersProtobuf).
		NamespaceIfScoped(c.namespace, c.namespace != "").
		Resource(c.resource).
		Name(name).
		VersionedParams(&options, c.parameterCodec).
		Do(ctx).
		Into(result)
	return result, err
}

// List 按标签与字段选择器返回匹配的资源列表。
func (l *alsoLister[T, L]) List(ctx context.Context, opts metav1.ListOptions) (L, error) {
	list := l.newList()
	// 服务端超时时间由 ListOptions 指定，同时传给客户端 Timeout，避免两侧超时策略不一致。
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	err := l.client.client.Get().
		UseProtobufAsDefaultIfPreferred(l.client.prefersProtobuf).
		NamespaceIfScoped(l.client.namespace, l.client.namespace != "").
		Resource(l.client.resource).
		VersionedParams(&opts, l.client.parameterCodec).
		Timeout(timeout).
		Do(ctx).
		Into(list)
	return list, err
}

// Watch 返回监听所请求资源的 watch.Interface。
func (c *Client[T]) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	var timeout time.Duration
	if opts.TimeoutSeconds != nil {
		timeout = time.Duration(*opts.TimeoutSeconds) * time.Second
	}
	// 强制打开 watch 标志，避免调用方漏设而得到一次普通 List。
	opts.Watch = true
	return c.client.Get().
		UseProtobufAsDefaultIfPreferred(c.prefersProtobuf).
		NamespaceIfScoped(c.namespace, c.namespace != "").
		Resource(c.resource).
		VersionedParams(&opts, c.parameterCodec).
		Timeout(timeout).
		Watch(ctx)
}

// Create 按给定对象创建资源，返回服务端表示或错误。
func (c *Client[T]) Create(ctx context.Context, obj T, opts metav1.CreateOptions) (T, error) {
	result := c.newObject()
	err := c.client.Post().
		UseProtobufAsDefaultIfPreferred(c.prefersProtobuf).
		NamespaceIfScoped(c.namespace, c.namespace != "").
		Resource(c.resource).
		VersionedParams(&opts, c.parameterCodec).
		Body(obj).
		Do(ctx).
		Into(result)
	return result, err
}

// Update 按给定对象更新资源，返回服务端表示或错误。
func (c *Client[T]) Update(ctx context.Context, obj T, opts metav1.UpdateOptions) (T, error) {
	result := c.newObject()
	err := c.client.Put().
		UseProtobufAsDefaultIfPreferred(c.prefersProtobuf).
		NamespaceIfScoped(c.namespace, c.namespace != "").
		Resource(c.resource).
		Name(obj.GetName()).
		VersionedParams(&opts, c.parameterCodec).
		Body(obj).
		Do(ctx).
		Into(result)
	return result, err
}

// UpdateStatus 更新资源的 status 子资源，返回服务端表示或错误。
func (c *Client[T]) UpdateStatus(ctx context.Context, obj T, opts metav1.UpdateOptions) (T, error) {
	result := c.newObject()
	err := c.client.Put().
		UseProtobufAsDefaultIfPreferred(c.prefersProtobuf).
		NamespaceIfScoped(c.namespace, c.namespace != "").
		Resource(c.resource).
		Name(obj.GetName()).
		// status 是标准子资源，单独更新它可以绕过普通更新的 spec 校验与并发冲突范围。
		SubResource("status").
		VersionedParams(&opts, c.parameterCodec).
		Body(obj).
		Do(ctx).
		Into(result)
	return result, err
}

// Delete 按名称删除资源，出错时返回错误。
func (c *Client[T]) Delete(ctx context.Context, name string, opts metav1.DeleteOptions) error {
	return c.client.Delete().
		UseProtobufAsDefaultIfPreferred(c.prefersProtobuf).
		NamespaceIfScoped(c.namespace, c.namespace != "").
		Resource(c.resource).
		Name(name).
		Body(&opts).
		Do(ctx).
		Error()
}

// DeleteCollection 批量删除对象。
func (l *alsoLister[T, L]) DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	// 批量删除本质上是对集合的 DELETE，但仍需用 listOpts 里的 selectors 与超时来限定范围。
	var timeout time.Duration
	if listOpts.TimeoutSeconds != nil {
		timeout = time.Duration(*listOpts.TimeoutSeconds) * time.Second
	}
	return l.client.client.Delete().
		UseProtobufAsDefaultIfPreferred(l.client.prefersProtobuf).
		NamespaceIfScoped(l.client.namespace, l.client.namespace != "").
		Resource(l.client.resource).
		VersionedParams(&listOpts, l.client.parameterCodec).
		Timeout(timeout).
		Body(&opts).
		Do(ctx).
		Error()
}

// Patch 应用补丁并返回修改后的资源。
func (c *Client[T]) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (T, error) {
	result := c.newObject()
	err := c.client.Patch(pt).
		UseProtobufAsDefaultIfPreferred(c.prefersProtobuf).
		NamespaceIfScoped(c.namespace, c.namespace != "").
		Resource(c.resource).
		Name(name).
		SubResource(subresources...).
		VersionedParams(&opts, c.parameterCodec).
		Body(data).
		Do(ctx).
		Into(result)
	return result, err
}

// Apply 应用给定的声明式配置并返回服务端表示。
func (a *alsoApplier[T, C]) Apply(ctx context.Context, obj C, opts metav1.ApplyOptions) (T, error) {
	result := a.client.newObject()
	// C 可能是值类型，用零值比较判断调用方是否传了有效对象。
	if obj == *new(C) {
		return *new(T), fmt.Errorf("object provided to Apply must not be nil")
	}
	patchOpts := opts.ToPatchOptions()
	// server-side apply 必须以对象名定位目标，缺失名字无法构造 PATCH 请求。
	if obj.GetName() == nil {
		return *new(T), fmt.Errorf("obj.Name must be provided to Apply")
	}

	// apply.NewRequest 会按对象的 apiVersion/kind 生成 PATCH 请求体。
	request, err := apply.NewRequest(a.client.client, obj)
	if err != nil {
		return *new(T), err
	}

	err = request.
		UseProtobufAsDefaultIfPreferred(a.client.prefersProtobuf).
		NamespaceIfScoped(a.client.namespace, a.client.namespace != "").
		Resource(a.client.resource).
		Name(*obj.GetName()).
		VersionedParams(&patchOpts, a.client.parameterCodec).
		Do(ctx).
		Into(result)
	return result, err
}

// ApplyStatus 把给定的声明式配置应用到 status 子资源并返回服务端表示。
func (a *alsoApplier[T, C]) ApplyStatus(ctx context.Context, obj C, opts metav1.ApplyOptions) (T, error) {
	if obj == *new(C) {
		return *new(T), fmt.Errorf("object provided to Apply must not be nil")
	}
	patchOpts := opts.ToPatchOptions()

	if obj.GetName() == nil {
		return *new(T), fmt.Errorf("obj.Name must be provided to Apply")
	}

	request, err := apply.NewRequest(a.client.client, obj)
	if err != nil {
		return *new(T), err
	}

	result := a.client.newObject()
	err = request.
		UseProtobufAsDefaultIfPreferred(a.client.prefersProtobuf).
		NamespaceIfScoped(a.client.namespace, a.client.namespace != "").
		Resource(a.client.resource).
		Name(*obj.GetName()).
		// 与 UpdateStatus 相同：apply 也只作用于 status 子资源。
		SubResource("status").
		VersionedParams(&patchOpts, a.client.parameterCodec).
		Do(ctx).
		Into(result)
	return result, err
}
