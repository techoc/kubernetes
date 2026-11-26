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

package scale

import (
	"context"
	"fmt"

	autoscaling "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	serializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	restclient "k8s.io/client-go/rest"
)

// scaleConverter 在包级别共享：scheme 构建代价高，且只读，可被所有客户端复用。
var scaleConverter = NewScaleConverter()
var codecs = serializer.NewCodecFactory(scaleConverter.Scheme())
var parameterScheme = runtime.NewScheme()
var dynamicParameterCodec = runtime.NewParameterCodec(parameterScheme)

var versionV1 = schema.GroupVersion{Version: "v1"}

func init() {
	// 查询参数（如 GetOptions）按元数据 v1 编码，与目标资源的实际版本无关。
	metav1.AddToGroupVersion(parameterScheme, versionV1)
}

// scaleClient 是 ScalesGetter 的实现，借助 RESTMapper 与通用 REST 客户端支持可发现的资源。
// 行为类似 dynamic ClientPool，但专门面向 Scale。
//
// 它本身不持有任何按资源区分的客户端，所有请求都复用同一个 REST client，
// 只是每次调用前动态计算路径和版本：mapper 把调用方给出的（可能不带版本的）资源
// 解析为首选 GVR，scaleKindResolver 决定该资源的 scale 子资源实际使用的 GVK。
type scaleClient struct {
	mapper PreferredResourceMapper

	apiPathResolverFunc dynamic.APIPathResolverFunc
	scaleKindResolver   ScaleKindResolver
	clientBase          restclient.Interface
}

// NewForConfig 创建 ScalesGetter，用给定的 RESTMapper 把 Kind 解析为资源，
// 用 dynamic.APIPathResolverFunc 解析 API 路径。
func NewForConfig(cfg *restclient.Config, mapper PreferredResourceMapper, resolver dynamic.APIPathResolverFunc, scaleKindResolver ScaleKindResolver) (ScalesGetter, error) {
	// 显式清空 GroupVersion，免得 RESTClientFor 报错：通用 REST client 不绑定单一 GV。
	cfg.GroupVersion = &schema.GroupVersion{}

	// 不做版本转换：响应体由 convertToScale 自行解码/转换。
	cfg.NegotiatedSerializer = codecs.WithoutConversion()
	if len(cfg.UserAgent) == 0 {
		cfg.UserAgent = restclient.DefaultKubernetesUserAgent()
	}

	client, err := restclient.RESTClientFor(cfg)
	if err != nil {
		return nil, err
	}

	return New(client, mapper, resolver, scaleKindResolver), nil
}

// New 用给定客户端创建 ScalesGetter，客户端上的 GroupVersion 会被忽略。
func New(baseClient restclient.Interface, mapper PreferredResourceMapper, resolver dynamic.APIPathResolverFunc, scaleKindResolver ScaleKindResolver) ScalesGetter {
	return &scaleClient{
		mapper: mapper,

		apiPathResolverFunc: resolver,
		scaleKindResolver:   scaleKindResolver,
		clientBase:          baseClient,
	}
}

// apiPathFor 返回给定 GroupVersion 的绝对 API 路径。
func (c *scaleClient) apiPathFor(groupVer schema.GroupVersion) string {
	// 需要按 GroupVersion 决定 API 路径（解析不出时回退到 legacy 路径）。
	// TODO: 这里算是"作弊"：API 路径实际只取决于 group，但这里或许更应接受
	// GroupVersionResource 而不是 GroupVersionKind。
	// 解析器只关心 group，这里用空 Kind 即可；空前缀回退到历史路径 /api。
	apiPath := c.apiPathResolverFunc(groupVer.WithKind(""))
	if apiPath == "" {
		apiPath = "/api"
	}

	return restclient.DefaultVersionedAPIPath(apiPath, groupVer)
}

// pathAndVersionFor 为给定 GroupResource 返回合适的基础路径及完整 GroupVersionResource。
func (c *scaleClient) pathAndVersionFor(resource schema.GroupResource) (string, schema.GroupVersionResource, error) {
	// 清空版本，让 mapper 返回该资源的首选版本；后续请求都基于该版本构造。
	gvr, err := c.mapper.ResourceFor(resource.WithVersion(""))
	if err != nil {
		return "", gvr, fmt.Errorf("unable to get full preferred group-version-resource for %s: %v", resource.String(), err)
	}

	groupVer := gvr.GroupVersion()

	return c.apiPathFor(groupVer), gvr, nil
}

// namespacedScaleClient 是在给定命名空间内获取 Scale 的 ScaleInterface。
type namespacedScaleClient struct {
	client    *scaleClient
	namespace string
}

// convertToScale 把响应体转换为 autoscaling/v1.Scale。
func convertToScale(result *restclient.Result) (*autoscaling.Scale, error) {
	scaleBytes, err := result.Raw()
	if err != nil {
		return nil, err
	}
	// 各版本 /scale 端点返回的类型不同，先按所有已知 scale 版本解码。
	decoder := scaleConverter.codecs.UniversalDecoder(scaleConverter.ScaleVersions()...)
	rawScaleObj, err := runtime.Decode(decoder, scaleBytes)
	if err != nil {
		return nil, err
	}

	// 把任何来源的对象统一转换成 autoscaling/v1.Scale，调用方只需处理这一种类型。
	scaleObj, err := scaleConverter.ConvertToVersion(rawScaleObj, autoscaling.SchemeGroupVersion)
	if err != nil {
		return nil, fmt.Errorf("received an object from a /scale endpoint which was not convertible to autoscaling Scale: %v", err)
	}

	return scaleObj.(*autoscaling.Scale), nil
}

func (c *scaleClient) Scales(namespace string) ScaleInterface {
	return &namespacedScaleClient{
		client:    c,
		namespace: namespace,
	}
}

func (c *namespacedScaleClient) Get(ctx context.Context, resource schema.GroupResource, name string, opts metav1.GetOptions) (*autoscaling.Scale, error) {
	// 目前 /scale 端点可能返回不同类型，在替代 API 表示提案落地前必须兼容多种版本，
	// 实际就是 autoscaling/v1.Scale 与 extensions/v1beta1.Scale。

	path, gvr, err := c.client.pathAndVersionFor(resource)
	if err != nil {
		return nil, fmt.Errorf("unable to get client for %s: %v", resource.String(), err)
	}

	// 命名空间为空时不应构造 namespaced 路径（集群级资源）。
	result := c.client.clientBase.Get().
		AbsPath(path).
		NamespaceIfScoped(c.namespace, c.namespace != "").
		Resource(gvr.Resource).
		Name(name).
		SubResource("scale").
		SpecificallyVersionedParams(&opts, dynamicParameterCodec, versionV1).
		Do(ctx)
	if err := result.Error(); err != nil {
		return nil, err
	}

	return convertToScale(&result)
}

func (c *namespacedScaleClient) Update(ctx context.Context, resource schema.GroupResource, scale *autoscaling.Scale, opts metav1.UpdateOptions) (*autoscaling.Scale, error) {
	path, gvr, err := c.client.pathAndVersionFor(resource)
	if err != nil {
		return nil, fmt.Errorf("unable to get client for %s: %v", resource.String(), err)
	}

	// 目前 /scale 端点可能接收和返回不同类型，在替代 API 表示提案落地前必须兼容多版本。

	// 弄清楚这里实际需要哪个版本的 scale：写请求必须先确定端点接受的具体 GVK，再做编码，否则 server 会拒绝。
	desiredGVK, err := c.client.scaleKindResolver.ScaleForResource(gvr)
	if err != nil {
		return nil, fmt.Errorf("could not find proper group-version for scale subresource of %s: %v", gvr.String(), err)
	}

	// 转换为该端点期望的版本。
	scaleUpdate, err := scaleConverter.ConvertToVersion(scale, desiredGVK.GroupVersion())
	if err != nil {
		return nil, fmt.Errorf("could not convert scale update to external Scale: %v", err)
	}
	encoder := scaleConverter.codecs.LegacyCodec(desiredGVK.GroupVersion())
	scaleUpdateBytes, err := runtime.Encode(encoder, scaleUpdate)
	if err != nil {
		return nil, fmt.Errorf("could not encode scale update to external Scale: %v", err)
	}

	result := c.client.clientBase.Put().
		AbsPath(path).
		NamespaceIfScoped(c.namespace, c.namespace != "").
		Resource(gvr.Resource).
		Name(scale.Name).
		SubResource("scale").
		SpecificallyVersionedParams(&opts, dynamicParameterCodec, versionV1).
		Body(scaleUpdateBytes).
		Do(ctx)
	if err := result.Error(); err != nil {
		// 透传 API 返回的"原始"错误，这样调用方才能解读底层的 Reason 字段，
		// 例如 errors.IsConflict(err)：不要包装错误，否则调用方无法判断资源版本冲突。
		return nil, err
	}

	return convertToScale(&result)
}

func (c *namespacedScaleClient) Patch(ctx context.Context, gvr schema.GroupVersionResource, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions) (*autoscaling.Scale, error) {
	// Patch 的调用方已提供完整 GVR，无需再做 mapper 解析。
	groupVersion := gvr.GroupVersion()
	result := c.client.clientBase.Patch(pt).
		AbsPath(c.client.apiPathFor(groupVersion)).
		NamespaceIfScoped(c.namespace, c.namespace != "").
		Resource(gvr.Resource).
		Name(name).
		SubResource("scale").
		SpecificallyVersionedParams(&opts, dynamicParameterCodec, versionV1).
		Body(data).
		Do(ctx)
	if err := result.Error(); err != nil {
		return nil, err
	}

	return convertToScale(&result)
}
