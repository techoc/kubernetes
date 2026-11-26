/*
Copyright 2015 The Kubernetes Authors.

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

package discovery

import (
	"context"
	"encoding/json"
	goerrors "errors"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	openapi_v2 "github.com/google/gnostic-models/openapiv2"
	"google.golang.org/protobuf/proto"

	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/openapi"
	restclient "k8s.io/client-go/rest"
)

const (
	// defaultRetries 是资源发现遇到 API 组动态消失（如 CRD 被删除）时的重试次数。
	defaultRetries = 2
	// protobuf MIME 类型。
	openAPIV2mimePb = "application/com.github.proto-openapi.spec.v2@v1.0+protobuf"

	// defaultTimeout 是 RESTClient 未设置超时时单个请求的最大耗时。
	// 默认 32s，刻意与其他超时拉开差距，便于定位超时来源。
	defaultTimeout = 32 * time.Second

	// defaultBurst 是 discovery 客户端令牌桶限速器使用的默认 burst。
	// discovery 会短时间并发查询大量 GroupVersion，因此默认 burst 高于普通客户端的 10。
	defaultBurst = 300

	AcceptV1 = runtime.ContentTypeJSON
	// 聚合发现的 content-type。注意：服务端要求 "Accept" 头中的参数按
	// (g, v, as) 排序，但比较返回的 "Content-Type" 时本实现不依赖顺序。
	AcceptV2       = runtime.ContentTypeJSON + ";" + "g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList"
	AcceptV2NoPeer = runtime.ContentTypeJSON + ";" + "g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList;profile=nopeer"
	// 把聚合发现格式排在 accept 类型列表首位，优先使用。
	acceptDiscoveryFormats = AcceptV2 + "," + AcceptV1
)

// 聚合发现 content-type 对应的 GVK。
var v2GVK = schema.GroupVersionKind{Group: "apidiscovery.k8s.io", Version: "v2", Kind: "APIGroupDiscoveryList"}

// DiscoveryInterface 提供发现服务端所支持 API 组、版本与资源的方法。
//
// DiscoveryInterfaceWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 DiscoveryInterfaceWithContext。
type DiscoveryInterface interface {
	RESTClient() restclient.Interface
	ServerGroupsInterface
	ServerResourcesInterface
	ServerVersionInterface
	OpenAPISchemaInterface
	OpenAPIV3SchemaInterface
	// WithLegacy 返回只接收旧版发现格式的当前客户端副本；若当前客户端不支持
	// 仅旧版发现，则返回其自身指针。
	//
	// DiscoveryInterfaceWithContext.WithLegacyWithContext 支持上下文日志与取消，应优先使用。
	//
	// 上下文日志：请改用 DiscoveryInterfaceWithContext.WithLegacyWithContext。
	WithLegacy() DiscoveryInterface
}

// DiscoveryInterfaceWithContext 提供发现服务端所支持 API 组、版本与资源的方法。
type DiscoveryInterfaceWithContext interface {
	RESTClient() restclient.Interface
	ServerGroupsInterfaceWithContext
	ServerResourcesInterfaceWithContext
	ServerVersionInterfaceWithContext
	OpenAPISchemaInterfaceWithContext
	OpenAPIV3SchemaInterfaceWithContext
	// WithLegacyWithContext 返回只接收旧版发现格式的当前客户端副本；若当前客户端
	// 不支持仅旧版发现，则返回其自身指针。
	WithLegacyWithContext(ctx context.Context) DiscoveryInterfaceWithContext
}

// DiscoveryInterfaces 同时组合了两种接口变体，便于在过渡期兼容。
type DiscoveryInterfaces interface {
	DiscoveryInterface
	DiscoveryInterfaceWithContext
}

func ToDiscoveryInterfaceWithContext(i DiscoveryInterface) DiscoveryInterfaceWithContext {
	if i == nil {
		return nil
	}
	if i, ok := i.(DiscoveryInterfaceWithContext); ok {
		return i
	}
	return &discoveryInterfaceWrapper{
		ServerGroupsInterfaceWithContext:    ToServerGroupsInterfaceWithContext(i),
		ServerResourcesInterfaceWithContext: ToServerResourcesInterfaceWithContext(i),
		ServerVersionInterfaceWithContext:   ToServerVersionInterfaceWithContext(i),
		OpenAPISchemaInterfaceWithContext:   ToOpenAPISchemaInterfaceWithContext(i),
		OpenAPIV3SchemaInterfaceWithContext: OpenAPIV3ToSchemaInterfaceWithContext(i),
		delegate:                            i,
	}
}

type discoveryInterfaceWrapper struct {
	ServerGroupsInterfaceWithContext
	ServerResourcesInterfaceWithContext
	ServerVersionInterfaceWithContext
	OpenAPISchemaInterfaceWithContext
	OpenAPIV3SchemaInterfaceWithContext
	delegate DiscoveryInterface
}

func (i *discoveryInterfaceWrapper) RESTClient() restclient.Interface {
	return i.delegate.RESTClient()
}

func (i *discoveryInterfaceWrapper) WithLegacyWithContext(ctx context.Context) DiscoveryInterfaceWithContext {
	return ToDiscoveryInterfaceWithContext(i.delegate.WithLegacy())
}

// AggregatedDiscoveryInterface 扩展 DiscoveryInterface，新增可能同时返回发现组与
// 资源的方法，这是新版聚合发现格式（APIGroupDiscoveryList）的能力。
//
// AggregatedDiscoveryInterfaceWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 AggregatedDiscoveryInterfaceWithContext。
type AggregatedDiscoveryInterface interface {
	DiscoveryInterface

	// AggregatedDiscoveryInterfaceWithContext.GroupsAndMaybeResourcesWithContext 支持上下文日志与取消，应优先使用。
	//
	// 上下文日志：请改用 AggregatedDiscoveryInterfaceWithContext.GroupsAndMaybeResourcesWithContext。
	GroupsAndMaybeResources() (*metav1.APIGroupList, map[schema.GroupVersion]*metav1.APIResourceList, map[schema.GroupVersion]error, error)
}

// AggregatedDiscoveryInterfaceWithContext 扩展 DiscoveryInterface，新增可能同时返回
// 发现组与资源的方法，这是新版聚合发现格式（APIGroupDiscoveryList）的能力。
type AggregatedDiscoveryInterfaceWithContext interface {
	DiscoveryInterfaceWithContext

	GroupsAndMaybeResourcesWithContext(ctx context.Context) (*metav1.APIGroupList, map[schema.GroupVersion]*metav1.APIResourceList, map[schema.GroupVersion]error, error)
}

func ToAggregatedDiscoveryInterfaceWithContext(i AggregatedDiscoveryInterface) AggregatedDiscoveryInterfaceWithContext {
	if i == nil {
		return nil
	}
	if i, ok := i.(AggregatedDiscoveryInterfaceWithContext); ok {
		return i
	}
	return &aggregatedDiscoveryInterfaceWrapper{
		DiscoveryInterfaceWithContext: ToDiscoveryInterfaceWithContext(i),
		delegate:                      i,
	}
}

type aggregatedDiscoveryInterfaceWrapper struct {
	DiscoveryInterfaceWithContext
	delegate AggregatedDiscoveryInterface
}

func (i *aggregatedDiscoveryInterfaceWrapper) GroupsAndMaybeResourcesWithContext(ctx context.Context) (*metav1.APIGroupList, map[schema.GroupVersion]*metav1.APIResourceList, map[schema.GroupVersion]error, error) {
	return i.delegate.GroupsAndMaybeResources()
}

// CachedDiscoveryInterface 在 DiscoveryInterface 之上增加缓存失效与新鲜度判断。
// 注意：若 ServerResourcesForGroupVersion 返回缓存未命中错误，调用方必须显式调用
// Invalidate 清除缓存，否则下次访问仍会得到同样的错误。
//
// CachedDiscoveryInterfaceWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 CachedDiscoveryInterfaceWithContext。
type CachedDiscoveryInterface interface {
	DiscoveryInterface
	// Fresh 告诉调用方：缓存未命中时是否应重试（false = 重试，true = 无需重试）。
	//
	// TODO: 这部分设计需要重新审视：该接口无法正确加锁，语义也不清晰。
	//
	// CachedDiscoveryInterfaceWithContext.FreshWithContext 支持上下文日志与取消，应优先使用。
	//
	// 上下文日志：请改用 CachedDiscoveryInterfaceWithContext.FreshWithContext。
	Fresh() bool
	// Invalidate 保证后续不再使用当前时间之前缓存的数据。
	//
	// CachedDiscoveryInterfaceWithContext.InvalidateWithContext 支持上下文日志与取消，应优先使用。
	//
	// 上下文日志：请改用 CachedDiscoveryInterfaceWithContext.InvalidateWithContext。
	Invalidate()
}

// CachedDiscoveryInterfaceWithContext 在 DiscoveryInterfaceWithContext 之上增加缓存失效
// 与新鲜度判断。注意：若 ServerResourcesForGroupVersion 返回缓存未命中错误，调用方
// 必须显式调用 Invalidate 清除缓存，否则下次访问仍会得到同样的错误。
type CachedDiscoveryInterfaceWithContext interface {
	DiscoveryInterfaceWithContext
	// Fresh 告诉调用方：缓存未命中时是否应重试（false = 重试，true = 无需重试）。
	//
	// TODO: 这部分设计需要重新审视：该接口无法正确加锁，语义也不清晰。
	FreshWithContext(ctx context.Context) bool
	// Invalidate 保证后续不再使用当前时间之前缓存的数据。
	InvalidateWithContext(ctx context.Context)
}

func ToCachedDiscoveryInterfaceWithContext(i CachedDiscoveryInterface) CachedDiscoveryInterfaceWithContext {
	if i == nil {
		return nil
	}
	if i, ok := i.(CachedDiscoveryInterfaceWithContext); ok {
		return i
	}
	return &cachedDiscoveryInterfaceWrapper{
		DiscoveryInterfaceWithContext: ToDiscoveryInterfaceWithContext(i),
		delegate:                      i,
	}
}

type cachedDiscoveryInterfaceWrapper struct {
	DiscoveryInterfaceWithContext
	delegate CachedDiscoveryInterface
}

func (i *cachedDiscoveryInterfaceWrapper) FreshWithContext(ctx context.Context) bool {
	return i.delegate.Fresh()
}

func (i *cachedDiscoveryInterfaceWrapper) InvalidateWithContext(ctx context.Context) {
	i.delegate.Invalidate()
}

// ServerGroupsInterface 提供获取 API Server 支持哪些组的方法。
//
// ServerGroupsInterfaceWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerGroupsInterfaceWithContext。
type ServerGroupsInterface interface {
	// ServerGroups 返回支持的组，以及版本、preferred version 等信息。
	//
	// ServerGroupsInterfaceWithContext.ServerGroups 支持上下文日志与取消，应优先使用。
	//
	// 上下文日志：请改用 ServerGroupsInterfaceWithContext.ServerGroups。
	ServerGroups() (*metav1.APIGroupList, error)
}

// ServerGroupsInterfaceWithContext 提供获取 API Server 支持哪些组的方法。
type ServerGroupsInterfaceWithContext interface {
	// ServerGroups 返回支持的组，以及版本、preferred version 等信息。
	ServerGroupsWithContext(ctx context.Context) (*metav1.APIGroupList, error)
}

func ToServerGroupsInterfaceWithContext(i ServerGroupsInterface) ServerGroupsInterfaceWithContext {
	if i == nil {
		return nil
	}
	if i, ok := i.(ServerGroupsInterfaceWithContext); ok {
		return i
	}
	return &serverGroupsInterfaceWrapper{
		delegate: i,
	}
}

type serverGroupsInterfaceWrapper struct {
	delegate ServerGroupsInterface
}

func (i *serverGroupsInterfaceWrapper) ServerGroupsWithContext(ctx context.Context) (*metav1.APIGroupList, error) {
	return i.delegate.ServerGroups()
}

// ServerResourcesInterface 提供获取 API Server 支持哪些资源的方法。
//
// ServerGroupsAndResourcesWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerGroupsAndResourcesWithContext。
type ServerResourcesInterface interface {
	// ServerResourcesForGroupVersion 返回指定 group/version 支持的资源。
	//
	// ServerGroupsAndResourcesWithContext.ServerResourcesForGroupVersionWithContext 支持上下文日志与取消，应优先使用。
	//
	// 上下文日志：请改用 ServerGroupsAndResourcesWithContext.ServerResourcesForGroupVersionWithContext。
	ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error)
	// ServerGroupsAndResources 返回所有 group/version 的组与资源。
	//
	// 即使返回了非 nil 错误，组和资源的列表也可能包含部分成功的结果。
	//
	// ServerGroupsAndResourcesWithContext.ServerGroupsAndResourcesWithContext 支持上下文日志与取消，应优先使用。
	//
	// 上下文日志：请改用 ServerGroupsAndResourcesWithContext.ServerGroupsAndResourcesWithContext。
	ServerGroupsAndResources() ([]*metav1.APIGroup, []*metav1.APIResourceList, error)
	// ServerPreferredResources 返回服务端 preferred version 所支持的资源。
	//
	// 即使返回了非 nil 错误，组和资源的列表也可能包含部分成功的结果。
	//
	// ServerResourcesInterfaceWithContext.ServerPreferredResourcesWithContext 支持上下文日志与取消，应优先使用。
	//
	// 上下文日志：请改用 ServerResourcesInterfaceWithContext.ServerPreferredResourcesWithContext。
	ServerPreferredResources() ([]*metav1.APIResourceList, error)
	// ServerPreferredNamespacedResources 返回服务端 preferred version 所支持的命名空间级资源。
	//
	// 即使返回了非 nil 错误，资源列表也可能包含部分成功的结果。
	//
	// ServerResourcesInterfaceWithContext.ServerPreferredNamespacedResourcesWithContext 支持上下文日志与取消，应优先使用。
	//
	// 上下文日志：请改用 ServerResourcesInterfaceWithContext.ServerPreferredNamespacedResourcesWithContext。
	ServerPreferredNamespacedResources() ([]*metav1.APIResourceList, error)
}

// ServerResourcesInterfaceWithContext 提供获取 API Server 支持哪些资源的方法。
type ServerResourcesInterfaceWithContext interface {
	// ServerResourcesForGroupVersionWithContext 返回指定 group/version 支持的资源。
	ServerResourcesForGroupVersionWithContext(ctx context.Context, groupVersion string) (*metav1.APIResourceList, error)
	// ServerGroupsAndResourcesWithContext 返回所有 group/version 的组与资源。
	//
	// 即使返回了非 nil 错误，组和资源的列表也可能包含部分成功的结果。
	ServerGroupsAndResourcesWithContext(ctx context.Context) ([]*metav1.APIGroup, []*metav1.APIResourceList, error)
	// ServerPreferredResourcesWithContext 返回服务端 preferred version 所支持的资源。
	//
	// 即使返回了非 nil 错误，组和资源的列表也可能包含部分成功的结果。
	ServerPreferredResourcesWithContext(ctx context.Context) ([]*metav1.APIResourceList, error)
	// ServerPreferredNamespacedResourcesWithContext 返回服务端 preferred version 所支持的命名空间级资源。
	//
	// 即使返回了非 nil 错误，资源列表也可能包含部分成功的结果。
	ServerPreferredNamespacedResourcesWithContext(ctx context.Context) ([]*metav1.APIResourceList, error)
}

func ToServerResourcesInterfaceWithContext(i ServerResourcesInterface) ServerResourcesInterfaceWithContext {
	if i == nil {
		return nil
	}
	if i, ok := i.(ServerResourcesInterfaceWithContext); ok {
		return i
	}
	return &serverResourcesInterfaceWrapper{
		delegate: i,
	}
}

type serverResourcesInterfaceWrapper struct {
	delegate ServerResourcesInterface
}

func (i *serverResourcesInterfaceWrapper) ServerResourcesForGroupVersionWithContext(ctx context.Context, groupVersion string) (*metav1.APIResourceList, error) {
	return i.delegate.ServerResourcesForGroupVersion(groupVersion)
}

func (i *serverResourcesInterfaceWrapper) ServerGroupsAndResourcesWithContext(ctx context.Context) ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	return i.delegate.ServerGroupsAndResources()
}

func (i *serverResourcesInterfaceWrapper) ServerPreferredResourcesWithContext(ctx context.Context) ([]*metav1.APIResourceList, error) {
	return i.delegate.ServerPreferredResources()
}

func (i *serverResourcesInterfaceWrapper) ServerPreferredNamespacedResourcesWithContext(ctx context.Context) ([]*metav1.APIResourceList, error) {
	return i.delegate.ServerPreferredNamespacedResources()
}

// ServerVersionInterface 提供获取服务端版本的方法。
//
// ServerVersionInterfaceWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerVersionInterfaceWithContext。
type ServerVersionInterface interface {
	// ServerVersion 获取并解析服务端版本（git version）。
	//
	// ServerVersionInterfaceWithContext.ServerVersionWithContext 支持上下文日志与取消，应优先使用。
	//
	// 上下文日志：请改用 ServerVersionInterfaceWithContext.ServerVersionWithContext。
	ServerVersion() (*version.Info, error)
}

// ServerVersionInterface 提供获取服务端版本的方法。
type ServerVersionInterfaceWithContext interface {
	// ServerVersion 获取并解析服务端版本（git version）。
	ServerVersionWithContext(ctx context.Context) (*version.Info, error)
}

func ToServerVersionInterfaceWithContext(i ServerVersionInterface) ServerVersionInterfaceWithContext {
	if i == nil {
		return nil
	}
	if i, ok := i.(ServerVersionInterfaceWithContext); ok {
		return i
	}
	return &serverVersionInterfaceWrapper{
		delegate: i,
	}
}

type serverVersionInterfaceWrapper struct {
	delegate ServerVersionInterface
}

func (i *serverVersionInterfaceWrapper) ServerVersionWithContext(ctx context.Context) (*version.Info, error) {
	return i.delegate.ServerVersion()
}

// OpenAPISchemaInterface 提供获取 OpenAPI schema 的方法。
//
// OpenAPISchemaInterfaceWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 OpenAPISchemaInterfaceWithContext。
type OpenAPISchemaInterface interface {
	// OpenAPISchema 获取并解析服务端支持的 swagger API schema。
	//
	// OpenAPISchemaInterfaceWithContext.OpenAPISchemaWithContext 支持上下文日志与取消，应优先使用。
	//
	// 上下文日志：请改用 OpenAPISchemaInterfaceWithContext.OpenAPISchemaWithContext。
	OpenAPISchema() (*openapi_v2.Document, error)
}

// OpenAPISchemaInterfaceWithContext 提供获取 OpenAPI schema 的方法。
type OpenAPISchemaInterfaceWithContext interface {
	// OpenAPISchemaWithContext 获取并解析服务端支持的 swagger API schema。
	OpenAPISchemaWithContext(ctx context.Context) (*openapi_v2.Document, error)
}

func ToOpenAPISchemaInterfaceWithContext(i OpenAPISchemaInterface) OpenAPISchemaInterfaceWithContext {
	if i == nil {
		return nil
	}
	if i, ok := i.(OpenAPISchemaInterfaceWithContext); ok {
		return i
	}
	return &openAPISchemaInterfaceWrapper{
		delegate: i,
	}
}

type openAPISchemaInterfaceWrapper struct {
	delegate OpenAPISchemaInterface
}

func (i *openAPISchemaInterfaceWrapper) OpenAPISchemaWithContext(ctx context.Context) (*openapi_v2.Document, error) {
	return i.delegate.OpenAPISchema()
}

// OpenAPIV3SchemaInterfaceWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 OpenAPIV3SchemaInterfaceWithContext。
type OpenAPIV3SchemaInterface interface {
	// OpenAPIV3SchemaInterfaceWithContext.OpenAPIV3WithContext 支持上下文日志与取消，应优先使用。
	//
	// 上下文日志：请改用 OpenAPIV3SchemaInterfaceWithContext.OpenAPIV3WithContext。
	OpenAPIV3() openapi.Client
}

type OpenAPIV3SchemaInterfaceWithContext interface {
	OpenAPIV3WithContext(ctx context.Context) openapi.ClientWithContext
}

func OpenAPIV3ToSchemaInterfaceWithContext(i OpenAPIV3SchemaInterface) OpenAPIV3SchemaInterfaceWithContext {
	if i == nil {
		return nil
	}
	if i, ok := i.(OpenAPIV3SchemaInterfaceWithContext); ok {
		return i
	}
	return &openAPIV3SchemaInterfaceWrapper{
		delegate: i,
	}
}

type openAPIV3SchemaInterfaceWrapper struct {
	delegate OpenAPIV3SchemaInterface
}

func (i *openAPIV3SchemaInterfaceWrapper) OpenAPIV3WithContext(ctx context.Context) openapi.ClientWithContext {
	return ToOpenAPIClientWithContext(i.delegate.OpenAPIV3())
}

func ToOpenAPIClientWithContext(i openapi.Client) openapi.ClientWithContext { //nolint:staticcheck // Intentionally using the old interface here.
	if i == nil {
		return nil
	}
	if i, ok := i.(openapi.ClientWithContext); ok {
		return i
	}
	return &openAPIClientWrapper{
		delegate: i,
	}
}

type openAPIClientWrapper struct {
	//nolint:staticcheck // Intentionally using the old interface here.
	delegate openapi.Client
}

func (i *openAPIClientWrapper) PathsWithContext(ctx context.Context) (map[string]openapi.GroupVersionWithContext, error) {
	resultWithoutContext, err := i.delegate.Paths()
	result := make(map[string]openapi.GroupVersionWithContext, len(resultWithoutContext))
	for key, entry := range resultWithoutContext {
		result[key] = openapi.ToGroupVersionWithContext(entry)
	}
	return result, err
}

// DiscoveryClient 实现发现服务端所支持的 API 组、版本和资源的各类函数。
//
// DiscoveryClient 通过 /api、/apis、/version、/openapi 等端点了解服务端能力。
// 它本身无状态且可并发使用；需要缓存时请使用 cached discovery 包装。
type DiscoveryClient struct {
	restClient restclient.Interface

	LegacyPrefix string
	// 强制客户端只请求非聚合（旧版）发现格式。
	UseLegacyDiscovery bool
	// NoPeerDiscovery 请求聚合发现的 "nopeer" profile：只获取正在通信的那一个
	// apiserver 提供的发现文档，不含聚合层从 peer 收集的内容。适合需要判断单个
	// apiserver 状态（例如确认它已就绪可接收流量）的客户端。
	NoPeerDiscovery bool
}

var _ AggregatedDiscoveryInterface = &DiscoveryClient{}

// 把 metav1.APIVersions 转换为 metav1.APIGroup；由于 legacy v1 使用 APIVersions，
// 转换出的 group 为空字符串。
func apiVersionsToAPIGroup(apiVersions *metav1.APIVersions) (apiGroup metav1.APIGroup) {
	groupVersions := []metav1.GroupVersionForDiscovery{}
	for _, version := range apiVersions.Versions {
		groupVersion := metav1.GroupVersionForDiscovery{
			GroupVersion: version,
			Version:      version,
		}
		groupVersions = append(groupVersions, groupVersion)
	}
	apiGroup.Versions = groupVersions
	// /api 端点只会返回一个 groupVersion。
	apiGroup.PreferredVersion = groupVersions[0]
	return
}

// GroupsAndMaybeResources 返回发现组，以及（新版聚合发现格式下）按 group/version
// 索引的资源。它合并 /api 与 /apis 两个端点的发现组与资源（无论是否聚合）。
// legacy 组必须排在前面。服务端对两个端点要么都返回聚合格式，要么都返回旧格式。
// 为安全起见，只有两个端点都返回了资源时才会返回 resources。返回的 failedGVs
// 可以为空，但只有在返回错误时才为 nil。
//
// GroupsAndMaybeResourcesWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 GroupsAndMaybeResourcesWithContext。
func (d *DiscoveryClient) GroupsAndMaybeResources() (
	*metav1.APIGroupList,
	map[schema.GroupVersion]*metav1.APIResourceList,
	map[schema.GroupVersion]error,
	error) {
	return d.GroupsAndMaybeResourcesWithContext(context.Background())
}

// GroupsAndMaybeResources 返回发现组，以及（新版聚合发现格式下）按 group/version
// 索引的资源。它合并 /api 与 /apis 两个端点的发现组与资源（无论是否聚合）。
// legacy 组必须排在前面。服务端对两个端点要么都返回聚合格式，要么都返回旧格式。
// 为安全起见，只有两个端点都返回了资源时才会返回 resources。返回的 failedGVs
// 可以为空，但只有在返回错误时才为 nil。
func (d *DiscoveryClient) GroupsAndMaybeResourcesWithContext(ctx context.Context) (
	*metav1.APIGroupList,
	map[schema.GroupVersion]*metav1.APIResourceList,
	map[schema.GroupVersion]error,
	error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}

	// 传统组（legacy group）排在最前（只有一个 -- core/v1 组）。返回的 groups 必须非 nil，但可以为空，
	// 返回的 resources、apiResources map 则可能为 nil。
	// 先查 /api（核心组），保证核心组始终排在返回列表最前。
	groups, resources, failedGVs, err := d.downloadLegacy(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	// 从 /apis 下载的发现组与（可能的）资源。
	apiGroups, apiResources, failedApisGVs, aerr := d.downloadAPIs(ctx)
	if aerr != nil {
		return nil, nil, nil, aerr
	}
	// 把 /apis 的组合并到 legacy 组中。
	for _, group := range apiGroups.Groups {
		groups.Groups = append(groups.Groups, group)
	}
	// 出于安全考虑，只有两个端点都返回了资源时才返回资源：
	// 任一端点退化为非聚合格式时无法保证资源完整，宁可不返回资源也不返回不完整集合。
	if resources != nil && apiResources != nil {
		for gv, resourceList := range apiResources {
			resources[gv] = resourceList
		}
	} else if resources != nil {
		resources = nil
	}
	// 合并 /api 与 /apis 返回的失败 GroupVersion。
	for gv, err := range failedApisGVs {
		failedGVs[gv] = err
	}
	return groups, resources, failedGVs, err
}

// downloadLegacy 下载 legacy v1 端点 /api 的发现组与（可能的）资源，失败时返回
// 错误。若服务端返回的是非聚合发现，资源 map 可能为 nil。返回的 failedGVs 可以
// 为空，但只有在返回错误时才为 nil。
func (d *DiscoveryClient) downloadLegacy(ctx context.Context) (
	*metav1.APIGroupList,
	map[schema.GroupVersion]*metav1.APIResourceList,
	map[schema.GroupVersion]error,
	error) {
	accept := selectDiscoveryAcceptHeader(d.UseLegacyDiscovery, d.NoPeerDiscovery)
	var responseContentType string
	body, err := d.restClient.Get().
		AbsPath("/api").
		SetHeader("Accept", accept).
		Do(ctx).
		ContentType(&responseContentType).
		Raw()
	apiGroupList := &metav1.APIGroupList{}
	failedGVs := map[schema.GroupVersion]error{}
	if err != nil {
		// 容忍 404：某些聚合 apiserver 没有核心组，返回 404 属于正常情况，应视为空结果。
		if errors.IsNotFound(err) {
			// 返回空结构与 nil 错误，让上层继续处理其余结果。
			emptyGVMap := map[schema.GroupVersion]*metav1.APIResourceList{}
			return apiGroupList, emptyGVMap, failedGVs, nil
		} else {
			return nil, nil, nil, err
		}
	}

	var resourcesByGV map[schema.GroupVersion]*metav1.APIResourceList
	// 以响应 Content-Type（而非请求 Accept）判断服务端实际返回的是聚合还是非聚合格式。
	if isGVK, _ := ContentTypeIsGVK(responseContentType, v2GVK); isGVK {
		var aggregatedDiscovery apidiscoveryv2.APIGroupDiscoveryList
		err = json.Unmarshal(body, &aggregatedDiscovery)
		if err != nil {
			return nil, nil, nil, err
		}
		apiGroupList, resourcesByGV, failedGVs = SplitGroupsAndResources(aggregatedDiscovery)
	} else {
		// 默认使用非聚合发现 v1。
		var v metav1.APIVersions
		err = json.Unmarshal(body, &v)
		if err != nil {
			return nil, nil, nil, err
		}
		apiGroup := metav1.APIGroup{}
		if len(v.Versions) != 0 {
			apiGroup = apiVersionsToAPIGroup(&v)
		}
		apiGroupList.Groups = []metav1.APIGroup{apiGroup}
	}

	return apiGroupList, resourcesByGV, failedGVs, nil
}

// downloadAPIs 返回发现组与（聚合格式下）发现资源。groups 总会有值，但 resources
// map 可能为 nil。返回的 failedGVs 可以为空，但只有在返回错误时才为 nil。
func (d *DiscoveryClient) downloadAPIs(ctx context.Context) (
	*metav1.APIGroupList,
	map[schema.GroupVersion]*metav1.APIResourceList,
	map[schema.GroupVersion]error,
	error) {
	accept := selectDiscoveryAcceptHeader(d.UseLegacyDiscovery, d.NoPeerDiscovery)
	var responseContentType string
	body, err := d.restClient.Get().
		AbsPath("/apis").
		SetHeader("Accept", accept).
		Do(ctx).
		ContentType(&responseContentType).
		Raw()
	if err != nil {
		return nil, nil, nil, err
	}

	apiGroupList := &metav1.APIGroupList{}
	failedGVs := map[schema.GroupVersion]error{}
	var resourcesByGV map[schema.GroupVersion]*metav1.APIResourceList
	// 根据服务端响应的 content-type 判断：聚合还是非聚合。
	if isGVK, _ := ContentTypeIsGVK(responseContentType, v2GVK); isGVK {
		var aggregatedDiscovery apidiscoveryv2.APIGroupDiscoveryList
		err = json.Unmarshal(body, &aggregatedDiscovery)
		if err != nil {
			return nil, nil, nil, err
		}
		apiGroupList, resourcesByGV, failedGVs = SplitGroupsAndResources(aggregatedDiscovery)
	} else {
		// 默认使用非聚合发现 v1。
		err = json.Unmarshal(body, apiGroupList)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	return apiGroupList, resourcesByGV, failedGVs, nil
}

// selectDiscoveryAcceptHeader 决定 Accept 头：显式 legacy 优先；nopeer 模式把
// 单 apiserver 的聚合视图排在第一位，同时仍接受其他格式作为回退。
func selectDiscoveryAcceptHeader(useLegacy, nopeer bool) string {
	if useLegacy {
		return AcceptV1
	}
	if nopeer {
		return AcceptV2NoPeer + "," + acceptDiscoveryFormats
	}
	return acceptDiscoveryFormats
}

// ContentTypeIsGVK 检查 content-type 是否为 "application/json" 且匹配给定 GVK；
// content-type 字符串格式错误时返回错误。
// 注意：该函数对 content-type 参数的顺序不敏感，也能容忍代理或网关附加的参数。示例：
//
//	("application/json; g=apidiscovery.k8s.io;v=v2;as=APIGroupDiscoveryList", {apidiscovery.k8s.io, v2, APIGroupDiscoveryList}) = (true, nil)
//	("application/json; as=APIGroupDiscoveryList;v=v2;g=apidiscovery.k8s.io", {apidiscovery.k8s.io, v2, APIGroupDiscoveryList}) = (true, nil)
//	("application/json; as=APIGroupDiscoveryList;v=v2;g=apidiscovery.k8s.io;charset=utf-8", {apidiscovery.k8s.io, v2, APIGroupDiscoveryList}) = (true, nil)
//	("application/json", any GVK) = (false, nil)
//	("application/json; charset=UTF-8", any GVK) = (false, nil)
//	("malformed content type string", any GVK) = (false, error)
func ContentTypeIsGVK(contentType string, gvk schema.GroupVersionKind) (bool, error) {
	base, params, err := mime.ParseMediaType(contentType)
	if err != nil {
		return false, err
	}
	gvkMatch := runtime.ContentTypeJSON == base &&
		params["g"] == gvk.Group &&
		params["v"] == gvk.Version &&
		params["as"] == gvk.Kind
	return gvkMatch, nil
}

// ServerGroups 返回支持的组，以及版本、preferred version 等信息。
//
// ServerGroupsWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerGroupsWithContext。
func (d *DiscoveryClient) ServerGroups() (*metav1.APIGroupList, error) {
	return d.ServerGroupsWithContext(context.Background())
}

// ServerGroupsWithContext 返回支持的组，以及版本、preferred version 等信息。
func (d *DiscoveryClient) ServerGroupsWithContext(ctx context.Context) (*metav1.APIGroupList, error) {
	groups, _, _, err := d.GroupsAndMaybeResourcesWithContext(ctx)
	if err != nil {
		return nil, err
	}
	return groups, nil
}

// ServerResourcesForGroupVersion 返回指定 group/version 支持的资源。
//
// ServerResourcesForGroupVersionWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerResourcesForGroupVersionWithContext。
func (d *DiscoveryClient) ServerResourcesForGroupVersion(groupVersion string) (resources *metav1.APIResourceList, err error) {
	return d.ServerResourcesForGroupVersionWithContext(context.Background(), groupVersion)
}

// ServerResourcesForGroupVersionWithContext 返回指定 group/version 支持的资源。
func (d *DiscoveryClient) ServerResourcesForGroupVersionWithContext(ctx context.Context, groupVersion string) (resources *metav1.APIResourceList, err error) {
	url := url.URL{}
	if len(groupVersion) == 0 {
		return nil, fmt.Errorf("groupVersion shouldn't be empty")
	}
	// 核心组 "v1" 使用 LegacyPrefix（通常是 /api），其余组使用 /apis。
	if len(d.LegacyPrefix) > 0 && groupVersion == "v1" {
		url.Path = d.LegacyPrefix + "/" + groupVersion
	} else {
		url.Path = "/apis/" + groupVersion
	}
	resources = &metav1.APIResourceList{
		GroupVersion: groupVersion,
	}
	err = d.restClient.Get().AbsPath(url.String()).Do(ctx).Into(resources)
	if err != nil {
		// core/v1 未找到时返回空资源列表以作容忍；这种情况按理不该发生。
		// 保留该容忍是为了兼容旧调用方；移除前需确认无人依赖“v1 始终返回非 nil”。
		if groupVersion == "v1" && errors.IsNotFound(err) {
			return resources, nil
		}
		return nil, err
	}
	return resources, nil
}

// ServerGroupsAndResources 返回所有 group/version 支持的资源。
func (d *DiscoveryClient) ServerGroupsAndResources() ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	return d.ServerGroupsAndResourcesWithContext(context.Background())
}

// ServerGroupsAndResourcesWithContext 返回所有 group/version 支持的资源。
func (d *DiscoveryClient) ServerGroupsAndResourcesWithContext(ctx context.Context) ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	return withRetries(ctx, defaultRetries, func() ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
		return ServerGroupsAndResourcesWithContext(ctx, d)
	})
}

// 一个或多个 API 组加载失败时返回 ErrGroupDiscoveryFailed。
//
// 该错误携带部分成功的结果：调用方仍可使用其余 GroupVersion，
// 同时通过 IsGroupDiscoveryFailedError/GroupDiscoveryFailedErrorGroups 判断是否需要重试。
type ErrGroupDiscoveryFailed struct {
	// Groups 记录加载失败的组及其错误原因。
	Groups map[schema.GroupVersion]error
}

// Error 实现 error 接口。
func (e *ErrGroupDiscoveryFailed) Error() string {
	var groups []string
	for k, v := range e.Groups {
		groups = append(groups, fmt.Sprintf("%s: %v", k, v))
	}
	sort.Strings(groups)
	return fmt.Sprintf("unable to retrieve the complete list of server APIs: %s", strings.Join(groups, ", "))
}

// Is 让调用方可以用 errors.Is 判断被 ErrGroupDiscoveryFailed 包装的错误。
func (e *ErrGroupDiscoveryFailed) Is(target error) bool {
	_, ok := target.(*ErrGroupDiscoveryFailed)
	return ok
}

// IsGroupDiscoveryFailedError 判断给定错误是否表明服务端未能返回完整的 API 列表供客户端使用。
func IsGroupDiscoveryFailedError(err error) bool {
	_, ok := err.(*ErrGroupDiscoveryFailed)
	return err != nil && ok
}

// GroupDiscoveryFailedErrorGroups 在错误是 ErrGroupDiscoveryFailed 时返回 true，
// 并给出发现失败的 group/version 映射。
func GroupDiscoveryFailedErrorGroups(err error) (map[schema.GroupVersion]error, bool) {
	var groupDiscoveryError *ErrGroupDiscoveryFailed
	if err != nil && goerrors.As(err, &groupDiscoveryError) {
		return groupDiscoveryError.Groups, true
	}
	return nil, false
}

// ServerGroupsAndResourcesWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerGroupsAndResourcesWithContext。
func ServerGroupsAndResources(d DiscoveryInterface) ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	return ServerGroupsAndResourcesWithContext(context.Background(), ToDiscoveryInterfaceWithContext(d))
}

func ServerGroupsAndResourcesWithContext(ctx context.Context, d DiscoveryInterfaceWithContext) ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}

	var sgs *metav1.APIGroupList
	var resources []*metav1.APIResourceList
	var failedGVs map[schema.GroupVersion]error
	var err error

	// 若传入的 discovery 对象实现了更完整的 AggregatedDiscoveryInterface，则优先走聚合发现：
	// 一次请求即可同时拿到组和资源，减少 API 调用数。
	ad, ok := d.(AggregatedDiscoveryInterfaceWithContext)
	if !ok {
		if ad2, ok2 := d.(AggregatedDiscoveryInterface); ok2 {
			ad = ToAggregatedDiscoveryInterfaceWithContext(ad2)
			ok = true
		}
	}
	if ok {
		var resourcesByGV map[schema.GroupVersion]*metav1.APIResourceList
		sgs, resourcesByGV, failedGVs, err = ad.GroupsAndMaybeResourcesWithContext(ctx)
		for _, resourceList := range resourcesByGV {
			resources = append(resources, resourceList)
		}
	} else {
		sgs, err = d.ServerGroupsWithContext(ctx)
	}

	if sgs == nil {
		return nil, nil, err
	}
	resultGroups := []*metav1.APIGroup{}
	for i := range sgs.Groups {
		resultGroups = append(resultGroups, &sgs.Groups[i])
	}
	// 聚合发现成功时 resources 非 nil。
	if resources != nil {
		// 聚合发现返回的任何 stale GroupVersion 都必须作为失败的 GV 上报给调用方。
		var ferr error
		if len(failedGVs) > 0 {
			ferr = &ErrGroupDiscoveryFailed{Groups: failedGVs}
		}
		return resultGroups, resources, ferr
	}

	// 非聚合路径：并行拉取每个 GroupVersion 的资源，再按发现顺序重排。
	groupVersionResources, failedGroups := fetchGroupVersionResources(ctx, d, sgs)

	// 按 group/version 的发现顺序排列结果：map 遍历无序，必须按 serverGroupList 的顺序重建结果以保证稳定性。
	result := []*metav1.APIResourceList{}
	for _, apiGroup := range sgs.Groups {
		for _, version := range apiGroup.Versions {
			gv := schema.GroupVersion{Group: apiGroup.Name, Version: version.Version}
			if resources, ok := groupVersionResources[gv]; ok {
				result = append(result, resources)
			}
		}
	}

	if len(failedGroups) == 0 {
		return resultGroups, result, nil
	}

	return resultGroups, result, &ErrGroupDiscoveryFailed{Groups: failedGroups}
}

// ServerPreferredResources 使用给定的 discovery 接口查找 preferred 资源。
//
// ServerPreferredResourcesWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerPreferredResourcesWithContext。
func ServerPreferredResources(d DiscoveryInterface) ([]*metav1.APIResourceList, error) {
	return ServerPreferredResourcesWithContext(context.Background(), ToDiscoveryInterfaceWithContext(d))
}

// ServerPreferredResourcesWithContext 使用给定的 discovery 接口查找 preferred 资源。
func ServerPreferredResourcesWithContext(ctx context.Context, d DiscoveryInterfaceWithContext) ([]*metav1.APIResourceList, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var serverGroupList *metav1.APIGroupList
	var failedGroups map[schema.GroupVersion]error
	var groupVersionResources map[schema.GroupVersion]*metav1.APIResourceList
	var err error

	// 若传入的 discovery 对象实现了更宽泛的 AggregatedDiscoveryInterface，则尝试同时
	// 获取组与资源。"failedGroups" 是聚合发现格式中返回为 stale 的 GroupVersion。
	ad, ok := d.(AggregatedDiscoveryInterfaceWithContext)
	if !ok {
		if ad2, ok2 := d.(AggregatedDiscoveryInterface); ok2 {
			ad = ToAggregatedDiscoveryInterfaceWithContext(ad2)
			ok = true
		}
	}
	if ok {
		serverGroupList, groupVersionResources, failedGroups, err = ad.GroupsAndMaybeResourcesWithContext(ctx)
	} else {
		serverGroupList, err = d.ServerGroupsWithContext(ctx)
	}
	if err != nil {
		return nil, err
	}
	// 非聚合发现必须从 Groups 中拉取资源。
	if groupVersionResources == nil {
		groupVersionResources, failedGroups = fetchGroupVersionResources(ctx, d, serverGroupList)
	}

	result := []*metav1.APIResourceList{}
	grVersions := map[schema.GroupResource]string{}                         // 记录 GroupResource 选用的版本。
	grAPIResources := map[schema.GroupResource]*metav1.APIResource{}        // 记录 GroupResource 选用的 APIResource。
	gvAPIResourceLists := map[schema.GroupVersion]*metav1.APIResourceList{} // 供后续分组使用的 APIResourceList 模板。

	// 第一遍：为每个 GroupVersion 挑选“首选版本”的资源，并记录资源到版本的映射。
	for _, apiGroup := range serverGroupList.Groups {
		for _, version := range apiGroup.Versions {
			groupVersion := schema.GroupVersion{Group: apiGroup.Name, Version: version.Version}

			apiResourceList, ok := groupVersionResources[groupVersion]
			if !ok {
				continue
			}

			// 先创建空列表，后续循环再填充。
			emptyAPIResourceList := metav1.APIResourceList{
				GroupVersion: version.GroupVersion,
			}
			gvAPIResourceLists[groupVersion] = &emptyAPIResourceList
			result = append(result, &emptyAPIResourceList)

			for i := range apiResourceList.APIResources {
				apiResource := &apiResourceList.APIResources[i]
				// 子资源（含 "/"）不单独出现在首选资源列表中。
				if strings.Contains(apiResource.Name, "/") {
					continue
				}
				gv := schema.GroupResource{Group: apiGroup.Name, Resource: apiResource.Name}
				if _, ok := grAPIResources[gv]; ok && version.Version != apiGroup.PreferredVersion.Version {
					// 只在存在首选版本时用它覆盖：同一资源出现在多个版本时，优先保留 PreferredVersion 的定义。
					continue
				}
				grVersions[gv] = version.Version
				grAPIResources[gv] = apiResource
			}
		}
	}

	// 第二遍：按 GroupVersion 把选中的 APIResources 归并到各自的 APIResourceList。
	for groupResource, apiResource := range grAPIResources {
		version := grVersions[groupResource]
		groupVersion := schema.GroupVersion{Group: groupResource.Group, Version: version}
		apiResourceList := gvAPIResourceLists[groupVersion]
		apiResourceList.APIResources = append(apiResourceList.APIResources, *apiResource)
	}

	if len(failedGroups) == 0 {
		return result, nil
	}

	return result, &ErrGroupDiscoveryFailed{Groups: failedGroups}
}

// fetchServerResourcesForGroupVersions 使用 discovery 客户端并行拉取指定组的资源。
//
// 每个 GroupVersion 一个 goroutine；结果用互斥锁保护，避免并发写 map。
// 单个 GV 失败会记入 failedGroups，不影响其他结果返回。
func fetchGroupVersionResources(ctx context.Context, d DiscoveryInterfaceWithContext, apiGroups *metav1.APIGroupList) (map[schema.GroupVersion]*metav1.APIResourceList, map[schema.GroupVersion]error) {
	groupVersionResources := make(map[schema.GroupVersion]*metav1.APIResourceList)
	failedGroups := make(map[schema.GroupVersion]error)

	wg := &sync.WaitGroup{}
	resultLock := &sync.Mutex{}
	for _, apiGroup := range apiGroups.Groups {
		for _, version := range apiGroup.Versions {
			groupVersion := schema.GroupVersion{Group: apiGroup.Name, Version: version.Version}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer utilruntime.HandleCrashWithContext(ctx)

				apiResourceList, err := d.ServerResourcesForGroupVersionWithContext(ctx, groupVersion.String())

				// 加锁记录结果，避免并发写 map。
				resultLock.Lock()
				defer resultLock.Unlock()

				if err != nil {
					// TODO: 也许应只针对 NotFound 错误重试。
					failedGroups[groupVersion] = err
				}
				if apiResourceList != nil {
					// 即使出错，也可能已经返回了部分 fallback 结果。
					groupVersionResources[groupVersion] = apiResourceList
				}
			}()
		}
	}
	wg.Wait()

	return groupVersionResources, failedGroups
}

// ServerPreferredResources 返回服务端 preferred version 所支持的资源。
//
// ServerPreferredResourcesWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerPreferredResourcesWithContext。
func (d *DiscoveryClient) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	_, rs, err := withRetries(context.Background(), defaultRetries, func() ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
		rs, err := ServerPreferredResources(d)
		return nil, rs, err
	})
	return rs, err
}

// ServerPreferredResourcesWithContext 返回服务端 preferred version 所支持的资源。
func (d *DiscoveryClient) ServerPreferredResourcesWithContext(ctx context.Context) ([]*metav1.APIResourceList, error) {
	_, rs, err := withRetries(ctx, defaultRetries, func() ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
		rs, err := ServerPreferredResourcesWithContext(ctx, d)
		return nil, rs, err
	})
	return rs, err
}

// ServerPreferredNamespacedResources 返回服务端 preferred version 所支持的命名空间级资源。
//
// ServerPreferredNamespacedResourcesWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerPreferredNamespacedResourcesWithContext。
func (d *DiscoveryClient) ServerPreferredNamespacedResources() ([]*metav1.APIResourceList, error) {
	return ServerPreferredNamespacedResources(d)
}

// ServerPreferredNamespacedResources 返回服务端 preferred version 所支持的命名空间级资源。
func (d *DiscoveryClient) ServerPreferredNamespacedResourcesWithContext(ctx context.Context) ([]*metav1.APIResourceList, error) {
	return ServerPreferredNamespacedResourcesWithContext(ctx, d)
}

// ServerPreferredNamespacedResources 使用给定的 discovery 接口查找 preferred 命名空间级资源。
//
// ServerPreferredNamespacedResourcesWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerPreferredNamespacedResourcesWithContext。
func ServerPreferredNamespacedResources(d DiscoveryInterface) ([]*metav1.APIResourceList, error) {
	return ServerPreferredNamespacedResourcesWithContext(context.Background(), ToDiscoveryInterfaceWithContext(d))
}

// ServerPreferredNamespacedResourcesWithContext 使用给定的 discovery 接口查找 preferred 命名空间级资源。
func ServerPreferredNamespacedResourcesWithContext(ctx context.Context, d DiscoveryInterfaceWithContext) ([]*metav1.APIResourceList, error) {
	all, err := ServerPreferredResourcesWithContext(ctx, d)
	return FilteredBy(ResourcePredicateFunc(func(groupVersion string, r *metav1.APIResource) bool {
		return r.Namespaced
	}), all), err
}

// ServerVersion 获取并解析服务端版本（git version）。
func (d *DiscoveryClient) ServerVersion() (*version.Info, error) {
	return d.ServerVersionWithContext(context.Background())
}

// ServerVersionWithContext 获取并解析服务端版本（git version）。
func (d *DiscoveryClient) ServerVersionWithContext(ctx context.Context) (*version.Info, error) {
	body, err := d.restClient.Get().AbsPath("/version").Do(ctx).Raw()
	if err != nil {
		return nil, err
	}
	var info version.Info
	err = json.Unmarshal(body, &info)
	if err != nil {
		return nil, fmt.Errorf("unable to parse the server version: %v", err)
	}
	return &info, nil
}

// OpenAPISchema 用 rest client 获取 OpenAPI v2 schema 并解析 proto。
func (d *DiscoveryClient) OpenAPISchema() (*openapi_v2.Document, error) {
	return d.OpenAPISchemaWithContext(context.Background())
}

// OpenAPISchemaWithContext 用 rest client 获取 OpenAPI v2 schema 并解析 proto。
func (d *DiscoveryClient) OpenAPISchemaWithContext(ctx context.Context) (*openapi_v2.Document, error) {
	data, err := d.restClient.Get().AbsPath("/openapi/v2").SetHeader("Accept", openAPIV2mimePb).Do(ctx).Raw()
	if err != nil {
		return nil, err
	}
	document := &openapi_v2.Document{}
	err = proto.Unmarshal(data, document)
	if err != nil {
		return nil, err
	}
	return document, nil
}

// OpenAPIV3WithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 OpenAPIV3WithContext。
func (d *DiscoveryClient) OpenAPIV3() openapi.Client {
	return openapi.NewClient(d.restClient)
}

func (d *DiscoveryClient) OpenAPIV3WithContext(ctx context.Context) openapi.ClientWithContext {
	return openapi.NewClientWithContext(d.restClient)
}

// WithLegacy 返回只接收旧版发现格式的当前 discovery 客户端副本。
//
// WithLegacyWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 WithLegacyWithContext。
func (d *DiscoveryClient) WithLegacy() DiscoveryInterface {
	// 复制而非修改接收者，保持原客户端仍可继续使用聚合发现。
	client := *d
	client.UseLegacyDiscovery = true
	return &client
}

// WithLegacyWithContext 返回只接收旧版发现格式的当前 discovery 客户端副本。
func (d *DiscoveryClient) WithLegacyWithContext(ctx context.Context) DiscoveryInterfaceWithContext {
	client := *d
	client.UseLegacyDiscovery = true
	return &client
}

// withRetries 重试给定的恢复函数，以应对 ServerGroup() 返回之后服务端支持的组发生变化；
// 只有在 context 未被取消时才重试。
//
// 只重试 ErrGroupDiscoveryFailed（聚合组可能短暂过期）；其他错误立即返回。
// 达到最大次数后仍返回最后一次的部分结果，便于调用方降级使用。
func withRetries(ctx context.Context, maxRetries int, f func() ([]*metav1.APIGroup, []*metav1.APIResourceList, error)) ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	var result []*metav1.APIResourceList
	var resultGroups []*metav1.APIGroup
	var err error
	for i := 0; i < maxRetries; i++ {
		resultGroups, result, err = f()
		if err == nil {
			return resultGroups, result, nil
		}
		if _, ok := err.(*ErrGroupDiscoveryFailed); !ok {
			return nil, nil, err
		}
		if ctx.Err() != nil {
			// context 已取消，停止重试并把 f 返回的错误继续向上传播。
			return nil, nil, err
		}
	}
	return resultGroups, result, err
}

func setDiscoveryDefaults(config *restclient.Config) error {
	// discovery 使用绝对路径（/api、/apis），因此清空 API 前缀与 GroupVersion。
	config.APIPath = ""
	config.GroupVersion = nil
	if config.Timeout == 0 {
		config.Timeout = defaultTimeout
	}
	// 尚未配置 burst 限制时设置默认值。
	if config.Burst == 0 {
		// discovery 的流量具有突发性，提高默认 burst 以容纳对大量 API 组资源信息的查询。
		// 与 ConfigFlags#ToDiscoveryClient() 中的 burst 设置一致。
		// 详见 https://issue.k8s.io/86149
		config.Burst = defaultBurst
	}
	// 只解码不编码：discovery 客户端从不发送对象。
	codec := runtime.NoopEncoder{Decoder: scheme.Codecs.UniversalDecoder()}
	config.NegotiatedSerializer = serializer.NegotiatedSerializerWrapper(runtime.SerializerInfo{Serializer: codec})
	if len(config.UserAgent) == 0 {
		config.UserAgent = restclient.DefaultKubernetesUserAgent()
	}
	return nil
}

// NewDiscoveryClientForConfig 为给定配置创建 DiscoveryClient，用于发现 API Server
// 支持的资源。它等价于 NewDiscoveryClientForConfigAndClient(c, httpClient)，其中
// httpClient 由 rest.HTTPClientFor(c) 生成。
// 其中 httpClient 由 rest.HTTPClientFor(c) 生成。
func NewDiscoveryClientForConfig(c *restclient.Config) (*DiscoveryClient, error) {
	// 复制配置后再改默认值，避免修改调用方的对象。
	config := *c
	if err := setDiscoveryDefaults(&config); err != nil {
		return nil, err
	}
	httpClient, err := restclient.HTTPClientFor(&config)
	if err != nil {
		return nil, err
	}
	return NewDiscoveryClientForConfigAndClient(&config, httpClient)
}

// NewDiscoveryClientForConfigAndClient 为给定配置创建 DiscoveryClient，用于发现 API
// Server 支持的资源。注意传入的 http client 优先于配置中的 transport 设置。
func NewDiscoveryClientForConfigAndClient(c *restclient.Config, httpClient *http.Client) (*DiscoveryClient, error) {
	config := *c
	if err := setDiscoveryDefaults(&config); err != nil {
		return nil, err
	}
	// 使用未版本化 REST 客户端：discovery 端点不属于任何 GroupVersion。
	client, err := restclient.UnversionedRESTClientForConfigAndClient(&config, httpClient)
	return &DiscoveryClient{restClient: client, LegacyPrefix: "/api", UseLegacyDiscovery: false}, err
}

// NewDiscoveryClientForConfig 为给定配置创建 DiscoveryClient；出错时 panic。
func NewDiscoveryClientForConfigOrDie(c *restclient.Config) *DiscoveryClient {
	client, err := NewDiscoveryClientForConfig(c)
	if err != nil {
		panic(err)
	}
	return client

}

// NewDiscoveryClient 为给定 RESTClient 创建 DiscoveryClient。
func NewDiscoveryClient(c restclient.Interface) *DiscoveryClient {
	return &DiscoveryClient{restClient: c, LegacyPrefix: "/api", UseLegacyDiscovery: false}
}

// RESTClient 返回该客户端实现用于与 API Server 通信的 RESTClient。
func (d *DiscoveryClient) RESTClient() restclient.Interface {
	if d == nil {
		return nil
	}
	return d.restClient
}
