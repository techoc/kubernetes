/*
Copyright 2016 The Kubernetes Authors.

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

package disk

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	openapi_v2 "github.com/google/gnostic-models/openapiv2"
	"k8s.io/klog/v2"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/openapi"
	cachedopenapi "k8s.io/client-go/openapi/cached"
	restclient "k8s.io/client-go/rest"
)

// CachedDiscoveryClient 实现发现服务端所支持的 API 组、版本与资源的接口。
//
// 它把 discovery 结果以 JSON 文件形式缓存在本地目录，进程重启后仍可复用，
// 从而减少启动阶段对 API Server 的请求。delegate 通常已是内存缓存客户端，
// 因此读取路径是“磁盘 -> 内存 -> 实时请求”的逐级回退。
type CachedDiscoveryClient struct {
	delegate discovery.DiscoveryInterfaceWithContext

	// cacheDirectory 是缓存 discovery 文档的目录；必须按 host:port 区分，否则不同集群的缓存会互相污染。
	cacheDirectory string

	// ttl 定义缓存文件的有效时长。
	ttl time.Duration

	// mutex 保护下面几个可变字段，避免并发读写缓存状态时出现竞态。
	mutex sync.Mutex

	// ourFiles 记录本进程写出的所有缓存文件名。
	ourFiles map[string]struct{}
	// invalidated 为 true 时忽略所有非本进程写出的缓存文件（例如 Invalidate() 之后），避免沿用可能过期的外部数据。
	invalidated bool
	// fresh 表示当前使用的缓存文件是否都来自本进程。
	fresh bool

	// openAPIV3Client 包装 delegate 的客户端，用于缓存 OpenAPI v3 schema。
	openapiClient *cachedopenapi.Client
}

// 编译期断言：同时兼容旧接口与新接口，避免上游调用方因迁移而中断。
var (
	//nolint:staticcheck // Intentionally using the old interface here.
	_ discovery.CachedDiscoveryInterface            = &CachedDiscoveryClient{}
	_ discovery.CachedDiscoveryInterfaceWithContext = &CachedDiscoveryClient{}
)

// ServerResourcesForGroupVersion 返回指定 group/version 支持的资源。
//
// ServerResourcesForGroupVersionWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerResourcesForGroupVersionWithContext。
//
// 无 context 版本仅为兼容旧接口，内部使用 Background。
func (d *CachedDiscoveryClient) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	return d.ServerResourcesForGroupVersionWithContext(context.Background(), groupVersion)
}

// ServerResourcesForGroupVersionWithContext 返回指定 group/version 支持的资源。
func (d *CachedDiscoveryClient) ServerResourcesForGroupVersionWithContext(ctx context.Context, groupVersion string) (*metav1.APIResourceList, error) {
	// 每个 group/version 单独一个目录，文件名固定，便于外部工具检查缓存内容。
	filename := filepath.Join(d.cacheDirectory, groupVersion, "serverresources.json")
	cachedBytes, err := d.getCachedFile(filename)
	// 读取失败不直接返回错误：要么没有文件，要么无法完成缓存校验，两种情况都可回退到实时请求。
	if err == nil {
		cachedResources := &metav1.APIResourceList{}
		if err := runtime.DecodeInto(scheme.Codecs.UniversalDecoder(), cachedBytes, cachedResources); err == nil {
			klog.FromContext(ctx).V(10).Info("Returning cached discovery info", "fileName", filename)
			return cachedResources, nil
		}
	}

	liveResources, err := d.delegate.ServerResourcesForGroupVersionWithContext(ctx, groupVersion)
	if err != nil {
		klog.FromContext(ctx).V(3).Info("Skipped caching discovery info due to error", "err", err)
		return liveResources, err
	}
	if liveResources == nil || len(liveResources.APIResources) == 0 {
		// 空结果不落盘：否则一次异常响应会把空资源列表缓存一整个 TTL。
		klog.FromContext(ctx).V(3).Info("skipped caching discovery info, no resources found")
		return liveResources, err
	}

	if err := d.writeCachedFile(filename, liveResources); err != nil {
		klog.FromContext(ctx).V(1).Info("Failed to write cache", "fileName", filename, "err", err)
	}

	return liveResources, nil
}

// ServerGroupsAndResources 返回所有 group/version 支持的组与资源。
//
// ServerGroupsAndResourcesWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerGroupsAndResourcesWithContext。
func (d *CachedDiscoveryClient) ServerGroupsAndResources() ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	return d.ServerGroupsAndResourcesWithContext(context.Background())
}

// ServerGroupsAndResourcesWithContext 返回所有 group/version 支持的组与资源。
func (d *CachedDiscoveryClient) ServerGroupsAndResourcesWithContext(ctx context.Context) ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	return discovery.ServerGroupsAndResourcesWithContext(ctx, d)
}

// ServerGroups 返回服务端支持的组，以及支持的版本、preferred version 等信息。
//
// ServerGroupsWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerGroupsWithContext。
func (d *CachedDiscoveryClient) ServerGroups() (*metav1.APIGroupList, error) {
	return d.ServerGroupsWithContext(context.Background())
}

// ServerGroupsWithContext 返回服务端支持的组，以及版本与 preferred version 信息。
func (d *CachedDiscoveryClient) ServerGroupsWithContext(ctx context.Context) (*metav1.APIGroupList, error) {
	// groups 列表是全局的，直接放在缓存目录根下。
	filename := filepath.Join(d.cacheDirectory, "servergroups.json")
	cachedBytes, err := d.getCachedFile(filename)
	// 读取 groups 文件失败时回退到 delegate；缓存只是优化，不应使发现整体失败。
	if err == nil {
		cachedGroups := &metav1.APIGroupList{}
		if err := runtime.DecodeInto(scheme.Codecs.UniversalDecoder(), cachedBytes, cachedGroups); err == nil {
			klog.FromContext(ctx).V(10).Info("Returning cached discovery info", "fileName", filename)
			return cachedGroups, nil
		}
	}

	liveGroups, err := d.delegate.ServerGroupsWithContext(ctx)
	if err != nil {
		klog.FromContext(ctx).V(3).Info("Skipped caching discovery info due to error", "err", err)
		return liveGroups, err
	}
	if liveGroups == nil || len(liveGroups.Groups) == 0 {
		klog.FromContext(ctx).V(3).Info("Skipped caching discovery info, no groups found")
		return liveGroups, err
	}

	if err := d.writeCachedFile(filename, liveGroups); err != nil {
		klog.FromContext(ctx).V(1).Info("Failed to write cache", "fileName", filename, "err", err)
	}

	return liveGroups, nil
}

// getCachedFile 读取缓存文件并检查 TTL；任何失败都返回错误，由调用方回退到
// 实时请求，因此这里不区分“不存在/已过期/被失效”的原因（日志之外）。
func (d *CachedDiscoveryClient) getCachedFile(filename string) ([]byte, error) {
	// 失效后只信任本进程写出的文件，避免沿用其他进程可能写入的陈旧内容。
	d.mutex.Lock()
	_, ourFile := d.ourFiles[filename]
	if d.invalidated && !ourFile {
		d.mutex.Unlock()
		return nil, errors.New("cache invalidated")
	}
	d.mutex.Unlock()

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	if time.Now().After(fileInfo.ModTime().Add(d.ttl)) {
		return nil, errors.New("cache expired")
	}

	// 缓存存在且有效，尝试读取并使用它。
	cachedBytes, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	// 只要用到一个非本进程写入的文件，Fresh() 就应返回 false，
	// 提示调用方在缓存未命中时重试（而不是断言资源不存在）。
	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.fresh = d.fresh && ourFile

	return cachedBytes, nil
}

// writeCachedFile 把 obj 编码后原子地写入 filename。
// 先写临时文件再 rename，保证并发读者永远看不到半截 JSON。
func (d *CachedDiscoveryClient) writeCachedFile(filename string, obj runtime.Object) error {
	if err := os.MkdirAll(filepath.Dir(filename), 0750); err != nil {
		return err
	}

	bytes, err := runtime.Encode(scheme.Codecs.LegacyCodec(), obj)
	if err != nil {
		return err
	}

	f, err := os.CreateTemp(filepath.Dir(filename), filepath.Base(filename)+".")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(bytes)
	if err != nil {
		return err
	}

	err = os.Chmod(f.Name(), 0660)
	if err != nil {
		return err
	}

	name := f.Name()
	err = f.Close()
	if err != nil {
		return err
	}

	// 原子 rename：rename 与 ourFiles 更新必须在同一把锁内完成，否则并发 Invalidate 可能
	// 清空 ourFiles 后又被这里补回，破坏“失效后不信任旧文件”的语义。
	d.mutex.Lock()
	defer d.mutex.Unlock()
	err = os.Rename(name, filename)
	if err == nil {
		d.ourFiles[filename] = struct{}{}
	}
	return err
}

// RESTClient 返回该客户端实现用于与 API Server 通信的 RESTClient。
func (d *CachedDiscoveryClient) RESTClient() restclient.Interface {
	return d.delegate.RESTClient()
}

// ServerPreferredResources 返回服务端 preferred version 所支持的资源。
//
// ServerPreferredResourcesWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerPreferredResourcesWithContext。
func (d *CachedDiscoveryClient) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return d.ServerPreferredResourcesWithContext(context.Background())
}

// ServerPreferredResourcesWithContext 返回服务端 preferred version 所支持的资源。
func (d *CachedDiscoveryClient) ServerPreferredResourcesWithContext(ctx context.Context) ([]*metav1.APIResourceList, error) {
	return discovery.ServerPreferredResourcesWithContext(ctx, d)
}

// ServerPreferredNamespacedResources 返回服务端 preferred version 所支持的命名空间级资源。
//
// ServerPreferredNamespacedResourcesWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerPreferredNamespacedResourcesWithContext。
func (d *CachedDiscoveryClient) ServerPreferredNamespacedResources() ([]*metav1.APIResourceList, error) {
	return d.ServerPreferredNamespacedResourcesWithContext(context.Background())
}

// ServerPreferredNamespacedResourcesWithContext 返回服务端 preferred version 所支持的命名空间级资源。
func (d *CachedDiscoveryClient) ServerPreferredNamespacedResourcesWithContext(ctx context.Context) ([]*metav1.APIResourceList, error) {
	return discovery.ServerPreferredNamespacedResourcesWithContext(ctx, d)
}

// ServerVersion 获取并解析服务端版本（git version）。
//
// ServerVersionWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ServerVersionWithContext。
func (d *CachedDiscoveryClient) ServerVersion() (*version.Info, error) {
	return d.ServerVersionWithContext(context.Background())
}

// ServerVersionWithContext 获取并解析服务端版本（git version）。
func (d *CachedDiscoveryClient) ServerVersionWithContext(ctx context.Context) (*version.Info, error) {
	return d.delegate.ServerVersionWithContext(ctx)
}

// OpenAPISchema 获取并解析服务端支持的 swagger API schema。
//
// OpenAPISchemaWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 OpenAPISchemaWithContext。
func (d *CachedDiscoveryClient) OpenAPISchema() (*openapi_v2.Document, error) {
	return d.OpenAPISchemaWithContext(context.Background())
}

// OpenAPISchemaWithContext 获取并解析服务端支持的 swagger API schema。
func (d *CachedDiscoveryClient) OpenAPISchemaWithContext(ctx context.Context) (*openapi_v2.Document, error) {
	return d.delegate.OpenAPISchemaWithContext(ctx)
}

// OpenAPIV3 获取并解析服务端暴露的 OpenAPI v3 规范。
func (d *CachedDiscoveryClient) OpenAPIV3() openapi.Client {
	return d.openAPIV3(context.Background())
}

// OpenAPIV3WithContext 获取并解析服务端暴露的 OpenAPI v3 规范。
func (d *CachedDiscoveryClient) OpenAPIV3WithContext(ctx context.Context) openapi.ClientWithContext {
	return d.openAPIV3(ctx)
}

// openAPIV3 惰性创建并缓存 OpenAPI v3 客户端，使同一进程内的多个调用方共用
// 一份已拉取的 schema。
func (d *CachedDiscoveryClient) openAPIV3(ctx context.Context) *cachedopenapi.Client {
	// Invalidate 可能修改 openapiClient，因此这里必须加锁。
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.openapiClient == nil {
		// delegate 使用特殊 HTTP client 创建，它遵循 E-Tag 缓存响应，从而直接从磁盘提供缓存内容。
		d.openapiClient = cachedopenapi.NewClientWithContext(d.delegate.OpenAPIV3WithContext(ctx))
	}

	return d.openapiClient
}

// Fresh 告诉调用方：缓存未命中时是否应重试（false = 重试，true = 无需重试）。
//
// FreshWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 FreshWithContext。
func (d *CachedDiscoveryClient) Fresh() bool {
	return d.FreshWithContext(context.Background())
}

// FreshWithContext 告诉调用方：缓存未命中时是否应重试（false = 重试，true = 无需重试）。
func (d *CachedDiscoveryClient) FreshWithContext(ctx context.Context) bool {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	// 只读取标志，ctx 目前仅用于与 WithContext 系列签名保持一致。
	return d.fresh
}

// Invalidate 使当前时间之前缓存的所有数据在后续都不再被使用。
//
// InvalidateWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 InvalidateWithContext。
func (d *CachedDiscoveryClient) Invalidate() {
	d.InvalidateWithContext(context.Background())
}

// InvalidateWithContext 使当前时间之前缓存的所有数据在后续都不再被使用。
func (d *CachedDiscoveryClient) InvalidateWithContext(ctx context.Context) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	// 清空 ourFiles 并标记 invalidated：已有文件仍留在磁盘上，但除了本次
	// 失效之后重新写入的文件外都不再被读取。fresh 置 true 表示从现在起
	// 缓存内容都来自本进程，可以信任。
	d.ourFiles = map[string]struct{}{}
	d.fresh = true
	d.invalidated = true
	d.openapiClient = nil
	// delegate 可能是旧接口实现，需要先做一次适配；这保证内层缓存也被清空。
	ad, ok := d.delegate.(discovery.CachedDiscoveryInterfaceWithContext)
	if !ok {
		//nolint:staticcheck // Intentionally using the old interface here.
		if ad2, ok2 := d.delegate.(discovery.CachedDiscoveryInterface); ok2 {
			ad = discovery.ToCachedDiscoveryInterfaceWithContext(ad2)
			ok = true
		}
	}
	if ok {
		ad.InvalidateWithContext(ctx)
	}
}

// WithLegacy 返回当前缓存 discovery 客户端；该客户端不支持仅旧版发现。
func (d *CachedDiscoveryClient) WithLegacy() discovery.DiscoveryInterface {
	return d
}

// WithLegacyWithContext 返回当前缓存 discovery 客户端；该客户端不支持仅旧版发现。
func (d *CachedDiscoveryClient) WithLegacyWithContext(ctx context.Context) discovery.DiscoveryInterfaceWithContext {
	return d
}

// NewCachedDiscoveryClientForConfig 为给定 config 创建 DiscoveryClient，并包装为
// CachedDiscoveryClient。传入的配置会被更新为一个能处理缓存响应的自定义 transport。
// 目前接收两个不同的缓存目录：一个沿袭旧行为，用 --cache-dir 的值存储
// CacheRoundTripper 的缓存数据；另一个使用硬编码路径（~/.kube/cache/discovery/...）
// 缓存 CachedDiscoveryClient 的数据。httpCacheDir 为空时，restconfig 的 transport 不会被修改。
// restconfig 会被更新为能处理缓存响应的 roundtripper。
// discoveryCacheDir 为空时，缓存的服务器资源数据从当前目录查找。
func NewCachedDiscoveryClientForConfig(config *restclient.Config, discoveryCacheDir, httpCacheDir string, ttl time.Duration) (*CachedDiscoveryClient, error) {
	if len(httpCacheDir) > 0 {
		// 用能处理缓存响应的自定义 roundtripper 更新传入的 restconfig。
		// 复制 config 再包裹，避免修改调用方传入的共享配置。
		config = restclient.CopyConfig(config)
		config.Wrap(func(rt http.RoundTripper) http.RoundTripper {
			return newCacheRoundTripper(httpCacheDir, rt)
		})
	}

	discoveryClient, err := discovery.NewDiscoveryClientForConfig(config)
	if err != nil {
		return nil, err
	}

	// delegate 已用 memcache 缓存 discovery 组和资源。若服务端支持新版聚合发现格式，
	// 通常只返回（并缓存）组的 ServerGroups 也能一并缓存资源。
	return newCachedDiscoveryClient(memory.NewMemCacheClientWithContext(discoveryClient), discoveryCacheDir, ttl), nil
}

// NewCachedDiscoveryClient 创建 DiscoveryClient。cacheDirectory 是存放 discovery 文档的目录，必须按 host:port 唯一，否则缓存会串集群。
func newCachedDiscoveryClient(delegate discovery.DiscoveryInterfaceWithContext, cacheDirectory string, ttl time.Duration) *CachedDiscoveryClient {
	// fresh 初始为 true：刚创建时还没有读取任何外部文件。
	return &CachedDiscoveryClient{
		delegate:       delegate,
		cacheDirectory: cacheDirectory,
		ttl:            ttl,
		ourFiles:       map[string]struct{}{},
		fresh:          true,
	}
}
