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

// CachedDiscoveryClient implements the functions that discovery server-supported API groups,
// versions and resources.
//
// 它把 discovery 结果以 JSON 文件形式缓存在本地目录，进程重启后仍可复用，
// 从而减少启动阶段对 API Server 的请求。delegate 通常已是内存缓存客户端，
// 因此读取路径是“磁盘 -> 内存 -> 实时请求”的逐级回退。
type CachedDiscoveryClient struct {
	delegate discovery.DiscoveryInterfaceWithContext

	// cacheDirectory is the directory where discovery docs are held.  It must be unique per host:port combination to work well.
	cacheDirectory string

	// ttl is how long the cache should be considered valid
	ttl time.Duration

	// mutex protects the variables below
	mutex sync.Mutex

	// ourFiles are all filenames of cache files created by this process
	ourFiles map[string]struct{}
	// invalidated is true if all cache files should be ignored that are not ours (e.g. after Invalidate() was called)
	invalidated bool
	// fresh is true if all used cache files were ours
	fresh bool

	// caching openapi v3 client which wraps the delegate's client
	openapiClient *cachedopenapi.Client
}

// 编译期断言：同时兼容旧接口与新接口，避免上游调用方因迁移而中断。
var (
	//nolint:staticcheck // Intentionally using the old interface here.
	_ discovery.CachedDiscoveryInterface            = &CachedDiscoveryClient{}
	_ discovery.CachedDiscoveryInterfaceWithContext = &CachedDiscoveryClient{}
)

// ServerResourcesForGroupVersion returns the supported resources for a group and version.
//
// ServerResourcesForGroupVersionWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ServerResourcesForGroupVersionWithContext instead.
//
// 无 context 版本仅为兼容旧接口，内部使用 Background。
func (d *CachedDiscoveryClient) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	return d.ServerResourcesForGroupVersionWithContext(context.Background(), groupVersion)
}

// ServerResourcesForGroupVersionWithContext returns the supported resources for a group and version.
func (d *CachedDiscoveryClient) ServerResourcesForGroupVersionWithContext(ctx context.Context, groupVersion string) (*metav1.APIResourceList, error) {
	// 每个 group/version 单独一个目录，文件名固定，便于外部工具检查缓存内容。
	filename := filepath.Join(d.cacheDirectory, groupVersion, "serverresources.json")
	cachedBytes, err := d.getCachedFile(filename)
	// don't fail on errors, we either don't have a file or won't be able to run the cached check. Either way we can fallback.
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

// ServerGroupsAndResources returns the supported groups and resources for all groups and versions.
//
// ServerGroupsAndResourcesWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ServerGroupsAndResourcesWithContext instead.
func (d *CachedDiscoveryClient) ServerGroupsAndResources() ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	return d.ServerGroupsAndResourcesWithContext(context.Background())
}

// ServerGroupsAndResourcesWithContext returns the supported groups and resources for all groups and versions.
func (d *CachedDiscoveryClient) ServerGroupsAndResourcesWithContext(ctx context.Context) ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	return discovery.ServerGroupsAndResourcesWithContext(ctx, d)
}

// ServerGroups returns the supported groups, with information like supported versions and the
// preferred version.
//
// ServerGroupsWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ServerGroupsWithContext instead.
func (d *CachedDiscoveryClient) ServerGroups() (*metav1.APIGroupList, error) {
	return d.ServerGroupsWithContext(context.Background())
}

// ServerGroupsWithContext returns the supported groups, with information like supported versions and the
// preferred version.
func (d *CachedDiscoveryClient) ServerGroupsWithContext(ctx context.Context) (*metav1.APIGroupList, error) {
	// groups 列表是全局的，直接放在缓存目录根下。
	filename := filepath.Join(d.cacheDirectory, "servergroups.json")
	cachedBytes, err := d.getCachedFile(filename)
	// don't fail on errors, we either don't have a file or won't be able to run the cached check. Either way we can fallback.
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
	// after invalidation ignore cache files not created by this process
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

	// the cache is present and its valid.  Try to read and use it.
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

	// atomic rename
	// rename 与 ourFiles 更新必须在同一把锁内完成，否则并发 Invalidate 可能
	// 清空 ourFiles 后又被这里补回，破坏“失效后不信任旧文件”的语义。
	d.mutex.Lock()
	defer d.mutex.Unlock()
	err = os.Rename(name, filename)
	if err == nil {
		d.ourFiles[filename] = struct{}{}
	}
	return err
}

// RESTClient returns a RESTClient that is used to communicate with API server
// by this client implementation.
func (d *CachedDiscoveryClient) RESTClient() restclient.Interface {
	return d.delegate.RESTClient()
}

// ServerPreferredResources returns the supported resources with the version preferred by the
// server.
//
// ServerPreferredResourcesWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ServerPreferredResourcesWithContext instead.
func (d *CachedDiscoveryClient) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return d.ServerPreferredResourcesWithContext(context.Background())
}

// ServerPreferredResourcesWithContext returns the supported resources with the version preferred by the
// server.
func (d *CachedDiscoveryClient) ServerPreferredResourcesWithContext(ctx context.Context) ([]*metav1.APIResourceList, error) {
	return discovery.ServerPreferredResourcesWithContext(ctx, d)
}

// ServerPreferredNamespacedResources returns the supported namespaced resources with the
// version preferred by the server.
//
// ServerPreferredNamespacedResourcesWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ServerPreferredNamespacedResourcesWithContext instead.
func (d *CachedDiscoveryClient) ServerPreferredNamespacedResources() ([]*metav1.APIResourceList, error) {
	return d.ServerPreferredNamespacedResourcesWithContext(context.Background())
}

// ServerPreferredNamespacedResourcesWithContext returns the supported namespaced resources with the
// version preferred by the server.
func (d *CachedDiscoveryClient) ServerPreferredNamespacedResourcesWithContext(ctx context.Context) ([]*metav1.APIResourceList, error) {
	return discovery.ServerPreferredNamespacedResourcesWithContext(ctx, d)
}

// ServerVersion retrieves and parses the server's version (git version).
//
// ServerVersionWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ServerVersionWithContext instead.
func (d *CachedDiscoveryClient) ServerVersion() (*version.Info, error) {
	return d.ServerVersionWithContext(context.Background())
}

// ServerVersionWithContext retrieves and parses the server's version (git version).
func (d *CachedDiscoveryClient) ServerVersionWithContext(ctx context.Context) (*version.Info, error) {
	return d.delegate.ServerVersionWithContext(ctx)
}

// OpenAPISchema retrieves and parses the swagger API schema the server supports.
//
// OpenAPISchemaWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use OpenAPISchemaWithContext instead.
func (d *CachedDiscoveryClient) OpenAPISchema() (*openapi_v2.Document, error) {
	return d.OpenAPISchemaWithContext(context.Background())
}

// OpenAPISchemaWithContext retrieves and parses the swagger API schema the server supports.
func (d *CachedDiscoveryClient) OpenAPISchemaWithContext(ctx context.Context) (*openapi_v2.Document, error) {
	return d.delegate.OpenAPISchemaWithContext(ctx)
}

// OpenAPIV3 retrieves and parses the OpenAPIV3 specs exposed by the server
func (d *CachedDiscoveryClient) OpenAPIV3() openapi.Client {
	return d.openAPIV3(context.Background())
}

// OpenAPIV3WithContext retrieves and parses the OpenAPIV3 specs exposed by the server
func (d *CachedDiscoveryClient) OpenAPIV3WithContext(ctx context.Context) openapi.ClientWithContext {
	return d.openAPIV3(ctx)
}

// openAPIV3 惰性创建并缓存 OpenAPI v3 客户端，使同一进程内的多个调用方共用
// 一份已拉取的 schema。
func (d *CachedDiscoveryClient) openAPIV3(ctx context.Context) *cachedopenapi.Client {
	// Must take lock since Invalidate call may modify openapiClient
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.openapiClient == nil {
		// Delegate is discovery client created with special HTTP client which
		// respects E-Tag cache responses to serve cache from disk.
		d.openapiClient = cachedopenapi.NewClientWithContext(d.delegate.OpenAPIV3WithContext(ctx))
	}

	return d.openapiClient
}

// Fresh is supposed to tell the caller whether or not to retry if the cache
// fails to find something (false = retry, true = no need to retry).
//
// FreshWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use FreshWithContext instead.
func (d *CachedDiscoveryClient) Fresh() bool {
	return d.FreshWithContext(context.Background())
}

// FreshWithContext is supposed to tell the caller whether or not to retry if the cache
// fails to find something (false = retry, true = no need to retry).
func (d *CachedDiscoveryClient) FreshWithContext(ctx context.Context) bool {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	// 只读取标志，ctx 目前仅用于与 WithContext 系列签名保持一致。
	return d.fresh
}

// Invalidate enforces that no cached data is used in the future that is older than the current time.
//
// InvalidateWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use InvalidateWithContext instead.
func (d *CachedDiscoveryClient) Invalidate() {
	d.InvalidateWithContext(context.Background())
}

// InvalidateWithContext enforces that no cached data is used in the future that is older than the current time.
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

// WithLegacy returns current cached discovery client;
// current client does not support legacy-only discovery.
func (d *CachedDiscoveryClient) WithLegacy() discovery.DiscoveryInterface {
	return d
}

// WithLegacyWithContext returns current cached discovery client;
// current client does not support legacy-only discovery.
func (d *CachedDiscoveryClient) WithLegacyWithContext(ctx context.Context) discovery.DiscoveryInterfaceWithContext {
	return d
}

// NewCachedDiscoveryClientForConfig creates a new DiscoveryClient for the given config, and wraps
// the created client in a CachedDiscoveryClient. The provided configuration is updated with a
// custom transport that understands cache responses.
// We receive two distinct cache directories for now, in order to preserve old behavior
// which makes use of the --cache-dir flag value for storing cache data from the CacheRoundTripper,
// and makes use of the hardcoded destination (~/.kube/cache/discovery/...) for storing
// CachedDiscoveryClient cache data. If httpCacheDir is empty, the restconfig's transport will not
// be updated with a roundtripper that understands cache responses.
// If discoveryCacheDir is empty, cached server resource data will be looked up in the current directory.
func NewCachedDiscoveryClientForConfig(config *restclient.Config, discoveryCacheDir, httpCacheDir string, ttl time.Duration) (*CachedDiscoveryClient, error) {
	if len(httpCacheDir) > 0 {
		// update the given restconfig with a custom roundtripper that
		// understands how to handle cache responses.
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

	// The delegate caches the discovery groups and resources (memcache). "ServerGroups",
	// which usually only returns (and caches) the groups, can now store the resources as
	// well if the server supports the newer aggregated discovery format.
	return newCachedDiscoveryClient(memory.NewMemCacheClientWithContext(discoveryClient), discoveryCacheDir, ttl), nil
}

// NewCachedDiscoveryClient creates a new DiscoveryClient.  cacheDirectory is the directory where discovery docs are held.  It must be unique per host:port combination to work well.
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
