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

// Package memory 提供基于内存的 discovery 缓存实现。
//
// 缓存不会在 miss 时回源，必须由调用方定期调用 Invalidate 触发刷新，
// 以避免对 apiserver 造成不可控的请求压力。
package memory

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"syscall"

	openapi_v2 "github.com/google/gnostic-models/openapiv2"

	errorsutil "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/openapi"
	cachedopenapi "k8s.io/client-go/openapi/cached"
	restclient "k8s.io/client-go/rest"
	"k8s.io/klog/v2"
)

// cacheEntry 保存一个 group/version 的资源列表或拉取时产生的错误。
type cacheEntry struct {
	resourceList *metav1.APIResourceList
	err          error
}

// memCacheClient can Invalidate() to stay up-to-date with discovery
// information.
//
// TODO: Switch to a watch interface. Right now it will poll after each
// Invalidate() call.
//
// memCacheClient 缓存 discovery 信息；调用 Invalidate 后会在下次访问时重新拉取。
// 目前没有 watch 机制，每次 Invalidate 之后的访问都会重新轮询。
type memCacheClient struct {
	delegate discovery.DiscoveryInterfaceWithContext

	// lock 保护以下所有缓存字段。刷新过程持有写锁，保证同一时刻只有一次刷新。
	lock                        sync.RWMutex
	groupToServerResources      map[string]*cacheEntry
	groupList                   *metav1.APIGroupList
	cacheValid                  bool
	openapiClient               *cachedopenapi.Client
	receivedAggregatedDiscovery bool
}

// Error Constants
//
// ErrCacheNotFound 表示请求的 group/version 不在缓存中（而不是服务端不存在）。
var (
	ErrCacheNotFound = errors.New("not found")
)

// Server returning empty ResourceList for Group/Version.
//
// emptyResponseError 表示服务端对某个 group/version 返回了空资源列表；
// 这类情况（如 metrics）很常见，因此以较低日志级别记录。
type emptyResponseError struct {
	gv string
}

func (e *emptyResponseError) Error() string {
	return fmt.Sprintf("received empty response for: %s", e.gv)
}

var _ discovery.CachedDiscoveryInterface = &memCacheClient{}
var _ discovery.CachedDiscoveryInterfaceWithContext = &memCacheClient{}

// isTransientConnectionError checks whether given error is "Connection refused" or
// "Connection reset" error which usually means that apiserver is temporarily
// unavailable.
func isTransientConnectionError(err error) bool {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno == syscall.ECONNREFUSED || errno == syscall.ECONNRESET
	}
	return false
}

// isTransientError 判断错误是否属于可重试的临时错误：
// context 取消、连接被拒/重置、5xx 服务端错误或限流。
func isTransientError(err error) bool {
	// Context cancelation is transient, retrying with a different context can work.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	if isTransientConnectionError(err) {
		return true
	}

	if t, ok := err.(errorsutil.APIStatus); ok && t.Status().Code >= 500 {
		return true
	}

	return errorsutil.IsTooManyRequests(err)
}

// ServerResourcesForGroupVersion returns the supported resources for a group and version.
func (d *memCacheClient) ServerResourcesForGroupVersion(groupVersion string) (*metav1.APIResourceList, error) {
	return d.ServerResourcesForGroupVersionWithContext(context.Background(), groupVersion)
}

// ServerResourcesForGroupVersion returns the supported resources for a group and version.
//
// ServerResourcesForGroupVersionWithContext 返回指定 group/version 的资源列表。
// 若缓存中的条目来自临时错误，会透明地重试一次；非临时错误则直接返回缓存结果。
func (d *memCacheClient) ServerResourcesForGroupVersionWithContext(ctx context.Context, groupVersion string) (*metav1.APIResourceList, error) {
	d.lock.Lock()
	defer d.lock.Unlock()
	if !d.cacheValid {
		if err := d.refreshLocked(ctx); err != nil {
			return nil, err
		}
	}
	cachedVal, ok := d.groupToServerResources[groupVersion]
	if !ok {
		return nil, ErrCacheNotFound
	}

	// 只对临时错误重试，避免把 NotFound 等永久错误也变成额外请求。
	if cachedVal.err != nil && isTransientError(cachedVal.err) {
		r, err := d.serverResourcesForGroupVersion(ctx, groupVersion)
		if err != nil {
			// Don't log "empty response" as an error; it is a common response for metrics.
			if _, emptyErr := err.(*emptyResponseError); emptyErr {
				// Log at same verbosity as disk cache.
				klog.FromContext(ctx).V(3).Info(err.Error())
			} else {
				utilruntime.HandleErrorWithContext(ctx, err, "Couldn't get resource list", "gv", groupVersion)
			}
		}
		cachedVal = &cacheEntry{r, err}
		d.groupToServerResources[groupVersion] = cachedVal
	}

	return cachedVal.resourceList, cachedVal.err
}

// ServerGroupsAndResources returns the groups and supported resources for all groups and versions.
//
// ServerGroupsAndResourcesWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ServerGroupsAndResourcesWithContext instead.
func (d *memCacheClient) ServerGroupsAndResources() ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	return d.ServerGroupsAndResourcesWithContext(context.Background())
}

// ServerGroupsAndResources returns the groups and supported resources for all groups and versions.
func (d *memCacheClient) ServerGroupsAndResourcesWithContext(ctx context.Context) ([]*metav1.APIGroup, []*metav1.APIResourceList, error) {
	return discovery.ServerGroupsAndResourcesWithContext(ctx, d)
}

// GroupsAndMaybeResources returns the list of APIGroups, and possibly the map of group/version
// to resources. The returned groups will never be nil, but the resources map can be nil
// if there are no cached resources.
//
// GroupsAndMaybeResourcesWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use GroupsAndMaybeResourcesWithContext instead.
func (d *memCacheClient) GroupsAndMaybeResources() (*metav1.APIGroupList, map[schema.GroupVersion]*metav1.APIResourceList, map[schema.GroupVersion]error, error) {
	return d.GroupsAndMaybeResourcesWithContext(context.Background())
}

// GroupsAndMaybeResourcesWithContext returns the list of APIGroups, and possibly the map of group/version
// to resources. The returned groups will never be nil, but the resources map can be nil
// if there are no cached resources.
//
// 只有服务端支持聚合发现（receivedAggregatedDiscovery）且缓存中确有资源时，
// 才返回 resourcesMap；否则返回 nil，由调用方回退到逐组查询。
func (d *memCacheClient) GroupsAndMaybeResourcesWithContext(ctx context.Context) (*metav1.APIGroupList, map[schema.GroupVersion]*metav1.APIResourceList, map[schema.GroupVersion]error, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}

	d.lock.Lock()
	defer d.lock.Unlock()

	if !d.cacheValid {
		if err := d.refreshLocked(ctx); err != nil {
			return nil, nil, nil, err
		}
	}
	// Build the resourceList from the cache?
	var resourcesMap map[schema.GroupVersion]*metav1.APIResourceList
	var failedGVs map[schema.GroupVersion]error
	if d.receivedAggregatedDiscovery && len(d.groupToServerResources) > 0 {
		resourcesMap = map[schema.GroupVersion]*metav1.APIResourceList{}
		failedGVs = map[schema.GroupVersion]error{}
		for gv, cacheEntry := range d.groupToServerResources {
			groupVersion, err := schema.ParseGroupVersion(gv)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("failed to parse group version (%v): %v", gv, err)
			}
			if cacheEntry.err != nil {
				// 单个 group/version 的失败不影响其他结果，单独汇报。
				failedGVs[groupVersion] = cacheEntry.err
			} else {
				resourcesMap[groupVersion] = cacheEntry.resourceList
			}
		}
	}
	return d.groupList, resourcesMap, failedGVs, nil
}

// ServerGroupsWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ServerGroupsWithContext instead.
func (d *memCacheClient) ServerGroups() (*metav1.APIGroupList, error) {
	return d.ServerGroupsWithContext(context.Background())
}

// ServerGroupsWithContext 只取 API 组列表，忽略资源映射。
func (d *memCacheClient) ServerGroupsWithContext(ctx context.Context) (*metav1.APIGroupList, error) {
	groups, _, _, err := d.GroupsAndMaybeResourcesWithContext(ctx)
	if err != nil {
		return nil, err
	}
	return groups, nil
}

// RESTClient 直接透传给 delegates，不走缓存。
func (d *memCacheClient) RESTClient() restclient.Interface {
	return d.delegate.RESTClient()
}

// ServerPreferredResourcesWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ServerPreferredResourcesWithContext instead.
func (d *memCacheClient) ServerPreferredResources() ([]*metav1.APIResourceList, error) {
	return d.ServerPreferredResourcesWithContext(context.Background())
}

// ServerPreferredResourcesWithContext 返回服务端首选的资源版本列表。
func (d *memCacheClient) ServerPreferredResourcesWithContext(ctx context.Context) ([]*metav1.APIResourceList, error) {
	return discovery.ServerPreferredResourcesWithContext(ctx, d)
}

// ServerPreferredNamespacedResourcesWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ServerPreferredNamespacedResourcesWithContext instead.
func (d *memCacheClient) ServerPreferredNamespacedResources() ([]*metav1.APIResourceList, error) {
	return d.ServerPreferredNamespacedResourcesWithContext(context.Background())
}

// ServerPreferredNamespacedResourcesWithContext 返回带有 namespace 作用域的首选资源列表。
func (d *memCacheClient) ServerPreferredNamespacedResourcesWithContext(ctx context.Context) ([]*metav1.APIResourceList, error) {
	return discovery.ServerPreferredNamespacedResourcesWithContext(ctx, d)
}

// ServerVersionWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ServerVersionWithContext instead.
func (d *memCacheClient) ServerVersion() (*version.Info, error) {
	return d.ServerVersionWithContext(context.Background())
}

// ServerVersionWithContext 直接透传给 delegate，不缓存版本信息。
func (d *memCacheClient) ServerVersionWithContext(ctx context.Context) (*version.Info, error) {
	return d.delegate.ServerVersionWithContext(ctx)
}

// OpenAPISchemaWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use OpenAPISchemaWithContext instead.
func (d *memCacheClient) OpenAPISchema() (*openapi_v2.Document, error) {
	return d.OpenAPISchemaWithContext(context.Background())
}

// OpenAPISchemaWithContext 直接透传给 delegate，不缓存 v2 schema。
func (d *memCacheClient) OpenAPISchemaWithContext(ctx context.Context) (*openapi_v2.Document, error) {
	return d.delegate.OpenAPISchemaWithContext(ctx)
}

// OpenAPIV3WithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use OpenAPIV3WithContext instead.
func (d *memCacheClient) OpenAPIV3() openapi.Client {
	return d.openAPIV3(context.Background())
}

// OpenAPIV3WithContext 返回缓存的 OpenAPI v3 客户端。
func (d *memCacheClient) OpenAPIV3WithContext(ctx context.Context) openapi.ClientWithContext {
	return d.openAPIV3(ctx)
}

func (d *memCacheClient) openAPIV3(ctx context.Context) *cachedopenapi.Client {
	// Must take lock since Invalidate call may modify openapiClient
	// Invalidate 会把 openapiClient 置 nil，因此这里必须持锁惰性创建。
	d.lock.Lock()
	defer d.lock.Unlock()

	if d.openapiClient == nil {
		d.openapiClient = cachedopenapi.NewClientWithContext(d.delegate.OpenAPIV3WithContext(ctx))
	}

	return d.openapiClient
}

// FreshWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use FreshWithContext instead.
func (d *memCacheClient) Fresh() bool {
	return d.FreshWithContext(context.Background())
}

// FreshWithContext 报告缓存是否已填充。
func (d *memCacheClient) FreshWithContext(ctx context.Context) bool {
	d.lock.RLock()
	defer d.lock.RUnlock()
	// Return whether the cache is populated at all. It is still possible that
	// a single entry is missing due to transient errors and the attempt to read
	// that entry will trigger retry.
	return d.cacheValid
}

// Invalidate enforces that no cached data that is older than the current time
// is used.
//
// InvalidateWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use InvalidateWithContext instead.
func (d *memCacheClient) Invalidate() {
	d.InvalidateWithContext(context.Background())
}

// InvalidateWithContext enforces that no cached data that is older than the current time
// is used.
//
// 清空所有缓存并递归调用 delegate 的 Invalidate（若其也是缓存实现），
// 保证整条缓存链在本次调用之后都不再返回旧数据。
func (d *memCacheClient) InvalidateWithContext(ctx context.Context) {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.cacheValid = false
	d.groupToServerResources = nil
	d.groupList = nil
	d.openapiClient = nil
	d.receivedAggregatedDiscovery = false
	ad, ok := d.delegate.(discovery.CachedDiscoveryInterfaceWithContext)
	if !ok {
		//nolint:staticcheck // Intentionally using the old interface here.
		ad2, ok2 := d.delegate.(discovery.CachedDiscoveryInterface)
		if ok2 {
			ad = discovery.ToCachedDiscoveryInterfaceWithContext(ad2)
			ok = true
		}
	}
	if ok {
		ad.InvalidateWithContext(ctx)
	}
}

// refreshLocked refreshes the state of cache. The caller must hold d.lock for
// writing.
//
// 优先使用聚合发现（一次请求拿到所有组和资源）；不支持时退化为先取组列表、
// 再并发拉取每个 group/version 的资源。
func (d *memCacheClient) refreshLocked(ctx context.Context) error {
	// TODO: Could this multiplicative set of calls be replaced by a single call
	// to ServerResources? If it's possible for more than one resulting
	// APIResourceList to have the same GroupVersion, the lists would need merged.
	var gl *metav1.APIGroupList
	var err error

	ad, ok := d.delegate.(discovery.AggregatedDiscoveryInterfaceWithContext)
	if !ok {
		//nolint:staticcheck // Intentionally using the old interface here.
		if ad2, ok2 := d.delegate.(discovery.AggregatedDiscoveryInterface); ok2 {
			ad = discovery.ToAggregatedDiscoveryInterfaceWithContext(ad2)
			ok = true
		}
	}
	if ok {
		var resources map[schema.GroupVersion]*metav1.APIResourceList
		var failedGVs map[schema.GroupVersion]error
		gl, resources, failedGVs, err = ad.GroupsAndMaybeResourcesWithContext(ctx)
		if resources != nil && err == nil {
			// Cache the resources.
			d.groupToServerResources = map[string]*cacheEntry{}
			d.groupList = gl
			for gv, resources := range resources {
				d.groupToServerResources[gv.String()] = &cacheEntry{resources, nil}
			}
			// Cache GroupVersion discovery errors
			// 单个 GV 的失败也缓存，避免每次访问都重新请求失败的 GV。
			for gv, err := range failedGVs {
				d.groupToServerResources[gv.String()] = &cacheEntry{nil, err}
			}
			d.receivedAggregatedDiscovery = true
			d.cacheValid = true
			return nil
		}
	} else {
		gl, err = d.delegate.ServerGroupsWithContext(ctx)
	}
	if err != nil || len(gl.Groups) == 0 {
		utilruntime.HandleErrorWithContext(ctx, err, "Couldn't get current server API group list")
		return err
	}

	// 并发拉取各 GV 的资源列表，用 WaitGroup 等待全部完成，
	// resultLock 保护共享的 rl map。
	wg := &sync.WaitGroup{}
	resultLock := &sync.Mutex{}
	rl := map[string]*cacheEntry{}
	for _, g := range gl.Groups {
		for _, v := range g.Versions {
			gv := v.GroupVersion
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer utilruntime.HandleCrashWithContext(ctx)

				r, err := d.serverResourcesForGroupVersion(ctx, gv)
				if err != nil {
					// Don't log "empty response" as an error; it is a common response for metrics.
					if _, emptyErr := err.(*emptyResponseError); emptyErr {
						// Log at same verbosity as disk cache.
						klog.FromContext(ctx).V(3).Info(err.Error())
					} else {
						utilruntime.HandleErrorWithContext(ctx, err, "Couldn't get resource list", "groupVersion", gv)
					}
				}

				resultLock.Lock()
				defer resultLock.Unlock()
				rl[gv] = &cacheEntry{r, err}
			}()
		}
	}
	wg.Wait()

	d.groupToServerResources, d.groupList = rl, gl
	d.cacheValid = true
	return nil
}

// serverResourcesForGroupVersion 调用 delegate，并把空资源列表转成 emptyResponseError，
// 以便调用方区分“成功但为空”与真正的错误。
func (d *memCacheClient) serverResourcesForGroupVersion(ctx context.Context, groupVersion string) (*metav1.APIResourceList, error) {
	r, err := d.delegate.ServerResourcesForGroupVersionWithContext(ctx, groupVersion)
	if err != nil {
		return r, err
	}
	if len(r.APIResources) == 0 {
		return r, &emptyResponseError{gv: groupVersion}
	}
	return r, nil
}

// WithLegacy returns current memory-cached discovery client;
// current client does not support legacy-only discovery.
//
// WithLegacyWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use WithLegacyWithContext instead.
func (d *memCacheClient) WithLegacy() discovery.DiscoveryInterface {
	return d
}

// WithLegacyWithContext returns current memory-cached discovery client;
// current client does not support legacy-only discovery.
//
// 本实现不支持 legacy-only discovery，因此原样返回自身。
func (d *memCacheClient) WithLegacyWithContext(ctx context.Context) discovery.DiscoveryInterfaceWithContext {
	return d
}

// NewMemCacheClient creates a new CachedDiscoveryInterface which caches
// discovery information in memory and will stay up-to-date if Invalidate is
// called with regularity.
//
// NOTE: The client will NOT resort to live lookups on cache misses.
//
// NewMemCacheClientWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use NewMemCacheClientWithContext instead.
func NewMemCacheClient(delegate discovery.DiscoveryInterface) discovery.CachedDiscoveryInterface {
	return newMemCacheClient(discovery.ToDiscoveryInterfaceWithContext(delegate))
}

// NewMemCacheClientWithContext creates a new CachedDiscoveryInterfaceWithContext which caches
// discovery information in memory and will stay up-to-date if Invalidate is
// called with regularity.
//
// NOTE: The client will NOT resort to live lookups on cache misses.
//
// 创建基于内存的 discovery 缓存；缓存 miss 时不会回源，需要调用方定期 Invalidate。
func NewMemCacheClientWithContext(delegate discovery.DiscoveryInterfaceWithContext) discovery.CachedDiscoveryInterfaceWithContext {
	return newMemCacheClient(delegate)
}

func newMemCacheClient(delegate discovery.DiscoveryInterfaceWithContext) *memCacheClient {
	return &memCacheClient{
		delegate:                    delegate,
		groupToServerResources:      map[string]*cacheEntry{},
		receivedAggregatedDiscovery: false,
	}
}
