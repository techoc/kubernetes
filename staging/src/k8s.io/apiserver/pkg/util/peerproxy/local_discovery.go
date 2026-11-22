/*
Copyright 2025 The Kubernetes Authors.

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

package peerproxy

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
)

// RunLocalDiscoveryCacheSync populated the localDiscoveryInfoCache and
// starts a goroutine to periodically refresh the local discovery cache.
//
// 初始填充失败会直接返回错误，阻止 server 启动：没有发现缓存就无法判断某资源应由本机
// 还是 peer 处理，贸然运行会把请求转发到错误的位置。
func (h *peerProxyHandler) RunLocalDiscoveryCacheSync(stopCh <-chan struct{}) error {
	klog.Info("localDiscoveryCacheInvalidation goroutine started")
	// Populate the cache initially.
	if err := h.populateLocalDiscoveryCache(); err != nil {
		return fmt.Errorf("failed to populate initial local discovery cache: %w", err)
	}

	go func() {
		for {
			select {
			case <-h.localDiscoveryCacheTicker.C:
				klog.V(4).Infof("Invalidating local discovery cache")
				// 刷新失败时保留旧缓存：旧数据只是可能过时，总比完全没有数据导致全部请求被转发要好。
				if err := h.populateLocalDiscoveryCache(); err != nil {
					klog.Errorf("Failed to repopulate local discovery cache: %v", err)
				}
			case <-stopCh:
				klog.Info("localDiscoveryCacheInvalidation goroutine received stop signal")
				if h.localDiscoveryCacheTicker != nil {
					h.localDiscoveryCacheTicker.Stop()
					klog.Info("localDiscoveryCacheTicker stopped")
				}
				klog.Info("localDiscoveryCacheInvalidation goroutine exiting")
				return
			}
		}
	}()
	return nil
}

// populateLocalDiscoveryCache 从 discovery 拉取全部 group/version 的资源列表，
// 并重建 GVR 集合后整体替换缓存。
func (h *peerProxyHandler) populateLocalDiscoveryCache() error {
	_, resourcesByGV, _, err := h.discoveryClient.GroupsAndMaybeResources()
	if err != nil {
		return fmt.Errorf("error getting API group resources from discovery: %w", err)
	}

	// 先构建新集合再一次性替换，避免读者看到部分填充的缓存。
	freshLocalDiscoveryResponse := map[schema.GroupVersionResource]bool{}
	for gv, resources := range resourcesByGV {
		for _, resource := range resources.APIResources {
			gvr := gv.WithResource(resource.Name)
			freshLocalDiscoveryResponse[gvr] = true
		}
	}

	h.localDiscoveryInfoCache.Store(freshLocalDiscoveryResponse)
	// Signal that the cache has been populated.
	h.localDiscoveryInfoCachePopulatedOnce.Do(func() {
		close(h.localDiscoveryInfoCachePopulated)
	})
	return nil
}

// shouldServeLocally checks if the requested resource is present in the local
// discovery cache indicating the request can be served by this server.
//
// 缓存缺失或类型异常时返回 false（交给 peer 处理），属于保守选择。
func (h *peerProxyHandler) shouldServeLocally(gvr schema.GroupVersionResource) bool {
	cacheValue := h.localDiscoveryInfoCache.Load()
	if cacheValue == nil {
		return false
	}

	cache, ok := cacheValue.(map[schema.GroupVersionResource]bool)
	if !ok {
		klog.Warning("Invalid cache type in localDiscoveryInfoCache")
		return false
	}

	exists, ok := cache[gvr]
	if !ok {
		klog.V(4).Infof("resource not found for %v in local discovery cache\n", gvr.GroupVersion())
		return false
	}

	return exists
}
