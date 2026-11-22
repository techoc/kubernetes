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

package apiserver

import (
	"net/http"
	"strings"
	"sync"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/discovery"
)

// versionDiscoveryHandler 按 group/version 分发发现（discovery）请求到对应的
// APIVersionHandler；未命中时交给 delegate 处理。
type versionDiscoveryHandler struct {
	// TODO，写入不频繁，可优化
	discoveryLock sync.RWMutex
	discovery     map[schema.GroupVersion]*discovery.APIVersionHandler

	delegate http.Handler
}

// ServeHTTP 仅拦截 /apis/<group>/<version> 形式的三段路径，其余请求透传给 delegate。
func (r *versionDiscoveryHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	pathParts := splitPath(req.URL.Path)
	// 只匹配 /apis/<group>/<version>
	if len(pathParts) != 3 || pathParts[0] != "apis" {
		r.delegate.ServeHTTP(w, req)
		return
	}
	discovery, ok := r.getDiscovery(schema.GroupVersion{Group: pathParts[1], Version: pathParts[2]})
	if !ok {
		// 该 group/version 尚未注册发现信息，交由 delegate（通常是 notFoundHandler）。
		r.delegate.ServeHTTP(w, req)
		return
	}

	discovery.ServeHTTP(w, req)
}

// getDiscovery 读取指定 group/version 的发现处理器。
func (r *versionDiscoveryHandler) getDiscovery(gv schema.GroupVersion) (*discovery.APIVersionHandler, bool) {
	r.discoveryLock.RLock()
	defer r.discoveryLock.RUnlock()

	ret, ok := r.discovery[gv]
	return ret, ok
}

// setDiscovery 注册或覆盖指定 group/version 的发现处理器。
func (r *versionDiscoveryHandler) setDiscovery(gv schema.GroupVersion, discovery *discovery.APIVersionHandler) {
	r.discoveryLock.Lock()
	defer r.discoveryLock.Unlock()

	r.discovery[gv] = discovery
}

// unsetDiscovery 移除指定 group/version 的发现处理器，常用于 CRD 删除后。
func (r *versionDiscoveryHandler) unsetDiscovery(gv schema.GroupVersion) {
	r.discoveryLock.Lock()
	defer r.discoveryLock.Unlock()

	delete(r.discovery, gv)
}

// groupDiscoveryHandler 按 group 分发 /apis/<group> 的发现请求；
// 未命中时交给 delegate 处理。
type groupDiscoveryHandler struct {
	// TODO，写入不频繁，可优化
	discoveryLock sync.RWMutex
	discovery     map[string]*discovery.APIGroupHandler

	delegate http.Handler
}

// ServeHTTP 仅拦截 /apis/<group> 形式的两段路径，其余请求透传给 delegate。
func (r *groupDiscoveryHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	pathParts := splitPath(req.URL.Path)
	// 只匹配 /apis/<group>
	if len(pathParts) != 2 || pathParts[0] != "apis" {
		r.delegate.ServeHTTP(w, req)
		return
	}
	discovery, ok := r.getDiscovery(pathParts[1])
	if !ok {
		r.delegate.ServeHTTP(w, req)
		return
	}

	discovery.ServeHTTP(w, req)
}

// getDiscovery 读取指定 group 的发现处理器。
func (r *groupDiscoveryHandler) getDiscovery(group string) (*discovery.APIGroupHandler, bool) {
	r.discoveryLock.RLock()
	defer r.discoveryLock.RUnlock()

	ret, ok := r.discovery[group]
	return ret, ok
}

// setDiscovery 注册或覆盖指定 group 的发现处理器。
func (r *groupDiscoveryHandler) setDiscovery(group string, discovery *discovery.APIGroupHandler) {
	r.discoveryLock.Lock()
	defer r.discoveryLock.Unlock()

	r.discovery[group] = discovery
}

// unsetDiscovery 移除指定 group 的发现处理器。
func (r *groupDiscoveryHandler) unsetDiscovery(group string) {
	r.discoveryLock.Lock()
	defer r.discoveryLock.Unlock()

	delete(r.discovery, group)
}

// splitPath 返回 URL 路径的各段；空路径返回空切片而非含空串的切片。
func splitPath(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return []string{}
	}
	return strings.Split(path, "/")
}
