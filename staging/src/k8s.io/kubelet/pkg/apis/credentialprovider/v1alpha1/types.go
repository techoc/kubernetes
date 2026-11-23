/*
Copyright 2020 The Kubernetes Authors.

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

// 本文件保留 v1alpha1 的凭据协议结构供内部版本转换使用：
// 新版请求里的服务账号 token 与注解没有对应的 alpha 字段，转换时被有意丢弃。

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CredentialProviderRequest includes the image that the kubelet requires authentication for.
// Kubelet will pass this request object to the plugin via stdin. In general, plugins should
// prefer responding with the same apiVersion they were sent.
//
// 旧协议只带镜像信息；需要服务账号 token 的插件必须使用 v1。
type CredentialProviderRequest struct {
	metav1.TypeMeta `json:""`

	// image is the container image that is being pulled as part of the
	// credential provider plugin request. Plugins may optionally parse the image
	// to extract any information required to fetch credentials.
	//
	// 镜像字符串是旧协议里唯一的输入，插件只能靠它决定凭据。
	Image string `json:"image"`
}

// PluginCacheKeyType 控制凭据缓存粒度，值域必须与 v1 保持一致以便跨版本转换。
type PluginCacheKeyType string

const (
	// ImagePluginCacheKeyType means the kubelet will cache credentials on a per-image basis,
	// using the image passed from the kubelet directly as the cache key. This includes
	// the registry domain, port (if specified), and path but does not include tags or SHAs.
	//
	// 按仓库镜像缓存，不含 tag/SHA，同一仓库的多个 tag 共用凭据。
	ImagePluginCacheKeyType PluginCacheKeyType = "Image"
	// RegistryPluginCacheKeyType means the kubelet will cache credentials on a per-registry basis.
	// The cache key will be based on the registry domain and port (if present) parsed from the requested image.
	//
	// 凭据只与仓库相关时用此粒度，避免为同仓库的每个镜像重复调用插件。
	RegistryPluginCacheKeyType PluginCacheKeyType = "Registry"
	// GlobalPluginCacheKeyType means the kubelet will cache credentials for all images that
	// match for a given plugin. This cache key should only be returned by plugins that do not use
	// the image input at all.
	//
	// 与镜像无关的凭据才用此粒度，否则会在不同仓库之间误用。
	GlobalPluginCacheKeyType PluginCacheKeyType = "Global"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CredentialProviderResponse holds credentials that the kubelet should use for the specified
// image provided in the original request. Kubelet will read the response from the plugin via stdout.
// This response should be set to the same apiVersion as CredentialProviderRequest.
//
// 响应结构与 v1 相同，仍是 stdout 输出；保留独立类型只为兼容旧序列化路径。
type CredentialProviderResponse struct {
	metav1.TypeMeta `json:""`

	// cacheKeyType indiciates the type of caching key to use based on the image provided
	// in the request. There are three valid values for the cache key type: Image, Registry, and
	// Global. If an invalid value is specified, the response will NOT be used by the kubelet.
	//
	// 缓存粒度非法就废弃整份响应，不猜测插件意图。
	CacheKeyType PluginCacheKeyType `json:"cacheKeyType"`

	// cacheDuration indicates the duration the provided credentials should be cached for.
	// The kubelet will use this field to set the in-memory cache duration for credentials
	// in the AuthConfig. If null, the kubelet will use defaultCacheDuration provided in
	// CredentialProviderConfig. If set to 0, the kubelet will not cache the provided AuthConfig.
	//
	// 0 表示每次拉取都调用插件，适合短时效凭据；缺省则使用配置文件里的兜底时长。
	// +optional
	CacheDuration *metav1.Duration `json:"cacheDuration,omitempty"`

	// auth is a map containing authentication information passed into the kubelet.
	// Each key is a match image string (more on this below). The corresponding authConfig value
	// should be valid for all images that match against this key. A plugin should set
	// this field to null if no valid credentials can be returned for the requested image.
	//
	// Each key in the map is a pattern which can optionally contain a port and a path.
	// Globs can be used in the domain, but not in the port or the path. Globs are supported
	// as subdomains like '*.k8s.io' or 'k8s.*.io', and top-level-domains such as 'k8s.*'.
	// Matching partial subdomains like 'app*.k8s.io' is also supported. Each glob can only match
	// a single subdomain segment, so *.io does not match *.k8s.io.
	//
	// The kubelet will match images against the key when all of the below are true:
	// - Both contain the same number of domain parts and each part matches.
	// - The URL path of an imageMatch must be a prefix of the target image URL path.
	// - If the imageMatch contains a port, then the port must match in the image as well.
	//
	// When multiple keys are returned, the kubelet will traverse all keys in reverse order so that:
	// - longer keys come before shorter keys with the same prefix
	// - non-wildcard keys come before wildcard keys with the same prefix.
	//
	// For any given match, the kubelet will attempt an image pull with the provided credentials,
	// stopping after the first successfully authenticated pull.
	//
	// Example keys:
	//   - 123456789.dkr.ecr.us-east-1.amazonaws.com
	//   - *.azurecr.io
	//   - gcr.io
	//   - *.*.registry.io
	//   - registry.io:8080/path
	//
	// 多键匹配时优先尝试更具体（更长、非通配）的键，避免通用凭据盖掉特化凭据。
	// +optional
	Auth map[string]AuthConfig `json:"auth,omitempty"`
}

// AuthConfig contains authentication information for a container registry.
// Only username/password based authentication is supported today, but more authentication
// mechanisms may be added in the future.
//
// 空用户名/密码合法，表示匿名拉取或凭据由仓库侧的其他机制提供。
type AuthConfig struct {
	// username is the username used for authenticating to the container registry
	// An empty username is valid.
	Username string `json:"username"`

	// password is the password used for authenticating to the container registry
	// An empty password is valid.
	Password string `json:"password"`
}
