/*
Copyright 2022 The Kubernetes Authors.

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

// 本文件定义 kubelet 与 exec 凭据插件之间的 v1 请求/响应协议：
// 请求走 stdin，响应走 stdout，插件回包必须使用与请求相同的 apiVersion，
// 否则 kubelet 无法按约定解码。

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CredentialProviderRequest includes the image that the kubelet requires authentication for.
// Kubelet will pass this request object to the plugin via stdin. In general, plugins should
// prefer responding with the same apiVersion they were sent.
//
// kubelet 通过 stdin 把请求交给插件，插件按同一版本回包，
// 才能让 kubelet 用同一套解码逻辑处理结果。
type CredentialProviderRequest struct {
	metav1.TypeMeta `json:""`

	// image is the container image that is being pulled as part of the
	// credential provider plugin request. Plugins may optionally parse the image
	// to extract any information required to fetch credentials.
	//
	// 镜像字符串是插件匹配与取凭据的主要输入，端口、路径都可能在其中。
	Image string `json:"image"`

	// serviceAccountToken is the service account token bound to the pod for which
	// the image is being pulled. This token is only sent to the plugin if the
	// tokenAttributes.serviceAccountTokenAudience field is configured in the kubelet's credential
	// provider configuration.
	//
	// 仅在配置里声明 audience 时才下发 token，避免把凭据交给不需要它的插件。
	ServiceAccountToken string `json:"serviceAccountToken,omitempty" datapolicy:"token"`

	// serviceAccountAnnotations is a map of annotations on the service account bound to the
	// pod for which the image is being pulled. The list of annotations in the service account
	// that need to be passed to the plugin is configured in the kubelet's credential provider
	// configuration.
	//
	// 注解按需传递而不是全量下发，减少插件可见的服务账号信息。
	ServiceAccountAnnotations map[string]string `json:"serviceAccountAnnotations,omitempty"`
}

// PluginCacheKeyType 决定凭据缓存的粒度：粒度越粗调用插件越少，
// 但凭据串用范围也越大，插件要按自己凭据的有效范围选择。
type PluginCacheKeyType string

const (
	// ImagePluginCacheKeyType means the kubelet will cache credentials on a per-image basis,
	// using the image passed from the kubelet directly as the cache key. This includes
	// the registry domain, port (if specified), and path but does not include tags or SHAs.
	//
	// 不含 tag/SHA 是有意的：同仓库不同 tag 通常共用同一套仓库凭据。
	ImagePluginCacheKeyType PluginCacheKeyType = "Image"
	// RegistryPluginCacheKeyType means the kubelet will cache credentials on a per-registry basis.
	// The cache key will be based on the registry domain and port (if present) parsed from the requested image.
	//
	// 凭据属于仓库而非具体镜像时选这个粒度，能显著减少插件调用次数。
	RegistryPluginCacheKeyType PluginCacheKeyType = "Registry"
	// GlobalPluginCacheKeyType means the kubelet will cache credentials for all images that
	// match for a given plugin. This cache key should only be returned by plugins that do not use
	// the image input at all.
	//
	// 只有与镜像完全无关的凭据（例如固定 token）才可用，否则会跨仓库误用。
	GlobalPluginCacheKeyType PluginCacheKeyType = "Global"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CredentialProviderResponse holds credentials that the kubelet should use for the specified
// image provided in the original request. Kubelet will read the response from the plugin via stdout.
// This response should be set to the same apiVersion as CredentialProviderRequest.
//
// 插件从 stdout 输出响应；版本不一致时 kubelet 会拒绝使用，防止按错误结构解释凭据。
type CredentialProviderResponse struct {
	metav1.TypeMeta `json:""`

	// cacheKeyType indiciates the type of caching key to use based on the image provided
	// in the request. There are three valid values for the cache key type: Image, Registry, and
	// Global. If an invalid value is specified, the response will NOT be used by the kubelet.
	//
	// 非法缓存粒度直接废弃整份响应，避免缓存策略不明确时错误地复用凭据。
	CacheKeyType PluginCacheKeyType `json:"cacheKeyType"`

	// cacheDuration indicates the duration the provided credentials should be cached for.
	// The kubelet will use this field to set the in-memory cache duration for credentials
	// in the AuthConfig. If null, the kubelet will use defaultCacheDuration provided in
	// CredentialProviderConfig. If set to 0, the kubelet will not cache the provided AuthConfig.
	//
	// 设为 0 表示凭据一次性使用：插件每次拉取都被调用，但换来更强的隔离性。
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
	// 多个键按“更长、更具体者先试”的顺序使用，允许插件同时给出通用与特化凭据。
	// +optional
	Auth map[string]AuthConfig `json:"auth,omitempty"`
}

// AuthConfig contains authentication information for a container registry.
// Only username/password based authentication is supported today, but more authentication
// mechanisms may be added in the future.
//
// 目前只支持用户名/密码，字段允许为空以便插件表达“无需认证”或匿名拉取。
type AuthConfig struct {
	// username is the username used for authenticating to the container registry
	// An empty username is valid.
	Username string `json:"username"`

	// password is the password used for authenticating to the container registry
	// An empty password is valid.
	Password string `json:"password"`
}
