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

// CredentialProviderRequest 包含 kubelet 需要凭据才能拉取的镜像。
// kubelet 通过 stdin 把请求传给插件。一般应
// 以收到请求时相同的 apiVersion 回复。
// kubelet 通过 stdin 把请求交给插件，插件按同一版本回包，
// 才能让 kubelet 用同一套解码逻辑处理结果。
type CredentialProviderRequest struct {
	metav1.TypeMeta `json:""`

	// image 是本次凭据请求中要拉取的容器镜像。
	// 插件可按需解析镜像名，
	// 提取获取凭据所需的信息。
	// 镜像字符串是插件匹配与取凭据的主要输入，端口、路径都可能在其中。
	Image string `json:"image"`

	// serviceAccountToken 是待拉取镜像的 Pod 所绑定的 ServiceAccount token。
	// 只有在 kubelet 凭据插件配置中设置了
	// tokenAttributes.serviceAccountTokenAudience 后才会传输。
	// 仅在配置里声明 audience 时才下发 token，避免把凭据交给不需要它的插件。
	ServiceAccountToken string `json:"serviceAccountToken,omitempty" datapolicy:"token"`

	// serviceAccountAnnotations 是待拉取镜像的 Pod 所绑定 ServiceAccount 的注解映射。
	// 需要传给插件的注解列表在 kubelet 的凭据插件配置中指定。
	// 即需传给插件的注解列表，由 kubelet 凭据插件配置指定。
	// 注解按需传递而不是全量下发，减少插件可见的服务账号信息。
	ServiceAccountAnnotations map[string]string `json:"serviceAccountAnnotations,omitempty"`
}

// PluginCacheKeyType 决定凭据缓存的粒度：粒度越粗调用插件越少，
// 但凭据串用范围也越大，插件要按自己凭据的有效范围选择。
type PluginCacheKeyType string

const (
	// ImagePluginCacheKeyType 表示 kubelet 按镜像缓存凭据，
	// 直接以传入的镜像名为键，包含 registry 域名、端口（若有）和路径，
	// 但不含 tag 或 SHA。
	// 不含 tag/SHA 是有意的：同仓库不同 tag 通常共用同一套仓库凭据。
	ImagePluginCacheKeyType PluginCacheKeyType = "Image"
	// RegistryPluginCacheKeyType 表示 kubelet 按 registry 缓存凭据。
	// 缓存键取自请求镜像中的 registry 域名与端口（若有）。
	// 凭据属于仓库而非具体镜像时选这个粒度，能显著减少插件调用次数。
	RegistryPluginCacheKeyType PluginCacheKeyType = "Registry"
	// GlobalPluginCacheKeyType 表示 kubelet 为该插件匹配的所有镜像
	// 共用一份凭据。只有不依赖镜像区分凭据的插件才应返回此类型。
	// 也不使用镜像信息。
	// 只有与镜像完全无关的凭据（例如固定 token）才可用，否则会跨仓库误用。
	GlobalPluginCacheKeyType PluginCacheKeyType = "Global"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CredentialProviderResponse 携带 kubelet 应为原请求中指定镜像使用的凭据。
// kubelet 通过 stdout 读取响应。
// 响应的 apiVersion 应与 CredentialProviderRequest 一致。
// 插件从 stdout 输出响应；版本不一致时 kubelet 会拒绝使用，防止按错误结构解释凭据。
type CredentialProviderResponse struct {
	metav1.TypeMeta `json:""`

	// cacheKeyType 指定按请求镜像使用哪种缓存键。
	// 缓存键类型只有三种合法值：Image、Registry、Global。
	// 非法缓存粒度直接废弃整份响应，避免缓存策略不明确时错误地复用凭据。
	CacheKeyType PluginCacheKeyType `json:"cacheKeyType"`

	// cacheDuration 是凭据的缓存时长。
	// kubelet 据此设置 AuthConfig 中凭据的内存缓存时长；
	// 为 null 时使用 CredentialProviderConfig 中的 defaultCacheDuration，
	// 设为 0 表示不缓存。
	// 设为 0 表示凭据一次性使用：插件每次拉取都被调用，但换来更强的隔离性。
	// +optional
	CacheDuration *metav1.Duration `json:"cacheDuration,omitempty"`

	// auth 是传给 kubelet 的认证信息映射。
	// 每个 key 是一个镜像匹配串（见下文），对应的 authConfig
	// 必须对该 key 匹配的所有镜像有效；无有效凭据时应
	// 把该字段设为 null。
	// 每个 key 是可含端口与路径的匹配模式。
	// 通配符只用于域名，不用于端口和路径；
	// 支持子域（如 '*.k8s.io'、'k8s.*.io'）和顶级域（如 'k8s.*'）。
	// 也支持部分子域匹配（如 'app*.k8s.io'）。每个通配符只匹配
	// 一段子域，因此 *.io 不匹配 *.k8s.io。
	// 满足以下全部条件时镜像才算匹配：
	// - 域名段数相同且逐段匹配；
	// - imageMatch 的 URL 路径必须是目标镜像路径的前缀；
	// - imageMatch 若含端口，镜像中也必须一致。
	// 返回多个 key 时 kubelet 逆序遍历，确保：
	// - 同前缀下更长的 key 优先；
	// - 同前缀下非通配符 key 优先。
	// 对任一匹配，kubelet 用其凭据尝试拉取镜像，
	// 首次鉴权成功后停止。
	// Example keys:
	//   - 123456789.dkr.ecr.us-east-1.amazonaws.com
	//   - *.azurecr.io
	//   - gcr.io
	//   - *.*.registry.io
	//   - registry.io:8080/path
	// 多个键按“更长、更具体者先试”的顺序使用，允许插件同时给出通用与特化凭据。
	// +optional
	Auth map[string]AuthConfig `json:"auth,omitempty"`
}

// AuthConfig 保存容器 registry 的认证信息。
// 目前只支持用户名/密码认证，未来可能
// 增加更多方式。
// 目前只支持用户名/密码，字段允许为空以便插件表达“无需认证”或匿名拉取。
type AuthConfig struct {
	// username 是认证 registry 所用的用户名。
	// 空用户名合法。
	Username string `json:"username"`

	// password 是认证 registry 所用的密码。
	// 空密码合法。
	Password string `json:"password"`
}
