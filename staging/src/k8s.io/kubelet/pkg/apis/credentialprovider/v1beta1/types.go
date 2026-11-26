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

// 本包是镜像凭证插件 API 的 beta 版本，通过 stdin/stdout 与 exec 插件交换 JSON。
// 相比内部版本，此版本不含 ServiceAccountToken 等字段，以兼容老插件。
package v1beta1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CredentialProviderRequest 包含 kubelet 需要凭据才能拉取的镜像。
// kubelet 通过 stdin 把请求传给插件。一般应
// 以收到请求时相同的 apiVersion 回复。
// CredentialProviderRequest 是 beta 版本的请求结构，字段集是内部版本的子集；
// 插件按 apiVersion 字段即可判断必须遵循的响应格式。
type CredentialProviderRequest struct {
	metav1.TypeMeta `json:""`

	// image 是本次凭据请求中要拉取的容器镜像。
	// 插件可按需解析镜像名，
	// 提取获取凭据所需的信息。
	Image string `json:"image"`
}

// PluginCacheKeyType 决定凭证的缓存粒度；三档从细到粗排列，
// 插件应选择与其凭证适用范围一致的一档，避免把凭证误用于其他镜像。
type PluginCacheKeyType string

const (
	// ImagePluginCacheKeyType 表示 kubelet 按镜像缓存凭据，
	// 直接以传入的镜像名为键，包含 registry 域名、端口（若有）和路径，
	// 但不含 tag 或 SHA。
	ImagePluginCacheKeyType PluginCacheKeyType = "Image"
	// RegistryPluginCacheKeyType 表示 kubelet 按 registry 缓存凭据。
	// 缓存键取自请求镜像中的 registry 域名与端口（若有）。
	RegistryPluginCacheKeyType PluginCacheKeyType = "Registry"
	// GlobalPluginCacheKeyType 表示 kubelet 为该插件匹配的所有镜像
	// 共用一份凭据。只有不依赖镜像区分凭据的插件才应返回此类型。
	// 也不使用镜像信息。
	GlobalPluginCacheKeyType PluginCacheKeyType = "Global"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CredentialProviderResponse 携带 kubelet 应为原请求中指定镜像使用的凭据。
// kubelet 通过 stdout 读取响应。
// 响应的 apiVersion 应与 CredentialProviderRequest 一致。
// CredentialProviderResponse 在 beta 版本中只携带镜像匹配的凭证；
// 缓存时长为空时由 kubelet 配置中的默认值兜底。
type CredentialProviderResponse struct {
	metav1.TypeMeta `json:""`

	// cacheKeyType 指定按请求镜像使用哪种缓存键。
	// 缓存键类型只有三种合法值：Image、Registry、Global。
	//
	CacheKeyType PluginCacheKeyType `json:"cacheKeyType"`

	// cacheDuration 是凭据的缓存时长。
	// kubelet 据此设置 AuthConfig 中凭据的内存缓存时长；
	// 为 null 时使用 CredentialProviderConfig 中的 defaultCacheDuration，
	// 设为 0 表示不缓存。
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
	// +optional
	Auth map[string]AuthConfig `json:"auth,omitempty"`
}

// AuthConfig 保存容器 registry 的认证信息。
// 目前只支持用户名/密码认证，未来可能
// 增加更多方式。
// AuthConfig 只描述用户名/密码一种认证方式；
// 空用户名/密码合法，表示匿名拉取或由插件自行处理。
type AuthConfig struct {
	// username 是认证 registry 所用的用户名。
	// 空用户名合法。
	Username string `json:"username"`

	// password 是认证 registry 所用的密码。
	// 空密码合法。
	Password string `json:"password"`
}
