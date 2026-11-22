/*
Copyright 2019 The Kubernetes Authors.

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

package v1

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
)

// 默认值集中在此处定义，避免每个字段的默认逻辑分散；这些值属于对外兼容契约的一部分。
var (
	defaultTimeout          = &metav1.Duration{Duration: 3 * time.Second}
	defaultCacheSize  int32 = 1000
	defaultAPIVersion       = "v1"
)

// addDefaultingFuncs 把默认值函数注册进 scheme，由 scheme 在解码后统一调用。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_KMSConfiguration applies defaults to KMSConfiguration.
// KMS 调用超时默认 3 秒；未指定 APIVersion 时按 v1 处理。
func SetDefaults_KMSConfiguration(obj *KMSConfiguration) {
	if obj.Timeout == nil {
		obj.Timeout = defaultTimeout
	}

	if obj.APIVersion == "" {
		obj.APIVersion = defaultAPIVersion
	}

	// cacheSize is relevant only for kms v1
	// cacheSize 仅 KMS v1 使用，v2 由 KMS 插件自身缓存，故不设置默认值。
	if obj.CacheSize == nil && obj.APIVersion == "v1" {
		obj.CacheSize = &defaultCacheSize
	}
}

// SetDefaults_WebhookConfiguration 为授权 webhook 配置填充默认值。
// 默认缓存已授权结果 5 分钟、未授权结果 30 秒，两者都启用缓存：
// 授权结果变化较慢，未授权结果变化较快，短 TTL 可限制错误缓存的负面影响。
func SetDefaults_WebhookConfiguration(obj *WebhookConfiguration) {
	if obj.AuthorizedTTL.Duration == 0 {
		obj.AuthorizedTTL.Duration = 5 * time.Minute
	}
	if obj.CacheAuthorizedRequests == nil {
		obj.CacheAuthorizedRequests = ptr.To(true)
	}
	if obj.UnauthorizedTTL.Duration == 0 {
		obj.UnauthorizedTTL.Duration = 30 * time.Second
	}
	if obj.CacheUnauthorizedRequests == nil {
		obj.CacheUnauthorizedRequests = ptr.To(true)
	}
}
