/*
Copyright 2023 The Kubernetes Authors.

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

package v1beta1

import (
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
)

// addDefaultingFuncs 把默认值函数注册进 scheme，供解码时自动填充。
func addDefaultingFuncs(scheme *runtime.Scheme) error {
	return RegisterDefaults(scheme)
}

// SetDefaults_WebhookConfiguration 为结构化的 webhook 授权缓存配置填充默认值。
// TTL 只在零值时取默认，显式配置（包括负值）保持原样，由校验层拒绝非法值；
// 缓存开关默认开启，以降低高频授权检查对 webhook 的压力。
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
