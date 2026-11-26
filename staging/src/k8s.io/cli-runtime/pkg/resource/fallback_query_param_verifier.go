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

package resource

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
)

// fallbackQueryParamVerifier 封装主 Verifier 及其失败后的备用 Verifier。
type fallbackQueryParamVerifier struct {
	// primary 通常基于 OpenAPI v3；secondary 是旧版 OpenAPI v2 实现。
	primary   Verifier
	secondary Verifier
}

var _ Verifier = &fallbackQueryParamVerifier{}

// 新 Verifier 先调用主 Verifier；主 Verifier 返回 "NotFound" 时再调用备用 Verifier。
// 中文：用于兼容尚未提供 OpenAPI v3 的旧集群；构造时不影响既有行为，仅在主校验器失败时启用回退。
func NewFallbackQueryParamVerifier(primary Verifier, secondary Verifier) Verifier {
	return &fallbackQueryParamVerifier{
		primary:   primary,
		secondary: secondary,
	}
}

// 主、备 OpenAPI 端点都判定不支持 fieldValidation 时返回错误。
// 先查主端点；其 OpenAPI V3 文档获取失败时再查备端点。
// 支持该查询参数时返回 nil。
func (f *fallbackQueryParamVerifier) HasSupport(gvk schema.GroupVersionKind) error {
	err := f.primary.HasSupport(gvk)
	// 主端点报错时，改用备端点检查；
	// 但 "paramUnsupportedError" 不触发回退。
	// 中文：paramUnsupportedError 是确定性的“不支持”结论，回退也无法改变，故不再浪费一次请求。
	if err != nil && !IsParamUnsupportedError(err) {
		klog.V(7).Infof("openapi v3 error...falling back to legacy: %s", err)
		err = f.secondary.HasSupport(gvk)
	}
	return err
}
