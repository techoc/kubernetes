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

package aggregated

import (
	"k8s.io/apimachinery/pkg/runtime/schema"

	genericfeatures "k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
)

// 以下接口来自 "k8s.io/apiserver/pkg/endpoints/handlers/negotiation"

// DiscoveryEndpointRestrictions 允许请求在 /apis 上为聚合发现提供内容协商 GVK。
//
// 发现端点通常只允许按 Accept 头返回固定格式；这里额外放行 aggregated discovery
// 的 GVK，使客户端可以通过 Accept 参数显式请求聚合发现文档。
var DiscoveryEndpointRestrictions = discoveryEndpointRestrictions{}

// discoveryEndpointRestrictions 是 negotiation.Restrictions 的无状态实现。
type discoveryEndpointRestrictions struct{}

// AllowsMediaTypeTransform 仅允许转换为聚合发现的 GVK，其他转换一律拒绝。
func (discoveryEndpointRestrictions) AllowsMediaTypeTransform(mimeType string, mimeSubType string, gvk *schema.GroupVersionKind) bool {
	return IsAggregatedDiscoveryGVK(gvk)
}

// AllowsServerVersion 发现端点不提供版本转换。
func (discoveryEndpointRestrictions) AllowsServerVersion(string) bool { return false }

// AllowsStreamSchema 仅允许 watch 流。
func (discoveryEndpointRestrictions) AllowsStreamSchema(s string) bool { return s == "watch" }

// IsAggregatedDiscoveryGVK 检查给定的 GVK 是否为用于提供聚合发现的 GVK。
// 特性门控 AggregatedDiscoveryRemoveBetaType 开启后不再接受 v2beta1，用于推动客户端迁移。
func IsAggregatedDiscoveryGVK(gvk *schema.GroupVersionKind) bool {
	if gvk != nil {
		if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.AggregatedDiscoveryRemoveBetaType) {
			return gvk.Group == "apidiscovery.k8s.io" && gvk.Version == "v2" && gvk.Kind == "APIGroupDiscoveryList"
		}
		return gvk.Group == "apidiscovery.k8s.io" && (gvk.Version == "v2beta1" || gvk.Version == "v2") && gvk.Kind == "APIGroupDiscoveryList"
	}
	return false
}
