/*
Copyright 2024 The Kubernetes Authors.

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

// Package apply 帮助构造 server-side apply 请求。
//
// 编码格式由特性门控决定：默认 JSON（即 apply-patch+yaml 语义），同时开启
// ClientsAllowCBOR 与 ClientsPreferCBOR 时改用 CBOR。
package apply

import (
	"fmt"

	cbor "k8s.io/apimachinery/pkg/runtime/serializer/cbor/direct"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/json"
	"k8s.io/client-go/features"
	"k8s.io/client-go/rest"
)

// NewRequest 构造服务端 Apply 请求。apply configuration 对象用默认编码写入请求体，
// Content-Type 设为带结构化语法后缀（目前是 +yaml 或 +cbor）的 application/apply-patch，
// 详见
// https://www.iana.org/assignments/media-type-structured-suffix/media-type-structured-suffix.xhtml）。
// NewRequest 构造服务端 Apply 请求。apply configuration 对象用默认编码写入请求体，
// Content-Type 设为带结构化语法后缀（目前是 +yaml 或 +cbor）的 application/apply-patch，
// 详见
// https://www.iana.org/assignments/media-type-structured-suffix/media-type-structured-suffix.xhtml）。
func NewRequest(client rest.Interface, applyConfiguration interface{}) (*rest.Request, error) {
	// 默认沿用 YAML 后缀以保持与历史行为兼容：JSON 是 YAML 的子集，
	// 服务端按 apply-patch+yaml 解析即可。
	pt := types.ApplyYAMLPatchType
	marshal := json.Marshal

	// 必须两个门控同时开启才切 CBOR：Allow 表示客户端有能力，Prefer 表示
	// 默认就想用；只有 Prefer 而无 Allow 时仍走 JSON。
	if features.FeatureGates().Enabled(features.ClientsAllowCBOR) && features.FeatureGates().Enabled(features.ClientsPreferCBOR) {
		pt = types.ApplyCBORPatchType
		marshal = cbor.Marshal
	}

	body, err := marshal(applyConfiguration)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal apply configuration: %w", err)
	}

	return client.Patch(pt).Body(body), nil
}
