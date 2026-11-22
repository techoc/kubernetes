/*
Copyright 2025 The Kubernetes Authors.

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

package negotiate

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// StatuszEndpointRestrictions implements content negotiation restrictions for statusz.
// It is used to validate and restrict which GroupVersionKinds are allowed for structured responses.
type StatuszEndpointRestrictions struct {
	// RecognizedStructuredKinds 是白名单：只有注册过的 GVK 才能用于结构化响应，
	// 防止客户端通过 Accept 头构造任意内部类型。
	RecognizedStructuredKinds map[schema.GroupVersionKind]bool
}

// AllowsMediaTypeTransform checks if the provided GVK is supported for structured statusz responses.
func (s StatuszEndpointRestrictions) AllowsMediaTypeTransform(mimeType string, mimeSubType string, gvk *schema.GroupVersionKind) bool {
	// text/plain 只用于非结构化输出，出现 GVK 说明请求自相矛盾。
	if mimeType == "text" && mimeSubType == "plain" {
		return gvk == nil
	}

	if gvk != nil {
		return s.RecognizedStructuredKinds[*gvk]
	}

	// 没有 GVK 的其它媒体类型不被支持。
	return false
}

// AllowsServerVersion 不支持 server 版本参数。
func (StatuszEndpointRestrictions) AllowsServerVersion(s string) bool {
	return false
}

// AllowsStreamSchema 不支持流式 schema（statusz 不提供 watch）。
func (StatuszEndpointRestrictions) AllowsStreamSchema(s string) bool {
	return false
}
