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

// Package negotiate 定义 flagz 端点的内容协商限制，
// 决定客户端可以请求哪些响应格式与结构化 GVK。
//
// 中文说明：协商规则是安全边界的一部分，必须保持白名单式收敛。
package negotiate

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// FlagzEndpointRestrictions implements content negotiation restrictions for the flagz endpoint.
// It is used to validate and restrict which GroupVersionKinds are allowed for structured responses.
//
// 中文说明：flagz 只提供两种形态——纯文本，或白名单内的结构化对象。白名单可以防止
// 客户端把请求协商成任意资源类型，从而复用序列化器做越权转换。
type FlagzEndpointRestrictions struct {
	RecognizedStructuredKinds map[schema.GroupVersionKind]bool
}

// AllowsMediaTypeTransform checks if the provided GVK is supported for structured flagz responses.
// 中文说明：text/plain 必须不带 GVK；其它媒体类型必须携带白名单内的 GVK，缺一不可。
func (f FlagzEndpointRestrictions) AllowsMediaTypeTransform(mimeType string, mimeSubType string, gvk *schema.GroupVersionKind) bool {
	if mimeType == "text" && mimeSubType == "plain" {
		return gvk == nil
	}
	if gvk != nil {
		return f.RecognizedStructuredKinds[*gvk]
	}
	return false
}

// AllowsServerVersion 返回 false：flagz 不支持通过 serverVersion 参数协商。
func (FlagzEndpointRestrictions) AllowsServerVersion(string) bool {
	return false
}

// AllowsStreamSchema 返回 false：flagz 不是流式端点。
func (FlagzEndpointRestrictions) AllowsStreamSchema(s string) bool {
	return false
}
