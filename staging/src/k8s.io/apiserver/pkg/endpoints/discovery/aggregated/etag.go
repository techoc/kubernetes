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

// Package aggregated 实现聚合发现（Aggregated Discovery）端点：
// 把多个 group/version 的发现信息合并成按 content-type 协商的文档，
// 并通过 ETag 支持客户端条件请求，减少集群规模下的重复传输。
package aggregated

import (
	"crypto/sha512"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
)

// This file exposes helper functions used for calculating the E-Tag header
// used in discovery endpoint responses

// Attaches Cache-Busting functionality to an endpoint
//   - Sets ETag header to provided hash
//   - Replies with 304 Not Modified, if If-None-Match header matches hash
//
// hash should be the value of calculateETag on object. If hash is empty, then
// the object is simply serialized without E-Tag functionality
// 注意：hash 为空时仍会写出空的 ETag 头，调用方需自行判断是否需要 ETag 语义。
func ServeHTTPWithETag(
	object runtime.Object,
	hash string,
	targetGV schema.GroupVersion,
	serializer runtime.NegotiatedSerializer,
	w http.ResponseWriter,
	req *http.Request,
) {
	// ETag must be enclosed in double quotes:
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag
	quotedHash := strconv.Quote(hash)
	w.Header().Set("ETag", quotedHash)
	// 响应内容随 Accept 协商而变化，因此缓存键必须包含 Accept，
	// 否则客户端可能复用错误内容类型的缓存。
	w.Header().Set("Vary", "Accept")
	w.Header().Set("Cache-Control", "public")

	// If Request includes If-None-Match and matches hash, reply with 304
	// Otherwise, we delegate to the handler for actual content
	//
	// According to documentation, An Etag within an If-None-Match
	// header will be enclosed within double quotes:
	// https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-None-Match#directives
	if clientCachedHash := req.Header.Get("If-None-Match"); quotedHash == clientCachedHash {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	responsewriters.WriteObjectNegotiated(
		serializer,
		DiscoveryEndpointRestrictions,
		targetGV,
		w,
		req,
		http.StatusOK,
		object,
		true,
	)
}

// calculateETag 对资源的 JSON 序列化结果取 SHA-512 摘要。
// 使用内容哈希而非版本号，保证只要发现内容不变，ETag 就稳定；
// JSON 序列化的字段顺序由 Go 结构体/编码器保证确定，因此哈希可复现。
func calculateETag(resources interface{}) (string, error) {
	serialized, err := json.Marshal(resources)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%X", sha512.Sum512(serialized)), nil
}
