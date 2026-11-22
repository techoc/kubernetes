/*
Copyright 2015 The Kubernetes Authors.

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

package types

// Similarly to above, these are constants to support HTTP PATCH utilized by
// both the client and server that didn't make sense for a whole package to be
// dedicated to.
//
// PatchType 是 PATCH 请求的 Content-Type，取值必须与 API server 识别的内容
// 类型字符串完全一致；客户端与服务端共用这些常量以避免拼写漂移。
type PatchType string

const (
	JSONPatchType           PatchType = "application/json-patch+json"
	MergePatchType          PatchType = "application/merge-patch+json"
	StrategicMergePatchType PatchType = "application/strategic-merge-patch+json"
	// ApplyPatchType 保留旧名；实际值与 ApplyYAMLPatchType 相同。
	ApplyPatchType     PatchType = ApplyYAMLPatchType
	ApplyYAMLPatchType PatchType = "application/apply-patch+yaml"
	// ApplyCBORPatchType 是 apply patch 的 CBOR 编码形式，用于 SSA 的
	// 二进制传输。
	ApplyCBORPatchType PatchType = "application/apply-patch+cbor"
)
