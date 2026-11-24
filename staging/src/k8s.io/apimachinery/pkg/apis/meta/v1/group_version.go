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

package v1

import (
	"encoding/json"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupResource 表示 API 组和资源，不包含版本信息。查找阶段只需确定组与资源时使用它，
// 可避免为了暂时不需要的版本字段构造不完整的 GroupVersionResource。
//
// +protobuf.options.(gogoproto.goproto_stringer)=false
type GroupResource struct {
	Group    string `json:"group" protobuf:"bytes,1,opt,name=group"`
	Resource string `json:"resource" protobuf:"bytes,2,opt,name=resource"`
}

func (gr *GroupResource) String() string {
	if gr == nil {
		return "<nil>"
	}
	// 核心组为空字符串且属于隐式组，因此只输出资源名。
	// 非核心组采用 "resource.group" 格式，与 kubectl 的资源展示方式一致。
	if len(gr.Group) == 0 {
		return gr.Resource
	}
	return gr.Resource + "." + gr.Group
}

// GroupVersionResource 通过 group、version 和 resource 唯一标识一个资源。
// 它不匿名嵌入 GroupVersion，避免 Go 的隐式类型转换，也不直接复用 GroupVersion，
// 以便独立控制该类型的序列化行为。
//
// +protobuf.options.(gogoproto.goproto_stringer)=false
type GroupVersionResource struct {
	Group    string `json:"group" protobuf:"bytes,1,opt,name=group"`
	Version  string `json:"version" protobuf:"bytes,2,opt,name=version"`
	Resource string `json:"resource" protobuf:"bytes,3,opt,name=resource"`
}

func (gvr *GroupVersionResource) String() string {
	if gvr == nil {
		return "<nil>"
	}
	// 日志和错误消息使用的格式："group/version, Resource=name"。
	return strings.Join([]string{gvr.Group, "/", gvr.Version, ", Resource=", gvr.Resource}, "")
}

// GroupKind 表示 API 组和 Kind，不包含版本信息。查找阶段只需识别组与 Kind 时使用，
// 避免构造只有部分字段有效的 GroupVersionKind。
//
// +protobuf.options.(gogoproto.goproto_stringer)=false
type GroupKind struct {
	Group string `json:"group" protobuf:"bytes,1,opt,name=group"`
	Kind  string `json:"kind" protobuf:"bytes,2,opt,name=kind"`
}

func (gk *GroupKind) String() string {
	if gk == nil {
		return "<nil>"
	}
	// 可读字符串中省略隐式的核心组。
	if len(gk.Group) == 0 {
		return gk.Kind
	}
	return gk.Kind + "." + gk.Group
}

// GroupVersionKind 通过 group、version 和 kind 唯一标识一种 API 类型。
// 它不匿名嵌入 GroupVersion，以避免隐式类型转换，并保留独立的序列化行为。
//
// +protobuf.options.(gogoproto.goproto_stringer)=false
type GroupVersionKind struct {
	Group   string `json:"group" protobuf:"bytes,1,opt,name=group"`
	Version string `json:"version" protobuf:"bytes,2,opt,name=version"`
	Kind    string `json:"kind" protobuf:"bytes,3,opt,name=kind"`
}

// String 按 "group/version, Kind=name" 格式输出 Kind，供错误信息和日志使用。
// 接收者使用值类型，因此普通值和指针指向的值都可以直接调用。
func (gvk GroupVersionKind) String() string {
	return gvk.Group + "/" + gvk.Version + ", Kind=" + gvk.Kind
}

// GroupVersion 包含 group 和 version，用于唯一标识一个 API 版本。
//
// +protobuf.options.(gogoproto.goproto_stringer)=false
type GroupVersion struct {
	Group   string `json:"group" protobuf:"bytes,1,opt,name=group"`
	Version string `json:"version" protobuf:"bytes,2,opt,name=version"`
}

// Empty 判断 group 和 version 是否都为空。
func (gv GroupVersion) Empty() bool {
	return len(gv.Group) == 0 && len(gv.Version) == 0
}

// String 将 group 和 version 合并为 "group/version"。对于核心组的历史 v1 版本，
// 为保持兼容只返回 "v1"，而不是 "/v1"。
func (gv GroupVersion) String() string {
	// 内部 API 版本使用空字符串表示，保持其历史编码形式。
	if gv.Empty() {
		return ""
	}

	// 为向后兼容，核心组 v1 只写作 "v1"，而不是 "/v1"。
	if len(gv.Group) == 0 && gv.Version == "v1" {
		return gv.Version
	}
	if len(gv.Group) > 0 {
		return gv.Group + "/" + gv.Version
	}
	return gv.Version
}

// MarshalJSON implements the json.Marshaller interface.
func (gv GroupVersion) MarshalJSON() ([]byte, error) {
	s := gv.String()
	// GroupVersion 的格式最多包含一个斜杠；出现多个斜杠说明格式非法，
	// 应提前拒绝，避免生成无法解析的 apiVersion。
	if strings.Count(s, "/") > 1 {
		return []byte{}, fmt.Errorf("illegal GroupVersion %v: contains more than one /", s)
	}
	return json.Marshal(s)
}

// unmarshal 供 UnmarshalJSON 和 UnmarshalText 共用，确保两种入口接受相同的
// "group/version" 格式，也兼容核心组简写 "v1"。
func (gv *GroupVersion) unmarshal(value []byte) error {
	var s string
	if err := json.Unmarshal(value, &s); err != nil {
		return err
	}
	parsed, err := schema.ParseGroupVersion(s)
	if err != nil {
		return err
	}
	gv.Group, gv.Version = parsed.Group, parsed.Version
	return nil
}

// UnmarshalJSON implements the json.Unmarshaller interface.
func (gv *GroupVersion) UnmarshalJSON(value []byte) error {
	return gv.unmarshal(value)
}

// UnmarshalTEXT implements the Ugorji's encoding.TextUnmarshaler interface.
func (gv *GroupVersion) UnmarshalText(value []byte) error {
	return gv.unmarshal(value)
}
