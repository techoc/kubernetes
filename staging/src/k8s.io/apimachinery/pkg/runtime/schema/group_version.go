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

// Package schema 定义 API 的 group/version/kind 标识及其解析、格式化工具。
//
// 这些类型是 client-go、apiserver 与 CRD 生态共享的“坐标系统”：它们不携带
// 任何业务语义，只保证标识的解析与比较行为一致。
package schema

import (
	"fmt"
	"strings"
)

// ParseResourceArg takes the common style of string which may be either `resource.group.com` or `resource.version.group.com`
// and parses it out into both possibilities.  This code takes no responsibility for knowing which representation was intended
// but with a knowledge of all GroupVersions, calling code can take a very good guess.  If there are only two segments, then
// `*GroupVersionResource` is nil.
// `resource.group.com` -> `group=com, version=group, resource=resource` and `group=group.com, resource=resource`
//
// 返回两种可能解释，由调用方结合已知 GroupVersion 选择；少于三段时无法
// 区分 version，GVR 为 nil。
func ParseResourceArg(arg string) (*GroupVersionResource, GroupResource) {
	var gvr *GroupVersionResource
	// 至少三段（resource.version.group...）才可能构成完整 GVR。
	if strings.Count(arg, ".") >= 2 {
		s := strings.SplitN(arg, ".", 3)
		gvr = &GroupVersionResource{Group: s[2], Version: s[1], Resource: s[0]}
	}

	return gvr, ParseGroupResource(arg)
}

// ParseKindArg takes the common style of string which may be either `Kind.group.com` or `Kind.version.group.com`
// and parses it out into both possibilities. This code takes no responsibility for knowing which representation was intended
// but with a knowledge of all GroupKinds, calling code can take a very good guess. If there are only two segments, then
// `*GroupVersionKind` is nil.
// `Kind.group.com` -> `group=com, version=group, kind=Kind` and `group=group.com, kind=Kind`
func ParseKindArg(arg string) (*GroupVersionKind, GroupKind) {
	var gvk *GroupVersionKind
	if strings.Count(arg, ".") >= 2 {
		s := strings.SplitN(arg, ".", 3)
		gvk = &GroupVersionKind{Group: s[2], Version: s[1], Kind: s[0]}
	}

	return gvk, ParseGroupKind(arg)
}

// GroupResource specifies a Group and a Resource, but does not force a version.  This is useful for identifying
// concepts during lookup stages without having partially valid types
type GroupResource struct {
	Group    string
	Resource string
}

// WithVersion 补全版本，得到完整的 GroupVersionResource。
func (gr GroupResource) WithVersion(version string) GroupVersionResource {
	return GroupVersionResource{Group: gr.Group, Version: version, Resource: gr.Resource}
}

// Empty 判断 group 与 resource 是否都为空。
func (gr GroupResource) Empty() bool {
	return len(gr.Group) == 0 && len(gr.Resource) == 0
}

// String 输出 "resource.group"；core group 只输出 resource。
func (gr GroupResource) String() string {
	if len(gr.Group) == 0 {
		return gr.Resource
	}
	return gr.Resource + "." + gr.Group
}

// ParseGroupKind 解析 "kind.group"；没有点时整个字符串作为 kind（core group）。
func ParseGroupKind(gk string) GroupKind {
	i := strings.Index(gk, ".")
	if i == -1 {
		return GroupKind{Kind: gk}
	}

	return GroupKind{Group: gk[i+1:], Kind: gk[:i]}
}

// ParseGroupResource turns "resource.group" string into a GroupResource struct.  Empty strings are allowed
// for each field.
func ParseGroupResource(gr string) GroupResource {
	if i := strings.Index(gr, "."); i >= 0 {
		return GroupResource{Group: gr[i+1:], Resource: gr[:i]}
	}
	return GroupResource{Resource: gr}
}

// GroupVersionResource unambiguously identifies a resource.  It doesn't anonymously include GroupVersion
// to avoid automatic coercion.  It doesn't use a GroupVersion to avoid custom marshalling
type GroupVersionResource struct {
	Group    string
	Version  string
	Resource string
}

// Empty 判断三个字段是否都为空。
func (gvr GroupVersionResource) Empty() bool {
	return len(gvr.Group) == 0 && len(gvr.Version) == 0 && len(gvr.Resource) == 0
}

// GroupResource 丢弃版本信息。
func (gvr GroupVersionResource) GroupResource() GroupResource {
	return GroupResource{Group: gvr.Group, Resource: gvr.Resource}
}

// GroupVersion 丢弃资源名，只保留 group/version。
func (gvr GroupVersionResource) GroupVersion() GroupVersion {
	return GroupVersion{Group: gvr.Group, Version: gvr.Version}
}

// String 输出 "group/version, Resource=resource" 形式，便于日志与错误信息。
func (gvr GroupVersionResource) String() string {
	return strings.Join([]string{gvr.Group, "/", gvr.Version, ", Resource=", gvr.Resource}, "")
}

// GroupKind specifies a Group and a Kind, but does not force a version.  This is useful for identifying
// concepts during lookup stages without having partially valid types
type GroupKind struct {
	Group string
	Kind  string
}

// Empty 判断 group 与 kind 是否都为空。
func (gk GroupKind) Empty() bool {
	return len(gk.Group) == 0 && len(gk.Kind) == 0
}

// WithVersion 补全版本，得到完整的 GroupVersionKind。
func (gk GroupKind) WithVersion(version string) GroupVersionKind {
	return GroupVersionKind{Group: gk.Group, Version: version, Kind: gk.Kind}
}

// String 输出 "kind.group"；core group 只输出 kind。
func (gk GroupKind) String() string {
	if len(gk.Group) == 0 {
		return gk.Kind
	}
	return gk.Kind + "." + gk.Group
}

// GroupVersionKind unambiguously identifies a kind.  It doesn't anonymously include GroupVersion
// to avoid automatic coercion.  It doesn't use a GroupVersion to avoid custom marshalling
type GroupVersionKind struct {
	Group   string
	Version string
	Kind    string
}

// Empty returns true if group, version, and kind are empty
func (gvk GroupVersionKind) Empty() bool {
	return len(gvk.Group) == 0 && len(gvk.Version) == 0 && len(gvk.Kind) == 0
}

// GroupKind 丢弃版本信息。
func (gvk GroupVersionKind) GroupKind() GroupKind {
	return GroupKind{Group: gvk.Group, Kind: gvk.Kind}
}

// GroupVersion 丢弃 kind，只保留 group/version。
func (gvk GroupVersionKind) GroupVersion() GroupVersion {
	return GroupVersion{Group: gvk.Group, Version: gvk.Version}
}

// String 输出 "group/version, Kind=kind" 形式。
func (gvk GroupVersionKind) String() string {
	return gvk.Group + "/" + gvk.Version + ", Kind=" + gvk.Kind
}

// GroupVersion contains the "group" and the "version", which uniquely identifies the API.
type GroupVersion struct {
	Group   string
	Version string
}

// Empty returns true if group and version are empty
func (gv GroupVersion) Empty() bool {
	return len(gv.Group) == 0 && len(gv.Version) == 0
}

// String puts "group" and "version" into a single "group/version" string. For the legacy v1
// it returns "v1".
func (gv GroupVersion) String() string {
	if len(gv.Group) > 0 {
		return gv.Group + "/" + gv.Version
	}
	return gv.Version
}

// Identifier implements runtime.GroupVersioner interface.
func (gv GroupVersion) Identifier() string {
	return gv.String()
}

// KindForGroupVersionKinds identifies the preferred GroupVersionKind out of a list. It returns ok false
// if none of the options match the group. It prefers a match to group and version over just group.
// TODO: Move GroupVersion to a package under pkg/runtime, since it's used by scheme.
// TODO: Introduce an adapter type between GroupVersion and runtime.GroupVersioner, and use LegacyCodec(GroupVersion)
// in fewer places.
func (gv GroupVersion) KindForGroupVersionKinds(kinds []GroupVersionKind) (target GroupVersionKind, ok bool) {
	// 第一轮要求 group 与 version 都匹配，精确优先。
	for _, gvk := range kinds {
		if gvk.Group == gv.Group && gvk.Version == gv.Version {
			return gvk, true
		}
	}
	// 第二轮只要求 group 匹配，version 采用目标版本。
	for _, gvk := range kinds {
		if gvk.Group == gv.Group {
			return gv.WithKind(gvk.Kind), true
		}
	}
	return GroupVersionKind{}, false
}

// ParseGroupVersion turns "group/version" string into a GroupVersion struct. It reports error
// if it cannot parse the string.
func ParseGroupVersion(gv string) (GroupVersion, error) {
	// this can be the internal version for the legacy kube types
	// TODO once we've cleared the last uses as strings, this special case should be removed.
	// 空串与 "/" 都表示“无版本”，是历史遗留的表示法。
	if (len(gv) == 0) || (gv == "/") {
		return GroupVersion{}, nil
	}

	switch strings.Count(gv, "/") {
	case 0:
		// 无斜杠：只有 version，属于 core group。
		return GroupVersion{"", gv}, nil
	case 1:
		i := strings.Index(gv, "/")
		return GroupVersion{gv[:i], gv[i+1:]}, nil
	default:
		// 多于一个斜杠是非法输入（group 中不允许斜杠）。
		return GroupVersion{}, fmt.Errorf("unexpected GroupVersion string: %v", gv)
	}
}

// WithKind creates a GroupVersionKind based on the method receiver's GroupVersion and the passed Kind.
func (gv GroupVersion) WithKind(kind string) GroupVersionKind {
	return GroupVersionKind{Group: gv.Group, Version: gv.Version, Kind: kind}
}

// WithResource creates a GroupVersionResource based on the method receiver's GroupVersion and the passed Resource.
func (gv GroupVersion) WithResource(resource string) GroupVersionResource {
	return GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: resource}
}

// GroupVersions can be used to represent a set of desired group versions.
// TODO: Move GroupVersions to a package under pkg/runtime, since it's used by scheme.
// TODO: Introduce an adapter type between GroupVersions and runtime.GroupVersioner, and use LegacyCodec(GroupVersion)
// in fewer places.
type GroupVersions []GroupVersion

// Identifier implements runtime.GroupVersioner interface.
func (gvs GroupVersions) Identifier() string {
	groupVersions := make([]string, 0, len(gvs))
	for i := range gvs {
		groupVersions = append(groupVersions, gvs[i].String())
	}
	return fmt.Sprintf("[%s]", strings.Join(groupVersions, ","))
}

// KindForGroupVersionKinds identifies the preferred GroupVersionKind out of a list. It returns ok false
// if none of the options match the group.
func (gvs GroupVersions) KindForGroupVersionKinds(kinds []GroupVersionKind) (GroupVersionKind, bool) {
	var targets []GroupVersionKind
	for _, gv := range gvs {
		target, ok := gv.KindForGroupVersionKinds(kinds)
		if !ok {
			continue
		}
		targets = append(targets, target)
	}
	if len(targets) == 1 {
		return targets[0], true
	}
	if len(targets) > 1 {
		// 多个候选时优先返回与输入完全一致的那个，否则取第一个。
		return bestMatch(kinds, targets), true
	}
	return GroupVersionKind{}, false
}

// bestMatch tries to pick best matching GroupVersionKind and falls back to the first
// found if no exact match exists.
func bestMatch(kinds []GroupVersionKind, targets []GroupVersionKind) GroupVersionKind {
	for _, gvk := range targets {
		for _, k := range kinds {
			if k == gvk {
				return k
			}
		}
	}
	return targets[0]
}

// ToAPIVersionAndKind is a convenience method for satisfying runtime.Object on types that
// do not use TypeMeta.
func (gvk GroupVersionKind) ToAPIVersionAndKind() (string, string) {
	if gvk.Empty() {
		return "", ""
	}
	return gvk.GroupVersion().String(), gvk.Kind
}

// FromAPIVersionAndKind returns a GVK representing the provided fields for types that
// do not use TypeMeta. This method exists to support test types and legacy serializations
// that have a distinct group and kind.
// TODO: further reduce usage of this method.
func FromAPIVersionAndKind(apiVersion, kind string) GroupVersionKind {
	// 解析失败时退化为只有 kind，保持宽松以兼容历史数据。
	if gv, err := ParseGroupVersion(apiVersion); err == nil {
		return GroupVersionKind{Group: gv.Group, Version: gv.Version, Kind: kind}
	}
	return GroupVersionKind{Kind: kind}
}
