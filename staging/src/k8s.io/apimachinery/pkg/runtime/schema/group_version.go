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

// ParseResourceArg 解析常见的 resource.group.com 或 resource.version.group.com 格式，
// 同时返回带版本和不带版本的两种解释。该函数不判断调用方原本想表达哪一种，
// 调用方可结合已知 GroupVersion 集合进行判断；只有两段时，GroupVersionResource 返回 nil。
// 例如 resource.group.com 可解释为 group=com、version=group、resource=resource，
// 也可解释为 group=group.com、resource=resource。
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

// ParseKindArg 解析 Kind.group.com 或 Kind.version.group.com 格式，并返回带版本和不带版本的两种解释。
// 函数本身不判断哪种解释才是调用方的真实意图，调用方可结合已知 GroupKind 集合进行判断；
// 只有两段时，GroupVersionKind 返回 nil。
// `Kind.group.com` 会被解析为 `group=com, version=group, kind=Kind` 或
// `group=group.com, kind=Kind`，取决于点分段如何切分。
func ParseKindArg(arg string) (*GroupVersionKind, GroupKind) {
	var gvk *GroupVersionKind
	if strings.Count(arg, ".") >= 2 {
		s := strings.SplitN(arg, ".", 3)
		gvk = &GroupVersionKind{Group: s[2], Version: s[1], Kind: s[0]}
	}

	return gvk, ParseGroupKind(arg)
}

// GroupResource 标识 API 组和资源，但不绑定具体版本。查找阶段可先使用它标识资源，
// 避免构造一个版本信息尚不完整的 GroupVersionResource。
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

// ParseGroupResource 将 resource.group 字符串解析为 GroupResource；组名和资源名都允许为空。
func ParseGroupResource(gr string) GroupResource {
	if i := strings.Index(gr, "."); i >= 0 {
		return GroupResource{Group: gr[i+1:], Resource: gr[:i]}
	}
	return GroupResource{Resource: gr}
}

// GroupVersionResource 使用 group、version、resource 唯一标识一个资源。它不匿名嵌入 GroupVersion，
// 以避免 Go 自动类型转换；同时直接保存字段，避免引入额外的自定义编组行为。
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

// GroupKind 标识 API 组和 Kind，但不绑定具体版本。查找阶段可先用它表示尚未确定版本的类型。
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

// GroupVersionKind 使用 group、version、kind 唯一标识一种 API 类型。它不匿名嵌入 GroupVersion，
// 以避免 Go 自动类型转换；同时直接保存字段，避免引入额外的自定义编组行为。
type GroupVersionKind struct {
	Group   string
	Version string
	Kind    string
}

// Empty 判断 group、version 和 kind 是否都为空。
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

// GroupVersion 保存 API 组和版本，两者共同确定一个 API 版本。
type GroupVersion struct {
	Group   string
	Version string
}

// Empty 判断 group 和 version 是否都为空。
func (gv GroupVersion) Empty() bool {
	return len(gv.Group) == 0 && len(gv.Version) == 0
}

// String 将 group 和 version 合并为 group/version 字符串；对于历史遗留的 core v1，结果仅为 v1。
func (gv GroupVersion) String() string {
	if len(gv.Group) > 0 {
		return gv.Group + "/" + gv.Version
	}
	return gv.Version
}

// Identifier 实现 runtime.GroupVersioner 接口，返回该 GroupVersion 的稳定标识。
func (gv GroupVersion) Identifier() string {
	return gv.String()
}

// KindForGroupVersionKinds 从候选列表中选择最合适的 GroupVersionKind。没有候选项匹配当前 group 时返回 false；
// 同时匹配 group 和 version 的候选项优先级高于仅匹配 group 的候选项。
// TODO：由于 Scheme 也使用 GroupVersion，后续可考虑将其移动到 pkg/runtime 下的独立包。
// TODO：可在 GroupVersion 与 runtime.GroupVersioner 之间引入适配类型，减少直接使用 LegacyCodec(GroupVersion) 的位置。
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

// ParseGroupVersion 将 group/version 字符串解析为 GroupVersion；格式不合法时返回错误。
func ParseGroupVersion(gv string) (GroupVersion, error) {
	// 这里可能表示历史 Kubernetes 类型使用的内部版本。
	// TODO：清理完所有以字符串形式使用该值的旧代码后，应移除此特殊分支。
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

// WithKind 使用接收者中的 group/version 和传入的 kind 构造 GroupVersionKind。
func (gv GroupVersion) WithKind(kind string) GroupVersionKind {
	return GroupVersionKind{Group: gv.Group, Version: gv.Version, Kind: kind}
}

// WithResource 使用接收者中的 group/version 和传入的 resource 构造 GroupVersionResource。
func (gv GroupVersion) WithResource(resource string) GroupVersionResource {
	return GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: resource}
}

// GroupVersions 表示一组期望匹配的 API 组版本。
// TODO：由于 Scheme 也使用 GroupVersions，后续可考虑将其移动到 pkg/runtime 下的独立包。
// TODO：可在 GroupVersions 与 runtime.GroupVersioner 之间引入适配类型，减少直接使用 LegacyCodec(GroupVersion) 的位置。
type GroupVersions []GroupVersion

// Identifier 实现 runtime.GroupVersioner 接口，返回该 GroupVersion 的稳定标识。
func (gvs GroupVersions) Identifier() string {
	groupVersions := make([]string, 0, len(gvs))
	for i := range gvs {
		groupVersions = append(groupVersions, gvs[i].String())
	}
	return fmt.Sprintf("[%s]", strings.Join(groupVersions, ","))
}

// KindForGroupVersionKinds 从候选列表中选择与这组期望版本最匹配的 GroupVersionKind；
// 没有任何候选项匹配时返回 false。
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

// bestMatch 尝试从候选项中找出与输入列表完全一致的 GroupVersionKind；
// 如果不存在完全匹配项，则退回到 targets 中的第一个候选项。
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

// ToAPIVersionAndKind 为未内嵌 TypeMeta 的类型提供便捷方法，以满足 runtime.Object 所需的类型元信息读取。
func (gvk GroupVersionKind) ToAPIVersionAndKind() (string, string) {
	if gvk.Empty() {
		return "", ""
	}
	return gvk.GroupVersion().String(), gvk.Kind
}

// FromAPIVersionAndKind 为未使用 TypeMeta 的类型，根据传入字段构造 GVK。
// 该方法主要兼容测试类型，以及 group 与 kind 分开存储的历史序列化格式。
// TODO：继续减少该方法的调用点。
func FromAPIVersionAndKind(apiVersion, kind string) GroupVersionKind {
	// 解析失败时退化为只有 kind，保持宽松以兼容历史数据。
	if gv, err := ParseGroupVersion(apiVersion); err == nil {
		return GroupVersionKind{Group: gv.Group, Version: gv.Version, Kind: kind}
	}
	return GroupVersionKind{Kind: kind}
}
