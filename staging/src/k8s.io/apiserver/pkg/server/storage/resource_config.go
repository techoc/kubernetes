/*
Copyright 2016 The Kubernetes Authors.

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

package storage

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// APIResourceConfigSource is the interface to determine which groups and versions are enabled
//
// 区分"是否启用"与"是否被显式设置"：前者决定行为，后者用于判断用户配置是否
// 覆盖了默认值（例如决定是否打印提示或参与冲突检测）。
type APIResourceConfigSource interface {
	ResourceEnabled(resource schema.GroupVersionResource) bool
	AnyResourceForGroupEnabled(group string) bool
	ResourceExplicitlyEnabled(resource schema.GroupVersionResource) bool
	VersionExplicitlyEnabled(version schema.GroupVersion) bool
}

var _ APIResourceConfigSource = &ResourceConfig{}

// ResourceConfig 记录版本级与资源级的启停状态。
//
// 两级配置存在优先级：资源级设置覆盖版本级设置。因此任何改变版本级状态的调用
// 都必须清理该版本下的资源级偏好，否则旧覆盖会继续生效，产生"配置了却没生效"的困惑。
// 这也是下面每个 Enable/Disable 都要调用 removeMatchingResourcePreferences 的原因。
type ResourceConfig struct {
	// GroupVersionConfigs 是版本级的生效值；ResourceConfigs 是资源级的生效值。
	GroupVersionConfigs map[schema.GroupVersion]bool
	ResourceConfigs     map[schema.GroupVersionResource]bool
	// Explicit* 记录"由用户/配置显式设置"的条目，用于区分默认值与显式值。
	ExplicitGroupVersionConfigs map[schema.GroupVersion]bool
	ExplicitResourceConfigs     map[schema.GroupVersionResource]bool
}

// NewResourceConfig 创建空的配置；所有 map 都必须初始化，否则写入会 panic。
func NewResourceConfig() *ResourceConfig {
	return &ResourceConfig{
		GroupVersionConfigs:         map[schema.GroupVersion]bool{},
		ResourceConfigs:             map[schema.GroupVersionResource]bool{},
		ExplicitGroupVersionConfigs: map[schema.GroupVersion]bool{},
		ExplicitResourceConfigs:     map[schema.GroupVersionResource]bool{},
	}
}

// DisableMatchingVersions disables all group/versions for which the matcher function returns true.
// This will remove any preferences previously set on individual resources.
func (o *ResourceConfig) DisableMatchingVersions(matcher func(gv schema.GroupVersion) bool) {
	for version := range o.GroupVersionConfigs {
		if matcher(version) {
			o.GroupVersionConfigs[version] = false
			// 版本级变更优先于旧的资源级偏好，必须清除，否则覆盖仍然生效。
			o.removeMatchingResourcePreferences(resourceMatcherForVersion(version))
		}
	}
}

// EnableMatchingVersions enables all group/versions for which the matcher function returns true.
// This will remove any preferences previously set on individual resources.
func (o *ResourceConfig) EnableMatchingVersions(matcher func(gv schema.GroupVersion) bool) {
	for version := range o.GroupVersionConfigs {
		if matcher(version) {
			o.GroupVersionConfigs[version] = true
			o.removeMatchingResourcePreferences(resourceMatcherForVersion(version))
		}
	}
}

// resourceMatcherForVersion matches resources in the specified version
func resourceMatcherForVersion(gv schema.GroupVersion) func(gvr schema.GroupVersionResource) bool {
	return func(gvr schema.GroupVersionResource) bool {
		return gv == gvr.GroupVersion()
	}
}

// removeMatchingResourcePreferences removes individual resource preferences that match.  This is useful when an override of a version or level enablement should
// override the previously individual preferences.
//
// 先收集再删除：不能在遍历 map 的同时 delete，虽然 Go 允许，但语义上更清晰也更易审阅。
func (o *ResourceConfig) removeMatchingResourcePreferences(matcher func(gvr schema.GroupVersionResource) bool) {
	keysToRemove := []schema.GroupVersionResource{}
	for k := range o.ResourceConfigs {
		if matcher(k) {
			keysToRemove = append(keysToRemove, k)
		}
	}
	for _, k := range keysToRemove {
		delete(o.ResourceConfigs, k)
		delete(o.ExplicitResourceConfigs, k)
	}
}

// DisableVersions disables the versions entirely.
// This will remove any preferences previously set on individual resources.
func (o *ResourceConfig) DisableVersions(versions ...schema.GroupVersion) {
	for _, version := range versions {
		o.GroupVersionConfigs[version] = false

		// a preference about a version takes priority over the previously set resources
		o.removeMatchingResourcePreferences(resourceMatcherForVersion(version))
	}
}

// ExplicitlyDisableVersions 同时记录"显式禁用"并执行禁用；显式标记用于后续
// 区分用户意图与默认关闭，不能只调用 DisableVersions。
func (o *ResourceConfig) ExplicitlyDisableVersions(versions ...schema.GroupVersion) {
	for _, version := range versions {
		o.ExplicitGroupVersionConfigs[version] = false
	}
	o.DisableVersions(versions...)
}

// EnableVersions enables all resources in a given groupVersion.
// This will remove any preferences previously set on individual resources.
func (o *ResourceConfig) EnableVersions(versions ...schema.GroupVersion) {
	for _, version := range versions {
		o.GroupVersionConfigs[version] = true

		// a preference about a version takes priority over the previously set resources
		o.removeMatchingResourcePreferences(resourceMatcherForVersion(version))
	}

}

// ExplicitlyEnableVersions 记录显式启用并执行启用，语义与 ExplicitlyDisableVersions 对称。
func (o *ResourceConfig) ExplicitlyEnableVersions(versions ...schema.GroupVersion) {
	for _, version := range versions {
		o.ExplicitGroupVersionConfigs[version] = true
	}
	o.EnableVersions(versions...)
}

// TODO this must be removed and we enable/disable individual resources.
//
// 读取 map 的零值为 false，因此未显式登记的版本默认是关闭的；
// 默认开启由上层在初始化时统一登记，而不是在这里兜底。
func (o *ResourceConfig) versionEnabled(version schema.GroupVersion) bool {
	return o.GroupVersionConfigs[version]
}

// DisableResources 在资源级关闭指定资源，其优先级高于版本级设置。
func (o *ResourceConfig) DisableResources(resources ...schema.GroupVersionResource) {
	for _, resource := range resources {
		o.ResourceConfigs[resource] = false
	}
}

// ExplicitlyDisableResources 记录显式禁用并执行禁用。
func (o *ResourceConfig) ExplicitlyDisableResources(resources ...schema.GroupVersionResource) {
	for _, resource := range resources {
		o.ExplicitResourceConfigs[resource] = false
	}
	o.DisableResources(resources...)
}

// EnableResources 在资源级启用指定资源，可覆盖被关闭的版本。
func (o *ResourceConfig) EnableResources(resources ...schema.GroupVersionResource) {
	for _, resource := range resources {
		o.ResourceConfigs[resource] = true
	}
}

// ExplicitlyEnableResources 记录显式启用并执行启用。
func (o *ResourceConfig) ExplicitlyEnableResources(resources ...schema.GroupVersionResource) {
	for _, resource := range resources {
		o.ExplicitResourceConfigs[resource] = true
	}
	o.EnableResources(resources...)
}

// ResourceEnabled 按"资源级优先，版本级兜底，默认开启"的顺序判定。
//
// 注意最后返回 true 而非 versionEnabled 的结果：能走到这里说明资源级未设置
// 且所属版本已启用，此时资源默认可用。
func (o *ResourceConfig) ResourceEnabled(resource schema.GroupVersionResource) bool {
	// if a resource is explicitly set, that takes priority over the preference of the version.
	resourceEnabled, explicitlySet := o.ResourceConfigs[resource]
	if explicitlySet {
		return resourceEnabled
	}

	if !o.versionEnabled(resource.GroupVersion()) {
		return false
	}
	// they are enabled by default.
	return true
}

// AnyResourceForGroupEnabled 判断某个 group 下是否还有任何可用的资源。
//
// 用于决定是否暴露该 group 的发现端点：若整个 group 都关闭，继续返回空的
// APIGroup 会让客户端误以为存在资源。
func (o *ResourceConfig) AnyResourceForGroupEnabled(group string) bool {
	// 先看版本级：任一版本启用即认为该 group 可用。
	for version := range o.GroupVersionConfigs {
		if version.Group == group {
			if o.versionEnabled(version) {
				return true
			}
		}
	}
	// 再看资源级：可能是版本被关闭但个别资源被显式启用。
	for resource := range o.ResourceConfigs {
		if resource.Group == group && o.ResourceEnabled(resource) {
			return true
		}
	}

	return false
}

// ResourceExplicitlyEnabled 仅查询显式配置，不参与默认值推断；未设置一律返回 false。
func (o *ResourceConfig) ResourceExplicitlyEnabled(resource schema.GroupVersionResource) bool {
	resourceEnabled, explicitlySet := o.ExplicitResourceConfigs[resource]
	if explicitlySet {
		return resourceEnabled
	}
	return false
}

// VersionExplicitlyEnabled 仅查询显式配置，未设置一律返回 false。
func (o *ResourceConfig) VersionExplicitlyEnabled(version schema.GroupVersion) bool {
	versionEnabled, explicitlySet := o.ExplicitGroupVersionConfigs[version]
	if explicitlySet {
		return versionEnabled
	}
	return false
}
