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

package discovery

import (
	"fmt"

	apidiscovery "k8s.io/api/apidiscovery/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// StaleGroupVersionError 表示聚合发现响应中某个 GroupVersion 被标记为 "stale"。
// 之所以不用普通错误返回，是因为聚合发现会同时返回部分成功的数据；
// 调用方既需要失败的 GV 列表，也需要继续使用其余可用结果。
type StaleGroupVersionError struct {
	gv schema.GroupVersion
}

// Error 实现 error 接口；gv 为空的零值错误不应被构造或返回。
func (s StaleGroupVersionError) Error() string {
	return fmt.Sprintf("stale GroupVersion discovery: %v", s.gv)
}

// SplitGroupsAndResources 把 /api、/apis 返回的聚合发现（APIGroupDiscoveryList）
// 降级为旧版客户端期望的 APIGroupList 与按 GroupVersion 索引的 APIResourceList。
// 这样旧调用方无需修改即可使用新版聚合发现接口，也便于统一两条代码路径。
func SplitGroupsAndResources(aggregatedGroups apidiscovery.APIGroupDiscoveryList) (
	*metav1.APIGroupList,
	map[schema.GroupVersion]*metav1.APIResourceList,
	map[schema.GroupVersion]error) {
	// 聚合 group list 包含完整发现信息：组、版本与资源；被标记为 "stale"
	// 的 GroupVersion 表示对应聚合 apiserver 不可用，其结果不可信。
	groups := []*metav1.APIGroup{}
	failedGVs := map[schema.GroupVersion]error{}
	resourcesByGV := map[schema.GroupVersion]*metav1.APIResourceList{}
	for _, aggGroup := range aggregatedGroups.Items {
		// 逐组转换；同一 GV 在聚合结果中只会出现一次，因此直接覆盖即可。
		group, resources, failed := convertAPIGroup(aggGroup)
		groups = append(groups, group)
		for gv, resourceList := range resources {
			resourcesByGV[gv] = resourceList
		}
		for gv, err := range failed {
			failedGVs[gv] = err
		}
	}
	// 返回前把 groups 切片转换为 group list；预先分配容量，避免大量 API 组时反复扩容。
	groupList := &metav1.APIGroupList{}
	groupList.Groups = make([]metav1.APIGroup, 0, len(groups))
	for _, group := range groups {
		groupList.Groups = append(groupList.Groups, *group)
	}
	return groupList, resourcesByGV, failedGVs
}

// convertAPIGroup 只处理单个聚合 API 组，返回旧版 APIGroup 以及该组各版本的资源列表。
// 被标记为 stale 的版本会被跳过并记录到 failedGVs，而不是让整组失败。
func convertAPIGroup(g apidiscovery.APIGroupDiscovery) (
	*metav1.APIGroup,
	map[schema.GroupVersion]*metav1.APIResourceList,
	map[schema.GroupVersion]error) {
	// 遍历版本，转换为旧版的 group 与 resources。
	group := &metav1.APIGroup{}
	gvResources := map[schema.GroupVersion]*metav1.APIResourceList{}
	failedGVs := map[schema.GroupVersion]error{}
	group.Name = g.ObjectMeta.Name
	for _, v := range g.Versions {
		gv := schema.GroupVersion{Group: g.Name, Version: v.Version}
		if v.Freshness == apidiscovery.DiscoveryFreshnessStale {
			// stale 表示该版本的数据可能来自不可达的聚合 apiserver；
			// 保留为失败项，让上层决定是否重试，避免把过期数据当成最新。
			failedGVs[gv] = StaleGroupVersionError{gv: gv}
			continue
		}
		version := metav1.GroupVersionForDiscovery{}
		version.GroupVersion = gv.String()
		version.Version = v.Version
		group.Versions = append(group.Versions, version)
		// 旧版语义要求 PreferredVersion 是第一个可用版本，因此跳过 stale 版本。
		if group.PreferredVersion == (metav1.GroupVersionForDiscovery{}) {
			group.PreferredVersion = version
		}
		resourceList := &metav1.APIResourceList{}
		resourceList.GroupVersion = gv.String()
		for _, r := range v.Resources {
			resource, err := convertAPIResource(r)
			if err == nil {
				resourceList.APIResources = append(resourceList.APIResources, resource)
			}
			// 新格式的 Subresources 字段会被转换为完整的 APIResources。
			// 子资源需要父资源的名称/可见性来拼接成 "parent/subresource"；
			// 即使父资源缺少 GVK 而未被加入列表，仍用其部分结果构造子资源。
			for _, subresource := range r.Subresources {
				sr, err := convertAPISubresource(resource, subresource)
				if err == nil {
					resourceList.APIResources = append(resourceList.APIResources, sr)
				}
			}
		}
		gvResources[gv] = resourceList
	}
	return group, gvResources, failedGVs
}

// emptyKind 用于判断 ResponseKind 指针是否指向零值；零值等同于“未提供 GVK”。
var emptyKind = metav1.GroupVersionKind{}

// convertAPIResource 把 APIResourceDiscovery 转换为 APIResource，对缺失 GVK
// 的情况保持容错，因为该资源可能是子资源的父资源。
// 缺少 GVK 时返回部分结果与错误：部分结果仅用于父资源名字/作用域，
// 供子资源转换使用，调用方不应把它加入发现列表；此时父资源不进入发现
// 结果，子资源必须自带 GVK。
func convertAPIResource(in apidiscovery.APIResourceDiscovery) (metav1.APIResource, error) {
	result := metav1.APIResource{
		Name:         in.Resource,
		SingularName: in.SingularResource,
		Namespaced:   in.Scope == apidiscovery.ScopeNamespace,
		Verbs:        in.Verbs,
		ShortNames:   in.ShortNames,
		Categories:   in.Categories,
	}
	var err error
	if in.ResponseKind != nil && (*in.ResponseKind) != emptyKind {
		result.Group = in.ResponseKind.Group
		result.Version = in.ResponseKind.Version
		result.Kind = in.ResponseKind.Kind
	} else {
		err = fmt.Errorf("discovery resource %s missing GVK", in.Resource)
	}
	// 返回部分结果 + 错误，由调用方决定是否使用（仅用于子资源父级）。
	return result, err
}

// convertAPISubresource 把 APISubresourceDiscovery 转换为 APIResource。
// 子资源的 Name 采用 "parent/subresource"，而 SingularName、Namespaced 继承自父资源，
// 这与旧版非聚合发现返回的条目形态保持一致。GVK 必须由子资源自身提供。
func convertAPISubresource(parent metav1.APIResource, in apidiscovery.APISubresourceDiscovery) (metav1.APIResource, error) {
	result := metav1.APIResource{}
	if in.ResponseKind == nil || (*in.ResponseKind) == emptyKind {
		return result, fmt.Errorf("subresource %s/%s missing GVK", parent.Name, in.Subresource)
	}
	result.Name = fmt.Sprintf("%s/%s", parent.Name, in.Subresource)
	result.SingularName = parent.SingularName
	result.Namespaced = parent.Namespaced
	result.Group = in.ResponseKind.Group
	result.Version = in.ResponseKind.Version
	result.Kind = in.ResponseKind.Kind
	result.Verbs = in.Verbs
	return result, nil
}
