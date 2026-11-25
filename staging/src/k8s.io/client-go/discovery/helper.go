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

package discovery

import (
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	apimachineryversion "k8s.io/apimachinery/pkg/version"
)

// IsResourceEnabled 查询服务端以判断指定资源是否存在，只需一次 discovery 请求即可得出
// 结论。适用于编写控制器或 e2e 测试时探测可选 API（例如某个特性门控开启后才注册的资源）
// 是否已存在于目标集群。
func IsResourceEnabled(client DiscoveryInterface, resourceToCheck schema.GroupVersionResource) (bool, error) {
	// this is a single request.  The ServerResourcesForGroupVersion handles the core v1 group as legacy.
	// 中文：只查询目标 group/version，避免拉取全量 discovery 数据；核心 v1 组的遗留路径由
	// ServerResourcesForGroupVersion 内部处理。
	resourceList, err := client.ServerResourcesForGroupVersion(resourceToCheck.GroupVersion().String())
	if apierrors.IsNotFound(err) { // if the discovery endpoint isn't present, then the resource isn't present.
		// 中文：404 表示该 group/version 未注册，资源自然不存在，这不是错误。
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// 中文：group/version 存在不代表资源存在，还要逐个比对资源名。
	for _, actualResource := range resourceList.APIResources {
		if actualResource.Name == resourceToCheck.Resource {
			return true, nil
		}
	}

	return false, nil
}

// MatchesServerVersion 查询服务端，比较客户端与服务端的服务端构建信息（git hash），
// 联系不上服务端或版本不完全一致都会返回错误。适用于客户端与服务端二进制必须严格
// 同版本的场景（如 e2e 测试），任一字段不一致即返回错误。
func MatchesServerVersion(clientVersion apimachineryversion.Info, client DiscoveryInterface) error {
	sVer, err := client.ServerVersion()
	if err != nil {
		return fmt.Errorf("couldn't read version from server: %v", err)
	}
	// GitVersion includes GitCommit and GitTreeState, but best to be safe?
	// 中文：GitVersion 通常已隐含后两者，但仍显式比较，避免不同构建方式导致字段组合不一致。
	if clientVersion.GitVersion != sVer.GitVersion || clientVersion.GitCommit != sVer.GitCommit || clientVersion.GitTreeState != sVer.GitTreeState {
		return fmt.Errorf("server version (%#v) differs from client version (%#v)", sVer, clientVersion)
	}

	return nil
}

// ServerSupportsVersion 在服务端没有所需版本时返回错误。
// 通过 ServerGroups 列出所有 group/version，检查目标版本是否在其中。
// 注意：discovery 可能因 403 而返回空列表，此时故意放行，以免 RBAC 受限的客户端被误判为
// 不支持该版本。
func ServerSupportsVersion(client DiscoveryInterface, requiredGV schema.GroupVersion) error {
	groups, err := client.ServerGroups()
	if err != nil {
		// This is almost always a connection error, and higher level code should treat this as a generic error,
		// not a negotiation specific error.
		// 中文：连接类错误应原样向上传递，交由上层按普通错误处理，而不是包装成版本协商错误。
		return err
	}
	versions := metav1.ExtractGroupVersions(groups)
	serverVersions := sets.Set[string]{}
	for _, v := range versions {
		serverVersions.Insert(v)
	}

	if serverVersions.Has(requiredGV.String()) {
		return nil
	}

	// If the server supports no versions, then we should pretend it has the version because of old servers.
	// This can happen because discovery fails due to 403 Forbidden errors
	// 中文：空集合说明 discovery 结果不可信（多为旧版本服务端或 403），宁可假定支持。
	if len(serverVersions) == 0 {
		return nil
	}

	return fmt.Errorf("server does not support API version %q", requiredGV)
}

// GroupVersionResources 把 discovery 返回的 APIResourceList 拍平成一个 GVR 集合。
// 用 map[struct{}] 而非切片是为了 O(1) 的存在性判断。
func GroupVersionResources(rls []*metav1.APIResourceList) (map[schema.GroupVersionResource]struct{}, error) {
	gvrs := map[schema.GroupVersionResource]struct{}{}
	for _, rl := range rls {
		gv, err := schema.ParseGroupVersion(rl.GroupVersion)
		if err != nil {
			return nil, err
		}
		// 中文：每个 group/version 下的每个 resource 组合成一个 GVR。
		for i := range rl.APIResources {
			gvrs[schema.GroupVersionResource{Group: gv.Group, Version: gv.Version, Resource: rl.APIResources[i].Name}] = struct{}{}
		}
	}
	return gvrs, nil
}

// FilteredBy 按给定谓词逐条筛选资源；某个 group/version 若没有任何资源通过谓词，
// 则其空的 APIResourceList 整条被丢弃，避免调用方拿到空列表产生误解。
func FilteredBy(pred ResourcePredicate, rls []*metav1.APIResourceList) []*metav1.APIResourceList {
	result := []*metav1.APIResourceList{}
	for _, rl := range rls {
		// 中文：复制列表头，只替换资源切片，保持 group/version 等元数据不变。
		filtered := *rl
		filtered.APIResources = nil
		for i := range rl.APIResources {
			if pred.Match(rl.GroupVersion, &rl.APIResources[i]) {
				filtered.APIResources = append(filtered.APIResources, rl.APIResources[i])
			}
		}
		// 中文：nil 表示该 group/version 无匹配资源，直接跳过。
		if filtered.APIResources != nil {
			result = append(result, &filtered)
		}
	}
	return result
}

// ResourcePredicate 提供检查资源是否匹配给定条件的方法，是 FilteredBy 使用的筛选条件
// 抽象，便于调用方以函数或结构体形式提供谓词。
type ResourcePredicate interface {
	// Match 判断给定 group/version 下的资源是否满足条件。
	Match(groupVersion string, r *metav1.APIResource) bool
}

// ResourcePredicateFunc 基于自定义条件判断资源是否匹配，让普通函数直接满足
// ResourcePredicate 接口。
type ResourcePredicateFunc func(groupVersion string, r *metav1.APIResource) bool

// Match is a wrapper around ResourcePredicateFunc.
func (fn ResourcePredicateFunc) Match(groupVersion string, r *metav1.APIResource) bool {
	return fn(groupVersion, r)
}

// SupportsAllVerbs 是仅当资源支持全部给定动词时才匹配的谓词。
// 例如只会读的客户端可要求资源至少支持 get 与 list。
type SupportsAllVerbs struct {
	// Verbs 是要求资源全部支持的动词集合。
	Verbs []string
}

// Match 检查资源是否包含全部给定动词：按“全部包含”语义判断，而非任意一个。
func (p SupportsAllVerbs) Match(groupVersion string, r *metav1.APIResource) bool {
	return sets.NewString([]string(r.Verbs)...).HasAll(p.Verbs...)
}
