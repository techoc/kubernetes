/*
Copyright 2018 The Kubernetes Authors.

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

// Package testrestmapper 为测试提供基于 Scheme 构造的静态 RESTMapper，
// 让测试不必依赖真实 discovery 数据即可完成资源/类型的映射解析。
package testrestmapper

import (
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
)

// TestOnlyStaticRESTMapper returns a union RESTMapper of all known types with priorities chosen in the following order:
//  1. legacy kube group preferred version, extensions preferred version, metrics preferred version, legacy
//     kube any version, extensions any version, metrics any version, all other groups alphabetical preferred version,
//     all other groups alphabetical.
//
// TODO callers of this method should be updated to build their own specific restmapper based on their scheme for their tests
// TODO the things being tested are related to whether various cases are handled, not tied to the particular types being checked.
//
// 每个 group 只构造一次 mapper，避免同一 group 的多个版本生成重复的映射器。
// 若调用方显式传入 versionPatterns，则优先级完全由这些模式决定，忽略默认的 legacy 优先顺序。
func TestOnlyStaticRESTMapper(scheme *runtime.Scheme, versionPatterns ...schema.GroupVersion) meta.RESTMapper {
	unionMapper := meta.MultiRESTMapper{}
	unionedGroups := sets.NewString()
	for _, enabledVersion := range scheme.PrioritizedVersionsAllGroups() {
		// 以 group 去重：PrioritizedVersionsAllGroups 会为每个版本返回一项。
		if !unionedGroups.Has(enabledVersion.Group) {
			unionedGroups.Insert(enabledVersion.Group)
			unionMapper = append(unionMapper, newRESTMapper(enabledVersion.Group, scheme))
		}
	}

	if len(versionPatterns) != 0 {
		// 显式指定版本时，用 AnyResource/AnyKind 构造通配优先级，
		// 这样同组内任意资源都按给定版本顺序解析。
		resourcePriority := []schema.GroupVersionResource{}
		kindPriority := []schema.GroupVersionKind{}
		for _, versionPriority := range versionPatterns {
			resourcePriority = append(resourcePriority, versionPriority.WithResource(meta.AnyResource))
			kindPriority = append(kindPriority, versionPriority.WithKind(meta.AnyKind))
		}

		return meta.PriorityRESTMapper{Delegate: unionMapper, ResourcePriority: resourcePriority, KindPriority: kindPriority}
	}

	// 默认优先级：内置/扩展/metrics 组优先，其余组按字母序排在后面。
	prioritizedGroups := []string{"", "extensions", "metrics"}
	resourcePriority, kindPriority := prioritiesForGroups(scheme, prioritizedGroups...)

	prioritizedGroupsSet := sets.NewString(prioritizedGroups...)
	remainingGroups := sets.String{}
	for _, enabledVersion := range scheme.PrioritizedVersionsAllGroups() {
		if !prioritizedGroupsSet.Has(enabledVersion.Group) {
			remainingGroups.Insert(enabledVersion.Group)
		}
	}

	// 剩余组同样先 preferred 版本、后 any 版本。
	remainingResourcePriority, remainingKindPriority := prioritiesForGroups(scheme, remainingGroups.List()...)
	resourcePriority = append(resourcePriority, remainingResourcePriority...)
	kindPriority = append(kindPriority, remainingKindPriority...)

	return meta.PriorityRESTMapper{Delegate: unionMapper, ResourcePriority: resourcePriority, KindPriority: kindPriority}
}

// prioritiesForGroups returns the resource and kind priorities for a PriorityRESTMapper, preferring the preferred version of each group first,
// then any non-preferred version of the group second.
//
// 两轮遍历是刻意的：先让所有组的 preferred 版本整体排在前面，
// 再放各组的通配版本，保证跨组解析时不会因为组顺序而误选非首选版本。
func prioritiesForGroups(scheme *runtime.Scheme, groups ...string) ([]schema.GroupVersionResource, []schema.GroupVersionKind) {
	resourcePriority := []schema.GroupVersionResource{}
	kindPriority := []schema.GroupVersionKind{}

	for _, group := range groups {
		availableVersions := scheme.PrioritizedVersionsForGroup(group)
		if len(availableVersions) > 0 {
			resourcePriority = append(resourcePriority, availableVersions[0].WithResource(meta.AnyResource))
			kindPriority = append(kindPriority, availableVersions[0].WithKind(meta.AnyKind))
		}
	}
	for _, group := range groups {
		resourcePriority = append(resourcePriority, schema.GroupVersionResource{Group: group, Version: meta.AnyVersion, Resource: meta.AnyResource})
		kindPriority = append(kindPriority, schema.GroupVersionKind{Group: group, Version: meta.AnyVersion, Kind: meta.AnyKind})
	}

	return resourcePriority, kindPriority
}

// newRESTMapper 为单个 group 构造 DefaultRESTMapper，并用硬编码表区分集群级与命名空间级资源。
func newRESTMapper(group string, scheme *runtime.Scheme) meta.RESTMapper {
	mapper := meta.NewDefaultRESTMapper(scheme.PrioritizedVersionsForGroup(group))
	for _, gv := range scheme.PrioritizedVersionsForGroup(group) {
		for kind := range scheme.KnownTypes(gv) {
			// 这些类型不是真正的资源，不应出现在 RESTMapper 中。
			if ignoredKinds.Has(kind) {
				continue
			}
			// 默认按命名空间级处理，仅已知集群级类型才切换为 Root scope。
			scope := meta.RESTScopeNamespace
			if rootScopedKinds[gv.WithKind(kind).GroupKind()] {
				scope = meta.RESTScopeRoot
			}
			mapper.Add(gv.WithKind(kind), scope)
		}
	}

	return mapper
}

// hardcoded is good enough for the test we're running
var rootScopedKinds = map[schema.GroupKind]bool{
	{Group: "admission.k8s.io", Kind: "AdmissionReview"}: true,

	{Group: "admissionregistration.k8s.io", Kind: "ValidatingWebhookConfiguration"}: true,
	{Group: "admissionregistration.k8s.io", Kind: "MutatingWebhookConfiguration"}:   true,

	{Group: "authentication.k8s.io", Kind: "TokenReview"}: true,

	{Group: "authorization.k8s.io", Kind: "SubjectAccessReview"}:     true,
	{Group: "authorization.k8s.io", Kind: "SelfSubjectAccessReview"}: true,
	{Group: "authorization.k8s.io", Kind: "SelfSubjectRulesReview"}:  true,

	{Group: "certificates.k8s.io", Kind: "CertificateSigningRequest"}: true,

	{Group: "", Kind: "Node"}:             true,
	{Group: "", Kind: "Namespace"}:        true,
	{Group: "", Kind: "PersistentVolume"}: true,
	{Group: "", Kind: "ComponentStatus"}:  true,

	{Group: "rbac.authorization.k8s.io", Kind: "ClusterRole"}:        true,
	{Group: "rbac.authorization.k8s.io", Kind: "ClusterRoleBinding"}: true,

	{Group: "scheduling.k8s.io", Kind: "PriorityClass"}: true,

	{Group: "storage.k8s.io", Kind: "StorageClass"}:     true,
	{Group: "storage.k8s.io", Kind: "VolumeAttachment"}: true,

	{Group: "apiextensions.k8s.io", Kind: "CustomResourceDefinition"}: true,

	{Group: "apiserver.k8s.io", Kind: "AdmissionConfiguration"}: true,

	{Group: "audit.k8s.io", Kind: "Event"}:  true,
	{Group: "audit.k8s.io", Kind: "Policy"}: true,

	{Group: "apiregistration.k8s.io", Kind: "APIService"}: true,

	{Group: "metrics.k8s.io", Kind: "NodeMetrics"}: true,

	{Group: "wardle.example.com", Kind: "Fischer"}: true,
}

// hardcoded is good enough for the test we're running
var ignoredKinds = sets.NewString(
	"ListOptions",
	"DeleteOptions",
	"Status",
	"PodLogOptions",
	"PodExecOptions",
	"PodAttachOptions",
	"PodPortForwardOptions",
	"PodProxyOptions",
	"NodeProxyOptions",
	"ServiceProxyOptions",
)
