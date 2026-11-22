/*
Copyright 2017 The Kubernetes Authors.

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

package v1alpha1

import (
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Configuration provides configuration for the ResourceQuota admission controller.
// 它对应 resourcequota 准入插件的配置文件，决定哪些资源在缺少配额时必须被拒绝。
type Configuration struct {
	metav1.TypeMeta `json:""`

	// LimitedResources whose consumption is limited by default.
	// 每个条目描述一类资源；为空时所有资源都按默认配额策略处理。
	// +optional
	LimitedResources []LimitedResource `json:"limitedResources"`
}

// LimitedResource matches a resource whose consumption is limited by default.
// To consume the resource, there must exist an associated quota that limits
// its consumption.
// 该结构与 LimitedResources 配合，实现“没有覆盖配额就拒绝”的默认限制语义。
type LimitedResource struct {

	// APIGroup is the name of the APIGroup that contains the limited resource.
	// 留空表示 core 组；必须与 Resource 组合后才能唯一确定资源。
	// +optional
	APIGroup string `json:"apiGroup,omitempty"`

	// Resource is the name of the resource this rule applies to.
	// For example, if the administrator wants to limit consumption
	// of a storage resource associated with persistent volume claims,
	// the value would be "persistentvolumeclaims".
	Resource string `json:"resource"`

	// For each intercepted request, the quota system will evaluate
	// its resource usage.  It will iterate through each resource consumed
	// and if the resource contains any substring in this listing, the
	// quota system will ensure that there is a covering quota.  In the
	// absence of a covering quota, the quota system will deny the request.
	// For example, if an administrator wants to globally enforce that
	// that a quota must exist to consume persistent volume claims associated
	// with any storage class, the list would include
	// ".storageclass.storage.k8s.io/requests.storage"
	//
	// 匹配为子串匹配（而非前缀），因此条目应包含足够具体的资源名后缀，
	// 避免误伤其他资源键。
	MatchContains []string `json:"matchContains,omitempty"`
	// For each intercepted request, the quota system will figure out if the input object
	// satisfies a scope which is present in this listing, then
	// quota system will ensure that there is a covering quota.  In the
	// absence of a covering quota, the quota system will deny the request.
	// For example, if an administrator wants to globally enforce that
	// a quota must exist to create a pod with "cluster-services" priorityclass
	// the list would include "scopeName=PriorityClass, Operator=In, Value=cluster-services"
	// +optional
	MatchScopes []v1.ScopedResourceSelectorRequirement `json:"matchScopes,omitempty"`
}
