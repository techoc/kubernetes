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

package resourcequota

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Configuration provides configuration for the ResourceQuota admission controller.
//
// 中文说明：这是内部（无版本）类型，磁盘上的配置文件通过 install 包注册的
// 各版本类型转换而来；新增字段须同步所有已发布版本。
type Configuration struct {
	metav1.TypeMeta

	// LimitedResources whose consumption is limited by default.
	// +optional
	//
	// 中文说明：列表为空时不强制任何配额存在，即 ResourceQuota 保持
	// "只有声明了配额才限制"的传统行为。
	LimitedResources []LimitedResource
}

// LimitedResource matches a resource whose consumption is limited by default.
// To consume the resource, there must exist an associated quota that limits
// its consumption.
//
// 中文说明：用于在集群级别强制"必须先有配额才能消耗某类资源"，防止管理员
// 误创建未受配额约束的昂贵资源（如大容量 PVC）。
type LimitedResource struct {

	// APIGroup is the name of the APIGroup that contains the limited resource.
	// +optional
	//
	// 中文说明：留空表示核心组，与 corev1 资源的 Group 字段为空保持一致。
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
	// 中文说明：子串匹配意味着条目应包含足够长的限定片段，否则可能误命中
	// 不相关的资源名；这是宽松匹配换来的配置便利性。
	MatchContains []string

	// For each intercepted request, the quota system will figure out if the input object
	// satisfies a scope which is present in this listing, then
	// quota system will ensure that there is a covering quota.  In the
	// absence of a covering quota, the quota system will deny the request.
	// For example, if an administrator wants to globally enforce that
	// a quota must exist to create a pod with "cluster-services" priorityclass
	// the list would include
	// "PriorityClassNameIn=cluster-services"
	// +optional
	//	MatchScopes []string `json:"matchScopes,omitempty"`
	//
	// 中文说明：使用结构化选择器而非字符串，可复用 core/v1 的作用域语义，
	// 避免为配额插件另造一套解析逻辑。
	MatchScopes []corev1.ScopedResourceSelectorRequirement `json:"matchScopes,omitempty"`
}
