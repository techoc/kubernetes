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

// Configuration 提供 ResourceQuota 准入控制器的配置。
//
// 这是内部（无版本）类型，磁盘上的配置文件通过 install 包注册的
// 各版本类型转换而来；新增字段须同步所有已发布版本。
type Configuration struct {
	metav1.TypeMeta

	// LimitedResources 是默认限制消耗的资源列表。
	// +optional
	//
	// 列表为空时不强制任何配额存在，即 ResourceQuota 保持
	// "只有声明了配额才限制"的传统行为。
	LimitedResources []LimitedResource
}

// LimitedResource 描述一个默认限制消耗的资源。
// 要消耗该资源，必须存在限制其消耗的相关配额。
//
// 用于在集群级别强制"必须先有配额才能消耗某类资源"，防止管理员
// 误创建未受配额约束的昂贵资源（如大容量 PVC）。
type LimitedResource struct {

	// APIGroup 是包含被限制资源的 APIGroup 名称。
	// +optional
	//
	// 留空表示核心组，与 corev1 资源的 Group 字段为空保持一致。
	APIGroup string `json:"apiGroup,omitempty"`

	// Resource 是该规则适用的资源名称。
	// 例如，若管理员想限制与持久卷声明（PVC）关联的存储资源消耗，
	// 取值应为 "persistentvolumeclaims"。
	Resource string `json:"resource"`

	// 对每个被拦截的请求，配额系统都会评估其资源用量：
	// 遍历其消耗的每种资源，若资源名包含本列表中的任一子串，
	// 配额系统会确保存在覆盖配额（covering quota）；没有覆盖配额则拒绝请求。
	// 例如，若管理员想全局强制“消耗任意 storageclass 的 PVC 必须有配额”，
	// 列表中应包含
	// ".storageclass.storage.k8s.io/requests.storage"
	//
	// 子串匹配意味着条目应包含足够长的限定片段，否则可能误命中
	// 不相关的资源名；这是宽松匹配换来的配置便利性。
	MatchContains []string

	// 对每个被拦截的请求，配额系统会判断输入对象是否满足本列表中的某个作用域，
	// 若满足则确保存在覆盖配额；没有覆盖配额则拒绝请求。
	// 例如，若管理员想全局强制“创建使用 cluster-services 优先级类的 Pod
	// 必须有配额”，列表中应包含
	// "PriorityClassNameIn=cluster-services"
	// +optional
	//	MatchScopes []string `json:"matchScopes,omitempty"`
	//
	// 使用结构化选择器而非字符串，可复用 core/v1 的作用域语义，
	// 避免为配额插件另造一套解析逻辑。
	MatchScopes []corev1.ScopedResourceSelectorRequirement `json:"matchScopes,omitempty"`
}
