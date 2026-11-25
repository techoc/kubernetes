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

package v1beta1

import (
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Configuration 提供 ResourceQuota 准入控制器的配置。
//
// Configuration 是 ResourceQuota 准控器的外部配置版本；通过 conversion-gen
// 与内部包互转，因此字段定义需与内部版本保持一一对应。
type Configuration struct {
	metav1.TypeMeta `json:""`

	// LimitedResources 是默认限制消耗的资源列表。
	// +optional
	//
	// 列出的资源默认受配额限制：只有当命名空间内存在覆盖配额时才允许消费。
	LimitedResources []LimitedResource `json:"limitedResources"`
}

// LimitedResource 描述一个默认限制消耗的资源。
// 要消耗该资源，必须存在限制其消耗的相关配额。
//
// 该规则把“默认受限”策略与具体配额对象解耦：管理员只需声明规则，
// 实际限额仍由各命名空间的 ResourceQuota 决定。
type LimitedResource struct {

	// APIGroup 是包含被限制资源的 APIGroup 名称。
	// +optional
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
	// 使用子串匹配是为了覆盖按 storageclass 等维度展开的资源名，
	// 但这些字符串会出现在准入热路径上，因此列表应保持精简。
	MatchContains []string `json:"matchContains,omitempty"`
	// 对每个被拦截的请求，配额系统会判断输入对象是否满足本列表中的某个作用域，
	// 若满足则确保存在覆盖配额；没有覆盖配额则拒绝请求。
	// 例如，若管理员想全局强制“创建使用 cluster-services 优先级类的 Pod
	// 必须有配额”
	// 列表中应包含 "scopeName=PriorityClass, Operator=In, Value=cluster-services"
	// +optional
	MatchScopes []v1.ScopedResourceSelectorRequirement `json:"matchScopes,omitempty"`
}
