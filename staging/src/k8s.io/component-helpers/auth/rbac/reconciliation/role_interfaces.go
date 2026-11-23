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

package reconciliation

import (
	"context"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	rbacv1client "k8s.io/client-go/kubernetes/typed/rbac/v1"
)

// RoleRuleOwner 把 namespaced 的 Role 包成 RuleOwner，
// 与 ClusterRoleRuleOwner 成对出现，让调和算法只面对一种抽象。
// 方法用值接收者并配合指令 +k8s:deepcopy-gen:nonpointer-interfaces=true，
// 使生成代码按值类型满足接口，调用方持有轻量包装器即可。
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/component-helpers/auth/rbac/reconciliation.RuleOwner
// +k8s:deepcopy-gen:nonpointer-interfaces=true
type RoleRuleOwner struct {
	// Role 是被包装的命名空间角色；用指针避免拷贝包装器时复制底层对象。
	Role *rbacv1.Role
}

// GetObject 返回底层的 Role，供需要 runtime.Object 的通用代码使用。
func (o RoleRuleOwner) GetObject() runtime.Object {
	return o.Role
}

// GetNamespace 返回 Role 所在的命名空间；对账时用它定位 namespaced 的 API 资源。
func (o RoleRuleOwner) GetNamespace() string {
	return o.Role.Namespace
}

// GetName 返回 Role 的名称，作为对账时定位对象的主键。
func (o RoleRuleOwner) GetName() string {
	return o.Role.Name
}

// GetLabels 返回 Role 的标签，供对账比较与合并元数据。
func (o RoleRuleOwner) GetLabels() map[string]string {
	return o.Role.Labels
}

// SetLabels 用 in 整体替换 Role 的标签。
func (o RoleRuleOwner) SetLabels(in map[string]string) {
	o.Role.Labels = in
}

// GetAnnotations 返回 Role 的注解，其中包含“是否允许自动更新”的开关。
func (o RoleRuleOwner) GetAnnotations() map[string]string {
	return o.Role.Annotations
}

// SetAnnotations 用 in 整体替换 Role 的注解。
func (o RoleRuleOwner) SetAnnotations(in map[string]string) {
	o.Role.Annotations = in
}

// GetRules 返回 Role 当前的权限规则列表。
func (o RoleRuleOwner) GetRules() []rbacv1.PolicyRule {
	return o.Role.Rules
}

// SetRules 用 in 整体替换 Role 的权限规则列表。
func (o RoleRuleOwner) SetRules(in []rbacv1.PolicyRule) {
	o.Role.Rules = in
}

// GetAggregationRule 恒返回 nil：Role 没有聚合规则字段。
// 这里给出空实现而不是拆分接口，是为了让 RuleOwner 的调用方不必区分角色种类。
func (o RoleRuleOwner) GetAggregationRule() *rbacv1.AggregationRule {
	return nil
}

// SetAggregationRule 是空实现，静默忽略传入值：Role 没有可承载聚合规则的对象。
func (o RoleRuleOwner) SetAggregationRule(in *rbacv1.AggregationRule) {
}

// RoleModifier 把 namespaced 的 Role 操作包装成 Modifier。
// 依赖 RolesGetter 而非固定 RoleInterface，使调用方选择命名空间后才拿到具体接口；
// 另外注入 NamespaceClient 是为了在创建前补建命名空间，避免调用方漏做这一步。
type RoleModifier struct {
	// Client 提供按命名空间取 Role 接口的能力，调用时需先指定 namespace。
	Client rbacv1client.RolesGetter
	// NamespaceClient 用于在创建 Role 前尽力补建命名空间。
	NamespaceClient corev1client.NamespaceInterface
}

// Get 读取指定命名空间下的 Role，并包装成 RuleOwner 返回。
func (c RoleModifier) Get(namespace, name string) (RuleOwner, error) {
	ret, err := c.Client.Roles(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return RoleRuleOwner{Role: ret}, err
}

// Create 先确保命名空间存在，再创建 Role，并返回服务端创建后的对象。
// 传入的 in 必须是 RoleRuleOwner，类型不符属于程序错误。
func (c RoleModifier) Create(in RuleOwner) (RuleOwner, error) {
	// 角色依赖命名空间存在，补建放在这里而不是调用方，
	// 是为了消除"只创建角色、忘记建命名空间"这类调用顺序错误。
	if err := tryEnsureNamespace(c.NamespaceClient, in.GetNamespace()); err != nil {
		return nil, err
	}

	ret, err := c.Client.Roles(in.GetNamespace()).Create(context.TODO(), in.(RoleRuleOwner).Role, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	return RoleRuleOwner{Role: ret}, err
}

// Update 把调和后的规则写回 Role，并返回服务端更新后的对象。
func (c RoleModifier) Update(in RuleOwner) (RuleOwner, error) {
	ret, err := c.Client.Roles(in.GetNamespace()).Update(context.TODO(), in.(RoleRuleOwner).Role, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}
	return RoleRuleOwner{Role: ret}, err

}
