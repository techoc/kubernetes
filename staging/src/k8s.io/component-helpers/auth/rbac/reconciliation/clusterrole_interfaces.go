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
	rbacv1client "k8s.io/client-go/kubernetes/typed/rbac/v1"
)

// ClusterRoleRuleOwner 把 cluster 作用域的 ClusterRole 包成 RuleOwner，
// 目的是让同一套调和逻辑能同时处理 Role 与 ClusterRole，省掉调用方按作用域分支。
// 方法用值接收者、结构体里放指针，再配合指令 +k8s:deepcopy-gen:nonpointer-interfaces=true，
// 使生成代码按值类型满足接口，同时拷贝包装器不会复制整份规则切片。
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/component-helpers/auth/rbac/reconciliation.RuleOwner
// +k8s:deepcopy-gen:nonpointer-interfaces=true
type ClusterRoleRuleOwner struct {
	// ClusterRole 是被包装的集群角色；用指针而非值，避免复制包装器时连整份规则切片一起复制。
	ClusterRole *rbacv1.ClusterRole
}

// GetObject 返回底层的 ClusterRole，供需要 runtime.Object 的通用代码使用。
func (o ClusterRoleRuleOwner) GetObject() runtime.Object {
	return o.ClusterRole
}

// GetNamespace 返回命名空间；ClusterRole 是集群作用域对象，该值通常为空串。
// 统一接口里保留此方法，调用方就不必先判断角色种类再取值。
func (o ClusterRoleRuleOwner) GetNamespace() string {
	return o.ClusterRole.Namespace
}

// GetName 返回 ClusterRole 的名称，作为对账时定位对象的主键。
func (o ClusterRoleRuleOwner) GetName() string {
	return o.ClusterRole.Name
}

// GetLabels 返回 ClusterRole 的标签，供对账比较与合并元数据。
func (o ClusterRoleRuleOwner) GetLabels() map[string]string {
	return o.ClusterRole.Labels
}

// SetLabels 用 in 整体替换 ClusterRole 的标签。
func (o ClusterRoleRuleOwner) SetLabels(in map[string]string) {
	o.ClusterRole.Labels = in
}

// GetAnnotations 返回 ClusterRole 的注解，其中包含“是否允许自动更新”的开关。
func (o ClusterRoleRuleOwner) GetAnnotations() map[string]string {
	return o.ClusterRole.Annotations
}

// SetAnnotations 用 in 整体替换 ClusterRole 的注解。
func (o ClusterRoleRuleOwner) SetAnnotations(in map[string]string) {
	o.ClusterRole.Annotations = in
}

// GetRules 返回 ClusterRole 当前的权限规则列表。
func (o ClusterRoleRuleOwner) GetRules() []rbacv1.PolicyRule {
	return o.ClusterRole.Rules
}

// SetRules 用 in 整体替换 ClusterRole 的权限规则列表。
func (o ClusterRoleRuleOwner) SetRules(in []rbacv1.PolicyRule) {
	o.ClusterRole.Rules = in
}

// GetAggregationRule 返回聚合规则；这是 ClusterRole 独有的字段，
// Role 一侧提供了空实现，使 RuleOwner 的调用方可以无条件读写。
func (o ClusterRoleRuleOwner) GetAggregationRule() *rbacv1.AggregationRule {
	return o.ClusterRole.AggregationRule
}

// SetAggregationRule 设置聚合规则；nil 表示取消聚合、退回普通角色。
func (o ClusterRoleRuleOwner) SetAggregationRule(in *rbacv1.AggregationRule) {
	o.ClusterRole.AggregationRule = in
}

// ClusterRoleModifier 把 typed client 包装成 Modifier，
// 让调和循环无需关心底层是 namespaced 还是 cluster 作用域资源。
// 只依赖一个窄接口而不是完整的 Clientset，便于测试时替换。
type ClusterRoleModifier struct {
	// Client 是 cluster 作用域的 typed client；只依赖这个窄接口，测试时容易替换。
	Client rbacv1client.ClusterRoleInterface
}

// Get 忽略 namespace：ClusterRole 是 cluster 作用域对象，接口上保留该参数
// 只是为了和 RoleModifier 共用同一个 Modifier 签名。
func (c ClusterRoleModifier) Get(namespace, name string) (RuleOwner, error) {
	ret, err := c.Client.Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return ClusterRoleRuleOwner{ClusterRole: ret}, err
}

// Create 使用断言取回具体类型：Modifier 由调用方按对象种类成套提供，
// 传错类型属于程序化错误，没必要在这里额外返回错误分支。
func (c ClusterRoleModifier) Create(in RuleOwner) (RuleOwner, error) {
	ret, err := c.Client.Create(context.TODO(), in.(ClusterRoleRuleOwner).ClusterRole, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	return ClusterRoleRuleOwner{ClusterRole: ret}, err
}

// Update 把调和后的规则写回 ClusterRole，并返回服务端更新后的对象。
func (c ClusterRoleModifier) Update(in RuleOwner) (RuleOwner, error) {
	ret, err := c.Client.Update(context.TODO(), in.(ClusterRoleRuleOwner).ClusterRole, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}
	return ClusterRoleRuleOwner{ClusterRole: ret}, err

}
