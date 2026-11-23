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
	"k8s.io/apimachinery/pkg/types"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	rbacv1client "k8s.io/client-go/kubernetes/typed/rbac/v1"
)

// 指令 +k8s:deepcopy-gen=true 等生成指令让此适配器获得 DeepCopyRoleBinding。
// 协调逻辑需要在改动前复制一份现有对象，而 rbacv1.RoleBinding 本身没有返回 RoleBinding
// 接口的拷贝方法；把拷贝能力生成在适配器上，既满足 RoleBinding 接口，又避免改动 API 类型。
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/component-helpers/auth/rbac/reconciliation.RoleBinding
// +k8s:deepcopy-gen:nonpointer-interfaces=true
type RoleBindingAdapter struct {
	// RoleBinding 是被包装的命名空间角色绑定；适配器按值满足 RoleBinding 接口。
	RoleBinding *rbacv1.RoleBinding
}

// 下面这组方法把 RoleBinding 接口映射到具体的 rbacv1.RoleBinding 字段，
// 这样协调器就能用同一套接口处理 RoleBinding 和 ClusterRoleBinding，无需关心对象类型。

// GetObject 返回底层的 RoleBinding，供需要 runtime.Object 的通用代码使用。
func (o RoleBindingAdapter) GetObject() runtime.Object {
	return o.RoleBinding
}

// GetNamespace 返回 RoleBinding 所在的命名空间，供创建前补建命名空间使用。
func (o RoleBindingAdapter) GetNamespace() string {
	return o.RoleBinding.Namespace
}

// GetName 返回 RoleBinding 的名称，作为对账时定位对象的主键。
func (o RoleBindingAdapter) GetName() string {
	return o.RoleBinding.Name
}

// GetUID 返回对象的 UID；删除时作为前置条件，防止误删已被替换的同名对象。
func (o RoleBindingAdapter) GetUID() types.UID {
	return o.RoleBinding.UID
}

// GetLabels 返回 RoleBinding 的标签，供对账比较与合并元数据。
func (o RoleBindingAdapter) GetLabels() map[string]string {
	return o.RoleBinding.Labels
}

// SetLabels 用 in 整体替换 RoleBinding 的标签。
func (o RoleBindingAdapter) SetLabels(in map[string]string) {
	o.RoleBinding.Labels = in
}

// GetAnnotations 返回 RoleBinding 的注解，其中包含“是否允许自动更新”的开关。
func (o RoleBindingAdapter) GetAnnotations() map[string]string {
	return o.RoleBinding.Annotations
}

// SetAnnotations 用 in 整体替换 RoleBinding 的注解。
func (o RoleBindingAdapter) SetAnnotations(in map[string]string) {
	o.RoleBinding.Annotations = in
}

// GetRoleRef 返回绑定的角色引用；roleRef 不可变，变化时只能重建绑定。
func (o RoleBindingAdapter) GetRoleRef() rbacv1.RoleRef {
	return o.RoleBinding.RoleRef
}

// GetSubjects 返回当前绑定的主体列表，用于对账时计算缺失与多余项。
func (o RoleBindingAdapter) GetSubjects() []rbacv1.Subject {
	return o.RoleBinding.Subjects
}

// SetSubjects 用 in 整体替换绑定的主体列表。
func (o RoleBindingAdapter) SetSubjects(in []rbacv1.Subject) {
	o.RoleBinding.Subjects = in
}

// RoleBindingClientAdapter 把 typed client 包装成 RoleBindingModifier。
// 拆出这一层是为了让 ReconcileRoleBindingOptions.Run 只依赖四方法的小接口，
// 测试时可以用假实现替换，不必起一套 API Server。
// 同时保留 NamespaceClient，让创建绑定时有机会顺手补上命名空间（见下方 Create）。
type RoleBindingClientAdapter struct {
	// Client 提供按命名空间取 RoleBinding 接口的能力。
	Client rbacv1client.RoleBindingsGetter
	// NamespaceClient 用于在创建绑定前尽力补建命名空间。
	NamespaceClient corev1client.NamespaceInterface
}

// Get 读取指定命名空间下的 RoleBinding，并包装成 RoleBinding 接口返回。
func (c RoleBindingClientAdapter) Get(namespace, name string) (RoleBinding, error) {
	ret, err := c.Client.RoleBindings(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return RoleBindingAdapter{RoleBinding: ret}, err
}

// Create 先确保命名空间存在，再创建 RoleBinding，并返回服务端创建后的对象。
// 传入的 in 必须是 RoleBindingAdapter，类型不符属于程序错误。
func (c RoleBindingClientAdapter) Create(in RoleBinding) (RoleBinding, error) {
	// 绑定不能脱离命名空间存在；在写入前尽力确保命名空间就位。
	// tryEnsureNamespace 会吞掉权限不足等错误，因此只有真正创建绑定失败时才会暴露问题。
	if err := tryEnsureNamespace(c.NamespaceClient, in.GetNamespace()); err != nil {
		return nil, err
	}

	ret, err := c.Client.RoleBindings(in.GetNamespace()).Create(context.TODO(), in.(RoleBindingAdapter).RoleBinding, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	return RoleBindingAdapter{RoleBinding: ret}, err
}

// Update 把调和后的主体写回 RoleBinding，并返回服务端更新后的对象。
func (c RoleBindingClientAdapter) Update(in RoleBinding) (RoleBinding, error) {
	ret, err := c.Client.RoleBindings(in.GetNamespace()).Update(context.TODO(), in.(RoleBindingAdapter).RoleBinding, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}
	return RoleBindingAdapter{RoleBinding: ret}, err

}

// Delete 按名称和 UID 删除 RoleBinding；UID 前置条件保证只删对账时认定的那个对象。
func (c RoleBindingClientAdapter) Delete(namespace, name string, uid types.UID) error {
	// UID 前置条件让“删除旧对象再建新对象”的重协调串行化：
	// 如果目标在此期间被替换，删除会因为 UID 冲突失败，调用方据此重试而不是误删新对象。
	return c.Client.RoleBindings(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
}
