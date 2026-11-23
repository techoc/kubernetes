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
	rbacv1client "k8s.io/client-go/kubernetes/typed/rbac/v1"
)

// 指令 +k8s:deepcopy-gen=true 等指令要求生成器为这个适配器产出 DeepCopy 方法，
// 原因是它以值类型满足 RoleBinding 接口，需要在对账时无损复制。
// +k8s:deepcopy-gen=true
// +k8s:deepcopy-gen:interfaces=k8s.io/component-helpers/auth/rbac/reconciliation.RoleBinding
// +k8s:deepcopy-gen:nonpointer-interfaces=true
// ClusterRoleBindingAdapter 把集群级绑定包装成统一的 RoleBinding 接口，
// 让对账逻辑不必区分命名空间级和集群级对象；代价是 GetNamespace 只能返回空串，
// 因为集群角色绑定本身没有命名空间。
type ClusterRoleBindingAdapter struct {
	// ClusterRoleBinding 是被包装的集群角色绑定；值接收者配合指针字段，使拷贝适配器不会复制底层对象。
	ClusterRoleBinding *rbacv1.ClusterRoleBinding
}

// GetObject 返回底层的 ClusterRoleBinding，供需要 runtime.Object 的通用代码使用。
func (o ClusterRoleBindingAdapter) GetObject() runtime.Object {
	return o.ClusterRoleBinding
}

// GetNamespace 返回命名空间；ClusterRoleBinding 是集群作用域对象，该值通常为空串。
// 统一接口里保留此方法只是为了满足 RoleBinding 的签名。
func (o ClusterRoleBindingAdapter) GetNamespace() string {
	return o.ClusterRoleBinding.Namespace
}

// GetName 返回 ClusterRoleBinding 的名称，作为对账时定位对象的主键。
func (o ClusterRoleBindingAdapter) GetName() string {
	return o.ClusterRoleBinding.Name
}

// GetUID 返回对象的 UID；删除时作为前置条件，防止误删已被替换的同名对象。
func (o ClusterRoleBindingAdapter) GetUID() types.UID {
	return o.ClusterRoleBinding.UID
}

// GetLabels 返回 ClusterRoleBinding 的标签，供对账比较与合并元数据。
func (o ClusterRoleBindingAdapter) GetLabels() map[string]string {
	return o.ClusterRoleBinding.Labels
}

// SetLabels 用 in 整体替换 ClusterRoleBinding 的标签。
func (o ClusterRoleBindingAdapter) SetLabels(in map[string]string) {
	o.ClusterRoleBinding.Labels = in
}

// GetAnnotations 返回 ClusterRoleBinding 的注解，其中包含“是否允许自动更新”的开关。
func (o ClusterRoleBindingAdapter) GetAnnotations() map[string]string {
	return o.ClusterRoleBinding.Annotations
}

// SetAnnotations 用 in 整体替换 ClusterRoleBinding 的注解。
func (o ClusterRoleBindingAdapter) SetAnnotations(in map[string]string) {
	o.ClusterRoleBinding.Annotations = in
}

// GetRoleRef 返回绑定的角色引用；roleRef 在 RBAC 中不可变，变化时只能重建绑定。
func (o ClusterRoleBindingAdapter) GetRoleRef() rbacv1.RoleRef {
	return o.ClusterRoleBinding.RoleRef
}

// GetSubjects 返回当前绑定的主体列表，用于对账时计算缺失与多余项。
func (o ClusterRoleBindingAdapter) GetSubjects() []rbacv1.Subject {
	return o.ClusterRoleBinding.Subjects
}

// SetSubjects 用 in 整体替换绑定的主体列表。
func (o ClusterRoleBindingAdapter) SetSubjects(in []rbacv1.Subject) {
	o.ClusterRoleBinding.Subjects = in
}

// ClusterRoleBindingClientAdapter 把 typed client 塞进同一个 RoleBindingClient 接口。
// 接口上的 namespace 参数在这里被忽略：集群级 API 没有命名空间维度，
// 保留参数只是为了让调用方对两类绑定使用完全相同的调用形式。
type ClusterRoleBindingClientAdapter struct {
	// Client 是集群角色绑定的 typed client；namespace 参数在下面的方法中都会被忽略。
	Client rbacv1client.ClusterRoleBindingInterface
}

// Get 按名称读取 ClusterRoleBinding；namespace 参数被忽略，因为它没有命名空间维度。
func (c ClusterRoleBindingClientAdapter) Get(namespace, name string) (RoleBinding, error) {
	ret, err := c.Client.Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return ClusterRoleBindingAdapter{ClusterRoleBinding: ret}, err
}

// Create 创建 ClusterRoleBinding，并把返回值重新包装成 RoleBinding；
// 传入的 in 必须是 ClusterRoleBindingAdapter，类型不符属于程序错误。
func (c ClusterRoleBindingClientAdapter) Create(in RoleBinding) (RoleBinding, error) {
	ret, err := c.Client.Create(context.TODO(), in.(ClusterRoleBindingAdapter).ClusterRoleBinding, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}
	return ClusterRoleBindingAdapter{ClusterRoleBinding: ret}, err
}

// Update 把调和后的主体写回 ClusterRoleBinding，并返回服务端更新后的对象。
func (c ClusterRoleBindingClientAdapter) Update(in RoleBinding) (RoleBinding, error) {
	ret, err := c.Client.Update(context.TODO(), in.(ClusterRoleBindingAdapter).ClusterRoleBinding, metav1.UpdateOptions{})
	if err != nil {
		return nil, err
	}
	return ClusterRoleBindingAdapter{ClusterRoleBinding: ret}, err

}

// Delete 带上 UID 前置条件，是为了避免删掉同名但已被别人重建的绑定：
// 对账看到的对象和真正执行删除之间可能有竞态，UID 能够保证只删自己认定的那个。
func (c ClusterRoleBindingClientAdapter) Delete(namespace, name string, uid types.UID) error {
	return c.Client.Delete(context.TODO(), name, metav1.DeleteOptions{Preconditions: &metav1.Preconditions{UID: &uid}})
}
