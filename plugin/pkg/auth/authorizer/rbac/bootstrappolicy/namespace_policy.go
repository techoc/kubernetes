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

package bootstrappolicy

import (
	"strings"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/klog/v2"

	rbacv1helpers "k8s.io/kubernetes/pkg/apis/rbac/v1"
)

var (
	// namespaceRoles 是按命名空间分组的 Role 列表，集群启动时自动创建。
	namespaceRoles = map[string][]rbacv1.Role{}

	// namespaceRoleBindings 是按命名空间分组的 RoleBinding 列表，集群启动时自动创建。
	namespaceRoleBindings = map[string][]rbacv1.RoleBinding{}
)

// addNamespaceRole 向指定命名空间添加 Role。
// 只允许添加到 kube- 开头的保留命名空间，防止意外污染用户命名空间。
func addNamespaceRole(namespace string, role rbacv1.Role) {
	if !strings.HasPrefix(namespace, "kube-") {
		klog.Fatalf(`roles can only be bootstrapped into reserved namespaces starting with "kube-", not %q`, namespace)
	}

	existingRoles := namespaceRoles[namespace]
	for _, existingRole := range existingRoles {
		if role.Name == existingRole.Name {
			klog.Fatalf("role %q was already registered in %q", role.Name, namespace)
		}
	}

	role.Namespace = namespace
	addDefaultMetadata(&role)
	existingRoles = append(existingRoles, role)
	namespaceRoles[namespace] = existingRoles
}

// addNamespaceRoleBinding 向指定命名空间添加 RoleBinding。
// 只允许添加到 kube- 开头的保留命名空间。
func addNamespaceRoleBinding(namespace string, roleBinding rbacv1.RoleBinding) {
	if !strings.HasPrefix(namespace, "kube-") {
		klog.Fatalf(`rolebindings can only be bootstrapped into reserved namespaces starting with "kube-", not %q`, namespace)
	}

	existingRoleBindings := namespaceRoleBindings[namespace]
	for _, existingRoleBinding := range existingRoleBindings {
		if roleBinding.Name == existingRoleBinding.Name {
			klog.Fatalf("rolebinding %q was already registered in %q", roleBinding.Name, namespace)
		}
	}

	roleBinding.Namespace = namespace
	addDefaultMetadata(&roleBinding)
	existingRoleBindings = append(existingRoleBindings, roleBinding)
	namespaceRoleBindings[namespace] = existingRoleBindings
}

func init() {
	// 在 kube-system 命名空间中创建系统级 Role 和 RoleBinding。
	// 包括 extension-apiserver-authentication-reader、bootstrap-signer、
	// cloud-provider（已废弃）、token-cleaner、leader-locking 等。

	addNamespaceRole(metav1.NamespaceSystem, rbacv1.Role{
		// 允许读取 API Server 的认证配置 ConfigMap
		ObjectMeta: metav1.ObjectMeta{Name: "extension-apiserver-authentication-reader"},
		Rules: []rbacv1.PolicyRule{
			// this particular config map is exposed and contains authentication configuration information
			rbacv1helpers.NewRule("get", "list", "watch").Groups(legacyGroup).Resources("configmaps").Names("extension-apiserver-authentication").RuleOrDie(),
		},
	})
	addNamespaceRole(metav1.NamespaceSystem, rbacv1.Role{
		// 允许 bootstrap-signer 控制器查看 kube-system 中的 Secret
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "bootstrap-signer"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch").Groups(legacyGroup).Resources("secrets").RuleOrDie(),
		},
	})
	addNamespaceRole(metav1.NamespaceSystem, rbacv1.Role{
		// 允许云提供商访问/创建 kube-system ConfigMap
		// 从 Kubernetes 1.10 开始已废弃，将根据 GA 废弃策略删除。
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "cloud-provider"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("create", "get", "list", "watch").Groups(legacyGroup).Resources("configmaps").RuleOrDie(),
		},
	})
	addNamespaceRole(metav1.NamespaceSystem, rbacv1.Role{
		// 允许 token-cleaner 删除 kube-system 中的过期 Secret
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "token-cleaner"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch", "delete").Groups(legacyGroup).Resources("secrets").RuleOrDie(),
			eventsRule(),
		},
	})
	// 领导者选举 Role，允许控制器和调度器管理自己的 Lease
	addNamespaceRole(metav1.NamespaceSystem, rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "system::leader-locking-kube-controller-manager"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "watch", "list", "create", "update").Groups("coordination.k8s.io").Resources("leases").RuleOrDie(),
			rbacv1helpers.NewRule("get", "watch", "list", "create", "update").Groups("coordination.k8s.io").Resources("leasecandidates").RuleOrDie(),
		},
	})
	addNamespaceRole(metav1.NamespaceSystem, rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{Name: "system::leader-locking-kube-scheduler"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "watch", "list", "create", "update").Groups("coordination.k8s.io").Resources("leases").RuleOrDie(),
			rbacv1helpers.NewRule("get", "watch", "list", "create", "update").Groups("coordination.k8s.io").Resources("leasecandidates").RuleOrDie(),
		},
	})

	// 将 extension-apiserver-authentication-reader 角色绑定到控制器和调度器
	delegatedAuthBinding := rbacv1helpers.NewRoleBinding("extension-apiserver-authentication-reader", metav1.NamespaceSystem).Users(user.KubeControllerManager, user.KubeScheduler).BindingOrDie()
	delegatedAuthBinding.Name = "system::extension-apiserver-authentication-reader"
	addNamespaceRoleBinding(metav1.NamespaceSystem, delegatedAuthBinding)

	// 领导者选举 RoleBinding，绑定到对应的 ServiceAccount
	addNamespaceRoleBinding(metav1.NamespaceSystem,
		rbacv1helpers.NewRoleBinding("system::leader-locking-kube-controller-manager", metav1.NamespaceSystem).Users(user.KubeControllerManager).SAs(metav1.NamespaceSystem, "kube-controller-manager", "leader-election-controller").BindingOrDie())
	addNamespaceRoleBinding(metav1.NamespaceSystem,
		rbacv1helpers.NewRoleBinding("system::leader-locking-kube-scheduler", metav1.NamespaceSystem).Users(user.KubeScheduler).SAs(metav1.NamespaceSystem, "kube-scheduler").BindingOrDie())
	addNamespaceRoleBinding(metav1.NamespaceSystem,
		rbacv1helpers.NewRoleBinding(saRolePrefix+"bootstrap-signer", metav1.NamespaceSystem).SAs(metav1.NamespaceSystem, "bootstrap-signer").BindingOrDie())
	// cloud-provider 已废弃，从 Kubernetes 1.10 开始将根据 GA 废弃策略删除。
	addNamespaceRoleBinding(metav1.NamespaceSystem,
		rbacv1helpers.NewRoleBinding(saRolePrefix+"cloud-provider", metav1.NamespaceSystem).SAs(metav1.NamespaceSystem, "cloud-provider").BindingOrDie())
	addNamespaceRoleBinding(metav1.NamespaceSystem,
		rbacv1helpers.NewRoleBinding(saRolePrefix+"token-cleaner", metav1.NamespaceSystem).SAs(metav1.NamespaceSystem, "token-cleaner").BindingOrDie())

	addNamespaceRole(metav1.NamespacePublic, rbacv1.Role{
		// 允许 bootstrap-signer 在 public 命名空间中写入 cluster-info ConfigMap
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "bootstrap-signer"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch").Groups(legacyGroup).Resources("configmaps").RuleOrDie(),
			rbacv1helpers.NewRule("update").Groups(legacyGroup).Resources("configmaps").Names("cluster-info").RuleOrDie(),
			eventsRule(),
		},
	})
	addNamespaceRoleBinding(metav1.NamespacePublic,
		rbacv1helpers.NewRoleBinding(saRolePrefix+"bootstrap-signer", metav1.NamespacePublic).SAs(metav1.NamespaceSystem, "bootstrap-signer").BindingOrDie())

}

// NamespaceRoles 返回按命名空间分组的启动 Role 列表。
// 用于集群初始化时自动创建这些 Role。
func NamespaceRoles() map[string][]rbacv1.Role {
	return namespaceRoles
}

// NamespaceRoleBindings 返回按命名空间分组的启动 RoleBinding 列表。
// 用于集群初始化时自动创建这些 RoleBinding。
func NamespaceRoleBindings() map[string][]rbacv1.RoleBinding {
	return namespaceRoleBindings
}
