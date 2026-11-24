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

package rest

import (
	"context"
	"fmt"
	"time"

	"k8s.io/klog/v2"

	rbacapiv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	clientset "k8s.io/client-go/kubernetes"
	rbacv1client "k8s.io/client-go/kubernetes/typed/rbac/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/component-helpers/auth/rbac/reconciliation"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/rbac"
	"k8s.io/kubernetes/pkg/registry/rbac/clusterrole"
	clusterrolepolicybased "k8s.io/kubernetes/pkg/registry/rbac/clusterrole/policybased"
	clusterrolestore "k8s.io/kubernetes/pkg/registry/rbac/clusterrole/storage"
	"k8s.io/kubernetes/pkg/registry/rbac/clusterrolebinding"
	clusterrolebindingpolicybased "k8s.io/kubernetes/pkg/registry/rbac/clusterrolebinding/policybased"
	clusterrolebindingstore "k8s.io/kubernetes/pkg/registry/rbac/clusterrolebinding/storage"
	"k8s.io/kubernetes/pkg/registry/rbac/role"
	rolepolicybased "k8s.io/kubernetes/pkg/registry/rbac/role/policybased"
	rolestore "k8s.io/kubernetes/pkg/registry/rbac/role/storage"
	"k8s.io/kubernetes/pkg/registry/rbac/rolebinding"
	rolebindingpolicybased "k8s.io/kubernetes/pkg/registry/rbac/rolebinding/policybased"
	rolebindingstore "k8s.io/kubernetes/pkg/registry/rbac/rolebinding/storage"
	rbacregistryvalidation "k8s.io/kubernetes/pkg/registry/rbac/validation"
	"k8s.io/kubernetes/plugin/pkg/auth/authorizer/rbac/bootstrappolicy"
)

const PostStartHookName = "rbac/bootstrap-roles"

// RESTStorageProvider 装配 RBAC API Group，并持有用于 Role/Binding 防提权检查的 authorizer。
type RESTStorageProvider struct {
	Authorizer authorizer.UnconditionalAuthorizer
}

var _ genericapiserver.PostStartHookProvider = RESTStorageProvider{}

// NewRESTStorage 创建 rbac.authorization.k8s.io APIGroupInfo，并按资源启用配置装配 v1 存储和启动后 Bootstrap RBAC Hook。
func (p RESTStorageProvider) NewRESTStorage(apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (genericapiserver.APIGroupInfo, error) {
	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(rbac.GroupName, legacyscheme.Scheme, legacyscheme.ParameterCodec, legacyscheme.Codecs)
	// 如果在此处增加 API 版本，务必同时在 kube-apiserver/app/aggregator.go 中为该版本配置对应的优先级。
	// TODO：重构版本装配流程，将优先级信息直接提供给 APIGroupInfo。

	if storageMap, err := p.storage(rbacapiv1.SchemeGroupVersion, apiResourceConfigSource, restOptionsGetter); err != nil {
		return genericapiserver.APIGroupInfo{}, err
	} else if len(storageMap) > 0 {
		apiGroupInfo.VersionedResourcesStorageMap[rbacapiv1.SchemeGroupVersion.Version] = storageMap
	}

	return apiGroupInfo, nil
}

// storage 创建四种基础 Store 和统一 RuleResolver，再用 policybased Storage 包装，确保所有 RBAC 写入都经过防提权检查。
func (p RESTStorageProvider) storage(version schema.GroupVersion, apiResourceConfigSource serverstorage.APIResourceConfigSource, restOptionsGetter generic.RESTOptionsGetter) (map[string]rest.Storage, error) {
	storage := map[string]rest.Storage{}

	rolesStorage, err := rolestore.NewREST(restOptionsGetter)
	if err != nil {
		return storage, err
	}
	roleBindingsStorage, err := rolebindingstore.NewREST(restOptionsGetter)
	if err != nil {
		return storage, err
	}
	clusterRolesStorage, err := clusterrolestore.NewREST(restOptionsGetter)
	if err != nil {
		return storage, err
	}
	clusterRoleBindingsStorage, err := clusterrolebindingstore.NewREST(restOptionsGetter)
	if err != nil {
		return storage, err
	}

	authorizationRuleResolver := rbacregistryvalidation.NewDefaultRuleResolver(
		role.AuthorizerAdapter{Registry: role.NewRegistry(rolesStorage)},
		rolebinding.AuthorizerAdapter{Registry: rolebinding.NewRegistry(roleBindingsStorage)},
		clusterrole.AuthorizerAdapter{Registry: clusterrole.NewRegistry(clusterRolesStorage)},
		clusterrolebinding.AuthorizerAdapter{Registry: clusterrolebinding.NewRegistry(clusterRoleBindingsStorage)},
	)

	// 装配命名空间级 Role 存储。
	if resource := "roles"; apiResourceConfigSource.ResourceEnabled(rbacapiv1.SchemeGroupVersion.WithResource(resource)) {
		storage[resource] = rolepolicybased.NewStorage(rolesStorage, p.Authorizer, authorizationRuleResolver)
	}

	// 装配命名空间级 RoleBinding 存储。
	if resource := "rolebindings"; apiResourceConfigSource.ResourceEnabled(rbacapiv1.SchemeGroupVersion.WithResource(resource)) {
		storage[resource] = rolebindingpolicybased.NewStorage(roleBindingsStorage, p.Authorizer, authorizationRuleResolver)
	}

	// 装配集群级 ClusterRole 存储。
	if resource := "clusterroles"; apiResourceConfigSource.ResourceEnabled(rbacapiv1.SchemeGroupVersion.WithResource(resource)) {
		storage[resource] = clusterrolepolicybased.NewStorage(clusterRolesStorage, p.Authorizer, authorizationRuleResolver)
	}

	// 装配集群级 ClusterRoleBinding 存储。
	if resource := "clusterrolebindings"; apiResourceConfigSource.ResourceEnabled(rbacapiv1.SchemeGroupVersion.WithResource(resource)) {
		storage[resource] = clusterrolebindingpolicybased.NewStorage(clusterRoleBindingsStorage, p.Authorizer, authorizationRuleResolver)
	}

	return storage, nil
}

// PostStartHook 安排 API Server 启动后对账内置 RBAC 策略，确保默认控制器权限和管理员绑定可用。
func (p RESTStorageProvider) PostStartHook() (string, genericapiserver.PostStartHookFunc, error) {
	policy := &PolicyData{
		ClusterRoles:               append(bootstrappolicy.ClusterRoles(), bootstrappolicy.ControllerRoles()...),
		ClusterRoleBindings:        append(bootstrappolicy.ClusterRoleBindings(), bootstrappolicy.ControllerRoleBindings()...),
		Roles:                      bootstrappolicy.NamespaceRoles(),
		RoleBindings:               bootstrappolicy.NamespaceRoleBindings(),
		ClusterRolesToAggregate:    bootstrappolicy.ClusterRolesToAggregate(),
		ClusterRoleBindingsToSplit: bootstrappolicy.ClusterRoleBindingsToSplit(),
	}
	return PostStartHookName, policy.EnsureRBACPolicy(), nil
}

// PolicyData 汇总 API Server 启动时需要创建或对账的内置 RBAC 角色、绑定及升级迁移映射。
type PolicyData struct {
	ClusterRoles        []rbacapiv1.ClusterRole
	ClusterRoleBindings []rbacapiv1.ClusterRoleBinding
	Roles               map[string][]rbacapiv1.Role
	RoleBindings        map[string][]rbacapiv1.RoleBinding
	// ClusterRolesToAggregate 记录升级迁移时旧 ClusterRole 名称到新聚合角色名称的映射。
	ClusterRolesToAggregate map[string]string
	// ClusterRoleBindingsToSplit 记录需要拆分的旧 ClusterRoleBinding 名称及新绑定对象模板。
	ClusterRoleBindingsToSplit map[string]rbacapiv1.ClusterRoleBinding
}

// isConflictOrServiceUnavailable 判断错误是否属于并发冲突或存储版本升级期间的暂时不可用。
func isConflictOrServiceUnavailable(err error) bool {
	return apierrors.IsConflict(err) || apierrors.IsServiceUnavailable(err)
}

// retryOnConflictOrServiceUnavailable 仅对 Conflict 和 ServiceUnavailable 错误按指定退避策略重试。
func retryOnConflictOrServiceUnavailable(backoff wait.Backoff, fn func() error) error {
	return retry.OnError(backoff, isConflictOrServiceUnavailable, fn)
}

// EnsureRBACPolicy 周期性等待 loopback 客户端和 etcd 可用，再执行引导角色/绑定对账；初始化超时会阻止 API Server 正常启动。
func (p *PolicyData) EnsureRBACPolicy() genericapiserver.PostStartHookFunc {
	return func(hookContext genericapiserver.PostStartHookContext) error {
		// 初始化内置角色非常重要：端到端测试中曾出现 API Server 启动时 etcd 尚未就绪，导致角色未初始化、后续功能无法正常工作的情况。
		err := wait.Poll(1*time.Second, 30*time.Second, func() (done bool, err error) {
			client, err := clientset.NewForConfig(hookContext.LoopbackClientConfig)
			if err != nil {
				utilruntime.HandleError(fmt.Errorf("unable to initialize client set: %v", err))
				return false, nil
			}
			return ensureRBACPolicy(p, client)
		})
		// 如果在重试期限内始终无法完成初始化，则终止 API Server 启动。
		if err != nil {
			return fmt.Errorf("unable to initialize roles: %v", err)
		}

		return nil
	}
}

// ensureRBACPolicy 先确认 RBAC API 可读，再迁移旧聚合角色/绑定，最后逐项对账 ClusterRole、ClusterRoleBinding、Role 和 RoleBinding。
func ensureRBACPolicy(p *PolicyData, client clientset.Interface) (done bool, err error) {
	failedReconciliation := false
	// 开始对账前先通过 RBAC API 读取集群角色和绑定，确认 etcd 及存储链路已经可用。
	if _, err := client.RbacV1().ClusterRoles().List(context.TODO(), metav1.ListOptions{}); err != nil {
		utilruntime.HandleError(fmt.Errorf("unable to initialize clusterroles: %v", err))
		return false, nil
	}
	if _, err := client.RbacV1().ClusterRoleBindings().List(context.TODO(), metav1.ListOptions{}); err != nil {
		utilruntime.HandleError(fmt.Errorf("unable to initialize clusterrolebindings: %v", err))
		return false, nil
	}

	// 如果目标聚合 ClusterRole 尚不存在，则需要在新名称下复制旧角色，迁移其历史自定义规则。
	if err := primeAggregatedClusterRoles(p.ClusterRolesToAggregate, client.RbacV1()); err != nil {
		utilruntime.HandleError(fmt.Errorf("unable to prime aggregated clusterroles: %v", err))
		return false, nil
	}

	if err := primeSplitClusterRoleBindings(p.ClusterRoleBindingsToSplit, client.RbacV1()); err != nil {
		utilruntime.HandleError(fmt.Errorf("unable to prime split ClusterRoleBindings: %v", err))
		return false, nil
	}

	// 创建或对账集群级引导角色。
	for _, clusterRole := range p.ClusterRoles {
		opts := reconciliation.ReconcileRoleOptions{
			Role:    reconciliation.ClusterRoleRuleOwner{ClusterRole: &clusterRole},
			Client:  reconciliation.ClusterRoleModifier{Client: client.RbacV1().ClusterRoles()},
			Confirm: true,
		}
		// 存储版本升级阻塞 API Server 时，更新请求可能返回 ServiceUnavailable，需要纳入重试。
		err := retryOnConflictOrServiceUnavailable(retry.DefaultBackoff, func() error {
			result, err := opts.Run()
			if err != nil {
				return err
			}
			switch {
			case result.Protected && result.Operation != reconciliation.ReconcileNone:
				klog.Warningf("skipped reconcile-protected clusterrole.%s/%s with missing permissions: %v", rbac.GroupName, clusterRole.Name, result.MissingRules)
			case result.Operation == reconciliation.ReconcileUpdate:
				klog.V(2).Infof("updated clusterrole.%s/%s with additional permissions: %v", rbac.GroupName, clusterRole.Name, result.MissingRules)
			case result.Operation == reconciliation.ReconcileCreate:
				klog.V(2).Infof("created clusterrole.%s/%s", rbac.GroupName, clusterRole.Name)
			}
			return nil
		})
		if err != nil {
			// 单个对象对账失败不立即终止整个批次；继续处理其他对象，并在本轮结束后统一重试。
			utilruntime.HandleError(fmt.Errorf("unable to reconcile clusterrole.%s/%s: %v", rbac.GroupName, clusterRole.Name, err))
			failedReconciliation = true
		}
	}

	// 创建或对账集群级引导 RoleBinding。
	for _, clusterRoleBinding := range p.ClusterRoleBindings {
		opts := reconciliation.ReconcileRoleBindingOptions{
			RoleBinding: reconciliation.ClusterRoleBindingAdapter{ClusterRoleBinding: &clusterRoleBinding},
			Client:      reconciliation.ClusterRoleBindingClientAdapter{Client: client.RbacV1().ClusterRoleBindings()},
			Confirm:     true,
		}
		// 存储版本升级阻塞 API Server 时，更新请求可能返回 ServiceUnavailable，需要纳入重试。
		err := retryOnConflictOrServiceUnavailable(retry.DefaultBackoff, func() error {
			result, err := opts.Run()
			if err != nil {
				return err
			}
			switch {
			case result.Protected && result.Operation != reconciliation.ReconcileNone:
				klog.Warningf("skipped reconcile-protected clusterrolebinding.%s/%s with missing subjects: %v", rbac.GroupName, clusterRoleBinding.Name, result.MissingSubjects)
			case result.Operation == reconciliation.ReconcileUpdate:
				klog.V(2).Infof("updated clusterrolebinding.%s/%s with additional subjects: %v", rbac.GroupName, clusterRoleBinding.Name, result.MissingSubjects)
			case result.Operation == reconciliation.ReconcileCreate:
				klog.V(2).Infof("created clusterrolebinding.%s/%s", rbac.GroupName, clusterRoleBinding.Name)
			case result.Operation == reconciliation.ReconcileRecreate:
				klog.V(2).Infof("recreated clusterrolebinding.%s/%s", rbac.GroupName, clusterRoleBinding.Name)
			}
			return nil
		})
		if err != nil {
			// 单个对象对账失败不立即终止整个批次；继续处理其他对象，并在本轮结束后统一重试。
			utilruntime.HandleError(fmt.Errorf("unable to reconcile clusterrolebinding.%s/%s: %v", rbac.GroupName, clusterRoleBinding.Name, err))
			failedReconciliation = true
		}
	}

	// 创建或对账各命名空间中的引导 Role。
	for namespace, roles := range p.Roles {
		for _, role := range roles {
			opts := reconciliation.ReconcileRoleOptions{
				Role:    reconciliation.RoleRuleOwner{Role: &role},
				Client:  reconciliation.RoleModifier{Client: client.RbacV1(), NamespaceClient: client.CoreV1().Namespaces()},
				Confirm: true,
			}
			// 存储版本升级阻塞 API Server 时，更新请求可能返回 ServiceUnavailable，需要纳入重试。
			err := retryOnConflictOrServiceUnavailable(retry.DefaultBackoff, func() error {
				result, err := opts.Run()
				if err != nil {
					return err
				}
				switch {
				case result.Protected && result.Operation != reconciliation.ReconcileNone:
					klog.Warningf("skipped reconcile-protected role.%s/%s in %v with missing permissions: %v", rbac.GroupName, role.Name, namespace, result.MissingRules)
				case result.Operation == reconciliation.ReconcileUpdate:
					klog.V(2).Infof("updated role.%s/%s in %v with additional permissions: %v", rbac.GroupName, role.Name, namespace, result.MissingRules)
				case result.Operation == reconciliation.ReconcileCreate:
					klog.V(2).Infof("created role.%s/%s in %v", rbac.GroupName, role.Name, namespace)
				}
				return nil
			})
			if err != nil {
				// 单个对象对账失败不立即终止整个批次；继续处理其他对象，并在本轮结束后统一重试。
				utilruntime.HandleError(fmt.Errorf("unable to reconcile role.%s/%s in %v: %v", rbac.GroupName, role.Name, namespace, err))
				failedReconciliation = true
			}
		}
	}

	// 创建或对账各命名空间中的引导 RoleBinding。
	for namespace, roleBindings := range p.RoleBindings {
		for _, roleBinding := range roleBindings {
			opts := reconciliation.ReconcileRoleBindingOptions{
				RoleBinding: reconciliation.RoleBindingAdapter{RoleBinding: &roleBinding},
				Client:      reconciliation.RoleBindingClientAdapter{Client: client.RbacV1(), NamespaceClient: client.CoreV1().Namespaces()},
				Confirm:     true,
			}
			// 存储版本升级阻塞 API Server 时，更新请求可能返回 ServiceUnavailable，需要纳入重试。
			err := retryOnConflictOrServiceUnavailable(retry.DefaultBackoff, func() error {
				result, err := opts.Run()
				if err != nil {
					return err
				}
				switch {
				case result.Protected && result.Operation != reconciliation.ReconcileNone:
					klog.Warningf("skipped reconcile-protected rolebinding.%s/%s in %v with missing subjects: %v", rbac.GroupName, roleBinding.Name, namespace, result.MissingSubjects)
				case result.Operation == reconciliation.ReconcileUpdate:
					klog.V(2).Infof("updated rolebinding.%s/%s in %v with additional subjects: %v", rbac.GroupName, roleBinding.Name, namespace, result.MissingSubjects)
				case result.Operation == reconciliation.ReconcileCreate:
					klog.V(2).Infof("created rolebinding.%s/%s in %v", rbac.GroupName, roleBinding.Name, namespace)
				case result.Operation == reconciliation.ReconcileRecreate:
					klog.V(2).Infof("recreated rolebinding.%s/%s in %v", rbac.GroupName, roleBinding.Name, namespace)
				}
				return nil
			})
			if err != nil {
				// 单个对象对账失败不立即终止整个批次；继续处理其他对象，并在本轮结束后统一重试。
				utilruntime.HandleError(fmt.Errorf("unable to reconcile rolebinding.%s/%s in %v: %v", rbac.GroupName, roleBinding.Name, namespace, err))
				failedReconciliation = true
			}
		}
	}
	// 只要有对象对账失败，就返回未完成状态，让 PostStart Hook 在下一轮继续重试。
	if failedReconciliation {
		return false, nil
	}

	return true, nil
}

func (p RESTStorageProvider) GroupName() string {
	return rbac.GroupName
}

// primeAggregatedClusterRoles 迁移已经转为聚合角色的旧角色，确保旧角色上尚未迁移的自定义规则不会丢失。
func primeAggregatedClusterRoles(clusterRolesToAggregate map[string]string, clusterRoleClient rbacv1client.ClusterRolesGetter) error {
	for oldName, newName := range clusterRolesToAggregate {
		_, err := clusterRoleClient.ClusterRoles().Get(context.TODO(), newName, metav1.GetOptions{})
		if err == nil {
			continue
		}
		if !apierrors.IsNotFound(err) {
			return err
		}

		existingRole, err := clusterRoleClient.ClusterRoles().Get(context.TODO(), oldName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}
		if existingRole.AggregationRule != nil {
			// 旧角色本身已经是聚合角色，说明迁移已完成，此时没有需要复制的自定义规则。
			return nil
		}
		klog.V(1).Infof("migrating %v to %v", existingRole.Name, newName)
		existingRole.Name = newName
		existingRole.ResourceVersion = "" // 清空 ResourceVersion，使该对象可以作为新资源创建。
		if _, err := clusterRoleClient.ClusterRoles().Create(context.TODO(), existingRole, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}

	return nil
}

// primeSplitClusterRoleBindings 确保拆分后的目标 ClusterRoleBinding 存在；如果源绑定存在，则将其 Subjects、Annotations 和 Labels 复制到目标模板。
func primeSplitClusterRoleBindings(clusterRoleBindingToSplit map[string]rbacapiv1.ClusterRoleBinding, clusterRoleBindingClient rbacv1client.ClusterRoleBindingsGetter) error {
	for existingBindingName, clusterRoleBindingToCreate := range clusterRoleBindingToSplit {
		// 如果源 ClusterRoleBinding 不存在，则跳过该迁移项。
		existingRoleBinding, err := clusterRoleBindingClient.ClusterRoleBindings().Get(context.TODO(), existingBindingName, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return err
		}

		// 如果目标 ClusterRoleBinding 已存在，则无需重复创建。
		_, err = clusterRoleBindingClient.ClusterRoleBindings().Get(context.TODO(), clusterRoleBindingToCreate.Name, metav1.GetOptions{})
		if err == nil {
			continue
		}
		if !apierrors.IsNotFound(err) {
			return err
		}

		// 如果源绑定存在而目标绑定不存在，则复制源对象的 Subjects、Labels 和 Annotations 来创建目标绑定。
		klog.V(1).Infof("copying subjects, labels, and annotations from ClusterRoleBinding %q to template %q", existingBindingName, clusterRoleBindingToCreate.Name)
		newCRB := clusterRoleBindingToCreate.DeepCopy()
		newCRB.Subjects = existingRoleBinding.Subjects
		newCRB.Labels = existingRoleBinding.Labels
		newCRB.Annotations = existingRoleBinding.Annotations
		if _, err := clusterRoleBindingClient.ClusterRoleBindings().Create(context.TODO(), newCRB, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}
