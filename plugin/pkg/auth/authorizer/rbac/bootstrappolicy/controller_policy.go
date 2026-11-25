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

	"k8s.io/klog/v2"

	capi "k8s.io/api/certificates/v1beta1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	genericfeatures "k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	rbacv1helpers "k8s.io/kubernetes/pkg/apis/rbac/v1"
	"k8s.io/kubernetes/pkg/controlplane/controller/legacytokentracking"
	"k8s.io/kubernetes/pkg/features"
)

// saRolePrefix 是控制器 ServiceAccount 角色的名称前缀。
// 每个控制器的角色名为 system:controller:<controller-name>，
// 对应的 ServiceAccount 名为 <controller-name>。
const saRolePrefix = "system:controller:"

// addControllerRole 添加控制器的 ClusterRole 和 ClusterRoleBinding。
// 角色名必须以 system:controller: 开头，避免重复注册。
func addControllerRole(controllerRoles *[]rbacv1.ClusterRole, controllerRoleBindings *[]rbacv1.ClusterRoleBinding, role rbacv1.ClusterRole) {
	if !strings.HasPrefix(role.Name, saRolePrefix) {
		klog.Fatalf(`role %q must start with %q`, role.Name, saRolePrefix)
	}

	for _, existingRole := range *controllerRoles {
		if role.Name == existingRole.Name {
			klog.Fatalf("role %q was already registered", role.Name)
		}
	}

	*controllerRoles = append(*controllerRoles, role)
	addClusterRoleLabel(*controllerRoles)

	*controllerRoleBindings = append(*controllerRoleBindings,
		rbacv1helpers.NewClusterBinding(role.Name).SAs("kube-system", role.Name[len(saRolePrefix):]).BindingOrDie())
	addClusterRoleBindingLabel(*controllerRoleBindings)
}

func eventsRule() rbacv1.PolicyRule {
	// 所有控制器都可以创建/更新/patch Event 对象
	return rbacv1helpers.NewRule("create", "update", "patch").Groups(legacyGroup, eventsGroup).Resources("events").RuleOrDie()
}

// buildControllerRoles 构建所有控制器的 ClusterRole 和 ClusterRoleBinding。
// 包括 attachdetach、cronjob、daemon-set、deployment、replicaset、
// statefulset、job、garbagecollector、namespace、serviceaccount、
// bootstrap-signer、token-cleaner、node-ipam、endpoint、
// resourceclaim、podgc 等控制器的权限。
func buildControllerRoles() ([]rbacv1.ClusterRole, []rbacv1.ClusterRoleBinding) {
	// controllerRoles 保存控制器使用的 ClusterRole 的切片。
	controllerRoles := []rbacv1.ClusterRole{}
	// controllerRoleBindings 保存控制器使用的 ClusterRoleBinding 的切片。
	controllerRoleBindings := []rbacv1.ClusterRoleBinding{}

	addControllerRole(&controllerRoles, &controllerRoleBindings, func() rbacv1.ClusterRole {
		role := rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "attachdetach-controller"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("list", "watch").Groups(legacyGroup).Resources("persistentvolumes", "persistentvolumeclaims").RuleOrDie(),
				rbacv1helpers.NewRule("get", "list", "watch").Groups(legacyGroup).Resources("nodes").RuleOrDie(),
				rbacv1helpers.NewRule("patch", "update").Groups(legacyGroup).Resources("nodes/status").RuleOrDie(),
				rbacv1helpers.NewRule("list", "watch").Groups(legacyGroup).Resources("pods").RuleOrDie(),
				eventsRule(),
				rbacv1helpers.NewRule("get", "create", "delete", "list", "watch").Groups(storageGroup).Resources("volumeattachments").RuleOrDie(),
			},
		}

		role.Rules = append(role.Rules, rbacv1helpers.NewRule("get", "watch", "list").Groups("storage.k8s.io").Resources("csidrivers").RuleOrDie())
		role.Rules = append(role.Rules, rbacv1helpers.NewRule("get", "watch", "list").Groups("storage.k8s.io").Resources("csinodes").RuleOrDie())

		return role
	}())

	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "clusterrole-aggregation-controller"},
		Rules: []rbacv1.PolicyRule{
			// 此控制器必须拥有 clusterroles 的完整权限，以便以任意方式修改它们。
			rbacv1helpers.NewRule("escalate", "get", "list", "watch", "update", "patch").Groups(rbacGroup).Resources("clusterroles").RuleOrDie(),
		},
	})
	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "cronjob-controller"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch", "update").Groups(batchGroup).Resources("cronjobs").RuleOrDie(),
			rbacv1helpers.NewRule("get", "list", "watch", "create", "update", "delete", "patch").Groups(batchGroup).Resources("jobs").RuleOrDie(),
			rbacv1helpers.NewRule("update").Groups(batchGroup).Resources("cronjobs/status").RuleOrDie(),
			rbacv1helpers.NewRule("update").Groups(batchGroup).Resources("cronjobs/finalizers").RuleOrDie(),
			rbacv1helpers.NewRule("list", "watch", "delete").Groups(legacyGroup).Resources("pods").RuleOrDie(),
			eventsRule(),
		},
	})
	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "daemon-set-controller"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch").Groups(extensionsGroup, appsGroup).Resources("daemonsets").RuleOrDie(),
			rbacv1helpers.NewRule("update").Groups(extensionsGroup, appsGroup).Resources("daemonsets/status").RuleOrDie(),
			rbacv1helpers.NewRule("update").Groups(extensionsGroup, appsGroup).Resources("daemonsets/finalizers").RuleOrDie(),
			rbacv1helpers.NewRule("list", "watch").Groups(legacyGroup).Resources("nodes").RuleOrDie(),
			rbacv1helpers.NewRule("list", "watch", "create", "delete", "patch").Groups(legacyGroup).Resources("pods").RuleOrDie(),
			rbacv1helpers.NewRule("create").Groups(legacyGroup).Resources("pods/binding").RuleOrDie(),
			rbacv1helpers.NewRule("get", "list", "watch", "create", "delete", "update", "patch").Groups(appsGroup).Resources("controllerrevisions").RuleOrDie(),
			eventsRule(),
		},
	})
	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "deployment-controller"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch", "update").Groups(extensionsGroup, appsGroup).Resources("deployments").RuleOrDie(),
			rbacv1helpers.NewRule("update").Groups(extensionsGroup, appsGroup).Resources("deployments/status").RuleOrDie(),
			rbacv1helpers.NewRule("update").Groups(extensionsGroup, appsGroup).Resources("deployments/finalizers").RuleOrDie(),
			rbacv1helpers.NewRule("get", "list", "watch", "create", "update", "patch", "delete").Groups(appsGroup, extensionsGroup).Resources("replicasets").RuleOrDie(),
			// TODO: 在以下问题解决后移除 "update" 权限：
			// https://github.com/kubernetes/kubernetes/issues/36897
			rbacv1helpers.NewRule("get", "list", "watch", "update").Groups(legacyGroup).Resources("pods").RuleOrDie(),
			eventsRule(),
		},
	})
	addControllerRole(&controllerRoles, &controllerRoleBindings, func() rbacv1.ClusterRole {
		role := rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "disruption-controller"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("get", "list", "watch").Groups(extensionsGroup, appsGroup).Resources("deployments").RuleOrDie(),
				rbacv1helpers.NewRule("get", "list", "watch").Groups(appsGroup, extensionsGroup).Resources("replicasets").RuleOrDie(),
				rbacv1helpers.NewRule("get", "list", "watch").Groups(legacyGroup).Resources("replicationcontrollers").RuleOrDie(),
				rbacv1helpers.NewRule("get", "list", "watch").Groups(policyGroup).Resources("poddisruptionbudgets").RuleOrDie(),
				rbacv1helpers.NewRule("get", "list", "watch").Groups(appsGroup).Resources("statefulsets").RuleOrDie(),
				rbacv1helpers.NewRule("update").Groups(policyGroup).Resources("poddisruptionbudgets/status").RuleOrDie(),
				rbacv1helpers.NewRule("patch", "update").Groups(legacyGroup).Resources("pods/status").RuleOrDie(),
				rbacv1helpers.NewRule("get").Groups("*").Resources("*/scale").RuleOrDie(),
				eventsRule(),
			},
		}
		return role
	}())
	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "endpoint-controller"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch").Groups(legacyGroup).Resources("services", "pods").RuleOrDie(),
			rbacv1helpers.NewRule("get", "list", "watch", "create", "update", "delete").Groups(legacyGroup).Resources("endpoints").RuleOrDie(),
			rbacv1helpers.NewRule("create").Groups(legacyGroup).Resources("endpoints/restricted").RuleOrDie(),
			eventsRule(),
		},
	})

	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "endpointslice-controller"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch").Groups(legacyGroup).Resources("services", "pods", "nodes").RuleOrDie(),
			// 控制器需要能够设置 Service 的 finalizers，以创建 EndpointSlice
			// 资源；该资源由 Service 所有，并在 ownerRef 中设置 blockOwnerDeletion=true。
			rbacv1helpers.NewRule("update").Groups(legacyGroup).Resources("services/finalizers").RuleOrDie(),
			rbacv1helpers.NewRule("get", "list", "watch", "create", "update", "delete").Groups(discoveryGroup).Resources("endpointslices").RuleOrDie(),
			eventsRule(),
		},
	})

	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "endpointslicemirroring-controller"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch").Groups(legacyGroup).Resources("services", "endpoints").RuleOrDie(),
			// 控制器需要能够设置 Service 的 finalizers，以创建 EndpointSlice
			// 资源；该资源由 Service 所有，并在 ownerRef 中设置 blockOwnerDeletion=true。
			rbacv1helpers.NewRule("update").Groups(legacyGroup).Resources("services/finalizers").RuleOrDie(),
			// 控制器需要能够设置 Service 的 finalizers，以创建 EndpointSlice
			// 资源；该资源由 Endpoint 所有，并在 ownerRef 中设置 blockOwnerDeletion=true。
			// 参见 https://github.com/openshift/kubernetes/blob/8691466059314c3f7d6dcffcbb76d14596ca716c/pkg/controller/endpointslicemirroring/utils.go#L87-L88
			rbacv1helpers.NewRule("update").Groups(legacyGroup).Resources("endpoints/finalizers").RuleOrDie(),
			rbacv1helpers.NewRule("get", "list", "watch", "create", "update", "delete").Groups(discoveryGroup).Resources("endpointslices").RuleOrDie(),
			eventsRule(),
		},
	})

	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "expand-controller"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch", "update", "patch").Groups(legacyGroup).Resources("persistentvolumes").RuleOrDie(),
			rbacv1helpers.NewRule("update", "patch").Groups(legacyGroup).Resources("persistentvolumeclaims/status").RuleOrDie(),
			rbacv1helpers.NewRule("get", "list", "watch").Groups(legacyGroup).Resources("persistentvolumeclaims").RuleOrDie(),
			eventsRule(),
		},
	})

	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "ephemeral-volume-controller"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch").Groups(legacyGroup).Resources("pods").RuleOrDie(),
			rbacv1helpers.NewRule("update").Groups(legacyGroup).Resources("pods/finalizers").RuleOrDie(),
			rbacv1helpers.NewRule("get", "list", "watch", "create").Groups(legacyGroup).Resources("persistentvolumeclaims").RuleOrDie(),
			eventsRule(),
		},
	})

	if utilfeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) {
		rules := []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch").Groups(legacyGroup).Resources("pods").RuleOrDie(),
			rbacv1helpers.NewRule("update").Groups(legacyGroup).Resources("pods/finalizers").RuleOrDie(),
			rbacv1helpers.NewRule("get", "list", "watch", "create", "delete").Groups(resourceGroup).Resources("resourceclaims").RuleOrDie(),
			rbacv1helpers.NewRule("update", "patch").Groups(resourceGroup).Resources("resourceclaims", "resourceclaims/status").RuleOrDie(),
			rbacv1helpers.NewRule("update", "patch").Groups(legacyGroup).Resources("pods/status").RuleOrDie(),
			eventsRule(),
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.DRAWorkloadResourceClaims) {
			rules = append(rules,
				rbacv1helpers.NewRule("get", "list", "watch").Groups(schedulingGroup).Resources("podgroups").RuleOrDie(),
				rbacv1helpers.NewRule("update", "patch").Groups(schedulingGroup).Resources("podgroups/status").RuleOrDie(),
			)
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.DRAResourceClaimGranularStatusAuthorization) {
			rules = append(rules,
				rbacv1helpers.NewRule("update", "patch").Groups(resourceGroup).Resources("resourceclaims/binding").RuleOrDie(),
			)
		}
		addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "resource-claim-controller"},
			Rules:      rules,
		})
		if utilfeature.DefaultFeatureGate.Enabled(features.DRADeviceTaints) {
			rules := []rbacv1.PolicyRule{
				// 删除 Pod 以将其驱逐。
				rbacv1helpers.NewRule("get", "list", "watch", "delete").Groups(legacyGroup).Resources("pods").RuleOrDie(),
				// 设置 Pod 条件。
				rbacv1helpers.NewRule("update", "patch").Groups(legacyGroup).Resources("pods/status").RuleOrDie(),
				// 其余权限均为只读。
				rbacv1helpers.NewRule("get", "list", "watch").Groups(resourceGroup).Resources("resourceclaims").RuleOrDie(),
				rbacv1helpers.NewRule("get", "list", "watch").Groups(resourceGroup).Resources("resourceslices").RuleOrDie(),
				rbacv1helpers.NewRule("get", "list", "watch").Groups(resourceGroup).Resources("deviceclasses").RuleOrDie(),
				eventsRule(),
			}

			if utilfeature.DefaultFeatureGate.Enabled(features.DRADeviceTaintRules) {
				rules = append(rules,
					// 设置 DeviceTaintRule 条件。
					rbacv1helpers.NewRule("update", "patch").Groups(resourceGroup).Resources("devicetaintrules/status").RuleOrDie(),
					// spec 为只读。
					rbacv1helpers.NewRule("get", "list", "watch").Groups(resourceGroup).Resources("devicetaintrules").RuleOrDie(),
				)
			}

			addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
				// 名称与 k8s.io/kubernetes/cmd/kube-controller-manager/names 中相同。
				ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "device-taint-eviction-controller"},
				Rules:      rules,
			})
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.DRAResourcePoolStatus) {
			addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
				// 名称与 k8s.io/kubernetes/cmd/kube-controller-manager/names 中相同。
				ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "resourcepoolstatusrequest-controller"},
				Rules: []rbacv1.PolicyRule{
					// 读取和删除 ResourcePoolStatusRequest（TTL 清理需要删除权限）
					rbacv1helpers.NewRule("get", "list", "watch", "delete").Groups(resourceGroup).Resources("resourcepoolstatusrequests").RuleOrDie(),
					// 处理后更新状态
					rbacv1helpers.NewRule("update", "patch").Groups(resourceGroup).Resources("resourcepoolstatusrequests/status").RuleOrDie(),
					// 读取 ResourceSlice 以计算资源池状态
					rbacv1helpers.NewRule("get", "list", "watch").Groups(resourceGroup).Resources("resourceslices").RuleOrDie(),
					// 读取 ResourceClaim 以计算分配数量
					rbacv1helpers.NewRule("get", "list", "watch").Groups(resourceGroup).Resources("resourceclaims").RuleOrDie(),
					// 读取 DeviceTaintRule，将有污点设备计为不可用
					rbacv1helpers.NewRule("get", "list", "watch").Groups(resourceGroup).Resources("devicetaintrules").RuleOrDie(),
					eventsRule(),
				},
			})
		}
	}

	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "generic-garbage-collector"},
		Rules: []rbacv1.PolicyRule{
			// GC 控制器需要对任意资源执行 list/watch、选择性 get 和 update
			rbacv1helpers.NewRule("get", "list", "watch", "patch", "update", "delete").Groups("*").Resources("*").RuleOrDie(),
			eventsRule(),
		},
	})
	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "horizontal-pod-autoscaler"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch").Groups(autoscalingGroup).Resources("horizontalpodautoscalers").RuleOrDie(),
			rbacv1helpers.NewRule("update").Groups(autoscalingGroup).Resources("horizontalpodautoscalers/status").RuleOrDie(),
			rbacv1helpers.NewRule("get", "update").Groups("*").Resources("*/scale").RuleOrDie(),
			rbacv1helpers.NewRule("list", "watch").Groups(legacyGroup).Resources("pods").RuleOrDie(),
			// 允许列出 resource、custom 和 external 指标
			rbacv1helpers.NewRule("list", "watch").Groups(resMetricsGroup).Resources("pods").RuleOrDie(),
			rbacv1helpers.NewRule("get", "list", "watch").Groups(customMetricsGroup).Resources("*").RuleOrDie(),
			rbacv1helpers.NewRule("get", "list", "watch").Groups(externalMetricsGroup).Resources("*").RuleOrDie(),
			eventsRule(),
		},
	})
	jobControllerRules := []rbacv1.PolicyRule{
		rbacv1helpers.NewRule("get", "list", "watch", "update", "patch").Groups(batchGroup).Resources("jobs").RuleOrDie(),
		rbacv1helpers.NewRule("update").Groups(batchGroup).Resources("jobs/status").RuleOrDie(),
		rbacv1helpers.NewRule("update").Groups(batchGroup).Resources("jobs/finalizers").RuleOrDie(),
		rbacv1helpers.NewRule("list", "watch", "create", "delete", "patch").Groups(legacyGroup).Resources("pods").RuleOrDie(),
		eventsRule(),
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.WorkloadWithJob) {
		jobControllerRules = append(jobControllerRules,
			rbacv1helpers.NewRule("get", "list", "watch", "create", "update", "patch").Groups(schedulingGroup).Resources("workloads", "podgroups").RuleOrDie(),
		)
	}
	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "job-controller"},
		Rules:      jobControllerRules,
	})
	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "namespace-controller"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch", "delete").Groups(legacyGroup).Resources("namespaces").RuleOrDie(),
			rbacv1helpers.NewRule("update").Groups(legacyGroup).Resources("namespaces/finalize", "namespaces/status").RuleOrDie(),
			rbacv1helpers.NewRule("get", "list", "watch", "delete", "deletecollection").Groups("*").Resources("*").RuleOrDie(),
		},
	})
	addControllerRole(&controllerRoles, &controllerRoleBindings, func() rbacv1.ClusterRole {
		role := rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "node-controller"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("get", "list", "watch", "update", "delete", "patch").Groups(legacyGroup).Resources("nodes").RuleOrDie(),
				rbacv1helpers.NewRule("patch", "update").Groups(legacyGroup).Resources("nodes/status").RuleOrDie(),
				// 用于删除 Pod
				rbacv1helpers.NewRule("patch", "update").Groups(legacyGroup).Resources("pods/status").RuleOrDie(),
				rbacv1helpers.NewRule("list", "watch", "get", "delete").Groups(legacyGroup).Resources("pods").RuleOrDie(),
				rbacv1helpers.NewRule("get", "list", "watch").Groups(coordinationGroup).Resources("leases").RuleOrDie(),
				eventsRule(),
			},
		}
		return role
	}())
	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "persistent-volume-binder"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch", "update", "create", "delete").Groups(legacyGroup).Resources("persistentvolumes").RuleOrDie(),
			rbacv1helpers.NewRule("update").Groups(legacyGroup).Resources("persistentvolumes/status").RuleOrDie(),
			rbacv1helpers.NewRule("get", "list", "watch", "update").Groups(legacyGroup).Resources("persistentvolumeclaims").RuleOrDie(),
			rbacv1helpers.NewRule("update").Groups(legacyGroup).Resources("persistentvolumeclaims/status").RuleOrDie(),
			rbacv1helpers.NewRule("list", "watch", "get", "create", "delete").Groups(legacyGroup).Resources("pods").RuleOrDie(),
			rbacv1helpers.NewRule("get", "list", "watch").Groups(storageGroup).Resources("storageclasses").RuleOrDie(),
			rbacv1helpers.NewRule("list", "watch").Groups(legacyGroup).Resources("nodes").RuleOrDie(),
			eventsRule(),

			// recyclerClient.WatchPod
			rbacv1helpers.NewRule("watch").Groups(legacyGroup).Resources("events").RuleOrDie(),
		},
	})
	addControllerRole(&controllerRoles, &controllerRoleBindings, func() rbacv1.ClusterRole {
		role := rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "pod-garbage-collector"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("list", "watch", "delete").Groups(legacyGroup).Resources("pods").RuleOrDie(),
				rbacv1helpers.NewRule("get", "list", "watch").Groups(legacyGroup).Resources("nodes").RuleOrDie(),
				rbacv1helpers.NewRule("patch").Groups(legacyGroup).Resources("pods/status").RuleOrDie(),
			},
		}
		return role
	}())
	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "replicaset-controller"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch", "update").Groups(appsGroup, extensionsGroup).Resources("replicasets").RuleOrDie(),
			rbacv1helpers.NewRule("update").Groups(appsGroup, extensionsGroup).Resources("replicasets/status").RuleOrDie(),
			rbacv1helpers.NewRule("update").Groups(appsGroup, extensionsGroup).Resources("replicasets/finalizers").RuleOrDie(),
			rbacv1helpers.NewRule("list", "watch", "patch", "create", "delete").Groups(legacyGroup).Resources("pods").RuleOrDie(),
			eventsRule(),
		},
	})
	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "replication-controller"},
		Rules: []rbacv1.PolicyRule{
			// 1.0 控制器需要 get 和 update 权限；缺少这些权限会导致旧控制器无法在新服务端上运行
			rbacv1helpers.NewRule("get", "list", "watch", "update").Groups(legacyGroup).Resources("replicationcontrollers").RuleOrDie(),
			rbacv1helpers.NewRule("update").Groups(legacyGroup).Resources("replicationcontrollers/status").RuleOrDie(),
			rbacv1helpers.NewRule("update").Groups(legacyGroup).Resources("replicationcontrollers/finalizers").RuleOrDie(),
			rbacv1helpers.NewRule("list", "watch", "patch", "create", "delete").Groups(legacyGroup).Resources("pods").RuleOrDie(),
			eventsRule(),
		},
	})
	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "resourcequota-controller"},
		Rules: []rbacv1.PolicyRule{
			// 配额控制器需要对任意资源计量配额以进行协调，因此需要完整的查看权限
			rbacv1helpers.NewRule("list", "watch").Groups("*").Resources("*").RuleOrDie(),
			rbacv1helpers.NewRule("update").Groups(legacyGroup).Resources("resourcequotas/status").RuleOrDie(),
			eventsRule(),
		},
	})
	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "route-controller"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("list", "watch").Groups(legacyGroup).Resources("nodes").RuleOrDie(),
			rbacv1helpers.NewRule("patch").Groups(legacyGroup).Resources("nodes/status").RuleOrDie(),
			eventsRule(),
		},
	})
	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "service-account-controller"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("create").Groups(legacyGroup).Resources("serviceaccounts").RuleOrDie(),
			eventsRule(),
		},
	})
	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "service-controller"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch").Groups(legacyGroup).Resources("services").RuleOrDie(),
			rbacv1helpers.NewRule("patch", "update").Groups(legacyGroup).Resources("services/status").RuleOrDie(),
			rbacv1helpers.NewRule("list", "watch").Groups(legacyGroup).Resources("nodes").RuleOrDie(),
			eventsRule(),
		},
	})
	if utilfeature.DefaultFeatureGate.Enabled(features.MultiCIDRServiceAllocator) {
		addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "service-cidrs-controller"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("get", "list", "watch", "patch", "update").Groups(networkingGroup).Resources("servicecidrs").RuleOrDie(),
				rbacv1helpers.NewRule("patch", "update").Groups(networkingGroup).Resources("servicecidrs/finalizers").RuleOrDie(),
				rbacv1helpers.NewRule("patch", "update").Groups(networkingGroup).Resources("servicecidrs/status").RuleOrDie(),
				rbacv1helpers.NewRule("get", "list", "watch").Groups(networkingGroup).Resources("ipaddresses").RuleOrDie(),
				eventsRule(),
			},
		})
	}
	addControllerRole(&controllerRoles, &controllerRoleBindings, func() rbacv1.ClusterRole {
		role := rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "statefulset-controller"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("list", "watch").Groups(legacyGroup).Resources("pods").RuleOrDie(),
				rbacv1helpers.NewRule("get", "list", "watch").Groups(appsGroup).Resources("statefulsets").RuleOrDie(),
				rbacv1helpers.NewRule("update").Groups(appsGroup).Resources("statefulsets/status").RuleOrDie(),
				rbacv1helpers.NewRule("update").Groups(appsGroup).Resources("statefulsets/finalizers").RuleOrDie(),
				rbacv1helpers.NewRule("get", "create", "delete", "update", "patch").Groups(legacyGroup).Resources("pods").RuleOrDie(),
				rbacv1helpers.NewRule("get", "create", "delete", "update", "patch", "list", "watch").Groups(appsGroup).Resources("controllerrevisions").RuleOrDie(),
				rbacv1helpers.NewRule("get", "create", "list", "watch").Groups(legacyGroup).Resources("persistentvolumeclaims").RuleOrDie(),
				rbacv1helpers.NewRule("update", "delete").Groups(legacyGroup).Resources("persistentvolumeclaims").RuleOrDie(),
				rbacv1helpers.NewRule("update").Groups(legacyGroup).Resources("pods/finalizers").RuleOrDie(),
				eventsRule(),
			},
		}

		return role
	}())
	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "ttl-controller"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("update", "patch", "list", "watch").Groups(legacyGroup).Resources("nodes").RuleOrDie(),
			eventsRule(),
		},
	})
	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "certificate-controller"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch", "delete").Groups(certificatesGroup).Resources("certificatesigningrequests").RuleOrDie(),
			rbacv1helpers.NewRule("update").Groups(certificatesGroup).Resources("certificatesigningrequests/status", "certificatesigningrequests/approval").RuleOrDie(),
			rbacv1helpers.NewRule("approve").Groups(certificatesGroup).Resources("signers").Names(capi.KubeAPIServerClientKubeletSignerName).RuleOrDie(),
			rbacv1helpers.NewRule("sign").Groups(certificatesGroup).Resources("signers").Names(
				capi.LegacyUnknownSignerName,
				capi.KubeAPIServerClientSignerName,
				capi.KubeAPIServerClientKubeletSignerName,
				capi.KubeletServingSignerName,
			).RuleOrDie(),
			rbacv1helpers.NewRule("create").Groups(authorizationGroup).Resources("subjectaccessreviews").RuleOrDie(),
			eventsRule(),
		},
	})
	if utilfeature.DefaultFeatureGate.Enabled(features.PodCertificateRequest) {
		addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "podcertificaterequestcleaner"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("get", "list", "watch", "delete").Groups(certificatesGroup).Resources("podcertificaterequests").RuleOrDie(),
			},
		})
	}
	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "pvc-protection-controller"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch", "update").Groups(legacyGroup).Resources("persistentvolumeclaims").RuleOrDie(),
			rbacv1helpers.NewRule("update").Groups(legacyGroup).Resources("persistentvolumeclaims/status").RuleOrDie(),
			rbacv1helpers.NewRule("list", "watch", "get").Groups(legacyGroup).Resources("pods").RuleOrDie(),
			eventsRule(),
		},
	})
	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "pv-protection-controller"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch", "update").Groups(legacyGroup).Resources("persistentvolumes").RuleOrDie(),
			eventsRule(),
		},
	})
	if utilfeature.DefaultFeatureGate.Enabled(features.VolumeAttributesClass) {
		addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "volumeattributesclass-protection-controller"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("list", "watch", "get").Groups(legacyGroup).Resources("persistentvolumeclaims").RuleOrDie(),
				rbacv1helpers.NewRule("list", "watch", "get").Groups(legacyGroup).Resources("persistentvolumes").RuleOrDie(),
				rbacv1helpers.NewRule("get", "list", "watch", "update").Groups(storageGroup).Resources("volumeattributesclasses").RuleOrDie(),
				eventsRule(),
			},
		})
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
		addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "podgroup-protection-controller"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("get", "list", "watch", "update").Groups(schedulingGroup).Resources("podgroups").RuleOrDie(),
				rbacv1helpers.NewRule("get", "list", "watch").Groups(legacyGroup).Resources("pods").RuleOrDie(),
			},
		})
	}

	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "ttl-after-finished-controller"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch", "delete").Groups(batchGroup).Resources("jobs").RuleOrDie(),
			eventsRule(),
		},
	})
	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "root-ca-cert-publisher"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("create", "update").Groups(legacyGroup).Resources("configmaps").RuleOrDie(),
			eventsRule(),
		},
	})

	if utilfeature.DefaultFeatureGate.Enabled(features.ClusterTrustBundle) {
		addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "kube-apiserver-serving-clustertrustbundle-publisher"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("attest").Groups(certificatesGroup).Resources("signers").Names("kubernetes.io/kube-apiserver-serving").RuleOrDie(),
				rbacv1helpers.NewRule("create", "update", "delete", "list", "watch").Groups(certificatesGroup).Resources("clustertrustbundles").RuleOrDie(),
				eventsRule(),
			},
		})
	}

	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "validatingadmissionpolicy-status-controller"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get", "list", "watch").Groups(admissionRegistrationGroup).
				Resources("validatingadmissionpolicies").RuleOrDie(),
			rbacv1helpers.NewRule("get", "patch", "update").Groups(admissionRegistrationGroup).
				Resources("validatingadmissionpolicies/status").RuleOrDie(),
			eventsRule(),
		},
	})

	if utilfeature.DefaultFeatureGate.Enabled(genericfeatures.StorageVersionAPI) &&
		utilfeature.DefaultFeatureGate.Enabled(genericfeatures.APIServerIdentity) {
		addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "storage-version-garbage-collector"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("get", "list", "watch").Groups(coordinationGroup).Resources("leases").RuleOrDie(),
				rbacv1helpers.NewRule("get", "list", "watch", "patch", "update", "delete").Groups(internalAPIServerGroup).
					Resources("storageversions").RuleOrDie(),
				rbacv1helpers.NewRule("get", "patch", "update").Groups(internalAPIServerGroup).
					Resources("storageversions/status").RuleOrDie(),
			},
		})
	}

	addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "legacy-service-account-token-cleaner"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get").Groups(legacyGroup).Resources("configmaps").Names(legacytokentracking.ConfigMapName).RuleOrDie(),
			rbacv1helpers.NewRule("patch", "delete").Groups(legacyGroup).Resources("secrets").RuleOrDie(),
		},
	})

	if utilfeature.DefaultFeatureGate.Enabled(features.StorageVersionMigrator) {
		addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{
				Name: saRolePrefix + "storage-version-migrator-controller",
			},
			Rules: []rbacv1.PolicyRule{
				// 需要对任意资源执行 list 以获取当前 RV
				// 需要对任意资源执行 patch 以执行迁移
				rbacv1helpers.NewRule("list", "patch").Groups("*").Resources("*").RuleOrDie(),
				rbacv1helpers.NewRule("get").Groups("apiextensions.k8s.io").Resources("customresourcedefinitions").RuleOrDie(),
				rbacv1helpers.NewRule("patch").Groups("apiextensions.k8s.io").Resources("customresourcedefinitions/status").RuleOrDie(),
				rbacv1helpers.NewRule("update").Groups(storageVersionMigrationGroup).Resources("storageversionmigrations/status").RuleOrDie(),
			},
		})
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.SELinuxChangePolicy) {
		addControllerRole(&controllerRoles, &controllerRoleBindings, rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: saRolePrefix + "selinux-warning-controller"},
			Rules: []rbacv1.PolicyRule{
				eventsRule(),
				rbacv1helpers.NewRule("get", "list", "watch").Groups(legacyGroup).Resources("persistentvolumes").RuleOrDie(),
				rbacv1helpers.NewRule("get", "list", "watch").Groups(legacyGroup).Resources("persistentvolumeclaims").RuleOrDie(),
				rbacv1helpers.NewRule("get", "list", "watch").Groups(legacyGroup).Resources("pods").RuleOrDie(),
				rbacv1helpers.NewRule("get", "list", "watch").Groups(storageGroup).Resources("csidrivers").RuleOrDie(),
			},
		})
	}

	return controllerRoles, controllerRoleBindings
}

// ControllerRoles 返回控制器使用的 ClusterRole。
func ControllerRoles() []rbacv1.ClusterRole {
	controllerRoles, _ := buildControllerRoles()
	return controllerRoles
}

// ControllerRoleBindings 返回控制器使用的 ClusterRoleBinding。
func ControllerRoleBindings() []rbacv1.ClusterRoleBinding {
	_, controllerRoleBindings := buildControllerRoles()
	return controllerRoleBindings
}
