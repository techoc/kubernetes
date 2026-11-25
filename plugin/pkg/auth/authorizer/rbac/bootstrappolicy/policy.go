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
	capi "k8s.io/api/certificates/v1beta1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/authentication/serviceaccount"
	"k8s.io/apiserver/pkg/authentication/user"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	zpagesfeatures "k8s.io/component-base/zpages/features"

	rbacv1helpers "k8s.io/kubernetes/pkg/apis/rbac/v1"
	"k8s.io/kubernetes/pkg/features"
)

// Write 和 ReadWrite 等是常用 verb 列表，Label 和 Annotation 是启动策略的默认元数据。
var (
	Write      = []string{"create", "update", "patch", "delete", "deletecollection"}
	ReadWrite  = []string{"get", "list", "watch", "create", "update", "patch", "delete", "deletecollection"}
	Read       = []string{"get", "list", "watch"}
	ReadUpdate = []string{"get", "list", "watch", "update", "patch"}

	Label      = map[string]string{"kubernetes.io/bootstrapping": "rbac-defaults"}
	Annotation = map[string]string{rbacv1.AutoUpdateAnnotationKey: "true"}
)

const (
	// API 组常量，用于构建 RBAC 规则时引用各个 API 组
	legacyGroup                  = ""
	appsGroup                    = "apps"
	authenticationGroup          = "authentication.k8s.io"
	authorizationGroup           = "authorization.k8s.io"
	autoscalingGroup             = "autoscaling"
	batchGroup                   = "batch"
	certificatesGroup            = "certificates.k8s.io"
	coordinationGroup            = "coordination.k8s.io"
	discoveryGroup               = "discovery.k8s.io"
	extensionsGroup              = "extensions"
	lifecycleGroup               = "lifecycle.k8s.io"
	policyGroup                  = "policy"
	rbacGroup                    = "rbac.authorization.k8s.io"
	resourceGroup                = "resource.k8s.io"
	storageGroup                 = "storage.k8s.io"
	resMetricsGroup              = "metrics.k8s.io"
	customMetricsGroup           = "custom.metrics.k8s.io"
	externalMetricsGroup         = "external.metrics.k8s.io"
	networkingGroup              = "networking.k8s.io"
	eventsGroup                  = "events.k8s.io"
	internalAPIServerGroup       = "internal.apiserver.k8s.io"
	admissionRegistrationGroup   = "admissionregistration.k8s.io"
	storageVersionMigrationGroup = "storagemigration.k8s.io"
	schedulingGroup              = "scheduling.k8s.io"
)

// addDefaultMetadata 为启动策略中的对象添加默认标签和注释。
// 标签 kubernetes.io/bootstrapping=rbac-defaults 标识这是系统自动创建的资源。
// 注释 rbac.authorization.k8s.io/auto-update 允许系统自动更新内置角色。
func addDefaultMetadata(obj runtime.Object) {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		// 若出现此错误，说明静态代码存在缺陷。
		panic(err)
	}

	labels := metadata.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	for k, v := range Label {
		labels[k] = v
	}
	metadata.SetLabels(labels)

	annotations := metadata.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	for k, v := range Annotation {
		annotations[k] = v
	}
	metadata.SetAnnotations(annotations)
}

// addClusterRoleLabel 为所有 ClusterRole 添加默认元数据。
func addClusterRoleLabel(roles []rbacv1.ClusterRole) {
	for i := range roles {
		addDefaultMetadata(&roles[i])
	}
	return
}

// addClusterRoleBindingLabel 为所有 ClusterRoleBinding 添加默认元数据。
func addClusterRoleBindingLabel(rolebindings []rbacv1.ClusterRoleBinding) {
	for i := range rolebindings {
		addDefaultMetadata(&rolebindings[i])
	}
	return
}

// viewRules 返回 view 角色的规则，提供对常见资源的只读访问。
// 包括 Pod、Service、ConfigMap、Deployment、StatefulSet 等用户级资源。
// 根据功能门控动态添加 DRA、GenericWorkload 等新功能的权限。
func viewRules() []rbacv1.PolicyRule {
	rules := []rbacv1.PolicyRule{
		rbacv1helpers.NewRule(Read...).Groups(legacyGroup).Resources("pods", "replicationcontrollers", "replicationcontrollers/scale", "serviceaccounts",
			"services", "services/status", "endpoints", "persistentvolumeclaims", "persistentvolumeclaims/status", "configmaps").RuleOrDie(),
		rbacv1helpers.NewRule(Read...).Groups(legacyGroup).Resources("limitranges", "resourcequotas", "bindings",
			"pods/status", "resourcequotas/status", "namespaces/status", "replicationcontrollers/status", "pods/log").RuleOrDie(),
		// 在命名空间范围内读取 namespaces，表示可以读取当前命名空间，
		// 可用于判断主体有权访问哪些命名空间。
		rbacv1helpers.NewRule(Read...).Groups(legacyGroup).Resources("namespaces").RuleOrDie(),

		rbacv1helpers.NewRule(Read...).Groups(legacyGroup, eventsGroup).Resources("events").RuleOrDie(),

		rbacv1helpers.NewRule(Read...).Groups(discoveryGroup).Resources("endpointslices").RuleOrDie(),

		rbacv1helpers.NewRule(Read...).Groups(appsGroup).Resources(
			"controllerrevisions",
			"statefulsets", "statefulsets/status", "statefulsets/scale",
			"daemonsets", "daemonsets/status",
			"deployments", "deployments/status", "deployments/scale",
			"replicasets", "replicasets/status", "replicasets/scale").RuleOrDie(),

		rbacv1helpers.NewRule(Read...).Groups(autoscalingGroup).Resources("horizontalpodautoscalers", "horizontalpodautoscalers/status").RuleOrDie(),

		rbacv1helpers.NewRule(Read...).Groups(batchGroup).Resources("jobs", "cronjobs", "cronjobs/status", "jobs/status").RuleOrDie(),

		rbacv1helpers.NewRule(Read...).Groups(extensionsGroup).Resources("daemonsets", "daemonsets/status", "deployments", "deployments/scale", "deployments/status",
			"ingresses", "ingresses/status", "replicasets", "replicasets/scale", "replicasets/status", "replicationcontrollers/scale",
			"networkpolicies").RuleOrDie(),

		rbacv1helpers.NewRule(Read...).Groups(policyGroup).Resources("poddisruptionbudgets", "poddisruptionbudgets/status").RuleOrDie(),

		rbacv1helpers.NewRule(Read...).Groups(networkingGroup).Resources("networkpolicies", "ingresses", "ingresses/status").RuleOrDie(),
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) {
		rules = append(rules, rbacv1helpers.NewRule(Read...).Groups(resourceGroup).Resources("resourceclaims", "resourceclaims/status", "resourceclaimtemplates").RuleOrDie())
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
		rules = append(rules, rbacv1helpers.NewRule(Read...).Groups(schedulingGroup).Resources("workloads", "podgroups", "podgroups/status").RuleOrDie())
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.CompositePodGroup) {
		rules = append(rules, rbacv1helpers.NewRule(Read...).Groups(schedulingGroup).Resources("compositepodgroups", "compositepodgroups/status").RuleOrDie())
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.EvictionRequestAPI) {
		rules = append(rules, rbacv1helpers.NewRule(Read...).Groups(lifecycleGroup).Resources("evictionrequests", "evictions").RuleOrDie())
	}
	return rules
}

// editRules 返回 edit 角色的规则，提供对常见资源的读写访问。
// 包括创建/更新/删除 Pod、Service、Secret、ConfigMap 等。
// 不包括系统级资源（如 status 子资源、quota、limits）。
func editRules() []rbacv1.PolicyRule {
	rules := []rbacv1.PolicyRule{
		// 允许读取可能提升权限的资源。
		rbacv1helpers.NewRule(Read...).Groups(legacyGroup).Resources("pods/attach", "pods/proxy", "pods/exec", "pods/portforward", "secrets", "services/proxy").RuleOrDie(),
		rbacv1helpers.NewRule("impersonate").Groups(legacyGroup).Resources("serviceaccounts").RuleOrDie(),

		rbacv1helpers.NewRule(Write...).Groups(legacyGroup).Resources("pods", "pods/attach", "pods/proxy", "pods/exec", "pods/portforward").RuleOrDie(),
		rbacv1helpers.NewRule("create").Groups(legacyGroup).Resources("pods/eviction").RuleOrDie(),
		rbacv1helpers.NewRule(Write...).Groups(legacyGroup).Resources("replicationcontrollers", "replicationcontrollers/scale", "serviceaccounts",
			"services", "services/proxy", "persistentvolumeclaims", "configmaps", "secrets").RuleOrDie(),
		rbacv1helpers.NewRule("create").Groups(legacyGroup).Resources("serviceaccounts/token").RuleOrDie(),

		rbacv1helpers.NewRule(Write...).Groups(legacyGroup, eventsGroup).Resources("events").RuleOrDie(),

		rbacv1helpers.NewRule(Write...).Groups(appsGroup).Resources(
			"statefulsets", "statefulsets/scale",
			"daemonsets",
			"deployments", "deployments/scale", "deployments/rollback",
			"replicasets", "replicasets/scale").RuleOrDie(),

		rbacv1helpers.NewRule(Write...).Groups(autoscalingGroup).Resources("horizontalpodautoscalers").RuleOrDie(),

		rbacv1helpers.NewRule(Write...).Groups(batchGroup).Resources("jobs", "cronjobs").RuleOrDie(),

		rbacv1helpers.NewRule(Write...).Groups(extensionsGroup).Resources("daemonsets",
			"deployments", "deployments/scale", "deployments/rollback", "ingresses",
			"replicasets", "replicasets/scale", "replicationcontrollers/scale",
			"networkpolicies").RuleOrDie(),

		rbacv1helpers.NewRule(Write...).Groups(policyGroup).Resources("poddisruptionbudgets").RuleOrDie(),

		rbacv1helpers.NewRule(Write...).Groups(networkingGroup).Resources("networkpolicies", "ingresses").RuleOrDie(),

		rbacv1helpers.NewRule(ReadWrite...).Groups(coordinationGroup).Resources("leases").RuleOrDie(),
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) {
		rules = append(rules, rbacv1helpers.NewRule(Write...).Groups(resourceGroup).Resources("resourceclaims", "resourceclaimtemplates").RuleOrDie())
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
		rules = append(rules, rbacv1helpers.NewRule(Write...).Groups(schedulingGroup).Resources("workloads", "podgroups").RuleOrDie())
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.CompositePodGroup) {
		rules = append(rules, rbacv1helpers.NewRule(Write...).Groups(schedulingGroup).Resources("compositepodgroups").RuleOrDie())
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.EvictionRequestAPI) {
		// evictions 属于系统领域，只能由 evictionrequest-controller 更新；
		// evictionrequests 则面向用户。
		rules = append(rules, rbacv1helpers.NewRule(Write...).Groups(lifecycleGroup).Resources("evictionrequests").RuleOrDie())
	}
	return rules
}

// NodeRules 返回 system:node 角色的规则，定义 kubelet 的权限。
// kubelet 需要访问 Node、Pod、Secret、ConfigMap、PVC、PV 等资源，
// 但通过 Node 授权器和 NodeRestriction 准入插件进行细粒度限制。
func NodeRules() []rbacv1.PolicyRule {
	nodePolicyRules := []rbacv1.PolicyRule{
		// 检查 API 访问权限所需；这些 create 操作不会修改对象。
		rbacv1helpers.NewRule("create").Groups(authenticationGroup).Resources("tokenreviews").RuleOrDie(),
		rbacv1helpers.NewRule("create").Groups(authorizationGroup).Resources("subjectaccessreviews", "localsubjectaccessreviews").RuleOrDie(),

		// 构建 serviceLister，为 Service 填充环境变量。
		rbacv1helpers.NewRule(Read...).Groups(legacyGroup).Resources("services").RuleOrDie(),

		// 节点可以注册 Node API 对象并上报状态。
		// 使用 NodeRestriction 准入插件限制节点只能创建/更新自己的对象。
		rbacv1helpers.NewRule("create", "get", "list", "watch").Groups(legacyGroup).Resources("nodes").RuleOrDie(),
		rbacv1helpers.NewRule("update", "patch").Groups(legacyGroup).Resources("nodes/status").RuleOrDie(),
		rbacv1helpers.NewRule("update", "patch").Groups(legacyGroup).Resources("nodes").RuleOrDie(),

		// TODO：在 NodeRestriction 准入插件中将事件创建者限制为所属节点。
		rbacv1helpers.NewRule("create", "update", "patch").Groups(legacyGroup, eventsGroup).Resources("events").RuleOrDie(),

		// 使用 Node 授权器 将 get 限制为与节点相关的 Pod，
		// 并将 list/watch 限制为带有节点字段选择器的请求。
		rbacv1helpers.NewRule(Read...).Groups(legacyGroup).Resources("pods").RuleOrDie(),

		// 节点需要创建/删除 mirror pod。
		// 使用 NodeRestriction 准入插件限制节点只能创建/删除绑定到自身的 mirror pod。
		rbacv1helpers.NewRule("create", "delete").Groups(legacyGroup).Resources("pods").RuleOrDie(),
		// 节点需要上报其运行 Pod 的状态。
		// 使用 NodeRestriction 准入插件限制节点只能更新绑定到自身的 Pod 状态。
		rbacv1helpers.NewRule("update", "patch").Groups(legacyGroup).Resources("pods/status").RuleOrDie(),
		// 节点需要创建 Pod 驱逐请求。
		// 使用 NodeRestriction 准入插件限制节点只能驱逐绑定到自身的 Pod。
		rbacv1helpers.NewRule("create").Groups(legacyGroup).Resources("pods/eviction").RuleOrDie(),

		// imagePullSecrets、rbd/ceph 和 Secret 卷以及环境变量中的 Secret 需要此权限。
		// ConfigMap 卷和环境变量中的 ConfigMap 也需要此权限。
		// 使用节点授权模式将节点可 get 的 Secret/ConfigMap 限制为绑定到自身的 Pod 所引用的对象。
		rbacv1helpers.NewRule("get", "list", "watch").Groups(legacyGroup).Resources("secrets", "configmaps").RuleOrDie(),
		// 持久卷需要此权限。
		// 使用节点授权模式将节点可 get 的 PV/PVC 限制为绑定到自身的 Pod 所引用的对象。
		rbacv1helpers.NewRule("get").Groups(legacyGroup).Resources("persistentvolumeclaims", "persistentvolumes").RuleOrDie(),

		// TODO：将该权限纳入 Node 授权器，并限制为绑定到节点的 Pod 或 PV 所引用的 Endpoint。
		// glusterfs 卷需要此权限。
		rbacv1helpers.NewRule("get").Groups(legacyGroup).Resources("endpoints").RuleOrDie(),
		// 用于为节点创建专属客户端证书的 CertificateSigningRequest，并监听其签名结果。
		// kubelet 借此轮换自己的证书。
		rbacv1helpers.NewRule("create", "get", "list", "watch").Groups(certificatesGroup).Resources("certificatesigningrequests").RuleOrDie(),

		// Lease。
		rbacv1helpers.NewRule("get", "create", "update", "patch", "delete").Groups("coordination.k8s.io").Resources("leases").RuleOrDie(),

		// CSI 存储。
		rbacv1helpers.NewRule("get").Groups(storageGroup).Resources("volumeattachments").RuleOrDie(),

		// 使用 NodeRestriction 准入插件将节点可创建的令牌限制为绑定到该节点 Pod 的 ServiceAccount。
		rbacv1helpers.NewRule("create").Groups(legacyGroup).Resources("serviceaccounts/token").RuleOrDie(),
	}

	// 使用节点授权模式将节点可更新的 PVC 状态限制为绑定到自身的 Pod 所引用的 PVC。
	// 使用 NodeRestriction 准入插件限制节点只能更新 status 字段。
	pvcStatusPolicyRule := rbacv1helpers.NewRule("get", "update", "patch").Groups(legacyGroup).Resources("persistentvolumeclaims/status").RuleOrDie()
	nodePolicyRules = append(nodePolicyRules, pvcStatusPolicyRule)

	// CSI 存储。
	csiDriverRule := rbacv1helpers.NewRule("get", "watch", "list").Groups("storage.k8s.io").Resources("csidrivers").RuleOrDie()
	nodePolicyRules = append(nodePolicyRules, csiDriverRule)
	csiNodeInfoRule := rbacv1helpers.NewRule("get", "create", "update", "patch", "delete").Groups("storage.k8s.io").Resources("csinodes").RuleOrDie()
	nodePolicyRules = append(nodePolicyRules, csiNodeInfoRule)
	// 使用 节点授权模式 限制节点只能更新自己的 CSINode 状态。
	// 使用 NodeRestriction 准入插件限制节点只能更新 status 字段。
	if utilfeature.DefaultFeatureGate.Enabled(features.CSIVolumeHealth) {
		csiNodeStatusRule := rbacv1helpers.NewRule("get", "update", "patch").Groups("storage.k8s.io").Resources("csinodes/status").RuleOrDie()
		nodePolicyRules = append(nodePolicyRules, csiNodeStatusRule)
	}

	// RuntimeClass。
	nodePolicyRules = append(nodePolicyRules, rbacv1helpers.NewRule("get", "list", "watch").Groups("node.k8s.io").Resources("runtimeclasses").RuleOrDie())

	// DRA ResourceClaim。
	if utilfeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) {
		nodePolicyRules = append(nodePolicyRules, rbacv1helpers.NewRule("get").Groups(resourceGroup).Resources("resourceclaims").RuleOrDie())
		nodePolicyRules = append(nodePolicyRules, rbacv1helpers.NewRule("deletecollection").Groups(resourceGroup).Resources("resourceslices").RuleOrDie())
	}
	// kubelet 需要访问 ClusterTrustBundle，以支持 pemTrustAnchors 卷类型。
	if utilfeature.DefaultFeatureGate.Enabled(features.ClusterTrustBundle) {
		nodePolicyRules = append(nodePolicyRules, rbacv1helpers.NewRule("get", "list", "watch").Groups(certificatesGroup).Resources("clustertrustbundles").RuleOrDie())
	}
	// kubelet 需要访问 ServiceAccount，以向凭据提供程序发送 ServiceAccount token。
	// 使用 Node 授权器 将 get 限制为与节点相关的 ServiceAccount。
	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletServiceAccountTokenForCredentialProviders) {
		nodePolicyRules = append(nodePolicyRules, rbacv1helpers.NewRule("get").Groups(legacyGroup).Resources("serviceaccounts").RuleOrDie())
	}
	// kubelet 需要创建任意 PodCertificateRequest 以实现 podCertificate 卷。
	if utilfeature.DefaultFeatureGate.Enabled(features.PodCertificateRequest) {
		nodePolicyRules = append(nodePolicyRules, rbacv1helpers.NewRule("get", "list", "watch", "create").Groups(certificatesGroup).Resources("podcertificaterequests").RuleOrDie())
	}

	return nodePolicyRules
}

// ClusterRoles 返回集群启动时自动创建的 ClusterRole 列表。
// 包括 cluster-admin、system:discovery、system:monitoring、system:basic-user、
// system:public-info-viewer、admin、edit、view 等内置角色，
// 以及 kube-controller-manager、kube-scheduler、kube-proxy、kubelet 等组件角色。
func ClusterRoles() []rbacv1.ClusterRole {
	monitoringURLs := []string{
		"/metrics", "/metrics/slis",
		"/livez", "/readyz", "/healthz",
		"/livez/*", "/readyz/*", "/healthz/*",
	}

	if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentFlagz) {
		monitoringURLs = append(monitoringURLs, "/flagz")
	}

	if utilfeature.DefaultFeatureGate.Enabled(zpagesfeatures.ComponentStatusz) {
		monitoringURLs = append(monitoringURLs, "/statusz")
	}

	monitoringRules := []rbacv1.PolicyRule{
		rbacv1helpers.NewRule("get").URLs(monitoringURLs...).RuleOrDie(),

		// kubelet 指标需要此权限。
		rbacv1helpers.NewRule("get").Groups(legacyGroup).Resources("nodes/metrics").RuleOrDie(),
	}

	roles := []rbacv1.ClusterRole{
		{
			// 可执行任意操作的 root 角色。
			ObjectMeta: metav1.ObjectMeta{Name: "cluster-admin"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("*").Groups("*").Resources("*").RuleOrDie(),
				rbacv1helpers.NewRule("*").URLs("*").RuleOrDie(),
			},
		},
		{
			// 仅提供判断服务端就绪和发现 API 版本所需的最小权限，供协商使用。
			ObjectMeta: metav1.ObjectMeta{Name: "system:discovery"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("get").URLs(
					"/livez", "/readyz", "/healthz",
					"/version", "/version/",
					"/openapi", "/openapi/*",
					"/api", "/api/*",
					"/apis", "/apis/*",
				).RuleOrDie(),
			},
		},
		{
			// 仅提供监控端点的最小读取权限。
			// 通配健康检查端点允许读取单个检查项，这些端点可能包含更敏感的集群信息。
			ObjectMeta: metav1.ObjectMeta{Name: "system:monitoring"},
			Rules:      monitoringRules,
		},
	}

	basicUserRules := []rbacv1.PolicyRule{
		rbacv1helpers.NewRule("create").Groups(authorizationGroup).Resources("selfsubjectaccessreviews", "selfsubjectrulesreviews").RuleOrDie(),
		rbacv1helpers.NewRule("create").Groups(authenticationGroup).Resources("selfsubjectreviews").RuleOrDie(),
	}

	roles = append(roles, []rbacv1.ClusterRole{
		{
			// 提供最小资源访问权限，让普通用户查询自身信息。
			ObjectMeta: metav1.ObjectMeta{Name: "system:basic-user"},
			Rules:      basicUserRules,
		},
		{
			// 仅提供读取不敏感集群信息所需的权限。
			ObjectMeta: metav1.ObjectMeta{Name: "system:public-info-viewer"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("get").URLs(
					"/livez", "/readyz", "/healthz", "/version", "/version/",
				).RuleOrDie(),
			},
		},
		{
			// 命名空间管理员角色：拥有 edit 的全部权限，并可以向其他用户授权。
			ObjectMeta: metav1.ObjectMeta{Name: "admin"},
			AggregationRule: &rbacv1.AggregationRule{
				ClusterRoleSelectors: []metav1.LabelSelector{
					{MatchLabels: map[string]string{"rbac.authorization.k8s.io/aggregate-to-admin": "true"}},
				},
			},
		},
		{
			// 命名空间编辑角色：拥有命名空间内所有用户级操作的权限。
			// 不包含系统专属的 /status 子资源以及用于控制命名空间的 quota/limits。
			ObjectMeta: metav1.ObjectMeta{Name: "edit", Labels: map[string]string{"rbac.authorization.k8s.io/aggregate-to-admin": "true"}},
			AggregationRule: &rbacv1.AggregationRule{
				ClusterRoleSelectors: []metav1.LabelSelector{
					{MatchLabels: map[string]string{"rbac.authorization.k8s.io/aggregate-to-edit": "true"}},
				},
			},
		},
		{
			// 命名空间只读角色：仅允许读取命名空间内不会提升权限的资源。
			ObjectMeta: metav1.ObjectMeta{Name: "view", Labels: map[string]string{"rbac.authorization.k8s.io/aggregate-to-edit": "true"}},
			AggregationRule: &rbacv1.AggregationRule{
				ClusterRoleSelectors: []metav1.LabelSelector{
					{MatchLabels: map[string]string{"rbac.authorization.k8s.io/aggregate-to-view": "true"}},
				},
			},
		},
		{
			// 命名空间管理员角色：拥有 edit 的全部权限，并可以向其他用户授权。
			ObjectMeta: metav1.ObjectMeta{Name: "system:aggregate-to-admin", Labels: map[string]string{"rbac.authorization.k8s.io/aggregate-to-admin": "true"}},
			Rules: []rbacv1.PolicyRule{
				// 管理员角色的额外权限。
				rbacv1helpers.NewRule("create").Groups(authorizationGroup).Resources("localsubjectaccessreviews").RuleOrDie(),
				rbacv1helpers.NewRule(ReadWrite...).Groups(rbacGroup).Resources("roles", "rolebindings").RuleOrDie(),
			},
		},
		{
			// 命名空间编辑角色：拥有命名空间内所有用户级操作的权限。
			// 不包含系统专属的 /status 子资源以及用于控制命名空间的 quota/limits。
			ObjectMeta: metav1.ObjectMeta{Name: "system:aggregate-to-edit", Labels: map[string]string{"rbac.authorization.k8s.io/aggregate-to-edit": "true"}},
			Rules:      editRules(),
		},
		{
			// 命名空间只读角色：仅允许读取命名空间内不会提升权限的资源。
			ObjectMeta: metav1.ObjectMeta{Name: "system:aggregate-to-view", Labels: map[string]string{"rbac.authorization.k8s.io/aggregate-to-view": "true"}},
			Rules:      viewRules(),
		},
		{
			// 供 heapster 连接 API 服务器使用的角色。
			ObjectMeta: metav1.ObjectMeta{Name: "system:heapster"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule(Read...).Groups(legacyGroup).Resources("events", "pods", "nodes", "namespaces").RuleOrDie(),
				rbacv1helpers.NewRule(Read...).Groups(extensionsGroup).Resources("deployments").RuleOrDie(),
			},
		},
		{
			// 节点运行 Pod 所需的角色。
			ObjectMeta: metav1.ObjectMeta{Name: systemNodeRoleName},
			Rules:      NodeRules(),
		},
		{
			// 供 node-problem-detector 使用的角色；部署位置可能不同，因此不绑定到默认位置。
			ObjectMeta: metav1.ObjectMeta{Name: "system:node-problem-detector"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("get").Groups(legacyGroup).Resources("nodes").RuleOrDie(),
				rbacv1helpers.NewRule("patch").Groups(legacyGroup).Resources("nodes/status").RuleOrDie(),
				eventsRule(),
			},
		},
		{
			// 用于引导节点客户端证书的角色。
			ObjectMeta: metav1.ObjectMeta{Name: "system:node-bootstrapper"},
			Rules: []rbacv1.PolicyRule{
				// 用于创建节点专属客户端证书的 CertificateSigningRequest，并监听签名结果。
				rbacv1helpers.NewRule("create", "get", "list", "watch").Groups(certificatesGroup).Resources("certificatesigningrequests").RuleOrDie(),
			},
		},
		{
			// 用于认证和授权委托的角色。
			ObjectMeta: metav1.ObjectMeta{Name: "system:auth-delegator"},
			Rules: []rbacv1.PolicyRule{
				// 这些 create 操作不会修改对象。
				rbacv1helpers.NewRule("create").Groups(authenticationGroup).Resources("tokenreviews").RuleOrDie(),
				rbacv1helpers.NewRule("create").Groups(authorizationGroup).Resources("subjectaccessreviews").RuleOrDie(),
			},
		},
		{
			// 用于 API 注册、聚合和代理处理的角色。
			ObjectMeta: metav1.ObjectMeta{Name: "system:kube-aggregator"},
			Rules: []rbacv1.PolicyRule{
				// 需要查看所有 Service，以确认其引用的后端是否存在。
				rbacv1helpers.NewRule(Read...).Groups(legacyGroup).Resources("services", "endpoints").RuleOrDie(),
			},
		},
		{
			// 用于引导 kube-controller-manager，使其能创建其他控制器所需的
			// 共享 informer、ServiceAccount 和 Secret 独立身份。
			ObjectMeta: metav1.ObjectMeta{Name: "system:kube-controller-manager"},
			Rules: []rbacv1.PolicyRule{
				eventsRule(),
				// 领导者选举需要此权限。
				rbacv1helpers.NewRule("create").Groups(coordinationGroup).Resources("leases").RuleOrDie(),
				rbacv1helpers.NewRule("get", "update").Groups(coordinationGroup).Resources("leases").Names("kube-controller-manager").RuleOrDie(),
				// 基础资源。
				rbacv1helpers.NewRule("create").Groups(legacyGroup).Resources("secrets", "serviceaccounts").RuleOrDie(),
				rbacv1helpers.NewRule("delete").Groups(legacyGroup).Resources("secrets").RuleOrDie(),
				rbacv1helpers.NewRule("get").Groups(legacyGroup).Resources("namespaces", "secrets", "serviceaccounts", "configmaps").RuleOrDie(),
				rbacv1helpers.NewRule("update").Groups(legacyGroup).Resources("secrets", "serviceaccounts").RuleOrDie(),
				// 检查 API 访问权限所需；这些 create 操作不会修改对象。
				rbacv1helpers.NewRule("create").Groups(authenticationGroup).Resources("tokenreviews").RuleOrDie(),
				rbacv1helpers.NewRule("create").Groups(authorizationGroup).Resources("subjectaccessreviews").RuleOrDie(),
				// 所有共享 informer 需要此权限。
				rbacv1helpers.NewRule("list", "watch").Groups("*").Resources("*").RuleOrDie(),
				rbacv1helpers.NewRule("create").Groups(legacyGroup).Resources("serviceaccounts/token").RuleOrDie(),
			},
		},
		{
			// 供 kube-dns Pod 使用的角色。
			ObjectMeta: metav1.ObjectMeta{Name: "system:kube-dns"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("list", "watch").Groups(legacyGroup).Resources("endpoints", "services").RuleOrDie(),
			},
		},
		{
			// 供仓库外的持久卷供应器使用的角色。
			ObjectMeta: metav1.ObjectMeta{Name: "system:persistent-volume-provisioner"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("get", "list", "watch", "create", "delete").Groups(legacyGroup).Resources("persistentvolumes").RuleOrDie(),
				// 除读取外还需要 update，以便在 PVC 上设置锁注解。
				rbacv1helpers.NewRule("get", "list", "watch", "update").Groups(legacyGroup).Resources("persistentvolumeclaims").RuleOrDie(),
				rbacv1helpers.NewRule(Read...).Groups(storageGroup).Resources("storageclasses").RuleOrDie(),

				// 监听供给成功和失败事件需要此权限。
				rbacv1helpers.NewRule("watch").Groups(legacyGroup).Resources("events").RuleOrDie(),

				eventsRule(),
			},
		},
		{
			// 允许 csrapprover 控制器批准节点客户端 CSR 的角色。
			ObjectMeta: metav1.ObjectMeta{Name: "system:certificates.k8s.io:certificatesigningrequests:nodeclient"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("create").Groups(certificatesGroup).Resources("certificatesigningrequests/nodeclient").RuleOrDie(),
			},
		},
		{
			// 允许 csrapprover 控制器批准由节点自身申请的客户端 CSR 的角色。
			ObjectMeta: metav1.ObjectMeta{Name: "system:certificates.k8s.io:certificatesigningrequests:selfnodeclient"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("create").Groups(certificatesGroup).Resources("certificatesigningrequests/selfnodeclient").RuleOrDie(),
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "system:volume-scheduler"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule(ReadUpdate...).Groups(legacyGroup).Resources("persistentvolumes").RuleOrDie(),
				rbacv1helpers.NewRule(Read...).Groups(storageGroup).Resources("storageclasses").RuleOrDie(),
				rbacv1helpers.NewRule(ReadUpdate...).Groups(legacyGroup).Resources("persistentvolumeclaims").RuleOrDie(),
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "system:certificates.k8s.io:legacy-unknown-approver"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("approve").Groups(certificatesGroup).Resources("signers").Names(capi.LegacyUnknownSignerName).RuleOrDie(),
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "system:certificates.k8s.io:kubelet-serving-approver"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("approve").Groups(certificatesGroup).Resources("signers").Names(capi.KubeletServingSignerName).RuleOrDie(),
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "system:certificates.k8s.io:kube-apiserver-client-approver"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("approve").Groups(certificatesGroup).Resources("signers").Names(capi.KubeAPIServerClientSignerName).RuleOrDie(),
			},
		},
		{
			ObjectMeta: metav1.ObjectMeta{Name: "system:certificates.k8s.io:kube-apiserver-client-kubelet-approver"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule("approve").Groups(certificatesGroup).Resources("signers").Names(capi.KubeAPIServerClientKubeletSignerName).RuleOrDie(),
			},
		},
	}...)

	// 添加读取 ServiceAccountIssuerDiscovery 端点的集群角色。
	// 同时允许以斜杠结尾的 URL，兼容基于修复尾斜杠前发布的 OpenAPI 文档生成的客户端。
	roles = append(roles, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: "system:service-account-issuer-discovery"},
		Rules: []rbacv1.PolicyRule{
			rbacv1helpers.NewRule("get").URLs(
				"/.well-known/openid-configuration",
				"/.well-known/openid-configuration/",
				"/openid/v1/jwks",
				"/openid/v1/jwks/",
			).RuleOrDie(),
		},
	})

	// 添加集群角色 system:kubelet-api-admin。
	kubeletAPIAdminRules := []rbacv1.PolicyRule{
		// 允许只读访问 Node API 对象。
		rbacv1helpers.NewRule("get", "list", "watch").Groups(legacyGroup).Resources("nodes").RuleOrDie(),
		// 允许针对节点发起全部 API 调用。
		rbacv1helpers.NewRule("proxy").Groups(legacyGroup).Resources("nodes").RuleOrDie(),
		rbacv1helpers.NewRule("*").Groups(legacyGroup).Resources("nodes/proxy", "nodes/metrics", "nodes/stats", "nodes/log").RuleOrDie(),
	}

	if utilfeature.DefaultFeatureGate.Enabled(features.KubeletFineGrainedAuthz) {
		kubeletAPIAdminRules = append(kubeletAPIAdminRules, rbacv1helpers.NewRule("*").Groups(legacyGroup).Resources("nodes/pods", "nodes/healthz", "nodes/configz").RuleOrDie())
	}

	roles = append(roles, rbacv1.ClusterRole{
		// 供完整访问 kubelet API 的角色使用。
		ObjectMeta: metav1.ObjectMeta{Name: "system:kubelet-api-admin"},
		Rules:      kubeletAPIAdminRules,
	})

	// node-proxier 角色供 kube-proxy 使用。
	nodeProxierRules := []rbacv1.PolicyRule{
		rbacv1helpers.NewRule("list", "watch").Groups(legacyGroup).Resources("services", "endpoints").RuleOrDie(),
		rbacv1helpers.NewRule("get", "list", "watch").Groups(legacyGroup).Resources("nodes").RuleOrDie(),

		eventsRule(),
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.MultiCIDRServiceAllocator) {
		nodeProxierRules = append(nodeProxierRules, rbacv1helpers.NewRule("list", "watch").Groups(networkingGroup).Resources("servicecidrs").RuleOrDie())
	}

	nodeProxierRules = append(nodeProxierRules, rbacv1helpers.NewRule("list", "watch").Groups(discoveryGroup).Resources("endpointslices").RuleOrDie())
	roles = append(roles, rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: "system:node-proxier"},
		Rules:      nodeProxierRules,
	})

	kubeSchedulerRules := []rbacv1.PolicyRule{
		eventsRule(),
		// 用于访问 leader lease。
		// TODO：将权限限制到 kube-system 命名空间。
		rbacv1helpers.NewRule("create").Groups(coordinationGroup).Resources("leases").RuleOrDie(),
		rbacv1helpers.NewRule("get", "update", "list", "watch").Groups(coordinationGroup).Resources("leases").Names("kube-scheduler").RuleOrDie(),
		rbacv1helpers.NewRule(ReadWrite...).Groups(coordinationGroup).Resources("leasecandidates").RuleOrDie(),

		// 基础资源。
		rbacv1helpers.NewRule(Read...).Groups(legacyGroup).Resources("nodes").RuleOrDie(),
		rbacv1helpers.NewRule("get", "list", "watch", "delete").Groups(legacyGroup).Resources("pods").RuleOrDie(),
		rbacv1helpers.NewRule("create").Groups(legacyGroup).Resources("pods/binding", "bindings").RuleOrDie(),
		rbacv1helpers.NewRule("patch", "update").Groups(legacyGroup).Resources("pods/status").RuleOrDie(),
		// 用于选择 Pod 的资源
		rbacv1helpers.NewRule(Read...).Groups(legacyGroup).Resources("services", "replicationcontrollers").RuleOrDie(),
		rbacv1helpers.NewRule(Read...).Groups(appsGroup, extensionsGroup).Resources("replicasets").RuleOrDie(),
		rbacv1helpers.NewRule(Read...).Groups(appsGroup).Resources("statefulsets").RuleOrDie(),
		// Pod 使用或应用于 Pod 的资源
		rbacv1helpers.NewRule(Read...).Groups(policyGroup).Resources("poddisruptionbudgets").RuleOrDie(),
		rbacv1helpers.NewRule(Read...).Groups(legacyGroup).Resources("persistentvolumeclaims", "persistentvolumes").RuleOrDie(),
		// 检查 API 访问权限所需；这些 create 操作不会修改对象
		rbacv1helpers.NewRule("create").Groups(authenticationGroup).Resources("tokenreviews").RuleOrDie(),
		rbacv1helpers.NewRule("create").Groups(authorizationGroup).Resources("subjectaccessreviews").RuleOrDie(),
		// 卷限制需要此权限
		rbacv1helpers.NewRule(Read...).Groups(storageGroup).Resources("csinodes").RuleOrDie(),
		rbacv1helpers.NewRule("get", "list", "watch").Groups(storageGroup).Resources("volumeattachments").RuleOrDie(),
		// Pod 亲和性中的 namespaceSelector 功能需要此权限
		rbacv1helpers.NewRule(Read...).Groups(legacyGroup).Resources("namespaces").RuleOrDie(),
		rbacv1helpers.NewRule(Read...).Groups(storageGroup).Resources("csidrivers").RuleOrDie(),
		rbacv1helpers.NewRule(Read...).Groups(storageGroup).Resources("csistoragecapacities").RuleOrDie(),
	}
	// 动态资源分配需要此权限。
	if utilfeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) {
		kubeSchedulerRules = append(kubeSchedulerRules,
			rbacv1helpers.NewRule(Read...).Groups(resourceGroup).Resources("deviceclasses").RuleOrDie(),
			rbacv1helpers.NewRule(ReadUpdate...).Groups(resourceGroup).Resources("resourceclaims").RuleOrDie(),
			rbacv1helpers.NewRule(ReadUpdate...).Groups(resourceGroup).Resources("resourceclaims/status").RuleOrDie(),
			rbacv1helpers.NewRule("update").Groups(legacyGroup).Resources("pods/finalizers").RuleOrDie(),
			rbacv1helpers.NewRule(Read...).Groups(resourceGroup).Resources("resourceslices").RuleOrDie(),
		)
		if utilfeature.DefaultFeatureGate.Enabled(features.DRAExtendedResource) {
			kubeSchedulerRules = append(kubeSchedulerRules,
				rbacv1helpers.NewRule("create", "delete").Groups(resourceGroup).Resources("resourceclaims").RuleOrDie(),
			)
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.DRADeviceTaintRules) {
			kubeSchedulerRules = append(kubeSchedulerRules, rbacv1helpers.NewRule(Read...).Groups(resourceGroup).Resources("devicetaintrules").RuleOrDie())
		}
		if utilfeature.DefaultFeatureGate.Enabled(features.DRAResourceClaimGranularStatusAuthorization) {
			kubeSchedulerRules = append(kubeSchedulerRules,
				rbacv1helpers.NewRule("update", "patch").Groups(resourceGroup).Resources("resourceclaims/binding").RuleOrDie(),
			)
		}
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
		kubeSchedulerRules = append(kubeSchedulerRules, rbacv1helpers.NewRule(Read...).Groups(schedulingGroup).Resources("podgroups").RuleOrDie())
		kubeSchedulerRules = append(kubeSchedulerRules, rbacv1helpers.NewRule("patch", "update").Groups(schedulingGroup).Resources("podgroups/status").RuleOrDie())
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.CompositePodGroup) {
		kubeSchedulerRules = append(kubeSchedulerRules, rbacv1helpers.NewRule(Read...).Groups(schedulingGroup).Resources("compositepodgroups").RuleOrDie())
		kubeSchedulerRules = append(kubeSchedulerRules, rbacv1helpers.NewRule("patch", "update").Groups(schedulingGroup).Resources("compositepodgroups/status").RuleOrDie())
	}
	roles = append(roles, rbacv1.ClusterRole{
		// 供 kube-scheduler 使用的角色。
		ObjectMeta: metav1.ObjectMeta{Name: "system:kube-scheduler"},
		Rules:      kubeSchedulerRules,
	})

	// 默认 ClusterRole，允许读取 ClusterTrustBundle 对象。
	if utilfeature.DefaultFeatureGate.Enabled(features.ClusterTrustBundle) {
		roles = append(roles, rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{Name: "system:cluster-trust-bundle-discovery"},
			Rules: []rbacv1.PolicyRule{
				rbacv1helpers.NewRule(Read...).Groups(certificatesGroup).Resources("clustertrustbundles").RuleOrDie(),
			},
		})
	}

	addClusterRoleLabel(roles)
	return roles
}

const systemNodeRoleName = "system:node"

// ClusterRoleBindings 返回集群启动时自动创建的 ClusterRoleBinding 列表。
// 将内置角色绑定到系统用户、组和服务账户。
func ClusterRoleBindings() []rbacv1.ClusterRoleBinding {
	rolebindings := []rbacv1.ClusterRoleBinding{
		rbacv1helpers.NewClusterBinding("cluster-admin").Groups(user.SystemPrivilegedGroup).BindingOrDie(),
		rbacv1helpers.NewClusterBinding("system:monitoring").Groups(user.MonitoringGroup).BindingOrDie(),
		rbacv1helpers.NewClusterBinding("system:discovery").Groups(user.AllAuthenticated).BindingOrDie(),
		rbacv1helpers.NewClusterBinding("system:basic-user").Groups(user.AllAuthenticated).BindingOrDie(),
		rbacv1helpers.NewClusterBinding("system:public-info-viewer").Groups(user.AllAuthenticated, user.AllUnauthenticated).BindingOrDie(),
		rbacv1helpers.NewClusterBinding("system:node-proxier").Users(user.KubeProxy).BindingOrDie(),
		rbacv1helpers.NewClusterBinding("system:kube-controller-manager").Users(user.KubeControllerManager).BindingOrDie(),
		rbacv1helpers.NewClusterBinding("system:kube-dns").SAs("kube-system", "kube-dns").BindingOrDie(),
		rbacv1helpers.NewClusterBinding("system:kube-scheduler").Users(user.KubeScheduler).BindingOrDie(),
		rbacv1helpers.NewClusterBinding("system:volume-scheduler").Users(user.KubeScheduler).BindingOrDie(),

		// 自 1.7 提供 Node 授权器 起，system:node 角色到 system:nodes 组的默认绑定已弃用。
		// 保留该绑定，但 subjects 为空，以便收紧协调逻辑时可以移除该主体。
		{
			ObjectMeta: metav1.ObjectMeta{Name: systemNodeRoleName},
			RoleRef:    rbacv1.RoleRef{APIGroup: rbacv1.GroupName, Kind: "ClusterRole", Name: systemNodeRoleName},
		},
	}

	// 允许所有集群内工作负载（通过其 ServiceAccount）读取 OIDC 发现端点。
	// 具有某些写权限（创建 Pod、Secret、ServiceAccount 等）的用户可以获得
	// ServiceAccount 身份，从而访问这些信息。issuer URL 已经包含在 SA 令牌 JWT 中，
	// SA 也可以通过检查自己的 token 获得相同信息。因此 SA 应该有此访问权限。
	rolebindings = append(rolebindings,
		rbacv1helpers.NewClusterBinding("system:service-account-issuer-discovery").Groups(serviceaccount.AllServiceAccountsGroup).BindingOrDie(),
	)

	// ServiceAccount 可以读取 ClusterTrustBundle 对象
	if utilfeature.DefaultFeatureGate.Enabled(features.ClusterTrustBundle) {
		rolebindings = append(rolebindings, rbacv1helpers.NewClusterBinding("system:cluster-trust-bundle-discovery").Groups(serviceaccount.AllServiceAccountsGroup).BindingOrDie())
	}

	addClusterRoleBindingLabel(rolebindings)

	return rolebindings
}

// ClusterRolesToAggregate 返回需要聚合的 ClusterRole 映射。
// 用于将旧版角色（admin/edit/view）的规则聚合到对应的 system:aggregate-to-* 角色中。
func ClusterRolesToAggregate() map[string]string {
	return map[string]string{
		"admin": "system:aggregate-to-admin",
		"edit":  "system:aggregate-to-edit",
		"view":  "system:aggregate-to-view",
	}
}

// ClusterRoleBindingsToSplit 返回需要拆分的 ClusterRoleBinding 映射。
// 将 source ClusterRoleBinding 的 Subjects、Annotations 和 Labels
// 复制到 destination ClusterRoleBinding 模板中。
func ClusterRoleBindingsToSplit() map[string]rbacv1.ClusterRoleBinding {
	bindingsToSplit := map[string]rbacv1.ClusterRoleBinding{}
	for _, defaultClusterRoleBinding := range ClusterRoleBindings() {
		switch defaultClusterRoleBinding.Name {
		case "system:public-info-viewer":
			bindingsToSplit["system:discovery"] = defaultClusterRoleBinding
		}
	}
	return bindingsToSplit
}
