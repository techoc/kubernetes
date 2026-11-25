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

package node

import (
	"context"
	"fmt"

	"k8s.io/klog/v2"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate"
	certsapi "k8s.io/kubernetes/pkg/apis/certificates"
	coordapi "k8s.io/kubernetes/pkg/apis/coordination"
	api "k8s.io/kubernetes/pkg/apis/core"
	resourceapi "k8s.io/kubernetes/pkg/apis/resource"
	storageapi "k8s.io/kubernetes/pkg/apis/storage"
	"k8s.io/kubernetes/pkg/auth/nodeidentifier"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/plugin/pkg/auth/authorizer/rbac"
	"k8s.io/kubernetes/third_party/forked/gonum/graph"
	"k8s.io/kubernetes/third_party/forked/gonum/graph/traverse"
)

// NodeAuthorizer 授权 kubelet 的请求，逻辑如下：
//  1. 如果请求不是来自节点（NodeIdentity 返回 isNode=false），拒绝
//  2. 如果无法识别具体节点（NodeIdentity 返回 nodeName=），拒绝
//  3. 如果请求访问 secret、configmap、PV、ResourceClaim 或 PVC，除非 verb 是 get 且请求对象与请求节点相关，否则拒绝：
//     node <- configmap
//     node <- pod
//     node <- pod <- secret
//     node <- pod <- configmap
//     node <- pod <- pvc
//     node <- pod <- pvc <- pv
//     node <- pod <- pvc <- pv <- secret
//     node <- pod <- ResourceClaim
//     node <- pcr
//  4. 如果请求访问 ResourceSlice，检查现有 slice 对象到节点的边是否存在
//  5. 对于其他资源，使用静态定义的规则统一授权所有节点
type NodeAuthorizer struct {
	graph      *Graph                        // 资源到 Node 的依赖关系图
	identifier nodeidentifier.NodeIdentifier // 从用户信息中提取节点名称
	nodeRules  []rbacv1.PolicyRule           // 静态节点规则，用于非细分资源的授权

	// 允许在测试中覆盖功能门控。
	features featuregate.FeatureGate // 功能门控，用于测试时覆盖默认值
}

var _ = authorizer.Authorizer(&NodeAuthorizer{})
var _ = authorizer.RuleResolver(&NodeAuthorizer{})

// NewAuthorizer 创建一个新的 Node 授权器。
// graph 用于查找资源到节点的依赖关系，identifier 用于从用户信息提取节点名称，
// rules 是静态节点规则，用于非细分资源的授权。
func NewAuthorizer(graph *Graph, identifier nodeidentifier.NodeIdentifier, rules []rbacv1.PolicyRule) *NodeAuthorizer {
	return &NodeAuthorizer{
		graph:      graph,
		identifier: identifier,
		nodeRules:  rules,
		features:   utilfeature.DefaultFeatureGate,
	}
}

var (
	configMapResource     = api.Resource("configmaps")
	secretResource        = api.Resource("secrets")
	podResource           = api.Resource("pods")
	nodeResource          = api.Resource("nodes")
	resourceSlice         = resourceapi.Resource("resourceslices")
	pvcResource           = api.Resource("persistentvolumeclaims")
	pvResource            = api.Resource("persistentvolumes")
	resourceClaimResource = resourceapi.Resource("resourceclaims")
	vaResource            = storageapi.Resource("volumeattachments")
	svcAcctResource       = api.Resource("serviceaccounts")
	leaseResource         = coordapi.Resource("leases")
	csiNodeResource       = storageapi.Resource("csinodes")
	pcrResource           = certsapi.Resource("podcertificaterequests")
)

// RulesFor 对节点用户返回 incomplete=true：节点权限取决于动态图关系，无法完整枚举。
func (r *NodeAuthorizer) RulesFor(ctx context.Context, user user.Info, namespace string) ([]authorizer.ResourceRuleInfo, []authorizer.NonResourceRuleInfo, bool, error) {
	if _, isNode := r.identifier.NodeIdentity(user); isNode {
		// 节点没有完全枚举的权限，因为权限取决于图结构（动态变化）
		return nil, nil, true, fmt.Errorf("node authorizer does not support user rule resolution")
	}
	return nil, nil, false, nil
}

// ConditionsAwareAuthorize 不支持 conditions-aware 授权，直接转换 Authorize 结果。
func (r *NodeAuthorizer) ConditionsAwareAuthorize(ctx context.Context, attrs authorizer.Attributes) authorizer.ConditionsAwareDecision {
	return authorizer.ConditionsAwareDecisionFromParts(r.Authorize(ctx, attrs))
}

// EvaluateConditions 不支持条件评估。
func (*NodeAuthorizer) EvaluateConditions(_ context.Context, _ authorizer.ConditionsAwareDecision, _ authorizer.ConditionsData) (authorizer.Decision, string, error) {
	return authorizer.DecisionDeny, "", authorizer.ErrorConditionEvaluationNotSupported
}

// Authorize 根据请求者身份和资源到节点的图关系作出授权决定。
// 非节点请求返回 NoOpinion，以交由其他授权器处理。
func (r *NodeAuthorizer) Authorize(ctx context.Context, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
	nodeName, isNode := r.identifier.NodeIdentity(attrs.GetUser())
	if !isNode {
		// 非节点请求，不发表意见（交给其他授权器处理）
		return authorizer.DecisionNoOpinion, "", nil
	}
	if len(nodeName) == 0 {
		// 无法识别节点身份，拒绝
		klog.V(2).Infof("NODE DENY: unknown node for user %q", attrs.GetUser().GetName())
		return authorizer.DecisionNoOpinion, fmt.Sprintf("unknown node for user %q", attrs.GetUser().GetName()), nil
	}

	// 对特定资源进行细粒度访问控制
	if attrs.IsResourceRequest() {
		requestResource := schema.GroupResource{Group: attrs.GetAPIGroup(), Resource: attrs.GetResource()}
		switch requestResource {
		case secretResource:
			return r.authorizeReadNamespacedObject(nodeName, secretVertexType, attrs)
		case configMapResource:
			return r.authorizeReadNamespacedObject(nodeName, configMapVertexType, attrs)
		case pvcResource:
			if attrs.GetSubresource() == "status" {
				return r.authorizeStatusUpdate(nodeName, pvcVertexType, attrs)
			}
			return r.authorizeGet(nodeName, pvcVertexType, attrs)
		case pvResource:
			return r.authorizeGet(nodeName, pvVertexType, attrs)
		case resourceClaimResource:
			return r.authorizeGet(nodeName, resourceClaimVertexType, attrs)
		case vaResource:
			return r.authorizeGet(nodeName, vaVertexType, attrs)
		case svcAcctResource:
			return r.authorizeServiceAccount(nodeName, attrs)
		case leaseResource:
			return r.authorizeLease(nodeName, attrs)
		case csiNodeResource:
			return r.authorizeCSINode(nodeName, attrs)
		case resourceSlice:
			return r.authorizeResourceSlice(nodeName, attrs)
		case nodeResource:
			return r.authorizeNode(nodeName, attrs)
		case podResource:
			return r.authorizePod(nodeName, attrs)
		case pcrResource:
			if r.features.Enabled(features.PodCertificateRequest) {
				return r.authorizePodCertificateRequest(nodeName, attrs)
			}
			return authorizer.DecisionNoOpinion, "", nil
		}
	}

	// 其他资源的访问不细分，直接评估静态节点规则
	if rbac.RulesAllow(attrs, r.nodeRules...) {
		return authorizer.DecisionAllow, "", nil
	}
	return authorizer.DecisionNoOpinion, "", nil
}

// authorizeStatusUpdate 授权对指定类型资源的 status 子资源的 get/update/patch 请求，
// 仅当该资源与指定节点相关时才允许。
func (r *NodeAuthorizer) authorizeStatusUpdate(nodeName string, startingType vertexType, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
	switch attrs.GetVerb() {
	case "update", "patch":
		// 允许。
	default:
		klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
		return authorizer.DecisionNoOpinion, "can only get/update/patch this type", nil
	}

	if attrs.GetSubresource() != "status" {
		klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
		return authorizer.DecisionNoOpinion, "can only update status subresource", nil
	}

	return r.authorize(nodeName, startingType, attrs)
}

// authorizeGet 授权对指定类型资源的 get 请求，仅当该资源与指定节点相关时才允许。
func (r *NodeAuthorizer) authorizeGet(nodeName string, startingType vertexType, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
	if attrs.GetVerb() != "get" {
		klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
		return authorizer.DecisionNoOpinion, "can only get individual resources of this type", nil
	}
	if len(attrs.GetSubresource()) > 0 {
		klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
		return authorizer.DecisionNoOpinion, "cannot get subresource", nil
	}
	return r.authorize(nodeName, startingType, attrs)
}

// authorizeReadNamespacedObject 授权对指定类型资源的 get/list/watch 请求，
// 仅当该资源与指定节点相关且请求指定了命名空间时才允许。
func (r *NodeAuthorizer) authorizeReadNamespacedObject(nodeName string, startingType vertexType, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
	switch attrs.GetVerb() {
	case "get", "list", "watch":
		// 允许。
	default:
		klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
		return authorizer.DecisionNoOpinion, "can only read resources of this type", nil
	}

	if len(attrs.GetSubresource()) > 0 {
		klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
		return authorizer.DecisionNoOpinion, "cannot read subresource", nil
	}
	if len(attrs.GetNamespace()) == 0 {
		klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
		return authorizer.DecisionNoOpinion, "can only read namespaced object of this type", nil
	}
	return r.authorize(nodeName, startingType, attrs)
}

func (r *NodeAuthorizer) authorize(nodeName string, startingType vertexType, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
	if len(attrs.GetName()) == 0 {
		klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
		return authorizer.DecisionNoOpinion, "No Object name found", nil
	}

	// 在图中查找从请求资源到指定节点的路径
	ok, err := r.hasPathFrom(nodeName, startingType, attrs.GetNamespace(), attrs.GetName())
	if err != nil {
		klog.V(2).InfoS("NODE DENY", "err", err)
		return authorizer.DecisionNoOpinion, fmt.Sprintf("no relationship found between node '%s' and this object", nodeName), nil
	}
	if !ok {
		klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
		return authorizer.DecisionNoOpinion, fmt.Sprintf("no relationship found between node '%s' and this object", nodeName), nil
	}
	return authorizer.DecisionAllow, "", nil
}

// authorizeServiceAccount 授权 ServiceAccount 相关请求：
// - 当 KubeletServiceAccountTokenForCredentialProviders 或 PodCertificateRequest 功能启用时，授权 get 请求
// - 授权对运行在节点上的 Pod 的 ServiceAccount 的 token 子资源的 create 请求
func (r *NodeAuthorizer) authorizeServiceAccount(nodeName string, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
	verb := attrs.GetVerb()

	if verb == "get" && len(attrs.GetSubresource()) == 0 {
		if !(r.features.Enabled(features.KubeletServiceAccountTokenForCredentialProviders) || r.features.Enabled(features.PodCertificateRequest)) {
			klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
			return authorizer.DecisionNoOpinion, "not allowed to get service accounts", nil
		}
		return r.authorizeReadNamespacedObject(nodeName, serviceAccountVertexType, attrs)
	}

	return r.authorizeCreateToken(nodeName, serviceAccountVertexType, attrs)
}

// authorizeCreateToken 授权对运行在节点上的 Pod 的 ServiceAccount 的 token 子资源的 create 请求。
// 通过检查 ServiceAccount 到节点的图路径来验证权限。
func (r *NodeAuthorizer) authorizeCreateToken(nodeName string, startingType vertexType, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
	if attrs.GetVerb() != "create" || len(attrs.GetName()) == 0 {
		klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
		return authorizer.DecisionNoOpinion, "can only create tokens for individual service accounts", nil
	}

	if attrs.GetSubresource() != "token" {
		klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
		return authorizer.DecisionNoOpinion, "can only create token subresource of serviceaccount", nil
	}

	ok, err := r.hasPathFrom(nodeName, startingType, attrs.GetNamespace(), attrs.GetName())
	if err != nil {
		klog.V(2).Infof("NODE DENY: %v", err)
		return authorizer.DecisionNoOpinion, fmt.Sprintf("no relationship found between node '%s' and this object", nodeName), nil
	}
	if !ok {
		klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
		return authorizer.DecisionNoOpinion, fmt.Sprintf("no relationship found between node '%s' and this object", nodeName), nil
	}
	return authorizer.DecisionAllow, "", nil
}

// authorizeLease 授权节点对 coordination.k8s.io/leases 的请求。
// 节点只能访问 kube-node-lease 命名空间中与自己同名的 Lease，用于心跳保活。
func (r *NodeAuthorizer) authorizeLease(nodeName string, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
	// 允许的 verb：get、create、update、patch、delete。
	verb := attrs.GetVerb()
	switch verb {
	case "get", "create", "update", "patch", "delete":
		// 允许。
	default:
		klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
		return authorizer.DecisionNoOpinion, "can only get, create, update, patch, or delete a node lease", nil
	}

	// 请求必须针对节点 Lease 的系统命名空间
	if attrs.GetNamespace() != api.NamespaceNodeLease {
		klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
		return authorizer.DecisionNoOpinion, fmt.Sprintf("can only access leases in the %q system namespace", api.NamespaceNodeLease), nil
	}

	// 请求必须来自与 Lease 同名的节点
	// 对 create 操作跳过此检查，因为授权器在创建时不知道名称
	// noderestriction 准入插件 可以在创建时执行此检查
	if verb != "create" && attrs.GetName() != nodeName {
		klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
		return authorizer.DecisionNoOpinion, "can only access node lease with the same name as the requesting node", nil
	}

	return authorizer.DecisionAllow, "", nil
}

// authorizeCSINode 授权节点对 storage.k8s.io/csinodes 的请求。
// 节点只能访问与自己同名的 CSINode，用于报告 CSI 卷健康状态等。
func (r *NodeAuthorizer) authorizeCSINode(nodeName string, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
	verb := attrs.GetVerb()
	switch attrs.GetSubresource() {
	case "":
		// 允许的 verb：get、create、update、patch、delete。
		switch verb {
		case "get", "create", "update", "patch", "delete":
			// 允许。
		default:
			klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
			return authorizer.DecisionNoOpinion, "can only get, create, update, patch, or delete a CSINode", nil
		}
	case "status":
		if !r.features.Enabled(features.CSIVolumeHealth) {
			klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
			return authorizer.DecisionNoOpinion, "CSINode status access requires CSIVolumeHealth feature", nil
		}
		// 允许的 verb：get、update、patch（用于 CSI 卷健康状态上报）。
		switch verb {
		case "get", "update", "patch":
			// 允许。
		default:
			klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
			return authorizer.DecisionNoOpinion, "can only get, update, or patch CSINode status", nil
		}
	default:
		klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
		return authorizer.DecisionNoOpinion, "cannot authorize CSINode subresources", nil
	}

	// 请求必须来自与 CSINode 同名的节点
	// 对 create 操作跳过此检查，因为授权器在创建时不知道名称
	// noderestriction 准入插件 可以在创建时执行此检查
	if verb != "create" && attrs.GetName() != nodeName {
		klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
		return authorizer.DecisionNoOpinion, "can only access CSINode with the same name as the requesting node", nil
	}

	return authorizer.DecisionAllow, "", nil
}

// authorizeResourceSlice 授权节点对 resource.k8s.io/resourceslices 的请求。
// 节点只能访问与自己相关的 ResourceSlice（通过 nodeName 字段关联）。
func (r *NodeAuthorizer) authorizeResourceSlice(nodeName string, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
	if len(attrs.GetSubresource()) > 0 {
		klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
		return authorizer.DecisionNoOpinion, "cannot authorize ResourceSlice subresources", nil
	}

	// 允许的 verb：get、create、update、patch、delete、watch、list、deletecollection。
	verb := attrs.GetVerb()
	switch verb {
	case "create":
		// 创建请求由 noderestriction 准入插件 检查 ResourceSlice.NodeName 字段。
		// 授权器无法访问新对象的内容，所以直接允许。
		return authorizer.DecisionAllow, "", nil
	case "get", "update", "patch", "delete":
		// 通过图中已建立的边来验证访问权限
		return r.authorize(nodeName, sliceVertexType, attrs)
	case "watch", "list", "deletecollection":
		// 只允许带 nodeName 字段选择器的请求
		reqs, _ := attrs.GetFieldSelector()
		for _, req := range reqs {
			if req.Field == resourceapi.ResourceSliceSelectorNodeName && req.Operator == selection.Equals && req.Value == nodeName {
				return authorizer.DecisionAllow, "", nil
			}
		}
		// 否则拒绝。
		klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
		return authorizer.DecisionNoOpinion, "can only list/watch/deletecollection resourceslices with nodeName field selector", nil
	default:
		klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
		return authorizer.DecisionNoOpinion, "only the following verbs are allowed for a ResourceSlice: get, watch, list, create, update, patch, delete, deletecollection", nil
	}
}

func (r *NodeAuthorizer) authorizePodCertificateRequest(nodeName string, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
	if len(attrs.GetSubresource()) != 0 {
		return authorizer.DecisionNoOpinion, "nodes may not access the status subresource of PodCertificateRequests", nil
	}

	switch attrs.GetVerb() {
	case "create":
		// 创建请求由 noderestriction 准入插件 进一步限制
		return authorizer.DecisionAllow, "", nil
	case "get":
		return r.authorize(nodeName, pcrVertexType, attrs)
	case "list", "watch":
		// 只允许带 nodeName 字段选择器的请求
		reqs, _ := attrs.GetFieldSelector()
		for _, req := range reqs {
			if req.Field == "spec.nodeName" && req.Operator == selection.Equals && req.Value == nodeName {
				return authorizer.DecisionAllow, "", nil
			}
		}
		// 否则拒绝。
		klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
		return authorizer.DecisionNoOpinion, "can only list/watch podcertificaterequests with nodeName field selector", nil
	}

	klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
	return authorizer.DecisionNoOpinion, fmt.Sprintf("nodes may not %s podcertificaterequests", attrs.GetVerb()), nil
}

// authorizeNode 授权节点对 Node API 对象的请求。
// 节点只能访问自己的 Node 对象，不能访问其他节点。
func (r *NodeAuthorizer) authorizeNode(nodeName string, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
	switch attrs.GetSubresource() {
	case "":
		switch attrs.GetVerb() {
		case "create", "update", "patch":
			// 使用 NodeRestriction 准入插件 限制节点只能创建/更新自己的 API 对象
			return authorizer.DecisionAllow, "", nil
		case "get", "list", "watch":
			// 直接比较名称，而不是使用图，
			// 这样 kubelet 可以在创建 Node 对象之前尝试读取
			switch attrs.GetName() {
			case nodeName:
				return authorizer.DecisionAllow, "", nil
			case "":
				klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
				return authorizer.DecisionNoOpinion, fmt.Sprintf("node '%s' cannot read all nodes, only its own Node object", nodeName), nil
			default:
				klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
				return authorizer.DecisionNoOpinion, fmt.Sprintf("node '%s' cannot read '%s', only its own Node object", nodeName, attrs.GetName()), nil
			}
		}
	case "status":
		switch attrs.GetVerb() {
		case "update", "patch":
			// 使用 NodeRestriction 准入插件 限制节点只能更新自己的 Node 状态
			return authorizer.DecisionAllow, "", nil
		}
	}

	klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
	return authorizer.DecisionNoOpinion, "", nil
}

// authorizePod 授权节点对 Pod API 对象的请求。
// 节点只能访问运行在自己身上的 Pod，通过图路径或字段选择器限制。
func (r *NodeAuthorizer) authorizePod(nodeName string, attrs authorizer.Attributes) (authorizer.Decision, string, error) {
	switch attrs.GetSubresource() {
	case "":
		switch attrs.GetVerb() {
		case "get":
			return r.authorizeGet(nodeName, podVertexType, attrs)

		case "list", "watch":
			// 允许带 nodeName 字段选择器的请求
			reqs, _ := attrs.GetFieldSelector()
			for _, req := range reqs {
				if req.Field == "spec.nodeName" && req.Operator == selection.Equals && req.Value == nodeName {
					return authorizer.DecisionAllow, "", nil
				}
			}
			// 允许读取已知与节点相关的单个 Pod
			if attrs.GetName() != "" {
				return r.authorize(nodeName, podVertexType, attrs)
			}
			// 否则拒绝。
			klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
			return authorizer.DecisionNoOpinion, "can only list/watch pods with spec.nodeName field selector", nil

		case "create", "delete":
			// 节点需要创建/删除 mirror pod
			// 使用 NodeRestriction 准入插件 限制节点只能创建/删除绑定到自己的镜像 Pod
			return authorizer.DecisionAllow, "", nil
		}

	case "status":
		switch attrs.GetVerb() {
		case "update", "patch":
			// 节点需要报告运行在其上的 Pod 的状态
			// 使用 NodeRestriction 准入插件 限制节点只能更新绑定到自己的 Pod 的状态
			return authorizer.DecisionAllow, "", nil
		}

	case "eviction":
		if attrs.GetVerb() == "create" {
			// 节点需要驱逐运行在其上的 Pod
			// 使用 NodeRestriction 准入插件 限制节点只能驱逐绑定到自己的 Pod
			return authorizer.DecisionAllow, "", nil
		}
	}

	klog.V(2).Infof("NODE DENY: '%s' %#v", nodeName, attrs)
	return authorizer.DecisionNoOpinion, "", nil
}

// hasPathFrom 检查从指定类型的资源到指定 Node 是否存在有向路径。
// 先尝试使用 destinationEdgeIndex 快速查找（O(1)），如果索引不存在或不权威，
// 则使用 DFS 遍历图（O(V+E)）。
func (r *NodeAuthorizer) hasPathFrom(nodeName string, startingType vertexType, startingNamespace, startingName string) (bool, error) {
	r.graph.lock.RLock()
	defer r.graph.lock.RUnlock()

	nodeVertex, exists := r.graph.getVertexRLocked(nodeVertexType, "", nodeName)
	if !exists {
		return false, fmt.Errorf("unknown node '%s' cannot get %s %s/%s", nodeName, vertexTypes[startingType], startingNamespace, startingName)
	}

	startingVertex, exists := r.graph.getVertexRLocked(startingType, startingNamespace, startingName)
	if !exists {
		return false, fmt.Errorf("node '%s' cannot get unknown %s %s/%s", nodeName, vertexTypes[startingType], startingNamespace, startingName)
	}

	if index, indexExists := r.graph.destinationEdgeIndex[startingVertex.ID()]; indexExists {
		// 快速检查：如果索引中已记录目标节点，直接返回 true
		if index.has(nodeVertex.ID()) {
			return true, nil
		}
		// 对于某些类型的顶点，destination edge 索引是权威的（只要索引存在），
		// 因此可以在此快速失败，避免下面可能昂贵的 DFS 遍历
		if vertexTypeWithAuthoritativeIndex[startingType] {
			return false, fmt.Errorf("node '%s' cannot get %s %s/%s, no relationship to this object was found in the node authorizer graph", nodeName, vertexTypes[startingType], startingNamespace, startingName)
		}
	}

	found := false
	traversal := &traverse.VisitingDepthFirst{
		EdgeFilter: func(edge graph.Edge) bool {
			if destinationEdge, ok := edge.(*destinationEdge); ok {
				if destinationEdge.DestinationID() != nodeVertex.ID() {
					// 不跟踪指向其他节点的路径
					return false
				}
				// 找到指向目标节点的路径
				found = true
			}
			// 继续遍历此边
			return true
		},
	}
	traversal.Walk(r.graph.graph, startingVertex, func(n graph.Node) bool {
		if n.ID() == nodeVertex.ID() {
			// 找到目标节点
			found = true
		}
		// 如果已找到目标节点，停止遍历
		return found
	})
	if !found {
		return false, fmt.Errorf("node '%s' cannot get %s %s/%s, no relationship to this object was found in the node authorizer graph", nodeName, vertexTypes[startingType], startingNamespace, startingName)
	}
	return true, nil
}
