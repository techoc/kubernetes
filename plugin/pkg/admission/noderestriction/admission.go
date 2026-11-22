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

package noderestriction

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	apiresource "k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/diff"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	apiserveradmission "k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/client-go/informers"
	corev1lister "k8s.io/client-go/listers/core/v1"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	"k8s.io/component-base/featuregate"
	"k8s.io/component-helpers/storage/ephemeral"
	csitrans "k8s.io/csi-translation-lib"
	"k8s.io/klog/v2"
	kubeletapis "k8s.io/kubelet/pkg/apis"
	podutil "k8s.io/kubernetes/pkg/api/pod"
	authenticationapi "k8s.io/kubernetes/pkg/apis/authentication"
	certapi "k8s.io/kubernetes/pkg/apis/certificates"
	coordapi "k8s.io/kubernetes/pkg/apis/coordination"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/policy"
	"k8s.io/kubernetes/pkg/apis/resource"
	"k8s.io/kubernetes/pkg/apis/storage"
	"k8s.io/kubernetes/pkg/auth/nodeidentifier"
	"k8s.io/kubernetes/pkg/features"
)

// PluginName 是该插件的名称。
const PluginName = "NodeRestriction"

// Register 注册该插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		return NewPlugin(nodeidentifier.NewDefaultNodeIdentifier()), nil
	})
}

// NewPlugin 创建一个新的 NodeRestriction 准入插件，
// 用于识别来自节点的请求。
func NewPlugin(nodeIdentifier nodeidentifier.NodeIdentifier) *Plugin {
	return &Plugin{
		Handler:        admission.NewHandler(admission.Create, admission.Update, admission.Delete),
		nodeIdentifier: nodeIdentifier,
	}
}

// Plugin 持有该准入插件的状态并实现其逻辑。
type Plugin struct {
	*admission.Handler
	nodeIdentifier       nodeidentifier.NodeIdentifier
	podsGetter           corev1lister.PodLister
	nodesGetter          corev1lister.NodeLister
	serviceAccountGetter corev1lister.ServiceAccountLister
	csiDriverGetter      storagelisters.CSIDriverLister
	pvcGetter            corev1lister.PersistentVolumeClaimLister
	pvGetter             corev1lister.PersistentVolumeLister
	csiTranslator        csitrans.CSITranslator

	authz authorizer.UnconditionalAuthorizer

	inspectedFeatureGates                          bool
	expansionRecoveryEnabled                       bool
	dynamicResourceAllocationEnabled               bool
	allowInsecureKubeletCertificateSigningRequests bool
	serviceAccountNodeAudienceRestriction          bool
	podCertificateRequestsEnabled                  bool
	csiVolumeHealthEnabled                         bool
}

var (
	_ admission.Interface                                 = &Plugin{}
	_ apiserveradmission.WantsExternalKubeInformerFactory = &Plugin{}
	_ apiserveradmission.WantsFeatures                    = &Plugin{}
	_ apiserveradmission.WantsUnconditionalAuthorizer     = &Plugin{}
)

// InspectFeatureGates 从特性门控读取开关状态，避免在插件内直接依赖全局变量。
func (p *Plugin) InspectFeatureGates(featureGates featuregate.FeatureGate) {
	p.expansionRecoveryEnabled = featureGates.Enabled(features.RecoverVolumeExpansionFailure)
	p.dynamicResourceAllocationEnabled = featureGates.Enabled(features.DynamicResourceAllocation)
	p.allowInsecureKubeletCertificateSigningRequests = featureGates.Enabled(features.AllowInsecureKubeletCertificateSigningRequests)
	p.serviceAccountNodeAudienceRestriction = featureGates.Enabled(features.ServiceAccountNodeAudienceRestriction)
	p.podCertificateRequestsEnabled = featureGates.Enabled(features.PodCertificateRequest)
	p.csiVolumeHealthEnabled = featureGates.Enabled(features.CSIVolumeHealth)
	p.inspectedFeatureGates = true
}

// SetExternalKubeInformerFactory 向插件注册 informer 工厂并设置所需的 lister。
func (p *Plugin) SetExternalKubeInformerFactory(f informers.SharedInformerFactory) {
	p.podsGetter = f.Core().V1().Pods().Lister()
	p.nodesGetter = f.Core().V1().Nodes().Lister()
	if p.serviceAccountNodeAudienceRestriction {
		p.csiDriverGetter = f.Storage().V1().CSIDrivers().Lister()
		p.pvcGetter = f.Core().V1().PersistentVolumeClaims().Lister()
		p.pvGetter = f.Core().V1().PersistentVolumes().Lister()
		p.csiTranslator = csitrans.New()
	}
	p.serviceAccountGetter = f.Core().V1().ServiceAccounts().Lister()
}

// ValidateInitialization 校验插件是否已完成必要的初始化。
func (p *Plugin) ValidateInitialization() error {
	if p.nodeIdentifier == nil {
		return fmt.Errorf("%s requires a node identifier", PluginName)
	}
	if p.podsGetter == nil {
		return fmt.Errorf("%s requires a pod getter", PluginName)
	}
	if p.nodesGetter == nil {
		return fmt.Errorf("%s requires a node getter", PluginName)
	}
	if p.serviceAccountNodeAudienceRestriction {
		if p.csiDriverGetter == nil {
			return fmt.Errorf("%s requires a CSI driver getter", PluginName)
		}
		if p.pvcGetter == nil {
			return fmt.Errorf("%s requires a PVC getter", PluginName)
		}
		if p.pvGetter == nil {
			return fmt.Errorf("%s requires a PV getter", PluginName)
		}
		if p.authz == nil {
			return fmt.Errorf("%s requires an authorizer", PluginName)
		}
	}
	if p.podCertificateRequestsEnabled {
		if p.authz == nil {
			return fmt.Errorf("%s requires an authorizer", PluginName)
		}
	}
	if p.serviceAccountGetter == nil {
		return fmt.Errorf("%s requires a service account getter", PluginName)
	}
	if !p.inspectedFeatureGates {
		return fmt.Errorf("%s has not inspected feature gates", PluginName)
	}
	return nil
}

// SetUnconditionalAuthorizer 设置鉴权器；仅相关特性启用时才需要。
func (p *Plugin) SetUnconditionalAuthorizer(authz authorizer.UnconditionalAuthorizer) {
	if p.serviceAccountNodeAudienceRestriction || p.podCertificateRequestsEnabled {
		p.authz = authz
	}
}

var (
	podResource                   = api.Resource("pods")
	nodeResource                  = api.Resource("nodes")
	pvcResource                   = api.Resource("persistentvolumeclaims")
	svcacctResource               = api.Resource("serviceaccounts")
	leaseResource                 = coordapi.Resource("leases")
	csiNodeResource               = storage.Resource("csinodes")
	resourceSliceResource         = resource.Resource("resourceslices")
	csrResource                   = certapi.Resource("certificatesigningrequests")
	podCertificateRequestResource = certapi.Resource("podcertificaterequests")
)

// Admit 按资源类型分发请求，执行对应的节点限制策略。
func (p *Plugin) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	nodeName, isNode := p.nodeIdentifier.NodeIdentity(a.GetUserInfo())

	if !isNode {
		// 非节点用户不受本插件约束。
		return nil
	}

	if len(nodeName) == 0 {
		// 无法确定具体节点时一律拒绝，避免权限边界模糊。
		return admission.NewForbidden(a, fmt.Errorf("could not determine node from user %q", a.GetUserInfo().GetName()))
	}

	// 走到这里，调用方已明确对应到某个节点名。

	// TODO: 若节点不存在且请求不是创建 Node，则拒绝。

	switch a.GetResource().GroupResource() {
	case podResource:
		switch a.GetSubresource() {
		case "":
			return p.admitPod(nodeName, a)
		case "status":
			return p.admitPodStatus(nodeName, a)
		case "eviction":
			return p.admitPodEviction(nodeName, a)
		default:
			return admission.NewForbidden(a, fmt.Errorf("unexpected pod subresource %q, only 'status' and 'eviction' are allowed", a.GetSubresource()))
		}

	case nodeResource:
		return p.admitNode(nodeName, a)

	case pvcResource:
		switch a.GetSubresource() {
		case "status":
			return p.admitPVCStatus(nodeName, a)
		default:
			return admission.NewForbidden(a, fmt.Errorf("may only update PVC status"))
		}

	case svcacctResource:
		return p.admitServiceAccount(ctx, nodeName, a)

	case podCertificateRequestResource:
		return p.admitPodCertificateRequest(ctx, nodeName, a)

	case leaseResource:
		return p.admitLease(nodeName, a)

	case csiNodeResource:
		return p.admitCSINode(nodeName, a)

	case resourceSliceResource:
		return p.admitResourceSlice(nodeName, a)

	case csrResource:
		if p.allowInsecureKubeletCertificateSigningRequests {
			return nil
		}
		return p.admitCSR(nodeName, a)
	default:
		return nil
	}
}

// admitPod 仅在 Pod 绑定到当前节点且满足附加条件时，才允许节点创建或删除该 Pod。
func (p *Plugin) admitPod(nodeName string, a admission.Attributes) error {
	switch a.GetOperation() {
	case admission.Create:
		return p.admitPodCreate(nodeName, a)

	case admission.Delete:
		// 读取现存 Pod 以确认其归属节点。
		existingPod, err := p.podsGetter.Pods(a.GetNamespace()).Get(a.GetName())
		if apierrors.IsNotFound(err) {
			return err
		}
		if err != nil {
			return admission.NewForbidden(a, err)
		}
		// 节点只能删除绑定到自己的 Pod，防止跨节点越权删除。
		if existingPod.Spec.NodeName != nodeName {
			return admission.NewForbidden(a, fmt.Errorf("node %q can only delete pods with spec.nodeName set to itself", nodeName))
		}
		return nil

	default:
		return admission.NewForbidden(a, fmt.Errorf("unexpected operation %q, node %q can only create and delete mirror pods", a.GetOperation(), nodeName))
	}
}

// admitPodCreate 限制节点只能创建绑定到自身、且以自身为属主的静态 Pod。
func (p *Plugin) admitPodCreate(nodeName string, a admission.Attributes) error {
	// 请求对象必须是 Pod。
	pod, ok := a.GetObject().(*api.Pod)
	if !ok {
		return admission.NewForbidden(a, fmt.Errorf("unexpected type %T", a.GetObject()))
	}

	// 节点仅允许创建静态 Pod，其余 Pod 由控制面创建。
	if _, isMirrorPod := pod.Annotations[api.MirrorPodAnnotationKey]; !isMirrorPod {
		return admission.NewForbidden(a, fmt.Errorf("pod does not have %q annotation, node %q can only create mirror pods", api.MirrorPodAnnotationKey, nodeName))
	}

	// 节点只能创建调度到自己的 Pod。
	if pod.Spec.NodeName != nodeName {
		return admission.NewForbidden(a, fmt.Errorf("node %q can only create pods with spec.nodeName set to itself", nodeName))
	}
	if len(pod.OwnerReferences) > 1 {
		return admission.NewForbidden(a, fmt.Errorf("node %q can only create pods with a single owner reference set to itself", nodeName))
	}
	if len(pod.OwnerReferences) == 0 {
		return admission.NewForbidden(a, fmt.Errorf("node %q can only create pods with an owner reference set to itself", nodeName))
	}
	if len(pod.OwnerReferences) == 1 {
		owner := pod.OwnerReferences[0]
		if owner.APIVersion != v1.SchemeGroupVersion.String() ||
			owner.Kind != "Node" ||
			owner.Name != nodeName {
			return admission.NewForbidden(a, fmt.Errorf("node %q can only create pods with an owner reference set to itself", nodeName))
		}
		if owner.Controller == nil || !*owner.Controller {
			return admission.NewForbidden(a, fmt.Errorf("node %q can only create pods with a controller owner reference set to itself", nodeName))
		}
		if owner.BlockOwnerDeletion != nil && *owner.BlockOwnerDeletion {
			return admission.NewForbidden(a, fmt.Errorf("node %q must not set blockOwnerDeletion on an owner reference", nodeName))
		}

		// 校验 OwnerReference 中的 UID，防止节点冒用同名对象。
		node, err := p.nodesGetter.Get(nodeName)
		if apierrors.IsNotFound(err) {
			return err
		}
		if err != nil {
			return admission.NewForbidden(a, fmt.Errorf("error looking up node %s to verify uid: %v", nodeName, err))
		}
		if owner.UID != node.UID {
			return admission.NewForbidden(a, fmt.Errorf("node %s UID mismatch: expected %s got %s", nodeName, owner.UID, node.UID))
		}
	}

	// 禁止静态 Pod 引用其他 API 对象，避免借由引用读取或篡改其他资源。
	isPodReferencingAPIObjects, resource, err := podutil.HasAPIObjectReference(pod)
	if err != nil {
		return admission.NewForbidden(a, fmt.Errorf("error checking mirror pod for API references: %w", err))
	}
	if isPodReferencingAPIObjects {
		return admission.NewForbidden(a, fmt.Errorf("node %q can not create pods that reference %s", nodeName, resource))
	}

	return nil
}

// admitPodStatus 仅在 Pod 绑定到当前节点时允许更新其状态，
// 并禁止借状态更新篡改标签或资源声明状态。
func (p *Plugin) admitPodStatus(nodeName string, a admission.Attributes) error {
	switch a.GetOperation() {
	case admission.Update:
		// 旧对象必须是 Pod。
		oldPod, ok := a.GetOldObject().(*api.Pod)
		if !ok {
			return admission.NewForbidden(a, fmt.Errorf("unexpected type %T", a.GetOldObject()))
		}
		// 只能更新绑定到自身的 Pod 的状态。
		if oldPod.Spec.NodeName != nodeName {
			return admission.NewForbidden(a, fmt.Errorf("node %q can only update pod status for pods with spec.nodeName set to itself", nodeName))
		}
		newPod, ok := a.GetObject().(*api.Pod)
		if !ok {
			return admission.NewForbidden(a, fmt.Errorf("unexpected type %T", a.GetObject()))
		}
		if !labels.Equals(oldPod.Labels, newPod.Labels) {
			return admission.NewForbidden(a, fmt.Errorf("node %q cannot update labels through pod status", nodeName))
		}
		if !resourceClaimStatusesEqual(oldPod.Status.ResourceClaimStatuses, newPod.Status.ResourceClaimStatuses) {
			return admission.NewForbidden(a, fmt.Errorf("node %q cannot update resource claim statues", nodeName))
		}
		if !extendedResourceClaimStatusEqual(oldPod.Status.ExtendedResourceClaimStatus, newPod.Status.ExtendedResourceClaimStatus) {
			return admission.NewForbidden(a, fmt.Errorf("node %q cannot update extended resource claim status", nodeName))
		}
		if !nodeAllocatableResourceClaimStatusesEqual(oldPod.Status.NodeAllocatableResourceClaimStatuses, newPod.Status.NodeAllocatableResourceClaimStatuses) {
			return admission.NewForbidden(a, fmt.Errorf("node %q cannot update node allocatable resource claim statuses", nodeName))
		}
		return nil

	default:
		return admission.NewForbidden(a, fmt.Errorf("unexpected operation %q", a.GetOperation()))
	}
}

// resourceClaimStatusesEqual 比较两个资源声明状态切片是否一致。
func resourceClaimStatusesEqual(statusA, statusB []api.PodResourceClaimStatus) bool {
	if len(statusA) != len(statusB) {
		return false
	}
	// 状态条目通常只新增不修改，但这并非强保证，因此仍需逐项比较。
	for i := range statusA {
		if statusA[i].Name != statusB[i].Name {
			return false
		}
		claimNameA := statusA[i].ResourceClaimName
		claimNameB := statusB[i].ResourceClaimName
		if (claimNameA == nil) != (claimNameB == nil) {
			return false
		}
		if claimNameA != nil && *claimNameA != *claimNameB {
			return false
		}
	}
	return true
}

// extendedResourceClaimStatusEqual 比较扩展资源声明状态是否一致。
func extendedResourceClaimStatusEqual(statusA, statusB *api.PodExtendedResourceClaimStatus) bool {
	if statusA == nil && statusB == nil {
		return true
	}
	if statusA == nil || statusB == nil {
		return false
	}
	if statusA.ResourceClaimName != statusB.ResourceClaimName {
		return false
	}
	// 状态条目通常只新增不修改，但这并非强保证，因此仍需逐项比较。
	return slices.Equal(statusA.RequestMappings, statusB.RequestMappings)
}

// nodeAllocatableResourceClaimStatusesEqual 比较节点可分配资源声明状态切片是否一致。
func nodeAllocatableResourceClaimStatusesEqual(statusA, statusB []api.NodeAllocatableResourceClaimStatus) bool {
	if len(statusA) != len(statusB) {
		return false
	}
	// 状态条目通常只新增不修改，但这并非强保证，因此仍需逐项比较。
	for i := range statusA {
		if statusA[i].ResourceClaimName != statusB[i].ResourceClaimName {
			return false
		}
		if !slices.Equal(statusA[i].Containers, statusB[i].Containers) {
			return false
		}
		if !nodeAllocatableMappedResourcesEqual(statusA[i].Mapping, statusB[i].Mapping) {
			return false
		}
		if !nodeAllocatableOverheadResourcesEqual(statusA[i].Overhead, statusB[i].Overhead) {
			return false
		}
	}
	return true
}

// nodeAllocatableMappedResourcesEqual 比较可分配资源映射列表的名称与数量是否一致。
func nodeAllocatableMappedResourcesEqual(a, b []api.NodeAllocatableMappedResources) bool {
	itemEqual := func(x, y api.NodeAllocatableMappedResources) bool {
		return x.Name == y.Name && apiresource.QuantityPtrEqual(x.Quantity, y.Quantity)
	}
	return slices.EqualFunc(a, b, itemEqual)
}

// nodeAllocatableOverheadResourcesEqual 比较可分配资源开销列表是否一致。
func nodeAllocatableOverheadResourcesEqual(a, b []api.NodeAllocatableOverheadResources) bool {
	itemEqual := func(x, y api.NodeAllocatableOverheadResources) bool {
		return x.Name == y.Name && apiresource.QuantityPtrEqual(x.PerPod, y.PerPod) && apiresource.QuantityPtrEqual(x.PerContainer, y.PerContainer)
	}
	return slices.EqualFunc(a, b, itemEqual)
}

// admitPodEviction 仅在 Pod 绑定到当前节点时允许节点发起驱逐。
func (p *Plugin) admitPodEviction(nodeName string, a admission.Attributes) error {
	switch a.GetOperation() {
	case admission.Create:
		// 请求对象必须是 Eviction。
		eviction, ok := a.GetObject().(*policy.Eviction)
		if !ok {
			return admission.NewForbidden(a, fmt.Errorf("unexpected type %T", a.GetObject()))
		}
		// 优先使用请求 URL 中的 Pod 名，避免被请求体中的名字误导。
		podName := a.GetName()
		if len(podName) == 0 {
			if len(eviction.Name) == 0 {
				return admission.NewForbidden(a, fmt.Errorf("could not determine pod from request data"))
			}
			podName = eviction.Name
		}
		// 读取现存 Pod 以确认其归属节点。
		existingPod, err := p.podsGetter.Pods(a.GetNamespace()).Get(podName)
		if apierrors.IsNotFound(err) {
			return err
		}
		if err != nil {
			return admission.NewForbidden(a, err)
		}
		// 只能驱逐绑定到自身的 Pod。
		if existingPod.Spec.NodeName != nodeName {
			return admission.NewForbidden(a, fmt.Errorf("node %s can only evict pods with spec.nodeName set to itself", nodeName))
		}
		return nil

	default:
		return admission.NewForbidden(a, fmt.Errorf("unexpected operation %s", a.GetOperation()))
	}
}

// admitPVCStatus 限制节点更新 PVC 时只能修改允许的 status 字段，不能改动元数据。
func (p *Plugin) admitPVCStatus(nodeName string, a admission.Attributes) error {
	switch a.GetOperation() {
	case admission.Update:
		oldPVC, ok := a.GetOldObject().(*api.PersistentVolumeClaim)
		if !ok {
			return admission.NewForbidden(a, fmt.Errorf("unexpected type %T", a.GetOldObject()))
		}

		newPVC, ok := a.GetObject().(*api.PersistentVolumeClaim)
		if !ok {
			return admission.NewForbidden(a, fmt.Errorf("unexpected type %T", a.GetObject()))
		}

		// 复制对象后再清零允许变动的字段。
		oldPVC = oldPVC.DeepCopy()
		newPVC = newPVC.DeepCopy()

		// 新对象可留空 resourceVersion 表示无条件更新，故不参与比较。
		oldPVC.ObjectMeta.ResourceVersion = ""
		newPVC.ObjectMeta.ResourceVersion = ""

		oldPVC.Status.Capacity = nil
		newPVC.Status.Capacity = nil

		oldPVC.Status.Conditions = nil
		newPVC.Status.Conditions = nil

		if p.expansionRecoveryEnabled {
			oldPVC.Status.AllocatedResourceStatuses = nil
			newPVC.Status.AllocatedResourceStatuses = nil

			oldPVC.Status.AllocatedResources = nil
			newPVC.Status.AllocatedResources = nil
		}

		// TODO(apelisse): 目前没有可靠的机制确认只有应变更的字段发生了变化，暂且忽略。
		oldPVC.ObjectMeta.ManagedFields = nil
		newPVC.ObjectMeta.ManagedFields = nil

		// 确保元数据未变，禁止节点重新打标、增加 finalizer 或属主等。
		if !apiequality.Semantic.DeepEqual(oldPVC, newPVC) {
			return admission.NewForbidden(a, fmt.Errorf("node %q is not allowed to update fields other than status.quantity and status.conditions: %v", nodeName, diff.Diff(oldPVC, newPVC)))
		}

		return nil

	default:
		return admission.NewForbidden(a, fmt.Errorf("unexpected operation %q", a.GetOperation()))
	}
}

// admitNode 限制节点只能操作自身的 Node 对象，且不得改动敏感字段。
func (p *Plugin) admitNode(nodeName string, a admission.Attributes) error {
	requestedName := a.GetName()

	if requestedName != nodeName {
		return admission.NewForbidden(a, fmt.Errorf("node %q is not allowed to modify node %q", nodeName, requestedName))
	}

	if a.GetOperation() == admission.Create {
		node, ok := a.GetObject().(*api.Node)
		if !ok {
			return admission.NewForbidden(a, fmt.Errorf("unexpected type %T", a.GetObject()))
		}

		// 禁止创建时设置 configSource：节点权限被限定在 Node.Spec 所列资源内，
		// 允许设置会造成视图权限提升。
		if node.Spec.ConfigSource != nil {
			return admission.NewForbidden(a, fmt.Errorf("node %q is not allowed to create pods with a non-nil configSource", nodeName))
		}

		// 注册时只允许设置白名单内的标签，防止节点通过标签吸引特权工作负载。
		modifiedLabels := getModifiedLabels(node.Labels, nil)
		if forbiddenLabels := p.getForbiddenLabels(modifiedLabels); len(forbiddenLabels) > 0 {
			return admission.NewForbidden(a, fmt.Errorf("node %q is not allowed to set the following labels: %s", nodeName, strings.Join(forbiddenLabels.List(), ", ")))
		}
	}

	if a.GetOperation() == admission.Update {
		node, ok := a.GetObject().(*api.Node)
		if !ok {
			return admission.NewForbidden(a, fmt.Errorf("unexpected type %T", a.GetObject()))
		}
		oldNode, ok := a.GetOldObject().(*api.Node)
		if !ok {
			return admission.NewForbidden(a, fmt.Errorf("unexpected type %T", a.GetObject()))
		}

		// 禁止更新 configSource，理由同创建阶段。
		// 仅在新值非 nil 时比较：旧 kubelet 可能在 status 更新中丢掉该字段。
		if node.Spec.ConfigSource != nil && !apiequality.Semantic.DeepEqual(node.Spec.ConfigSource, oldNode.Spec.ConfigSource) {
			return admission.NewForbidden(a, fmt.Errorf("node %q is not allowed to update configSource to a new non-nil configSource", nodeName))
		}

		// 禁止修改污点，否则节点可移除限制、吸引本不该调度过来的工作负载。
		if !apiequality.Semantic.DeepEqual(node.Spec.Taints, oldNode.Spec.Taints) {
			return admission.NewForbidden(a, fmt.Errorf("node %q is not allowed to modify taints", nodeName))
		}

		// 禁止修改属主引用，防止节点篡改自身归属。
		if !apiequality.Semantic.DeepEqual(node.OwnerReferences, oldNode.OwnerReferences) {
			return admission.NewForbidden(a, fmt.Errorf("node %q is not allowed to modify ownerReferences", nodeName))
		}

		// 更新标签同样受限，防止将特权工作负载引到自身。
		modifiedLabels := getModifiedLabels(node.Labels, oldNode.Labels)
		if forbiddenUpdateLabels := p.getForbiddenLabels(modifiedLabels); len(forbiddenUpdateLabels) > 0 {
			return admission.NewForbidden(a, fmt.Errorf("is not allowed to modify labels: %s", strings.Join(forbiddenUpdateLabels.List(), ", ")))
		}
	}

	return nil
}

// getModifiedLabels 返回两个标签映射中值不同的键集合。
func getModifiedLabels(a, b map[string]string) sets.String {
	modified := sets.NewString()
	for k, v1 := range a {
		if v2, ok := b[k]; !ok || v1 != v2 {
			modified.Insert(k)
		}
	}
	for k, v1 := range b {
		if v2, ok := a[k]; !ok || v1 != v2 {
			modified.Insert(k)
		}
	}
	return modified
}

// isKubernetesLabel 判断标签是否属于 Kubernetes 保留的命名空间。
func isKubernetesLabel(key string) bool {
	namespace := getLabelNamespace(key)
	if namespace == "kubernetes.io" || strings.HasSuffix(namespace, ".kubernetes.io") {
		return true
	}
	if namespace == "k8s.io" || strings.HasSuffix(namespace, ".k8s.io") {
		return true
	}
	return false
}

// getLabelNamespace 提取标签键的前缀部分，无前缀时返回空串。
func getLabelNamespace(key string) string {
	if parts := strings.SplitN(key, "/", 2); len(parts) == 2 {
		return parts[0]
	}
	return ""
}

// getForbiddenLabels 从变更的标签中筛出节点无权添加、删除或修改的部分。
func (p *Plugin) getForbiddenLabels(modifiedLabels sets.String) sets.String {
	if len(modifiedLabels) == 0 {
		return nil
	}

	forbiddenLabels := sets.NewString()
	for label := range modifiedLabels {
		namespace := getLabelNamespace(label)
		// node-restriction 命名空间下的标签由控制面管理，kubelet 不得设置。
		if namespace == v1.LabelNamespaceNodeRestriction || strings.HasSuffix(namespace, "."+v1.LabelNamespaceNodeRestriction) {
			forbiddenLabels.Insert(label)
		}
		// kubelet 也只能设置其已知的系统标签，不能随意写 kubernetes.io / k8s.io 标签。
		if isKubernetesLabel(label) && !kubeletapis.IsKubeletLabel(label) {
			// TODO: 待标签策略就绪后交由其判断。
			forbiddenLabels.Insert(label)
		}
	}
	return forbiddenLabels
}

// admitServiceAccount 校验节点请求 ServiceAccount 令牌时绑定的 Pod 是否运行在自身之上。
func (p *Plugin) admitServiceAccount(ctx context.Context, nodeName string, a admission.Attributes) error {
	if a.GetOperation() != admission.Create {
		return nil
	}
	if a.GetSubresource() != "token" {
		return nil
	}
	tr, ok := a.GetObject().(*authenticationapi.TokenRequest)
	if !ok {
		return admission.NewForbidden(a, fmt.Errorf("unexpected type %T", a.GetObject()))
	}

	// 节点的令牌请求必须绑定 Pod，且该 Pod 必须调度在本节点上。
	ref := tr.Spec.BoundObjectRef
	if ref == nil ||
		ref.APIVersion != "v1" ||
		ref.Kind != "Pod" ||
		ref.Name == "" {
		return admission.NewForbidden(a, fmt.Errorf("node requested token not bound to a pod"))
	}
	if ref.UID == "" {
		return admission.NewForbidden(a, fmt.Errorf("node requested token with a pod binding without a uid"))
	}
	pod, err := p.podsGetter.Pods(a.GetNamespace()).Get(ref.Name)
	if apierrors.IsNotFound(err) {
		return err
	}
	if err != nil {
		return admission.NewForbidden(a, err)
	}
	if ref.UID != pod.UID {
		return admission.NewForbidden(a, fmt.Errorf("the UID in the bound object reference (%s) does not match the UID in record. The object might have been deleted and then recreated", ref.UID))
	}
	if pod.Spec.NodeName != nodeName {
		return admission.NewForbidden(a, fmt.Errorf("node requested token bound to a pod scheduled on a different node"))
	}

	if p.serviceAccountNodeAudienceRestriction {
		if err := p.validateNodeServiceAccountAudience(ctx, tr, pod, a); err != nil {
			return admission.NewForbidden(a, err)
		}
	}

	// 注意：令牌同一时刻只能绑定一个对象。强制绑定 Pod 可杜绝伪造 Node 绑定；
	// kube-apiserver 在收到 Pod 绑定后会自动推断并设置 Node 绑定。详见：
	// https://github.com/kubernetes/kubernetes/issues/121723 for more info.

	return nil
}

// validateNodeServiceAccountAudience 校验节点请求的 audience 已在 Pod 中声明，或节点具备相应授权。
func (p *Plugin) validateNodeServiceAccountAudience(ctx context.Context, tr *authenticationapi.TokenRequest, pod *v1.Pod, a admission.Attributes) error {
	// 请求的 audience 必须能在 Pod 的卷挂载中找到，这里最多只允许一个。
	requestedAudience := ""
	switch len(tr.Spec.Audiences) {
	case 0:
		requestedAudience = ""
	case 1:
		requestedAudience = tr.Spec.Audiences[0]
	default:
		return fmt.Errorf("node may only request 0 or 1 audiences")
	}

	foundAudiencesInPodSpec, err := p.podReferencesAudience(ctx, pod, requestedAudience)
	if err != nil {
		return fmt.Errorf("error validating audience %q: %w", requestedAudience, err)
	}
	if foundAudiencesInPodSpec {
		return nil
	}

	attrs := buildAuthorizerAttributes(
		a,
		"request-serviceaccounts-token-audience",
		requestedAudience,
		a.GetName(), // 该 API 操作针对 ServiceAccount，故此名称为 SA 名。
	)

	authorized, _, err := p.authz.Authorize(ctx, attrs)
	// RBAC 等鉴权器可能在求值出错的情况下仍允许请求，因此先检查决策再看错误。
	// 与 withAuthorization 保持同样顺序（ref: https://github.com/kubernetes/kubernetes/blob/2b025e645975d6d51bf38c008f972c632cf49657/staging/src/k8s.io/apiserver/pkg/endpoints/filters/authorization.go#L71-L91）
	if authorized == authorizer.DecisionAllow {
		return nil
	}
	if err != nil {
		return fmt.Errorf("error authorizing %s to request tokens for audience %q: %w", a.GetUserInfo().GetName(), requestedAudience, err)
	}

	return fmt.Errorf("%s is not authorized to request tokens for audience %q", a.GetUserInfo().GetName(), requestedAudience)
}

// podReferencesAudience 判断 Pod 的卷定义（含 CSI 链路）是否引用了指定 audience。
func (p *Plugin) podReferencesAudience(ctx context.Context, pod *v1.Pod, audience string) (bool, error) {
	var errs []error

	for _, v := range pod.Spec.Volumes {
		if v.Projected != nil {
			for _, src := range v.Projected.Sources {
				if src.ServiceAccountToken != nil && src.ServiceAccountToken.Audience == audience {
					return true, nil
				}
			}
		}

		// 也允许经由 CSI 驱动请求令牌的 audience，覆盖以下链路：
		// - pod --> ephemeral --> pvc --> pv --> csi --> driver --> tokenrequest with audience
		// - pod --> pvc --> pv --> csi --> driver --> tokenrequest with audience
		// - pod --> csi --> driver --> tokenrequest with audience
		var driverName string
		var err error
		switch {
		case v.Ephemeral != nil && v.Ephemeral.VolumeClaimTemplate != nil:
			pvcName := ephemeral.VolumeClaimName(pod, &v)
			driverName, err = p.getCSIFromPVC(ctx, pod.Namespace, pvcName)
		case v.PersistentVolumeClaim != nil:
			driverName, err = p.getCSIFromPVC(ctx, pod.Namespace, v.PersistentVolumeClaim.ClaimName)
		case v.CSI != nil:
			driverName = v.CSI.Driver
		case p.csiTranslator.IsInlineMigratable(&v):
			pv, translateErr := p.csiTranslator.TranslateInTreeInlineVolumeToCSI(klog.FromContext(ctx), &v, pod.Namespace)
			if translateErr != nil {
				err = translateErr
				break
			}
			if pv != nil && pv.Spec.CSI != nil {
				driverName = pv.Spec.CSI.Driver
			}
		}

		if err != nil {
			errs = append(errs, err)
			continue
		}

		if len(driverName) > 0 {
			hasAudience, hasAudienceErr := p.csiDriverHasAudience(driverName, audience)
			if hasAudienceErr != nil {
				errs = append(errs, hasAudienceErr)
				continue
			}
			if hasAudience {
				return true, nil
			}
		}
	}

	return false, utilerrors.NewAggregate(errs)
}

// getCSIFromPVC 沿 PVC -> PV -> CSI -> Driver 链路找出 CSI 驱动名。
func (p *Plugin) getCSIFromPVC(ctx context.Context, namespace, claimName string) (string, error) {
	pvc, err := p.pvcGetter.PersistentVolumeClaims(namespace).Get(claimName)
	if err != nil {
		return "", err
	}
	pv, err := p.pvGetter.Get(pvc.Spec.VolumeName)
	if err != nil {
		return "", err
	}
	if pv.Spec.CSI != nil {
		return pv.Spec.CSI.Driver, nil
	}

	if p.csiTranslator.IsPVMigratable(pv) {
		// 树内 PV 需先转换为 CSI 表示，才能读到驱动名。
		translatedPV, err := p.csiTranslator.TranslateInTreePVToCSI(klog.FromContext(ctx), pv)
		if err != nil {
			return "", err
		}
		if translatedPV != nil && translatedPV.Spec.CSI != nil {
			return translatedPV.Spec.CSI.Driver, nil
		}
	}

	return "", nil
}

// csiDriverHasAudience 检查指定 CSI 驱动的 TokenRequests 是否包含给定 audience。
func (p *Plugin) csiDriverHasAudience(driverName, audience string) (bool, error) {
	driver, err := p.csiDriverGetter.Get(driverName)
	if err != nil {
		return false, err
	}

	for _, tokenRequest := range driver.Spec.TokenRequests {
		if tokenRequest.Audience == audience {
			return true, nil
		}
	}
	return false, nil
}

// admitPodCertificateRequest 校验节点提交的 PodCertificateRequest 与运行中的 Pod 及 ServiceAccount 是否一致。
func (p *Plugin) admitPodCertificateRequest(ctx context.Context, nodeName string, a admission.Attributes) error {
	if !p.podCertificateRequestsEnabled {
		return admission.NewForbidden(a, fmt.Errorf("PodCertificateRequest feature gate is disabled"))
	}

	if a.GetOperation() != admission.Create {
		return admission.NewForbidden(a, fmt.Errorf("unexpected operation %v", a.GetOperation()))
	}

	if len(a.GetSubresource()) != 0 {
		return admission.NewForbidden(a, fmt.Errorf("unexpected subresource %v", a.GetSubresource()))
	}

	namespace := a.GetNamespace()

	req, ok := a.GetObject().(*certapi.PodCertificateRequest)
	if !ok {
		return admission.NewForbidden(a, fmt.Errorf("unexpected type %T", a.GetObject()))
	}

	// 交叉核对请求中声明的节点名和 UID 与请求方身份。
	if string(req.Spec.NodeName) != nodeName {
		return admission.NewForbidden(a, fmt.Errorf("PodCertificateRequest.Spec.NodeName=%q, which is not the requesting node %q", req.Spec.NodeName, nodeName))
	}
	node, err := p.nodesGetter.Get(string(req.Spec.NodeName))
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("while retrieving node %q named in the PodCertificateRequest: %w", req.Spec.NodeName, err)
	}
	if err != nil {
		return admission.NewForbidden(a, fmt.Errorf("while retrieving node %q named in the PodCertificateRequest: %w", req.Spec.NodeName, err))
	}
	if node.ObjectMeta.UID != req.Spec.NodeUID {
		// 可能只是 informer 缓存滞后，不返回 Forbidden，以便重试有机会成功。
		return fmt.Errorf("PodCertificateRequest for pod %q names node UID %q, inconsistent with the running node (%q)", namespace+"/"+req.Spec.PodName, req.Spec.NodeUID, node.ObjectMeta.UID)
	}

	// 确认请求指向的 Pod 真实存在且运行在该节点上。
	pod, err := p.podsGetter.Pods(namespace).Get(req.Spec.PodName)
	if apierrors.IsNotFound(err) {
		return fmt.Errorf("while retrieving pod %q named in the PodCertificateRequest: %w", namespace+"/"+req.Spec.PodName, err)
	}
	if err != nil {
		return admission.NewForbidden(a, fmt.Errorf("while retrieving pod %q named in the PodCertificateRequest: %w", namespace+"/"+req.Spec.PodName, err))
	}
	if req.Spec.PodUID != pod.ObjectMeta.UID {
		// 可能只是 informer 缓存滞后，不返回 Forbidden，以便重试有机会成功。
		return fmt.Errorf("PodCertificateRequest for pod %q contains pod UID (%q) which differs from running pod %q", namespace+"/"+req.Spec.PodName, req.Spec.PodUID, string(pod.ObjectMeta.UID))
	}
	if pod.Spec.NodeName != string(req.Spec.NodeName) {
		return admission.NewForbidden(a, fmt.Errorf("pod %q is not running on node %q named in the PodCertificateRequest", namespace+"/"+req.Spec.PodName, req.Spec.NodeName))
	}

	// 静态 Pod 不颁发 Pod 证书。
	if _, isMirror := pod.Annotations[api.MirrorPodAnnotationKey]; isMirror {
		return admission.NewForbidden(a, fmt.Errorf("pod %q is a mirror pod", namespace+"/"+req.Spec.PodName))
	}

	if req.Spec.ServiceAccountName != pod.Spec.ServiceAccountName {
		// UID 已匹配，故不可能是缓存滞后导致，可以直接拒绝。
		return admission.NewForbidden(a, fmt.Errorf("PodCertificateRequest for pod %q contains serviceAccountName (%q) that differs from running pod (%q)", namespace+"/"+req.Spec.PodName, req.Spec.ServiceAccountName, pod.Spec.ServiceAccountName))
	}
	sa, err := p.serviceAccountGetter.ServiceAccounts(namespace).Get(req.Spec.ServiceAccountName)
	if apierrors.IsNotFound(err) {
		// 可能只是 informer 缓存滞后，不返回 Forbidden，以便重试有机会成功。
		return fmt.Errorf("while retrieving service account %q named in the PodCertificateRequest: %w", namespace+"/"+req.Spec.ServiceAccountName, err)
	}
	if err != nil {
		return admission.NewForbidden(a, fmt.Errorf("while retrieving service account %q named in the PodCertificateRequest: %w", namespace+"/"+req.Spec.ServiceAccountName, err))
	}
	if req.Spec.ServiceAccountUID != sa.ObjectMeta.UID {
		// 可能只是 informer 缓存滞后，不返回 Forbidden，以便重试有机会成功。
		return fmt.Errorf("PodCertificateRequest for pod %q names service account UID %q, which differs from the running service account (%q)", namespace+"/"+req.Spec.PodName, req.Spec.ServiceAccountUID, sa.ObjectMeta.UID)
	}

	// 最后确认节点确有理由请求该 ServiceAccount 与签名者的组合。
	if err := p.validateNodePodCertificateSigner(ctx, pod, req, a); err != nil {
		return admission.NewForbidden(a, err)
	}

	return nil
}

// validateNodePodCertificateSigner 确认 Pod 挂载了对应签名者的证书卷，或节点具备显式授权。
func (p *Plugin) validateNodePodCertificateSigner(ctx context.Context, pod *v1.Pod, req *certapi.PodCertificateRequest, a admission.Attributes) error {
	// Pod 直接声明了该签名者，则无需额外授权。
	if podHasPodCertificateVolumeForSigner(pod, req.Spec.SignerName) {
		return nil
	}

	// 否则回退到鉴权检查，逻辑类似 validateNodeServiceAccountAudience。

	// RBAC 无法处理含斜杠的资源名，沿用将 signerName 编码进 ClusterTrustBundle 名称时的替换规则。
	mungedSignerName := strings.ReplaceAll(req.Spec.SignerName, "/", ":")

	attrs := buildAuthorizerAttributes(
		a,
		"request-serviceaccounts-podcertificate-signer",
		mungedSignerName,
		req.Spec.ServiceAccountName,
	)

	authorized, _, err := p.authz.Authorize(ctx, attrs)
	// RBAC 等鉴权器可能在求值出错的情况下仍允许请求，因此先检查决策再看错误。
	// 与 withAuthorization 保持同样顺序（ref: https://github.com/kubernetes/kubernetes/blob/2b025e645975d6d51bf38c008f972c632cf49657/staging/src/k8s.io/apiserver/pkg/endpoints/filters/authorization.go#L71-L91）
	if authorized == authorizer.DecisionAllow {
		return nil
	}
	if err != nil {
		return fmt.Errorf("error authorizing %s to request podcertificaterequests for signer %q: %w", a.GetUserInfo().GetName(), req.Spec.SignerName, err)
	}
	return fmt.Errorf("pod %q does not mount a podCertificate projected volume for signer %q", a.GetNamespace()+"/"+req.Spec.PodName, req.Spec.SignerName)
}

// buildAuthorizerAttributes 根据准入属性构造鉴权所需的属性记录。
func buildAuthorizerAttributes(a admission.Attributes, verb, resource, name string) authorizer.AttributesRecord {
	return authorizer.AttributesRecord{
		User:            a.GetUserInfo(),
		Verb:            verb,
		APIGroup:        a.GetResource().Group,
		APIVersion:      a.GetResource().Version,
		Resource:        resource,
		Namespace:       a.GetNamespace(),
		Name:            name,
		ResourceRequest: true,
	}
}

// podHasPodCertificateVolumeForSigner 检查 Pod 是否挂载了指定签名者的 podCertificate 卷。
func podHasPodCertificateVolumeForSigner(pod *v1.Pod, signerName string) bool {
	for i := range pod.Spec.Volumes {
		if pod.Spec.Volumes[i].Projected == nil {
			continue
		}
		for j := range pod.Spec.Volumes[i].Projected.Sources {
			s := &pod.Spec.Volumes[i].Projected.Sources[j]
			if s.PodCertificate != nil && s.PodCertificate.SignerName == signerName {
				return true
			}
		}
	}
	return false
}

// admitLease 限制节点只能访问自己名下、位于专用系统命名空间中的 Lease。
func (p *Plugin) admitLease(nodeName string, a admission.Attributes) error {
	// Lease 必须位于节点专用的系统命名空间。
	if a.GetNamespace() != api.NamespaceNodeLease {
		return admission.NewForbidden(a, fmt.Errorf("can only access leases in the %q system namespace", api.NamespaceNodeLease))
	}

	// 请求方节点名必须与 Lease 名一致。
	if a.GetOperation() == admission.Create {
		// 创建时 a.GetName() 为空，只能从请求对象取名字。
		lease, ok := a.GetObject().(*coordapi.Lease)
		if !ok {
			return admission.NewForbidden(a, fmt.Errorf("unexpected type %T", a.GetObject()))
		}
		if lease.Name != nodeName {
			return admission.NewForbidden(a, fmt.Errorf("can only access node lease with the same name as the requesting node"))
		}
	} else {
		if a.GetName() != nodeName {
			return admission.NewForbidden(a, fmt.Errorf("can only access node lease with the same name as the requesting node"))
		}
	}

	return nil
}

// admitCSINode 限制节点只能操作同名 CSINode，并管控 status 子资源的使用。
func (p *Plugin) admitCSINode(nodeName string, a admission.Attributes) error {
	// 请求方节点名必须与 CSINode 对象名一致。
	if a.GetOperation() == admission.Create {
		// 创建时 a.GetName() 为空，需从请求对象提取名字。
		accessor, err := meta.Accessor(a.GetObject())
		if err != nil {
			return admission.NewForbidden(a, fmt.Errorf("unable to access the object name"))
		}
		if accessor.GetName() != nodeName {
			return admission.NewForbidden(a, fmt.Errorf("can only access CSINode with the same name as the requesting node"))
		}
	} else {
		if a.GetName() != nodeName {
			return admission.NewForbidden(a, fmt.Errorf("can only access CSINode with the same name as the requesting node"))
		}
	}

	if a.GetSubresource() == "status" {
		if !p.csiVolumeHealthEnabled {
			return admission.NewForbidden(a, fmt.Errorf("CSIVolumeHealth feature gate is disabled"))
		}
		return p.admitCSINodeStatus(nodeName, a)
	}

	return nil
}

// admitCSINodeStatus 确保节点无法通过 status 子资源修改 CSINode 的 Spec。
func (p *Plugin) admitCSINodeStatus(nodeName string, a admission.Attributes) error {
	switch a.GetOperation() {
	case admission.Update:
		oldCSINode, ok := a.GetOldObject().(*storage.CSINode)
		if !ok {
			return admission.NewForbidden(a, fmt.Errorf("unexpected type %T", a.GetOldObject()))
		}
		newCSINode, ok := a.GetObject().(*storage.CSINode)
		if !ok {
			return admission.NewForbidden(a, fmt.Errorf("unexpected type %T", a.GetObject()))
		}
		if !apiequality.Semantic.DeepEqual(oldCSINode.Spec, newCSINode.Spec) {
			return admission.NewForbidden(a, fmt.Errorf("node %q is not allowed to modify CSINode spec via status", nodeName))
		}
		return nil
	default:
		return admission.NewForbidden(a, fmt.Errorf("unexpected operation %q", a.GetOperation()))
	}
}

// admitResourceSlice 校验节点创建或删除的 ResourceSlice 是否声明了本节点名。
func (p *Plugin) admitResourceSlice(nodeName string, a admission.Attributes) error {
	// 创建或删除时，ResourceSlice 的 NodeName 必须与请求方节点一致。
	//
	// 其他操作由节点鉴权器检查。这里仍需额外校验，因为鉴权器无法看到创建对象的内容，
	// 也无法逐一检查 DeleteCollection 中被删除的每个对象。
	switch a.GetOperation() {
	case admission.Create:
		slice, ok := a.GetObject().(*resource.ResourceSlice)
		if !ok {
			return admission.NewForbidden(a, fmt.Errorf("unexpected type %T", a.GetObject()))
		}

		if slice.Spec.NodeName == nil || *slice.Spec.NodeName != nodeName {
			return admission.NewForbidden(a, errors.New("can only create ResourceSlice with the same NodeName as the requesting node"))
		}
	case admission.Delete:
		slice, ok := a.GetOldObject().(*resource.ResourceSlice)
		if !ok {
			return admission.NewForbidden(a, fmt.Errorf("unexpected type %T", a.GetOldObject()))
		}

		if slice.Spec.NodeName == nil || *slice.Spec.NodeName != nodeName {
			return admission.NewForbidden(a, errors.New("can only delete ResourceSlice with the same NodeName as the requesting node"))
		}
	}

	return nil
}

// admitCSR 限制节点创建的 kubelet 相关 CSR 使用与自身节点名对应的 CN。
func (p *Plugin) admitCSR(nodeName string, a admission.Attributes) error {
	// 对于 kubelet 服务端和 API server 客户端签名者，以 "system:node:" 开头的 CN
	// 必须精确等于节点名。其他 CSR 属性由签名者在校验阶段检查。
	if a.GetOperation() != admission.Create {
		return nil
	}

	csr, ok := a.GetObject().(*certapi.CertificateSigningRequest)
	if !ok {
		return admission.NewForbidden(a, fmt.Errorf("unexpected type %T", a.GetObject()))
	}
	if csr.Spec.SignerName != certapi.KubeletServingSignerName && csr.Spec.SignerName != certapi.KubeAPIServerClientKubeletSignerName {
		return nil
	}

	x509cr, err := certapi.ParseCSR(csr.Spec.Request)
	if err != nil {
		return admission.NewForbidden(a, fmt.Errorf("unable to parse csr: %w", err))
	}
	if x509cr.Subject.CommonName != fmt.Sprintf("system:node:%s", nodeName) {
		return admission.NewForbidden(a, fmt.Errorf("can only create a node CSR with CN=system:node:%s", nodeName))
	}

	return nil
}
