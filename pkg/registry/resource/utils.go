/*
Copyright 2025 The Kubernetes Authors.

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

package resource

import (
	"context"
	"fmt"

	resourcev1 "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/authentication/serviceaccount"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizer"
	"k8s.io/apiserver/pkg/endpoints/filters"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	v1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/apis/resource"
)

// AuthorizedForAdmin 根据 Namespace 标签检查请求是否有权为设备申请 AdminAccess。
func AuthorizedForAdmin(ctx context.Context, deviceRequests []resource.DeviceRequest, namespaceName string, nsClient v1.NamespaceInterface) field.ErrorList {
	var allErrs field.ErrorList
	adminRequested := false
	var adminAccessPath *field.Path

	// ResourceClaim Spec 创建后不可变，因此无需比较旧请求。

	for i := range deviceRequests {
		// 子请求不能设置 AdminAccess；该字段只能出现在 Exactly 请求中。
		if deviceRequests[i].Exactly == nil {
			continue
		}
		value := deviceRequests[i].Exactly.AdminAccess
		if value != nil && *value {
			adminRequested = true
			adminAccessPath = field.NewPath("spec", "devices", "requests").Index(i).Child("adminAccess")
			break
		}
	}
	if !adminRequested {
		// 只有明确请求 AdminAccess 时才需要执行授权校验。
		return allErrs
	}

	// 从存储中读取所属 Namespace，以检查其授权标签。
	ns, err := nsClient.Get(ctx, namespaceName, metav1.GetOptions{})
	if err != nil {
		return append(allErrs, field.InternalError(adminAccessPath, fmt.Errorf("could not retrieve namespace to verify admin access: %w", err)))
	}
	if ns.Labels[resource.DRAAdminNamespaceLabelKey] != "true" {
		return append(allErrs, field.Forbidden(adminAccessPath, fmt.Sprintf("admin access to devices requires the `%s: true` label on the containing namespace", resource.DRAAdminNamespaceLabelKey)))
	}

	return allErrs
}

// AuthorizedForAdminStatus 根据 Namespace 标签检查 Status 中新增的设备 AdminAccess 是否获得授权。
func AuthorizedForAdminStatus(ctx context.Context, newAllocationResult, oldAllocationResult []resource.DeviceRequestAllocationResult, namespaceName string, nsClient v1.NamespaceInterface) field.ErrorList {
	var allErrs field.ErrorList
	var adminAccessPath *field.Path

	if wasGranted, _ := adminRequested(oldAllocationResult); wasGranted {
		// 如果旧 Status 已经授予 AdminAccess，则无需重复校验，因为 Status.Allocation 不可变。
		return allErrs
	}
	isRequested, adminAccessPath := adminRequested(newAllocationResult)
	if !isRequested {
		// 只有明确请求 AdminAccess 时才需要执行授权校验。
		return allErrs
	}

	// 从存储中读取所属 Namespace，以检查其授权标签。
	ns, err := nsClient.Get(ctx, namespaceName, metav1.GetOptions{})
	if err != nil {
		return append(allErrs, field.InternalError(adminAccessPath, fmt.Errorf("could not retrieve namespace to verify admin access: %w", err)))
	}
	if ns.Labels[resource.DRAAdminNamespaceLabelKey] != "true" {
		return append(allErrs, field.Forbidden(adminAccessPath, fmt.Sprintf("admin access to devices requires the `%s: true` label on the containing namespace", resource.DRAAdminNamespaceLabelKey)))
	}

	return allErrs
}

// adminRequested 检查设备分配结果中是否包含 AdminAccess=true，并返回对应字段路径。
func adminRequested(deviceRequestResults []resource.DeviceRequestAllocationResult) (bool, *field.Path) {
	for i := range deviceRequestResults {
		value := deviceRequestResults[i].AdminAccess
		if value != nil && *value {
			return true, field.NewPath("status", "allocation", "devices", "results").Index(i).Child("adminAccess")
		}
	}
	return false, nil
}

// AuthorizedForBinding 通过检查合成的 resourceclaims/binding 子资源权限，
// 判断调用者是否有权更新 status.allocation 和 status.reservedFor。
func AuthorizedForBinding(ctx context.Context, fieldPath *field.Path, authz authorizer.UnconditionalAuthorizer, newStatus, oldStatus resource.ResourceClaimStatus) field.ErrorList {
	var allErrs field.ErrorList

	if equality.Semantic.DeepEqual(newStatus.Allocation, oldStatus.Allocation) &&
		equality.Semantic.DeepEqual(newStatus.ReservedFor, oldStatus.ReservedFor) {
		return allErrs
	}

	baseAttrs, err := filters.GetAuthorizerAttributes(ctx)
	if err != nil {
		return append(allErrs, field.InternalError(fieldPath, fmt.Errorf("cannot build authorizer attributes: %w", err)))
	}

	attrs := &syntheticSubresourceAttrs{
		Attributes:  baseAttrs,
		verb:        baseAttrs.GetVerb(),           // verb is unchanged but must be specified
		subresource: resourcev1.SubresourceBinding, // the scheduler calls this "bind claim"
		namespace:   "",                            // cluster-wide
		name:        "",                            // all names
	}

	if err := checkAuthorization(ctx, authz, attrs); err != nil {
		return append(allErrs, field.Forbidden(fieldPath, fmt.Sprintf(`changing status.allocation or status.reservedFor requires resource="resourceclaims/binding", verb="%s" permission: %s`, attrs.verb, err)))
	}
	return allErrs
}

// AuthorizedForDeviceStatus 通过合成的 resourceclaims/driver 子资源，按 Driver 分别执行授权检查。
// 如果 ServiceAccount 与设备分配到同一节点，优先使用 associated-node 动词前缀；否则使用 arbitrary-node 前缀。
func AuthorizedForDeviceStatus(ctx context.Context, fieldPath *field.Path, a authorizer.UnconditionalAuthorizer, newStatus, oldStatus resource.ResourceClaimStatus) field.ErrorList {
	var allErrs field.ErrorList

	driversToAuthz := getModifiedDrivers(newStatus, oldStatus)
	if len(driversToAuthz) == 0 {
		return allErrs
	}

	baseAttrs, err := filters.GetAuthorizerAttributes(ctx)
	if err != nil {
		return append(allErrs, field.InternalError(fieldPath, fmt.Errorf("cannot build authorizer attributes: %w", err)))
	}

	// 如果 ServiceAccount 所属节点与 ResourceClaim 分配节点相同，先检查 associated-node 动词，失败后回退到 arbitrary-node；
	// 否则只检查 arbitrary-node 动词。
	requestVerb := baseAttrs.GetVerb()
	var verbs []string
	if saAssociatedWithAllocatedNode(baseAttrs.GetUser(), nodeNameFromAllocation(newStatus.Allocation)) {
		verbs = []string{resourcev1.VerbPrefixAssociatedNode + requestVerb, resourcev1.VerbPrefixArbitraryNode + requestVerb}
	} else {
		verbs = []string{resourcev1.VerbPrefixArbitraryNode + requestVerb}
	}

	for _, driverName := range sets.List(driversToAuthz) {
		if err := checkDriverAuthorization(ctx, baseAttrs, verbs, driverName, a); err != nil {
			allErrs = append(allErrs, field.Forbidden(fieldPath, fmt.Sprintf(`changing status.devices requires resource="resourceclaims/driver", verb="%s" permission: %s`, verbs, err)))
		}

	}

	return allErrs
}

// checkDriverAuthorization 按顺序尝试给定动词；任一动词授权成功即视为通过。
func checkDriverAuthorization(ctx context.Context, baseAttrs authorizer.Attributes, verbs []string, driverName string, a authorizer.UnconditionalAuthorizer) error {
	if len(verbs) == 0 {
		return fmt.Errorf("no verbs set for driver %s", driverName) // impossible for all inputs today
	}

	var firstErr error
	for _, verb := range verbs { // verbs are OR'd with each other
		attrs := &syntheticSubresourceAttrs{
			Attributes:  baseAttrs,
			verb:        verb,
			subresource: resourcev1.SubresourceDriver,
			namespace:   baseAttrs.GetNamespace(),
			name:        driverName,
		}
		err := checkAuthorization(ctx, a, attrs)
		if err == nil {
			return nil
		}
		if firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// getModifiedDrivers 找出新旧 ResourceClaim 之间设备状态条目新增、删除或发生变化的全部 Driver。
func getModifiedDrivers(newAllocatedDeviceStatus, oldAllocatedDeviceStatus resource.ResourceClaimStatus) sets.Set[string] {
	driversToAuthz := sets.Set[string]{}

	oldDevices := make(map[deviceKey]resource.AllocatedDeviceStatus)
	for _, d := range oldAllocatedDeviceStatus.Devices {
		oldDevices[makeDeviceKey(d)] = d
	}

	// 检查新增或发生变化的设备状态条目。
	for _, d := range newAllocatedDeviceStatus.Devices {
		key := makeDeviceKey(d)
		oldDevice, ok := oldDevices[key]
		delete(oldDevices, key) // 从旧设备映射中删除已处理条目，剩余条目即为已移除设备。

		// 如果设备条目是新增的或内容发生变化，则需要校验对应 Driver 的授权。
		if !ok || !equality.Semantic.DeepEqual(oldDevice, d) {
			driversToAuthz.Insert(d.Driver)
		}
	}

	// 检查已经从新对象中移除的设备状态条目。
	for _, d := range oldDevices {
		// oldDevices 中剩余的设备均已从新 ResourceClaim 状态中移除。
		driversToAuthz.Insert(d.Driver)
	}

	return driversToAuthz
}

type deviceKey struct {
	driver  string
	pool    string
	device  string
	shareID string
}

// makeDeviceKey 使用 Driver、Pool、Device 和 ShareID 唯一标识一个已分配设备。
func makeDeviceKey(d resource.AllocatedDeviceStatus) deviceKey {
	key := deviceKey{
		driver: d.Driver,
		pool:   d.Pool,
		device: d.Device,
	}
	if d.ShareID != nil {
		key.shareID = *d.ShareID
	}
	return key
}

// nodeNameFromAllocation 仅在 NodeSelector 能精确表达单个 metadata.name 时返回节点名称。
func nodeNameFromAllocation(allocation *resource.AllocationResult) string {
	if allocation == nil || allocation.NodeSelector == nil {
		return ""
	}
	ns := allocation.NodeSelector
	if len(ns.NodeSelectorTerms) != 1 {
		return ""
	}
	term := ns.NodeSelectorTerms[0]
	if len(term.MatchExpressions) != 0 || len(term.MatchFields) != 1 {
		return ""
	}
	f := term.MatchFields[0]
	if f.Key != "metadata.name" || f.Operator != core.NodeSelectorOpIn || len(f.Values) != 1 {
		return ""
	}
	return f.Values[0]
}

// saAssociatedWithAllocatedNode 检查调用者是否为带有唯一、合法 NodeName 扩展属性的 ServiceAccount，且该节点与分配节点一致。
func saAssociatedWithAllocatedNode(u user.Info, allocatedNodeName string) bool {
	if len(allocatedNodeName) == 0 {
		return false
	}

	// 调用者必须是 ServiceAccount。
	if _, _, err := serviceaccount.SplitUsername(u.GetName()); err != nil {
		return false
	}

	// 用户信息必须且只能包含一个节点名称扩展属性。
	nodeNames := u.GetExtra()[serviceaccount.NodeNameKey]
	if len(nodeNames) != 1 {
		return false
	}
	nodeName := nodeNames[0]

	// 扩展属性中的节点名称必须符合 Kubernetes 节点名称格式。
	if len(validation.ValidateNodeName(nodeName, false)) != 0 {
		return false
	}

	return nodeName == allocatedNodeName
}

// checkAuthorization 执行授权判断；明确 Allow 时即使同时返回评估错误也视为授权成功。
func checkAuthorization(ctx context.Context, a authorizer.UnconditionalAuthorizer, attributes authorizer.Attributes) error {
	authorized, reason, err := a.Authorize(ctx, attributes)

	// RBAC 等授权器可能在评估过程中遇到错误，但最终仍明确允许请求；因此这里必须先判断授权决策，再处理错误。
	if authorized == authorizer.DecisionAllow {
		return nil
	}

	msg := reason
	switch {
	case err != nil && len(reason) > 0:
		msg = fmt.Sprintf("%v: %s", err, reason)
	case err != nil:
		msg = err.Error()
	}

	return responsewriters.ForbiddenStatusError(attributes, msg)
}

// syntheticSubresourceAttrs 在原始请求授权属性上覆盖 verb、subresource、namespace 和 name，构造 DRA 专用子资源授权请求。
type syntheticSubresourceAttrs struct {
	authorizer.Attributes
	verb        string
	subresource string
	namespace   string
	name        string
}

func (a *syntheticSubresourceAttrs) GetVerb() string        { return a.verb }
func (a *syntheticSubresourceAttrs) GetSubresource() string { return a.subresource }
func (a *syntheticSubresourceAttrs) GetNamespace() string   { return a.namespace }
func (a *syntheticSubresourceAttrs) GetName() string        { return a.name }
