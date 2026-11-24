/*
Copyright 2014 The Kubernetes Authors.

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
	"net"
	"net/http"
	"net/url"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilnet "k8s.io/apimachinery/pkg/util/net"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/registry/generic"
	"k8s.io/apiserver/pkg/registry/rest"
	pkgstorage "k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/client"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// nodeStrategy 定义 Node 主资源的 REST 写入策略，负责字段重置、Feature Gate 兼容、校验和弃用告警。
type nodeStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是 Node 主资源的默认策略。Node 是集群级资源；普通更新不能借助主资源接口修改 Status。
// Nodes is the default logic that applies when creating and updating Node
// objects.
var Strategy = nodeStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped is false for nodes.
// Node 不属于任何 Namespace，存储 key 和 API URL 都是集群级作用域。
func (nodeStrategy) NamespaceScoped() bool {
	return false
}

// GetResetFields returns the set of fields that get reset by the strategy
// and should not be modified by the user.
// 普通 Node 写入请求不允许修改 Status；状态回写必须经过 nodes/status 子资源。
func (nodeStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("status"),
		),
	}

	return fields
}

// AllowCreateOnUpdate is false for nodes.
func (nodeStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// PrepareForCreate clears fields that are not allowed to be set by end users on creation.
// 创建前先按 Feature Gate 清理不可用字段；创建时的 ConfigSource/Status.Config 还有额外兼容限制。
func (nodeStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	node := obj.(*api.Node)
	dropDisabledFields(node, nil)
}

// PrepareForUpdate clears fields that are not allowed to be set by end users on update.
// 普通更新从旧对象恢复 Status，避免调用方通过 nodes 主资源接口伪造节点状态。
func (nodeStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newNode := obj.(*api.Node)
	oldNode := old.(*api.Node)
	newNode.Status = oldNode.Status

	dropDisabledFields(newNode, oldNode)
}

// dropDisabledFields 对创建和更新采用不同规则：创建时清理已移除的动态配置字段，更新时保留仍在使用的旧字段。
func dropDisabledFields(node *api.Node, oldNode *api.Node) {
	// Node 创建阶段历史上允许请求携带包括 Status 在内的字段，因此不在这里统一清空 Status；动态配置字段另按兼容规则处理。
	// for create
	if oldNode == nil {
		node.Spec.ConfigSource = nil
		node.Status.Config = nil
	}

	// for update
	// 旧 Node 没有使用 ConfigSource 时，不允许更新请求借此重新启用已废弃的动态配置来源。
	if !nodeConfigSourceInUse(oldNode) && oldNode != nil {
		node.Spec.ConfigSource = nil
	}

	// 某些字段由多个能力共同使用；只有相关 Feature Gate 都关闭时才清理 RuntimeHandlers。
	if !utilfeature.DefaultFeatureGate.Enabled(features.RecursiveReadOnlyMounts) && !utilfeature.DefaultFeatureGate.Enabled(features.UserNamespacesSupport) {
		node.Status.RuntimeHandlers = nil
	}

	// 已存在并正在使用的旧字段不能因 Feature Gate 关闭而被升级过程意外清空。
	if !utilfeature.DefaultFeatureGate.Enabled(features.SupplementalGroupsPolicy) && !supplementalGroupsPolicyInUse(oldNode) {
		node.Status.Features = nil
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.NodeDeclaredFeatures) && !nodeDeclaredFeaturesInUse(oldNode) {
		node.Status.DeclaredFeatures = nil
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingSchedulerPreemption) && !nodePodPreemptionPolicyInUse(oldNode) {
		node.Spec.PodPreemptionPolicy = nil
	}
}

// nodeConfigSourceInUse returns true if node's Spec ConfigSource is set(used)
// nodeConfigSourceInUse 判断旧 Node 是否已经使用 Spec.ConfigSource，以决定更新时能否移除该字段。
func nodeConfigSourceInUse(node *api.Node) bool {
	if node == nil {
		return false
	}
	if node.Spec.ConfigSource != nil {
		return true
	}
	return false
}

// Validate validates a new node.
// Validate 执行 Node 创建/主资源校验；NodeSpec 与 NodeStatus 的字段约束集中在 core validation 包。
func (nodeStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	node := obj.(*api.Node)
	return validation.ValidateNode(node)
}

// DeclarativeValidationConfig declares the options referenced by this type's tags,
// mapped to whether each is enabled.
// 将影响声明式校验的 Feature Gate 状态传递给 REST 校验框架，使标签校验规则与当前功能开关一致。
func (nodeStrategy) DeclarativeValidationConfig(ctx context.Context, obj, oldObj runtime.Object) rest.DeclarativeValidationConfig {
	return rest.DeclarativeValidationConfig{Options: map[string]bool{
		string(features.InPlacePodVerticalScalingSchedulerPreemption): utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingSchedulerPreemption),
	}}
}

// WarningsOnCreate returns warnings for the creation of the given object.
func (nodeStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	return nodeWarnings(obj)
}

// Canonicalize normalizes the object after validation.
func (nodeStrategy) Canonicalize(obj runtime.Object) {
}

// ValidateUpdate is the default update validation for an end user.
// 更新时同时执行完整 Node 校验和新旧对象之间的不可变字段/状态转移校验。
func (nodeStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	errorList := validation.ValidateNode(obj.(*api.Node))
	return append(errorList, validation.ValidateNodeUpdate(obj.(*api.Node), old.(*api.Node))...)
}

// WarningsOnUpdate returns warnings for the given update.
func (nodeStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nodeWarnings(obj)
}

func (nodeStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

type nodeStatusStrategy struct {
	nodeStrategy
}

var StatusStrategy = nodeStatusStrategy{Strategy}

// GetResetFields returns the set of fields that get reset by the strategy
// and should not be modified by the user.
func (nodeStatusStrategy) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	fields := map[fieldpath.APIVersion]*fieldpath.Set{
		"v1": fieldpath.NewSet(
			fieldpath.MakePathOrDie("spec"),
		),
	}

	return fields
}

// nodes/status 只允许修改 Status：先恢复整个 Spec，再按旧对象是否使用过字段决定是否保留兼容配置。
func (nodeStatusStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newNode := obj.(*api.Node)
	oldNode := old.(*api.Node)
	// kubelet 不能通过 /status 回写 Spec，调度策略和节点配置必须走主资源接口。
	newNode.Spec = oldNode.Spec

	if !nodeStatusConfigInUse(oldNode) {
		newNode.Status.Config = nil
	}

	if !utilfeature.DefaultFeatureGate.Enabled(features.KubeletInUserNamespace) && oldNode.Status.NodeInfo.RunningInUserNamespace == nil {
		newNode.Status.NodeInfo.RunningInUserNamespace = nil
	}
}

// nodeStatusConfigInUse returns true if node's Status Config is set(used)
// nodeStatusConfigInUse 判断旧 Node 是否仍使用 Status.Config，避免字段在兼容期被意外清空。
func nodeStatusConfigInUse(node *api.Node) bool {
	if node == nil {
		return false
	}
	if node.Status.Config != nil {
		return true
	}
	return false
}

// Status 子资源仅执行 Node 状态更新校验，不重复校验未开放修改的 Spec。
func (nodeStatusStrategy) ValidateUpdate(ctx context.Context, obj, old runtime.Object) field.ErrorList {
	return validation.ValidateNodeUpdate(obj.(*api.Node), old.(*api.Node))
}

// WarningsOnUpdate returns warnings for the given update.
func (nodeStatusStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	return nil
}

// Canonicalize normalizes the object after validation.
func (nodeStatusStrategy) Canonicalize(obj runtime.Object) {
}

// ResourceGetter is an interface for retrieving resources by ResourceLocation.
type ResourceGetter interface {
	Get(context.Context, string, *metav1.GetOptions) (runtime.Object, error)
}

// NodeToSelectableFields returns a field set that represents the object.
// NodeToSelectableFields 生成 API Server 支持的 Node 字段选择器，当前除 ObjectMeta 字段外还暴露 spec.unschedulable。
func NodeToSelectableFields(node *api.Node) fields.Set {
	objectMetaFieldsSet := generic.ObjectMetaFieldsSet(&node.ObjectMeta, false)
	specificFieldsSet := fields.Set{
		"spec.unschedulable": fmt.Sprint(node.Spec.Unschedulable),
	}
	return generic.MergeFieldsSets(objectMetaFieldsSet, specificFieldsSet)
}

// GetAttrs returns labels and fields of a given object for filtering purposes.
// GetAttrs 提取标签和可选字段，供 LIST/WATCH 的 LabelSelector 与 FieldSelector 过滤使用。
func GetAttrs(obj runtime.Object) (labels.Set, fields.Set, error) {
	nodeObj, ok := obj.(*api.Node)
	if !ok {
		return nil, nil, fmt.Errorf("not a node")
	}
	return labels.Set(nodeObj.ObjectMeta.Labels), NodeToSelectableFields(nodeObj), nil
}

// MatchNode returns a generic matcher for a given label and field selector.
// MatchNode 将用户选择器与属性提取函数组合成通用存储层过滤谓词。
func MatchNode(label labels.Selector, field fields.Selector) pkgstorage.SelectionPredicate {
	return pkgstorage.SelectionPredicate{
		Label:    label,
		Field:    field,
		GetAttrs: GetAttrs,
	}
}

// ResourceLocation returns a URL and transport which one can use to send traffic for the specified node.
// ResourceLocation 将节点名/可选 scheme 和端口转换为 Kubelet 代理地址，并选择对应 Transport。
func ResourceLocation(getter ResourceGetter, connection client.ConnectionInfoGetter, proxyTransport http.RoundTripper, ctx context.Context, id string) (*url.URL, http.RoundTripper, error) {
	schemeReq, name, portReq, valid := utilnet.SplitSchemeNamePort(id)
	if !valid {
		return nil, nil, errors.NewBadRequest(fmt.Sprintf("invalid node request %q", id))
	}

	// ConnectionInfoGetter 根据 Node 对象的地址、DaemonEndpoints 和 Kubelet 客户端配置计算真实连接参数。
	info, err := connection.GetConnectionInfo(ctx, types.NodeName(name))
	if err != nil {
		return nil, nil, err
	}

	if err := isProxyableHostname(ctx, info.Hostname); err != nil {
		return nil, nil, errors.NewBadRequest(err.Error())
	}

	// 使用 Kubelet 专用 Transport 可复用其 TLS/认证配置；请求显式指定其他端口时则改用通用代理 Transport。
	// - no port is specified in request (Kubelet's port is default)
	// - the requested port matches the kubelet port for this node
	if portReq == "" || portReq == info.Port {
		return &url.URL{
				Scheme: info.Scheme,
				Host:   net.JoinHostPort(info.Hostname, info.Port),
			},
			info.Transport,
			nil
	}

	// Otherwise, return the requested scheme and port, and the proxy transport
	return &url.URL{Scheme: schemeReq, Host: net.JoinHostPort(info.Hostname, portReq)}, proxyTransport, nil
}

// isProxyableHostname 解析目标主机的全部 DNS 地址；只要有非全局单播地址就拒绝代理，降低 SSRF 风险。
func isProxyableHostname(ctx context.Context, hostname string) error {
	resp, err := net.DefaultResolver.LookupIPAddr(ctx, hostname)
	if err != nil {
		return err
	}

	if len(resp) == 0 {
		return fmt.Errorf("no addresses for hostname")
	}
	for _, host := range resp {
		if !host.IP.IsGlobalUnicast() {
			return fmt.Errorf("address not allowed")
		}
	}

	return nil
}

// nodeWarnings 生成已移除/弃用字段与 PodCIDR 格式相关的非阻断告警，不替代正式字段校验。
func nodeWarnings(obj runtime.Object) []string {
	newNode := obj.(*api.Node)
	var warnings []string
	if newNode.Spec.ConfigSource != nil {
		// KEP https://github.com/kubernetes/enhancements/issues/281
		warnings = append(warnings, "spec.configSource: the feature is removed")
	}
	if len(newNode.Spec.DoNotUseExternalID) > 0 {
		warnings = append(warnings, "spec.externalID: this field is deprecated, and is unused by Kubernetes")
	}

	if len(newNode.Spec.PodCIDRs) > 0 {
		podCIDRsField := field.NewPath("spec", "podCIDRs")
		for i, value := range newNode.Spec.PodCIDRs {
			warnings = append(warnings, utilvalidation.GetWarningsForCIDR(podCIDRsField.Index(i), value)...)
		}
	}

	return warnings
}

// supplementalGroupsPolicyInUse returns true if the node.status has NodeFeature
// 下面三个 InUse helper 用于 Feature Gate 关闭时判断旧对象是否已在使用字段，从而决定是否兼容保留。
func supplementalGroupsPolicyInUse(node *api.Node) bool {
	if node == nil {
		return false
	}
	return node.Status.Features != nil
}

// nodeDeclaredFeaturesInUse returns true if the node.status has DeclaredFeatures
func nodeDeclaredFeaturesInUse(node *api.Node) bool {
	return node != nil && node.Status.DeclaredFeatures != nil
}

// nodePodPreemptionPolicyInUse returns true if the node.spec has PodPreemptionPolicy
func nodePodPreemptionPolicyInUse(node *api.Node) bool {
	return node != nil && node.Spec.PodPreemptionPolicy != nil
}
