/*
Copyright 2019 The Kubernetes Authors.

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

package endpointslice

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	discoveryv1beta1 "k8s.io/api/discovery/v1beta1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage/names"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	apivalidation "k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/apis/discovery"
	"k8s.io/kubernetes/pkg/apis/discovery/validation"
	endpointslicecontroller "k8s.io/kubernetes/pkg/controller/endpointslice"
	endpointslicemirroringcontroller "k8s.io/kubernetes/pkg/controller/endpointslicemirroring"
	"k8s.io/kubernetes/pkg/features"
)

// endpointSliceStrategy 定义 EndpointSlice 资源创建、更新时使用的 REST 策略。
type endpointSliceStrategy struct {
	rest.DeclarativeValidation
	names.NameGenerator
}

// Strategy 是创建、更新和删除 EndpointSlice 对象时使用的默认策略。
var Strategy = endpointSliceStrategy{rest.DeclarativeValidation{Scheme: legacyscheme.Scheme}, names.SimpleNameGenerator}

// NamespaceScoped 返回 true，表示 EndpointSlice 必须归属于某个命名空间。
func (endpointSliceStrategy) NamespaceScoped() bool {
	return true
}

// PrepareForCreate 在创建 EndpointSlice 前初始化 Generation，并清理当前特性门控不允许写入的字段。
func (endpointSliceStrategy) PrepareForCreate(ctx context.Context, obj runtime.Object) {
	endpointSlice := obj.(*discovery.EndpointSlice)
	endpointSlice.Generation = 1

	dropDisabledFieldsOnCreate(endpointSlice)
	dropTopologyOnV1(ctx, nil, endpointSlice)
}

// PrepareForUpdate 根据 Spec 和标签的变化维护 Generation，并保护特性门控及 API 版本兼容字段。
func (endpointSliceStrategy) PrepareForUpdate(ctx context.Context, obj, old runtime.Object) {
	newEPS := obj.(*discovery.EndpointSlice)
	oldEPS := old.(*discovery.EndpointSlice)

	// 若除 ObjectMeta 外的资源内容或 Labels 发生变化，则递增 Generation。
	// 如果 EndpointSlice 将来新增 Status 字段，此处的比较逻辑也需要相应调整。
	ogNewMeta := newEPS.ObjectMeta
	ogOldMeta := oldEPS.ObjectMeta
	newEPS.ObjectMeta = metav1.ObjectMeta{}
	oldEPS.ObjectMeta = metav1.ObjectMeta{}

	if !apiequality.Semantic.DeepEqual(newEPS, oldEPS) || !apiequality.Semantic.DeepEqual(ogNewMeta.Labels, ogOldMeta.Labels) {
		ogNewMeta.Generation = ogOldMeta.Generation + 1
	}

	newEPS.ObjectMeta = ogNewMeta
	oldEPS.ObjectMeta = ogOldMeta

	dropDisabledFieldsOnUpdate(oldEPS, newEPS)
	dropTopologyOnV1(ctx, oldEPS, newEPS)
}

// Validate 校验新 EndpointSlice 的地址类型、端点数据及其他字段约束。
func (endpointSliceStrategy) Validate(ctx context.Context, obj runtime.Object) field.ErrorList {
	endpointSlice := obj.(*discovery.EndpointSlice)
	return validation.ValidateEndpointSliceCreate(endpointSlice)
}

// WarningsOnCreate 返回创建 EndpointSlice 时的非阻断性警告，包括弃用地址类型和格式异常的 IP。
func (endpointSliceStrategy) WarningsOnCreate(ctx context.Context, obj runtime.Object) []string {
	eps := obj.(*discovery.EndpointSlice)
	if eps == nil {
		return nil
	}
	var warnings []string
	warnings = append(warnings, warnOnDeprecatedAddressType(eps.AddressType)...)
	warnings = append(warnings, warnOnBadIPs(eps)...)
	return warnings
}

// Canonicalize 在校验后规范化 EndpointSlice；当前无需额外处理。
func (endpointSliceStrategy) Canonicalize(obj runtime.Object) {
}

// AllowCreateOnUpdate 返回 false，EndpointSlice 必须通过 POST 创建，不能通过 PUT 更新请求隐式创建。
func (endpointSliceStrategy) AllowCreateOnUpdate(ctx context.Context) bool {
	return false
}

// ValidateUpdate 对照旧对象校验 EndpointSlice 更新是否合法。
func (endpointSliceStrategy) ValidateUpdate(ctx context.Context, new, old runtime.Object) field.ErrorList {
	newEPS := new.(*discovery.EndpointSlice)
	oldEPS := old.(*discovery.EndpointSlice)
	return validation.ValidateEndpointSliceUpdate(newEPS, oldEPS)
}

// WarningsOnUpdate 返回更新 EndpointSlice 时的非阻断性警告，主要检查端点 IP 格式。
func (endpointSliceStrategy) WarningsOnUpdate(ctx context.Context, obj, old runtime.Object) []string {
	eps := obj.(*discovery.EndpointSlice)
	if eps == nil {
		return nil
	}
	var warnings []string
	warnings = append(warnings, warnOnBadIPs(eps)...)
	return warnings
}

// AllowUnconditionalUpdate 返回 true，允许更新请求不提供 ResourceVersion，由通用更新流程处理版本信息。
func (endpointSliceStrategy) AllowUnconditionalUpdate(ctx context.Context) bool {
	return true
}

// dropDisabledFieldsOnCreate 在创建时清除特性门控未启用的端点字段。
func dropDisabledFieldsOnCreate(endpointSlice *discovery.EndpointSlice) {
	if utilfeature.DefaultFeatureGate.Enabled(features.PreferSameTrafficDistribution) {
		return
	}

	for i := range endpointSlice.Endpoints {
		if endpointSlice.Endpoints[i].Hints != nil {
			endpointSlice.Endpoints[i].Hints.ForNodes = nil
		}
	}
}

// dropDisabledFieldsOnUpdate 在特性门控关闭时，清除旧对象中尚未使用的新字段，
// 但如果旧对象已经设置该字段，则保留它以兼容存量数据。
func dropDisabledFieldsOnUpdate(oldEPS, newEPS *discovery.EndpointSlice) {
	if utilfeature.DefaultFeatureGate.Enabled(features.PreferSameTrafficDistribution) {
		return
	}

	for _, ep := range oldEPS.Endpoints {
		if ep.Hints != nil && ep.Hints.ForNodes != nil {
			return
		}
	}

	for i := range newEPS.Endpoints {
		if newEPS.Endpoints[i].Hints != nil {
			newEPS.Endpoints[i].Hints.ForNodes = nil
		}
	}
}

// dropTopologyOnV1 处理 discovery.k8s.io/v1 请求：将旧 DeprecatedTopology 中合法且已存在的节点名迁移到 NodeName，
// 随后清除 DeprecatedTopology，避免通过 v1 接口继续写入已弃用字段。
func dropTopologyOnV1(ctx context.Context, oldEPS, newEPS *discovery.EndpointSlice) {
	if info, ok := genericapirequest.RequestInfoFrom(ctx); ok {
		requestGV := schema.GroupVersion{Group: info.APIGroup, Version: info.APIVersion}
		if requestGV == discoveryv1beta1.SchemeGroupVersion {
			return
		}

		// 端点列表未发生变化时保留旧拓扑数据，避免仅更新其他字段就丢失兼容信息。
		if oldEPS != nil && apiequality.Semantic.DeepEqual(oldEPS.Endpoints, newEPS.Endpoints) {
			return
		}

		// 新对象只能沿用旧 EndpointSlice 的 DeprecatedTopology 中已经存在的节点名，
		// 不允许借助 v1 更新请求引入新的旧版拓扑节点名。
		prevNodeNames := getDeprecatedTopologyNodeNames(oldEPS)

		for i := range newEPS.Endpoints {
			ep := &newEPS.Endpoints[i]

			newTopologyNodeName, ok := ep.DeprecatedTopology[corev1.LabelHostname]
			if ep.NodeName == nil && ok && prevNodeNames.Has(newTopologyNodeName) && len(apivalidation.ValidateNodeName(newTopologyNodeName, false)) == 0 {
				// 将旧版用于保存节点名的 hostname 拓扑标签迁移到 NodeName，
				// 使部分更新也能保留原有节点归属信息。
				ep.NodeName = &newTopologyNodeName
			}
			// 按 API 约定，清除通过 v1 请求写入的 DeprecatedTopology。
			ep.DeprecatedTopology = nil
		}
	}
}

// getDeprecatedTopologyNodeNames 收集指定 EndpointSlice 的 DeprecatedTopology 中出现的非空节点名，并以集合形式返回。
func getDeprecatedTopologyNodeNames(eps *discovery.EndpointSlice) sets.String {
	if eps == nil {
		return nil
	}
	var names sets.String
	for _, ep := range eps.Endpoints {
		if nodeName, ok := ep.DeprecatedTopology[corev1.LabelHostname]; ok && len(nodeName) > 0 {
			if names == nil {
				names = sets.NewString()
			}
			names.Insert(nodeName)
		}
	}
	return names
}

// warnOnDeprecatedAddressType 当 EndpointSlice 使用已弃用的 FQDN 地址类型时返回警告。
func warnOnDeprecatedAddressType(addressType discovery.AddressType) []string {
	if addressType == discovery.AddressTypeFQDN {
		return []string{fmt.Sprintf("%s: FQDN endpoints are deprecated", field.NewPath("spec").Child("addressType"))}
	}
	return nil
}

// warnOnBadIPs 检查所有端点地址，并返回 IP 格式异常的非阻断性警告。
func warnOnBadIPs(eps *discovery.EndpointSlice) []string {
	// 如果请求来自 EndpointSlice 或 EndpointSlice 镜像控制器，则跳过 IP 格式检查；
	// 这些控制器在生成 EndpointSlice 时已经会修正输入数据中的无效 IP，跳过检查可以减少重复开销。
	if eps.Labels[discoveryv1.LabelManagedBy] == endpointslicecontroller.ControllerName ||
		eps.Labels[discoveryv1.LabelManagedBy] == endpointslicemirroringcontroller.ControllerName {
		return nil
	}

	var warnings []string
	for i := range eps.Endpoints {
		for j, addr := range eps.Endpoints[i].Addresses {
			fldPath := field.NewPath("endpoints").Index(i).Child("addresses").Index(j)
			warnings = append(warnings, utilvalidation.GetWarningsForIP(fldPath, addr)...)
		}
	}
	return warnings
}
