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

// Package rules 判断一次准入请求是否命中 webhook 配置中的 RuleWithOperations。
// 匹配维度包括 scope、operation、apiGroup、apiVersion 和 resource/subresource，
// 语义与 ValidatingWebhookConfiguration/MutatingWebhookConfiguration 文档一致。
package rules

import (
	"strings"

	"k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
)

// Matcher 判断 Attr 是否命中 Rule。
//
// 所有字段都是只读的；Matches 不会有副作用，可在同一 Matcher 上重复调用。
type Matcher struct {
	Rule v1.RuleWithOperations
	Attr admission.Attributes
}

// Matches 返回 Attr 是否命中 Rule。
//
// 五个条件必须同时满足（逻辑与）。各子检查独立求值，短路顺序无业务含义。
func (r *Matcher) Matches() bool {
	return r.scope() &&
		r.operation() &&
		r.group() &&
		r.version() &&
		r.resource()
}

// exactOrWildcard 判断 requested 是否出现在 items 中，列表中包含 "*" 时视为匹配任意值。
func exactOrWildcard(items []string, requested string) bool {
	for _, item := range items {
		if item == "*" {
			return true
		}
		if item == requested {
			return true
		}
	}

	return false
}

// namespaceResource 用于识别“对 namespaces 对象本身”的请求。
// 这类请求的 namespace 字段被设置为目标 namespace 名，但对象本身是集群作用域的，
// 必须单独处理，否则会被误判为 namespaced 请求。
var namespaceResource = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"}

// scope 实现作用域匹配：Rule.Scope 为空或 AllScopes 时匹配一切。
func (r *Matcher) scope() bool {
	if r.Rule.Scope == nil || *r.Rule.Scope == v1.AllScopes {
		return true
	}
	// 对 namespace 对象本身的请求，attr.GetNamespace() 会被设置为该 namespace 的名字。
	switch *r.Rule.Scope {
	case v1.NamespacedScope:
		// 首先确认请求的不是 namespace 对象（namespace 对象是集群级作用域）
		return r.Attr.GetResource() != namespaceResource && r.Attr.GetNamespace() != metav1.NamespaceNone
	case v1.ClusterScope:
		// 若请求针对 namespace 对象也返回 true（namespace 对象是集群级作用域）
		return r.Attr.GetResource() == namespaceResource || r.Attr.GetNamespace() == metav1.NamespaceNone
	default:
		return false
	}
}

// group 匹配 API group，支持 "*" 通配。
func (r *Matcher) group() bool {
	return exactOrWildcard(r.Rule.APIGroups, r.Attr.GetResource().Group)
}

// version 匹配 API version，支持 "*" 通配。
func (r *Matcher) version() bool {
	return exactOrWildcard(r.Rule.APIVersions, r.Attr.GetResource().Version)
}

// operation 匹配操作类型；OperationAll（"*"）匹配所有操作。
func (r *Matcher) operation() bool {
	attrOp := r.Attr.GetOperation()
	for _, op := range r.Rule.Operations {
		if op == v1.OperationAll {
			return true
		}
		// 两组常量的取值相同，因此这是合法的强制类型转换
		// （并且有测试覆盖）。
		if op == v1.OperationType(attrOp) {
			return true
		}
	}
	return false
}

// splitResource 把 "resource/subresource" 形式的规则项拆成两部分；
// 没有 "/" 时 subresource 为空串。
func splitResource(resSub string) (res, sub string) {
	parts := strings.SplitN(resSub, "/", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return parts[0], ""
}

// resource 匹配 resource 及其 subresource。
// 规则项中的 "*" 与空字符串各有含义：资源部分的 "*" 匹配任意资源，
// subresource 部分的 "*" 匹配“任意 subresource（含无 subresource 的情况）”。
// 例如规则 "pods" 只匹配 pods 本身，而 "pods/*" 匹配 pods 的所有子资源。
func (r *Matcher) resource() bool {
	opRes, opSub := r.Attr.GetResource().Resource, r.Attr.GetSubresource()
	for _, res := range r.Rule.Resources {
		res, sub := splitResource(res)
		resMatch := res == "*" || res == opRes
		subMatch := sub == "*" || sub == opSub
		if resMatch && subMatch {
			return true
		}
	}
	return false
}

// IsExemptAdmissionConfigurationResource 判断某个 admission.Attributes 对象
// 描述的是否是对 ValidatingWebhookConfiguration、MutatingWebhookConfiguration、
// ValidatingAdmissionPolicy 或 ValidatingAdmissionPolicyBinding 的准入操作。
//
// 对 admissionregistration.k8s.io 配置对象自身的请求豁免准入 webhook 与策略，
// 否则自引用的策略可能导致递归或无法自举（bootstrap）——例如在集群还没有任何
// 可用 webhook 时就无法创建第一个 webhook 配置。
func IsExemptAdmissionConfigurationResource(attr admission.Attributes) bool {
	gvk := attr.GetKind()
	if gvk.Group == "admissionregistration.k8s.io" {
		if gvk.Kind == "ValidatingWebhookConfiguration" || gvk.Kind == "MutatingWebhookConfiguration" || gvk.Kind == "ValidatingAdmissionPolicy" || gvk.Kind == "ValidatingAdmissionPolicyBinding" || gvk.Kind == "MutatingAdmissionPolicy" || gvk.Kind == "MutatingAdmissionPolicyBinding" {
			return true
		}
	}
	return false
}
