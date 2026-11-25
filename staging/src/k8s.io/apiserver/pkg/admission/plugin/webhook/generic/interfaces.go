/*
Copyright 2018 The Kubernetes Authors.

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

package generic

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/plugin/webhook"
)

// VersionedAttributeAccessor 为某个 GVK 提供版本化的准入属性视图。
// webhook 可以请求与原始请求不同版本的对象，因此需要按 GVK 单独转换。
type VersionedAttributeAccessor interface {
	VersionedAttribute(gvk schema.GroupVersionKind) (*admission.VersionedAttributes, error)
}

// Source 抽象动态 webhook 配置的来源（例如来自 API 的配置对象）。
// HasSynced 用于判断配置是否已完成初始同步，未同步前调用方应视为无 hook。
type Source interface {
	Webhooks() []webhook.WebhookAccessor
	HasSynced() bool
}

// WebhookInvocation 描述如何调用 webhook，包括其注册的 resource 与 subresource，
// 以及应发送给它的 kind。
//
// 一个 webhook 的匹配规则以 Resource/Subresource 为准，但实际发送的对象
// 可能是 Kind 指定的版本；两者分离是为了支持 webhook 要求旧版本的场景。
type WebhookInvocation struct {
	Webhook     webhook.WebhookAccessor
	Resource    schema.GroupVersionResource
	Subresource string
	Kind        schema.GroupVersionKind
}

// Dispatcher 以准入属性为参数，把 webhook 调用分发给一组 webhook。
type Dispatcher interface {
	// 将请求分发给 webhooks。Dispatcher 可能选择不调用某个 hook，
	// 例如其 rules、namespaceSelector 或 objectSelector 不匹配。
	// 返回非 nil 错误意味着请求被拒绝。
	Dispatch(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces, hooks []webhook.WebhookAccessor) error
}
