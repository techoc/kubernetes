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

package internalversion

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ListOptions 是内部版本（internal）的列表/监听选项。它与 metav1.ListOptions 的
// 区别在于选择器已经解析成 labels.Selector / fields.Selector，从而让内部组件
// 不必重复解析字符串；字符串形式仅在 API 边界使用。
type ListOptions struct {
	metav1.TypeMeta

	// 已解析的标签选择器；为 nil 时表示"全部匹配"。
	LabelSelector labels.Selector
	// 已解析的字段选择器；为 nil 时表示"全部匹配"。
	FieldSelector fields.Selector
	// 为 true 时监听此列表的变化。
	Watch bool
	//
	// BOOKMARK 事件携带最新 resourceVersion，用于客户端在 watch 断开后从已知点恢复，
	// 而不必重新 list 全量数据。服务端可自行决定发送时机。
	AllowWatchBookmarks bool
	// resourceVersion 限制请求可使用的资源版本。
	// 参见 https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions。
	ResourceVersion string
	// resourceVersionMatch 决定 resourceVersion 如何应用于列表请求。
	// 设置 resourceVersion 时，强烈建议同时指定该匹配方式。
	// 参见 https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions。
	ResourceVersionMatch metav1.ResourceVersionMatch

	// 由 API 服务器在请求处理层转换为超时；watch 请求会据此限定时长。
	TimeoutSeconds *int64
	// Limit 指定服务端返回结果的最大数量。并非所有资源类型都支持该字段；
	// 若支持且仍有结果，服务端会在返回的列表对象中设置 continue 字段。
	Limit int64
	// Continue 是服务端返回的令牌，客户端配合 limit 可分批获取结果。
	// 服务端可拒绝无法识别的续传令牌；令牌过期后会返回 410 错误。
	Continue string

	//
	//
	//
	// 该字段由服务端在转换 ListOptions 时根据 resourceVersion 补默认值，因此内部版本中
	// 可能已经是非 nil。
	SendInitialEvents *bool

	//
	// 延迟解析的原因：apimachinery 不依赖 CEL，存储层才有解析器。这样可保持依赖方向
	// 单一，同时让分片选择器在存储层与其它过滤条件一起生效。
	ShardSelector string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// List 是内部版本的列表类型。Items 使用 runtime.Object 而非具体类型，因为内部
// 版本通常不知道具体资源类型，由调用方按需断言。
type List struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []runtime.Object
}
