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

// ListOptions is the query options to a standard REST list call.
//
// ListOptions 是内部版本（internal）的列表/监听选项。它与 metav1.ListOptions 的
// 区别在于选择器已经解析成 labels.Selector / fields.Selector，从而让内部组件
// 不必重复解析字符串；字符串形式仅在 API 边界使用。
type ListOptions struct {
	metav1.TypeMeta

	// A selector based on labels
	// 已解析的标签选择器；为 nil 时表示"全部匹配"。
	LabelSelector labels.Selector
	// A selector based on fields
	// 已解析的字段选择器；为 nil 时表示"全部匹配"。
	FieldSelector fields.Selector
	// If true, watch for changes to this list
	Watch bool
	// allowWatchBookmarks requests watch events with type "BOOKMARK".
	// Servers that do not implement bookmarks may ignore this flag and
	// bookmarks are sent at the server's discretion. Clients should not
	// assume bookmarks are returned at any specific interval, nor may they
	// assume the server will send any BOOKMARK event during a session.
	// If this is not a watch, this field is ignored.
	//
	// BOOKMARK 事件携带最新 resourceVersion，用于客户端在 watch 断开后从已知点恢复，
	// 而不必重新 list 全量数据。服务端可自行决定发送时机。
	AllowWatchBookmarks bool
	// resourceVersion sets a constraint on what resource versions a request may be served from.
	// See https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions for
	// details.
	ResourceVersion string
	// resourceVersionMatch determines how resourceVersion is applied to list calls.
	// It is highly recommended that resourceVersionMatch be set for list calls where
	// resourceVersion is set.
	// See https://kubernetes.io/docs/reference/using-api/api-concepts/#resource-versions for
	// details.
	ResourceVersionMatch metav1.ResourceVersionMatch

	// Timeout for the list/watch call.
	// 由 API 服务器在请求处理层转换为超时；watch 请求会据此限定时长。
	TimeoutSeconds *int64
	// Limit specifies the maximum number of results to return from the server. The server may
	// not support this field on all resource types, but if it does and more results remain it
	// will set the continue field on the returned list object.
	Limit int64
	// Continue is a token returned by the server that lets a client retrieve chunks of results
	// from the server by specifying limit. The server may reject requests for continuation tokens
	// it does not recognize and will return a 410 error if the token can no longer be used because
	// it has expired.
	Continue string

	// `sendInitialEvents=true` may be set together with `watch=true`.
	// In that case, the watch stream will begin with synthetic events to
	// produce the current state of objects in the collection. Once all such
	// events have been sent, a synthetic "Bookmark" event  will be sent.
	// The bookmark will report the ResourceVersion (RV) corresponding to the
	// set of objects, and be marked with `"k8s.io/initial-events-end": "true"` annotation.
	// Afterwards, the watch stream will proceed as usual, sending watch events
	// corresponding to changes (subsequent to the RV) to objects watched.
	//
	// When `sendInitialEvents` option is set, we require `resourceVersionMatch`
	// option to also be set. The semantic of the watch request is as following:
	// - `resourceVersionMatch` = NotOlderThan
	//   is interpreted as "data at least as new as the provided `resourceVersion`"
	//   and the bookmark event is send when the state is synced
	//   to a `resourceVersion` at least as fresh as the one provided by the ListOptions.
	//   If `resourceVersion` is unset, this is interpreted as "consistent read" and the
	//   bookmark event is send when the state is synced at least to the moment
	//   when request started being processed.
	// - `resourceVersionMatch` set to any other value or unset
	//   Invalid error is returned.
	//
	// Defaults to true if `resourceVersion=""` or `resourceVersion="0"` (for backward
	// compatibility reasons) and to false otherwise.
	//
	// 该字段由服务端在转换 ListOptions 时根据 resourceVersion 补默认值，因此内部版本中
	// 可能已经是非 nil。
	SendInitialEvents *bool

	// ShardSelector is the raw shard selector string from the request.
	// Parsing is deferred to the apiserver storage layer where the CEL
	// parser dependency is available.
	//
	// 延迟解析的原因：apimachinery 不依赖 CEL，存储层才有解析器。这样可保持依赖方向
	// 单一，同时让分片选择器在存储层与其它过滤条件一起生效。
	ShardSelector string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// List holds a list of objects, which may not be known by the server.
//
// List 是内部版本的列表类型。Items 使用 runtime.Object 而非具体类型，因为内部
// 版本通常不知道具体资源类型，由调用方按需断言。
type List struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []runtime.Object
}
