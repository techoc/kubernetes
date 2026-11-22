/*
Copyright 2015 The Kubernetes Authors.

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

// Package testing 提供 fake client 使用的 Action 抽象及构造函数。
//
// Action 描述“对哪个资源执行了什么操作”。fake client 在每次调用时构造
// Action 并交给 Reactor，测试据此决定返回值或断言客户端行为。构造函数
// 分为带命名空间与 NewRoot...（集群级）两套，二者必须保持一致，因为代码
// 生成器依赖该约定。

package testing

import (
	"fmt"
	"path"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// All NewRoot... functions return non-namespaced actions, and are equivalent to
// calling the corresponding New... function with an empty namespace.
// This is assumed by the fake client generator.
//
// 所有 NewRoot... 构造函数返回集群级（非命名空间）动作，等价于以空
// namespace 调用对应的 New... 构造函数；fake client 生成器依赖这一约定。

// NewRootGetAction 构造集群级 get 动作，使用空的 GetOptions。
func NewRootGetAction(resource schema.GroupVersionResource, name string) GetActionImpl {
	return NewRootGetActionWithOptions(resource, name, metav1.GetOptions{})
}

// NewRootGetActionWithOptions 构造集群级 get 动作并携带 GetOptions。
func NewRootGetActionWithOptions(resource schema.GroupVersionResource, name string, opts metav1.GetOptions) GetActionImpl {
	action := GetActionImpl{}
	action.Verb = "get"
	action.Resource = resource
	action.Name = name
	action.GetOptions = opts

	return action
}

// NewGetAction 构造命名空间级 get 动作，使用空的 GetOptions。
func NewGetAction(resource schema.GroupVersionResource, namespace, name string) GetActionImpl {
	return NewGetActionWithOptions(resource, namespace, name, metav1.GetOptions{})
}

// NewGetActionWithOptions 构造命名空间级 get 动作并携带 GetOptions。
func NewGetActionWithOptions(resource schema.GroupVersionResource, namespace, name string, opts metav1.GetOptions) GetActionImpl {
	action := GetActionImpl{}
	action.Verb = "get"
	action.Resource = resource
	action.Namespace = namespace
	action.Name = name
	action.GetOptions = opts

	return action
}

// NewGetSubresourceAction 构造命名空间级子资源 get 动作。
func NewGetSubresourceAction(resource schema.GroupVersionResource, namespace, subresource, name string) GetActionImpl {
	return NewGetSubresourceActionWithOptions(resource, namespace, subresource, name, metav1.GetOptions{})
}

// NewGetSubresourceActionWithOptions 构造命名空间级子资源 get 动作并携带 GetOptions。
func NewGetSubresourceActionWithOptions(resource schema.GroupVersionResource, namespace, subresource, name string, opts metav1.GetOptions) GetActionImpl {
	action := GetActionImpl{}
	action.Verb = "get"
	action.Resource = resource
	action.Subresource = subresource
	action.Namespace = namespace
	action.Name = name
	action.GetOptions = opts

	return action
}

// NewRootGetSubresourceAction 构造集群级子资源 get 动作。
func NewRootGetSubresourceAction(resource schema.GroupVersionResource, subresource, name string) GetActionImpl {
	return NewRootGetSubresourceActionWithOptions(resource, subresource, name, metav1.GetOptions{})
}

// NewRootGetSubresourceActionWithOptions 构造集群级子资源 get 动作并携带 GetOptions。
func NewRootGetSubresourceActionWithOptions(resource schema.GroupVersionResource, subresource, name string, opts metav1.GetOptions) GetActionImpl {
	action := GetActionImpl{}
	action.Verb = "get"
	action.Resource = resource
	action.Subresource = subresource
	action.Name = name
	action.GetOptions = opts

	return action
}

// NewRootListAction 构造集群级 list 动作。
//
// opts 必须是 metav1.ListOptions；选择器会被解析为 ListRestrictions，
// 同时规范化后回填到 ListOptions 中。
func NewRootListAction(resource schema.GroupVersionResource, kind schema.GroupVersionKind, opts interface{}) ListActionImpl {
	action := ListActionImpl{}
	action.Verb = "list"
	action.Resource = resource
	action.Kind = kind
	labelSelector, fieldSelector, _ := ExtractFromListOptions(opts)
	action.ListRestrictions = ListRestrictions{labelSelector, fieldSelector}
	action.ListOptions = metav1.ListOptions{LabelSelector: labelSelector.String(), FieldSelector: fieldSelector.String()}

	return action
}

// NewRootListActionWithOptions 构造集群级 list 动作，直接使用给定的 ListOptions。
func NewRootListActionWithOptions(resource schema.GroupVersionResource, kind schema.GroupVersionKind, opts metav1.ListOptions) ListActionImpl {
	action := ListActionImpl{}
	action.Verb = "list"
	action.Resource = resource
	action.Kind = kind
	action.ListOptions = opts

	labelSelector, fieldSelector, _ := ExtractFromListOptions(opts)
	action.ListRestrictions = ListRestrictions{labelSelector, fieldSelector}
	action.ListOptions = metav1.ListOptions{LabelSelector: labelSelector.String(), FieldSelector: fieldSelector.String()}

	return action
}

// NewListAction 构造命名空间级 list 动作。
func NewListAction(resource schema.GroupVersionResource, kind schema.GroupVersionKind, namespace string, opts interface{}) ListActionImpl {
	action := ListActionImpl{}
	action.Verb = "list"
	action.Resource = resource
	action.Kind = kind
	action.Namespace = namespace
	labelSelector, fieldSelector, _ := ExtractFromListOptions(opts)
	action.ListRestrictions = ListRestrictions{labelSelector, fieldSelector}
	action.ListOptions = metav1.ListOptions{LabelSelector: labelSelector.String(), FieldSelector: fieldSelector.String()}

	return action
}

// NewListActionWithOptions 构造命名空间级 list 动作，直接使用给定的 ListOptions。
func NewListActionWithOptions(resource schema.GroupVersionResource, kind schema.GroupVersionKind, namespace string, opts metav1.ListOptions) ListActionImpl {
	action := ListActionImpl{}
	action.Verb = "list"
	action.Resource = resource
	action.Kind = kind
	action.Namespace = namespace
	action.ListOptions = opts

	labelSelector, fieldSelector, _ := ExtractFromListOptions(opts)
	action.ListRestrictions = ListRestrictions{labelSelector, fieldSelector}

	return action
}

// NewRootCreateAction 构造集群级 create 动作。
func NewRootCreateAction(resource schema.GroupVersionResource, object runtime.Object) CreateActionImpl {
	return NewRootCreateActionWithOptions(resource, object, metav1.CreateOptions{})
}

// NewRootCreateActionWithOptions 构造集群级 create 动作并携带 CreateOptions。
func NewRootCreateActionWithOptions(resource schema.GroupVersionResource, object runtime.Object, opts metav1.CreateOptions) CreateActionImpl {
	action := CreateActionImpl{}
	action.Verb = "create"
	action.Resource = resource
	action.Object = object
	action.CreateOptions = opts

	return action
}

// NewCreateAction 构造命名空间级 create 动作。
func NewCreateAction(resource schema.GroupVersionResource, namespace string, object runtime.Object) CreateActionImpl {
	return NewCreateActionWithOptions(resource, namespace, object, metav1.CreateOptions{})
}

// NewCreateActionWithOptions 构造命名空间级 create 动作并携带 CreateOptions。
func NewCreateActionWithOptions(resource schema.GroupVersionResource, namespace string, object runtime.Object, opts metav1.CreateOptions) CreateActionImpl {
	action := CreateActionImpl{}
	action.Verb = "create"
	action.Resource = resource
	action.Namespace = namespace
	action.Object = object
	action.CreateOptions = opts

	return action
}

// NewRootCreateSubresourceAction 构造集群级子资源 create 动作。
//
// 子资源 create 通常带有 name（例如 pods/eviction、pods/binding），
// 因此与普通 create 不同，这里显式设置 Name。
func NewRootCreateSubresourceAction(resource schema.GroupVersionResource, name, subresource string, object runtime.Object) CreateActionImpl {
	return NewRootCreateSubresourceActionWithOptions(resource, name, subresource, object, metav1.CreateOptions{})
}

// NewRootCreateSubresourceActionWithOptions 构造集群级子资源 create 动作并携带 CreateOptions。
func NewRootCreateSubresourceActionWithOptions(resource schema.GroupVersionResource, name, subresource string, object runtime.Object, opts metav1.CreateOptions) CreateActionImpl {
	action := CreateActionImpl{}
	action.Verb = "create"
	action.Resource = resource
	action.Subresource = subresource
	action.Name = name
	action.Object = object
	action.CreateOptions = opts

	return action
}

// NewCreateSubresourceAction 构造命名空间级子资源 create 动作。
func NewCreateSubresourceAction(resource schema.GroupVersionResource, name, subresource, namespace string, object runtime.Object) CreateActionImpl {
	return NewCreateSubresourceActionWithOptions(resource, name, subresource, namespace, object, metav1.CreateOptions{})
}

// NewCreateSubresourceActionWithOptions 构造命名空间级子资源 create 动作并携带 CreateOptions。
func NewCreateSubresourceActionWithOptions(resource schema.GroupVersionResource, name, subresource, namespace string, object runtime.Object, opts metav1.CreateOptions) CreateActionImpl {
	action := CreateActionImpl{}
	action.Verb = "create"
	action.Resource = resource
	action.Namespace = namespace
	action.Subresource = subresource
	action.Name = name
	action.Object = object
	action.CreateOptions = opts

	return action
}

// NewRootUpdateAction 构造集群级 update 动作。
func NewRootUpdateAction(resource schema.GroupVersionResource, object runtime.Object) UpdateActionImpl {
	return NewRootUpdateActionWithOptions(resource, object, metav1.UpdateOptions{})
}

// NewRootUpdateActionWithOptions 构造集群级 update 动作并携带 UpdateOptions。
func NewRootUpdateActionWithOptions(resource schema.GroupVersionResource, object runtime.Object, opts metav1.UpdateOptions) UpdateActionImpl {
	action := UpdateActionImpl{}
	action.Verb = "update"
	action.Resource = resource
	action.Object = object
	action.UpdateOptions = opts

	return action
}

// NewUpdateAction 构造命名空间级 update 动作。
func NewUpdateAction(resource schema.GroupVersionResource, namespace string, object runtime.Object) UpdateActionImpl {
	return NewUpdateActionWithOptions(resource, namespace, object, metav1.UpdateOptions{})
}

// NewUpdateActionWithOptions 构造命名空间级 update 动作并携带 UpdateOptions。
func NewUpdateActionWithOptions(resource schema.GroupVersionResource, namespace string, object runtime.Object, opts metav1.UpdateOptions) UpdateActionImpl {
	action := UpdateActionImpl{}
	action.Verb = "update"
	action.Resource = resource
	action.Namespace = namespace
	action.Object = object
	action.UpdateOptions = opts

	return action
}

// NewRootPatchAction 构造集群级 patch 动作。
func NewRootPatchAction(resource schema.GroupVersionResource, name string, pt types.PatchType, patch []byte) PatchActionImpl {
	return NewRootPatchActionWithOptions(resource, name, pt, patch, metav1.PatchOptions{})
}

// NewRootPatchActionWithOptions 构造集群级 patch 动作并携带 PatchOptions。
func NewRootPatchActionWithOptions(resource schema.GroupVersionResource, name string, pt types.PatchType, patch []byte, opts metav1.PatchOptions) PatchActionImpl {
	action := PatchActionImpl{}
	action.Verb = "patch"
	action.Resource = resource
	action.Name = name
	action.PatchType = pt
	action.Patch = patch
	action.PatchOptions = opts

	return action
}

// NewPatchAction 构造命名空间级 patch 动作。
func NewPatchAction(resource schema.GroupVersionResource, namespace string, name string, pt types.PatchType, patch []byte) PatchActionImpl {
	return NewPatchActionWithOptions(resource, namespace, name, pt, patch, metav1.PatchOptions{})
}

// NewPatchActionWithOptions 构造命名空间级 patch 动作并携带 PatchOptions。
func NewPatchActionWithOptions(resource schema.GroupVersionResource, namespace string, name string, pt types.PatchType, patch []byte, opts metav1.PatchOptions) PatchActionImpl {
	action := PatchActionImpl{}
	action.Verb = "patch"
	action.Resource = resource
	action.Namespace = namespace
	action.Name = name
	action.PatchType = pt
	action.Patch = patch
	action.PatchOptions = opts

	return action
}

// NewRootPatchSubresourceAction 构造集群级子资源 patch 动作。
//
// 多个子资源段会用 "/" 连接（例如 "status" 或更深的嵌套），与 REST 路径一致。
func NewRootPatchSubresourceAction(resource schema.GroupVersionResource, name string, pt types.PatchType, patch []byte, subresources ...string) PatchActionImpl {
	return NewRootPatchSubresourceActionWithOptions(resource, name, pt, patch, metav1.PatchOptions{}, subresources...)
}

// NewRootPatchSubresourceActionWithOptions 构造集群级子资源 patch 动作并携带 PatchOptions。
func NewRootPatchSubresourceActionWithOptions(resource schema.GroupVersionResource, name string, pt types.PatchType, patch []byte, opts metav1.PatchOptions, subresources ...string) PatchActionImpl {
	action := PatchActionImpl{}
	action.Verb = "patch"
	action.Resource = resource
	action.Subresource = path.Join(subresources...)
	action.Name = name
	action.PatchType = pt
	action.Patch = patch
	action.PatchOptions = opts

	return action
}

// NewPatchSubresourceAction 构造命名空间级子资源 patch 动作。
func NewPatchSubresourceAction(resource schema.GroupVersionResource, namespace, name string, pt types.PatchType, patch []byte, subresources ...string) PatchActionImpl {
	return NewPatchSubresourceActionWithOptions(resource, namespace, name, pt, patch, metav1.PatchOptions{}, subresources...)
}

// NewPatchSubresourceActionWithOptions 构造命名空间级子资源 patch 动作并携带 PatchOptions。
func NewPatchSubresourceActionWithOptions(resource schema.GroupVersionResource, namespace, name string, pt types.PatchType, patch []byte, opts metav1.PatchOptions, subresources ...string) PatchActionImpl {
	action := PatchActionImpl{}
	action.Verb = "patch"
	action.Resource = resource
	action.Subresource = path.Join(subresources...)
	action.Namespace = namespace
	action.Name = name
	action.PatchType = pt
	action.Patch = patch
	action.PatchOptions = opts

	return action
}

// NewRootUpdateSubresourceAction 构造集群级子资源 update 动作。
func NewRootUpdateSubresourceAction(resource schema.GroupVersionResource, subresource string, object runtime.Object) UpdateActionImpl {
	return NewRootUpdateSubresourceActionWithOptions(resource, subresource, object, metav1.UpdateOptions{})
}

// NewRootUpdateSubresourceActionWithOptions 构造集群级子资源 update 动作并携带 UpdateOptions。
func NewRootUpdateSubresourceActionWithOptions(resource schema.GroupVersionResource, subresource string, object runtime.Object, opts metav1.UpdateOptions) UpdateActionImpl {
	action := UpdateActionImpl{}
	action.Verb = "update"
	action.Resource = resource
	action.Subresource = subresource
	action.Object = object
	action.UpdateOptions = opts

	return action
}

// NewUpdateSubresourceAction 构造命名空间级子资源 update 动作。
func NewUpdateSubresourceAction(resource schema.GroupVersionResource, subresource string, namespace string, object runtime.Object) UpdateActionImpl {
	return NewUpdateSubresourceActionWithOptions(resource, subresource, namespace, object, metav1.UpdateOptions{})
}

// NewUpdateSubresourceActionWithOptions 构造命名空间级子资源 update 动作并携带 UpdateOptions。
func NewUpdateSubresourceActionWithOptions(resource schema.GroupVersionResource, subresource string, namespace string, object runtime.Object, opts metav1.UpdateOptions) UpdateActionImpl {
	action := UpdateActionImpl{}
	action.Verb = "update"
	action.Resource = resource
	action.Subresource = subresource
	action.Namespace = namespace
	action.Object = object
	action.UpdateOptions = opts

	return action
}

// NewRootDeleteAction 构造集群级 delete 动作。
func NewRootDeleteAction(resource schema.GroupVersionResource, name string) DeleteActionImpl {
	return NewRootDeleteActionWithOptions(resource, name, metav1.DeleteOptions{})
}

// NewRootDeleteActionWithOptions 构造集群级 delete 动作并携带 DeleteOptions。
func NewRootDeleteActionWithOptions(resource schema.GroupVersionResource, name string, opts metav1.DeleteOptions) DeleteActionImpl {
	action := DeleteActionImpl{}
	action.Verb = "delete"
	action.Resource = resource
	action.Name = name
	action.DeleteOptions = opts

	return action
}

// NewRootDeleteSubresourceAction 构造集群级子资源 delete 动作。
func NewRootDeleteSubresourceAction(resource schema.GroupVersionResource, subresource string, name string) DeleteActionImpl {
	return NewRootDeleteSubresourceActionWithOptions(resource, subresource, name, metav1.DeleteOptions{})
}

// NewRootDeleteSubresourceActionWithOptions 构造集群级子资源 delete 动作并携带 DeleteOptions。
func NewRootDeleteSubresourceActionWithOptions(resource schema.GroupVersionResource, subresource string, name string, opts metav1.DeleteOptions) DeleteActionImpl {
	action := DeleteActionImpl{}
	action.Verb = "delete"
	action.Resource = resource
	action.Subresource = subresource
	action.Name = name
	action.DeleteOptions = opts

	return action
}

// NewDeleteAction 构造命名空间级 delete 动作。
func NewDeleteAction(resource schema.GroupVersionResource, namespace, name string) DeleteActionImpl {
	return NewDeleteActionWithOptions(resource, namespace, name, metav1.DeleteOptions{})
}

// NewDeleteActionWithOptions 构造命名空间级 delete 动作并携带 DeleteOptions。
func NewDeleteActionWithOptions(resource schema.GroupVersionResource, namespace, name string, opts metav1.DeleteOptions) DeleteActionImpl {
	action := DeleteActionImpl{}
	action.Verb = "delete"
	action.Resource = resource
	action.Namespace = namespace
	action.Name = name
	action.DeleteOptions = opts

	return action
}

// NewDeleteSubresourceAction 构造命名空间级子资源 delete 动作。
func NewDeleteSubresourceAction(resource schema.GroupVersionResource, subresource, namespace, name string) DeleteActionImpl {
	return NewDeleteSubresourceActionWithOptions(resource, subresource, namespace, name, metav1.DeleteOptions{})
}

// NewDeleteSubresourceActionWithOptions 构造命名空间级子资源 delete 动作并携带 DeleteOptions。
func NewDeleteSubresourceActionWithOptions(resource schema.GroupVersionResource, subresource, namespace, name string, opts metav1.DeleteOptions) DeleteActionImpl {
	action := DeleteActionImpl{}
	action.Verb = "delete"
	action.Resource = resource
	action.Subresource = subresource
	action.Namespace = namespace
	action.Name = name
	action.DeleteOptions = opts

	return action
}

// NewRootDeleteCollectionAction 构造集群级 delete-collection 动作。
//
// opts 按 metav1.ListOptions 解析；类型不符时 ListOptions 取零值，
// 这与历史行为保持一致。
func NewRootDeleteCollectionAction(resource schema.GroupVersionResource, opts interface{}) DeleteCollectionActionImpl {
	listOpts, _ := opts.(metav1.ListOptions)
	return NewRootDeleteCollectionActionWithOptions(resource, metav1.DeleteOptions{}, listOpts)
}

// NewRootDeleteCollectionActionWithOptions 构造集群级 delete-collection 动作，
// 同时携带删除选项与列表选择条件。
func NewRootDeleteCollectionActionWithOptions(resource schema.GroupVersionResource, deleteOpts metav1.DeleteOptions, listOpts metav1.ListOptions) DeleteCollectionActionImpl {
	action := DeleteCollectionActionImpl{}
	action.Verb = "delete-collection"
	action.Resource = resource
	action.DeleteOptions = deleteOpts
	action.ListOptions = listOpts

	labelSelector, fieldSelector, _ := ExtractFromListOptions(listOpts)
	action.ListRestrictions = ListRestrictions{labelSelector, fieldSelector}

	return action
}

// NewDeleteCollectionAction 构造命名空间级 delete-collection 动作。
func NewDeleteCollectionAction(resource schema.GroupVersionResource, namespace string, opts interface{}) DeleteCollectionActionImpl {
	listOpts, _ := opts.(metav1.ListOptions)
	return NewDeleteCollectionActionWithOptions(resource, namespace, metav1.DeleteOptions{}, listOpts)
}

// NewDeleteCollectionActionWithOptions 构造命名空间级 delete-collection 动作，
// 同时携带删除选项与列表选择条件。
func NewDeleteCollectionActionWithOptions(resource schema.GroupVersionResource, namespace string, deleteOpts metav1.DeleteOptions, listOpts metav1.ListOptions) DeleteCollectionActionImpl {
	action := DeleteCollectionActionImpl{}
	action.Verb = "delete-collection"
	action.Resource = resource
	action.Namespace = namespace
	action.DeleteOptions = deleteOpts
	action.ListOptions = listOpts

	labelSelector, fieldSelector, _ := ExtractFromListOptions(listOpts)
	action.ListRestrictions = ListRestrictions{labelSelector, fieldSelector}

	return action
}

// NewRootWatchAction 构造集群级 watch 动作。
func NewRootWatchAction(resource schema.GroupVersionResource, opts interface{}) WatchActionImpl {
	listOpts, _ := opts.(metav1.ListOptions)
	return NewRootWatchActionWithOptions(resource, listOpts)
}

// NewRootWatchActionWithOptions 构造集群级 watch 动作并解析选择器与 resourceVersion。
func NewRootWatchActionWithOptions(resource schema.GroupVersionResource, opts metav1.ListOptions) WatchActionImpl {
	action := WatchActionImpl{}
	action.Verb = "watch"
	action.Resource = resource
	action.ListOptions = opts

	labelSelector, fieldSelector, resourceVersion := ExtractFromListOptions(opts)
	action.WatchRestrictions = WatchRestrictions{labelSelector, fieldSelector, resourceVersion}

	return action
}

// ExtractFromListOptions 把 ListOptions 中的字符串选择器解析为 Selector。
//
// 空选择器会被替换为“匹配一切”，使调用方无需额外判空。opts 不是
// metav1.ListOptions 或选择器非法时会 panic：这只应发生在测试代码的
// 构造错误，尽早暴露比默默忽略更好。
func ExtractFromListOptions(opts interface{}) (labelSelector labels.Selector, fieldSelector fields.Selector, resourceVersion string) {
	var err error
	switch t := opts.(type) {
	case metav1.ListOptions:
		labelSelector, err = labels.Parse(t.LabelSelector)
		if err != nil {
			panic(fmt.Errorf("invalid selector %q: %v", t.LabelSelector, err))
		}
		fieldSelector, err = fields.ParseSelector(t.FieldSelector)
		if err != nil {
			panic(fmt.Errorf("invalid selector %q: %v", t.FieldSelector, err))
		}
		resourceVersion = t.ResourceVersion
	default:
		panic(fmt.Errorf("expect a ListOptions %T", opts))
	}
	if labelSelector == nil {
		labelSelector = labels.Everything()
	}
	if fieldSelector == nil {
		fieldSelector = fields.Everything()
	}
	return labelSelector, fieldSelector, resourceVersion
}

// NewWatchAction 构造命名空间级 watch 动作。
func NewWatchAction(resource schema.GroupVersionResource, namespace string, opts interface{}) WatchActionImpl {
	listOpts, _ := opts.(metav1.ListOptions)
	return NewWatchActionWithOptions(resource, namespace, listOpts)
}

// NewWatchActionWithOptions 构造命名空间级 watch 动作并解析选择器与 resourceVersion。
func NewWatchActionWithOptions(resource schema.GroupVersionResource, namespace string, opts metav1.ListOptions) WatchActionImpl {
	action := WatchActionImpl{}
	action.Verb = "watch"
	action.Resource = resource
	action.Namespace = namespace
	action.ListOptions = opts

	labelSelector, fieldSelector, resourceVersion := ExtractFromListOptions(opts)
	action.WatchRestrictions = WatchRestrictions{labelSelector, fieldSelector, resourceVersion}

	return action
}

// NewProxyGetAction 构造通过 pod proxy 子资源发起 GET 的动作。
func NewProxyGetAction(resource schema.GroupVersionResource, namespace, scheme, name, port, path string, params map[string]string) ProxyGetActionImpl {
	action := ProxyGetActionImpl{}
	action.Verb = "get"
	action.Resource = resource
	action.Namespace = namespace
	action.Scheme = scheme
	action.Name = name
	action.Port = port
	action.Path = path
	action.Params = params
	return action
}

// ListRestrictions 保存 list 操作解析后的标签与字段选择器。
type ListRestrictions struct {
	Labels labels.Selector
	Fields fields.Selector
}

// WatchRestrictions 保存 watch 操作解析后的选择器与起始 resourceVersion。
type WatchRestrictions struct {
	Labels          labels.Selector
	Fields          fields.Selector
	ResourceVersion string
}

// Action 是所有 fake 动作的最小公共接口。
type Action interface {
	GetNamespace() string
	GetVerb() string
	GetResource() schema.GroupVersionResource
	GetSubresource() string
	Matches(verb, resource string) bool

	// DeepCopy is used to copy an action to avoid any risk of accidental mutation.  Most people never need to call this
	// because the invocation logic deep copies before calls to storage and reactors.
	DeepCopy() Action
}

// GenericAction 是携带任意 Value 的动作接口，供自定义 reactor 使用。
type GenericAction interface {
	Action
	GetValue() interface{}
}

// GetAction 是 get 动作接口。
type GetAction interface {
	Action
	GetName() string
}

// ListAction 是 list 动作接口。
type ListAction interface {
	Action
	GetListRestrictions() ListRestrictions
}

// CreateAction 是 create 动作接口。
type CreateAction interface {
	Action
	GetObject() runtime.Object
}

// UpdateAction 是 update 动作接口。
type UpdateAction interface {
	Action
	GetObject() runtime.Object
}

// DeleteAction 是单对象 delete 动作接口。
type DeleteAction interface {
	Action
	GetName() string
	GetDeleteOptions() metav1.DeleteOptions
}

// DeleteCollectionAction 是批量 delete 动作接口。
type DeleteCollectionAction interface {
	Action
	GetListRestrictions() ListRestrictions
}

// PatchAction 是 patch 动作接口。
type PatchAction interface {
	Action
	GetName() string
	GetPatchType() types.PatchType
	GetPatch() []byte
}

// WatchAction 是 watch 动作接口。
type WatchAction interface {
	Action
	GetWatchRestrictions() WatchRestrictions
}

// ProxyGetAction 是 proxy GET 动作接口。
type ProxyGetAction interface {
	Action
	GetScheme() string
	GetName() string
	GetPort() string
	GetPath() string
	GetParams() map[string]string
}

// ActionImpl 是所有动作实现共享的公共字段：命名空间、动词、资源与子资源。
type ActionImpl struct {
	Namespace   string
	Verb        string
	Resource    schema.GroupVersionResource
	Subresource string
}

// GetNamespace 返回动作所属命名空间。
func (a ActionImpl) GetNamespace() string {
	return a.Namespace
}

// GetVerb 返回动作动词（get/list/create/update/patch/delete/watch 等）。
func (a ActionImpl) GetVerb() string {
	return a.Verb
}

// GetResource 返回动作目标资源。
func (a ActionImpl) GetResource() schema.GroupVersionResource {
	return a.Resource
}

// GetSubresource 返回子资源名，没有则为空。
func (a ActionImpl) GetSubresource() string {
	return a.Subresource
}

// Matches 判断动作是否匹配给定的动词与资源。
//
// resource 可以写成 "resource" 或 "resource/subresource"。为兼容旧的
// 匹配方式，不含 "/" 时只比较 resource。比较不区分大小写。
func (a ActionImpl) Matches(verb, resource string) bool {
	// Stay backwards compatible.
	if !strings.Contains(resource, "/") {
		return strings.EqualFold(verb, a.Verb) &&
			strings.EqualFold(resource, a.Resource.Resource)
	}

	parts := strings.SplitN(resource, "/", 2)
	topresource, subresource := parts[0], parts[1]

	return strings.EqualFold(verb, a.Verb) &&
		strings.EqualFold(topresource, a.Resource.Resource) &&
		strings.EqualFold(subresource, a.Subresource)
}

// DeepCopy 返回动作的副本。ActionImpl 只含值类型字段，直接拷贝即可。
func (a ActionImpl) DeepCopy() Action {
	ret := a
	return ret
}

// GenericActionImpl 是 GenericAction 的默认实现。
type GenericActionImpl struct {
	ActionImpl
	Value interface{}
}

// GetValue 返回动作携带的任意值。
func (a GenericActionImpl) GetValue() interface{} {
	return a.Value
}

// DeepCopy 返回动作副本。
func (a GenericActionImpl) DeepCopy() Action {
	return GenericActionImpl{
		ActionImpl: a.ActionImpl.DeepCopy().(ActionImpl),
		// TODO this is wrong, but no worse than before
		// Value 未做深拷贝：其类型未知，保持历史行为。
		Value: a.Value,
	}
}

// GetActionImpl 是 GetAction 的默认实现。
type GetActionImpl struct {
	ActionImpl
	Name       string
	GetOptions metav1.GetOptions
}

// GetName 返回 get 的对象名。
func (a GetActionImpl) GetName() string {
	return a.Name
}

// GetGetOptions 返回 get 的选项。
func (a GetActionImpl) GetGetOptions() metav1.GetOptions {
	return a.GetOptions
}

// DeepCopy 返回动作副本，选项也做深拷贝。
func (a GetActionImpl) DeepCopy() Action {
	return GetActionImpl{
		ActionImpl: a.ActionImpl.DeepCopy().(ActionImpl),
		Name:       a.Name,
		GetOptions: *a.GetOptions.DeepCopy(),
	}
}

// ListActionImpl 是 ListAction 的默认实现。
type ListActionImpl struct {
	ActionImpl
	Kind             schema.GroupVersionKind
	Name             string
	ListRestrictions ListRestrictions
	ListOptions      metav1.ListOptions
}

// GetKind 返回 list 的资源类型。
func (a ListActionImpl) GetKind() schema.GroupVersionKind {
	return a.Kind
}

// GetListRestrictions 返回解析后的选择器。
func (a ListActionImpl) GetListRestrictions() ListRestrictions {
	return a.ListRestrictions
}

// GetListOptions 返回原始 ListOptions。
func (a ListActionImpl) GetListOptions() metav1.ListOptions {
	return a.ListOptions
}

// DeepCopy 返回动作副本，选择器与选项均做深拷贝。
func (a ListActionImpl) DeepCopy() Action {
	return ListActionImpl{
		ActionImpl: a.ActionImpl.DeepCopy().(ActionImpl),
		Kind:       a.Kind,
		Name:       a.Name,
		ListRestrictions: ListRestrictions{
			Labels: a.ListRestrictions.Labels.DeepCopySelector(),
			Fields: a.ListRestrictions.Fields.DeepCopySelector(),
		},
		ListOptions: *a.ListOptions.DeepCopy(),
	}
}

// CreateActionImpl 是 CreateAction 的默认实现。
type CreateActionImpl struct {
	ActionImpl
	Name          string
	Object        runtime.Object
	CreateOptions metav1.CreateOptions
}

// GetObject 返回要创建的对象。
func (a CreateActionImpl) GetObject() runtime.Object {
	return a.Object
}

// GetCreateOptions 返回创建选项。
func (a CreateActionImpl) GetCreateOptions() metav1.CreateOptions {
	return a.CreateOptions
}

// DeepCopy 返回动作副本，对象与选项均做深拷贝。
func (a CreateActionImpl) DeepCopy() Action {
	return CreateActionImpl{
		ActionImpl:    a.ActionImpl.DeepCopy().(ActionImpl),
		Name:          a.Name,
		Object:        a.Object.DeepCopyObject(),
		CreateOptions: *a.CreateOptions.DeepCopy(),
	}
}

// UpdateActionImpl 是 UpdateAction 的默认实现。
type UpdateActionImpl struct {
	ActionImpl
	Object        runtime.Object
	UpdateOptions metav1.UpdateOptions
}

// GetObject 返回要更新的对象。
func (a UpdateActionImpl) GetObject() runtime.Object {
	return a.Object
}

// GetUpdateOptions 返回更新选项。
func (a UpdateActionImpl) GetUpdateOptions() metav1.UpdateOptions {
	return a.UpdateOptions
}

// DeepCopy 返回动作副本，对象与选项均做深拷贝。
func (a UpdateActionImpl) DeepCopy() Action {
	return UpdateActionImpl{
		ActionImpl:    a.ActionImpl.DeepCopy().(ActionImpl),
		Object:        a.Object.DeepCopyObject(),
		UpdateOptions: *a.UpdateOptions.DeepCopy(),
	}
}

// PatchActionImpl 是 PatchAction 的默认实现。
type PatchActionImpl struct {
	ActionImpl
	Name         string
	PatchType    types.PatchType
	Patch        []byte
	PatchOptions metav1.PatchOptions
}

// GetName 返回被 patch 的对象名。
func (a PatchActionImpl) GetName() string {
	return a.Name
}

// GetPatch 返回 patch 内容字节。
func (a PatchActionImpl) GetPatch() []byte {
	return a.Patch
}

// GetPatchType 返回 patch 类型（json/merge/strategic/apply）。
func (a PatchActionImpl) GetPatchType() types.PatchType {
	return a.PatchType
}

// GetPatchOptions 返回 patch 选项。
func (a PatchActionImpl) GetPatchOptions() metav1.PatchOptions {
	return a.PatchOptions
}

// DeepCopy 返回动作副本，patch 字节与选项均做深拷贝。
func (a PatchActionImpl) DeepCopy() Action {
	patch := make([]byte, len(a.Patch))
	copy(patch, a.Patch)
	return PatchActionImpl{
		ActionImpl:   a.ActionImpl.DeepCopy().(ActionImpl),
		Name:         a.Name,
		PatchType:    a.PatchType,
		Patch:        patch,
		PatchOptions: *a.PatchOptions.DeepCopy(),
	}
}

// DeleteActionImpl 是 DeleteAction 的默认实现。
type DeleteActionImpl struct {
	ActionImpl
	Name          string
	DeleteOptions metav1.DeleteOptions
}

// GetName 返回被删除的对象名。
func (a DeleteActionImpl) GetName() string {
	return a.Name
}

// GetDeleteOptions 返回删除选项。
func (a DeleteActionImpl) GetDeleteOptions() metav1.DeleteOptions {
	return a.DeleteOptions
}

// DeepCopy 返回动作副本，删除选项做深拷贝。
func (a DeleteActionImpl) DeepCopy() Action {
	return DeleteActionImpl{
		ActionImpl:    a.ActionImpl.DeepCopy().(ActionImpl),
		Name:          a.Name,
		DeleteOptions: *a.DeleteOptions.DeepCopy(),
	}
}

// DeleteCollectionActionImpl 是 DeleteCollectionAction 的默认实现。
type DeleteCollectionActionImpl struct {
	ActionImpl
	ListRestrictions ListRestrictions
	DeleteOptions    metav1.DeleteOptions
	ListOptions      metav1.ListOptions
}

// GetListRestrictions 返回解析后的选择器。
func (a DeleteCollectionActionImpl) GetListRestrictions() ListRestrictions {
	return a.ListRestrictions
}

// GetDeleteOptions 返回删除选项。
func (a DeleteCollectionActionImpl) GetDeleteOptions() metav1.DeleteOptions {
	return a.DeleteOptions
}

// GetListOptions 返回原始 ListOptions。
func (a DeleteCollectionActionImpl) GetListOptions() metav1.ListOptions {
	return a.ListOptions
}

// DeepCopy 返回动作副本，选择器、删除选项与列表选项均做深拷贝。
func (a DeleteCollectionActionImpl) DeepCopy() Action {
	return DeleteCollectionActionImpl{
		ActionImpl: a.ActionImpl.DeepCopy().(ActionImpl),
		ListRestrictions: ListRestrictions{
			Labels: a.ListRestrictions.Labels.DeepCopySelector(),
			Fields: a.ListRestrictions.Fields.DeepCopySelector(),
		},
		DeleteOptions: *a.DeleteOptions.DeepCopy(),
		ListOptions:   *a.ListOptions.DeepCopy(),
	}
}

// WatchActionImpl 是 WatchAction 的默认实现。
type WatchActionImpl struct {
	ActionImpl
	WatchRestrictions WatchRestrictions
	ListOptions       metav1.ListOptions
}

// GetWatchRestrictions 返回解析后的选择器与起始 resourceVersion。
func (a WatchActionImpl) GetWatchRestrictions() WatchRestrictions {
	return a.WatchRestrictions
}

// GetListOptions 返回原始 ListOptions。
func (a WatchActionImpl) GetListOptions() metav1.ListOptions {
	return a.ListOptions
}

// DeepCopy 返回动作副本，选择器与列表选项均做深拷贝。
func (a WatchActionImpl) DeepCopy() Action {
	return WatchActionImpl{
		ActionImpl: a.ActionImpl.DeepCopy().(ActionImpl),
		WatchRestrictions: WatchRestrictions{
			Labels:          a.WatchRestrictions.Labels.DeepCopySelector(),
			Fields:          a.WatchRestrictions.Fields.DeepCopySelector(),
			ResourceVersion: a.WatchRestrictions.ResourceVersion,
		},
		ListOptions: *a.ListOptions.DeepCopy(),
	}
}

// ProxyGetActionImpl 是 ProxyGetAction 的默认实现。
type ProxyGetActionImpl struct {
	ActionImpl
	Scheme string
	Name   string
	Port   string
	Path   string
	Params map[string]string
}

// GetScheme 返回 proxy URL 的 scheme。
func (a ProxyGetActionImpl) GetScheme() string {
	return a.Scheme
}

// GetName 返回被代理的对象名。
func (a ProxyGetActionImpl) GetName() string {
	return a.Name
}

// GetPort 返回被代理的端口。
func (a ProxyGetActionImpl) GetPort() string {
	return a.Port
}

// GetPath 返回代理路径。
func (a ProxyGetActionImpl) GetPath() string {
	return a.Path
}

// GetParams 返回附加查询参数。
func (a ProxyGetActionImpl) GetParams() map[string]string {
	return a.Params
}

// DeepCopy 返回动作副本，参数 map 做深拷贝。
func (a ProxyGetActionImpl) DeepCopy() Action {
	params := map[string]string{}
	for k, v := range a.Params {
		params[k] = v
	}
	return ProxyGetActionImpl{
		ActionImpl: a.ActionImpl.DeepCopy().(ActionImpl),
		Scheme:     a.Scheme,
		Name:       a.Name,
		Port:       a.Port,
		Path:       a.Path,
		Params:     params,
	}
}
