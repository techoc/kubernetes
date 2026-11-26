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

package resource

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
)

// 共享的元数据访问器，统一读写 name/namespace/resourceVersion 等字段，无需区分具体类型。
var metadataAccessor = meta.NewAccessor()

// Helper 提供检索和修改 RESTful 资源的方法。
type Helper struct {
	// Resource 是服务端识别的复数资源名，如 "pods"。
	Resource string
	// Subresource 是服务端识别的子资源名，如 "status"；为空表示直接操作主资源。
	Subresource string
	// 能修改该资源的 RESTClient。
	RESTClient RESTClient
	// NamespaceScoped 为 true 表示该资源类型属于命名空间作用域，
	// 决定请求路径是否带 namespaces/<ns> 前缀。
	NamespaceScoped bool
	// 为 true 时对支持服务端 dry-run 的操作和资源启用 dry-run，不落盘。
	//
	// 仅适用于开启 dry-run 的 apiserver 及支持 dry-run 的资源；
	// 否则变更仍会写入存储。
	ServerDryRun bool

	// FieldManager 标识发起变更的执行者。
	FieldManager string

	// FieldValidation 指定服务端执行字段校验的方式（Ignore、Warn 或 Strict）。
	FieldValidation string
}

// NewHelper 从 ResourceMapping 创建 Helper。
// 资源名与命名空间作用域都取自 RESTMapping，保证与实际注册的资源一致。
func NewHelper(client RESTClient, mapping *meta.RESTMapping) *Helper {
	return &Helper{
		Resource:        mapping.Resource.Resource,
		RESTClient:      client,
		NamespaceScoped: mapping.Scope.Name() == meta.RESTScopeNameNamespace,
	}
}

// DryRun 设置是否为 true：为 true 时使用 server-side dry-run，不把变更写入存储；
// 否则变更会落盘。
// 注意 server-side dry-run 只在服务端支持时生效，否则变更仍会落盘，调用方需自行确认兼容性。
func (m *Helper) DryRun(dryRun bool) *Helper {
	m.ServerDryRun = dryRun
	return m
}

// WithFieldManager 设置创建或更新操作中的字段管理器标识。
func (m *Helper) WithFieldManager(fieldManager string) *Helper {
	m.FieldManager = fieldManager
	return m
}

// WithFieldValidation 设置服务端的字段校验方式（Ignore、Warn 或 Strict）。
func (m *Helper) WithFieldValidation(validationDirective string) *Helper {
	m.FieldValidation = validationDirective
	return m
}

// Subresource 让 Helper 访问 (<resource>/[ns/<namespace>/]<name>/<subresource>) 路径。
func (m *Helper) WithSubresource(subresource string) *Helper {
	m.Subresource = subresource
	return m
}

func (m *Helper) Get(namespace, name string) (runtime.Object, error) {
	// context.TODO 是历史遗留：Helper 接口尚未接受调用方 context。
	req := m.RESTClient.Get().
		NamespaceIfScoped(namespace, m.NamespaceScoped).
		Resource(m.Resource).
		Name(name).
		SubResource(m.Subresource)
	return req.Do(context.TODO()).Get()
}

func (m *Helper) List(namespace, apiVersion string, options *metav1.ListOptions) (runtime.Object, error) {
	req := m.RESTClient.Get().
		NamespaceIfScoped(namespace, m.NamespaceScoped).
		Resource(m.Resource).
		VersionedParams(options, metav1.ParameterCodec)
	return req.Do(context.TODO()).Get()
}

// FollowContinue 处理使用 list 分页（chunking）时 API 服务端返回的 continue 参数。
// 要利用它，调用方给出的初始 ListOptions 应包含非零的 Limit 参数。
// 循环拉取直到服务端不再返回 continue token，调用方据此拿到完整结果集。
func FollowContinue(initialOpts *metav1.ListOptions,
	listFunc func(metav1.ListOptions) (runtime.Object, error)) error {
	opts := initialOpts
	for {
		list, err := listFunc(*opts)
		if err != nil {
			return err
		}
		nextContinueToken, _ := metadataAccessor.Continue(list)
		if len(nextContinueToken) == 0 {
			return nil
		}
		opts.Continue = nextContinueToken
	}
}

// 给 List 操作常见的错误补充上下文，同时尽量保留 StatusError 类型。
func EnhanceListError(err error, opts metav1.ListOptions, subj string) error {
	// ResourceExpired 需要用户重试整个列表，改写消息没有意义，原样返回。
	if apierrors.IsResourceExpired(err) {
		return err
	}
	if apierrors.IsBadRequest(err) || apierrors.IsNotFound(err) {
		if se, ok := err.(*apierrors.StatusError); ok {
			// 修改消息但不掩盖它仍然是 API 错误：仅改 Message，保留 StatusError 类型，调用方仍可做 IsNotFound 等判断。
			if len(opts.LabelSelector) == 0 && len(opts.FieldSelector) == 0 {
				se.ErrStatus.Message = fmt.Sprintf("Unable to list %q: %v", subj,
					se.ErrStatus.Message)
			} else {
				se.ErrStatus.Message = fmt.Sprintf(
					"Unable to find %q that match label selector %q, field selector %q: %v", subj,
					opts.LabelSelector,
					opts.FieldSelector, se.ErrStatus.Message)
			}
			return se
		}
		if len(opts.LabelSelector) == 0 && len(opts.FieldSelector) == 0 {
			return fmt.Errorf("Unable to list %q: %v", subj, err)
		}
		return fmt.Errorf("Unable to find %q that match label selector %q, field selector %q: %v",
			subj, opts.LabelSelector, opts.FieldSelector, err)
	}
	return err
}

func (m *Helper) Watch(namespace, apiVersion string, options *metav1.ListOptions) (watch.Interface, error) {
	// 复用 ListOptions，额外置 Watch 标志即可切换为 watch 请求。
	options.Watch = true
	return m.RESTClient.Get().
		NamespaceIfScoped(namespace, m.NamespaceScoped).
		Resource(m.Resource).
		VersionedParams(options, metav1.ParameterCodec).
		Watch(context.TODO())
}

func (m *Helper) WatchSingle(namespace, name, resourceVersion string) (watch.Interface, error) {
	// 用 fieldSelector 把 watch 收窄到单个对象，从指定 resourceVersion 开始。
	return m.RESTClient.Get().
		NamespaceIfScoped(namespace, m.NamespaceScoped).
		Resource(m.Resource).
		VersionedParams(&metav1.ListOptions{
			ResourceVersion: resourceVersion,
			Watch:           true,
			FieldSelector:   fields.OneTermEqualSelector("metadata.name", name).String(),
		}, metav1.ParameterCodec).
		Watch(context.TODO())
}

func (m *Helper) Delete(namespace, name string) (runtime.Object, error) {
	return m.DeleteWithOptions(namespace, name, nil)
}

func (m *Helper) DeleteWithOptions(namespace, name string, options *metav1.DeleteOptions) (runtime.Object, error) {
	// nil 也要归一成空 options，后面才能安全地追加 dry-run。
	if options == nil {
		options = &metav1.DeleteOptions{}
	}
	if m.ServerDryRun {
		options.DryRun = []string{metav1.DryRunAll}
	}

	return m.RESTClient.Delete().
		NamespaceIfScoped(namespace, m.NamespaceScoped).
		Resource(m.Resource).
		Name(name).
		Body(options).
		Do(context.TODO()).
		Get()
}

func (m *Helper) Create(namespace string, modify bool, obj runtime.Object) (runtime.Object, error) {
	return m.CreateWithOptions(namespace, modify, obj, nil)
}

func (m *Helper) CreateWithOptions(namespace string, modify bool, obj runtime.Object, options *metav1.CreateOptions) (runtime.Object, error) {
	if options == nil {
		options = &metav1.CreateOptions{}
	}
	if m.ServerDryRun {
		options.DryRun = []string{metav1.DryRunAll}
	}
	if m.FieldManager != "" {
		options.FieldManager = m.FieldManager
	}
	if m.FieldValidation != "" {
		options.FieldValidation = m.FieldValidation
	}
	if modify {
		// 按客户端逻辑尝试为对象设置版本。
		// 创建时若对象带了 resourceVersion，服务端会拒绝；先清空以适配“apply 式”创建。
		version, err := metadataAccessor.ResourceVersion(obj)
		if err != nil {
			// 不知道如何清除该对象的版本，原样发给服务端。
			return m.createResource(m.RESTClient, m.Resource, namespace, obj, options)
		}
		if version != "" {
			if err := metadataAccessor.SetResourceVersion(obj, ""); err != nil {
				return nil, err
			}
		}
	}

	return m.createResource(m.RESTClient, m.Resource, namespace, obj, options)
}

func (m *Helper) createResource(c RESTClient, resource, namespace string, obj runtime.Object, options *metav1.CreateOptions) (runtime.Object, error) {
	// POST <resource>?<options>；options 走查询参数编码。
	return c.Post().
		NamespaceIfScoped(namespace, m.NamespaceScoped).
		Resource(resource).
		VersionedParams(options, metav1.ParameterCodec).
		Body(obj).
		Do(context.TODO()).
		Get()
}
func (m *Helper) Patch(namespace, name string, pt types.PatchType, data []byte, options *metav1.PatchOptions) (runtime.Object, error) {
	if options == nil {
		options = &metav1.PatchOptions{}
	}
	if m.ServerDryRun {
		options.DryRun = []string{metav1.DryRunAll}
	}
	// fieldManager/fieldValidation 通过查询参数下发，未设置时保持服务端默认。
	if m.FieldManager != "" {
		options.FieldManager = m.FieldManager
	}
	if m.FieldValidation != "" {
		options.FieldValidation = m.FieldValidation
	}
	return m.RESTClient.Patch(pt).
		NamespaceIfScoped(namespace, m.NamespaceScoped).
		Resource(m.Resource).
		Name(name).
		SubResource(m.Subresource).
		VersionedParams(options, metav1.ParameterCodec).
		Body(data).
		Do(context.TODO()).
		Get()
}

func (m *Helper) Replace(namespace, name string, overwrite bool, obj runtime.Object) (runtime.Object, error) {
	c := m.RESTClient
	var options = &metav1.UpdateOptions{}
	if m.ServerDryRun {
		options.DryRun = []string{metav1.DryRunAll}
	}
	if m.FieldManager != "" {
		options.FieldManager = m.FieldManager
	}
	if m.FieldValidation != "" {
		options.FieldValidation = m.FieldValidation
	}

	// 按客户端逻辑尝试为对象设置版本。
	// overwrite 且对象没有 resourceVersion 时先取服务端版本回填，避免无条件覆盖他人改动。
	version, err := metadataAccessor.ResourceVersion(obj)
	if err != nil {
		// 不知道如何为该对象设置版本，原样发给服务端。
		return m.replaceResource(c, m.Resource, namespace, name, obj, options)
	}
	if version == "" && overwrite {
		// 取回对象的当前版本，用于覆盖服务端对象。
		serverObj, err := c.Get().NamespaceIfScoped(namespace, m.NamespaceScoped).Resource(m.Resource).Name(name).SubResource(m.Subresource).Do(context.TODO()).Get()
		if err != nil {
			// 对象尚不存在，需要创建。
			return m.replaceResource(c, m.Resource, namespace, name, obj, options)
		}
		serverVersion, err := metadataAccessor.ResourceVersion(serverObj)
		if err != nil {
			return nil, err
		}
		if err := metadataAccessor.SetResourceVersion(obj, serverVersion); err != nil {
			return nil, err
		}
	}

	return m.replaceResource(c, m.Resource, namespace, name, obj, options)
}

func (m *Helper) replaceResource(c RESTClient, resource, namespace, name string, obj runtime.Object, options *metav1.UpdateOptions) (runtime.Object, error) {
	// PUT <resource>/<name>；子资源路径由 m.Subresource 拼接。
	return c.Put().
		NamespaceIfScoped(namespace, m.NamespaceScoped).
		Resource(resource).
		Name(name).
		SubResource(m.Subresource).
		VersionedParams(options, metav1.ParameterCodec).
		Body(obj).
		Do(context.TODO()).
		Get()
}
