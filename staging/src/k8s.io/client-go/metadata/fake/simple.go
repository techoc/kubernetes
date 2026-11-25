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

// Package fake provides an in-memory implementation of metadata.Getter for tests.
//
// Package fake 提供 metadata.Getter 的测试实现。与真实客户端不同，它额外暴露
// CreateFake/UpdateFake，因为 metadata 接口本身只有只读操作；所有动作都会经过
// testing.Fake 的反应器，便于测试断言和注入错误。
package fake

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/testing"
)

// MetadataClient assists in creating fake objects for use when testing, since metadata.Getter
// does not expose create
type MetadataClient interface {
	metadata.Getter
	CreateFake(obj *metav1.PartialObjectMetadata, opts metav1.CreateOptions, subresources ...string) (*metav1.PartialObjectMetadata, error)
	UpdateFake(obj *metav1.PartialObjectMetadata, opts metav1.UpdateOptions, subresources ...string) (*metav1.PartialObjectMetadata, error)
}

// NewTestScheme creates a unique Scheme for each test.
//
// 每个测试使用独立 Scheme，避免并行测试之间通过全局注册表互相污染。
func NewTestScheme() *runtime.Scheme {
	return runtime.NewScheme()
}

// NewSimpleMetadataClient creates a new client that will use the provided scheme and respond with the
// provided objects when requests are made. It will track actions made to the client which can be checked
// with GetActions().
func NewSimpleMetadataClient(scheme *runtime.Scheme, objects ...runtime.Object) *FakeMetadataClient {
	gvkFakeList := schema.GroupVersionKind{Group: "fake-metadata-client-group", Version: "v1", Kind: "List"}
	if !scheme.Recognizes(gvkFakeList) {
		// 要用本客户端执行 List，scheme 中必须注册 v1.List；元数据 List 结果统一用 v1.List 承载，
		// 由于这是测试用的类型，这里直接修改调用方传入的 scheme。
		scheme.AddKnownTypeWithName(gvkFakeList, &metav1.List{})
	}

	codecs := serializer.NewCodecFactory(scheme)
	o := testing.NewObjectTracker(scheme, codecs.UniversalDeserializer())
	for _, obj := range objects {
		// 预先放入的对象视为已存在；重复添加会返回错误，说明测试数据有误。
		if err := o.Add(obj); err != nil {
			panic(err)
		}
	}

	cs := &FakeMetadataClient{scheme: scheme, tracker: o}
	cs.AddReactor("*", "*", testing.ObjectReaction(o))
	cs.AddWatchReactor("*", func(action testing.Action) (handled bool, ret watch.Interface, err error) {
		var opts metav1.ListOptions
		if watchAction, ok := action.(testing.WatchActionImpl); ok {
			opts = watchAction.ListOptions
		}
		gvr := action.GetResource()
		ns := action.GetNamespace()
		watch, err := o.Watch(gvr, ns, opts)
		if err != nil {
			return false, nil, err
		}
		return true, watch, nil
	})

	return cs
}

// FakeMetadataClient implements clientset.Interface. Meant to be embedded into a
// struct to get a default implementation. This makes faking out just the method
// you want to test easier.
type FakeMetadataClient struct {
	testing.Fake
	scheme  *runtime.Scheme
	tracker testing.ObjectTracker
}

// metadataResourceClient 绑定具体资源与（可选）命名空间，复用统一的方法实现。
type metadataResourceClient struct {
	client    *FakeMetadataClient
	namespace string
	resource  schema.GroupVersionResource
}

var (
	_ metadata.Interface = &FakeMetadataClient{}
	_ testing.FakeClient = &FakeMetadataClient{}
)

// Tracker 返回底层对象跟踪器，测试可直接操作以准备数据。
func (c *FakeMetadataClient) Tracker() testing.ObjectTracker {
	return c.tracker
}

// Resource returns an interface for accessing the provided resource.
func (c *FakeMetadataClient) Resource(resource schema.GroupVersionResource) metadata.Getter {
	return &metadataResourceClient{client: c, resource: resource}
}

// IsWatchListSemanticsUnSupported 返回 true：该 fake 不支持 WatchList 语义，
// 以便 Reflector 在没有真实流式能力时回退到普通 List+Watch。
func (c *FakeMetadataClient) IsWatchListSemanticsUnSupported() bool {
	return true
}

// Namespace returns an interface for accessing the current resource in the specified
// namespace.
//
// 复制值而不是修改接收者，避免同一 Resource 句柄被并发用于多个命名空间。
func (c *metadataResourceClient) Namespace(ns string) metadata.ResourceInterface {
	ret := *c
	ret.namespace = ns
	return &ret
}

// CreateFake records the object creation and processes it via the reactor.
//
// 四个分支对应 根/命名空间 × 主资源/子资源 的组合；子资源动作需要对象名，
// 因此先从对象中取 metadata。
func (c *metadataResourceClient) CreateFake(obj *metav1.PartialObjectMetadata, opts metav1.CreateOptions, subresources ...string) (*metav1.PartialObjectMetadata, error) {
	var uncastRet runtime.Object
	var err error
	switch {
	case len(c.namespace) == 0 && len(subresources) == 0:
		uncastRet, err = c.client.Fake.
			Invokes(testing.NewRootCreateAction(c.resource, obj), obj)

	case len(c.namespace) == 0 && len(subresources) > 0:
		var accessor metav1.Object // avoid shadowing err
		accessor, err = meta.Accessor(obj)
		if err != nil {
			return nil, err
		}
		name := accessor.GetName()
		uncastRet, err = c.client.Fake.
			Invokes(testing.NewRootCreateSubresourceAction(c.resource, name, strings.Join(subresources, "/"), obj), obj)

	case len(c.namespace) > 0 && len(subresources) == 0:
		uncastRet, err = c.client.Fake.
			Invokes(testing.NewCreateAction(c.resource, c.namespace, obj), obj)

	case len(c.namespace) > 0 && len(subresources) > 0:
		var accessor metav1.Object // avoid shadowing err
		accessor, err = meta.Accessor(obj)
		if err != nil {
			return nil, err
		}
		name := accessor.GetName()
		uncastRet, err = c.client.Fake.
			Invokes(testing.NewCreateSubresourceAction(c.resource, name, strings.Join(subresources, "/"), c.namespace, obj), obj)

	}

	if err != nil {
		return nil, err
	}
	if uncastRet == nil {
		return nil, err
	}
	// 反应器可能返回任意 runtime.Object，这里严格断言，尽早暴露测试中的错误桩。
	ret, ok := uncastRet.(*metav1.PartialObjectMetadata)
	if !ok {
		return nil, fmt.Errorf("unexpected return value type %T", uncastRet)
	}
	return ret, err
}

// UpdateFake records the object update and processes it via the reactor.
func (c *metadataResourceClient) UpdateFake(obj *metav1.PartialObjectMetadata, opts metav1.UpdateOptions, subresources ...string) (*metav1.PartialObjectMetadata, error) {
	var uncastRet runtime.Object
	var err error
	switch {
	case len(c.namespace) == 0 && len(subresources) == 0:
		uncastRet, err = c.client.Fake.
			Invokes(testing.NewRootUpdateAction(c.resource, obj), obj)

	case len(c.namespace) == 0 && len(subresources) > 0:
		uncastRet, err = c.client.Fake.
			Invokes(testing.NewRootUpdateSubresourceAction(c.resource, strings.Join(subresources, "/"), obj), obj)

	case len(c.namespace) > 0 && len(subresources) == 0:
		uncastRet, err = c.client.Fake.
			Invokes(testing.NewUpdateAction(c.resource, c.namespace, obj), obj)

	case len(c.namespace) > 0 && len(subresources) > 0:
		uncastRet, err = c.client.Fake.
			Invokes(testing.NewUpdateSubresourceAction(c.resource, strings.Join(subresources, "/"), c.namespace, obj), obj)

	}

	if err != nil {
		return nil, err
	}
	if uncastRet == nil {
		return nil, err
	}
	ret, ok := uncastRet.(*metav1.PartialObjectMetadata)
	if !ok {
		return nil, fmt.Errorf("unexpected return value type %T", uncastRet)
	}
	return ret, err
}

// UpdateStatus records the object status update and processes it via the reactor.
func (c *metadataResourceClient) UpdateStatus(obj *metav1.PartialObjectMetadata, opts metav1.UpdateOptions) (*metav1.PartialObjectMetadata, error) {
	var uncastRet runtime.Object
	var err error
	// status 永远是子资源，因此只需区分是否带命名空间。
	switch {
	case len(c.namespace) == 0:
		uncastRet, err = c.client.Fake.
			Invokes(testing.NewRootUpdateSubresourceAction(c.resource, "status", obj), obj)

	case len(c.namespace) > 0:
		uncastRet, err = c.client.Fake.
			Invokes(testing.NewUpdateSubresourceAction(c.resource, "status", c.namespace, obj), obj)

	}

	if err != nil {
		return nil, err
	}
	if uncastRet == nil {
		return nil, err
	}
	ret, ok := uncastRet.(*metav1.PartialObjectMetadata)
	if !ok {
		return nil, fmt.Errorf("unexpected return value type %T", uncastRet)
	}
	return ret, err
}

// Delete records the object deletion and processes it via the reactor.
func (c *metadataResourceClient) Delete(ctx context.Context, name string, opts metav1.DeleteOptions, subresources ...string) error {
	var err error
	switch {
	case len(c.namespace) == 0 && len(subresources) == 0:
		_, err = c.client.Fake.
			Invokes(testing.NewRootDeleteActionWithOptions(c.resource, name, opts), &metav1.Status{Status: "metadata delete fail"})

	case len(c.namespace) == 0 && len(subresources) > 0:
		_, err = c.client.Fake.
			Invokes(testing.NewRootDeleteSubresourceActionWithOptions(c.resource, strings.Join(subresources, "/"), name, opts), &metav1.Status{Status: "metadata delete fail"})

	case len(c.namespace) > 0 && len(subresources) == 0:
		_, err = c.client.Fake.
			Invokes(testing.NewDeleteActionWithOptions(c.resource, c.namespace, name, opts), &metav1.Status{Status: "metadata delete fail"})

	case len(c.namespace) > 0 && len(subresources) > 0:
		_, err = c.client.Fake.
			Invokes(testing.NewDeleteSubresourceActionWithOptions(c.resource, strings.Join(subresources, "/"), c.namespace, name, opts), &metav1.Status{Status: "metadata delete fail"})
	}

	return err
}

// DeleteCollection records the object collection deletion and processes it via the reactor.
func (c *metadataResourceClient) DeleteCollection(ctx context.Context, opts metav1.DeleteOptions, listOptions metav1.ListOptions) error {
	var err error
	switch {
	case len(c.namespace) == 0:
		action := testing.NewRootDeleteCollectionAction(c.resource, listOptions)
		_, err = c.client.Fake.Invokes(action, &metav1.Status{Status: "metadata deletecollection fail"})

	case len(c.namespace) > 0:
		action := testing.NewDeleteCollectionAction(c.resource, c.namespace, listOptions)
		_, err = c.client.Fake.Invokes(action, &metav1.Status{Status: "metadata deletecollection fail"})

	}

	return err
}

// Get records the object retrieval and processes it via the reactor.
func (c *metadataResourceClient) Get(ctx context.Context, name string, opts metav1.GetOptions, subresources ...string) (*metav1.PartialObjectMetadata, error) {
	var uncastRet runtime.Object
	var err error
	switch {
	case len(c.namespace) == 0 && len(subresources) == 0:
		uncastRet, err = c.client.Fake.
			Invokes(testing.NewRootGetAction(c.resource, name), &metav1.Status{Status: "metadata get fail"})

	case len(c.namespace) == 0 && len(subresources) > 0:
		uncastRet, err = c.client.Fake.
			Invokes(testing.NewRootGetSubresourceAction(c.resource, strings.Join(subresources, "/"), name), &metav1.Status{Status: "metadata get fail"})

	case len(c.namespace) > 0 && len(subresources) == 0:
		uncastRet, err = c.client.Fake.
			Invokes(testing.NewGetAction(c.resource, c.namespace, name), &metav1.Status{Status: "metadata get fail"})

	case len(c.namespace) > 0 && len(subresources) > 0:
		uncastRet, err = c.client.Fake.
			Invokes(testing.NewGetSubresourceAction(c.resource, c.namespace, strings.Join(subresources, "/"), name), &metav1.Status{Status: "metadata get fail"})
	}

	if err != nil {
		return nil, err
	}
	if uncastRet == nil {
		return nil, err
	}
	ret, ok := uncastRet.(*metav1.PartialObjectMetadata)
	if !ok {
		return nil, fmt.Errorf("unexpected return value type %T", uncastRet)
	}
	return ret, err
}

// List records the object deletion and processes it via the reactor.
//
// List 的 Kind 留空：tracker 会自动补上 "List" 后缀，与真实 scheme 的行为一致。
func (c *metadataResourceClient) List(ctx context.Context, opts metav1.ListOptions) (*metav1.PartialObjectMetadataList, error) {
	var obj runtime.Object
	var err error
	switch {
	case len(c.namespace) == 0:
		obj, err = c.client.Fake.
			Invokes(testing.NewRootListAction(c.resource, schema.GroupVersionKind{Group: "fake-metadata-client-group", Version: "v1", Kind: "" /*List is appended by the tracker automatically*/}, opts), &metav1.Status{Status: "metadata list fail"})

	case len(c.namespace) > 0:
		obj, err = c.client.Fake.
			Invokes(testing.NewListAction(c.resource, schema.GroupVersionKind{Group: "fake-metadata-client-group", Version: "v1", Kind: "" /*List is appended by the tracker automatically*/}, c.namespace, opts), &metav1.Status{Status: "metadata list fail"})

	}

	if obj == nil {
		return nil, err
	}

	// fake 的 tracker 不一定实现标签过滤，这里在客户端侧再过滤一次。
	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}

	inputList, ok := obj.(*metav1.List)
	if !ok {
		return nil, fmt.Errorf("incoming object is incorrect type %T", obj)
	}

	list := &metav1.PartialObjectMetadataList{
		ListMeta: inputList.ListMeta,
	}
	for i := range inputList.Items {
		item, ok := inputList.Items[i].Object.(*metav1.PartialObjectMetadata)
		if !ok {
			return nil, fmt.Errorf("item %d in list %T is %T", i, inputList, inputList.Items[i].Object)
		}
		metadata, err := meta.Accessor(item)
		if err != nil {
			return nil, err
		}
		if label.Matches(labels.Set(metadata.GetLabels())) {
			list.Items = append(list.Items, *item)
		}
	}
	return list, nil
}

// Watch 强制把 opts.Watch 置为 true，然后交给 tracker 生成 watch 事件流。
func (c *metadataResourceClient) Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error) {
	opts.Watch = true
	switch {
	case len(c.namespace) == 0:
		return c.client.Fake.
			InvokesWatch(testing.NewRootWatchAction(c.resource, opts))

	case len(c.namespace) > 0:
		return c.client.Fake.
			InvokesWatch(testing.NewWatchAction(c.resource, c.namespace, opts))
	}

	// 命名空间长度不可能同时为 0 和 >0，走到这里说明逻辑错误。
	panic("math broke")
}

// Patch records the object patch and processes it via the reactor.
func (c *metadataResourceClient) Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions, subresources ...string) (*metav1.PartialObjectMetadata, error) {
	var uncastRet runtime.Object
	var err error
	switch {
	case len(c.namespace) == 0 && len(subresources) == 0:
		uncastRet, err = c.client.Fake.
			Invokes(testing.NewRootPatchAction(c.resource, name, pt, data), &metav1.Status{Status: "metadata patch fail"})

	case len(c.namespace) == 0 && len(subresources) > 0:
		uncastRet, err = c.client.Fake.
			Invokes(testing.NewRootPatchSubresourceAction(c.resource, name, pt, data, subresources...), &metav1.Status{Status: "metadata patch fail"})

	case len(c.namespace) > 0 && len(subresources) == 0:
		uncastRet, err = c.client.Fake.
			Invokes(testing.NewPatchAction(c.resource, c.namespace, name, pt, data), &metav1.Status{Status: "metadata patch fail"})

	case len(c.namespace) > 0 && len(subresources) > 0:
		uncastRet, err = c.client.Fake.
			Invokes(testing.NewPatchSubresourceAction(c.resource, c.namespace, name, pt, data, subresources...), &metav1.Status{Status: "metadata patch fail"})

	}

	if err != nil {
		return nil, err
	}
	if uncastRet == nil {
		return nil, err
	}
	ret, ok := uncastRet.(*metav1.PartialObjectMetadata)
	if !ok {
		return nil, fmt.Errorf("unexpected return value type %T", uncastRet)
	}
	return ret, err
}
