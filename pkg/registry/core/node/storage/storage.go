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

package storage

import (
	"context"
	"fmt"
	"net/http"
	"net/url"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/registry/generic"
	genericregistry "k8s.io/apiserver/pkg/registry/generic/registry"
	"k8s.io/apiserver/pkg/registry/rest"
	api "k8s.io/kubernetes/pkg/apis/core"
	k8s_api_v1 "k8s.io/kubernetes/pkg/apis/core/v1"
	"k8s.io/kubernetes/pkg/kubelet/client"
	"k8s.io/kubernetes/pkg/printers"
	printersinternal "k8s.io/kubernetes/pkg/printers/internalversion"
	printerstorage "k8s.io/kubernetes/pkg/printers/storage"
	"k8s.io/kubernetes/pkg/registry/core/node"
	noderest "k8s.io/kubernetes/pkg/registry/core/node/rest"
	"sigs.k8s.io/structured-merge-diff/v6/fieldpath"
)

// NodeStorage 聚合 Node 主资源、Status 子资源和 Proxy 子资源，并向其他组件暴露 Kubelet 连接信息。
type NodeStorage struct {
	Node   *REST
	Status *StatusREST
	Proxy  *noderest.ProxyREST

	KubeletConnectionInfo client.ConnectionInfoGetter
}

// REST implements a RESTStorage for nodes.
type REST struct {
	*genericregistry.Store
	connection     client.ConnectionInfoGetter
	proxyTransport http.RoundTripper
}

// StatusREST implements the REST endpoint for changing the status of a node.
type StatusREST struct {
	store *genericregistry.Store
}

// New creates a new Node object.
func (r *StatusREST) New() runtime.Object {
	return &api.Node{}
}

// Destroy cleans up resources on shutdown.
func (r *StatusREST) Destroy() {
	// Given that underlying store is shared with REST,
	// we don't destroy it here explicitly.
}

// Get retrieves the object from the storage. It is required to support Patch.
func (r *StatusREST) Get(ctx context.Context, name string, options *metav1.GetOptions) (runtime.Object, error) {
	return r.store.Get(ctx, name, options)
}

// Update alters the status subset of an object.
func (r *StatusREST) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	// We are explicitly setting forceAllowCreate to false in the call to the underlying storage because
	// subresources should never allow create on update.
	return r.store.Update(ctx, name, objInfo, createValidation, updateValidation, false, options)
}

// GetResetFields implements rest.ResetFieldsStrategy
func (r *StatusREST) GetResetFields() map[fieldpath.APIVersion]*fieldpath.Set {
	return r.store.GetResetFields()
}

func (r *StatusREST) ConvertToTable(ctx context.Context, object runtime.Object, tableOptions runtime.Object) (*metav1.Table, error) {
	return r.store.ConvertToTable(ctx, object, tableOptions)
}

// NewStorage 创建集群级 Node REST 存储，并构造从 Node 地址/端口推导 Kubelet 连接信息的 Getter。
// NewStorage returns a NodeStorage object that will work against nodes.
func NewStorage(optsGetter generic.RESTOptionsGetter, kubeletClientConfig client.KubeletClientConfig, proxyTransport http.RoundTripper) (*NodeStorage, error) {
	// genericregistry.Store 提供通用 CRUD；Node 特有的对象类型、字段选择器和写入策略在这里注入。
	store := &genericregistry.Store{
		NewFunc:                   func() runtime.Object { return &api.Node{} },
		NewListFunc:               func() runtime.Object { return &api.NodeList{} },
		PredicateFunc:             node.MatchNode,
		DefaultQualifiedResource:  api.Resource("nodes"),
		SingularQualifiedResource: api.Resource("node"),

		CreateStrategy:      node.Strategy,
		UpdateStrategy:      node.Strategy,
		DeleteStrategy:      node.Strategy,
		ResetFieldsStrategy: node.Strategy,

		TableConvertor: printerstorage.TableConvertor{TableGenerator: printers.NewTableGenerator().With(printersinternal.AddHandlers)},
	}
	// StoreOptions 指定 etcd/后端配置和属性提取函数，供 CompleteWithOptions 完成存储初始化。
	options := &generic.StoreOptions{
		RESTOptions: optsGetter,
		AttrFunc:    node.GetAttrs,
	}
	if err := store.CompleteWithOptions(options); err != nil {
		return nil, err
	}

	// Status 子资源复用相同持久化后端，但复制 Store 并换成 StatusStrategy，隔离可写字段范围。
	statusStore := *store
	statusStore.UpdateStrategy = node.StatusStrategy
	statusStore.ResetFieldsStrategy = node.StatusStrategy

	// 为 Node 主资源、Status 和 Proxy 分别创建 REST Handler；它们共享底层 Store，但职责不同。
	nodeREST := &REST{Store: store, proxyTransport: proxyTransport}
	statusREST := &StatusREST{store: &statusStore}
	proxyREST := &noderest.ProxyREST{Store: store, ProxyTransport: proxyTransport}

	// NodeConnectionInfoGetter 需要读取 API Node，因此用已装配的 REST handler 作为 NodeGetter，避免另建不一致的数据源。
	nodeGetter := client.NodeGetterFunc(func(ctx context.Context, nodeName string, options metav1.GetOptions) (*v1.Node, error) {
		obj, err := nodeREST.Get(ctx, nodeName, &options)
		if err != nil {
			return nil, err
		}
		node, ok := obj.(*api.Node)
		if !ok {
			return nil, fmt.Errorf("unexpected type %T", obj)
		}
		// 目前连接信息计算依赖外部 v1.Node，故将内部 Node 转成 v1；未来可只提取 NodeAddresses 和 DaemonEndpoints。
		externalNode := &v1.Node{}
		err = k8s_api_v1.Convert_core_Node_To_v1_Node(node, externalNode, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to convert to v1.Node: %v", err)
		}
		return externalNode, nil
	})
	// 根据节点地址优先级、Kubelet 端口、TLS 证书和客户端配置创建统一连接信息提供者。
	connectionInfoGetter, err := client.NewNodeConnectionInfoGetter(nodeGetter, kubeletClientConfig)
	if err != nil {
		return nil, err
	}
	// 将同一个连接信息 Getter 注入 Node 主资源重定向和 Proxy 子资源，确保两条代理路径使用一致配置。
	nodeREST.connection = connectionInfoGetter
	proxyREST.Connection = connectionInfoGetter

	return &NodeStorage{
		Node:                  nodeREST,
		Status:                statusREST,
		Proxy:                 proxyREST,
		KubeletConnectionInfo: connectionInfoGetter,
	}, nil
}

// Implement Redirector.
var _ = rest.Redirector(&REST{})

// ResourceLocation returns a URL to which one can send traffic for the specified node.
// ResourceLocation 将 Node proxy 请求委托给 node 包的地址解析逻辑，返回目标 URL 与对应 HTTP Transport。
func (r *REST) ResourceLocation(ctx context.Context, id string) (*url.URL, http.RoundTripper, error) {
	return node.ResourceLocation(r, r.connection, r.proxyTransport, ctx, id)
}

// ShortNames implements the ShortNamesProvider interface. Returns a list of short names for a resource.
func (r *REST) ShortNames() []string {
	return []string{"no"}
}
