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
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
)

// RESTClientGetter 是命令层获取 REST 配置、发现客户端与 RESTMapper 的统一入口。
// 抽象成接口便于用假实现替换真实集群访问（如 TestConfigFlags）。
type RESTClientGetter interface {
	ToRESTConfig() (*rest.Config, error)
	ToDiscoveryClient() (discovery.CachedDiscoveryInterface, error)
	ToRESTMapper() (meta.RESTMapper, error)
}

// 以下函数类型用于延迟构造；需要时再创建，避免无谓的发现请求。
type ClientConfigFunc func() (*rest.Config, error)
type RESTMapperFunc func() (meta.RESTMapper, error)
type CategoryExpanderFunc func() (restmapper.CategoryExpander, error)

// RESTClient 以通用方式处理 RESTful 资源。
type RESTClient interface {
	Get() *rest.Request
	Post() *rest.Request
	Patch(types.PatchType) *rest.Request
	Delete() *rest.Request
	Put() *rest.Request
}

// RequestTransform 可修改即将发出的请求。
type RequestTransform func(*rest.Request)

// 包装给定的 RESTClient，对每个新建请求依次执行所有 transform。
//
// 没有 transform 时直接返回原 client，避免为常见路径引入包装与额外调用开销。
func NewClientWithOptions(c RESTClient, transforms ...RequestTransform) RESTClient {
	if len(transforms) == 0 {
		return c
	}
	return &clientOptions{c: c, transforms: transforms}
}

// clientOptions 是 RESTClient 的装饰器：所有动词返回的请求都先经过 transforms。
type clientOptions struct {
	c          RESTClient
	transforms []RequestTransform
}

func (c *clientOptions) modify(req *rest.Request) *rest.Request {
	for _, transform := range c.transforms {
		transform(req)
	}
	return req
}

func (c *clientOptions) Get() *rest.Request {
	return c.modify(c.c.Get())
}

func (c *clientOptions) Post() *rest.Request {
	return c.modify(c.c.Post())
}
func (c *clientOptions) Patch(t types.PatchType) *rest.Request {
	return c.modify(c.c.Patch(t))
}
func (c *clientOptions) Delete() *rest.Request {
	return c.modify(c.c.Delete())
}
func (c *clientOptions) Put() *rest.Request {
	return c.modify(c.c.Put())
}

// ContentValidator 校验序列化为字节数组的 API 对象。
type ContentValidator interface {
	ValidateBytes(data []byte) error
}

// Visitor 让客户端遍历资源列表。
type Visitor interface {
	Visit(VisitorFunc) error
}

// VisitorFunc 用函数实现 Visitor。遍历出错时传入的 error 描述问题，由函数决定如何处理；
// 返回 nil 表示接受错误并继续遍历，便于忽略特定错误或按某种方式聚合。
//
// 注意返回 nil 表示“接受该错误并继续”，而不是“没有错误”——
// 这是 visitor 链支持批量操作（部分失败继续）的关键约定。
type VisitorFunc func(*Info, error) error
