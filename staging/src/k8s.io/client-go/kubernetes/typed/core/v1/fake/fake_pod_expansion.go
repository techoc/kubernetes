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

package fake

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	v1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	restclient "k8s.io/client-go/rest"
	fakerest "k8s.io/client-go/rest/fake"
	core "k8s.io/client-go/testing"
)

// Bind 通过 Fake 反应器模拟 Pod 绑定（create pods/binding 子资源）。
func (c *fakePods) Bind(ctx context.Context, binding *v1.Binding, opts metav1.CreateOptions) error {
	action := core.CreateActionImpl{}
	action.Verb = "create"
	action.Namespace = binding.Namespace
	action.Resource = c.Resource()
	action.Subresource = "binding"
	action.Object = binding

	_, err := c.Fake.Invokes(action, binding)
	return err
}

// GetBinding 模拟读取 pods/binding 子资源；Fake 返回 nil 时保持与真实客户端一致，
// 返回 nil 结果而不是做类型断言。
func (c *fakePods) GetBinding(name string) (result *v1.Binding, err error) {
	obj, err := c.Fake.
		Invokes(core.NewGetSubresourceAction(c.Resource(), c.Namespace(), "binding", name), &v1.Binding{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1.Binding), err
}

// GetLogs 模拟 pods/log：先在 Fake 反应器里触发一次 generic get 动作，让测试可以
// 记录/拦截该请求，再构造一个只返回预设日志内容的假 RESTClient。
// 默认返回 "fake logs"，测试可用反应器替换为 *runtime.Unknown 提供自定义内容。
func (c *fakePods) GetLogs(name string, opts *v1.PodLogOptions) *restclient.Request {
	action := core.GenericActionImpl{}
	action.Verb = "get"
	action.Namespace = c.Namespace()
	action.Resource = c.Resource()
	action.Subresource = "log"
	action.Value = opts

	defaultLogResponse := &runtime.Unknown{Raw: []byte("fake logs")}
	obj, err := c.Fake.Invokes(action, defaultLogResponse)
	logs := defaultLogResponse.Raw
	if err == nil {
		unknown, ok := obj.(*runtime.Unknown)
		if !ok || unknown == nil {
			err = fmt.Errorf("fake Pods.GetLogs expected reactor to return *runtime.Unknown, got %T", obj)
		} else {
			logs = unknown.Raw
		}
	}

	// 把反应器产生的错误延迟到真正发起请求时返回，保持返回类型为 *Request。
	fakeClient := &fakerest.RESTClient{
		Client: fakerest.CreateHTTPClient(func(request *http.Request) (*http.Response, error) {
			if err != nil {
				return nil, err
			}
			resp := &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(bytes.NewReader(logs)),
			}
			return resp, nil
		}),
		NegotiatedSerializer: scheme.Codecs.WithoutConversion(),
		GroupVersion:         c.Kind().GroupVersion(),
		VersionedAPIPath:     fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/log", c.Namespace(), name),
	}
	return fakeClient.Request()
}

// Evict 是 EvictV1beta1 的别名，保留 policy/v1beta1 的旧调用入口。
func (c *fakePods) Evict(ctx context.Context, eviction *policyv1beta1.Eviction) error {
	return c.EvictV1beta1(ctx, eviction)
}

// EvictV1 模拟 policy/v1 的 pods/eviction 子资源创建。
func (c *fakePods) EvictV1(ctx context.Context, eviction *policyv1.Eviction) error {
	action := core.CreateActionImpl{}
	action.Verb = "create"
	action.Namespace = c.Namespace()
	action.Resource = c.Resource()
	action.Subresource = "eviction"
	action.Object = eviction

	_, err := c.Fake.Invokes(action, eviction)
	return err
}

// EvictV1beta1 模拟 policy/v1beta1 的 pods/eviction 子资源创建。
func (c *fakePods) EvictV1beta1(ctx context.Context, eviction *policyv1beta1.Eviction) error {
	action := core.CreateActionImpl{}
	action.Verb = "create"
	action.Namespace = c.Namespace()
	action.Resource = c.Resource()
	action.Subresource = "eviction"
	action.Object = eviction

	_, err := c.Fake.Invokes(action, eviction)
	return err
}

// ProxyGet 模拟通过 apiserver 代理访问 Pod 端口的请求，返回 ResponseWrapper 以便
// 调用方沿用与真实客户端相同的读取方式。
func (c *fakePods) ProxyGet(scheme, name, port, path string, params map[string]string) restclient.ResponseWrapper {
	return c.Fake.InvokesProxy(core.NewProxyGetAction(c.Resource(), c.Namespace(), scheme, name, port, path, params))
}
