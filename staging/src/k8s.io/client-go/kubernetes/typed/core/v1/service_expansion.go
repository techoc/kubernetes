/*
Copyright 2016 The Kubernetes Authors.

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

package v1

import (
	"k8s.io/apimachinery/pkg/util/net"
	restclient "k8s.io/client-go/rest"
)

// ServiceExpansion 为生成的 ServiceInterface 追加手写方法。
//
// ProxyGet 仅暴露 GET 语义的服务代理访问；它不返回已解析的对象，而是直接给出
// 请求构造器，交由调用方读取原始响应。
type ServiceExpansion interface {
	ProxyGet(scheme, name, port, path string, params map[string]string) restclient.ResponseWrapper
}

// ProxyGet 经代理调用服务并返回响应。
//
// 名称采用 "scheme:name:port" 的形式，由 net.JoinSchemeNamePort 负责转义；
// 请求路径构造为 /api/v1/namespaces/<ns>/services/<scheme:name:port>/proxy/<path>。
func (c *services) ProxyGet(scheme, name, port, path string, params map[string]string) restclient.ResponseWrapper {
	request := c.GetClient().Get().
		Namespace(c.GetNamespace()).
		Resource("services").
		SubResource("proxy").
		Name(net.JoinSchemeNamePort(scheme, name, port)).
		Suffix(path)
	for k, v := range params {
		request = request.Param(k, v)
	}
	return request
}
