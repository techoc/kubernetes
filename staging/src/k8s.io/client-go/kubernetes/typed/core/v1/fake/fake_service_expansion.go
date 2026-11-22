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
	restclient "k8s.io/client-go/rest"
	core "k8s.io/client-go/testing"
)

// ProxyGet 用伪造的 ProxyGetAction 记录这次调用，使测试能对“通过 apiserver 代理访问
// Service”这一行为做断言，而不需要真的发起 HTTP 请求。
func (c *fakeServices) ProxyGet(scheme, name, port, path string, params map[string]string) restclient.ResponseWrapper {
	return c.Fake.InvokesProxy(core.NewProxyGetAction(c.Resource(), c.Namespace(), scheme, name, port, path, params))
}
