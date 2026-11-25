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

package testing

import (
	"fmt"
	"net/url"

	"k8s.io/apiserver/pkg/util/webhook"
)

// serviceResolver 总是返回固定 URL，用于把 webhook 请求导向测试服务器。
type serviceResolver struct {
	base url.URL
}

// NewServiceResolver 返回一个静态 service 解析器，总是返回给定 URL；
// 对 failResolve 命名空间则返回错误。
// 特殊命名空间 failResolve 用于模拟服务解析失败的分支。
func NewServiceResolver(base url.URL) webhook.ServiceResolver {
	return &serviceResolver{base}
}

// ResolveEndpoint 忽略 namespace/name/port，直接返回预置 URL。
func (f serviceResolver) ResolveEndpoint(namespace, name string, port int32) (*url.URL, error) {
	if namespace == "failResolve" {
		return nil, fmt.Errorf("couldn't resolve service location")
	}
	u := f.base
	return &u, nil
}

// panickingResolver 在解析时 panic，用于验证调用方对 panic 的恢复处理。
type panickingResolver struct {
	panicMessage string
}

// NewPanickingServiceResolver 返回一个在解析时 panic 的静态 service 解析器。
func NewPanickingServiceResolver(panicMessage string) webhook.ServiceResolver {
	return &panickingResolver{panicMessage}
}

// ResolveEndpoint 无条件 panic，消息由构造时指定。
func (f panickingResolver) ResolveEndpoint(namespace, name string, port int32) (*url.URL, error) {
	panic(f.panicMessage)
}
